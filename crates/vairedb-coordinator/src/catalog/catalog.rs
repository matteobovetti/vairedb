//! Persistent metadata store backed by redb. Holds the authoritative records
//! for tables, shards, and nodes, and provides shard-assignment helpers. All
//! mutations run inside committed redb transactions; records are stored as
//! length-prefixed protobuf encodings.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use vairedb_common::proto::vairedb::v1::{
    AnonymizationSecret, NodeMeta, NodeState, SchemaMeta, ShardMeta, TableMeta, ViewMeta,
};

use crate::error::{CoordinatorError, Result};
use crate::util::{logical_shard_id, now_unix_secs};

type RecordTable = TableDefinition<'static, &'static str, &'static [u8]>;

const TABLES_TABLE: RecordTable = TableDefinition::new("tables");
const VIEWS_TABLE: RecordTable = TableDefinition::new("views");
const SCHEMAS_TABLE: RecordTable = TableDefinition::new("schemas");
const SHARDS_TABLE: RecordTable = TableDefinition::new("shards");
const NODES_TABLE: RecordTable = TableDefinition::new("nodes");
const ANONYMIZATION_SECRET_TABLE: RecordTable = TableDefinition::new("anonymization_secret");

/// Decode a stored record, mapping any prost failure to a sanitized
/// serialization error rather than leaking wire-format details.
fn decode_record<M: Message + Default>(bytes: &[u8]) -> Result<M> {
    M::decode(bytes).map_err(|e| CoordinatorError::Serialization(e.to_string()))
}

/// Half-open key range `["{name}:", "{name};\0")` that covers exactly the
/// `"{name}:..."`-prefixed keys. `';'` is the next ASCII codepoint after `':'`,
/// so the upper bound excludes any key for a different table.
fn prefix_range(name: &str) -> (String, String) {
    (format!("{}:", name), format!("{};\x00", name))
}

/// Authoritative metadata store for the coordinator, backed by a single redb
/// database holding the `tables`, `shards`, and `nodes` tables.
pub struct MetadataCatalog {
    db: Arc<Database>,
}

impl MetadataCatalog {
    /// Open (creating if needed) the redb database at `path` and ensure the
    /// catalog's tables exist. Errors if the underlying storage cannot be
    /// opened or the tables cannot be initialized.
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::create(path).map_err(|e| match e {
            redb::DatabaseError::Storage(se) => CoordinatorError::CatalogStorage(se),
            other => CoordinatorError::Internal(other.to_string()),
        })?;
        let catalog = Self { db: Arc::new(db) };
        catalog.init_tables()?;
        Ok(catalog)
    }

    fn init_tables(&self) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let _ = write_txn.open_table(TABLES_TABLE)?;
            let _ = write_txn.open_table(VIEWS_TABLE)?;
            let _ = write_txn.open_table(SCHEMAS_TABLE)?;
            let _ = write_txn.open_table(SHARDS_TABLE)?;
            let _ = write_txn.open_table(NODES_TABLE)?;
            let _ = write_txn.open_table(ANONYMIZATION_SECRET_TABLE)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Encode `value` and upsert it at `key` in `table` within a single
    /// committed write transaction.
    fn put_record<M: Message>(&self, table: RecordTable, key: &str, value: &M) -> Result<()> {
        let bytes = value.encode_to_vec();
        let write_txn = self.db.begin_write()?;
        {
            let mut t = write_txn.open_table(table)?;
            t.insert(key, bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Fetch and decode the record at `key`, or `None` if absent.
    fn get_record<M: Message + Default>(&self, table: RecordTable, key: &str) -> Result<Option<M>> {
        let read_txn = self.db.begin_read()?;
        let t = read_txn.open_table(table)?;
        match t.get(key)? {
            Some(val) => Ok(Some(decode_record(val.value())?)),
            None => Ok(None),
        }
    }

    /// Remove the record at `key` (no-op if absent).
    fn delete_record(&self, table: RecordTable, key: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut t = write_txn.open_table(table)?;
            t.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Decode every record in `table` that satisfies `keep`, in key order.
    fn list_records<M, F>(&self, table: RecordTable, keep: F) -> Result<Vec<M>>
    where
        M: Message + Default,
        F: Fn(&M) -> bool,
    {
        let read_txn = self.db.begin_read()?;
        let t = read_txn.open_table(table)?;
        let mut results = Vec::new();
        for entry in t.iter()? {
            let entry = entry.map_err(CoordinatorError::CatalogStorage)?;
            let value: M = decode_record(entry.1.value())?;
            if keep(&value) {
                results.push(value);
            }
        }
        Ok(results)
    }

    /// Decode every record in `table` whose key is `"{prefix}:..."`, in key order.
    fn scan_prefix<M: Message + Default>(
        &self,
        table: RecordTable,
        prefix: &str,
    ) -> Result<Vec<M>> {
        let (start, end) = prefix_range(prefix);
        let read_txn = self.db.begin_read()?;
        let t = read_txn.open_table(table)?;
        let mut results = Vec::new();
        for entry in t.range(start.as_str()..end.as_str())? {
            let entry = entry.map_err(CoordinatorError::CatalogStorage)?;
            results.push(decode_record(entry.1.value())?);
        }
        Ok(results)
    }

    /// Read `node_id`, apply `mutate`, and write it back. Errors with
    /// `NodeNotFound` if the node is absent.
    fn modify_node(&self, node_id: &str, mutate: impl FnOnce(&mut NodeMeta)) -> Result<()> {
        let mut node = self
            .get_node(node_id)?
            .ok_or_else(|| CoordinatorError::NodeNotFound(node_id.to_string()))?;
        mutate(&mut node);
        self.put_node(&node)
    }

    /// Upsert a table's metadata, keyed by its table name.
    pub fn put_table(&self, meta: &TableMeta) -> Result<()> {
        self.put_record(TABLES_TABLE, &meta.table_name, meta)
    }

    /// Claim a table name: store `meta` only if neither a table nor a view of that
    /// name exists yet, and report whether the claim succeeded.
    ///
    /// The existence checks and the write share one redb write transaction, and redb
    /// admits a single writer at a time, so two concurrent `CREATE TABLE` of the
    /// same name cannot both observe "absent" — nor can a `CREATE TABLE` and a
    /// `CREATE VIEW` racing for one name. Without this, a check-then-put sequence
    /// lets both callers pass the check and both go on to assign shards and
    /// broadcast DDL — two shard layouts for one catalog key, with the second
    /// silently overwriting the first.
    pub fn create_table_if_absent(&self, meta: &TableMeta) -> Result<bool> {
        let bytes = meta.encode_to_vec();
        let write_txn = self.db.begin_write()?;
        let claimed = {
            let views = write_txn.open_table(VIEWS_TABLE)?;
            let taken_by_view = views.get(meta.table_name.as_str())?.is_some();
            let mut t = write_txn.open_table(TABLES_TABLE)?;
            if taken_by_view || t.get(meta.table_name.as_str())?.is_some() {
                false
            } else {
                t.insert(meta.table_name.as_str(), bytes.as_slice())?;
                true
            }
        };
        write_txn.commit()?;
        Ok(claimed)
    }

    /// Fetch a table's metadata by name, or `None` if it does not exist.
    pub fn get_table(&self, name: &str) -> Result<Option<TableMeta>> {
        self.get_record(TABLES_TABLE, name)
    }

    /// Remove a table's metadata by name (no-op if absent). Does not touch the
    /// table's shard records; see `delete_shards_for_table`.
    pub fn delete_table(&self, name: &str) -> Result<()> {
        self.delete_record(TABLES_TABLE, name)
    }

    /// Return all registered tables, in key order.
    pub fn list_tables(&self) -> Result<Vec<TableMeta>> {
        self.list_records(TABLES_TABLE, |_| true)
    }

    /// Find the table carrying the index named `index_name`, or `None` if no
    /// table does.
    ///
    /// Indexes live inside their table's [`TableMeta`], because that is where
    /// every other per-table schema fact lives and it keeps a table and its
    /// indexes impossible to leave inconsistent. `DROP INDEX` names only the
    /// index, so resolving it means a scan — which is what this is. Cheap enough:
    /// it runs on DDL only, and the table list is the metadata of one cluster.
    ///
    /// `index_name` must already be canonical (see
    /// `query_router::canonicalize_ident`), as the stored names are.
    pub fn table_with_index(&self, index_name: &str) -> Result<Option<TableMeta>> {
        Ok(self
            .list_tables()?
            .into_iter()
            .find(|table| table.indexes.iter().any(|idx| idx.name == index_name)))
    }

    /// Find the table carrying an index-backed constraint named `name`, or `None`.
    ///
    /// A constraint VaireDB added to a table after the fact is enforced by one
    /// physical index per shard, named after the constraint — so unlike a constraint
    /// the shards declared in their own `CREATE TABLE`, its name occupies the
    /// cluster-wide relation namespace and has to be resolvable the same way an
    /// index's is. `name` must already be canonical.
    pub fn table_with_index_backed_constraint(&self, name: &str) -> Result<Option<TableMeta>> {
        Ok(self.list_tables()?.into_iter().find(|table| {
            table
                .constraints
                .iter()
                .any(|c| c.index_backed && c.name == name)
        }))
    }

    /// Claim a view name: store `meta` only if neither a view nor a table of that
    /// name exists yet, and report whether the claim succeeded.
    ///
    /// Views and tables live in separate redb tables but in one relation namespace,
    /// so the claim has to look in both — and it looks in both inside a single write
    /// transaction, for the reason [`Self::create_table_if_absent`] gives.
    pub fn create_view_if_absent(&self, meta: &ViewMeta) -> Result<bool> {
        let bytes = meta.encode_to_vec();
        let write_txn = self.db.begin_write()?;
        let claimed = {
            let tables = write_txn.open_table(TABLES_TABLE)?;
            let taken_by_table = tables.get(meta.view_name.as_str())?.is_some();
            let mut t = write_txn.open_table(VIEWS_TABLE)?;
            if taken_by_table || t.get(meta.view_name.as_str())?.is_some() {
                false
            } else {
                t.insert(meta.view_name.as_str(), bytes.as_slice())?;
                true
            }
        };
        write_txn.commit()?;
        Ok(claimed)
    }

    /// Upsert a view's definition, keyed by its view name. Used to redefine a view
    /// that already exists (`CREATE OR REPLACE VIEW`, `ALTER VIEW ... AS`); a new
    /// view is claimed with [`Self::create_view_if_absent`] instead.
    pub fn put_view(&self, meta: &ViewMeta) -> Result<()> {
        self.put_record(VIEWS_TABLE, &meta.view_name, meta)
    }

    /// Fetch a view's definition by name, or `None` if no view of that name exists.
    pub fn get_view(&self, name: &str) -> Result<Option<ViewMeta>> {
        self.get_record(VIEWS_TABLE, name)
    }

    /// Remove a view's definition by name (no-op if absent).
    pub fn delete_view(&self, name: &str) -> Result<()> {
        self.delete_record(VIEWS_TABLE, name)
    }

    /// Return every registered view, in key order.
    pub fn list_views(&self) -> Result<Vec<ViewMeta>> {
        self.list_records(VIEWS_TABLE, |_| true)
    }

    /// Claim a schema name: store `meta` only if no schema of that name exists
    /// yet, and report whether the claim succeeded.
    ///
    /// The check and the write share one redb write transaction, for the reason
    /// [`Self::create_table_if_absent`] gives — two concurrent `CREATE SCHEMA` of
    /// one name must not both see "absent" and both report success.
    ///
    /// The default schema is never stored — it always exists and cannot be created —
    /// so this is only ever called for a named one.
    pub fn create_schema_if_absent(&self, meta: &SchemaMeta) -> Result<bool> {
        let bytes = meta.encode_to_vec();
        let write_txn = self.db.begin_write()?;
        let claimed = {
            let mut t = write_txn.open_table(SCHEMAS_TABLE)?;
            if t.get(meta.schema_name.as_str())?.is_some() {
                false
            } else {
                t.insert(meta.schema_name.as_str(), bytes.as_slice())?;
                true
            }
        };
        write_txn.commit()?;
        Ok(claimed)
    }

    /// Fetch a schema's metadata by name, or `None` if it does not exist. The
    /// default schema and the metadata schemas are not records, so they are not found
    /// here: existence questions that must account for them go through the handler's
    /// `require_schema_exists`.
    pub fn get_schema(&self, name: &str) -> Result<Option<SchemaMeta>> {
        self.get_record(SCHEMAS_TABLE, name)
    }

    /// Remove a schema by name (no-op if absent). Does not touch the relations it
    /// contained: `DROP SCHEMA` refuses a non-empty schema, so by the time this
    /// runs there are none.
    pub fn delete_schema(&self, name: &str) -> Result<()> {
        self.delete_record(SCHEMAS_TABLE, name)
    }

    /// Return every named schema, in key order. The default schema is not among
    /// them — it is not a record.
    pub fn list_schemas(&self) -> Result<Vec<SchemaMeta>> {
        self.list_records(SCHEMAS_TABLE, |_| true)
    }

    /// Upsert a shard's metadata under the composite key `"{table}:{shard_id}"`,
    /// keeping shards for one table grouped together for prefix scans.
    pub fn put_shard(&self, meta: &ShardMeta) -> Result<()> {
        let key = format!("{}:{}", meta.table_name, meta.shard_id);
        self.put_record(SHARDS_TABLE, &key, meta)
    }

    /// Return all shards belonging to `table_name`, ordered by hash bucket.
    ///
    /// Deliberately not in stored-key order: the keys are the strings
    /// `"{table}:shard{n}"`, so a raw scan returns `shard10` before `shard2` and
    /// `shards[i]` stops being the shard of bucket `i` from eleven shards on.
    /// Sorting on the bucket each record carries restores that, which is what
    /// every caller that reads the list in order — broadcast plans, the
    /// `vairedb_catalog.shards` view, per-shard DDL — reasonably expects. Routing
    /// does not rely on it and matches on the bucket itself; see
    /// [`write_router::shard_for_bucket`](crate::write_router::shard_for_bucket).
    pub fn get_shards_for_table(&self, table_name: &str) -> Result<Vec<ShardMeta>> {
        let mut shards: Vec<ShardMeta> = self.scan_prefix(SHARDS_TABLE, table_name)?;
        shards.sort_by_key(|shard| shard.hash_bucket);
        Ok(shards)
    }

    /// Delete every shard record belonging to `table_name`. Collects matching
    /// keys in a read transaction, then removes them in one write transaction.
    pub fn delete_shards_for_table(&self, table_name: &str) -> Result<()> {
        let (start, end) = prefix_range(table_name);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SHARDS_TABLE)?;
        let mut keys_to_delete = Vec::new();
        for entry in table.range(start.as_str()..end.as_str())? {
            let entry = entry.map_err(CoordinatorError::CatalogStorage)?;
            keys_to_delete.push(entry.0.value().to_string());
        }
        drop(table);
        drop(read_txn);

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SHARDS_TABLE)?;
            for key in &keys_to_delete {
                table.remove(key.as_str())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Upsert a node's metadata, keyed by its node id.
    pub fn put_node(&self, meta: &NodeMeta) -> Result<()> {
        self.put_record(NODES_TABLE, &meta.node_id, meta)
    }

    /// Fetch a node's metadata by id, or `None` if it is not registered.
    pub fn get_node(&self, node_id: &str) -> Result<Option<NodeMeta>> {
        self.get_record(NODES_TABLE, node_id)
    }

    /// Return only nodes currently in the `Alive` state.
    pub fn list_alive_nodes(&self) -> Result<Vec<NodeMeta>> {
        self.list_records(NODES_TABLE, |node: &NodeMeta| {
            node.state == NodeState::Alive as i32
        })
    }

    /// Return every shard record across all tables, by table name then hash
    /// bucket — the same bucket ordering [`Self::get_shards_for_table`] uses, so
    /// `vairedb_catalog.shards` lists a table's shards `0, 1, 2, …` rather than
    /// lexicographically by shard id.
    pub fn list_all_shards(&self) -> Result<Vec<ShardMeta>> {
        let mut shards: Vec<ShardMeta> = self.list_records(SHARDS_TABLE, |_| true)?;
        shards.sort_by(|a, b| {
            a.table_name
                .cmp(&b.table_name)
                .then(a.hash_bucket.cmp(&b.hash_bucket))
        });
        Ok(shards)
    }

    /// Return every registered node regardless of state, in key order.
    pub fn list_all_nodes(&self) -> Result<Vec<NodeMeta>> {
        self.list_records(NODES_TABLE, |_| true)
    }

    /// Upsert an anonymization secret, keyed by its id.
    pub fn put_anonymization_secret(&self, secret: &AnonymizationSecret) -> Result<()> {
        self.put_record(ANONYMIZATION_SECRET_TABLE, &secret.id, secret)
    }

    /// Fetch an anonymization secret by id, or `None` if it is not registered.
    pub fn get_anonymization_secret(&self, id: &str) -> Result<Option<AnonymizationSecret>> {
        self.get_record(ANONYMIZATION_SECRET_TABLE, id)
    }

    /// Return every registered anonymization secret, in key order.
    pub fn list_anonymization_secrets(&self) -> Result<Vec<AnonymizationSecret>> {
        self.list_records(ANONYMIZATION_SECRET_TABLE, |_| true)
    }

    /// Set a node's state. Errors with `NodeNotFound` if the node is absent.
    pub fn update_node_state(&self, node_id: &str, state: NodeState) -> Result<()> {
        self.modify_node(node_id, |node| node.state = state as i32)
    }

    /// Record a fresh heartbeat for a node and mark it `Alive`. Errors with
    /// `NodeNotFound` if the node is absent.
    pub fn update_node_heartbeat(&self, node_id: &str) -> Result<()> {
        self.modify_node(node_id, |node| {
            node.last_heartbeat = Some(prost_types::Timestamp {
                seconds: now_unix_secs() as i64,
                nanos: 0,
            });
            node.state = NodeState::Alive as i32;
        })
    }

    /// Build (but do not persist) `shard_count` shard assignments for
    /// `table_name`, distributing primaries round-robin over alive nodes and
    /// placing up to `replication_factor - 1` replicas on the following nodes.
    /// Errors with `NoAliveNodes` if no node is currently alive.
    pub fn assign_shards_round_robin(
        &self,
        table_name: &str,
        shard_count: u32,
        replication_factor: u32,
    ) -> Result<Vec<ShardMeta>> {
        let alive_nodes = self.list_alive_nodes()?;
        if alive_nodes.is_empty() {
            return Err(CoordinatorError::NoAliveNodes);
        }

        let node_count = alive_nodes.len();
        let mut shards = Vec::new();

        for i in 0..shard_count {
            let primary_idx = (i as usize) % node_count;
            let primary_node_id = alive_nodes[primary_idx].node_id.clone();

            let mut replica_node_ids = Vec::new();
            for r in 1..replication_factor {
                let replica_idx = (primary_idx + r as usize) % node_count;
                if replica_idx != primary_idx {
                    replica_node_ids.push(alive_nodes[replica_idx].node_id.clone());
                }
            }

            let shard = ShardMeta {
                shard_id: logical_shard_id(i),
                table_name: table_name.to_string(),
                primary_node_id,
                replica_node_ids,
                hash_bucket: i,
                range_lower: String::new(),
                range_upper: String::new(),
            };
            shards.push(shard);
        }

        Ok(shards)
    }

    /// Return a map from node id to advertised address for all registered
    /// nodes, used to resolve where to route requests.
    pub fn get_node_address_map(&self) -> Result<HashMap<String, String>> {
        Ok(self
            .list_all_nodes()?
            .into_iter()
            .map(|node| (node.node_id, node.advertised_address))
            .collect())
    }
}
