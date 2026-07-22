//! Persistent metadata store backed by redb. Holds the authoritative records
//! for tables, shards, and nodes, and provides shard-assignment helpers. All
//! mutations run inside committed redb transactions; records are stored as
//! length-prefixed protobuf encodings.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use vairedb_common::proto::vairedb::v1::{
    AnonymizationSecret, NodeMeta, NodeState, ShardMeta, TableMeta,
};

use crate::error::{CoordinatorError, Result};
use crate::util::{logical_shard_id, now_unix_secs};

type RecordTable = TableDefinition<'static, &'static str, &'static [u8]>;

const TABLES_TABLE: RecordTable = TableDefinition::new("tables");
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

    /// Upsert a shard's metadata under the composite key `"{table}:{shard_id}"`,
    /// keeping shards for one table grouped together for prefix scans.
    pub fn put_shard(&self, meta: &ShardMeta) -> Result<()> {
        let key = format!("{}:{}", meta.table_name, meta.shard_id);
        self.put_record(SHARDS_TABLE, &key, meta)
    }

    /// Return all shards belonging to `table_name`, in shard-key order.
    pub fn get_shards_for_table(&self, table_name: &str) -> Result<Vec<ShardMeta>> {
        self.scan_prefix(SHARDS_TABLE, table_name)
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

    /// Return every shard record across all tables, in key order.
    pub fn list_all_shards(&self) -> Result<Vec<ShardMeta>> {
        self.list_records(SHARDS_TABLE, |_| true)
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
