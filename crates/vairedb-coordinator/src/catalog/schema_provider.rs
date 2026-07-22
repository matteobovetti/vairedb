//! DataFusion `SchemaProvider` that surfaces the metadata catalog as a set of
//! read-only virtual tables (under the `vairedb_catalog` schema), letting the
//! query planner inspect table, column, shard, and node metadata via SQL.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemTable;

use crate::catalog::{MetadataCatalog, ShardStrategy};
use crate::util::node_state_str;

/// The virtual tables exposed under the `vairedb_catalog` schema. This is the
/// single source of truth for which relations exist: `table_names`, `table`, and
/// `table_exist` all derive from it, so adding a table means adding one arm to
/// [`VaireDbCatalogSchema::build_provider`] and one entry here — never editing
/// three separate match/list sites.
const VIRTUAL_TABLES: [&str; 5] = [
    "tables",
    "columns",
    "shards",
    "nodes",
    "anonymization_secret",
];

/// `SchemaProvider` exposing the metadata catalog's contents as virtual tables.
/// Each table is materialized on demand from the live catalog state.
pub struct VaireDbCatalogSchema {
    catalog: Arc<MetadataCatalog>,
}

impl std::fmt::Debug for VaireDbCatalogSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaireDbCatalogSchema").finish()
    }
}

impl VaireDbCatalogSchema {
    /// Construct a schema provider backed by the given metadata catalog.
    pub fn new(catalog: Arc<MetadataCatalog>) -> Self {
        Self { catalog }
    }

    /// Build the in-memory provider for one virtual table, or `None` for an
    /// unknown name. Each arm supplies only the table's schema and column arrays;
    /// the shared [`make_memtable`] handles batch assembly and the empty-table
    /// fallback so that pattern lives in exactly one place.
    fn build_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match name {
            "tables" => Some(self.build_tables_provider()),
            "columns" => Some(self.build_columns_provider()),
            "shards" => Some(self.build_shards_provider()),
            "nodes" => Some(self.build_nodes_provider()),
            "anonymization_secret" => Some(self.build_anonymization_secret_provider()),
            _ => None,
        }
    }

    fn build_tables_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("shard_strategy", DataType::Utf8, false),
            Field::new("shard_key", DataType::Utf8, false),
            Field::new("shard_count", DataType::Int32, false),
            Field::new("replication_factor", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let tables = self.catalog.list_tables().unwrap_or_default();

        let mut table_names = Vec::with_capacity(tables.len());
        let mut strategies = Vec::with_capacity(tables.len());
        let mut keys = Vec::with_capacity(tables.len());
        let mut counts = Vec::with_capacity(tables.len());
        let mut repl_factors = Vec::with_capacity(tables.len());
        let mut created_ats: Vec<Option<i64>> = Vec::with_capacity(tables.len());

        for t in &tables {
            table_names.push(t.table_name.as_str());
            strategies.push(shard_strategy_name(t.shard_strategy));
            keys.push(t.shard_key.as_str());
            counts.push(t.shard_count as i32);
            repl_factors.push(t.replication_factor as i32);
            created_ats.push(t.created_at.as_ref().map(|ts| ts.seconds * 1_000_000));
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(strategies)),
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(counts)),
                Arc::new(Int32Array::from(repl_factors)),
                Arc::new(TimestampMicrosecondArray::from(created_ats)),
            ],
        )
    }

    fn build_columns_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Boolean, false),
            Field::new("default_expr", DataType::Utf8, true),
        ]));

        let tables = self.catalog.list_tables().unwrap_or_default();

        let mut tbl_names = Vec::new();
        let mut col_names = Vec::new();
        let mut ordinals = Vec::new();
        let mut data_types = Vec::new();
        let mut nullables = Vec::new();
        let mut defaults: Vec<Option<String>> = Vec::new();

        for t in &tables {
            for (i, col) in t.columns.iter().enumerate() {
                tbl_names.push(t.table_name.clone());
                col_names.push(col.name.clone());
                ordinals.push((i + 1) as i32);
                data_types.push(col.data_type.clone());
                nullables.push(col.nullable);
                defaults.push(if col.default_expr.is_empty() {
                    None
                } else {
                    Some(col.default_expr.clone())
                });
            }
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(tbl_names)),
                Arc::new(StringArray::from(col_names)),
                Arc::new(Int32Array::from(ordinals)),
                Arc::new(StringArray::from(data_types)),
                Arc::new(BooleanArray::from(nullables)),
                Arc::new(StringArray::from(defaults)),
            ],
        )
    }

    fn build_shards_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("shard_id", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("primary_node_id", DataType::Utf8, false),
            Field::new("replica_node_ids", DataType::Utf8, true),
            Field::new("hash_bucket", DataType::Int32, false),
            Field::new("range_lower", DataType::Utf8, true),
            Field::new("range_upper", DataType::Utf8, true),
        ]));

        let shards = self.catalog.list_all_shards().unwrap_or_default();

        let mut shard_ids = Vec::with_capacity(shards.len());
        let mut table_names = Vec::with_capacity(shards.len());
        let mut primary_nodes = Vec::with_capacity(shards.len());
        let mut replica_nodes = Vec::with_capacity(shards.len());
        let mut hash_buckets = Vec::with_capacity(shards.len());
        let mut range_lowers: Vec<Option<String>> = Vec::with_capacity(shards.len());
        let mut range_uppers: Vec<Option<String>> = Vec::with_capacity(shards.len());

        for p in &shards {
            shard_ids.push(p.shard_id.as_str());
            table_names.push(p.table_name.as_str());
            primary_nodes.push(p.primary_node_id.as_str());
            replica_nodes.push(p.replica_node_ids.join(","));
            hash_buckets.push(p.hash_bucket as i32);
            range_lowers.push(if p.range_lower.is_empty() {
                None
            } else {
                Some(p.range_lower.clone())
            });
            range_uppers.push(if p.range_upper.is_empty() {
                None
            } else {
                Some(p.range_upper.clone())
            });
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(shard_ids)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(primary_nodes)),
                Arc::new(StringArray::from(replica_nodes)),
                Arc::new(Int32Array::from(hash_buckets)),
                Arc::new(StringArray::from(range_lowers)),
                Arc::new(StringArray::from(range_uppers)),
            ],
        )
    }

    /// Build the `anonymization_secret` virtual table. Deliberately exposes only
    /// the secret id and algorithm — never the `secret_key`, which must not be
    /// readable via SQL.
    fn build_anonymization_secret_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("algo", DataType::Utf8, false),
        ]));

        let secrets = self
            .catalog
            .list_anonymization_secrets()
            .unwrap_or_default();

        let mut ids = Vec::with_capacity(secrets.len());
        let mut algos = Vec::with_capacity(secrets.len());
        for s in &secrets {
            ids.push(s.id.as_str());
            algos.push(s.algo.as_str());
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(algos)),
            ],
        )
    }

    fn build_nodes_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_id", DataType::Utf8, false),
            Field::new("advertised_address", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new(
                "last_heartbeat",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "registered_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let nodes = self.catalog.list_all_nodes().unwrap_or_default();

        let mut node_ids = Vec::with_capacity(nodes.len());
        let mut addresses = Vec::with_capacity(nodes.len());
        let mut states = Vec::with_capacity(nodes.len());
        let mut heartbeats: Vec<Option<i64>> = Vec::with_capacity(nodes.len());
        let mut registered: Vec<Option<i64>> = Vec::with_capacity(nodes.len());

        for n in &nodes {
            node_ids.push(n.node_id.as_str());
            addresses.push(n.advertised_address.as_str());
            states.push(node_state_str(n.state));
            heartbeats.push(n.last_heartbeat.as_ref().map(|ts| ts.seconds * 1_000_000));
            registered.push(n.registered_at.as_ref().map(|ts| ts.seconds * 1_000_000));
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(node_ids)),
                Arc::new(StringArray::from(addresses)),
                Arc::new(StringArray::from(states)),
                Arc::new(TimestampMicrosecondArray::from(heartbeats)),
                Arc::new(TimestampMicrosecondArray::from(registered)),
            ],
        )
    }
}

/// Wrap a single batch of `columns` (matching `schema`) in an in-memory
/// `TableProvider`. Centralizes the `RecordBatch`/`MemTable` construction that
/// every virtual-table builder shares; the inputs are coordinator-built and
/// schema-aligned, so construction is infallible here.
fn make_memtable(
    schema: Arc<Schema>,
    columns: Vec<datafusion::arrow::array::ArrayRef>,
) -> Arc<dyn TableProvider> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

#[async_trait]
impl SchemaProvider for VaireDbCatalogSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        VIRTUAL_TABLES.iter().map(|s| s.to_string()).collect()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.build_provider(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        VIRTUAL_TABLES.contains(&name)
    }
}

fn shard_strategy_name(value: i32) -> &'static str {
    match ShardStrategy::try_from(value) {
        Ok(ShardStrategy::Hash) => "HASH",
        Ok(ShardStrategy::Range) => "RANGE",
        _ => "UNSPECIFIED",
    }
}
