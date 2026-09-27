//! DataFusion `SchemaProvider` that surfaces the metadata catalog as a set of
//! read-only virtual tables (under the `vairedb_catalog` schema), letting the
//! query planner inspect table, column, shard, and node metadata via SQL.

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

/// Materializes one virtual table from the live catalog state.
type VirtualTableBuilder = fn(&VaireDbCatalogSchema) -> Arc<dyn TableProvider>;

/// The virtual tables exposed under the `vairedb_catalog` schema, each paired with
/// its builder. `table_names`, `table` and `table_exist` all derive from this one
/// list, which makes both halves of the old split unrepresentable: a name with no
/// builder is a relation DataFusion believes in and then cannot read, and a builder
/// with no name is a table only a query that already knows it can reach.
const VIRTUAL_TABLES: [(&str, VirtualTableBuilder); 7] = [
    ("schemas", VaireDbCatalogSchema::build_schemas_provider),
    ("tables", VaireDbCatalogSchema::build_tables_provider),
    ("views", VaireDbCatalogSchema::build_views_provider),
    ("columns", VaireDbCatalogSchema::build_columns_provider),
    ("shards", VaireDbCatalogSchema::build_shards_provider),
    ("nodes", VaireDbCatalogSchema::build_nodes_provider),
    (
        "anonymization_secret",
        VaireDbCatalogSchema::build_anonymization_secret_provider,
    ),
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
    /// unknown name. Each builder supplies only the table's schema and column
    /// arrays; the shared [`make_memtable`] handles batch assembly so that pattern
    /// lives in exactly one place.
    fn build_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        VIRTUAL_TABLES
            .iter()
            .find(|(table, _)| *table == name)
            .map(|(_, build)| build(self))
    }

    /// Build the `schemas` virtual table: the namespaces someone created. The
    /// default schema is not listed, because it is not a record — it always exists,
    /// and a relation in it carries no qualifier.
    fn build_schemas_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_name", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let schemas = self.catalog.list_schemas().unwrap_or_default();

        let mut names = Vec::with_capacity(schemas.len());
        let mut created_ats: Vec<Option<i64>> = Vec::with_capacity(schemas.len());
        for s in &schemas {
            names.push(s.schema_name.as_str());
            created_ats.push(micros(s.created_at.as_ref()));
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(TimestampMicrosecondArray::from(created_ats)),
            ],
        )
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
            created_ats.push(micros(t.created_at.as_ref()));
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

    /// Build the `views` virtual table. A view is stored as its query text and
    /// nothing else, so this is the whole record: there are no columns to list in
    /// the `columns` table, since a view's columns are whatever its query returns
    /// the next time it is read.
    fn build_views_provider(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("view_name", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, false),
            // The explicit column list the client gave, comma-joined, or NULL when
            // it gave none and the query's own output names stand.
            Field::new("columns", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let views = self.catalog.list_views().unwrap_or_default();

        let mut names = Vec::with_capacity(views.len());
        let mut definitions = Vec::with_capacity(views.len());
        let mut columns: Vec<Option<String>> = Vec::with_capacity(views.len());
        let mut created_ats: Vec<Option<i64>> = Vec::with_capacity(views.len());

        for v in &views {
            names.push(v.view_name.as_str());
            definitions.push(v.definition.as_str());
            columns.push((!v.columns.is_empty()).then(|| v.columns.join(",")));
            created_ats.push(micros(v.created_at.as_ref()));
        }

        make_memtable(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(definitions)),
                Arc::new(StringArray::from(columns)),
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
        let mut defaults: Vec<Option<&str>> = Vec::new();

        for t in &tables {
            for (i, col) in t.columns.iter().enumerate() {
                tbl_names.push(t.table_name.as_str());
                col_names.push(col.name.as_str());
                ordinals.push((i + 1) as i32);
                data_types.push(col.data_type.as_str());
                nullables.push(col.nullable);
                defaults.push(non_empty(&col.default_expr));
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
        let mut range_lowers: Vec<Option<&str>> = Vec::with_capacity(shards.len());
        let mut range_uppers: Vec<Option<&str>> = Vec::with_capacity(shards.len());

        for p in &shards {
            shard_ids.push(p.shard_id.as_str());
            table_names.push(p.table_name.as_str());
            primary_nodes.push(p.primary_node_id.as_str());
            replica_nodes.push(p.replica_node_ids.join(","));
            hash_buckets.push(p.hash_bucket as i32);
            range_lowers.push(non_empty(&p.range_lower));
            range_uppers.push(non_empty(&p.range_upper));
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
            heartbeats.push(micros(n.last_heartbeat.as_ref()));
            registered.push(micros(n.registered_at.as_ref()));
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
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .expect("the builder's arrays match the schema it declared");
    Arc::new(
        MemTable::try_new(schema, vec![vec![batch]]).expect("one batch of the table's own schema"),
    )
}

/// A protobuf timestamp as the epoch microseconds the Arrow column holds, or `None`
/// for an unset one.
fn micros(ts: Option<&prost_types::Timestamp>) -> Option<i64> {
    ts.map(|ts| ts.seconds * 1_000_000)
}

/// `value`, unless it is empty — which is how a protobuf record spells "unset" for
/// a string field, and what a nullable catalog column has to surface as NULL.
fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

#[async_trait]
impl SchemaProvider for VaireDbCatalogSchema {
    fn table_names(&self) -> Vec<String> {
        VIRTUAL_TABLES
            .iter()
            .map(|(name, _)| name.to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.build_provider(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        VIRTUAL_TABLES.iter().any(|(table, _)| *table == name)
    }
}

fn shard_strategy_name(value: i32) -> &'static str {
    match ShardStrategy::try_from(value) {
        Ok(ShardStrategy::Hash) => "HASH",
        Ok(ShardStrategy::Range) => "RANGE",
        _ => "UNSPECIFIED",
    }
}
