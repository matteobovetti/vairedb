use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use datafusion::catalog::SchemaProvider;
use datafusion::execution::context::SessionContext;

use vairedb_coordinator::catalog::{
    ColumnDef, MetadataCatalog, NodeMeta, NodeState, ShardMeta, ShardStrategy, TableMeta,
    VaireDbCatalogSchema,
};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_catalog_provider_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn make_catalog() -> Arc<MetadataCatalog> {
    let path = temp_db_path();
    Arc::new(MetadataCatalog::open(&path).unwrap())
}

fn sample_table() -> TableMeta {
    TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "orders".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "amount".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: true,
                default_expr: "0".to_string(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "id".to_string(),
        shard_count: 2,
        replication_factor: 2,
        created_at: Some(prost_types::Timestamp {
            seconds: 1700000000,
            nanos: 0,
        }),
    }
}

#[tokio::test]
async fn test_table_names() {
    let catalog = make_catalog();
    let schema = VaireDbCatalogSchema::new(catalog);
    let mut names = schema.table_names();
    names.sort();
    assert_eq!(
        names,
        vec![
            "anonymization_secret",
            "columns",
            "nodes",
            "shards",
            "tables"
        ]
    );
}

#[tokio::test]
async fn test_table_exist() {
    let catalog = make_catalog();
    let schema = VaireDbCatalogSchema::new(catalog);
    assert!(schema.table_exist("tables"));
    assert!(schema.table_exist("columns"));
    assert!(schema.table_exist("shards"));
    assert!(schema.table_exist("nodes"));
    assert!(schema.table_exist("anonymization_secret"));
    assert!(!schema.table_exist("nonexistent"));
}

#[tokio::test]
async fn test_anonymization_secret_view_hides_secret_key() {
    use vairedb_coordinator::catalog::AnonymizationSecret;

    let catalog = make_catalog();
    catalog
        .put_anonymization_secret(&AnonymizationSecret {
            id: "sid".to_string(),
            algo: "HMAC-SHA256".to_string(),
            secret_key: "top_secret_pepper".to_string(),
        })
        .unwrap();

    let schema = VaireDbCatalogSchema::new(Arc::clone(&catalog));
    let provider = schema.table("anonymization_secret").await.unwrap().unwrap();

    // The schema must expose only id and algo — never the secret key.
    let fields = provider.schema();
    let names: Vec<&str> = fields.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "algo"]);

    let ctx = SessionContext::new();
    ctx.register_table("s", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM s")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let rendered = format!("{:?}", batches[0]);
    assert!(
        !rendered.contains("top_secret_pepper"),
        "secret key leaked into the view: {rendered}"
    );
}

#[tokio::test]
async fn test_tables_view_empty() {
    let catalog = make_catalog();
    let schema = VaireDbCatalogSchema::new(catalog);
    let provider = schema.table("tables").await.unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let batches = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 0);
}

#[tokio::test]
async fn test_tables_view_with_data() {
    let catalog = make_catalog();
    catalog.put_table(&sample_table()).unwrap();

    let schema = VaireDbCatalogSchema::new(Arc::clone(&catalog));
    let provider = schema.table("tables").await.unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let batches = ctx
        .sql("SELECT table_name, shard_strategy, shard_key, shard_count, replication_factor FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);

    let table_name = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(table_name.value(0), "orders");

    let strategy = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(strategy.value(0), "HASH");

    let key = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(key.value(0), "id");

    let count = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(count.value(0), 2);

    let rf = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(rf.value(0), 2);
}

#[tokio::test]
async fn test_columns_view() {
    let catalog = make_catalog();
    catalog.put_table(&sample_table()).unwrap();

    let schema = VaireDbCatalogSchema::new(Arc::clone(&catalog));
    let provider = schema.table("columns").await.unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let batches = ctx.sql("SELECT table_name, column_name, ordinal_position, data_type, is_nullable, default_expr FROM t ORDER BY ordinal_position").await.unwrap().collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 2);

    let col_names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(col_names.value(0), "id");
    assert_eq!(col_names.value(1), "amount");

    let nullables = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap();
    assert!(!nullables.value(0));
    assert!(nullables.value(1));
}

#[tokio::test]
async fn test_shards_view() {
    let catalog = make_catalog();
    catalog
        .put_shard(&ShardMeta {
            shard_id: "shard0".to_string(),
            table_name: "orders".to_string(),
            primary_node_id: "node-1".to_string(),
            replica_node_ids: vec!["node-2".to_string(), "node-3".to_string()],
            hash_bucket: 0,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();

    let schema = VaireDbCatalogSchema::new(Arc::clone(&catalog));
    let provider = schema.table("shards").await.unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let batches = ctx
        .sql("SELECT shard_id, table_name, primary_node_id, replica_node_ids, hash_bucket FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);

    let primary = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(primary.value(0), "node-1");

    let replicas = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(replicas.value(0), "node-2,node-3");
}

#[tokio::test]
async fn test_nodes_view() {
    let catalog = make_catalog();
    catalog
        .put_node(&NodeMeta {
            node_id: "node-1".to_string(),
            advertised_address: "10.0.1.5:50051".to_string(),
            state: NodeState::Alive as i32,
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            registered_at: Some(prost_types::Timestamp {
                seconds: 1699000000,
                nanos: 0,
            }),
        })
        .unwrap();

    let schema = VaireDbCatalogSchema::new(Arc::clone(&catalog));
    let provider = schema.table("nodes").await.unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let batches = ctx
        .sql("SELECT node_id, advertised_address, state FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);

    let node_id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(node_id.value(0), "node-1");

    let state = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(state.value(0), "ALIVE");
}

#[tokio::test]
async fn test_nonexistent_table_returns_none() {
    let catalog = make_catalog();
    let schema = VaireDbCatalogSchema::new(catalog);
    assert!(schema.table("foo").await.unwrap().is_none());
}

#[tokio::test]
async fn test_full_query_through_session_context() {
    let catalog = make_catalog();
    catalog.put_table(&sample_table()).unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "node-1".to_string(),
            advertised_address: "10.0.1.5:50051".to_string(),
            state: NodeState::Alive as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let ctx = SessionContext::new();
    let schema_provider = Arc::new(VaireDbCatalogSchema::new(Arc::clone(&catalog)));
    let default_catalog = ctx.catalog("datafusion").unwrap();
    default_catalog
        .register_schema("vairedb_catalog", schema_provider)
        .unwrap();

    let batches = ctx
        .sql("SELECT table_name FROM vairedb_catalog.tables")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "orders");

    let batches = ctx
        .sql("SELECT node_id, state FROM vairedb_catalog.nodes")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let node_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(node_ids.value(0), "node-1");
}
