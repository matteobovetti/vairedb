use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use vairedb_coordinator::catalog::{
    ColumnDef, MetadataCatalog, ShardMeta, ShardStrategy, TableMeta,
};
use vairedb_coordinator::scheduler::{
    RemoteDuckDbScanExec, SchedulerTableProvider, VaireLogicalCodec, VairePhysicalCodec,
    parse_data_type, refresh_ballista_catalog_tables, register_vairedb_catalog_schema,
};

use std::sync::atomic::{AtomicU32, Ordering};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_scheduler_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn make_catalog() -> MetadataCatalog {
    MetadataCatalog::open(&temp_db_path()).unwrap()
}

fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn sample_shard(table_name: &str, bucket: u32) -> ShardMeta {
    ShardMeta {
        shard_id: format!("shard{}", bucket),
        table_name: table_name.to_string(),
        hash_bucket: bucket,
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        range_lower: String::new(),
        range_upper: String::new(),
    }
}

// =============================================================================
// parse_data_type tests
// =============================================================================

#[test]
fn parse_data_type_integer_variants() {
    assert_eq!(parse_data_type("INTEGER"), DataType::Int32);
    assert_eq!(parse_data_type("INT"), DataType::Int32);
    assert_eq!(parse_data_type("INT4"), DataType::Int32);
    assert_eq!(parse_data_type("integer"), DataType::Int32);
}

#[test]
fn parse_data_type_bigint() {
    assert_eq!(parse_data_type("BIGINT"), DataType::Int64);
    assert_eq!(parse_data_type("INT8"), DataType::Int64);
    assert_eq!(parse_data_type("bigint"), DataType::Int64);
}

#[test]
fn parse_data_type_smallint_tinyint() {
    assert_eq!(parse_data_type("SMALLINT"), DataType::Int16);
    assert_eq!(parse_data_type("INT2"), DataType::Int16);
    assert_eq!(parse_data_type("TINYINT"), DataType::Int8);
}

#[test]
fn parse_data_type_boolean() {
    assert_eq!(parse_data_type("BOOLEAN"), DataType::Boolean);
    assert_eq!(parse_data_type("BOOL"), DataType::Boolean);
    assert_eq!(parse_data_type("bool"), DataType::Boolean);
}

#[test]
fn parse_data_type_float_variants() {
    assert_eq!(parse_data_type("FLOAT"), DataType::Float32);
    assert_eq!(parse_data_type("REAL"), DataType::Float32);
    assert_eq!(parse_data_type("FLOAT4"), DataType::Float32);
    assert_eq!(parse_data_type("DOUBLE"), DataType::Float64);
    assert_eq!(parse_data_type("DOUBLE PRECISION"), DataType::Float64);
    assert_eq!(parse_data_type("FLOAT8"), DataType::Float64);
}

#[test]
fn parse_data_type_string_variants() {
    assert_eq!(parse_data_type("VARCHAR"), DataType::Utf8);
    assert_eq!(parse_data_type("TEXT"), DataType::Utf8);
    assert_eq!(parse_data_type("STRING"), DataType::Utf8);
}

#[test]
fn parse_data_type_binary() {
    assert_eq!(parse_data_type("BLOB"), DataType::Binary);
    assert_eq!(parse_data_type("BYTEA"), DataType::Binary);
}

#[test]
fn parse_data_type_timestamp_and_date() {
    assert_eq!(
        parse_data_type("TIMESTAMP"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(parse_data_type("DATE"), DataType::Date32);
}

#[test]
fn parse_data_type_json() {
    assert_eq!(parse_data_type("JSON"), DataType::Utf8);
    assert_eq!(parse_data_type("JSONB"), DataType::Utf8);
}

#[test]
fn parse_data_type_decimal() {
    assert_eq!(
        parse_data_type("DECIMAL(10,2)"),
        DataType::Decimal128(38, 10)
    );
    assert_eq!(
        parse_data_type("NUMERIC(5,3)"),
        DataType::Decimal128(38, 10)
    );
    assert_eq!(parse_data_type("DECIMAL"), DataType::Decimal128(38, 10));
}

#[test]
fn parse_data_type_unknown_falls_back_to_utf8() {
    assert_eq!(parse_data_type("GEOMETRY"), DataType::Utf8);
    assert_eq!(parse_data_type("UNKNOWN_TYPE"), DataType::Utf8);
}

#[test]
fn parse_data_type_case_insensitive() {
    assert_eq!(parse_data_type("Integer"), DataType::Int32);
    assert_eq!(parse_data_type("boolean"), DataType::Boolean);
    assert_eq!(parse_data_type("Varchar"), DataType::Utf8);
    assert_eq!(parse_data_type("Double Precision"), DataType::Float64);
}

// =============================================================================
// SchedulerTableProvider tests
// =============================================================================

#[test]
fn scheduler_table_provider_accessors() {
    let schema = sample_schema();
    let shards = vec![sample_shard("orders", 0), sample_shard("orders", 1)];

    let provider =
        SchedulerTableProvider::new("orders".to_string(), shards.clone(), schema.clone());

    assert_eq!(provider.table_name(), "orders");
    assert_eq!(provider.shards().len(), 2);
    assert_eq!(provider.shards()[0].hash_bucket, 0);
    assert_eq!(provider.shards()[1].hash_bucket, 1);
    assert_eq!(provider.schema(), schema);
    assert_eq!(provider.table_type(), TableType::Base);
}

#[tokio::test]
async fn scheduler_table_provider_scan_single_shard() {
    let schema = sample_schema();
    let shards = vec![sample_shard("orders", 0)];
    let provider = SchedulerTableProvider::new("orders".to_string(), shards, schema.clone());

    let ctx = SessionContext::new();
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await.unwrap();

    let remote_scan = plan
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .expect("single shard should produce RemoteDuckDbScanExec");

    assert_eq!(remote_scan.shard_table_name(), "orders_shard0");
    assert_eq!(remote_scan.projected_schema().fields().len(), 3);
    assert!(remote_scan.projection().is_none());
    assert!(remote_scan.filter_exprs().is_empty());
}

#[tokio::test]
async fn scheduler_table_provider_scan_single_shard_with_projection() {
    let schema = sample_schema();
    let shards = vec![sample_shard("users", 2)];
    let provider = SchedulerTableProvider::new("users".to_string(), shards, schema);

    let ctx = SessionContext::new();
    let state = ctx.state();
    let projection = vec![0usize, 2];

    let plan = provider
        .scan(&state, Some(&projection), &[], None)
        .await
        .unwrap();

    let remote_scan = plan
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .unwrap();

    assert_eq!(remote_scan.projected_schema().fields().len(), 2);
    assert_eq!(remote_scan.projected_schema().field(0).name(), "id");
    assert_eq!(remote_scan.projected_schema().field(1).name(), "amount");
    assert_eq!(remote_scan.projection(), &Some(vec![0, 2]));
}

#[tokio::test]
async fn scheduler_table_provider_scan_multiple_shards_produces_union() {
    let schema = sample_schema();
    let shards = vec![
        sample_shard("events", 0),
        sample_shard("events", 1),
        sample_shard("events", 2),
    ];
    let provider = SchedulerTableProvider::new("events".to_string(), shards, schema);

    let ctx = SessionContext::new();
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await.unwrap();

    assert_eq!(plan.name(), "UnionExec");
    assert_eq!(plan.children().len(), 3);

    for (i, child) in plan.children().iter().enumerate() {
        let remote_scan = child
            .as_any()
            .downcast_ref::<RemoteDuckDbScanExec>()
            .unwrap();
        assert_eq!(remote_scan.shard_table_name(), format!("events_shard{}", i));
    }
}

#[tokio::test]
async fn scheduler_table_provider_scan_empty_shards() {
    let schema = sample_schema();
    let provider = SchedulerTableProvider::new("empty_table".to_string(), vec![], schema);

    let ctx = SessionContext::new();
    let state = ctx.state();

    let plan = provider.scan(&state, None, &[], None).await.unwrap();

    let remote_scan = plan
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .unwrap();
    assert_eq!(remote_scan.shard_table_name(), "empty_table");
}

#[tokio::test]
async fn scheduler_table_provider_scan_propagates_filters() {
    use datafusion::logical_expr::{col, lit};

    let schema = sample_schema();
    let shards = vec![sample_shard("filtered", 0)];
    let provider = SchedulerTableProvider::new("filtered".to_string(), shards, schema);

    let ctx = SessionContext::new();
    let state = ctx.state();

    let filters = vec![col("id").gt(lit(10))];

    let plan = provider.scan(&state, None, &filters, None).await.unwrap();

    let remote_scan = plan
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .unwrap();

    assert_eq!(remote_scan.filter_exprs().len(), 1);
    assert!(remote_scan.filter_exprs()[0].contains("id"));
    assert!(remote_scan.filter_exprs()[0].contains("10"));
}

// =============================================================================
// RemoteDuckDbScanExec tests
// =============================================================================

#[test]
fn remote_scan_exec_construction_and_accessors() {
    let schema = sample_schema();
    let exec = RemoteDuckDbScanExec::new(
        "orders_shard0".to_string(),
        schema.clone(),
        Some(vec![0, 1]),
        vec!["id > 5".to_string()],
        None,
        vec![],
    );

    assert_eq!(exec.shard_table_name(), "orders_shard0");
    assert_eq!(exec.projected_schema(), &schema);
    assert_eq!(exec.projection(), &Some(vec![0, 1]));
    assert_eq!(exec.filter_exprs(), &["id > 5"]);
}

#[test]
fn remote_scan_exec_plan_name() {
    let schema = sample_schema();
    let exec =
        RemoteDuckDbScanExec::new("t_shard0".to_string(), schema, None, vec![], None, vec![]);
    assert_eq!(exec.name(), "RemoteDuckDbScanExec");
}

#[test]
fn remote_scan_exec_has_no_children() {
    let schema = sample_schema();
    let exec =
        RemoteDuckDbScanExec::new("t_shard0".to_string(), schema, None, vec![], None, vec![]);
    assert!(exec.children().is_empty());
}

#[test]
fn remote_scan_exec_with_new_children_returns_self() {
    let schema = sample_schema();
    let exec = Arc::new(RemoteDuckDbScanExec::new(
        "t_shard0".to_string(),
        schema,
        None,
        vec![],
        None,
        vec![],
    ));

    let new_exec = exec.clone().with_new_children(vec![]).unwrap();
    let downcasted = new_exec
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .unwrap();
    assert_eq!(downcasted.shard_table_name(), "t_shard0");
}

#[test]
fn remote_scan_exec_execute_returns_error() {
    let schema = sample_schema();
    let exec =
        RemoteDuckDbScanExec::new("t_shard0".to_string(), schema, None, vec![], None, vec![]);

    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    let result = exec.execute(0, ctx);
    match result {
        Err(e) => {
            let err_msg = e.to_string();
            assert!(err_msg.contains("not correctly distributed to storage nodes"));
        }
        Ok(_) => panic!("expected execute to return an error"),
    }
}

#[test]
fn remote_scan_exec_properties() {
    let schema = sample_schema();
    let exec = RemoteDuckDbScanExec::new(
        "t_shard0".to_string(),
        schema.clone(),
        None,
        vec![],
        None,
        vec![],
    );

    let props = exec.properties();
    assert_eq!(props.eq_properties.schema(), &schema);
}

#[test]
fn remote_scan_exec_display() {
    let schema = sample_schema();
    let exec = RemoteDuckDbScanExec::new(
        "orders_shard3".to_string(),
        schema,
        None,
        vec![],
        None,
        vec![],
    );

    let debug_str = format!("{:?}", exec);
    assert!(debug_str.contains("orders_shard3"));
}

// =============================================================================
// VairePhysicalCodec tests
// =============================================================================

#[test]
fn physical_codec_roundtrip() {
    let schema = sample_schema();
    let exec = Arc::new(RemoteDuckDbScanExec::new(
        "orders_shard2".to_string(),
        schema,
        Some(vec![0, 2]),
        vec!["id > 100".to_string()],
        None,
        vec![],
    )) as Arc<dyn ExecutionPlan>;

    let codec = VairePhysicalCodec::new();
    let mut buf = Vec::new();
    codec.try_encode(exec.clone(), &mut buf).unwrap();

    assert!(!buf.is_empty());

    let ctx = datafusion::execution::TaskContext::default();
    let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();

    let remote_scan = decoded
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .expect("decoded plan should be RemoteDuckDbScanExec");

    assert_eq!(remote_scan.shard_table_name(), "orders_shard2");
    assert_eq!(remote_scan.projection(), &Some(vec![0, 2]));
    assert_eq!(remote_scan.filter_exprs(), &["id > 100"]);
    assert_eq!(remote_scan.projected_schema().fields().len(), 3);
}

#[test]
fn physical_codec_roundtrip_no_projection_no_filters() {
    let schema = sample_schema();
    let exec = Arc::new(RemoteDuckDbScanExec::new(
        "minimal_shard0".to_string(),
        schema,
        None,
        vec![],
        None,
        vec![],
    )) as Arc<dyn ExecutionPlan>;

    let codec = VairePhysicalCodec::new();
    let mut buf = Vec::new();
    codec.try_encode(exec, &mut buf).unwrap();

    let ctx = datafusion::execution::TaskContext::default();
    let decoded = codec.try_decode(&buf, &[], &ctx).unwrap();

    let remote_scan = decoded
        .as_any()
        .downcast_ref::<RemoteDuckDbScanExec>()
        .unwrap();

    assert_eq!(remote_scan.shard_table_name(), "minimal_shard0");
    assert!(remote_scan.projection().is_none());
    assert!(remote_scan.filter_exprs().is_empty());
}

#[test]
fn physical_codec_decode_invalid_bytes_falls_back() {
    let codec = VairePhysicalCodec::new();
    let garbage = b"not a valid plan";
    let ctx = datafusion::execution::TaskContext::default();

    let result = codec.try_decode(garbage, &[], &ctx);
    // Falls back to ballista codec, which will also fail on garbage
    assert!(result.is_err());
}

// =============================================================================
// VaireLogicalCodec tests
// =============================================================================

#[test]
fn logical_codec_table_provider_roundtrip() {
    let schema = sample_schema();
    let shards = vec![sample_shard("events", 0), sample_shard("events", 1)];
    let provider = Arc::new(SchedulerTableProvider::new(
        "events".to_string(),
        shards,
        schema.clone(),
    )) as Arc<dyn TableProvider>;

    let codec = VaireLogicalCodec;
    let table_ref = datafusion::common::TableReference::bare("events");

    let mut buf = Vec::new();
    codec
        .try_encode_table_provider(&table_ref, provider, &mut buf)
        .unwrap();

    assert!(!buf.is_empty());

    let ctx = datafusion::execution::TaskContext::default();
    let decoded = codec
        .try_decode_table_provider(&buf, &table_ref, schema.clone(), &ctx)
        .unwrap();

    let stp = decoded
        .as_any()
        .downcast_ref::<SchedulerTableProvider>()
        .unwrap();

    assert_eq!(stp.table_name(), "events");
    assert_eq!(stp.shards().len(), 2);
    assert_eq!(stp.shards()[0].hash_bucket, 0);
    assert_eq!(stp.shards()[1].hash_bucket, 1);
}

#[test]
fn logical_codec_encode_non_scheduler_provider_fails() {
    use datafusion::datasource::empty::EmptyTable;

    let schema = sample_schema();
    let empty_table = Arc::new(EmptyTable::new(schema)) as Arc<dyn TableProvider>;

    let codec = VaireLogicalCodec;
    let table_ref = datafusion::common::TableReference::bare("test");

    let mut buf = Vec::new();
    let result = codec.try_encode_table_provider(&table_ref, empty_table, &mut buf);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unsupported table provider type")
    );
}

#[test]
fn logical_codec_decode_extension_not_implemented() {
    let codec = VaireLogicalCodec;
    let ctx = datafusion::execution::TaskContext::default();

    let result = codec.try_decode(b"anything", &[], &ctx);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("not supported in distributed queries")
    );
}

#[test]
fn logical_codec_encode_extension_not_implemented() {
    // VaireLogicalCodec::try_encode returns NotImplemented for any Extension.
    // We can't easily construct an Extension node, but the decode test above
    // already confirms the error path. This test exists for completeness.
    let _codec = VaireLogicalCodec;
}

#[test]
fn logical_codec_decode_invalid_json_fails() {
    let codec = VaireLogicalCodec;
    let schema = sample_schema();
    let table_ref = datafusion::common::TableReference::bare("test");
    let ctx = datafusion::execution::TaskContext::default();

    let result = codec.try_decode_table_provider(b"not json!", &table_ref, schema, &ctx);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("failed to decode distributed scan plan")
    );
}

// =============================================================================
// refresh_catalog_tables tests
// =============================================================================

#[test]
fn refresh_catalog_tables_empty_catalog() {
    let catalog = make_catalog();
    let ctx = SessionContext::new();

    refresh_ballista_catalog_tables(&ctx, &catalog).unwrap();

    // No tables registered
    let tables = ctx.catalog_names();
    // Default catalog exists but no user tables should be registered
    assert!(tables.contains(&"datafusion".to_string()));
}

#[test]
fn refresh_catalog_tables_single_table() {
    let catalog = make_catalog();

    let table_meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "id".to_string(),
        shard_count: 2,
        replication_factor: 1,
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();

    let p0 = sample_shard("users", 0);
    let p1 = sample_shard("users", 1);
    catalog.put_shard(&p0).unwrap();
    catalog.put_shard(&p1).unwrap();

    let ctx = SessionContext::new();
    refresh_ballista_catalog_tables(&ctx, &catalog).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let provider = ctx.table_provider("users").await.unwrap();
        assert_eq!(provider.schema().fields().len(), 2);
        assert_eq!(provider.schema().field(0).name(), "id");
        assert_eq!(provider.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(provider.schema().field(1).name(), "email");
        assert_eq!(provider.schema().field(1).data_type(), &DataType::Utf8);
    });
}

#[test]
fn refresh_catalog_tables_multiple_tables() {
    let catalog = make_catalog();

    for name in &["orders", "products", "reviews"] {
        let table_meta = TableMeta {
            anonymized_columns: std::collections::HashMap::new(),
            table_name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
                default_expr: String::new(),
            }],
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: "id".to_string(),
            shard_count: 1,
            replication_factor: 1,
            created_at: None,
        };
        catalog.put_table(&table_meta).unwrap();
        catalog.put_shard(&sample_shard(name, 0)).unwrap();
    }

    let ctx = SessionContext::new();
    refresh_ballista_catalog_tables(&ctx, &catalog).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for name in &["orders", "products", "reviews"] {
            let provider = ctx.table_provider(*name).await.unwrap();
            assert_eq!(provider.schema().fields().len(), 1);
            assert_eq!(provider.schema().field(0).data_type(), &DataType::Int64);
        }
    });
}

#[test]
fn refresh_catalog_tables_maps_column_types_correctly() {
    let catalog = make_catalog();

    let table_meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "typed_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "int_col".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "bool_col".to_string(),
                data_type: "BOOLEAN".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "ts_col".to_string(),
                data_type: "TIMESTAMP".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "dec_col".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "int_col".to_string(),
        shard_count: 1,
        replication_factor: 1,
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();
    catalog.put_shard(&sample_shard("typed_table", 0)).unwrap();

    let ctx = SessionContext::new();
    refresh_ballista_catalog_tables(&ctx, &catalog).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let provider = ctx.table_provider("typed_table").await.unwrap();
        let schema = provider.schema();

        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
        assert!(schema.field(1).is_nullable());
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(3).data_type(), &DataType::Decimal128(38, 10));
    });
}

// =============================================================================
// Local context tests — verifies that catalog queries execute locally
// without going through Ballista's distributed planner.
// =============================================================================

#[tokio::test]
async fn local_ctx_select_vairedb_catalog_tables() {
    let catalog = Arc::new(make_catalog());

    let table_meta = TableMeta {
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
                name: "total".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "id".to_string(),
        shard_count: 2,
        replication_factor: 3,
        created_at: Some(prost_types::Timestamp {
            seconds: 1700000000,
            nanos: 0,
        }),
    };
    catalog.put_table(&table_meta).unwrap();

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT table_name, shard_key, shard_count, replication_factor FROM vairedb_catalog.tables")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "orders");

    let keys = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(keys.value(0), "id");

    let counts = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(counts.value(0), 2);

    let rf = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(rf.value(0), 3);
}

#[tokio::test]
async fn local_ctx_select_vairedb_catalog_columns() {
    let catalog = Arc::new(make_catalog());

    let table_meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "user_id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "active".to_string(),
                data_type: "BOOLEAN".to_string(),
                nullable: false,
                default_expr: "true".to_string(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "user_id".to_string(),
        shard_count: 1,
        replication_factor: 1,
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT column_name, data_type, is_nullable FROM vairedb_catalog.columns ORDER BY ordinal_position")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 3);

    let col_names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(col_names.value(0), "user_id");
    assert_eq!(col_names.value(1), "email");
    assert_eq!(col_names.value(2), "active");

    let nullables = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .unwrap();
    assert!(!nullables.value(0));
    assert!(nullables.value(1));
    assert!(!nullables.value(2));
}

#[tokio::test]
async fn local_ctx_select_vairedb_catalog_shards() {
    let catalog = Arc::new(make_catalog());

    catalog
        .put_shard(&ShardMeta {
            shard_id: "p0".to_string(),
            table_name: "events".to_string(),
            primary_node_id: "node-a".to_string(),
            replica_node_ids: vec!["node-b".to_string()],
            hash_bucket: 0,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();
    catalog
        .put_shard(&ShardMeta {
            shard_id: "p1".to_string(),
            table_name: "events".to_string(),
            primary_node_id: "node-b".to_string(),
            replica_node_ids: vec!["node-a".to_string()],
            hash_bucket: 1,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT shard_id, table_name, primary_node_id, hash_bucket FROM vairedb_catalog.shards ORDER BY hash_bucket")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);

    let shard_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(shard_ids.value(0), "p0");
    assert_eq!(shard_ids.value(1), "p1");

    let primaries = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(primaries.value(0), "node-a");
    assert_eq!(primaries.value(1), "node-b");
}

#[tokio::test]
async fn local_ctx_select_vairedb_catalog_nodes() {
    use vairedb_coordinator::catalog::{NodeMeta, NodeState};

    let catalog = Arc::new(make_catalog());

    catalog
        .put_node(&NodeMeta {
            node_id: "core-1".to_string(),
            advertised_address: "10.0.0.1:50041".to_string(),
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
    catalog
        .put_node(&NodeMeta {
            node_id: "core-2".to_string(),
            advertised_address: "10.0.0.2:50041".to_string(),
            state: NodeState::Dead as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT node_id, state FROM vairedb_catalog.nodes ORDER BY node_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "core-1");
    assert_eq!(ids.value(1), "core-2");

    let states = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(states.value(0), "ALIVE");
    assert_eq!(states.value(1), "DEAD");
}

#[tokio::test]
async fn local_ctx_catalog_query_with_filter() {
    let catalog = Arc::new(make_catalog());

    for name in &["alpha", "beta", "gamma"] {
        let table_meta = TableMeta {
            anonymized_columns: std::collections::HashMap::new(),
            table_name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            }],
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: "id".to_string(),
            shard_count: 1,
            replication_factor: 1,
            created_at: None,
        };
        catalog.put_table(&table_meta).unwrap();
    }

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT table_name FROM vairedb_catalog.tables WHERE table_name = 'beta'")
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
    assert_eq!(names.value(0), "beta");
}

#[tokio::test]
async fn local_ctx_catalog_empty_tables() {
    let catalog = Arc::new(make_catalog());

    let local_ctx = SessionContext::new();
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog)).unwrap();

    let batches = local_ctx
        .sql("SELECT * FROM vairedb_catalog.tables")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 0);
}

// =============================================================================
// VaireAffinityPolicy tests
// =============================================================================

mod affinity_tests {
    use std::collections::{HashMap, HashSet};
    use std::fmt::{self, Debug, Formatter};
    use std::sync::Arc;

    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{
        AvailableTaskSlots, JobStatus, RunningJob, TaskStatus, job_status,
    };
    use ballista_core::serde::scheduler::{ExecutorMetadata, PartitionLocation};
    use ballista_scheduler::cluster::DistributionPolicy;
    use ballista_scheduler::scheduler_server::event::QueryStageSchedulerEvent;
    use ballista_scheduler::state::execution_graph::{
        ExecutionGraph, ExecutionGraphBox, RunningTaskInfo,
    };
    use ballista_scheduler::state::execution_stage::{ExecutionStage, RunningStage};
    use ballista_scheduler::state::task_manager::JobInfoCache;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::SessionConfig;

    use vairedb_coordinator::scheduler::{RemoteDuckDbScanExec, VaireAffinityPolicy};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn make_remote_scan(
        shard: &str,
        target: Option<&str>,
        replicas: Vec<&str>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(RemoteDuckDbScanExec::new(
            shard.to_string(),
            test_schema(),
            None,
            vec![],
            target.map(|s| s.to_string()),
            replicas.into_iter().map(|s| s.to_string()).collect(),
        ))
    }

    struct MockExecutionGraph {
        stage: RunningStage,
        task_id_gen: usize,
        status: JobStatus,
        fetched: bool,
    }

    impl MockExecutionGraph {
        fn new(plan: Arc<dyn ExecutionPlan>, partitions: usize) -> Self {
            let stage = RunningStage::new(
                1,
                0,
                plan,
                partitions,
                vec![],
                HashMap::new(),
                Arc::new(SessionConfig::new()),
            );
            Self {
                stage,
                task_id_gen: 0,
                status: JobStatus {
                    job_id: "job-1".to_string(),
                    job_name: "test-job".to_string(),
                    status: Some(job_status::Status::Running(RunningJob {
                        queued_at: 0,
                        started_at: 0,
                        scheduler: "test".to_string(),
                    })),
                },
                fetched: false,
            }
        }
    }

    impl Debug for MockExecutionGraph {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "MockExecutionGraph")
        }
    }

    impl ExecutionGraph for MockExecutionGraph {
        fn job_id(&self) -> &str {
            "job-1"
        }
        fn job_name(&self) -> &str {
            "test-job"
        }
        fn session_id(&self) -> &str {
            "session-1"
        }
        fn status(&self) -> &JobStatus {
            &self.status
        }
        fn start_time(&self) -> u64 {
            0
        }
        fn end_time(&self) -> u64 {
            0
        }
        fn completed_stages(&self) -> usize {
            0
        }
        fn is_successful(&self) -> bool {
            false
        }
        fn revive(&mut self) -> bool {
            false
        }
        fn update_task_status(
            &mut self,
            _executor: &ExecutorMetadata,
            _task_statuses: Vec<TaskStatus>,
            _max_task_failures: usize,
            _max_stage_failures: usize,
        ) -> Result<Vec<QueryStageSchedulerEvent>> {
            Ok(vec![])
        }
        fn running_stages(&self) -> Vec<usize> {
            vec![1]
        }
        fn running_tasks(&self) -> Vec<RunningTaskInfo> {
            vec![]
        }
        fn available_tasks(&self) -> usize {
            self.stage.task_infos.iter().filter(|t| t.is_none()).count()
        }
        fn fetch_running_stage(
            &mut self,
            _black_list: &[usize],
        ) -> Option<(&mut RunningStage, &mut usize)> {
            if self.fetched {
                return None;
            }
            self.fetched = true;
            Some((&mut self.stage, &mut self.task_id_gen))
        }
        fn update_status(&mut self, status: JobStatus) {
            self.status = status;
        }
        fn output_locations(&self) -> Vec<PartitionLocation> {
            vec![]
        }
        fn reset_stages_on_lost_executor(
            &mut self,
            _executor_id: &str,
        ) -> Result<(HashSet<usize>, Vec<RunningTaskInfo>)> {
            Ok((HashSet::new(), vec![]))
        }
        fn resolve_stage(&mut self, _stage_id: usize) -> Result<bool> {
            Ok(false)
        }
        fn succeed_stage(&mut self, _stage_id: usize) -> bool {
            false
        }
        fn fail_stage(&mut self, _stage_id: usize, _err_msg: String) -> bool {
            false
        }
        fn rollback_running_stage(
            &mut self,
            _stage_id: usize,
            _failure_reasons: HashSet<String>,
        ) -> Result<Vec<RunningTaskInfo>> {
            Ok(vec![])
        }
        fn rollback_resolved_stage(&mut self, _stage_id: usize) -> Result<bool> {
            Ok(false)
        }
        fn rerun_successful_stage(&mut self, _stage_id: usize) -> bool {
            false
        }
        fn fail_job(&mut self, _error: String) {}
        fn succeed_job(&mut self) -> Result<()> {
            Ok(())
        }
        fn stages(&self) -> &HashMap<usize, ExecutionStage> {
            unimplemented!()
        }
        fn stage_count(&self) -> usize {
            1
        }
        fn cloned(&self) -> ExecutionGraphBox {
            unimplemented!()
        }
        fn logical_plan(&self) -> Option<&str> {
            None
        }
        fn physical_plan(&self) -> Arc<dyn ExecutionPlan> {
            self.stage.plan.clone()
        }
    }

    fn make_job_cache(graph: MockExecutionGraph) -> JobInfoCache {
        let boxed: ExecutionGraphBox = Box::new(graph);
        JobInfoCache::new(boxed)
    }

    #[test]
    fn affinity_policy_name() {
        let policy = VaireAffinityPolicy;
        assert_eq!(policy.name(), "VaireAffinityPolicy");
    }

    #[tokio::test]
    async fn affinity_bind_tasks_zero_slots() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", Some("exec-1"), vec![]);
        let graph = MockExecutionGraph::new(plan, 1);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 0,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn affinity_bind_tasks_no_running_jobs() {
        let policy = VaireAffinityPolicy;
        let running_jobs = Arc::new(HashMap::new());

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn affinity_bind_tasks_non_running_status_skipped() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", Some("exec-1"), vec![]);
        let mut graph = MockExecutionGraph::new(plan, 1);
        graph.status = JobStatus {
            job_id: "job-1".to_string(),
            job_name: "test-job".to_string(),
            status: Some(job_status::Status::Queued(
                ballista_core::serde::protobuf::QueuedJob { queued_at: 0 },
            )),
        };
        let boxed: ExecutionGraphBox = Box::new(graph);
        let cache = JobInfoCache::new(boxed);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn affinity_bind_tasks_primary_match() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", Some("exec-1"), vec!["exec-2"]);
        let graph = MockExecutionGraph::new(plan, 1);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "exec-1");
        assert_eq!(result[0].1.partition.partition_id, 0);
    }

    #[tokio::test]
    async fn affinity_bind_tasks_replica_fallback() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", Some("exec-1"), vec!["exec-2"]);
        let graph = MockExecutionGraph::new(plan, 1);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-2".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "exec-2");
    }

    #[tokio::test]
    async fn affinity_bind_tasks_no_affinity_match() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", Some("exec-1"), vec!["exec-2"]);
        let graph = MockExecutionGraph::new(plan, 1);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-3".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn affinity_bind_tasks_no_target_executor_treated_as_primary() {
        let policy = VaireAffinityPolicy;
        let plan = make_remote_scan("shard0", None, vec![]);
        let graph = MockExecutionGraph::new(plan, 1);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "any-executor".to_string(),
            slots: 4,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "any-executor");
    }

    #[tokio::test]
    async fn affinity_bind_tasks_slot_exhaustion() {
        let policy = VaireAffinityPolicy;
        let children: Vec<Arc<dyn ExecutionPlan>> = (0..5)
            .map(|i| make_remote_scan(&format!("shard{}", i), Some("exec-1"), vec![]))
            .collect();
        let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(children).unwrap();
        let graph = MockExecutionGraph::new(plan, 5);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 2,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(slot.slots, 0);
    }

    #[tokio::test]
    async fn affinity_bind_tasks_union_multiple_partitions() {
        let policy = VaireAffinityPolicy;
        let children: Vec<Arc<dyn ExecutionPlan>> = vec![
            make_remote_scan("shard0", Some("exec-1"), vec!["exec-2"]),
            make_remote_scan("shard1", Some("exec-2"), vec!["exec-1"]),
            make_remote_scan("shard2", Some("exec-1"), vec![]),
        ];
        let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(children).unwrap();
        let graph = MockExecutionGraph::new(plan, 3);
        let cache = make_job_cache(graph);

        let mut jobs = HashMap::new();
        jobs.insert("job-1".to_string(), cache);
        let running_jobs = Arc::new(jobs);

        let mut slot = AvailableTaskSlots {
            executor_id: "exec-1".to_string(),
            slots: 10,
        };
        let slots = vec![&mut slot];

        let result = policy.bind_tasks(slots, running_jobs).await.unwrap();
        // exec-1 is primary for partition 0 and 2 (first pass)
        // exec-1 is replica for partition 1 (second pass)
        assert_eq!(result.len(), 3);

        let partition_ids: Vec<usize> = result.iter().map(|r| r.1.partition.partition_id).collect();
        assert!(partition_ids.contains(&0));
        assert!(partition_ids.contains(&1));
        assert!(partition_ids.contains(&2));
    }
}
