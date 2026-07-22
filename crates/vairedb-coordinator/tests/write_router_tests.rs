use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use datafusion::scalar::ScalarValue;
use vairedb_coordinator::catalog::{
    ColumnDef, MetadataCatalog, NodeMeta, NodeState, ShardMeta, ShardStrategy, TableMeta,
};
use vairedb_coordinator::error::CoordinatorError;
use vairedb_coordinator::sql_compat;
use vairedb_coordinator::write_router::{WriteRouter, compute_shard_index};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_write_router_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn setup_catalog_with_table() -> (Arc<MetadataCatalog>, TableMeta) {
    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());

    let node = NodeMeta {
        node_id: "node-0".to_string(),
        advertised_address: "10.0.0.1:50041".to_string(),
        state: NodeState::Alive as i32,
        last_heartbeat: Some(prost_types::Timestamp {
            seconds: 1000,
            nanos: 0,
        }),
        registered_at: None,
    };
    catalog.put_node(&node).unwrap();

    let table_meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "orders".to_string(),
        columns: vec![
            ColumnDef {
                name: "customer_id".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "amount".to_string(),
                data_type: "INT".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "customer_id".to_string(),
        shard_count: 3,
        replication_factor: 3,
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();

    for i in 0..3 {
        let shard = ShardMeta {
            shard_id: format!("orders_part{}", i),
            table_name: "orders".to_string(),
            primary_node_id: "node-0".to_string(),
            replica_node_ids: vec![],
            hash_bucket: i,
            range_lower: String::new(),
            range_upper: String::new(),
        };
        catalog.put_shard(&shard).unwrap();
    }

    (catalog, table_meta)
}

#[test]
fn test_compute_shard_index_deterministic() {
    let idx1 = compute_shard_index("42", 6);
    let idx2 = compute_shard_index("42", 6);
    assert_eq!(idx1, idx2);
    assert!(idx1 < 6);
}

#[test]
fn test_compute_shard_index_range() {
    for i in 0..100 {
        let idx = compute_shard_index(&i.to_string(), 6);
        assert!(idx < 6);
    }
}

#[test]
fn test_compute_shard_index_single_shard() {
    for i in 0..50 {
        let idx = compute_shard_index(&i.to_string(), 1);
        assert_eq!(idx, 0);
    }
}

#[test]
fn test_compute_quorum_size_rf3() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);
    assert_eq!(router.compute_quorum_size(3), 2);
}

#[test]
fn test_compute_quorum_size_rf1() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);
    assert_eq!(router.compute_quorum_size(1), 1);
}

#[test]
fn test_compute_quorum_size_rf5() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);
    assert_eq!(router.compute_quorum_size(5), 3);
}

#[test]
fn test_get_target_nodes_primary_only() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "p0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let nodes = router.get_target_nodes(&shard);
    assert_eq!(nodes, vec!["node-0".to_string()]);
}

#[test]
fn test_get_target_nodes_with_replicas() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "p0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec!["node-1".to_string(), "node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let nodes = router.get_target_nodes(&shard);
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0], "node-0");
    assert_eq!(nodes[1], "node-1");
    assert_eq!(nodes[2], "node-2");
}

#[test]
fn test_resolve_target_shards_with_key() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "INSERT INTO orders (customer_id, amount) VALUES (42, 100)";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_resolve_target_shards_without_key_returns_all() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "DELETE FROM orders";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_resolve_target_shards_empty_table_errors() {
    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());
    let table_meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        table_name: "empty_table".to_string(),
        columns: vec![],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "id".to_string(),
        shard_count: 3,
        replication_factor: 3,
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();
    let router = WriteRouter::new(catalog);

    let sql = "INSERT INTO empty_table (id) VALUES (1)";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router.resolve_target_shards(&stmts[0], &table_meta, &[]);
    assert!(result.is_err());
}

#[test]
fn test_generate_shard_local_sql() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "orders_part0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let sql = "INSERT INTO orders (customer_id) VALUES (1)";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let (result, _params) = router
        .generate_shard_local_sql(&stmts[0], &shard, &[])
        .unwrap();
    assert!(result.contains("orders_shard0"));
}

#[test]
fn test_generate_shard_local_sql_malformed_placeholder_errors() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "orders_part0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    // `$foo` parses as a placeholder whose name is not a positional index, so
    // renumber_placeholders returns None. With non-empty params this must
    // surface as an error rather than silently dropping the bind parameters.
    let sql = "INSERT INTO orders (customer_id) VALUES ($foo)";
    let stmts = sql_compat::parse_sql(sql).unwrap();
    let params = vec![ScalarValue::Int64(Some(1))];

    let result = router.generate_shard_local_sql(&stmts[0], &shard, &params);
    assert!(matches!(result, Err(CoordinatorError::Internal(_))));
}

#[test]
fn test_generate_shard_local_sql_with_bytea() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "t_part0".to_string(),
        table_name: "t".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 1,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let sql = "CREATE TABLE t (data BYTEA)";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let (result, _params) = router
        .generate_shard_local_sql(&stmts[0], &shard, &[])
        .unwrap();
    assert!(result.contains("BLOB"));
    assert!(result.contains("t_shard1"));
}

#[test]
fn test_resolve_target_shards_update_with_key() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "UPDATE orders SET amount = 200 WHERE customer_id = 42";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_resolve_target_shards_update_without_key() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "UPDATE orders SET amount = 0 WHERE amount > 100";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_resolve_target_shards_delete_with_key() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "DELETE FROM orders WHERE customer_id = 7";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_resolve_target_shards_select_returns_all() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let sql = "SELECT * FROM orders";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let result = router
        .resolve_target_shards(&stmts[0], &table_meta, &[])
        .unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_generate_shard_local_sql_update() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "orders_part2".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 2,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let sql = "UPDATE orders SET amount = 99 WHERE customer_id = 1";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let (result, _params) = router
        .generate_shard_local_sql(&stmts[0], &shard, &[])
        .unwrap();
    assert!(result.contains("orders_shard2"));
    assert!(result.contains("99"));
}

#[test]
fn test_generate_shard_local_sql_delete() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let shard = ShardMeta {
        shard_id: "orders_part1".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 1,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let sql = "DELETE FROM orders WHERE customer_id = 5";
    let stmts = sql_compat::parse_sql(sql).unwrap();

    let (result, _params) = router
        .generate_shard_local_sql(&stmts[0], &shard, &[])
        .unwrap();
    assert!(result.contains("orders_shard1"));
}

#[test]
fn test_compute_quorum_size_even_rf() {
    let (catalog, _) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);
    assert_eq!(router.compute_quorum_size(2), 2);
    assert_eq!(router.compute_quorum_size(4), 3);
}

#[test]
fn test_compute_shard_index_empty_value() {
    let idx = compute_shard_index("", 4);
    assert!(idx < 4);
}

// ---------------------------------------------------------------------------
// Multi-shard INSERT splitting. Mirrors the grouping in
// pgwire_handler::handle_insert_with_split: each VALUES row is bucketed by
// compute_shard_index on its shard-key value, then split_insert_by_rows rebuilds
// a per-shard INSERT carrying only that shard's rows.
// ---------------------------------------------------------------------------

/// Group VALUES-row indices by target shard exactly as handle_insert_with_split
/// does, so the split logic can be asserted without a live cluster.
fn group_rows_by_shard(
    stmt: &sqlparser::ast::Statement,
    shard_key: &str,
    shard_count: usize,
) -> std::collections::HashMap<usize, Vec<usize>> {
    let keys = sql_compat::extract_insert_row_shard_keys(stmt, shard_key, &[])
        .expect("multi-row INSERT should expose per-row shard keys");
    let mut shard_rows: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();
    for (row_idx, key_value) in &keys {
        let shard_idx = compute_shard_index(key_value, shard_count);
        shard_rows.entry(shard_idx).or_default().push(*row_idx);
    }
    shard_rows
}

#[test]
fn test_multi_row_insert_splits_across_shards() {
    // Pick three ids that hash to three distinct buckets so the INSERT must fan
    // out to every shard.
    let mut by_bucket: std::collections::HashMap<usize, i64> = std::collections::HashMap::new();
    let mut id = 1i64;
    while by_bucket.len() < 3 {
        by_bucket
            .entry(compute_shard_index(&id.to_string(), 3))
            .or_insert(id);
        id += 1;
    }
    let ids: Vec<i64> = by_bucket.values().copied().collect();
    let values = ids
        .iter()
        .map(|i| format!("({i}, 100)"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO orders (customer_id, amount) VALUES {values}");
    let stmts = sql_compat::parse_sql(&sql).unwrap();

    let shard_rows = group_rows_by_shard(&stmts[0], "customer_id", 3);
    assert_eq!(
        shard_rows.len(),
        3,
        "three ids hashing to distinct buckets must split into three shard groups"
    );
    // Every original row index is assigned to exactly one shard group.
    let mut assigned: Vec<usize> = shard_rows.values().flatten().copied().collect();
    assigned.sort_unstable();
    assert_eq!(assigned, vec![0, 1, 2]);
}

#[test]
fn test_multi_row_insert_same_shard_one_group() {
    // Three ids that all hash to the same bucket must stay in a single group.
    let bucket = compute_shard_index("1", 3);
    let mut ids = Vec::new();
    let mut id = 1i64;
    while ids.len() < 3 {
        if compute_shard_index(&id.to_string(), 3) == bucket {
            ids.push(id);
        }
        id += 1;
    }
    let values = ids
        .iter()
        .map(|i| format!("({i}, 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO orders (customer_id, amount) VALUES {values}");
    let stmts = sql_compat::parse_sql(&sql).unwrap();

    let shard_rows = group_rows_by_shard(&stmts[0], "customer_id", 3);
    assert_eq!(shard_rows.len(), 1, "co-located rows must form one group");
    assert_eq!(shard_rows[&bucket].len(), 3);
}

#[test]
fn test_resolve_consistent_shard_routing() {
    let (catalog, table_meta) = setup_catalog_with_table();
    let router = WriteRouter::new(catalog);

    let insert_sql = "INSERT INTO orders (customer_id, amount) VALUES (42, 100)";
    let update_sql = "UPDATE orders SET amount = 200 WHERE customer_id = 42";
    let delete_sql = "DELETE FROM orders WHERE customer_id = 42";

    let insert_stmts = sql_compat::parse_sql(insert_sql).unwrap();
    let update_stmts = sql_compat::parse_sql(update_sql).unwrap();
    let delete_stmts = sql_compat::parse_sql(delete_sql).unwrap();

    let insert_shards = router
        .resolve_target_shards(&insert_stmts[0], &table_meta, &[])
        .unwrap();
    let update_shards = router
        .resolve_target_shards(&update_stmts[0], &table_meta, &[])
        .unwrap();
    let delete_shards = router
        .resolve_target_shards(&delete_stmts[0], &table_meta, &[])
        .unwrap();

    assert_eq!(insert_shards[0].shard_id, update_shards[0].shard_id);
    assert_eq!(insert_shards[0].shard_id, delete_shards[0].shard_id);
}
