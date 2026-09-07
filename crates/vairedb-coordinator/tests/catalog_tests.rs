use std::sync::atomic::{AtomicU32, Ordering};

use vairedb_coordinator::catalog::{
    AnonymizationSecret, ColumnDef, MetadataCatalog, NodeMeta, NodeState, SchemaMeta, ShardMeta,
    ShardStrategy, TableMeta,
};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_catalog_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn make_catalog() -> MetadataCatalog {
    let path = temp_db_path();
    MetadataCatalog::open(&path).unwrap()
}

fn sample_table_meta() -> TableMeta {
    TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        table_name: "orders".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "customer_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            },
            ColumnDef {
                name: "amount".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: true,
                default_expr: String::new(),
            },
        ],
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "customer_id".to_string(),
        shard_count: 6,
        replication_factor: 3,
        created_at: None,
    }
}

fn sample_node(id: &str, addr: &str) -> NodeMeta {
    NodeMeta {
        node_id: id.to_string(),
        advertised_address: addr.to_string(),
        state: NodeState::Alive as i32,
        last_heartbeat: Some(prost_types::Timestamp {
            seconds: 1000,
            nanos: 0,
        }),
        registered_at: None,
    }
}

#[test]
fn test_put_and_get_table() {
    let catalog = make_catalog();
    let meta = sample_table_meta();

    catalog.put_table(&meta).unwrap();
    let result = catalog.get_table("orders").unwrap().unwrap();

    assert_eq!(result.table_name, "orders");
    assert_eq!(result.shard_count, 6);
    assert_eq!(result.replication_factor, 3);
    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].data_type, "INTEGER");
    assert!(result.columns[2].nullable);
    assert_eq!(result.shard_strategy, ShardStrategy::Hash as i32);
    assert_eq!(result.shard_key, "customer_id");
}

#[test]
fn test_get_table_not_found() {
    let catalog = make_catalog();
    let result = catalog.get_table("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_delete_table() {
    let catalog = make_catalog();
    let meta = sample_table_meta();

    catalog.put_table(&meta).unwrap();
    catalog.delete_table("orders").unwrap();

    let result = catalog.get_table("orders").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_create_table_if_absent_claims_a_free_name() {
    let catalog = make_catalog();
    let meta = sample_table_meta();

    assert!(catalog.create_table_if_absent(&meta).unwrap());
    assert_eq!(
        catalog.get_table("orders").unwrap().unwrap().shard_key,
        "customer_id"
    );
}

#[test]
fn test_create_table_if_absent_refuses_a_taken_name_without_overwriting() {
    let catalog = make_catalog();
    let meta = sample_table_meta();
    catalog.create_table_if_absent(&meta).unwrap();

    // A second claim on the same name, with a different layout: it must be
    // refused *and* leave the first table's metadata untouched. An overwrite here
    // would strand every row already written under the original shard key.
    let mut other = sample_table_meta();
    other.shard_key = "id".to_string();
    other.shard_count = 2;
    assert!(!catalog.create_table_if_absent(&other).unwrap());

    let stored = catalog.get_table("orders").unwrap().unwrap();
    assert_eq!(stored.shard_key, "customer_id");
    assert_eq!(stored.shard_count, 6);
}

#[test]
fn test_create_table_if_absent_claims_again_after_delete() {
    let catalog = make_catalog();
    let meta = sample_table_meta();

    assert!(catalog.create_table_if_absent(&meta).unwrap());
    catalog.delete_table("orders").unwrap();
    assert!(
        catalog.create_table_if_absent(&meta).unwrap(),
        "a dropped name is free again"
    );
}

#[test]
fn test_concurrent_create_table_if_absent_yields_exactly_one_winner() {
    use std::sync::Arc;

    let catalog = Arc::new(make_catalog());
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let catalog = Arc::clone(&catalog);
            std::thread::spawn(move || {
                let mut meta = sample_table_meta();
                // Distinguishable layouts, so a lost write would be visible.
                meta.shard_count = i + 1;
                catalog.create_table_if_absent(&meta).unwrap()
            })
        })
        .collect();

    let winners = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .filter(|claimed| *claimed)
        .count();
    assert_eq!(winners, 1, "exactly one claim on the same name may succeed");
    assert!(catalog.get_table("orders").unwrap().is_some());
}

#[test]
fn test_list_tables() {
    let catalog = make_catalog();
    let mut meta1 = sample_table_meta();
    meta1.table_name = "table_a".to_string();
    let mut meta2 = sample_table_meta();
    meta2.table_name = "table_b".to_string();

    catalog.put_table(&meta1).unwrap();
    catalog.put_table(&meta2).unwrap();

    let tables = catalog.list_tables().unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
    assert!(names.contains(&"table_a"));
    assert!(names.contains(&"table_b"));
}

#[test]
fn test_put_and_get_shard() {
    let catalog = make_catalog();
    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string(), "node-3".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    catalog.put_shard(&shard).unwrap();
    let results = catalog.get_shards_for_table("orders").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].shard_id, "shard0");
    assert_eq!(results[0].primary_node_id, "node-1");
    assert_eq!(results[0].replica_node_ids, vec!["node-2", "node-3"]);
    assert_eq!(results[0].hash_bucket, 0);
}

#[test]
fn test_get_shards_empty() {
    let catalog = make_catalog();
    let results = catalog.get_shards_for_table("nonexistent").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_delete_shards_for_table() {
    let catalog = make_catalog();

    for i in 0..3 {
        catalog
            .put_shard(&ShardMeta {
                shard_id: format!("shard{}", i),
                table_name: "orders".to_string(),
                primary_node_id: "node-1".to_string(),
                replica_node_ids: vec![],
                hash_bucket: i,
                range_lower: String::new(),
                range_upper: String::new(),
            })
            .unwrap();
    }

    catalog.delete_shards_for_table("orders").unwrap();
    let results = catalog.get_shards_for_table("orders").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_put_and_get_node() {
    let catalog = make_catalog();
    let node = sample_node("node-1", "10.0.0.1:50041");

    catalog.put_node(&node).unwrap();
    let result = catalog.get_node("node-1").unwrap().unwrap();

    assert_eq!(result.node_id, "node-1");
    assert_eq!(result.advertised_address, "10.0.0.1:50041");
    assert_eq!(result.state, NodeState::Alive as i32);
    assert_eq!(result.last_heartbeat.unwrap().seconds, 1000);
}

#[test]
fn test_get_node_not_found() {
    let catalog = make_catalog();
    let result = catalog.get_node("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_list_alive_nodes() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "n3".to_string(),
            advertised_address: "a:3".to_string(),
            state: NodeState::Dead as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let alive = catalog.list_alive_nodes().unwrap();
    assert_eq!(alive.len(), 2);
}

#[test]
fn test_list_all_nodes() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "n2".to_string(),
            advertised_address: "a:2".to_string(),
            state: NodeState::Dead as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let all = catalog.list_all_nodes().unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn test_update_node_state() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();

    catalog.update_node_state("n1", NodeState::Suspect).unwrap();
    let node = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Suspect as i32);

    catalog.update_node_state("n1", NodeState::Dead).unwrap();
    let node = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Dead as i32);
}

#[test]
fn test_update_node_state_not_found() {
    let catalog = make_catalog();
    let result = catalog.update_node_state("nonexistent", NodeState::Dead);
    assert!(result.is_err());
}

#[test]
fn test_update_node_heartbeat() {
    let catalog = make_catalog();
    catalog
        .put_node(&NodeMeta {
            node_id: "n1".to_string(),
            advertised_address: "a:1".to_string(),
            state: NodeState::Suspect as i32,
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            registered_at: None,
        })
        .unwrap();

    catalog.update_node_heartbeat("n1").unwrap();
    let node = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Alive as i32);
    assert!(node.last_heartbeat.unwrap().seconds > 0);
}

#[test]
fn test_assign_shards_round_robin() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();
    catalog.put_node(&sample_node("n3", "a:3")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 6, 3).unwrap();

    assert_eq!(shards.len(), 6);
    for (i, p) in shards.iter().enumerate() {
        assert_eq!(p.shard_id, format!("shard{}", i));
        assert_eq!(p.table_name, "orders");
        assert_eq!(p.hash_bucket, i as u32);
        assert!(!p.primary_node_id.is_empty());
        assert!(!p.replica_node_ids.contains(&p.primary_node_id));
    }
}

#[test]
fn test_assign_shards_no_nodes() {
    let catalog = make_catalog();
    let result = catalog.assign_shards_round_robin("orders", 6, 3);
    assert!(result.is_err());
}

#[test]
fn test_get_node_address_map() {
    let catalog = make_catalog();
    catalog
        .put_node(&sample_node("n1", "10.0.0.1:50041"))
        .unwrap();
    catalog
        .put_node(&sample_node("n2", "10.0.0.2:50041"))
        .unwrap();

    let map = catalog.get_node_address_map().unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("n1").unwrap(), "10.0.0.1:50041");
    assert_eq!(map.get("n2").unwrap(), "10.0.0.2:50041");
}

#[test]
fn test_table_meta_with_range_strategy() {
    let catalog = make_catalog();
    let meta = TableMeta {
        anonymized_columns: std::collections::HashMap::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        table_name: "events".to_string(),
        columns: vec![ColumnDef {
            name: "ts".to_string(),
            data_type: "TIMESTAMP".to_string(),
            nullable: false,
            default_expr: String::new(),
        }],
        shard_strategy: ShardStrategy::Range as i32,
        shard_key: "ts".to_string(),
        shard_count: 4,
        replication_factor: 2,
        created_at: None,
    };

    catalog.put_table(&meta).unwrap();
    let result = catalog.get_table("events").unwrap().unwrap();
    assert_eq!(result.shard_strategy, ShardStrategy::Range as i32);
}

#[test]
fn test_serialization_roundtrip_table_meta() {
    use prost::Message;
    let meta = sample_table_meta();
    let bytes = meta.encode_to_vec();
    let deserialized = TableMeta::decode(bytes.as_slice()).unwrap();

    assert_eq!(deserialized.table_name, meta.table_name);
    assert_eq!(deserialized.shard_count, meta.shard_count);
    assert_eq!(deserialized.replication_factor, meta.replication_factor);
    assert_eq!(deserialized.columns.len(), meta.columns.len());
    assert_eq!(deserialized.shard_strategy, meta.shard_strategy);
}

#[test]
fn test_serialization_roundtrip_node_meta() {
    use prost::Message;
    let meta = NodeMeta {
        node_id: "test-node".to_string(),
        advertised_address: "192.168.1.1:9999".to_string(),
        state: NodeState::Suspect as i32,
        last_heartbeat: Some(prost_types::Timestamp {
            seconds: 123456789,
            nanos: 0,
        }),
        registered_at: None,
    };
    let bytes = meta.encode_to_vec();
    let deserialized = NodeMeta::decode(bytes.as_slice()).unwrap();

    assert_eq!(deserialized.node_id, meta.node_id);
    assert_eq!(deserialized.advertised_address, meta.advertised_address);
    assert_eq!(deserialized.state, NodeState::Suspect as i32);
    assert_eq!(deserialized.last_heartbeat.unwrap().seconds, 123456789);
}

#[test]
fn test_serialization_roundtrip_shard_meta() {
    use prost::Message;
    let meta = ShardMeta {
        shard_id: "shard5".to_string(),
        table_name: "users".to_string(),
        primary_node_id: "primary-node".to_string(),
        replica_node_ids: vec!["replica-1".to_string(), "replica-2".to_string()],
        hash_bucket: 5,
        range_lower: String::new(),
        range_upper: String::new(),
    };
    let bytes = meta.encode_to_vec();
    let deserialized = ShardMeta::decode(bytes.as_slice()).unwrap();

    assert_eq!(deserialized.shard_id, "shard5");
    assert_eq!(deserialized.table_name, "users");
    assert_eq!(deserialized.hash_bucket, 5);
    assert_eq!(deserialized.primary_node_id, "primary-node");
    assert_eq!(
        deserialized.replica_node_ids,
        vec!["replica-1", "replica-2"]
    );
}

#[test]
fn test_update_node_heartbeat_not_found() {
    let catalog = make_catalog();
    let result = catalog.update_node_heartbeat("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_get_shards_isolation_between_tables() {
    let catalog = make_catalog();

    catalog
        .put_shard(&ShardMeta {
            shard_id: "shard0".to_string(),
            table_name: "orders".to_string(),
            primary_node_id: "n1".to_string(),
            replica_node_ids: vec![],
            hash_bucket: 0,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();
    catalog
        .put_shard(&ShardMeta {
            shard_id: "shard0".to_string(),
            table_name: "orders_archive".to_string(),
            primary_node_id: "n2".to_string(),
            replica_node_ids: vec![],
            hash_bucket: 0,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();

    let orders = catalog.get_shards_for_table("orders").unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].table_name, "orders");

    let archive = catalog.get_shards_for_table("orders_archive").unwrap();
    assert_eq!(archive.len(), 1);
    assert_eq!(archive[0].table_name, "orders_archive");
}

#[test]
fn test_put_table_overwrites_existing() {
    let catalog = make_catalog();
    let mut meta = sample_table_meta();
    catalog.put_table(&meta).unwrap();

    meta.shard_count = 12;
    meta.replication_factor = 5;
    catalog.put_table(&meta).unwrap();

    let result = catalog.get_table("orders").unwrap().unwrap();
    assert_eq!(result.shard_count, 12);
    assert_eq!(result.replication_factor, 5);

    let all = catalog.list_tables().unwrap();
    assert_eq!(all.len(), 1);
}

#[test]
fn test_assign_shards_single_node() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 4, 3).unwrap();

    assert_eq!(shards.len(), 4);
    for p in &shards {
        assert_eq!(p.primary_node_id, "n1");
        assert!(p.replica_node_ids.is_empty());
    }
}

#[test]
fn test_assign_shards_replication_factor_one() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();
    catalog.put_node(&sample_node("n3", "a:3")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 3, 1).unwrap();

    assert_eq!(shards.len(), 3);
    for p in &shards {
        assert!(p.replica_node_ids.is_empty());
    }
}

#[test]
fn test_list_tables_empty() {
    let catalog = make_catalog();
    let tables = catalog.list_tables().unwrap();
    assert!(tables.is_empty());
}

#[test]
fn test_list_all_nodes_empty() {
    let catalog = make_catalog();
    let nodes = catalog.list_all_nodes().unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn test_delete_table_nonexistent() {
    let catalog = make_catalog();
    let result = catalog.delete_table("nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_delete_shards_isolation() {
    let catalog = make_catalog();

    for i in 0..2 {
        catalog
            .put_shard(&ShardMeta {
                shard_id: format!("shard{}", i),
                table_name: "table_a".to_string(),
                primary_node_id: "n1".to_string(),
                replica_node_ids: vec![],
                hash_bucket: i,
                range_lower: String::new(),
                range_upper: String::new(),
            })
            .unwrap();
    }
    catalog
        .put_shard(&ShardMeta {
            shard_id: "shard0".to_string(),
            table_name: "table_b".to_string(),
            primary_node_id: "n2".to_string(),
            replica_node_ids: vec![],
            hash_bucket: 0,
            range_lower: String::new(),
            range_upper: String::new(),
        })
        .unwrap();

    catalog.delete_shards_for_table("table_a").unwrap();

    let a = catalog.get_shards_for_table("table_a").unwrap();
    assert!(a.is_empty());

    let b = catalog.get_shards_for_table("table_b").unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].table_name, "table_b");
}

#[test]
fn test_get_node_address_map_includes_dead_nodes() {
    let catalog = make_catalog();
    catalog
        .put_node(&sample_node("n1", "10.0.0.1:50041"))
        .unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "n2".to_string(),
            advertised_address: "10.0.0.2:50041".to_string(),
            state: NodeState::Dead as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let map = catalog.get_node_address_map().unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("n1").unwrap(), "10.0.0.1:50041");
    assert_eq!(map.get("n2").unwrap(), "10.0.0.2:50041");
}

#[test]
fn test_assign_shards_round_robin_distribution() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();
    catalog.put_node(&sample_node("n3", "a:3")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 6, 2).unwrap();

    let primaries: Vec<&str> = shards.iter().map(|p| p.primary_node_id.as_str()).collect();
    assert!(primaries.contains(&"n1"));
    assert!(primaries.contains(&"n2"));
    assert!(primaries.contains(&"n3"));

    for p in &shards {
        assert_eq!(p.replica_node_ids.len(), 1);
        assert_ne!(p.replica_node_ids[0], p.primary_node_id);
    }
}

#[test]
fn test_assign_shards_replication_exceeds_nodes() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 4, 5).unwrap();

    assert_eq!(shards.len(), 4);
    for p in &shards {
        // With 2 nodes and replication_factor=5, offsets 1,3 land on a different
        // node (offsets 2,4 wrap back to primary and are skipped).
        assert_eq!(p.replica_node_ids.len(), 2);
        for replica in &p.replica_node_ids {
            assert_ne!(replica, &p.primary_node_id);
        }
    }
}

#[test]
fn test_assign_shards_replication_factor_zero() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog.put_node(&sample_node("n2", "a:2")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 3, 0).unwrap();

    assert_eq!(shards.len(), 3);
    for p in &shards {
        assert!(p.replica_node_ids.is_empty());
    }
}

#[test]
fn test_put_node_overwrites_existing() {
    let catalog = make_catalog();
    catalog
        .put_node(&sample_node("n1", "10.0.0.1:50041"))
        .unwrap();

    let updated = NodeMeta {
        node_id: "n1".to_string(),
        advertised_address: "10.0.0.99:50041".to_string(),
        state: NodeState::Suspect as i32,
        last_heartbeat: Some(prost_types::Timestamp {
            seconds: 9999,
            nanos: 0,
        }),
        registered_at: None,
    };
    catalog.put_node(&updated).unwrap();

    let result = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(result.advertised_address, "10.0.0.99:50041");
    assert_eq!(result.state, NodeState::Suspect as i32);

    let all = catalog.list_all_nodes().unwrap();
    assert_eq!(all.len(), 1);
}

#[test]
fn test_list_alive_nodes_excludes_suspect() {
    let catalog = make_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "n2".to_string(),
            advertised_address: "a:2".to_string(),
            state: NodeState::Suspect as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();
    catalog
        .put_node(&NodeMeta {
            node_id: "n3".to_string(),
            advertised_address: "a:3".to_string(),
            state: NodeState::Dead as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let alive = catalog.list_alive_nodes().unwrap();
    assert_eq!(alive.len(), 1);
    assert_eq!(alive[0].node_id, "n1");
}

#[test]
fn test_get_node_address_map_empty() {
    let catalog = make_catalog();
    let map = catalog.get_node_address_map().unwrap();
    assert!(map.is_empty());
}

#[test]
fn test_delete_shards_for_nonexistent_table() {
    let catalog = make_catalog();
    let result = catalog.delete_shards_for_table("nonexistent");
    assert!(result.is_ok());
}

#[test]
fn test_put_and_get_anonymization_secret() {
    let catalog = make_catalog();
    let secret = AnonymizationSecret {
        id: "my_sid".to_string(),
        algo: "HMAC-SHA256".to_string(),
        secret_key: "super_secret".to_string(),
    };
    catalog.put_anonymization_secret(&secret).unwrap();

    let fetched = catalog.get_anonymization_secret("my_sid").unwrap().unwrap();
    assert_eq!(fetched.id, "my_sid");
    assert_eq!(fetched.algo, "HMAC-SHA256");
    assert_eq!(fetched.secret_key, "super_secret");
}

#[test]
fn test_get_missing_anonymization_secret_is_none() {
    let catalog = make_catalog();
    assert!(
        catalog
            .get_anonymization_secret("absent")
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_list_anonymization_secrets() {
    let catalog = make_catalog();
    catalog
        .put_anonymization_secret(&AnonymizationSecret {
            id: "a".to_string(),
            algo: "HMAC-SHA256".to_string(),
            secret_key: "k1".to_string(),
        })
        .unwrap();
    catalog
        .put_anonymization_secret(&AnonymizationSecret {
            id: "b".to_string(),
            algo: "HMAC-SHA256".to_string(),
            secret_key: "k2".to_string(),
        })
        .unwrap();

    let all = catalog.list_anonymization_secrets().unwrap();
    assert_eq!(all.len(), 2);
}

// ---------------------------------------------------------------------------
// Shard ordering. Records are keyed by the string `"{table}:shard{n}"`, so a raw
// prefix scan hands them back lexicographically — `shard10` before `shard2`.
// Every caller that reads the list in order means bucket order, so that is what
// the catalog returns.
// ---------------------------------------------------------------------------

/// One shard record of `table` for `bucket`, keyed as production keys it.
fn shard_record(table: &str, bucket: u32) -> ShardMeta {
    ShardMeta {
        shard_id: format!("shard{bucket}"),
        table_name: table.to_string(),
        primary_node_id: "n1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: bucket,
        range_lower: String::new(),
        range_upper: String::new(),
    }
}

#[test]
fn test_get_shards_for_table_is_ordered_by_bucket() {
    let catalog = make_catalog();
    // Stored back to front, to show the returned order comes from the bucket and
    // not from the order the records were written in.
    for bucket in (0..12).rev() {
        catalog.put_shard(&shard_record("orders", bucket)).unwrap();
    }

    let buckets: Vec<u32> = catalog
        .get_shards_for_table("orders")
        .unwrap()
        .iter()
        .map(|shard| shard.hash_bucket)
        .collect();

    assert_eq!(buckets, (0..12).collect::<Vec<_>>());
}

#[test]
fn test_list_all_shards_is_ordered_by_table_then_bucket() {
    let catalog = make_catalog();
    for table in ["orders", "invoices"] {
        for bucket in 0..12 {
            catalog.put_shard(&shard_record(table, bucket)).unwrap();
        }
    }

    let listed: Vec<(String, u32)> = catalog
        .list_all_shards()
        .unwrap()
        .iter()
        .map(|shard| (shard.table_name.clone(), shard.hash_bucket))
        .collect();

    let mut expected: Vec<(String, u32)> = Vec::new();
    for table in ["invoices", "orders"] {
        for bucket in 0..12 {
            expected.push((table.to_string(), bucket));
        }
    }
    assert_eq!(listed, expected);
}

// --- schemas ---

// A schema is claimed by name in one write transaction, so two concurrent
// `CREATE SCHEMA`s cannot both believe they created it: the second is told the name
// was already there.
#[test]
fn test_create_schema_is_claimed_once() {
    let catalog = make_catalog();
    let meta = SchemaMeta {
        schema_name: "sales".to_string(),
        created_at: None,
    };

    assert!(catalog.create_schema_if_absent(&meta).unwrap());
    assert!(
        !catalog.create_schema_if_absent(&meta).unwrap(),
        "the second claim must report the name was taken"
    );
    assert_eq!(
        catalog.get_schema("sales").unwrap().unwrap().schema_name,
        "sales"
    );
}

#[test]
fn test_schema_roundtrip_and_delete() {
    let catalog = make_catalog();
    for name in ["sales", "billing"] {
        catalog
            .create_schema_if_absent(&SchemaMeta {
                schema_name: name.to_string(),
                created_at: None,
            })
            .unwrap();
    }

    let listed: Vec<String> = catalog
        .list_schemas()
        .unwrap()
        .into_iter()
        .map(|s| s.schema_name)
        .collect();
    assert_eq!(listed, vec!["billing", "sales"], "listed in key order");

    catalog.delete_schema("sales").unwrap();
    assert!(catalog.get_schema("sales").unwrap().is_none());
    // Deleting a name that is not there is a no-op, so DROP SCHEMA IF EXISTS needs
    // no separate path.
    catalog.delete_schema("sales").unwrap();
    assert_eq!(catalog.list_schemas().unwrap().len(), 1);
}

// The default schema is not a record: it always exists, and a relation in it
// carries no qualifier, so nothing stores or lists it.
#[test]
fn test_the_default_schema_is_not_a_record() {
    let catalog = make_catalog();
    assert!(catalog.get_schema("public").unwrap().is_none());
    assert!(catalog.list_schemas().unwrap().is_empty());
}
