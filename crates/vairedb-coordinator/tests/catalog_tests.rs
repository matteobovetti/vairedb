//! `MetadataCatalog` behaviour a caller can observe: what a record round-trips,
//! what order a list comes back in, and which operations are atomic claims rather
//! than check-then-write.

use vairedb_coordinator::catalog::{
    AnonymizationSecret, ColumnDef, NodeMeta, NodeState, SchemaMeta, ShardMeta, ShardStrategy,
    TableMeta,
};

mod common;
use common::temp_catalog;

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

/// An alive node with a heartbeat behind it.
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

/// The same node in `state`, for the lists that must or must not include it.
fn node_in_state(id: &str, addr: &str, state: NodeState) -> NodeMeta {
    NodeMeta {
        state: state as i32,
        ..sample_node(id, addr)
    }
}

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

// ---------------------------------------------------------------------------
// tables
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_table_round_trips_every_field() {
    let catalog = temp_catalog();
    catalog.put_table(&sample_table_meta()).unwrap();

    let result = catalog.get_table("orders").unwrap().unwrap();
    assert_eq!(result.table_name, "orders");
    assert_eq!(result.shard_count, 6);
    assert_eq!(result.replication_factor, 3);
    assert_eq!(result.shard_strategy, ShardStrategy::Hash as i32);
    assert_eq!(result.shard_key, "customer_id");
    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].data_type, "INTEGER");
    assert!(result.columns[2].nullable);
}

#[test]
fn a_range_sharded_table_round_trips() {
    let catalog = temp_catalog();
    let mut meta = sample_table_meta();
    meta.shard_strategy = ShardStrategy::Range as i32;
    catalog.put_table(&meta).unwrap();

    let result = catalog.get_table("orders").unwrap().unwrap();
    assert_eq!(result.shard_strategy, ShardStrategy::Range as i32);
}

#[test]
fn put_table_overwrites_existing() {
    let catalog = temp_catalog();
    let mut meta = sample_table_meta();
    catalog.put_table(&meta).unwrap();

    meta.shard_count = 12;
    meta.replication_factor = 5;
    catalog.put_table(&meta).unwrap();

    let result = catalog.get_table("orders").unwrap().unwrap();
    assert_eq!(result.shard_count, 12);
    assert_eq!(result.replication_factor, 5);
    assert_eq!(
        catalog.list_tables().unwrap().len(),
        1,
        "one record, not two"
    );
}

#[test]
fn delete_table_removes_the_record() {
    let catalog = temp_catalog();
    catalog.put_table(&sample_table_meta()).unwrap();

    catalog.delete_table("orders").unwrap();
    assert!(catalog.get_table("orders").unwrap().is_none());
}

#[test]
fn list_tables_returns_every_table() {
    let catalog = temp_catalog();
    for name in ["table_b", "table_a"] {
        let mut meta = sample_table_meta();
        meta.table_name = name.to_string();
        catalog.put_table(&meta).unwrap();
    }

    let names: Vec<String> = catalog
        .list_tables()
        .unwrap()
        .into_iter()
        .map(|t| t.table_name)
        .collect();
    assert_eq!(names, vec!["table_a", "table_b"], "listed in key order");
}

#[test]
fn create_table_if_absent_claims_a_free_name() {
    let catalog = temp_catalog();
    assert!(
        catalog
            .create_table_if_absent(&sample_table_meta())
            .unwrap()
    );
    assert_eq!(
        catalog.get_table("orders").unwrap().unwrap().shard_key,
        "customer_id"
    );
}

#[test]
fn create_table_if_absent_refuses_a_taken_name_without_overwriting() {
    let catalog = temp_catalog();
    catalog
        .create_table_if_absent(&sample_table_meta())
        .unwrap();

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
fn create_table_if_absent_claims_again_after_delete() {
    let catalog = temp_catalog();
    let meta = sample_table_meta();

    assert!(catalog.create_table_if_absent(&meta).unwrap());
    catalog.delete_table("orders").unwrap();
    assert!(
        catalog.create_table_if_absent(&meta).unwrap(),
        "a dropped name is free again"
    );
}

#[test]
fn concurrent_create_table_if_absent_yields_exactly_one_winner() {
    use std::sync::Arc;

    let catalog = Arc::new(temp_catalog());
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

// ---------------------------------------------------------------------------
// shards
//
// Records are keyed by the string `"{table}:shard{n}"`, so a raw prefix scan hands
// them back lexicographically — `shard10` before `shard2` — and a scan for `orders`
// would reach `orders_archive`'s keys if its upper bound were wrong. Both the read
// and the delete path depend on that range, so both are pinned here.
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_shard_round_trips_every_field() {
    let catalog = temp_catalog();
    let mut shard = shard_record("orders", 0);
    shard.primary_node_id = "node-1".to_string();
    shard.replica_node_ids = vec!["node-2".to_string(), "node-3".to_string()];
    catalog.put_shard(&shard).unwrap();

    let results = catalog.shards_for_table("orders").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].shard_id, "shard0");
    assert_eq!(results[0].hash_bucket, 0);
    assert_eq!(results[0].primary_node_id, "node-1");
    assert_eq!(results[0].replica_node_ids, vec!["node-2", "node-3"]);
}

#[test]
fn shards_for_table_is_ordered_by_bucket() {
    let catalog = temp_catalog();
    // Stored back to front, to show the returned order comes from the bucket and
    // not from the order the records were written in.
    for bucket in (0..12).rev() {
        catalog.put_shard(&shard_record("orders", bucket)).unwrap();
    }

    let buckets: Vec<u32> = catalog
        .shards_for_table("orders")
        .unwrap()
        .iter()
        .map(|shard| shard.hash_bucket)
        .collect();

    assert_eq!(buckets, (0..12).collect::<Vec<_>>());
}

#[test]
fn list_all_shards_is_ordered_by_table_then_bucket() {
    let catalog = temp_catalog();
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

#[test]
fn shards_for_table_stops_at_a_longer_table_name() {
    let catalog = temp_catalog();
    catalog.put_shard(&shard_record("orders", 0)).unwrap();
    catalog
        .put_shard(&shard_record("orders_archive", 0))
        .unwrap();

    let orders = catalog.shards_for_table("orders").unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].table_name, "orders");

    let archive = catalog.shards_for_table("orders_archive").unwrap();
    assert_eq!(archive.len(), 1);
    assert_eq!(archive[0].table_name, "orders_archive");
}

#[test]
fn delete_shards_for_table_stops_at_a_longer_table_name() {
    let catalog = temp_catalog();
    for bucket in 0..3 {
        catalog.put_shard(&shard_record("orders", bucket)).unwrap();
    }
    catalog
        .put_shard(&shard_record("orders_archive", 0))
        .unwrap();

    catalog.delete_shards_for_table("orders").unwrap();

    assert!(catalog.shards_for_table("orders").unwrap().is_empty());
    assert_eq!(
        catalog.shards_for_table("orders_archive").unwrap().len(),
        1,
        "dropping `orders` must not take the shards of a table it merely prefixes"
    );
}

// ---------------------------------------------------------------------------
// nodes
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_node_round_trips_every_field() {
    let catalog = temp_catalog();
    catalog
        .put_node(&sample_node("node-1", "10.0.0.1:50041"))
        .unwrap();

    let result = catalog.get_node("node-1").unwrap().unwrap();
    assert_eq!(result.node_id, "node-1");
    assert_eq!(result.advertised_address, "10.0.0.1:50041");
    assert_eq!(result.state, NodeState::Alive as i32);
    assert_eq!(result.last_heartbeat.unwrap().seconds, 1000);
}

#[test]
fn put_node_overwrites_existing() {
    let catalog = temp_catalog();
    catalog
        .put_node(&sample_node("n1", "10.0.0.1:50041"))
        .unwrap();

    catalog
        .put_node(&node_in_state("n1", "10.0.0.99:50041", NodeState::Suspect))
        .unwrap();

    let result = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(result.advertised_address, "10.0.0.99:50041");
    assert_eq!(result.state, NodeState::Suspect as i32);
    assert_eq!(catalog.list_all_nodes().unwrap().len(), 1);
}

#[test]
fn list_alive_nodes_excludes_suspect_and_dead() {
    let catalog = temp_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();
    catalog
        .put_node(&node_in_state("n2", "a:2", NodeState::Suspect))
        .unwrap();
    catalog
        .put_node(&node_in_state("n3", "a:3", NodeState::Dead))
        .unwrap();

    let alive = catalog.list_alive_nodes().unwrap();
    assert_eq!(alive.len(), 1);
    assert_eq!(alive[0].node_id, "n1");

    assert_eq!(
        catalog.list_all_nodes().unwrap().len(),
        3,
        "`list_all_nodes` is state-blind"
    );
}

#[test]
fn update_node_state_sets_the_state() {
    let catalog = temp_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();

    for state in [NodeState::Suspect, NodeState::Dead] {
        catalog.update_node_state("n1", state).unwrap();
        assert_eq!(
            catalog.get_node("n1").unwrap().unwrap().state,
            state as i32,
            "{state:?}"
        );
    }
}

#[test]
fn update_node_heartbeat_refreshes_the_stamp_and_revives_the_node() {
    let catalog = temp_catalog();
    let mut stale = node_in_state("n1", "a:1", NodeState::Suspect);
    // Epoch, so any real clock reading is visibly newer.
    stale.last_heartbeat = Some(prost_types::Timestamp {
        seconds: 0,
        nanos: 0,
    });
    catalog.put_node(&stale).unwrap();

    catalog.update_node_heartbeat("n1").unwrap();

    let node = catalog.get_node("n1").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Alive as i32);
    assert!(node.last_heartbeat.unwrap().seconds > 0);
}

#[test]
fn updating_an_absent_node_is_an_error() {
    // Not a silent no-op: a heartbeat or a state change for a node nobody registered
    // means the caller and the catalog disagree about the cluster.
    let catalog = temp_catalog();
    assert!(
        catalog
            .update_node_state("absent", NodeState::Dead)
            .is_err()
    );
    assert!(catalog.update_node_heartbeat("absent").is_err());
}

#[test]
fn node_address_map_includes_nodes_of_every_state() {
    // Routing needs an address for a node it is about to give up on, so the map is
    // deliberately not filtered by state.
    let catalog = temp_catalog();
    catalog
        .put_node(&sample_node("n1", "10.0.0.1:50041"))
        .unwrap();
    catalog
        .put_node(&node_in_state("n2", "10.0.0.2:50041", NodeState::Dead))
        .unwrap();

    let map = catalog.node_address_map().unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(map["n1"], "10.0.0.1:50041");
    assert_eq!(map["n2"], "10.0.0.2:50041");
}

// ---------------------------------------------------------------------------
// shard assignment
// ---------------------------------------------------------------------------

#[test]
fn assign_shards_round_robin_cycles_primaries_over_the_alive_nodes() {
    let catalog = temp_catalog();
    for (id, addr) in [("n1", "a:1"), ("n2", "a:2"), ("n3", "a:3")] {
        catalog.put_node(&sample_node(id, addr)).unwrap();
    }

    let shards = catalog.assign_shards_round_robin("orders", 6, 3).unwrap();

    assert_eq!(shards.len(), 6);
    let primaries: Vec<&str> = shards.iter().map(|s| s.primary_node_id.as_str()).collect();
    assert_eq!(
        primaries,
        vec!["n1", "n2", "n3", "n1", "n2", "n3"],
        "primaries cycle over the alive nodes in catalog order"
    );
    // Replicas are the nodes following the primary, so the layout of every shard is
    // determined — and no shard may hold a replica of itself.
    assert_eq!(shards[0].replica_node_ids, vec!["n2", "n3"]);
    for (i, shard) in shards.iter().enumerate() {
        assert_eq!(shard.shard_id, format!("shard{i}"));
        assert_eq!(shard.table_name, "orders");
        assert_eq!(shard.hash_bucket, i as u32);
        assert_eq!(shard.replica_node_ids.len(), 2);
        assert!(!shard.replica_node_ids.contains(&shard.primary_node_id));
    }
}

#[test]
fn assign_shards_gives_a_lone_node_no_replicas() {
    // Every replica offset wraps back onto the primary, and a node is not its own
    // replica — so the shard is simply unreplicated rather than duplicated.
    let catalog = temp_catalog();
    catalog.put_node(&sample_node("n1", "a:1")).unwrap();

    let shards = catalog.assign_shards_round_robin("orders", 4, 3).unwrap();

    assert_eq!(shards.len(), 4);
    for shard in &shards {
        assert_eq!(shard.primary_node_id, "n1");
        assert!(shard.replica_node_ids.is_empty());
    }
}

#[test]
fn assign_shards_without_replication_gives_no_replicas() {
    let catalog = temp_catalog();
    for (id, addr) in [("n1", "a:1"), ("n2", "a:2"), ("n3", "a:3")] {
        catalog.put_node(&sample_node(id, addr)).unwrap();
    }

    // A replication factor counts the primary, so 1 and 0 both mean "no replica".
    for factor in [0, 1] {
        let shards = catalog
            .assign_shards_round_robin("orders", 3, factor)
            .unwrap();
        assert_eq!(shards.len(), 3);
        for shard in &shards {
            assert!(
                shard.replica_node_ids.is_empty(),
                "replication_factor {factor}"
            );
        }
    }
}

#[test]
fn assign_shards_repeats_a_node_when_replication_exceeds_the_cluster() {
    // Today's behaviour, pinned: with 2 nodes and factor 5, the offsets that wrap
    // back onto the primary are dropped and the rest all name the one other node, so
    // the same node is listed twice. A cluster cannot hold 5 copies on 2 machines,
    // and the assignment does not pretend otherwise by inventing nodes.
    let catalog = temp_catalog();
    for (id, addr) in [("n1", "a:1"), ("n2", "a:2")] {
        catalog.put_node(&sample_node(id, addr)).unwrap();
    }

    let shards = catalog.assign_shards_round_robin("orders", 4, 5).unwrap();

    assert_eq!(shards.len(), 4);
    for shard in &shards {
        let other = if shard.primary_node_id == "n1" {
            "n2"
        } else {
            "n1"
        };
        assert_eq!(shard.replica_node_ids, vec![other, other]);
    }
}

#[test]
fn assign_shards_without_an_alive_node_is_an_error() {
    // A layout naming no node would be worse than no layout: the table would appear
    // to exist and every write to it would have nowhere to go.
    let catalog = temp_catalog();
    catalog
        .put_node(&node_in_state("n1", "a:1", NodeState::Dead))
        .unwrap();
    assert!(catalog.assign_shards_round_robin("orders", 6, 3).is_err());
}

// ---------------------------------------------------------------------------
// anonymization secrets
// ---------------------------------------------------------------------------

#[test]
fn put_and_get_anonymization_secret_round_trips_the_key() {
    // The key is stored in full; it is the `vairedb_catalog.anonymization_secret`
    // view that withholds it from SQL, not the catalog.
    let catalog = temp_catalog();
    catalog
        .put_anonymization_secret(&AnonymizationSecret {
            id: "my_sid".to_string(),
            algo: "HMAC-SHA256".to_string(),
            secret_key: "super_secret".to_string(),
        })
        .unwrap();

    let fetched = catalog.get_anonymization_secret("my_sid").unwrap().unwrap();
    assert_eq!(fetched.id, "my_sid");
    assert_eq!(fetched.algo, "HMAC-SHA256");
    assert_eq!(fetched.secret_key, "super_secret");
}

#[test]
fn list_anonymization_secrets_returns_every_secret() {
    let catalog = temp_catalog();
    for id in ["a", "b"] {
        catalog
            .put_anonymization_secret(&AnonymizationSecret {
                id: id.to_string(),
                algo: "HMAC-SHA256".to_string(),
                secret_key: format!("key_{id}"),
            })
            .unwrap();
    }

    assert_eq!(catalog.list_anonymization_secrets().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// schemas
// ---------------------------------------------------------------------------

#[test]
fn create_schema_is_claimed_once() {
    // One write transaction, so two concurrent `CREATE SCHEMA`s cannot both believe
    // they created it: the second is told the name was already there.
    let catalog = temp_catalog();
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
fn schema_round_trip_and_delete() {
    let catalog = temp_catalog();
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
    assert_eq!(catalog.list_schemas().unwrap().len(), 1);
}

#[test]
fn the_default_schema_is_not_a_record() {
    // It always exists, and a relation in it carries no qualifier, so nothing stores
    // or lists it.
    let catalog = temp_catalog();
    assert!(catalog.get_schema("public").unwrap().is_none());
    assert!(catalog.list_schemas().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// the empty catalog
// ---------------------------------------------------------------------------

/// An absent record is `None` and an absent list is empty — never an error, because
/// "does it exist?" is a question every DDL path asks before it decides anything.
/// Deleting what is not there is a no-op for the same reason: `DROP ... IF EXISTS`
/// needs no separate path.
#[test]
fn an_empty_catalog_answers_empty_and_deleting_from_it_is_a_no_op() {
    let catalog = temp_catalog();

    assert!(catalog.get_table("absent").unwrap().is_none());
    assert!(catalog.get_view("absent").unwrap().is_none());
    assert!(catalog.get_schema("absent").unwrap().is_none());
    assert!(catalog.get_node("absent").unwrap().is_none());
    assert!(
        catalog
            .get_anonymization_secret("absent")
            .unwrap()
            .is_none()
    );

    assert!(catalog.list_tables().unwrap().is_empty());
    assert!(catalog.list_views().unwrap().is_empty());
    assert!(catalog.list_schemas().unwrap().is_empty());
    assert!(catalog.list_all_nodes().unwrap().is_empty());
    assert!(catalog.list_all_shards().unwrap().is_empty());
    assert!(catalog.shards_for_table("absent").unwrap().is_empty());
    assert!(catalog.list_anonymization_secrets().unwrap().is_empty());
    assert!(catalog.node_address_map().unwrap().is_empty());

    catalog.delete_table("absent").unwrap();
    catalog.delete_view("absent").unwrap();
    catalog.delete_schema("absent").unwrap();
    catalog.delete_shards_for_table("absent").unwrap();
}
