//! Where a write goes and what it looks like when it gets there: bucket
//! resolution, shard-local rewriting, and the quorum/replica arithmetic.
//!
//! The hash function and the bucket-to-shard lookup are a cross-node contract —
//! the coordinator routes on them and the core nodes create tables named by them —
//! so the tests that pin them are about agreement, not just about not crashing.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use vairedb_coordinator::catalog::{
    ColumnDef, MetadataCatalog, NodeMeta, NodeState, ShardMeta, ShardStrategy, TableMeta,
};
use vairedb_coordinator::error::CoordinatorError;
use vairedb_coordinator::pgwire_handler::parser;
use vairedb_coordinator::sqlparser::ast::Statement;
use vairedb_coordinator::write_router::{
    WriteRouter, compute_quorum_size, compute_shard_index, shard_for_bucket, target_nodes,
};
use vairedb_coordinator::write_sql_cl;

mod common;
use common::temp_catalog;

/// The shard count the default fixture uses.
const SHARDS: u32 = 3;

/// A shard count above ten, where the lexicographic order of the stored shard ids
/// (`shard10` before `shard2`) stops agreeing with the bucket order. Below eleven
/// the two coincide and indexing by position happens to work.
const WIDE_SHARDS: u32 = 11;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// One shard record of `table` for `bucket`, primaried on the single test node.
///
/// `shard_id` is spelled the way production spells it, which is what makes the
/// stored key order lexicographic rather than numeric.
fn shard_of(table: &str, bucket: u32) -> ShardMeta {
    ShardMeta {
        shard_id: format!("shard{bucket}"),
        table_name: table.to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: bucket,
        range_lower: String::new(),
        range_upper: String::new(),
    }
}

/// `orders`, sharded on `customer_id` over `shard_count` buckets, in a catalog of
/// its own.
fn catalog_with_orders(shard_count: u32) -> (Arc<MetadataCatalog>, TableMeta) {
    let catalog = Arc::new(temp_catalog());

    catalog
        .put_node(&NodeMeta {
            node_id: "node-0".to_string(),
            advertised_address: "10.0.0.1:50041".to_string(),
            state: NodeState::Alive as i32,
            last_heartbeat: None,
            registered_at: None,
        })
        .unwrap();

    let table_meta = TableMeta {
        table_name: "orders".to_string(),
        columns: ["customer_id", "amount"]
            .into_iter()
            .map(|name| ColumnDef {
                name: name.to_string(),
                data_type: "INT".to_string(),
                nullable: false,
                default_expr: String::new(),
            })
            .collect(),
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: "customer_id".to_string(),
        shard_count,
        replication_factor: 3,
        anonymized_columns: HashMap::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        created_at: None,
    };
    catalog.put_table(&table_meta).unwrap();

    for bucket in 0..shard_count {
        catalog.put_shard(&shard_of("orders", bucket)).unwrap();
    }

    (catalog, table_meta)
}

fn router_for(shard_count: u32) -> (WriteRouter, TableMeta) {
    let (catalog, table_meta) = catalog_with_orders(shard_count);
    (WriteRouter::new(catalog), table_meta)
}

fn parse_one(sql: &str) -> Statement {
    parser::parse_sql(sql)
        .expect("the fixture SQL parses")
        .swap_remove(0)
}

// ---------------------------------------------------------------------------
// The hash
// ---------------------------------------------------------------------------

/// The routing contract: a key always hashes to the same bucket, and always to one
/// that exists. A key landing outside `0..shard_count` would route a write to a
/// shard that is not there; a key hashing differently twice would put an UPDATE on
/// a different shard than the INSERT it is meant to amend.
#[test]
fn a_key_hashes_to_the_same_in_range_bucket_every_time() {
    for shard_count in [1, 3, 4, WIDE_SHARDS as usize] {
        for key in (0..100).map(|i| i.to_string()).chain(["".to_string()]) {
            let bucket = compute_shard_index(&key, shard_count);
            assert!(
                bucket < shard_count,
                "{key:?} hashed outside 0..{shard_count}"
            );
            assert_eq!(
                bucket,
                compute_shard_index(&key, shard_count),
                "{key:?} hashed differently twice"
            );
        }
    }
}

/// A bucket is resolved by the `hash_bucket` each record carries, never by the
/// record's position in the list. Past ten shards the two disagree, and routing by
/// position would fail silently — every shard can run the statement, so the row is
/// simply written to a shard that does not own its key.
#[test]
fn a_bucket_resolves_by_its_shard_record_not_by_list_position() {
    // The order a prefix scan hands the shards back in.
    let mut buckets: Vec<u32> = (0..WIDE_SHARDS).collect();
    buckets.sort_by_key(|bucket| format!("shard{bucket}"));
    assert_ne!(
        buckets[2], 2,
        "the fixture must be an order in which position and bucket disagree"
    );

    let shards: Vec<ShardMeta> = buckets.iter().map(|b| shard_of("orders", *b)).collect();
    for bucket in 0..WIDE_SHARDS as usize {
        let shard = shard_for_bucket(&shards, bucket, "orders").unwrap();
        assert_eq!(shard.hash_bucket as usize, bucket);
    }
}

/// An incomplete layout is an error, not something to fall back from: no shard owns
/// the keys that hash to the missing bucket, and any other shard would be wrong.
#[test]
fn a_bucket_with_no_shard_record_is_refused_by_name() {
    let shards: Vec<ShardMeta> = [0, 1, 3].map(|b| shard_of("orders", b)).to_vec();

    let err = shard_for_bucket(&shards, 2, "orders").expect_err("bucket 2 has no shard");

    assert!(
        matches!(err, CoordinatorError::ShardNotAssigned(_)),
        "got: {err:?}"
    );
    let message = err.to_string();
    assert!(message.contains("orders"), "got: {message}");
    assert!(message.contains("bucket 2"), "got: {message}");
}

// ---------------------------------------------------------------------------
// Choosing the target shards
// ---------------------------------------------------------------------------

/// A statement that pins the shard key goes to exactly the shard owning that key's
/// bucket — for every DML form, and across a shard count where position and bucket
/// disagree. That INSERT, UPDATE and DELETE agree on the bucket is the property
/// that makes a row findable by the statements that amend it.
#[test]
fn a_pinned_shard_key_routes_every_dml_form_to_the_bucket_that_owns_it() {
    let (router, table_meta) = router_for(WIDE_SHARDS);

    for id in 1..=60i64 {
        let expected = compute_shard_index(&id.to_string(), WIDE_SHARDS as usize);
        for sql in [
            format!("INSERT INTO orders (customer_id, amount) VALUES ({id}, 1)"),
            format!("UPDATE orders SET amount = 2 WHERE customer_id = {id}"),
            format!("DELETE FROM orders WHERE customer_id = {id}"),
        ] {
            let shards = router
                .resolve_target_shards(&parse_one(&sql), &table_meta, &[])
                .unwrap();

            assert_eq!(shards.len(), 1, "`{sql}` must route to one shard");
            assert_eq!(
                shards[0].hash_bucket as usize, expected,
                "`{sql}` must go to the shard owning bucket {expected}"
            );
        }
    }
}

/// A statement that does not pin the shard key must reach every shard: any of them
/// may hold a matching row, so a broadcast that missed one would under-report and
/// leave rows behind.
#[test]
fn a_statement_that_does_not_pin_the_key_broadcasts_in_bucket_order() {
    let (router, table_meta) = router_for(WIDE_SHARDS);

    for sql in [
        "DELETE FROM orders",
        "UPDATE orders SET amount = 0 WHERE amount > 100",
        "SELECT * FROM orders",
    ] {
        let shards = router
            .resolve_target_shards(&parse_one(sql), &table_meta, &[])
            .unwrap();

        let buckets: Vec<u32> = shards.iter().map(|shard| shard.hash_bucket).collect();
        assert_eq!(
            buckets,
            (0..WIDE_SHARDS).collect::<Vec<_>>(),
            "`{sql}` must reach every shard, in bucket order"
        );
    }
}

/// A table with no shard records cannot be written. There is no shard to pick, and
/// picking none would silently accept a write that went nowhere.
#[test]
fn a_table_with_no_shards_cannot_be_routed() {
    let (catalog, table_meta) = catalog_with_orders(SHARDS);
    catalog.delete_shards_for_table("orders").unwrap();
    let router = WriteRouter::new(catalog);

    let sql = "INSERT INTO orders (customer_id, amount) VALUES (1, 1)";
    let err = router
        .resolve_target_shards(&parse_one(sql), &table_meta, &[])
        .expect_err("a table with no shards has nowhere to put the row");

    assert!(
        matches!(err, CoordinatorError::ShardNotAssigned(_)),
        "got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Rewriting to shard-local SQL
// ---------------------------------------------------------------------------

/// The rewritten statement names the physical shard table, which is the string the
/// core node created its DuckDB table under. Every DML form must be rewritten: one
/// that kept the logical name would fail on a node that has no such table.
#[test]
fn every_dml_form_is_rewritten_to_the_physical_shard_table() {
    let (router, _) = router_for(SHARDS);

    for (bucket, sql, rest) in [
        (0u32, "INSERT INTO orders (customer_id) VALUES (1)", "1"),
        (1, "DELETE FROM orders WHERE customer_id = 5", "5"),
        (
            2,
            "UPDATE orders SET amount = 99 WHERE customer_id = 1",
            "99",
        ),
    ] {
        let (rewritten, params) = router
            .generate_shard_local_sql(&parse_one(sql), &shard_of("orders", bucket), &[])
            .unwrap();

        assert!(
            rewritten.contains(&format!("orders_shard{bucket}")),
            "`{sql}` was not rewritten to the shard table: {rewritten}"
        );
        assert!(
            !rewritten.contains(" orders "),
            "the logical name survived the rewrite: {rewritten}"
        );
        assert!(
            rewritten.contains(rest),
            "the rewrite dropped part of the statement: {rewritten}"
        );
        assert!(params.is_empty(), "no placeholders were bound");
    }
}

/// DDL is rewritten too, types included: a `BYTEA` column has to reach DuckDB as
/// `BLOB`, or the shard table would not be creatable.
#[test]
fn ddl_is_rewritten_with_its_types_translated() {
    let (router, _) = router_for(SHARDS);

    let (rewritten, _) = router
        .generate_shard_local_sql(
            &parse_one("CREATE TABLE orders (data BYTEA)"),
            &shard_of("orders", 1),
            &[],
        )
        .unwrap();

    assert!(rewritten.contains("orders_shard1"), "got: {rewritten}");
    assert!(rewritten.contains("BLOB"), "got: {rewritten}");
}

/// A placeholder that is not a positional index cannot be renumbered, and the bind
/// parameters would then be dropped — the statement would run against whatever the
/// engine made of `$foo`. Refused instead.
#[test]
fn a_placeholder_that_cannot_be_renumbered_is_an_error_not_a_dropped_parameter() {
    let (router, _) = router_for(SHARDS);

    let result = router.generate_shard_local_sql(
        &parse_one("INSERT INTO orders (customer_id) VALUES ($foo)"),
        &shard_of("orders", 0),
        &[ScalarValue::Int64(Some(1))],
    );

    assert!(
        matches!(result, Err(CoordinatorError::Internal(_))),
        "got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Quorum and replica targets
// ---------------------------------------------------------------------------

/// A quorum is a strict majority, so an even replication factor needs the same
/// count as the next odd one — that is what stops two quorums from overlapping.
#[test]
fn a_quorum_is_a_strict_majority_of_the_replication_factor() {
    let sizes: Vec<usize> = (1..=6).map(compute_quorum_size).collect();
    assert_eq!(sizes, vec![1, 2, 2, 3, 3, 4]);
}

/// The primary comes first, then the replicas in order. Order is load-bearing: the
/// primary's acknowledgement is what makes a write durable, so it must be
/// identifiable rather than just present.
#[test]
fn the_primary_leads_the_replica_targets() {
    let mut shard = shard_of("orders", 0);
    assert_eq!(target_nodes(&shard).collect::<Vec<_>>(), vec!["node-0"]);

    shard.replica_node_ids = vec!["node-1".to_string(), "node-2".to_string()];
    assert_eq!(
        target_nodes(&shard).collect::<Vec<_>>(),
        vec!["node-0", "node-1", "node-2"]
    );
}

// ---------------------------------------------------------------------------
// Multi-shard INSERT splitting
//
// Mirrors the grouping in pgwire_handler::handle_insert_with_split: each VALUES
// row is bucketed by its own shard-key value, then split_insert_by_rows rebuilds a
// per-shard INSERT carrying only that shard's rows.
// ---------------------------------------------------------------------------

/// Group VALUES-row indices by target shard exactly as handle_insert_with_split
/// does, so the split can be asserted without a live cluster.
fn group_rows_by_shard(stmt: &Statement, shard_count: usize) -> HashMap<usize, Vec<usize>> {
    let keys = write_sql_cl::extract_insert_row_shard_keys(stmt, "customer_id", &[])
        .expect("a multi-row INSERT exposes per-row shard keys");

    let mut shard_rows: HashMap<usize, Vec<usize>> = HashMap::new();
    for (row_idx, key_value) in &keys {
        shard_rows
            .entry(compute_shard_index(key_value, shard_count))
            .or_default()
            .push(*row_idx);
    }
    shard_rows
}

fn insert_of(ids: &[i64]) -> Statement {
    let values: Vec<String> = ids.iter().map(|id| format!("({id}, 1)")).collect();
    parse_one(&format!(
        "INSERT INTO orders (customer_id, amount) VALUES {}",
        values.join(", ")
    ))
}

/// Ids hashing to distinct buckets split into one group each; ids sharing a bucket
/// stay in one group. A row grouped by the statement's bucket rather than its own
/// would be written to a shard that does not own its key.
#[test]
fn a_multi_row_insert_groups_each_row_by_its_own_key() {
    // Three ids on three different buckets, and three that share one.
    let mut first_of_bucket: HashMap<usize, i64> = HashMap::new();
    let mut co_located: Vec<i64> = Vec::new();
    let target = compute_shard_index("1", SHARDS as usize);
    let mut id = 1i64;
    while first_of_bucket.len() < SHARDS as usize || co_located.len() < 3 {
        let bucket = compute_shard_index(&id.to_string(), SHARDS as usize);
        first_of_bucket.entry(bucket).or_insert(id);
        if bucket == target && co_located.len() < 3 {
            co_located.push(id);
        }
        id += 1;
    }

    let spread: Vec<i64> = first_of_bucket.values().copied().collect();
    let groups = group_rows_by_shard(&insert_of(&spread), SHARDS as usize);
    assert_eq!(
        groups.len(),
        SHARDS as usize,
        "ids on distinct buckets must split into one group each"
    );
    // Every row is assigned to exactly one group, and none is lost.
    let mut assigned: Vec<usize> = groups.values().flatten().copied().collect();
    assigned.sort_unstable();
    assert_eq!(assigned, (0..spread.len()).collect::<Vec<_>>());

    let groups = group_rows_by_shard(&insert_of(&co_located), SHARDS as usize);
    assert_eq!(groups.len(), 1, "co-located rows must form one group");
    assert_eq!(groups[&target].len(), 3);
}
