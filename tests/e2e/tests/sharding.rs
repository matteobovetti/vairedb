mod common;
use common::*;
use std::collections::HashSet;
use xxhash_rust::xxh3::xxh3_64;

// Shard-count and replication-factor configuration variants, and the placement
// metadata they produce in `vairedb_catalog.shards`: shard count (explicit,
// default, and the rf>nodes rejection), replica counts, no-duplicate-node
// placement, string shard keys, multi-row insert splitting, and a shard count
// past ten, where a shard's position in a lexicographically keyed list is no
// longer its hash bucket.
//
// Predicate routing (single-shard vs broadcast) lives in `shard_routing.rs`.

#[tokio::test]
async fn test_shard_count_one() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_one",
        "(id INTEGER NOT NULL, v VARCHAR) \
         WITH (shards = 1, replication_factor = 3, shard_by = 'id')",
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(shards.len(), 1, "expected exactly 1 shard");

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![1, 2, 3]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_shard_count_five_rf_one() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_five",
        "(id INTEGER NOT NULL, v VARCHAR) \
         WITH (shards = 5, replication_factor = 1, shard_by = 'id')",
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(shards.len(), 5, "expected 5 shards");

    // rf = 1 means no replicas, and each primary on a distinct node.
    let mut primaries = HashSet::new();
    for (bucket, primary, replicas) in &shards {
        assert!(
            replicas.is_empty(),
            "shard bucket {bucket} should have no replicas with rf=1, got {replicas:?}"
        );
        assert!(
            primaries.insert(primary.clone()),
            "primary node {primary} used by more than one shard"
        );
    }
    assert_eq!(primaries.len(), 5, "expected 5 distinct primary nodes");

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("5"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_rf_less_than_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_rf2",
        "(id INTEGER NOT NULL) \
         WITH (shards = 3, replication_factor = 2, shard_by = 'id')",
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(shards.len(), 3);

    for (bucket, primary, replicas) in &shards {
        assert_eq!(
            replicas.len(),
            1,
            "shard bucket {bucket} should have rf-1 = 1 replica, got {}",
            replicas.len()
        );
        let mut nodes = HashSet::new();
        nodes.insert(primary.as_str());
        for r in replicas {
            assert!(
                nodes.insert(r.as_str()),
                "shard bucket {bucket} has the same node {r} as primary and replica"
            );
        }
    }

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_rf_exceeds_node_count_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("sh_rf_big");

    // replication_factor (7) > number of core nodes (5) would force replicas to be
    // placed on duplicate nodes, so the coordinator must reject the CREATE outright.
    let result = execute(
        &client,
        &format!(
            "CREATE TABLE {tbl} (id INTEGER NOT NULL) \
             WITH (shards = 3, replication_factor = 7, shard_by = 'id')"
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "CREATE TABLE with rf > node count should be rejected, got {result:?}"
    );

    // The rejection must leave no catalog state behind.
    let tables = simple_query_rows(
        &client,
        &format!("SELECT table_name FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert!(
        tables.is_empty(),
        "rejected CREATE TABLE must not leave a catalog row, found {tables:?}"
    );
}

#[tokio::test]
async fn test_default_shard_count() {
    let client = ready_client().await;

    // Omitting `shards` defaults the shard count to the number of alive nodes.
    let tbl = create_table(
        &client,
        "sh_default",
        "(id INTEGER NOT NULL) \
         WITH (replication_factor = 3, shard_by = 'id')",
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(
        shards.len(),
        EXPECTED_NODES,
        "default shard count should equal alive node count ({EXPECTED_NODES})"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_string_shard_key() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_strkey",
        &format!(
            "(name VARCHAR NOT NULL, score INTEGER NOT NULL) \
             WITH (shards = {SHARD_COUNT}, replication_factor = 3, shard_by = 'name')"
        ),
    )
    .await;

    let names = ["alice", "bob", "carol", "dave", "erin", "frank", "grace"];
    for (i, name) in names.iter().enumerate() {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (name, score) VALUES ('{name}', {i})"),
        )
        .await
        .unwrap();
    }

    // The router hashes the literal's serialized form, which for a string literal
    // includes the surrounding single quotes (e.g. 'alice'). Reproduce that here
    // and confirm every name maps into a valid bucket and the set is non-degenerate.
    let mut used_buckets = HashSet::new();
    for name in &names {
        let literal = format!("'{name}'");
        let bucket = (xxh3_64(literal.as_bytes()) as usize) % SHARD_COUNT;
        assert!(bucket < SHARD_COUNT);
        used_buckets.insert(bucket);
    }
    assert!(
        used_buckets.len() > 1,
        "string shard key produced a degenerate distribution: all names in one bucket"
    );

    // All rows must be readable back regardless of routing.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some(names.len().to_string().as_str())
    );

    // A point lookup on the shard key must return exactly the matching row.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT score FROM {tbl} WHERE name = 'carol'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

/// Twelve shards: past ten, where the catalog's shard records — keyed by the
/// string `"{table}:shard{n}"` — stop coming back in bucket order, because
/// `shard10` sorts before `shard2`.
const WIDE_SHARD_COUNT: usize = 12;

// A table with more shards than the shard ids sort numerically for. Everything
// here holds trivially at three shards and is the first thing to break at twelve
// if any part of the write path treats a shard's position in the catalog's list
// as its hash bucket — silently, because every shard can execute the statement.
#[tokio::test]
async fn test_more_than_ten_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_wide",
        &format!(
            "(id INTEGER NOT NULL, v VARCHAR) \
             WITH (shards = {WIDE_SHARD_COUNT}, replication_factor = 3, shard_by = 'id')"
        ),
    )
    .await;

    // Deliberately unordered: the catalog must hand a table's shards back in
    // bucket order on its own, since that is the order every caller reading the
    // list positionally assumes.
    let listed = simple_query_rows(
        &client,
        &format!("SELECT hash_bucket FROM vairedb_catalog.shards WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    let buckets: Vec<u64> = listed
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(
        buckets,
        (0..WIDE_SHARD_COUNT as u64).collect::<Vec<_>>(),
        "the catalog should list shards by hash bucket, got {buckets:?}"
    );

    // Ids spanning every bucket, including the two-digit ones whose bucket and
    // lexicographic position disagree.
    let ids: Vec<i64> = (1..=60).collect();
    let high_ids: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|&id| bucket_of_in(id, WIDE_SHARD_COUNT) >= 10)
        .collect();
    assert!(
        high_ids.len() >= 2,
        "the test ids must reach the buckets above nine; got {high_ids:?}"
    );

    let values: Vec<String> = ids.iter().map(|id| format!("({id}, 'v{id}')")).collect();
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES {}", values.join(", ")),
    )
    .await
    .unwrap();
    assert_eq!(affected, ids.len() as u64, "every row should be inserted");

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(
        got, ids,
        "the split insert should have stored every row once"
    );

    // A point lookup, update and delete are each routed to a single shard: they
    // have to reach the one the row was written to.
    let target = high_ids[0];
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = {target}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "id {target} should be found on its shard");
    assert_eq!(rows[0][0].as_deref(), Some(format!("v{target}").as_str()));

    let affected = execute(
        &client,
        &format!("UPDATE {tbl} SET v = 'updated' WHERE id = {target}"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1, "the update should reach the row's shard");

    let affected = execute(&client, &format!("DELETE FROM {tbl} WHERE id = {target}"))
        .await
        .unwrap();
    assert_eq!(affected, 1, "the delete should reach the row's shard");
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64 - 1);

    // A MERGE from a VALUES list splits its rows the same way an INSERT does, so
    // it must find the surviving row and re-insert the deleted one — on the shard
    // each key belongs to, or the match fails and the row is stored twice.
    let surviving = high_ids[1];
    let affected = execute(
        &client,
        &format!(
            "MERGE INTO {tbl} t \
             USING (VALUES ({target}, 'merged'), ({surviving}, 'merged')) AS s(id, v) \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET v = s.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        ),
    )
    .await
    .unwrap();
    assert_eq!(affected, 2, "one row re-inserted, one updated");
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id, v FROM {tbl} WHERE v = 'merged' ORDER BY id"),
    )
    .await
    .unwrap();
    let merged: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    let mut expected = vec![target, surviving];
    expected.sort_unstable();
    assert_eq!(
        merged, expected,
        "each merged row should exist exactly once: {rows:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_multi_row_insert_split() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "sh_split",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Pick ids that land in different buckets so the multi-row INSERT is split.
    let ids: Vec<i64> = (1..=9).collect();
    let buckets: HashSet<u64> = ids.iter().map(|&id| bucket_of(id)).collect();
    assert!(
        buckets.len() > 1,
        "test ids do not span multiple shards; pick different ids"
    );

    let values: Vec<String> = ids.iter().map(|i| format!("({i}, 'v{i}')")).collect();
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES {}", values.join(", ")),
    )
    .await
    .unwrap();
    assert_eq!(affected, ids.len() as u64, "all rows should be inserted");

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, ids);

    drop_table(&client, &tbl).await;
}
