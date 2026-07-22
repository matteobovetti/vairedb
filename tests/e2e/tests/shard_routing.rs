mod common;
use common::*;
use std::collections::HashSet;
use tokio_postgres::Client;
use xxhash_rust::xxh3::xxh3_64;

// Write-router predicate routing. The router only extracts a single shard from
// `col = literal` (and AND-chains); `IN`, `OR`, `BETWEEN`, and functions on the
// key fall back to broadcasting the statement to ALL shards. Broadcast is coarse
// but still produces correct RESULTS, so these tests pin that fallback for
// SELECT/UPDATE/DELETE. The shard hash is computed over the STRING form of the
// literal, which a couple of tests document with an in-test `xxh3_64` mirror.
// Also covers multi-statement simple-query handling.
//
// Shard-count / replication-factor placement configuration lives in `sharding.rs`.

async fn setup_nums(client: &Client, tbl: &str) {
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    let values: Vec<String> = (1..=6).map(|i| format!("({i}, 'v{i}')")).collect();
    execute(
        client,
        &format!("INSERT INTO {tbl} (id, v) VALUES {}", values.join(", ")),
    )
    .await
    .unwrap();
}

// SELECT with an IN list on the shard key: broadcast fan-out still returns exactly
// the matching rows.
#[tokio::test]
async fn test_select_in_list_on_shard_key() {
    let client = ready_client().await;
    let tbl = unique_table_name("sr_in");
    setup_nums(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE id IN (2, 4, 6) ORDER BY id"),
    )
    .await
    .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![2, 4, 6]);

    drop_table(&client, &tbl).await;
}

// DELETE with an OR predicate on the shard key broadcasts and removes exactly the
// matching rows (no over-deletion, no under-deletion).
#[tokio::test]
async fn test_delete_or_predicate_on_shard_key() {
    let client = ready_client().await;
    let tbl = unique_table_name("sr_or_del");
    setup_nums(&client, &tbl).await;

    execute(
        &client,
        &format!("DELETE FROM {tbl} WHERE id = 1 OR id = 5"),
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
    assert_eq!(got, vec![2, 3, 4, 6]);

    drop_table(&client, &tbl).await;
}

// UPDATE with a BETWEEN range on the shard key broadcasts and updates exactly the
// in-range rows.
#[tokio::test]
async fn test_update_between_range_on_shard_key() {
    let client = ready_client().await;
    let tbl = unique_table_name("sr_between");
    setup_nums(&client, &tbl).await;

    execute(
        &client,
        &format!("UPDATE {tbl} SET v = 'x' WHERE id BETWEEN 2 AND 4"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<(i64, String)> = rows
        .iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().parse().unwrap(),
                r[1].as_deref().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1, "v1".into()),
            (2, "x".into()),
            (3, "x".into()),
            (4, "x".into()),
            (5, "v5".into()),
            (6, "v6".into()),
        ]
    );

    drop_table(&client, &tbl).await;
}

// The router hashes the STRING FORM of the literal, so the integer literal `1`
// and the string literal `'1'` route to different buckets. This documents that
// behavior and confirms an integer-keyed point lookup still finds its row.
#[tokio::test]
async fn test_value_form_hashing_is_string_based() {
    let client = ready_client().await;
    let tbl = unique_table_name("sr_valueform");
    setup_nums(&client, &tbl).await;

    // The integer-literal and string-literal forms of the same value hash
    // differently because the router hashes the serialized literal text.
    let int_bucket = (xxh3_64(b"1") as usize) % SHARD_COUNT;
    let str_bucket = (xxh3_64(b"'1'") as usize) % SHARD_COUNT;
    assert_ne!(
        int_bucket, str_bucket,
        "integer literal 1 and string literal '1' are expected to hash to different buckets"
    );

    // A point lookup with the matching integer literal form returns the row.
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("v1"));

    drop_table(&client, &tbl).await;
}

// A single simple-query message carrying multiple statements: the handler loops
// over every parsed statement, so all of them execute.
#[tokio::test]
async fn test_multi_statement_simple_query() {
    let client = ready_client().await;

    // Pick three ids that span more than one shard so the batch is not trivially
    // single-shard.
    let buckets: HashSet<u64> = [1i64, 2, 3].iter().map(|&i| bucket_of(i)).collect();
    assert!(buckets.len() > 1, "test ids should span multiple shards");

    let tbl = create_table(
        &client,
        "sr_multi",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Three statements in one simple-query string.
    client
        .simple_query(&format!(
            "INSERT INTO {tbl} (id, v) VALUES (1, 'a'); \
             INSERT INTO {tbl} (id, v) VALUES (2, 'b'); \
             INSERT INTO {tbl} (id, v) VALUES (3, 'c')"
        ))
        .await
        .expect("multi-statement simple query should execute every statement");

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![1, 2, 3], "all three inserts should have applied");

    drop_table(&client, &tbl).await;
}
