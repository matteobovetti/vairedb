mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::future::join_all;

// The suite runs `--test-threads=1`, so tests are serialized at the process
// level. Concurrency is exercised *within* a single test by opening several
// `connect()` clients and driving them with `join_all` / `tokio::join!`.
//
// One test (`test_concurrent_create_table_exactly_one_wins`) asserts the
// DESIRED behavior of a known bug and is `#[ignore]`d per the xfail convention
// (see shard_key_hazards.rs); run it on demand with `cargo test -- --ignored`.

#[tokio::test]
async fn test_concurrent_writers_same_shard_no_lost_updates() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cc_same_shard",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // All ids hash to the same shard, so every write contends on one primary.
    const K: usize = 12;
    let ids = ids_in_bucket(0, K, 1);

    let clients = join_all((0..K).map(|_| connect())).await;
    let inserts = clients.iter().zip(&ids).map(|(c, &id)| {
        let tbl = tbl.clone();
        async move { execute(c, &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'w')")).await }
    });
    let results = join_all(inserts).await;
    for (id, r) in ids.iter().zip(&results) {
        assert_eq!(
            *r.as_ref().unwrap(),
            1,
            "concurrent insert of id {id} should affect exactly one row"
        );
    }

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    let mut want = ids.clone();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "all concurrent rows must be present exactly once"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_concurrent_writers_different_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cc_diff_shard",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Spread ids across all shards: take some from each bucket.
    let mut ids = Vec::new();
    for b in 0..SHARD_COUNT as u64 {
        ids.extend(ids_in_bucket(b, 4, 1));
    }
    let k = ids.len();

    let clients = join_all((0..k).map(|_| connect())).await;
    let inserts = clients.iter().zip(&ids).map(|(c, &id)| {
        let tbl = tbl.clone();
        async move { execute(c, &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'w')")).await }
    });
    let results = join_all(inserts).await;
    for (id, r) in ids.iter().zip(&results) {
        assert_eq!(
            *r.as_ref().unwrap(),
            1,
            "concurrent insert of id {id} should affect exactly one row"
        );
    }

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref().unwrap().parse::<usize>().unwrap(),
        k,
        "every row must land on exactly one shard (no broadcast duplication)"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_concurrent_readers_under_writes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cc_read_write",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    const N: i64 = 40;
    const READERS: usize = 4;
    let done = Arc::new(AtomicBool::new(false));

    let writer_client = connect().await;
    let writer_tbl = tbl.clone();
    let writer_done = done.clone();
    let writer = async move {
        for id in 1..=N {
            execute(
                &writer_client,
                &format!("INSERT INTO {writer_tbl} (id, v) VALUES ({id}, 'w')"),
            )
            .await
            .unwrap();
        }
        writer_done.store(true, Ordering::SeqCst);
    };

    let reader_clients = join_all((0..READERS).map(|_| connect())).await;
    let readers = reader_clients.into_iter().map(|c| {
        let tbl = tbl.clone();
        let done = done.clone();
        async move {
            // Keep polling while the writer is running; readers must never see a
            // count outside [0, N] and must never error mid-write.
            loop {
                let rows = simple_query_rows(&c, &format!("SELECT COUNT(*) FROM {tbl}"))
                    .await
                    .expect("concurrent read should not error");
                let count: i64 = rows[0][0].as_deref().unwrap().parse().unwrap();
                assert!(
                    (0..=N).contains(&count),
                    "observed count {count} outside [0, {N}]"
                );
                if done.load(Ordering::SeqCst) {
                    break;
                }
            }
        }
    });

    let reader_all = join_all(readers);
    tokio::join!(writer, reader_all);

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref().unwrap().parse::<i64>().unwrap(),
        N,
        "all writes should be visible after the writer completes"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_concurrent_inserts_then_consistent_read() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cc_read_your_writes",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    const K: usize = 16;
    let ids: Vec<i64> = (1..=K as i64).collect();

    let clients = join_all((0..K).map(|_| connect())).await;
    let inserts = clients.iter().zip(&ids).map(|(c, &id)| {
        let tbl = tbl.clone();
        async move { execute(c, &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'w')")).await }
    });
    for r in join_all(inserts).await {
        assert_eq!(r.unwrap(), 1);
    }

    // After all quorum writes ack, a fresh read must see every row.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref().unwrap().parse::<usize>().unwrap(),
        K,
        "read-your-writes: all acked inserts must be visible"
    );

    drop_table(&client, &tbl).await;
}

// Strong consistency: quorum write + read-from-primary must return the latest
// value. A single client that UPDATEs a row and immediately reads it back must
// never observe a stale value. Repeating the write/read cycle tightens the odds
// of catching a misrouted read that lands on a lagging replica instead of the
// primary.
#[tokio::test]
async fn test_read_after_update_sees_latest() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cc_read_after_update",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let id = id_for_bucket(0, 1);
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'init')"),
    )
    .await
    .unwrap();

    const ITERS: usize = 20;
    for i in 0..ITERS {
        let want = format!("u{i}");
        execute(
            &client,
            &format!("UPDATE {tbl} SET v = '{want}' WHERE id = {id}"),
        )
        .await
        .unwrap();

        let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = {id}"))
            .await
            .unwrap();
        assert_eq!(
            rows[0][0].as_deref(),
            Some(want.as_str()),
            "read-after-write must return the value just written (iteration {i})"
        );
    }

    drop_table(&client, &tbl).await;
}

// KNOWN BUG: `handle_create_table` checks `get_table()` for an existing table
// and only later commits to the catalog, with no lock/CAS in between. Two
// concurrent `CREATE TABLE <same>` can both pass the existence check and both
// proceed. Correct behavior: exactly one succeeds, the other is rejected with
// TableAlreadyExists, and the catalog holds exactly one table row.
#[tokio::test]
#[ignore = "known bug: concurrent CREATE TABLE has no catalog lock/CAS; both pass the get_table existence check and may both create the table"]
async fn test_concurrent_create_table_exactly_one_wins() {
    let setup = ready_client().await;
    let tbl = unique_table_name("cc_create_race");
    execute(&setup, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();

    let c1 = connect().await;
    let c2 = connect().await;
    let create = format!("CREATE TABLE {tbl} (id INTEGER NOT NULL) {CREATE_OPTS}");
    let (r1, r2) = tokio::join!(execute(&c1, &create), execute(&c2, &create));

    let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
    assert_eq!(
        oks, 1,
        "exactly one concurrent CREATE TABLE should succeed (got {oks})"
    );

    let rows = simple_query_rows(
        &setup,
        &format!("SELECT COUNT(*) FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("1"),
        "catalog must hold exactly one row for the table"
    );

    execute(&setup, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
}
