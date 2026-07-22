mod common;
use common::*;
use tokio_postgres::Client;

// Fault tolerance for the shard-replica path: replication convergence under
// node outages PLUS multi-shard DML and DDL behavior when a node fails
// mid-operation. The node-stopping xfails at the end of this file document known
// v0.1 gaps (non-atomic multi-shard DML, best-effort DDL rollback) and live here
// — rather than next to their happy-path siblings — because they share the
// `restore_node` helper and the single-threaded stop/start cleanup discipline.
//
// With rf=3 and shards=3 the quorum is floor(3/2)+1 = 2, so every write acks on
// the primary + one replica synchronously and leaves the THIRD replica to be
// filled asynchronously by the tail-replication retry loop (replication.rs
// spawn_retry_loop), made idempotent by the core-side write_id dedup cache. That
// loop is the system's only convergence mechanism — there is no read-repair and
// no missed-write replay on rejoin.
//
// Reads go only through the coordinator pgwire on 5432; individual replicas are
// not addressable. The affinity policy routes a shard's reads to primary OR any
// replica, so a lagging replica is detected statistically: poll_until_consistent
// fires many reads so a stale replica is hit with high probability.
//
// The fault-injection tests stop/start core containers and are slower than the
// rest of the suite: a stopped node only flips to DEAD after the coordinator's
// 15s heartbeat timeout. Each test MUST restart any node it stopped and wait for
// it to return to ALIVE before finishing, or it poisons the single-threaded run.

/// Restart a core (if stopped) and block until it is ALIVE again. Safe to call
/// even when the node is already running.
async fn restore_node(client: &Client, node_id: &str) {
    start_core(node_id);
    wait_for_node_state(client, node_id, "ALIVE", NODE_DOWN_WAIT).await;
    // Re-confirm full cluster health so subsequent tests start from a clean slate.
    wait_for_cluster_ready(client).await;
}

#[tokio::test]
async fn test_tail_replication_converges_healthy() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ec_healthy",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Insert rows spanning all buckets, one statement each. Every write leaves one
    // replica to be filled by tail replication.
    let ids: Vec<i64> = (1..=15).collect();
    for id in &ids {
        let affected = execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'w')"),
        )
        .await
        .unwrap();
        assert_eq!(affected, 1);
    }

    // On a healthy cluster the tail-replication loop must fill every async replica
    // so that all reads — whichever replica the affinity policy picks — are complete.
    let converged = poll_until_consistent(&client, &tbl, &ids, NODE_DOWN_WAIT).await;
    assert!(
        converged,
        "tail replication did not converge on a healthy cluster within {NODE_DOWN_WAIT:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_clean_restart_preserves_data() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ec_restart",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let ids: Vec<i64> = (1..=12).collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'w')"),
        )
        .await
        .unwrap();
    }
    // Let the async replicas catch up before the restart so the assertion targets
    // persistence across stop/start, not initial replication lag.
    assert!(
        poll_until_consistent(&client, &tbl, &ids, NODE_DOWN_WAIT).await,
        "cluster did not reach a consistent state before the restart"
    );

    // Cleanly restart a node with no writes during the brief downtime. The
    // container filesystem (and its DuckDB shards) survives stop/start.
    let node = "core-3";
    stop_core(node);
    start_core(node);
    wait_for_node_state(&client, node, "ALIVE", NODE_DOWN_WAIT).await;
    wait_for_cluster_ready(&client).await;

    // The restarted node must serve complete, correct data once it rejoins.
    let converged = poll_until_consistent(&client, &tbl, &ids, NODE_DOWN_WAIT).await;
    assert!(
        converged,
        "restarted node did not serve complete data within {NODE_DOWN_WAIT:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_brief_replica_outage_converges_via_tail_retry() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ec_brief_outage",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Target a shard and one of its replicas (not the primary).
    let shards = fetch_shards(&client, &tbl).await;
    let (bucket, _primary, replicas) = shards
        .iter()
        .find(|(_, _, reps)| !reps.is_empty())
        .expect("expected a shard with at least one replica");
    let replica = replicas[0].clone();
    let bucket = *bucket as u64;

    // First write while everyone is up. Besides seeding a baseline row, this opens
    // and caches the coordinator->replica gRPC channel, so the next write's miss is
    // recorded as an app-level error (enqueued for tail retry) rather than a bare
    // connection failure that is never retried.
    let id1 = id_for_bucket(bucket, 1);
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id1}, 'before')"),
    )
    .await
    .unwrap();

    // Stop the replica but do NOT wait for the 15s dead threshold: while it is
    // merely unreachable (still ALIVE in the catalog), its missed write stays
    // queued for tail retry. Once it would be marked DEAD the retry is dropped.
    stop_core(&replica);

    // Quorum (primary + the other replica) still satisfies the write; the stopped
    // replica misses it and is enqueued for tail replication.
    let id2 = id_for_bucket(bucket, id1 + 1);
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id2}, 'during')"),
    )
    .await
    .expect("write should succeed via quorum despite a replica being unreachable");
    assert_eq!(affected, 1);

    // Bring the replica back promptly — well within the 15s dead threshold — so the
    // pending retry survives and the loop replays the missed write to it.
    start_core(&replica);
    wait_for_node_state(&client, &replica, "ALIVE", NODE_DOWN_WAIT).await;
    wait_for_cluster_ready(&client).await;

    // The recovered replica must converge: every read sees both rows.
    let converged = poll_until_consistent(&client, &tbl, &[id1, id2], NODE_DOWN_WAIT).await;
    assert!(
        converged,
        "replica did not converge via tail retry after a brief outage within {NODE_DOWN_WAIT:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_node_marked_dead_and_alive_again() {
    let client = ready_client().await;

    let node = "core-5";
    stop_core(node);
    // Catalog flips the node out of ALIVE after the heartbeat timeout.
    wait_for_node_state(&client, node, "DEAD", NODE_DOWN_WAIT).await;
    assert_eq!(
        alive_node_count(&client).await,
        EXPECTED_NODES - 1,
        "one node should have left the ALIVE set"
    );

    restore_node(&client, node).await;
    assert_eq!(
        alive_node_count(&client).await,
        EXPECTED_NODES,
        "node should rejoin the ALIVE set after restart"
    );
}

#[tokio::test]
async fn test_read_with_one_node_down() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ft_read_down",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, v) VALUES \
             (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g')"
        ),
    )
    .await
    .unwrap();

    // Stop a pure replica (core-5 is replica of bucket 2, primary of none of the
    // buckets we read here) so every shard still has a reachable copy.
    let node = "core-5";
    stop_core(node);
    wait_for_node_state(&client, node, "DEAD", NODE_DOWN_WAIT).await;

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("7"),
        "full scan must return all rows from surviving replicas"
    );

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5, 6, 7]);

    restore_node(&client, node).await;
    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_write_survives_one_replica_down() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ft_write_replica",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Target a shard and stop one of its replicas (not the primary). With rf=3 the
    // quorum is 2, so primary + one surviving replica still satisfies the write.
    let shards = fetch_shards(&client, &tbl).await;
    let (bucket, _primary, replicas) = shards
        .iter()
        .find(|(_, _, reps)| !reps.is_empty())
        .expect("expected a shard with at least one replica");
    let replica = replicas[0].clone();
    let bucket = *bucket as u64;

    let id1 = id_for_bucket(bucket, 1);
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id1}, 'before')"),
    )
    .await
    .unwrap();

    stop_core(&replica);
    wait_for_node_state(&client, &replica, "DEAD", NODE_DOWN_WAIT).await;

    // Writes routed to this shard must still succeed via quorum.
    let id2 = id_for_bucket(bucket, id1 + 1);
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id2}, 'during')"),
    )
    .await
    .expect("write should succeed with quorum despite a replica being down");
    assert_eq!(affected, 1);

    // While the replica is down, both rows are readable: reads are served from
    // the surviving copies and the dead node is skipped.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("2"));

    // Restore the replica BEFORE the convergence assertion. The assertion below
    // is allowed to fail (this is an #[ignore] xfail), and a panic would skip the
    // trailing DROP/restore — so the node must already be back ALIVE here.
    restore_node(&client, &replica).await;

    // CORRECT BEHAVIOR (currently broken): once the replica is back, every read
    // must see both rows regardless of which replica the affinity policy picks.
    // Poll for a run of consecutive complete reads so a single lucky read of a
    // fresh replica cannot mask a still-stale one. Today the recovered replica
    // never receives the write made while it was down, so stale 1-row reads keep
    // recurring and this times out — the intended xfail failure under --ignored.
    let converged = poll_until_consistent(&client, &tbl, &[id1, id2], NODE_DOWN_WAIT).await;
    assert!(
        converged,
        "recovered replica did not converge: reads kept returning stale data within {NODE_DOWN_WAIT:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_primary_down_write_fails() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ft_primary_down",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Stop the primary of a target shard. The replication path requires the
    // primary to ack, so a write routed to this shard must fail even though
    // replicas are alive.
    let shards = fetch_shards(&client, &tbl).await;
    let (bucket, primary, _replicas) = shards.first().expect("table should have shards").clone();

    stop_core(&primary);
    wait_for_node_state(&client, &primary, "DEAD", NODE_DOWN_WAIT).await;

    let id = id_for_bucket(bucket as u64, 1);
    let result = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'x')"),
    )
    .await;
    assert!(
        result.is_err(),
        "write to a shard whose primary is down must fail (primary-must-ack)"
    );

    // After the primary recovers, writes routed to the shard succeed again.
    restore_node(&client, &primary).await;
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'x')"),
    )
    .await
    .expect("write should succeed once the primary is back");
    assert_eq!(affected, 1);

    drop_table(&client, &tbl).await;
}

// ---------------------------------------------------------------------------
// Multi-shard DML / DDL fault tolerance.
//
// A stopped core stays ALIVE in the catalog until the 15s heartbeat timeout, so
// stopping a node WITHOUT waiting for DEAD gives a stable window in which it is
// still a routing/broadcast target but unreachable — exactly the mid-operation
// failure these tests need. Each test restores the node before any panicking
// assertion (single-threaded run discipline).
// ---------------------------------------------------------------------------

// A multi-shard statement that fails on one shard must be atomic: either every
// shard applies it or none do. Today handle_dml loops over target shards calling
// execute_write_with_quorum sequentially with no cross-shard rollback, so the
// shards processed before the failing one keep their mutation. We stop the
// primary of the LAST shard (processed last) so the earlier shards commit their
// UPDATE and only the final shard's write fails — leaving a partially-applied
// statement.
#[tokio::test]
#[ignore = "known gap: multi-shard DML is non-atomic; a statement that fails on one shard leaves the other shards mutated (no cross-shard rollback)"]
async fn test_multi_shard_dml_partial_failure_is_atomic() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ft_ms_dml",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // One row per bucket so a no-WHERE UPDATE fans out to every shard.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'before')"),
        )
        .await
        .unwrap();
    }

    // Stop the primary of the highest bucket (processed last by handle_dml). The
    // earlier shards commit 'after'; this shard's write fails (primary-must-ack).
    let shards = fetch_shards(&client, &tbl).await;
    let (_bucket, last_primary, _) = shards.last().expect("table has shards").clone();
    stop_core(&last_primary);

    let result = execute(&client, &format!("UPDATE {tbl} SET v = 'after'")).await;
    assert!(
        result.is_err(),
        "UPDATE must fail because one shard's primary is down"
    );

    // Restore BEFORE the atomicity assertion so a panic cannot skip cleanup.
    restore_node(&client, &last_primary).await;

    // CORRECT BEHAVIOR (currently broken): a failed UPDATE leaves no row mutated.
    // Today the earlier shards already wrote 'after', so this count is > 0 and the
    // assertion fails — the intended xfail under --ignored.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT COUNT(*) FROM {tbl} WHERE v = 'after'"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("0"),
        "a failed multi-shard UPDATE must not leave any shard mutated"
    );

    drop_table(&client, &tbl).await;
}

// A failed CREATE TABLE must clean up after itself. handle_create_table writes
// the catalog, then broadcasts the per-shard DDL with best-effort rollback (DROP
// on already-succeeded nodes + catalog delete) if any node send fails. This
// PASSING regression guards that path: with one node unreachable mid-broadcast
// the CREATE fails AND leaves no catalog state, and the name is reusable once the
// node is back.
#[tokio::test]
async fn test_create_table_cleans_up_on_node_failure() {
    let client = ready_client().await;
    let tbl = unique_table_name("ft_create_rollback");
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();

    // Node is still ALIVE (no DEAD wait) so it remains a broadcast target, but it
    // is unreachable, so at least one per-shard DDL send fails.
    let node = "core-1";
    stop_core(node);

    let result = execute(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    assert!(
        result.is_err(),
        "CREATE TABLE must fail when a target node is unreachable"
    );

    // Rollback must have removed the half-created table from the catalog.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT table_name FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        0,
        "failed CREATE must leave no table in the catalog, found {rows:?}"
    );

    restore_node(&client, node).await;

    // The name must be cleanly reusable once the cluster is healthy again. A node
    // reports ALIVE (heartbeat) before its WriteService gRPC endpoint is bound, and
    // the coordinator may briefly hold a stale cached channel to the restarted node,
    // so the first retry can still hit "DDL broadcast failed". Bounded-retry the
    // CREATE until the write path is actually ready.
    let create_sql = format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}");
    let deadline = tokio::time::Instant::now() + NODE_DOWN_WAIT;
    loop {
        match execute(&client, &create_sql).await {
            Ok(_) => break,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("CREATE did not succeed after recovery within {NODE_DOWN_WAIT:?}: {e}");
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }

    drop_table(&client, &tbl).await;
}

// ALTER TABLE must not change the catalog if the change cannot be applied to the
// cluster. handle_alter_table mutates the catalog (put_table with the new column)
// BEFORE broadcasting the DDL, then returns an error if any node is unreachable —
// leaving the catalog claiming a column that some shards never got.
#[tokio::test]
#[ignore = "known gap: ALTER mutates the catalog before broadcasting; a node failure during broadcast leaves the catalog reporting a column the cluster does not have"]
async fn test_alter_table_atomic_across_nodes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ft_alter_atomic",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Stop a broadcast target but keep it ALIVE so the ALTER reaches the failure
    // path after the catalog has already been mutated.
    let node = "core-1";
    stop_core(node);

    let result = execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD COLUMN extra INTEGER"),
    )
    .await;
    assert!(
        result.is_err(),
        "ALTER must fail when a target node is unreachable"
    );

    restore_node(&client, node).await;

    // CORRECT BEHAVIOR (currently broken): a failed ALTER must not have changed
    // the catalog. Today the column was persisted before the broadcast, so it is
    // present here and the assertion fails — the intended xfail under --ignored.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT column_name FROM vairedb_catalog.columns \
             WHERE table_name = '{tbl}' AND column_name = 'extra'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        0,
        "a failed ALTER must not leave the new column in the catalog"
    );

    drop_table(&client, &tbl).await;
}
