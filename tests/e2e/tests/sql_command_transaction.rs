mod common;
use common::*;
use tokio_postgres::Client;

// Row 33 of docs/specs/gap-analysis-command.md — transaction control
// (BEGIN / START TRANSACTION / COMMIT / ROLLBACK / SAVEPOINT / RELEASE).
//
//     cd tests/e2e && cargo test --test sql_command_transaction -- --test-threads=1
//
// VaireDB has no cross-shard commit protocol, so a block is NOT held open on the
// core nodes: the coordinator buffers the block's writes per connection and ships
// them at COMMIT, as one atomic node-local transaction per node set. What that
// buys and what it costs is exactly what this file pins down:
//
//   * COMMIT applies the block; ROLLBACK is a real rollback (nothing was sent);
//     savepoints truncate the buffer.
//   * A block confined to one node set commits atomically. One spanning several
//     is refused at COMMIT, before anything is written.
//   * A statement is only allowed inside a block if the coordinator can answer it
//     truthfully without running it. UPDATE/DELETE (row count unknowable), a read
//     of a table the block has written (it would miss the buffer), and DDL (not
//     buffered at all) are refused by name rather than answered with a guess.
//
// Every rejection here is a deliberate semantic, not an unimplemented statement,
// so each test asserts the SQLSTATE a driver branches on.

/// `in_failed_sql_transaction` — a statement was issued after an earlier one
/// failed inside the block. Drivers read this as "only ROLLBACK will help".
const SQLSTATE_IN_FAILED_TRANSACTION: &str = "25P02";
/// `no_active_sql_transaction` — a savepoint statement outside a block.
const SQLSTATE_NO_ACTIVE_TRANSACTION: &str = "25P01";
/// `read_only_sql_transaction` — a write inside a `BEGIN READ ONLY` block.
const SQLSTATE_READ_ONLY_TRANSACTION: &str = "25006";
/// `invalid_savepoint_specification` — the savepoint was never set, or is gone.
const SQLSTATE_INVALID_SAVEPOINT: &str = "3B001";
/// `undefined_table`, the error used here to fail a statement on purpose.
const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";

/// A table for a transaction test, plus `n` ids that all hash to one bucket —
/// i.e. writes that stay inside a single shard group and can therefore commit
/// atomically.
async fn table_and_ids(client: &Client, prefix: &str, n: usize) -> (String, Vec<i64>) {
    let tbl = create_table(
        client,
        prefix,
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    (tbl, ids_in_bucket(0, n, 1))
}

async fn insert(
    client: &Client,
    tbl: &str,
    id: i64,
    v: &str,
) -> Result<u64, tokio_postgres::Error> {
    execute(
        client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, '{v}')"),
    )
    .await
}

/// Assert `sql` fails with `sqlstate`, and that the message says why.
async fn assert_fails_with(client: &Client, sql: &str, sqlstate: &str, needle: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        sqlstate,
        "`{sql}` should carry SQLSTATE {sqlstate} (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains(needle),
        "`{sql}` should explain itself with `{needle}`, got: {}",
        err.message()
    );
}

/// The nodes a bucket's shard lives on: primary plus replicas, sorted. Two
/// buckets with different sets cannot be committed in one node-local
/// transaction, which is what makes a cross-shard COMMIT refusable.
fn node_set(shard: &ShardRow) -> Vec<String> {
    let mut nodes = vec![shard.1.clone()];
    nodes.extend(shard.2.iter().cloned());
    nodes.sort();
    nodes
}

/// Two buckets of `tbl` that live on different node sets.
async fn buckets_on_different_node_sets(client: &Client, tbl: &str) -> (u64, u64) {
    let shards = fetch_shards(client, tbl).await;
    for (i, left) in shards.iter().enumerate() {
        for right in &shards[i + 1..] {
            if node_set(left) != node_set(right) {
                return (left.0 as u64, right.0 as u64);
            }
        }
    }
    panic!("expected two shards on different node sets, got: {shards:?}");
}

// ============================================================================
// COMMIT and ROLLBACK
// ============================================================================

#[tokio::test]
async fn test_commit_persists_writes() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_commit", 2).await;

    execute(&client, "BEGIN").await.unwrap();
    // Both rows are buffered in the coordinator and reported now; the count is
    // exact because an INSERT ... VALUES carries its row count in the statement.
    assert_eq!(insert(&client, &tbl, ids[0], "a").await.unwrap(), 1);
    assert_eq!(insert(&client, &tbl, ids[1], "b").await.unwrap(), 1);
    execute(&client, "COMMIT").await.unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        2,
        "a committed block must apply every one of its writes"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_rollback_discards_writes() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_rollback", 1).await;

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "ROLLBACK").await.unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "a rolled-back INSERT must leave no row behind"
    );

    // The connection is usable again straight away.
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// A buffered write is invisible to everyone — including the connection that made
// it, which is why reading the table is refused — until COMMIT ships it. Another
// session is the honest way to observe that.
#[tokio::test]
async fn test_writes_are_invisible_to_another_session_until_commit() {
    let writer = ready_client().await;
    let reader = ready_client().await;
    let (tbl, ids) = table_and_ids(&writer, "tx_isolation", 1).await;

    execute(&writer, "BEGIN").await.unwrap();
    insert(&writer, &tbl, ids[0], "a").await.unwrap();
    assert_eq!(
        row_count(&reader, &tbl).await,
        0,
        "an uncommitted write must not be visible to another session"
    );

    execute(&writer, "COMMIT").await.unwrap();
    assert_eq!(
        row_count(&reader, &tbl).await,
        1,
        "COMMIT must make the write visible"
    );

    drop_table(&writer, &tbl).await;
}

// `START TRANSACTION`/`END` and `ABORT` are the same two commands under other
// names; a driver may send any of them.
#[tokio::test]
async fn test_alternate_spellings_commit_and_abort() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_spelling", 2).await;

    execute(&client, "START TRANSACTION").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "END").await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1, "END must commit");

    execute(&client, "BEGIN TRANSACTION").await.unwrap();
    insert(&client, &tbl, ids[1], "b").await.unwrap();
    execute(&client, "ABORT").await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1, "ABORT must roll back");

    drop_table(&client, &tbl).await;
}

// PostgreSQL accepts both outside a block (with a warning). Failing them would
// break clients that end a block defensively.
#[tokio::test]
async fn test_commit_and_rollback_outside_a_block_are_accepted() {
    let client = ready_client().await;
    execute(&client, "COMMIT").await.unwrap();
    execute(&client, "ROLLBACK").await.unwrap();
    // And the session still works.
    assert!(simple_query_rows(&client, "SELECT 1").await.is_ok());
}

// The whole point of the atomic batch: two writes to one node set either both
// land or neither does, and a driver's own transaction API must drive it.
#[tokio::test]
async fn test_driver_transaction_over_the_extended_protocol() {
    let mut client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_driver", 2).await;
    let (kept, dropped) = (ids[0] as i32, ids[1] as i32);

    let txn = client.transaction().await.unwrap();
    let affected = txn
        .execute(
            &format!("INSERT INTO {tbl} (id, v) VALUES ($1, $2)"),
            &[&kept, &"a"],
        )
        .await
        .expect("a bound INSERT must be accepted inside a transaction");
    assert_eq!(affected, 1);
    txn.commit().await.unwrap();

    // A transaction the driver drops without committing rolls back.
    let txn = client.transaction().await.unwrap();
    txn.execute(
        &format!("INSERT INTO {tbl} (id, v) VALUES ($1, $2)"),
        &[&dropped, &"b"],
    )
    .await
    .unwrap();
    drop(txn);

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "only the committed row survives");
    assert_eq!(rows[0][0].as_deref(), Some(kept.to_string().as_str()));

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Savepoints
// ============================================================================

#[tokio::test]
async fn test_savepoint_rolls_back_part_of_the_block() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_sp", 2).await;

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "keep").await.unwrap();
    execute(&client, "SAVEPOINT sp1").await.unwrap();
    insert(&client, &tbl, ids[1], "drop").await.unwrap();
    execute(&client, "ROLLBACK TO SAVEPOINT sp1").await.unwrap();
    execute(&client, "COMMIT").await.unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the write after the savepoint must be gone");
    assert_eq!(rows[0][0].as_deref(), Some("keep"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_release_savepoint_keeps_its_writes() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_release", 1).await;

    execute(&client, "BEGIN").await.unwrap();
    execute(&client, "SAVEPOINT sp1").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "RELEASE SAVEPOINT sp1").await.unwrap();
    // The savepoint is gone, so rolling back to it is an error — but its write
    // belongs to the block now.
    assert_fails_with(
        &client,
        "ROLLBACK TO SAVEPOINT sp1",
        SQLSTATE_INVALID_SAVEPOINT,
        "sp1",
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();

    // Re-run without the failed rollback to prove the released write commits.
    execute(&client, "BEGIN").await.unwrap();
    execute(&client, "SAVEPOINT sp1").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "RELEASE SAVEPOINT sp1").await.unwrap();
    execute(&client, "COMMIT").await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_savepoint_statements_outside_a_block_are_refused() {
    let client = ready_client().await;
    for sql in [
        "SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "ROLLBACK TO SAVEPOINT sp1",
    ] {
        assert_fails_with(
            &client,
            sql,
            SQLSTATE_NO_ACTIVE_TRANSACTION,
            "transaction block",
        )
        .await;
    }
}

#[tokio::test]
async fn test_unknown_savepoint_is_named_in_the_error() {
    let client = ready_client().await;
    execute(&client, "BEGIN").await.unwrap();
    assert_fails_with(
        &client,
        "ROLLBACK TO SAVEPOINT nosuch",
        SQLSTATE_INVALID_SAVEPOINT,
        "nosuch",
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();
}

// ============================================================================
// A failed block
// ============================================================================

#[tokio::test]
async fn test_a_failed_statement_aborts_the_block() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_failed", 1).await;
    let missing = unique_table_name("tx_failed_missing");

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    assert_fails_with(
        &client,
        &format!("INSERT INTO {missing} (id) VALUES (1)"),
        SQLSTATE_UNDEFINED_TABLE,
        &missing,
    )
    .await;

    // From here every statement is refused until the block ends.
    for sql in [
        format!("SELECT COUNT(*) FROM {tbl}"),
        format!("INSERT INTO {tbl} (id, v) VALUES ({}, 'b')", ids[0]),
    ] {
        assert_fails_with(
            &client,
            &sql,
            SQLSTATE_IN_FAILED_TRANSACTION,
            "current transaction is aborted",
        )
        .await;
    }

    // COMMIT of a failed block succeeds as a rollback, and the block's first
    // write — which had been accepted — must not have been applied.
    execute(&client, "COMMIT").await.unwrap();
    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "an aborted block must apply nothing, not even its accepted writes"
    );

    drop_table(&client, &tbl).await;
}

// Rolling back to a savepoint is how a client recovers from an error without
// losing the whole block.
#[tokio::test]
async fn test_a_savepoint_recovers_a_failed_block() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_recover", 2).await;
    let missing = unique_table_name("tx_recover_missing");

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "SAVEPOINT sp1").await.unwrap();
    execute_expect_err(&client, &format!("INSERT INTO {missing} (id) VALUES (1)")).await;

    execute(&client, "ROLLBACK TO SAVEPOINT sp1").await.unwrap();
    insert(&client, &tbl, ids[1], "b").await.unwrap();
    execute(&client, "COMMIT").await.unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        2,
        "the block survived the error and committed both writes"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// What a block refuses, and why
// ============================================================================

// The block's writes sit in the coordinator, so this read would silently miss
// them. Answering it would be worse than refusing it.
#[tokio::test]
async fn test_reading_a_written_table_inside_the_block_is_refused() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_read", 1).await;
    let (other, _) = table_and_ids(&client, "tx_read_other", 1).await;

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    assert_fails_with(
        &client,
        &format!("SELECT COUNT(*) FROM {tbl}"),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "COMMIT",
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();

    // A table the block has not written stays readable inside it.
    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    assert_eq!(
        row_count(&client, &other).await,
        0,
        "an untouched table is still readable inside a block"
    );
    execute(&client, "ROLLBACK").await.unwrap();

    drop_table(&client, &other).await;
    drop_table(&client, &tbl).await;
}

// How many rows an UPDATE or DELETE affects is only known once the shards run
// it, and a buffered statement has not run. Rather than report a guess a client
// might act on, the statement is refused with the reason.
#[tokio::test]
async fn test_update_and_delete_are_refused_inside_a_block() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_upd", 1).await;
    insert(&client, &tbl, ids[0], "a").await.unwrap();

    for (sql, command) in [
        (
            format!("UPDATE {tbl} SET v = 'b' WHERE id = {}", ids[0]),
            "UPDATE",
        ),
        (format!("DELETE FROM {tbl} WHERE id = {}", ids[0]), "DELETE"),
    ] {
        execute(&client, "BEGIN").await.unwrap();
        assert_fails_with(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED, command).await;
        execute(&client, "ROLLBACK").await.unwrap();
    }

    // Both still work outside a block, which is what the error tells the client.
    execute(
        &client,
        &format!("UPDATE {tbl} SET v = 'b' WHERE id = {}", ids[0]),
    )
    .await
    .unwrap();
    execute(&client, &format!("DELETE FROM {tbl} WHERE id = {}", ids[0]))
        .await
        .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}

// DDL updates the coordinator catalog and every shard immediately, so COMMIT
// could not roll it back.
#[tokio::test]
async fn test_ddl_is_refused_inside_a_block() {
    let client = ready_client().await;
    let (tbl, _) = table_and_ids(&client, "tx_ddl", 1).await;
    let other = unique_table_name("tx_ddl_new");
    let idx = unique_table_name("tx_ddl_idx");

    for sql in [
        format!("CREATE TABLE {other} (id INTEGER NOT NULL) {CREATE_OPTS}"),
        format!("ALTER TABLE {tbl} ADD COLUMN extra INTEGER"),
        format!("TRUNCATE TABLE {tbl}"),
        format!("CREATE INDEX {idx} ON {tbl} (id)"),
        format!("DROP INDEX {idx}"),
        format!("DROP TABLE {tbl}"),
    ] {
        execute(&client, "BEGIN").await.unwrap();
        assert_fails_with(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED, "DDL").await;
        execute(&client, "ROLLBACK").await.unwrap();
    }

    // Nothing was applied: the table is still there, unaltered.
    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}

// `READ ONLY` is honored by refusing the block's writes, not by ignoring the
// mode and writing anyway.
#[tokio::test]
async fn test_read_only_block_refuses_writes() {
    let client = ready_client().await;
    let (tbl, ids) = table_and_ids(&client, "tx_readonly", 1).await;

    execute(&client, "BEGIN READ ONLY").await.unwrap();
    // Reads are fine.
    assert_eq!(row_count(&client, &tbl).await, 0);
    assert_fails_with(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({}, 'a')", ids[0]),
        SQLSTATE_READ_ONLY_TRANSACTION,
        "read-only transaction",
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();

    // The mode belongs to the block, not to the session.
    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, ids[0], "a").await.unwrap();
    execute(&client, "COMMIT").await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// An anonymization secret lives in the coordinator catalog rather than on a
// shard, so no ROLLBACK could undo it. Buffering it would be a lie either way.
#[tokio::test]
async fn test_writing_an_anonymization_secret_inside_a_block_is_refused() {
    let client = ready_client().await;
    execute(&client, "BEGIN").await.unwrap();
    assert_fails_with(
        &client,
        "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
         VALUES ('tx_secret', 'HMAC-SHA256', 'k')",
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "transaction",
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();
}

// ============================================================================
// Atomicity across shard groups
// ============================================================================

// Grouping is by node set, not by table: two tables whose shards happen to live
// on the same nodes commit as one node-local transaction. Whether such a pair
// exists depends on how the scheduler placed the two tables, so the test looks
// the layout up and asserts the outcome that layout demands — both branches pin
// all-or-nothing behavior.
#[tokio::test]
async fn test_a_block_writing_two_tables_is_atomic_either_way() {
    let client = ready_client().await;
    let (left, _) = table_and_ids(&client, "tx_group_a", 1).await;
    let (right, _) = table_and_ids(&client, "tx_group_b", 1).await;

    let left_shards = fetch_shards(&client, &left).await;
    let right_shards = fetch_shards(&client, &right).await;
    let shared = left_shards.iter().find_map(|l| {
        right_shards
            .iter()
            .find(|r| node_set(r) == node_set(l))
            .map(|r| (l.0 as u64, r.0 as u64))
    });

    match shared {
        // One node set: the block is genuinely atomic and must commit.
        Some((left_bucket, right_bucket)) => {
            execute(&client, "BEGIN").await.unwrap();
            insert(&client, &left, id_for_bucket(left_bucket, 1), "a")
                .await
                .unwrap();
            insert(&client, &right, id_for_bucket(right_bucket, 1), "b")
                .await
                .unwrap();
            execute(&client, "COMMIT").await.unwrap();
            assert_eq!(row_count(&client, &left).await, 1);
            assert_eq!(row_count(&client, &right).await, 1);
        }
        // No shared node set: every pairing spans groups, so the COMMIT must be
        // refused and nothing written.
        None => {
            execute(&client, "BEGIN").await.unwrap();
            insert(
                &client,
                &left,
                id_for_bucket(left_shards[0].0 as u64, 1),
                "a",
            )
            .await
            .unwrap();
            insert(
                &client,
                &right,
                id_for_bucket(right_shards[0].0 as u64, 1),
                "b",
            )
            .await
            .unwrap();
            assert_fails_with(
                &client,
                "COMMIT",
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "shard groups",
            )
            .await;
            assert_eq!(row_count(&client, &left).await, 0);
            assert_eq!(row_count(&client, &right).await, 0);
        }
    }

    drop_table(&client, &right).await;
    drop_table(&client, &left).await;
}

// Two node sets cannot be committed atomically, and the refusal happens before
// anything is sent — so the client is told nothing was written, and that is true.
// `allow_cross_shard_transactions` in the coordinator config opts into the
// non-atomic alternative; the e2e cluster deliberately leaves it off.
#[tokio::test]
async fn test_commit_spanning_node_sets_is_refused_and_writes_nothing() {
    let client = ready_client().await;
    let (tbl, _) = table_and_ids(&client, "tx_cross", 1).await;
    let (left, right) = buckets_on_different_node_sets(&client, &tbl).await;

    execute(&client, "BEGIN").await.unwrap();
    insert(&client, &tbl, id_for_bucket(left, 1), "a")
        .await
        .unwrap();
    insert(&client, &tbl, id_for_bucket(right, 1), "b")
        .await
        .unwrap();

    assert_fails_with(
        &client,
        "COMMIT",
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "allow_cross_shard_transactions",
    )
    .await;
    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "a refused COMMIT must write nothing at all"
    );

    // The block ended with the failed COMMIT, so the session is idle again.
    insert(&client, &tbl, id_for_bucket(left, 1), "a")
        .await
        .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}
