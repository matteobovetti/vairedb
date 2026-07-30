mod common;
use common::*;

// Statements the coordinator does NOT route (anything that isn't
// SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE / ALTER TABLE)
// used to fall through the dispatch catch-all and return a fake `OK` with NO
// execution. A client therefore believed a transaction opened, a table was
// truncated, or a view was created when nothing happened.
//
// VaireDB now rejects each of these with `FeatureNotSupported`: SQLSTATE `0A000`
// and the `[VDB-1004]` marker, with a message naming the command. These tests
// assert that contract so a client gets an explicit "not implemented" error
// instead of a misleading success.
//
// Cleanup safety: tests that need a base table use `create_table` + a trailing
// `drop_table`; the unsupported statement only errors and touches nothing, so
// none of them can leave uncleanable catalog/shard state in the shared cluster.

/// Assert that `sql` fails with the not-supported contract (SQLSTATE `0A000`
/// FeatureNotSupported, message carrying the `[VDB-1004]` marker).
async fn assert_unsupported(client: &tokio_postgres::Client, sql: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        "0A000",
        "`{sql}` should carry SQLSTATE 0A000 (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("[VDB-1004]"),
        "`{sql}` message should carry the FeatureNotSupported VDB code: {}",
        err.message()
    );
}

// BEGIN / COMMIT / ROLLBACK are not implemented: DML is not transactional, so
// the coordinator rejects transaction-control statements rather than silently
// accepting them.
#[tokio::test]
async fn test_transaction_control_is_unsupported() {
    let client = ready_client().await;
    assert_unsupported(&client, "BEGIN").await;
    assert_unsupported(&client, "COMMIT").await;
    assert_unsupported(&client, "ROLLBACK").await;
}

// SET a runtime parameter and SHOW it: runtime parameters are not tracked, so
// both must report the feature as unsupported.
#[tokio::test]
async fn test_set_show_is_unsupported() {
    let client = ready_client().await;
    assert_unsupported(&client, "SET search_path TO myschema").await;
    assert_unsupported(&client, "SHOW search_path").await;
}

// TRUNCATE is not implemented: it must fail rather than return a fake OK while
// leaving rows in place.
#[tokio::test]
async fn test_truncate_is_unsupported() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "us_truncate",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    assert_unsupported(&client, &format!("TRUNCATE TABLE {tbl}")).await;

    drop_table(&client, &tbl).await;
}

// CREATE VIEW is not implemented: it must fail rather than return a fake OK for
// a view that does not exist.
#[tokio::test]
async fn test_create_view_is_unsupported() {
    let client = ready_client().await;
    let view = unique_table_name("us_view");
    let tbl = create_table(
        &client,
        "us_viewbase",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    assert_unsupported(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl} WHERE id = 1"),
    )
    .await;

    drop_table(&client, &tbl).await;
}

// EXPLAIN is not implemented: it must fail rather than return a fake OK with no
// plan rows.
#[tokio::test]
async fn test_explain_is_unsupported() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "us_explain",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    assert_unsupported(&client, &format!("EXPLAIN SELECT * FROM {tbl}")).await;

    drop_table(&client, &tbl).await;
}
