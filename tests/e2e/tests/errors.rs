mod common;
use common::*;
use std::collections::HashSet;

// Error and boundary paths surfacing to the PG client. Two strictness tiers:
//
//  1. SQLSTATE + `[VDB-NNNN]` enrichment — the richest diagnostics that reach the
//     client through the layered error chain (DuckDB error string -> core
//     CoreError -> coordinator error_enrichment -> PG ErrorInfo). Exact SQLSTATE
//     is asserted only where the mapping is verified stable (42P01 TableNotFound,
//     42P07 TableAlreadyExists). For DuckDB-surfaced runtime errors (type
//     mismatch, NOT NULL, unique) the SQLSTATE is not yet pinned, so we assert the
//     `[VDB-` enrichment marker instead.
//  2. Boundary inputs that must behave deterministically (syntax error, empty-table
//     aggregates, rf == node count, shards = 0, DROP of a missing table, dropping
//     the shard key) — these have no stable SQLSTATE equivalent.

#[tokio::test]
async fn test_table_not_found_sqlstate() {
    let client = ready_client().await;
    let tbl = unique_table_name("ep_missing");

    let err = execute_expect_err(&client, &format!("SELECT * FROM {tbl}")).await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "SELECT from missing table should carry SQLSTATE 42P01 (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("[VDB-1000]"),
        "message should carry the TableNotFound VDB code: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_duplicate_create_table_sqlstate() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ep_dup",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let err = execute_expect_err(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        "42P07",
        "duplicate CREATE should carry SQLSTATE 42P07 (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("[VDB-1005]"),
        "message should carry the TableAlreadyExists VDB code: {}",
        err.message()
    );

    // IF NOT EXISTS is idempotent and must not create a second catalog row.
    execute(
        &client,
        &format!("CREATE TABLE IF NOT EXISTS {tbl} (id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    let rows = simple_query_rows(
        &client,
        &format!("SELECT COUNT(*) FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_insert_into_missing_table_sqlstate() {
    let client = ready_client().await;
    let tbl = unique_table_name("ep_ins_missing");

    let err = execute_expect_err(&client, &format!("INSERT INTO {tbl} (id) VALUES (1)")).await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "INSERT into missing table should carry SQLSTATE 42P01 (got {}: {})",
        err.code().code(),
        err.message()
    );
}

#[tokio::test]
async fn test_type_mismatch_on_insert_surfaces() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ep_typemismatch",
        &format!("(id INTEGER NOT NULL, n INTEGER) {CREATE_OPTS}"),
    )
    .await;

    // A non-castable string into an INTEGER column must surface as an error.
    let err = execute_expect_err(
        &client,
        &format!("INSERT INTO {tbl} (id, n) VALUES (1, 'not_a_number')"),
    )
    .await;
    assert!(
        err.message().contains("[VDB-"),
        "type-mismatch error should be enriched with a VDB code: {}",
        err.message()
    );

    // No partial row should have landed.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_cast_error_in_select() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ep_cast",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    execute(&client, &format!("INSERT INTO {tbl} (id) VALUES (1)"))
        .await
        .unwrap();

    // An explicit, impossible cast must surface as an enriched error on the read
    // path. (Note: an implicit `WHERE id = 'abc'` does NOT error — DuckDB coerces
    // it to zero matching rows — so we force the cast to exercise the error path.)
    let err = execute_expect_err(
        &client,
        &format!("SELECT CAST('abc' AS INTEGER) FROM {tbl}"),
    )
    .await;
    assert!(
        err.message().contains("[VDB-"),
        "cast error should be enriched with a VDB code: {}",
        err.message()
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_not_null_violation_message() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ep_notnull",
        &format!("(id INTEGER NOT NULL, name VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // Omitting a NOT NULL column must surface an enriched error. We intentionally
    // do NOT assert SQLSTATE 23502 here: NOT NULL violations are classified as
    // EngineError, not a constraint-specific code, so only the VDB enrichment is
    // stable.
    let err = execute_expect_err(&client, &format!("INSERT INTO {tbl} (id) VALUES (1)")).await;
    assert!(
        err.message().contains("[VDB-"),
        "NOT NULL violation should be enriched with a VDB code: {}",
        err.message()
    );

    // No partial row should have landed.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_unique_constraint_conflict_surfaces() {
    let client = ready_client().await;

    // PRIMARY KEY on the shard key keeps the constraint shard-local, so DuckDB
    // on the owning core enforces it.
    let tbl = create_table(
        &client,
        "ep_unique",
        &format!("(id INTEGER NOT NULL PRIMARY KEY, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a')"),
    )
    .await
    .unwrap();

    // Re-inserting the same key must be rejected as a write conflict.
    let err = execute_expect_err(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'b')"),
    )
    .await;
    assert!(
        err.message().contains("[VDB-"),
        "unique conflict should be enriched with a VDB code: {}",
        err.message()
    );

    // Exactly one row survives, unchanged.
    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "duplicate key must not create a second row");
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("a"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_syntax_error() {
    let client = ready_client().await;

    let result = execute(&client, "SELEKT 1").await;
    assert!(result.is_err(), "malformed SQL should error");
}

#[tokio::test]
async fn test_aggregates_empty_table() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "eb_empty",
        &format!("(id INTEGER NOT NULL, amt INTEGER) {CREATE_OPTS}"),
    )
    .await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT COUNT(*), SUM(amt), AVG(amt), MIN(amt), MAX(amt) FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_deref(),
        Some("0"),
        "COUNT(*) on empty table is 0"
    );
    assert_eq!(rows[0][1], None, "SUM over no rows is NULL");
    assert_eq!(rows[0][2], None, "AVG over no rows is NULL");
    assert_eq!(rows[0][3], None, "MIN over no rows is NULL");
    assert_eq!(rows[0][4], None, "MAX over no rows is NULL");

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_rf_equals_node_count_boundary() {
    let client = ready_client().await;

    // rf == node count (5) is the boundary that must succeed (the opposite of
    // test_rf_exceeds_node_count_rejected). Every shard spans all 5 nodes.
    let tbl = create_table(
        &client,
        "eb_rf_max",
        &format!(
            "(id INTEGER NOT NULL) \
             WITH (shards = 3, replication_factor = {EXPECTED_NODES}, shard_by = 'id')"
        ),
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(shards.len(), 3);
    for (bucket, primary, replicas) in &shards {
        let mut nodes = HashSet::new();
        nodes.insert(primary.as_str());
        for r in replicas {
            assert!(
                nodes.insert(r.as_str()),
                "shard bucket {bucket} placed a node twice"
            );
        }
        assert_eq!(
            nodes.len(),
            EXPECTED_NODES,
            "shard bucket {bucket} should span all {EXPECTED_NODES} nodes"
        );
    }

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_shards_zero_defaults() {
    let client = ready_client().await;

    // Explicit shards = 0 falls back to the alive-node count (distinct from
    // omitting the option, covered by test_default_shard_count).
    let tbl = create_table(
        &client,
        "eb_shards0",
        "(id INTEGER NOT NULL) \
             WITH (shards = 0, replication_factor = 3, shard_by = 'id')",
    )
    .await;

    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(
        shards.len(),
        EXPECTED_NODES,
        "shards = 0 should default to alive node count ({EXPECTED_NODES})"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_drop_nonexistent_table() {
    let client = ready_client().await;
    let tbl = unique_table_name("eb_missing_drop");

    let result = execute(&client, &format!("DROP TABLE {tbl}")).await;
    assert!(result.is_err(), "DROP of a missing table should error");

    // IF EXISTS makes the drop a no-op.
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_drop_column_shard_key_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "eb_dropkey",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // The shard key cannot be dropped.
    let result = execute(&client, &format!("ALTER TABLE {tbl} DROP COLUMN id")).await;
    assert!(
        result.is_err(),
        "dropping the shard-key column should be rejected"
    );

    // The table must remain intact and queryable.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}
