mod common;
use common::*;

// Statements the coordinator does NOT route (anything that isn't
// SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE / ALTER TABLE)
// fall through the dispatch catch-all in `pgwire_handler.rs` and return a fake
// `Tag::new("OK")` with NO execution. A client therefore believes a transaction
// opened, an index was built, or a table was truncated when nothing happened.
//
// These tests assert the PostgreSQL-CORRECT behavior, so they convert to passing
// tests once real handlers exist. Until then they are #[ignore]'d xfails (run via
// `cargo test -- --ignored`), following the convention in `identifier_rewrite.rs`.
//
// Cleanup safety: every test uses `unique_table_name` + a trailing `DROP TABLE`
// and only fails on SELECT/DML assertions or on statements that touch nothing, so
// none of them can leave uncleanable catalog/shard state in the shared cluster.

// BEGIN ... ROLLBACK must undo the buffered DML. Today BEGIN/ROLLBACK are silent
// no-ops and every statement auto-commits, so the row survives the rollback.
#[tokio::test]
#[ignore = "known gap: BEGIN/ROLLBACK are silent no-ops; DML is not transactional, so the inserted row is not rolled back"]
async fn test_rollback_undoes_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "us_rollback",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(&client, "BEGIN").await.unwrap();
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a')"),
    )
    .await
    .unwrap();
    execute(&client, "ROLLBACK").await.unwrap();

    // Correct behavior: the rolled-back insert leaves no row.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("0"),
        "ROLLBACK should have undone the INSERT"
    );

    drop_table(&client, &tbl).await;
}

// SET a runtime parameter, then SHOW it: the value should round-trip. Today SET is
// a silent no-op and SHOW returns no rows.
#[tokio::test]
#[ignore = "known gap: SET is a silent no-op and SHOW returns no rows; runtime parameters are not tracked"]
async fn test_set_show_roundtrip() {
    let client = ready_client().await;

    execute(&client, "SET search_path TO myschema")
        .await
        .unwrap();

    let rows = simple_query_rows(&client, "SHOW search_path")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "SHOW should return exactly one row");
    assert_eq!(
        rows[0][0].as_deref(),
        Some("myschema"),
        "SHOW should reflect the value set by SET"
    );
}

// TRUNCATE must empty a populated table. Today it returns a fake OK and the rows
// remain.
#[tokio::test]
#[ignore = "known gap: TRUNCATE returns a fake OK with no execution; rows are not removed"]
async fn test_truncate_empties_table() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "us_truncate",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await
    .unwrap();

    execute(&client, &format!("TRUNCATE TABLE {tbl}"))
        .await
        .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("0"),
        "TRUNCATE should have removed all rows"
    );

    drop_table(&client, &tbl).await;
}

// CREATE VIEW then SELECT from the view must return the base rows. Today CREATE
// VIEW is a fake OK, so selecting from the view fails with table-not-found.
#[tokio::test]
#[ignore = "known gap: CREATE VIEW returns a fake OK with no execution; the view does not exist"]
async fn test_create_view_is_queryable() {
    let client = ready_client().await;
    let view = unique_table_name("us_view");
    let tbl = create_table(
        &client,
        "us_viewbase",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b')"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl} WHERE id = 1"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {view}"))
        .await
        .expect("selecting from the created view should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    execute(&client, &format!("DROP VIEW IF EXISTS {view}"))
        .await
        .ok();
    drop_table(&client, &tbl).await;
}

// EXPLAIN must return a query plan. Today it returns a fake OK with zero rows.
#[tokio::test]
#[ignore = "known gap: EXPLAIN returns a fake OK with no plan rows"]
async fn test_explain_returns_plan() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "us_explain",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let rows = simple_query_rows(&client, &format!("EXPLAIN SELECT * FROM {tbl}"))
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "EXPLAIN should return at least one plan row"
    );

    drop_table(&client, &tbl).await;
}
