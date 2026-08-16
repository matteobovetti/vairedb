mod common;
use common::*;

// Identifier handling on the write/DDL path (sql_compat::rewrite_to_shard_local).
// A logical table name is canonicalized to a single bare identifier (quoted names
// kept verbatim, unquoted names lowercased, a `schema.` qualifier dropped) and
// used consistently as the catalog key, the physical shard-name input, and the
// DataFusion registration key. So both the write path (rewrite_to_shard_local)
// and the read/DROP path (util::shard_table_name) emit the identical bare physical
// relation `{canonical}_shard{n}`, which round-trips and drops cleanly:
//   * a quoted name `"MyTable"` maps to physical `MyTable_shard0` (suffix inside);
//   * a schema-qualified `schema.tbl` maps to physical `tbl_shard0`.
//
// These were previously #[ignore]'d because the buggy rewrite left uncleanable
// catalog/shard state that poisoned the shared cluster. Now fixed, they run in the
// default suite as regression guards: DROP removes every shard table, so reruns
// stay safe. `test_plain_identifier_write_roundtrip` is the lowercase control.
//
// KNOWN LIMITATION (see `test_schema_qualified_name_collision`): because a
// schema-qualified name collapses to only its LAST part, two tables that differ
// only by schema (`schema_a.t` vs `schema_b.t`) collide on one catalog key and
// one set of physical shards. That xfail documents the correct (independent
// tables) behavior; closing it requires modeling the schema as a real namespace.

#[tokio::test]
async fn test_quoted_table_identifier() {
    let client = ready_client().await;
    let tbl = format!("\"{}\"", unique_table_name("Ident_Quoted"));

    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await
    .unwrap();

    // Correct behavior: the rows read back through the per-shard rewrite.
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
async fn test_schema_qualified_write() {
    let client = ready_client().await;
    let tbl = format!("ident_schema.{}", unique_table_name("sch_tbl"));

    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1,'a'),(2,'b')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

// Two tables that differ ONLY by schema must be INDEPENDENT: a row written to
// `schema_a.<t>` must not be visible through `schema_b.<t>`. Today the write/DDL
// path canonicalizes a schema-qualified name to only its last part, so both
// names collapse to one catalog key and one set of physical shards — the second
// CREATE collides with the first and the two names alias the same data. Closing
// this requires modeling the schema qualifier as a real namespace.
#[tokio::test]
#[ignore = "known bug: schema-qualified names collapse to their last part, so schema_a.t and schema_b.t collide on one catalog key and one physical table instead of being independent"]
async fn test_schema_qualified_name_collision() {
    let client = ready_client().await;
    let base = unique_table_name("sch_collide");
    let tbl_a = format!("ident_schema_a.{base}");
    let tbl_b = format!("ident_schema_b.{base}");

    execute(&client, &format!("DROP TABLE IF EXISTS {tbl_a}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl_b}"))
        .await
        .unwrap();

    execute(
        &client,
        &format!("CREATE TABLE {tbl_a} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    // Correct behavior: a table in a different schema is a distinct relation, so
    // this CREATE succeeds instead of colliding with tbl_a.
    execute(
        &client,
        &format!("CREATE TABLE {tbl_b} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!("INSERT INTO {tbl_a} (id, v) VALUES (1,'a')"),
    )
    .await
    .unwrap();

    // Correct behavior: the row is only in schema_a's table; schema_b is empty.
    let rows_b = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl_b}"))
        .await
        .unwrap();
    assert_eq!(
        rows_b[0][0].as_deref(),
        Some("0"),
        "a row written to {tbl_a} must not be visible through {tbl_b}"
    );

    let rows_a = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl_a}"))
        .await
        .unwrap();
    assert_eq!(rows_a[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl_a).await;
    drop_table(&client, &tbl_b).await;
}

// Control: an unquoted lowercase identifier (the convention used everywhere else)
// rewrites correctly and round-trips through the write path. This passes today
// and guards against regressions in the common case.
#[tokio::test]
async fn test_plain_identifier_write_roundtrip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ident_plain",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await
    .unwrap();
    execute(&client, &format!("UPDATE {tbl} SET v = 'z' WHERE id = 2"))
        .await
        .unwrap();
    let deleted = execute(&client, &format!("DELETE FROM {tbl} WHERE id = 3"))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));
    assert_eq!(rows[1][1].as_deref(), Some("z"));

    drop_table(&client, &tbl).await;
}
