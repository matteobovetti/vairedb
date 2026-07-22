mod common;
use common::*;

// Identifier handling on the write/DDL path (sql_compat::rewrite_to_shard_local).
// The per-shard rewrite appends `_shard{n}` to the LAST identifier part of a
// relation without accounting for quote style or schema qualification, so:
//   * a quoted name `"MyTable"` becomes the relation `"MyTable"_shard0` (suffix
//     OUTSIDE the quotes) — reads fail, and because the catalog stores the name
//     with literal quote characters, DROP TABLE can never match it to clean up;
//   * a schema-qualified `schema.tbl` fails the DDL broadcast outright.
//
// CRITICAL: these failures leave UNCLEANABLE catalog/shard state that poisons the
// shared persistent cluster (DROP cannot remove the malformed entries). Running
// them in the default suite would break every subsequent `make e2e-test`, the
// exact rerun-safety hazard fixed previously. They are therefore #[ignore]'d:
// they assert the CORRECT behavior (so they convert to passing tests once the
// rewrite is fixed) but never run in CI. Recovering a cluster polluted by an
// --ignored run requires recreating the coordinator and restarting the cores
// (the catalog redb lives in the coordinator container; cores re-register only
// at startup) — see the README/Makefile e2e targets.

#[tokio::test]
#[ignore = "known bug: quoted table identifier gets the shard suffix appended outside the quotes; reads fail and the entry cannot be dropped (pollutes the shared cluster)"]
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
#[ignore = "known bug: schema-qualified table name fails the per-shard DDL broadcast; only the last identifier part is suffixed"]
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
