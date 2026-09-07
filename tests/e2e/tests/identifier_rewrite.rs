mod common;
use common::*;

// Identifier handling on the write/DDL path (write_sql_cl::rewrite_to_shard_local).
// A logical table name is canonicalized to one catalog key — quoted names kept
// verbatim, unquoted names lowercased, a `schema.` qualifier kept as part of the
// key — and that key is used consistently as the catalog key, the physical
// shard-name input, and the DataFusion registration key. So both the write path
// (rewrite_to_shard_local) and the read/DROP path (util::shard_table_name) emit the
// identical physical relation, which round-trips and drops cleanly:
//   * a quoted name `"MyTable"` maps to physical `MyTable_shard0` (suffix inside);
//   * a schema-qualified `schema.tbl` maps to physical `schema_tbl_shard0`.
//
// These were previously #[ignore]'d because the buggy rewrite left uncleanable
// catalog/shard state that poisoned the shared cluster. Now fixed, they run in the
// default suite as regression guards: DROP removes every shard table, so reruns
// stay safe. `test_plain_identifier_write_roundtrip` is the lowercase control.
//
// A schema is a coordinator-catalog namespace: a core node has one flat DuckDB
// namespace and splices relation names into SQL unquoted, so the qualifier is
// folded into the physical identifier rather than becoming a DuckDB schema. That
// fold is not injective, which is why `schema.tbl` and `schema_tbl` cannot both
// exist — see `test_a_folded_physical_name_collision_is_refused`.

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
    let schema = unique_table_name("ident_sch");
    let tbl = format!("{schema}.sch_tbl");

    execute(&client, &format!("CREATE SCHEMA {schema}"))
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
    execute(&client, &format!("DROP SCHEMA {schema}"))
        .await
        .unwrap();
}

// Two tables that differ ONLY by schema are INDEPENDENT: a row written to
// `schema_a.<t>` is not visible through `schema_b.<t>`, because the qualifier is
// part of the catalog key and so of the physical per-shard names.
#[tokio::test]
async fn test_schema_qualified_name_collision() {
    let client = ready_client().await;
    let base = unique_table_name("sch_collide");
    let schema_a = format!("{base}_a");
    let schema_b = format!("{base}_b");
    let tbl_a = format!("{schema_a}.t");
    let tbl_b = format!("{schema_b}.t");

    execute(&client, &format!("CREATE SCHEMA {schema_a}"))
        .await
        .unwrap();
    execute(&client, &format!("CREATE SCHEMA {schema_b}"))
        .await
        .unwrap();

    execute(
        &client,
        &format!("CREATE TABLE {tbl_a} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    // A table in a different schema is a distinct relation, so this CREATE succeeds
    // instead of colliding with tbl_a.
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

    // The row is only in schema_a's table; schema_b's is empty.
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
    execute(&client, &format!("DROP SCHEMA {schema_a}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP SCHEMA {schema_b}"))
        .await
        .unwrap();
}

// The physical per-shard name folds `.` into `_`, so `s.t` and `s_t` both want
// `s_t_shard<n>`. The second name is refused rather than silently sharing one set
// of physical tables — which is exactly the collision schemas exist to remove.
// Refused whichever order the two are created in.
#[tokio::test]
async fn test_a_folded_physical_name_collision_is_refused() {
    let client = ready_client().await;
    let schema = unique_table_name("ident_fold");
    let qualified = format!("{schema}.t");
    let flat = format!("{schema}_t");

    execute(&client, &format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {qualified} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    let err = assert_sqlstate(
        &client,
        &format!("CREATE TABLE {flat} (id INTEGER NOT NULL) {CREATE_OPTS}"),
        SQLSTATE_DUPLICATE_TABLE,
    )
    .await;
    assert!(
        err.message().contains(&qualified),
        "the refusal must name the relation that owns the physical name: {}",
        err.message()
    );

    drop_table(&client, &qualified).await;

    // With the qualified table gone the flat name is free, and then the qualified
    // one is the name refused.
    execute(
        &client,
        &format!("CREATE TABLE {flat} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    assert_sqlstate(
        &client,
        &format!("CREATE TABLE {qualified} (id INTEGER NOT NULL) {CREATE_OPTS}"),
        SQLSTATE_DUPLICATE_TABLE,
    )
    .await;

    drop_table(&client, &flat).await;
    execute(&client, &format!("DROP SCHEMA {schema}"))
        .await
        .unwrap();
}

// A rename stays inside the schema, as in PostgreSQL: the new name is the
// relation's, never a way to move the table somewhere else.
#[tokio::test]
async fn test_rename_stays_in_the_schema() {
    let client = ready_client().await;
    let schema = unique_table_name("ident_rn");
    let before = format!("{schema}.t");
    let after = format!("{schema}.t2");

    execute(&client, &format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {before} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {before} (id, v) VALUES (1,'a')"),
    )
    .await
    .unwrap();

    execute(&client, &format!("ALTER TABLE {before} RENAME TO t2"))
        .await
        .unwrap();

    assert_eq!(row_count(&client, &after).await, 1);
    // Not in the default schema: the rename moved nothing out of {schema}.
    assert_sqlstate(&client, "SELECT COUNT(*) FROM t2", "42P01").await;

    drop_table(&client, &after).await;
    execute(&client, &format!("DROP SCHEMA {schema}"))
        .await
        .unwrap();
}

// An index lives in its table's schema, so the same index name can be used once per
// schema — the pattern one DDL script applied per tenant produces.
#[tokio::test]
async fn test_an_index_name_is_scoped_to_its_table_schema() {
    let client = ready_client().await;
    let base = unique_table_name("ident_idx");
    let schema_a = format!("{base}_a");
    let schema_b = format!("{base}_b");

    for schema in [&schema_a, &schema_b] {
        execute(&client, &format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        execute(
            &client,
            &format!("CREATE TABLE {schema}.t (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
        )
        .await
        .unwrap();
        execute(&client, &format!("CREATE INDEX t_v_idx ON {schema}.t (v)"))
            .await
            .unwrap();
    }

    // The index is recorded in its table's schema, so it is dropped by the
    // qualified name.
    for schema in [&schema_a, &schema_b] {
        execute(&client, &format!("DROP INDEX {schema}.t_v_idx"))
            .await
            .unwrap();
        drop_table(&client, &format!("{schema}.t")).await;
        execute(&client, &format!("DROP SCHEMA {schema}"))
            .await
            .unwrap();
    }
}

// Column identifiers fold the way PostgreSQL folds them: unquoted names are
// case-insensitive, so `INSERT INTO t (ID, V)` names the shard key declared `id`.
// Before the write path canonicalized its comparisons this INSERT was rejected
// ("must specify a value for shard key column"), and an UPDATE assigning `ID`
// slipped past the shard-key-relocation guard.
#[tokio::test]
async fn test_unquoted_column_names_are_case_insensitive_on_the_write_path() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ident_colcase",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (ID, V) VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await
    .unwrap();

    // Exactly three rows: had the shard key not matched, the rows would have been
    // broadcast to every shard and this would read back 3 * SHARD_COUNT.
    assert_eq!(row_count(&client, &tbl).await, 3);

    execute(&client, &format!("UPDATE {tbl} SET V = 'z' WHERE ID = 2"))
        .await
        .unwrap();
    let deleted = execute(&client, &format!("DELETE FROM {tbl} WHERE ID = 3"))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1].as_deref(), Some("z"));

    // Relocating the shard key is unsupported and must be refused whatever case
    // the client writes it in.
    assert_rejected(&client, &format!("UPDATE {tbl} SET ID = 99 WHERE ID = 1")).await;

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
