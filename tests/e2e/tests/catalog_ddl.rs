mod common;
use common::*;

// DDL statements (CREATE / ALTER / DROP TABLE) and the catalog state they
// produce: registered nodes, tables, columns, and shards. DDL is verified
// through its observable effect on `vairedb_catalog.*` rather than by
// re-querying the table, so this file owns both the statement surface and the
// catalog assertions for it.
//
// The `*_then_query` ALTER tests additionally guard the post-ALTER DATA path:
// after a schema change the per-shard tables and the Ballista catalog
// re-registration must agree with the catalog, so a query against the altered
// schema actually returns correct rows — not just a matching catalog row.
//
// What this file covers is the DDL that WORKS. The gap surface of the same three
// statements — CREATE TABLE AS SELECT, non-column ALTER operations, DROP of a
// non-table object — is `sql_command_ddl.rs`, the executable counterpart of rows
// 5-7 of docs/specs/gap-analysis-command.md.

#[tokio::test]
async fn test_node_ids_are_correct() {
    let client = ready_client().await;

    let rows = simple_query_rows(
        &client,
        "SELECT node_id FROM vairedb_catalog.nodes ORDER BY node_id",
    )
    .await
    .unwrap();

    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(ids, vec!["core-1", "core-2", "core-3", "core-4", "core-5"]);
}

#[tokio::test]
async fn test_catalog_tables() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cat_tables",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT table_name FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some(tbl.as_str()));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_catalog_columns() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cat_cols",
        &format!("(id INTEGER NOT NULL, score DOUBLE, label VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT column_name, data_type FROM vairedb_catalog.columns \
             WHERE table_name = '{tbl}' ORDER BY column_name"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 3);
    let col_names: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"score"));
    assert!(col_names.contains(&"label"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_catalog_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "cat_parts",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT table_name, shard_id FROM vairedb_catalog.shards \
             WHERE table_name = '{tbl}'"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 3, "expected 3 shards, got {}", rows.len());

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_alter_table_add_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_add",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD COLUMN age INTEGER"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT column_name FROM vairedb_catalog.columns \
             WHERE table_name = '{tbl}' AND column_name = 'age'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_alter_table_drop_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_drop",
        &format!("(id INTEGER NOT NULL, name VARCHAR, extra INTEGER) {CREATE_OPTS}"),
    )
    .await;

    execute(&client, &format!("ALTER TABLE {tbl} DROP COLUMN extra"))
        .await
        .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT column_name FROM vairedb_catalog.columns \
             WHERE table_name = '{tbl}' AND column_name = 'extra'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 0);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_alter_table_rename_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_rename",
        &format!("(id INTEGER NOT NULL, old_name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("ALTER TABLE {tbl} RENAME COLUMN old_name TO new_name"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT column_name FROM vairedb_catalog.columns \
             WHERE table_name = '{tbl}' AND column_name = 'new_name'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_alter_add_column_then_query() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_add_query",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Seed rows spanning every shard so the query exercises all per-shard tables.
    let seeded: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &seeded {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, name) VALUES ({id}, 'seed')"),
        )
        .await
        .unwrap();
    }

    execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD COLUMN age INTEGER"),
    )
    .await
    .unwrap();

    // A row written after the ALTER must be able to set the new column.
    let new_id = id_for_bucket(0, seeded[0] + 1);
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, name, age) VALUES ({new_id}, 'fresh', 42)"),
    )
    .await
    .unwrap();

    // The new column must be queryable end-to-end: the fresh row returns its age
    // and the pre-existing rows return NULL for the column they never had.
    let rows = simple_query_rows(&client, &format!("SELECT id, age FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), seeded.len() + 1);
    let fresh = rows
        .iter()
        .find(|r| r[0].as_deref() == Some(new_id.to_string().as_str()))
        .expect("fresh row must be present");
    assert_eq!(fresh[1].as_deref(), Some("42"));
    let null_age_count = rows.iter().filter(|r| r[1].is_none()).count();
    assert_eq!(
        null_age_count,
        seeded.len(),
        "rows that predate the ADD COLUMN must report NULL for the new column"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_alter_rename_column_then_query() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_rename_query",
        &format!("(id INTEGER NOT NULL, old_name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, old_name) VALUES ({id}, 'v{id}')"),
        )
        .await
        .unwrap();
    }

    execute(
        &client,
        &format!("ALTER TABLE {tbl} RENAME COLUMN old_name TO new_name"),
    )
    .await
    .unwrap();

    // The renamed column must be queryable and return the original data.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT new_name FROM {tbl} ORDER BY new_name"),
    )
    .await
    .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    let mut want: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
    want.sort();
    assert_eq!(got, want.iter().map(String::as_str).collect::<Vec<_>>());

    // The old name must no longer resolve on the read path.
    let result = simple_query_rows(&client, &format!("SELECT old_name FROM {tbl}")).await;
    assert!(
        result.is_err(),
        "querying the pre-rename column name must fail after RENAME COLUMN"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_drop_table() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_drop",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(&client, &format!("DROP TABLE {tbl}"))
        .await
        .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT table_name FROM vairedb_catalog.tables WHERE table_name = '{tbl}'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let client = ready_client().await;
    let tbl = unique_table_name("ddl_drop_ifex");

    let result = execute(&client, &format!("DROP TABLE IF EXISTS {tbl}")).await;
    assert!(result.is_ok());
}
