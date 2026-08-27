mod common;
use common::*;

// CREATE TABLE / ALTER TABLE / DROP — rows 5-7 of
// docs/specs/gap-analysis-command.md. See `sql_command_select.rs` for the
// four-file layout and the passing/#[ignore] convention.
//
//     cd tests/e2e && cargo test --test sql_command_ddl -- --ignored --test-threads=1
//
// This file owns the *gap* surface of the three DDL rows — what the doc marks
// 🟡 and the one ✅ caveat. The DDL that already works, and the catalog state it
// produces, lives in its own files and is not duplicated here:
//   * CREATE/ALTER/DROP TABLE happy paths + `vairedb_catalog.*` assertions ->
//     `catalog_ddl.rs`;
//   * `WITH (shards, replication_factor, shard_by)` variants -> `sharding.rs`;
//   * `anonymized_columns` -> `anonymization.rs`;
//   * DROP of the shard-key column, duplicate CREATE, rf > node count ->
//     `errors.rs`;
//   * PG -> DuckDB column type mapping -> `data_types_round_trips.rs`.
//
// Two ALTER restrictions are deliberately NOT xfailed, because they are
// correctness rules rather than gaps: the shard-key column cannot be dropped
// (dropping it would strand every row) and anonymized columns cannot be renamed,
// dropped or retyped (it would silently disable pseudonymization). Both keep
// their rejection tests in `errors.rs` / `anonymization.rs`.

// ============================================================================
// CREATE TABLE — row 5 (✅), with the CREATE TABLE AS SELECT caveat
// ============================================================================

// The doc's row-5 caveat: `CREATE TABLE AS SELECT` is "not validated for
// sharding". It carries no column list, so `parse_create_table_config` cannot
// derive a shard key from the columns and falls back to `"id"`. Whatever the
// coordinator does with it, the one thing a client must never get is a silent OK
// for a table that then cannot be read.
#[tokio::test]
async fn test_create_table_as_select_is_not_a_silent_ok() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "ddl_ctas_src",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {src} (id, value) VALUES (1, 'a'), (2, 'b')"),
    )
    .await
    .unwrap();

    let dst = unique_table_name("ddl_ctas_dst");
    let ctas = format!("CREATE TABLE {dst} AS SELECT id, value FROM {src}");

    match execute(&client, &ctas).await {
        // Rejected outright: acceptable, and the table must not exist.
        Err(_) => {
            assert!(
                simple_query_rows(&client, &format!("SELECT id FROM {dst}"))
                    .await
                    .is_err(),
                "a failed CTAS must not leave {dst} behind"
            );
        }
        // Accepted: then it has to be a real, readable table holding the
        // SELECT's rows — not an empty or unqueryable shell.
        Ok(_) => {
            let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
                .await
                .expect("a CTAS that reports success must produce a readable table");
            let got: Vec<i64> = rows
                .iter()
                .map(|r| r[0].as_deref().unwrap().parse().unwrap())
                .collect();
            assert_eq!(got, vec![1, 2], "CTAS must materialize the SELECT's rows");
            execute(&client, &format!("DROP TABLE IF EXISTS {dst}"))
                .await
                .unwrap();
        }
    }

    drop_table(&client, &src).await;
}

// Target state: CTAS materializes the SELECT and shards the result like any
// other table — a registered shard set plus the source's rows.
//
// Observed today: CTAS classifies as CreateTable and IS broadcast, but the shard
// DDL fails, so the client gets `08006 [VDB-3007] DDL broadcast to node core-1
// failed` — the per-shard CREATE never runs and no rows are materialized.
#[tokio::test]
#[ignore = "gap (row 5): CREATE TABLE AS SELECT is not validated for sharding — it has no column list, so the shard key falls back to \"id\", and the per-shard DDL broadcast fails (08006)"]
async fn test_create_table_as_select_shards_the_result() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "ddl_ctasx_src",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // One id per bucket so a correctly sharded copy has to span all shards.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {src} (id, value) VALUES ({id}, 'v{id}')"),
        )
        .await
        .unwrap();
    }

    let dst = unique_table_name("ddl_ctasx_dst");
    execute(
        &client,
        &format!("CREATE TABLE {dst} AS SELECT id, value FROM {src} {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    // The copy is a first-class sharded table.
    let shards = fetch_shards(&client, &dst).await;
    assert_eq!(
        shards.len(),
        SHARD_COUNT,
        "CTAS must register {SHARD_COUNT} shards, got {}",
        shards.len()
    );

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids.clone();
    want.sort_unstable();
    assert_eq!(got, want, "every source row must be materialized once");

    execute(&client, &format!("DROP TABLE IF EXISTS {dst}"))
        .await
        .unwrap();
    drop_table(&client, &src).await;
}

// ============================================================================
// ALTER TABLE — row 6 (🟡 column ops only)
// ============================================================================

// Everything outside the supported column-op set is rejected by
// `apply_alter_operation`'s catch-all with `0A000`. The supported ops (ADD /
// DROP / RENAME COLUMN, ALTER COLUMN SET DATA TYPE / SET|DROP NOT NULL /
// SET|DROP DEFAULT) are exercised in `catalog_ddl.rs`.
#[tokio::test]
async fn test_alter_table_non_column_ops_currently_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_ops",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Constraint operations.
    for op in [
        "ADD CONSTRAINT ck_pos CHECK (id > 0)",
        "ADD CONSTRAINT uq_v UNIQUE (v)",
        "ADD PRIMARY KEY (id)",
        "DROP CONSTRAINT ck_pos",
    ] {
        let sql = format!("ALTER TABLE {tbl} {op}");
        let err = assert_rejected(&client, &sql).await;
        assert_eq!(
            err.code().code(),
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "`{sql}` should be rejected 0A000, got {}: {}",
            err.code().code(),
            err.message()
        );
    }

    // Table-level operations.
    let renamed = unique_table_name("ddl_alter_renamed");
    let err = assert_rejected(&client, &format!("ALTER TABLE {tbl} RENAME TO {renamed}")).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "RENAME TO should be rejected 0A000, got {}: {}",
        err.code().code(),
        err.message()
    );

    // The table is still there under its original name, unchanged.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .expect("a rejected ALTER must leave the table readable");
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

// Target state: a CHECK constraint added after the fact is enforced on every
// shard, so a violating INSERT fails.
#[tokio::test]
#[ignore = "gap (row 6): ALTER TABLE ADD CONSTRAINT is rejected 0A000 — apply_alter_operation handles column ops only, and the catalog has no constraint model"]
async fn test_alter_table_add_check_constraint_is_enforced() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_check",
        &format!("(id INTEGER NOT NULL, amount INTEGER) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD CONSTRAINT amount_positive CHECK (amount > 0)"),
    )
    .await
    .unwrap();

    // A conforming row is accepted.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, amount) VALUES (1, 10)"),
    )
    .await
    .unwrap();

    // A violating row is refused, on whichever shard it routes to.
    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, amount) VALUES (2, -5)"),
    )
    .await;
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// Target state: renaming a table moves the catalog entry and the per-shard
// physical tables together, so the new name reads and the old one is gone.
#[tokio::test]
#[ignore = "gap (row 6): ALTER TABLE ... RENAME TO is rejected 0A000 — renaming would have to re-key the catalog entry and rename every `{table}_shard{n}` physical table"]
async fn test_alter_table_rename_to_moves_the_table() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_rename_from",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'x')"),
    )
    .await
    .unwrap();

    let renamed = unique_table_name("ddl_rename_to");
    execute(&client, &format!("ALTER TABLE {tbl} RENAME TO {renamed}"))
        .await
        .unwrap();

    // The row is readable under the new name...
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {renamed} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("x"));

    // ...and the old name no longer resolves.
    assert!(
        simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
            .await
            .is_err(),
        "the pre-rename name must stop resolving"
    );

    execute(&client, &format!("DROP TABLE IF EXISTS {renamed}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
}

// ============================================================================
// DROP — row 7 (🟡 only DROP TABLE is meaningful)
// ============================================================================

// Every `DROP <kind>` lands in `handle_drop_table`, which only knows tables. For
// a non-table object that means the catalog lookup misses and the client gets
// "table does not exist" — wrong noun, but an honest failure rather than a fake
// OK. `DROP TABLE` itself (incl. `IF EXISTS`) is covered by `catalog_ddl.rs`.
#[tokio::test]
async fn test_drop_of_non_table_objects_currently_fails() {
    let client = ready_client().await;

    for kind in ["VIEW", "INDEX", "SCHEMA", "SEQUENCE"] {
        let name = unique_table_name("ddl_drop_obj");
        let sql = format!("DROP {kind} {name}");
        let err = assert_rejected(&client, &sql).await;
        assert!(
            !err.message().is_empty(),
            "`{sql}` must report an error naming the missing object"
        );
    }
}

// Target state (PostgreSQL 42809 "wrong object type"): `DROP VIEW` naming a
// TABLE must refuse and leave the table intact. Today the statement reaches
// `handle_drop_table`, which finds the table in the catalog and drops it —
// a `DROP VIEW` (or `DROP INDEX`, `DROP SEQUENCE`) silently destroys a table.
//
// Confirmed against the e2e cluster: `DROP VIEW <table>` returns success and the
// table with its rows is gone. This is the highest-consequence entry in the whole
// command gap map — a client typo on an object kind is unrecoverable data loss.
#[tokio::test]
#[ignore = "gap (row 7): DROP <any kind> is classified DropTable, so `DROP VIEW t` on a table named t drops the table instead of reporting a wrong-object-type error"]
async fn test_drop_view_must_not_drop_a_table() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_drop_wrongkind",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'keep')"),
    )
    .await
    .unwrap();

    // `DROP VIEW <table>` must fail...
    assert_rejected(&client, &format!("DROP VIEW {tbl}")).await;

    // ...and the table must still be there, with its row.
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = 1"))
        .await
        .expect("the table must survive a DROP VIEW aimed at it");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("keep"));

    // Tolerant cleanup: when this xfail fails, the table is already gone.
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
}
