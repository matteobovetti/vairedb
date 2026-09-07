mod common;
use common::*;
use tokio_postgres::Client;

// CREATE TABLE / ALTER TABLE / DROP / TRUNCATE — rows 5-7 of
// docs/specs/gap-analysis-command.md. See `sql_command_select.rs` for the
// four-file layout and the passing/#[ignore] convention.
//
//     cd tests/e2e && cargo test --test sql_command_ddl -- --ignored --test-threads=1
//
// It also owns TRUNCATE, which the doc lists in its write-path table with no row
// number of its own: it is a table-level statement handled by the same
// `ddl.rs` broadcast machinery.
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

// The doc's row-5 caveat: `CREATE TABLE AS SELECT` carries no column list, so
// there is nothing to derive a shard key from. Whatever the coordinator does with
// it, the one thing a client must never get is a silent OK for a table that then
// cannot be read.
//
// This CTAS names no shard key, so it is refused before the catalog is touched and
// the Err branch is the one that runs — see
// `test_create_table_as_select_is_rejected_before_the_catalog_is_touched` for the
// stronger assertions on *how* it is refused. A CTAS that *does* name one is
// materialized: `test_create_table_as_select_shards_the_result` covers that. This
// test stays deliberately tolerant so it holds either way.
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

// A CTAS must be refused where the refusal costs nothing: at the coordinator,
// before any node or the catalog is written. Otherwise the client sees a transport
// error (`08006`, the DDL broadcast failing on a shard) for a table that is
// already registered — and the next CREATE of that name then fails as a duplicate.
#[tokio::test]
async fn test_create_table_as_select_is_rejected_before_the_catalog_is_touched() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "ddl_ctas_pre",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let dst = unique_table_name("ddl_ctas_predst");
    let err = execute_expect_err(
        &client,
        &format!("CREATE TABLE {dst} AS SELECT id, value FROM {src}"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "CTAS should be a clean feature rejection, not a broadcast failure (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("shard key"),
        "the rejection should say why: {}",
        err.message()
    );

    // Nothing was registered, so the name is still free: a normal CREATE of the
    // same name must succeed rather than collide with a half-created table.
    execute(
        &client,
        &format!("CREATE TABLE {dst} (id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .expect("a rejected CTAS must not have reserved the table name");

    drop_table(&client, &dst).await;
    drop_table(&client, &src).await;
}

// CTAS materializes the SELECT and shards the result like any other table — a
// registered shard set plus the source's rows.
//
// The `WITH (...)` has to come *before* `AS SELECT`: written after the query it
// parses as a hint on the source relation instead of a table option, and the
// statement then reads as a CTAS that names no shard key.
#[tokio::test]
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
        &format!("CREATE TABLE {dst} {CREATE_OPTS} AS SELECT id, value FROM {src}"),
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
// ALTER TABLE — row 6 (🟡 column ops + RENAME TO)
// ============================================================================

// Everything outside the supported set is rejected by `apply_alter_operation`'s
// catch-all with `0A000`. The supported column ops (ADD / DROP / RENAME COLUMN,
// ALTER COLUMN SET DATA TYPE / SET|DROP NOT NULL / SET|DROP DEFAULT) are
// exercised in `catalog_ddl.rs`; `RENAME TO` is the one table-level op that is
// honored, and it has its own tests below.
#[tokio::test]
async fn test_alter_table_non_column_ops_currently_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_alter_ops",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Constraint operations the shards' engine cannot deliver on a table that
    // already exists, or that VaireDB cannot enforce across shards. `ADD CONSTRAINT
    // ... UNIQUE` over the shard key is the one that is honored — see
    // `test_alter_table_add_unique_constraint_on_the_shard_key`.
    for op in [
        // DuckDB implements no ALTER TABLE ADD CHECK at all.
        "ADD CONSTRAINT ck_pos CHECK (id > 0)",
        // Uniqueness off the shard key cannot be enforced per shard.
        "ADD CONSTRAINT uq_v UNIQUE (v)",
        // Accepted once per table by the shards and never droppable, so a partial
        // broadcast could be neither retried nor undone.
        "ADD PRIMARY KEY (id)",
        // No name to derive the per-shard index names from.
        "ADD UNIQUE (id)",
        // Nothing in the catalog depends on a constraint.
        "DROP CONSTRAINT ck_pos CASCADE",
        // A foreign key spans shards, so no shard can check it.
        "ADD CONSTRAINT fk_v FOREIGN KEY (v) REFERENCES other (v)",
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

    // A constraint that was never declared is missing, not unsupported.
    let err = assert_rejected(
        &client,
        &format!("ALTER TABLE {tbl} DROP CONSTRAINT ck_pos"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "dropping a missing constraint should report 42P01, got {}: {}",
        err.code().code(),
        err.message()
    );
    execute(
        &client,
        &format!("ALTER TABLE {tbl} DROP CONSTRAINT IF EXISTS ck_pos"),
    )
    .await
    .expect("IF EXISTS on a missing constraint must succeed");

    // The table is still there under its original name, unchanged.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .expect("a rejected ALTER must leave the table readable");
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

/// One id per shard, each the smallest `>= start` that hashes to its bucket, so a
/// statement can be aimed at every shard in turn. A constraint that reached only
/// some of them then fails one of the rounds instead of passing by coincidence.
fn ids_across_shards(start: i64) -> Vec<i64> {
    (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, start))
        .collect()
}

// A CHECK is a predicate on one row, so every shard applying it to its own rows
// is the same as applying it to all of them — which makes it exactly enforceable
// across the cluster. It has to be declared in CREATE TABLE: the shards' engine
// implements no `ALTER TABLE ... ADD CHECK`.
#[tokio::test]
async fn test_create_table_check_constraint_is_enforced_on_every_shard() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_check",
        &format!(
            "(id INTEGER NOT NULL, amount INTEGER, CONSTRAINT amount_positive CHECK (amount > 0)) \
             {CREATE_OPTS}"
        ),
    )
    .await;

    // One conforming row per shard, so a CHECK that reached only some of them shows
    // up as a rejection rather than passing by coincidence.
    let ids = ids_across_shards(1);
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, amount) VALUES ({id}, 10)"),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("a conforming row on the shard of id {id} must be accepted: {e}")
        });
    }

    // A violating row is refused, whichever shard it routes to.
    for id in ids_across_shards(1000) {
        let sql = format!("INSERT INTO {tbl} (id, amount) VALUES ({id}, -5)");
        assert_rejected(&client, &sql).await;
    }
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    drop_table(&client, &tbl).await;
}

// Uniqueness over the shard key is the one uniqueness VaireDB can promise: equal
// shard keys always hash to one shard, so the shard that would see a duplicate is
// the only shard that has to check. VaireDB enforces it with one unique index per
// shard, which is also what makes it droppable again.
#[tokio::test]
async fn test_alter_table_add_unique_constraint_on_the_shard_key() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_add_unique",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let ids = ids_across_shards(1);
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'a')"),
        )
        .await
        .unwrap();
    }

    execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD CONSTRAINT uq_id UNIQUE (id)"),
    )
    .await
    .expect("UNIQUE over the shard key must be accepted");

    // Every shard now refuses a duplicate of the row it already holds.
    for id in &ids {
        let sql = format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'b')");
        assert_rejected(&client, &sql).await;
    }
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    // The name is a relation name cluster-wide, so an index may not take it, and
    // DROP INDEX is not what removes it.
    let err = assert_rejected(&client, &format!("CREATE INDEX uq_id ON {tbl} (v)")).await;
    assert_eq!(err.code().code(), "42P07", "got {}", err.message());
    let err = assert_rejected(&client, "DROP INDEX uq_id").await;
    assert_eq!(err.code().code(), "42809", "got {}", err.message());

    // Dropping it drops the per-shard indexes, so the duplicates go through again.
    execute(&client, &format!("ALTER TABLE {tbl} DROP CONSTRAINT uq_id"))
        .await
        .expect("an added constraint must be droppable");
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'b')"),
        )
        .await
        .unwrap_or_else(|e| panic!("the duplicate on the shard of id {id} must be accepted: {e}"));
    }
    assert_eq!(row_count(&client, &tbl).await, 2 * ids.len() as i64);

    drop_table(&client, &tbl).await;
}

// A declared constraint is part of every shard's table definition, and the shards'
// engine can neither drop it nor let a column it covers change. Both facts are
// reported up front, rather than as a broadcast that failed on every node.
#[tokio::test]
async fn test_a_declared_constraint_blocks_the_column_changes_the_shards_refuse() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_declared_constraint",
        &format!(
            "(id INTEGER NOT NULL, amount INTEGER, note VARCHAR, tag VARCHAR, \
             CONSTRAINT amount_positive CHECK (amount > 0), \
             CONSTRAINT uq_id_note UNIQUE (id, note)) {CREATE_OPTS}"
        ),
    )
    .await;

    // Each entry names the constraint the coordinator must point at, so a refusal
    // that came from some other guard — the shard key, say — does not pass for one.
    for (sql, constraint) in [
        // Declared in the shards' own CREATE TABLE, so unremovable.
        (
            format!("ALTER TABLE {tbl} DROP CONSTRAINT amount_positive"),
            "amount_positive",
        ),
        (
            format!("ALTER TABLE {tbl} DROP CONSTRAINT uq_id_note"),
            "uq_id_note",
        ),
        // The shards refuse to retype a column under any constraint, CHECK included.
        (
            format!("ALTER TABLE {tbl} ALTER COLUMN amount SET DATA TYPE BIGINT"),
            "amount_positive",
        ),
        (
            format!("ALTER TABLE {tbl} ALTER COLUMN note SET DATA TYPE TEXT"),
            "uq_id_note",
        ),
        // …and refuse to drop one a UNIQUE covers.
        (format!("ALTER TABLE {tbl} DROP COLUMN note"), "uq_id_note"),
    ] {
        let err = assert_rejected(&client, &sql).await;
        assert_eq!(
            err.code().code(),
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "`{sql}` should be rejected 0A000, got {}: {}",
            err.code().code(),
            err.message()
        );
        assert!(
            err.message().contains(constraint),
            "`{sql}` should name {constraint}: {}",
            err.message()
        );
    }

    // Unlike an index, a declared constraint is not a dependency on the whole table:
    // a column it does not cover still changes.
    execute(&client, &format!("ALTER TABLE {tbl} DROP COLUMN tag"))
        .await
        .expect("an unconstrained column may still be dropped");

    // Both constraints are still enforced after all that.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, amount, note) VALUES (1, 5, 'a')"),
    )
    .await
    .unwrap();
    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, amount, note) VALUES (2, -5, 'b')"),
    )
    .await;
    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, amount, note) VALUES (1, 5, 'a')"),
    )
    .await;
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// The shards rewrite a CHECK around a renamed column rather than dropping it, so the
// constraint has to still bite under the new name — and the catalog has to agree,
// which is what keeps the guards above pointing at the right column.
#[tokio::test]
async fn test_renaming_a_checked_column_keeps_the_check() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_check_rename",
        &format!(
            "(id INTEGER NOT NULL, amount INTEGER, \
             CONSTRAINT amount_positive CHECK (amount > 0)) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!("ALTER TABLE {tbl} RENAME COLUMN amount TO total"),
    )
    .await
    .expect("a CHECK must not block a rename");

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, total) VALUES (1, 5)"),
    )
    .await
    .unwrap();
    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, total) VALUES (2, -5)"),
    )
    .await;
    assert_eq!(row_count(&client, &tbl).await, 1);

    // The catalog followed the rename, so the constraint blocks a retype of the new
    // name and no longer knows the old one.
    let err = assert_rejected(
        &client,
        &format!("ALTER TABLE {tbl} ALTER COLUMN total SET DATA TYPE BIGINT"),
    )
    .await;
    assert_eq!(err.code().code(), SQLSTATE_FEATURE_NOT_SUPPORTED);
    assert!(
        err.message().contains("amount_positive"),
        "got {}",
        err.message()
    );

    drop_table(&client, &tbl).await;
}

// Renaming a table moves the catalog entry and every `{table}_shard{n}` physical
// table together, so the new name reads and writes and the old one is gone.
// Every shard has a row, so a rename that moved only some of them shows up as a
// short count rather than passing by coincidence.
#[tokio::test]
async fn test_alter_table_rename_to_moves_the_table() {
    let client = ready_client().await;
    let (tbl, ids) = table_with_a_row_per_shard(&client, "ddl_rename_from").await;

    let renamed = unique_table_name("ddl_rename_to");
    execute(&client, &format!("ALTER TABLE {tbl} RENAME TO {renamed}"))
        .await
        .unwrap();

    // Every shard's row is readable under the new name...
    assert_eq!(
        row_count(&client, &renamed).await,
        ids.len() as i64,
        "the rename must move every shard"
    );
    for id in &ids {
        let rows = simple_query_rows(&client, &format!("SELECT v FROM {renamed} WHERE id = {id}"))
            .await
            .unwrap();
        assert_eq!(
            rows[0][0].as_deref(),
            Some(format!("v{id}").as_str()),
            "shard {} did not follow the rename",
            bucket_of(*id)
        );
    }

    // ...the shard layout still routes, so the renamed table takes writes...
    execute(
        &client,
        &format!("INSERT INTO {renamed} (id, v) VALUES ({}, 'after')", ids[0]),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&client, &renamed).await, ids.len() as i64 + 1);

    // ...and the old name no longer resolves.
    assert!(
        simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
            .await
            .is_err(),
        "the pre-rename name must stop resolving"
    );

    drop_table(&client, &renamed).await;
}

// Renaming onto a name that is already taken must fail as a duplicate relation
// (42P07) and change nothing: the alternative — clobbering the catalog entry —
// would leave the occupant's shards orphaned and unreadable.
#[tokio::test]
async fn test_alter_table_rename_onto_an_existing_name_is_refused() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "ddl_rename_dup_src",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let dst = create_table(
        &client,
        "ddl_rename_dup_dst",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {dst} (id, v) VALUES (1, 'kept')"),
    )
    .await
    .unwrap();

    let err = assert_rejected(&client, &format!("ALTER TABLE {src} RENAME TO {dst}")).await;
    assert_eq!(
        err.code().code(),
        "42P07",
        "renaming onto an existing name should be a duplicate relation, got {}: {}",
        err.code().code(),
        err.message()
    );

    // Both tables survive, with their own rows.
    assert_eq!(row_count(&client, &src).await, 0);
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {dst} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("kept"));

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
}

// `ALTER TABLE t RENAME TO t` is the same duplicate-relation case, and the one
// most likely to be typed by accident. It must not half-rename the table.
#[tokio::test]
async fn test_alter_table_rename_to_its_own_name_is_refused() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_rename_self",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'x')"),
    )
    .await
    .unwrap();

    let err = assert_rejected(&client, &format!("ALTER TABLE {tbl} RENAME TO {tbl}")).await;
    assert_eq!(err.code().code(), "42P07", "got {}", err.message());
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// A rename of a table that is not there is `42P01`, and `IF EXISTS` turns that
// into the silent success PostgreSQL gives it.
#[tokio::test]
async fn test_alter_table_rename_of_a_missing_table() {
    let client = ready_client().await;
    let missing = unique_table_name("ddl_rename_missing");
    let target = unique_table_name("ddl_rename_missing_to");

    let err = assert_rejected(
        &client,
        &format!("ALTER TABLE {missing} RENAME TO {target}"),
    )
    .await;
    assert_eq!(err.code().code(), "42P01", "got {}", err.message());

    execute(
        &client,
        &format!("ALTER TABLE IF EXISTS {missing} RENAME TO {target}"),
    )
    .await
    .expect("IF EXISTS must make a rename of a missing table a no-op");

    // The no-op really was one: nothing was created under either name.
    assert!(
        simple_query_rows(&client, &format!("SELECT 1 FROM {target}"))
            .await
            .is_err(),
        "a no-op rename must not conjure the target table"
    );
}

// A rename mixed with a column op is a syntax error, as in PostgreSQL: the two
// actions cannot both be applied — one of them would silently be dropped, or
// applied to a name that no longer exists.
#[tokio::test]
async fn test_alter_table_rename_combined_with_a_column_op_is_refused() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ddl_rename_mixed",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let renamed = unique_table_name("ddl_rename_mixed_to");

    let err = assert_rejected(
        &client,
        &format!("ALTER TABLE {tbl} RENAME TO {renamed}, ADD COLUMN extra INTEGER"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_SYNTAX_ERROR,
        "got {}: {}",
        err.code().code(),
        err.message()
    );

    // Neither half happened: the old name still resolves, without the column.
    assert!(
        simple_query_rows(&client, &format!("SELECT id, v FROM {tbl}"))
            .await
            .is_ok(),
        "the table must survive a refused rename"
    );
    assert!(
        simple_query_rows(&client, &format!("SELECT extra FROM {tbl}"))
            .await
            .is_err(),
        "the refused statement must not have added the column"
    );
    assert!(
        simple_query_rows(&client, &format!("SELECT 1 FROM {renamed}"))
            .await
            .is_err(),
        "the refused statement must not have renamed the table"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// DROP — row 7 (🟡 only DROP TABLE is meaningful)
// ============================================================================

// `DROP INDEX`, `DROP VIEW` and `DROP SCHEMA` each have a handler of their own,
// because indexes, views and schemas are all modelled (an index is real, one per
// shard — see `sql_command_unsupported.rs` row 15; a view and a schema live in the
// coordinator catalog). Every other `DROP <kind>` lands in `handle_drop_table`,
// which only knows tables.
// VaireDB models no further non-table objects, so a `DROP <kind>` naming something
// that does not exist reports 42P01 — and must name the kind the client asked for,
// not "table".
// `DROP TABLE` itself (incl. `IF EXISTS`) is covered by `catalog_ddl.rs`.
#[tokio::test]
async fn test_drop_of_non_table_objects_reports_the_right_noun() {
    let client = ready_client().await;

    for kind in ["VIEW", "INDEX", "SCHEMA", "SEQUENCE"] {
        // A missing schema is 3F000 rather than 42P01, but the requirement is the
        // same: the error names the kind the client asked for.
        let name = unique_table_name("ddl_drop_obj");
        let sql = format!("DROP {kind} {name}");
        let err = assert_rejected(&client, &sql).await;
        let msg = err.message().to_lowercase();
        assert!(
            msg.contains(&kind.to_lowercase()),
            "`{sql}` must report an error naming the missing {kind}, got: {}",
            err.message()
        );
    }
}

// PostgreSQL 42809 "wrong object type": `DROP VIEW` naming a TABLE must refuse
// and leave the table intact. Before this was fixed the statement reached
// `handle_drop_table`, which found the table in the catalog and dropped it —
// a `DROP VIEW` (or `DROP INDEX`, `DROP SEQUENCE`) silently destroyed a table,
// the highest-consequence entry in the whole command gap map: a client typo on
// an object kind was unrecoverable data loss.
#[tokio::test]
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

    // `DROP VIEW <table>` must fail with "wrong object type"...
    let err = assert_rejected(&client, &format!("DROP VIEW {tbl}")).await;
    assert_eq!(
        err.code().code(),
        "42809",
        "DROP VIEW aimed at a table must be a wrong-object-type error, got: {}",
        err.message()
    );

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

// ============================================================================
// TRUNCATE — the doc's un-numbered write-path entry, now routed
// ============================================================================
//
// Not a DuckDB overview row, but a standard PostgreSQL statement clients do send,
// and one of the original fake-OK offenders: a client believed the table was
// emptied while every row stayed in place. It is now broadcast per shard by
// `handle_truncate`, which is why it lives here rather than in
// `sql_command_unsupported.rs`.
//
// What is deliberately still refused, because a per-shard broadcast cannot mean
// what PostgreSQL means: several tables in one statement (PG empties them in one
// transaction), CASCADE/RESTRICT (the catalog tracks no dependent objects) and
// RESTART IDENTITY (it tracks no sequences).

/// A table with one row per shard bucket, so "every shard was emptied" is
/// actually observable rather than a single-shard coincidence.
async fn table_with_a_row_per_shard(client: &Client, prefix: &str) -> (String, Vec<i64>) {
    let tbl = create_table(
        client,
        prefix,
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'v{id}')"),
        )
        .await
        .unwrap();
    }
    assert_eq!(row_count(client, &tbl).await, ids.len() as i64);
    (tbl, ids)
}

#[tokio::test]
async fn test_truncate_empties_every_shard() {
    let client = ready_client().await;
    let (tbl, ids) = table_with_a_row_per_shard(&client, "ddl_truncate").await;

    execute(&client, &format!("TRUNCATE TABLE {tbl}"))
        .await
        .unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "TRUNCATE must remove every shard's rows"
    );

    // Not just the aggregate: no individual shard may have kept its row.
    for id in &ids {
        let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = {id}"))
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "shard {} kept its row after TRUNCATE",
            bucket_of(*id)
        );
    }

    // The table itself survives with its schema, and still accepts writes.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES ({}, 'after')", ids[0]),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// `TABLE` is optional in PostgreSQL. Both spellings must reach the same handler —
// one of them classified as unsupported would be a coin-flip for the client.
#[tokio::test]
async fn test_truncate_without_the_table_keyword_also_empties() {
    let client = ready_client().await;
    let (tbl, _) = table_with_a_row_per_shard(&client, "ddl_truncate_bare").await;

    execute(&client, &format!("TRUNCATE {tbl}")).await.unwrap();
    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_truncate_a_missing_table_reports_table_not_found() {
    let client = ready_client().await;
    let missing = unique_table_name("ddl_truncate_absent");

    let err = assert_rejected(&client, &format!("TRUNCATE TABLE {missing}")).await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "TRUNCATE of a missing table must be 42P01, got: {}",
        err.message()
    );
}

// The refused forms must refuse *before* anything is emptied: a partial TRUNCATE
// that then reports an error is worse than no TRUNCATE at all.
#[tokio::test]
async fn test_truncate_forms_that_cannot_be_honored_leave_every_row_in_place() {
    let client = ready_client().await;
    let (tbl, _) = table_with_a_row_per_shard(&client, "ddl_truncate_refused").await;
    let (other, _) = table_with_a_row_per_shard(&client, "ddl_truncate_refused2").await;

    for sql in [
        format!("TRUNCATE TABLE {tbl}, {other}"),
        format!("TRUNCATE TABLE {tbl} CASCADE"),
        format!("TRUNCATE TABLE {tbl} RESTRICT"),
        format!("TRUNCATE TABLE {tbl} RESTART IDENTITY"),
    ] {
        assert_unsupported(&client, &sql).await;
        assert_eq!(
            row_count(&client, &tbl).await,
            SHARD_COUNT as i64,
            "`{sql}` was refused but still emptied rows"
        );
        assert_eq!(row_count(&client, &other).await, SHARD_COUNT as i64);
    }

    drop_table(&client, &other).await;
    drop_table(&client, &tbl).await;
}
