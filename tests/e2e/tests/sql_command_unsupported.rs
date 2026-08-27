mod common;
use common::*;
use tokio_postgres::Client;

// Rows 8-36 of docs/specs/gap-analysis-command.md — every statement the doc
// marks ❌. See `sql_command_select.rs` for the four-file layout and the
// passing/#[ignore] convention.
//
//     cd tests/e2e && cargo test --test sql_command_unsupported -- --ignored --test-threads=1
//
// Each statement gets up to two tests:
//
//   * `test_<x>_currently_rejected` (passing) — the statement fails loudly. This
//     is the contract that matters to a client TODAY: these statements used to
//     fall through the dispatch catch-all and return a fake `OK` with NO
//     execution, so a client believed a transaction opened, a table was
//     truncated, or a view was created when nothing happened. A statement may
//     fail at either rejection point — parse (`42601`, most DuckDB-only syntax)
//     or classification (`0A000` + `[VDB-1004]`) — so `assert_unsupported` is
//     used where the doc pins `0A000`, and `assert_rejected` where either is
//     acceptable.
//
//   * `test_<x>_<target behavior>` + `#[ignore]` — the PostgreSQL-correct
//     behavior, written so it fails by construction until the gap closes.
//
// Sections follow the doc's "Prioritized gaps (wire-protocol impact)" ranking,
// then the statements it leaves unranked, then the ones it declares out of
// scope. Statements in the last group get NO xfail: they are single-node DuckDB
// concerns that should stay rejected, and their test is there to keep them
// rejected.
//
// Two ❌ rows are covered by their sibling files instead, because they belong to
// a statement family that file already owns:
//   * row 25 `MERGE INTO` / upsert -> `sql_command_dml.rs`;
//   * `DROP VIEW`/`INDEX`/`SCHEMA`/`SEQUENCE`, which all land in the DROP TABLE
//     handler -> `sql_command_ddl.rs` (row 7).

/// A small two-column table with one row per shard bucket, for the statements
/// that need something to operate on.
async fn setup_rows(client: &Client, tbl: &str) -> Vec<i64> {
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
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
    ids
}

// ============================================================================
// 1. Transaction control — row 33 (BEGIN / COMMIT / ROLLBACK / SAVEPOINT)
// ============================================================================
//
// Prioritized gap #1: most PG drivers open a transaction implicitly, so this is
// the single biggest client-compatibility gap. Even auto-commit emulation
// (accept BEGIN/COMMIT as no-ops, treat every statement as its own transaction)
// would unblock many clients — but ROLLBACK cannot be faked, which is why the
// xfails below assert both halves.

#[tokio::test]
async fn test_transaction_control_currently_rejected() {
    let client = ready_client().await;
    assert_unsupported(&client, "BEGIN").await;
    assert_unsupported(&client, "START TRANSACTION").await;
    assert_unsupported(&client, "COMMIT").await;
    assert_unsupported(&client, "ROLLBACK").await;
    assert_unsupported(&client, "SAVEPOINT sp1").await;
    assert_unsupported(&client, "RELEASE SAVEPOINT sp1").await;
}

#[tokio::test]
#[ignore = "gap (row 33): transaction control is rejected 0A000 — DML is not transactional, so BEGIN/COMMIT cannot be honored"]
async fn test_commit_persists_writes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_tx_commit",
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
    execute(&client, "COMMIT").await.unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        1,
        "a committed INSERT must be visible"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 33): ROLLBACK is rejected 0A000 — per-shard writes are applied immediately, so there is nothing to undo"]
async fn test_rollback_discards_writes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_tx_rollback",
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

    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "a rolled-back INSERT must leave no row behind"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 2. Session configuration — rows 28-31 (SET / RESET / SHOW / SET VARIABLE)
// ============================================================================
//
// Prioritized gap #2: drivers send `SET` (client_encoding, search_path,
// application_name, extra_float_digits, …) at connect time, so a rejection here
// can break a client before it runs a single query.

#[tokio::test]
async fn test_set_and_show_currently_rejected() {
    let client = ready_client().await;
    // SET and SHOW parse, so they reach classification: 0A000, labelled "SET" /
    // "SHOW" (`SHOW ALL` included).
    assert_unsupported(&client, "SET search_path TO myschema").await;
    assert_unsupported(&client, "SET client_encoding TO 'UTF8'").await;
    assert_unsupported(&client, "SET application_name = 'vairedb-e2e'").await;
    assert_unsupported(&client, "SHOW search_path").await;
    assert_unsupported(&client, "SHOW ALL").await;
    // RESET does NOT parse under sqlparser's PostgreSqlDialect, so it fails one
    // step earlier, at 42601 — as does DuckDB's `SET VARIABLE` (row 30).
    assert_rejected(&client, "RESET search_path").await;
    assert_rejected(&client, "RESET ALL").await;
    assert_rejected(&client, "SET VARIABLE my_var = 42").await;
}

#[tokio::test]
#[ignore = "gap (rows 29/31): SET and SHOW are rejected 0A000 — the coordinator tracks no session runtime parameters"]
async fn test_set_then_show_round_trips() {
    let client = ready_client().await;

    execute(&client, "SET search_path TO myschema")
        .await
        .unwrap();

    let rows = simple_query_rows(&client, "SHOW search_path")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "SHOW must return exactly one row");
    assert_eq!(
        rows[0][0].as_deref(),
        Some("myschema"),
        "SHOW must report the value SET on this session"
    );
}

#[tokio::test]
#[ignore = "gap (row 28): RESET is rejected at parse (42601) — sqlparser's PostgreSqlDialect has no RESET, and session config is not modeled anyway"]
async fn test_reset_restores_the_default() {
    let client = ready_client().await;

    execute(&client, "SET search_path TO myschema")
        .await
        .unwrap();
    execute(&client, "RESET search_path").await.unwrap();

    let rows = simple_query_rows(&client, "SHOW search_path")
        .await
        .unwrap();
    assert_ne!(
        rows[0][0].as_deref(),
        Some("myschema"),
        "RESET must discard the session value"
    );
}

// The specific compatibility shape the doc calls out: the SETs a driver issues
// on connect should be accepted (no-op is fine) rather than failing the session.
#[tokio::test]
#[ignore = "gap (row 29): every SET is rejected 0A000, including the no-op-safe parameters drivers send at connect time"]
async fn test_driver_startup_sets_are_accepted() {
    let client = ready_client().await;

    for sql in [
        "SET client_encoding TO 'UTF8'",
        "SET application_name = 'vairedb-e2e'",
        "SET extra_float_digits = 3",
        "SET DateStyle TO 'ISO'",
    ] {
        execute(&client, sql)
            .await
            .unwrap_or_else(|e| panic!("driver startup statement `{sql}` must be accepted: {e}"));
    }
}

// ============================================================================
// 3. COPY — row 14
// ============================================================================
//
// Prioritized gap #3 and already on the roadmap ("massive data import SQL
// command"); `copy_handler` is still a `NoopHandler`.
//
// Only the file-based forms are exercised. `COPY … FROM STDIN` / `TO STDOUT`
// would put the connection into the copy sub-protocol, which the Noop copy
// handler does not drive, so a client-side test of those belongs with the
// protocol work rather than here.

#[tokio::test]
async fn test_copy_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_copy");
    setup_rows(&client, &tbl).await;

    assert_unsupported(
        &client,
        &format!("COPY {tbl} TO '/tmp/{tbl}.csv' (FORMAT CSV)"),
    )
    .await;
    assert_unsupported(
        &client,
        &format!("COPY {tbl} FROM '/tmp/{tbl}.csv' (FORMAT CSV)"),
    )
    .await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 14): COPY is rejected 0A000 and copy_handler is a NoopHandler — export must gather from every shard and import must route each row by its shard key"]
async fn test_copy_to_file_then_back_round_trips() {
    let client = ready_client().await;
    let src = unique_table_name("un_copyx_src");
    let ids = setup_rows(&client, &src).await;

    // Server-side path, inside the coordinator container.
    let path = format!("/tmp/{src}.csv");
    execute(
        &client,
        &format!("COPY {src} TO '{path}' (FORMAT CSV, HEADER)"),
    )
    .await
    .unwrap();

    let dst = create_table(
        &client,
        "un_copyx_dst",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("COPY {dst} FROM '{path}' (FORMAT CSV, HEADER)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids;
    want.sort_unstable();
    assert_eq!(
        got, want,
        "COPY must export every shard's rows and re-import them exactly once"
    );

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
}

// ============================================================================
// 4. Views — rows 20 (CREATE VIEW) and 8 (ALTER VIEW)
// ============================================================================
//
// Prioritized gap #4: the common BI/reporting need. `DROP VIEW` is row 7 and is
// tested in `sql_command_ddl.rs`, where it is a hazard rather than a no-op: it
// reaches the DROP TABLE handler and can drop a same-named table.

#[tokio::test]
async fn test_create_and_alter_view_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewbase");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_view");

    // Both CREATE spellings parse and are rejected 0A000, labelled "CREATE VIEW".
    assert_unsupported(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl} WHERE id > 0"),
    )
    .await;
    assert_unsupported(
        &client,
        &format!("CREATE OR REPLACE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await;
    // `ALTER VIEW … RENAME TO` fails earlier, at parse (42601): sqlparser's
    // PostgreSqlDialect only accepts `ALTER VIEW … AS <query>`.
    assert_rejected(&client, &format!("ALTER VIEW {view} RENAME TO {view}_2")).await;

    // Nothing was created, so the view name does not resolve.
    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .is_err(),
        "a rejected CREATE VIEW must not leave a queryable relation"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 20): CREATE VIEW is rejected 0A000 — the catalog models only tables, so a view has nowhere to live"]
async fn test_view_lifecycle_create_select_drop() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewx_base");
    let ids = setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewx");

    // A view over a sharded table must read through to every shard.
    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id, v FROM {tbl}"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {view} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), ids.len(), "the view must expose every base row");

    // A filtering view narrows the result.
    let filtered = unique_table_name("un_viewx_filtered");
    let one = ids[0];
    execute(
        &client,
        &format!("CREATE VIEW {filtered} AS SELECT id FROM {tbl} WHERE id = {one}"),
    )
    .await
    .unwrap();
    let rows = simple_query_rows(&client, &format!("SELECT id FROM {filtered}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some(one.to_string().as_str()));

    // DROP VIEW removes it without touching the base table.
    execute(&client, &format!("DROP VIEW {filtered}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 8): ALTER VIEW … RENAME TO does not even parse (42601), and views are unsupported, so there is nothing to alter"]
async fn test_alter_view_renames_the_view() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_alterview_base");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_alterview");
    let renamed = format!("{view}_r");

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();
    execute(&client, &format!("ALTER VIEW {view} RENAME TO {renamed}"))
        .await
        .unwrap();

    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {renamed}"))
            .await
            .is_ok(),
        "the renamed view must resolve"
    );
    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .is_err(),
        "the old view name must stop resolving"
    );

    execute(&client, &format!("DROP VIEW {renamed}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

// ============================================================================
// 5. EXPLAIN and DESCRIBE — rows 27 and 22
// ============================================================================
//
// Prioritized gap #5: widely used by tooling and humans for query inspection and
// schema exploration. `DESCRIBE` parses to the same sqlparser node family as
// EXPLAIN, so both carry the "EXPLAIN" label today.

#[tokio::test]
async fn test_explain_describe_pragma_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explain");
    setup_rows(&client, &tbl).await;

    assert_unsupported(&client, &format!("EXPLAIN SELECT * FROM {tbl}")).await;
    assert_unsupported(&client, &format!("EXPLAIN ANALYZE SELECT * FROM {tbl}")).await;
    assert_unsupported(&client, &format!("DESCRIBE {tbl}")).await;
    // Profiling PRAGMAs share row 27.
    assert_rejected(&client, "PRAGMA enable_profiling").await;
    assert_rejected(&client, "PRAGMA database_list").await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 27): EXPLAIN is rejected 0A000 — the coordinator has no plan-rendering path, although SELECTs already build a DataFusion LogicalPlan"]
async fn test_explain_returns_a_plan() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainx");
    setup_rows(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("EXPLAIN SELECT id FROM {tbl} WHERE id > 0"),
    )
    .await
    .unwrap();
    assert!(
        !rows.is_empty(),
        "EXPLAIN must return at least one plan row"
    );

    let plan: String = rows
        .iter()
        .filter_map(|r| r[r.len() - 1].as_deref())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains(&tbl),
        "the plan should name the relation being scanned, got:\n{plan}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 27): EXPLAIN ANALYZE is rejected 0A000 — no per-shard execution metrics are collected or aggregated"]
async fn test_explain_analyze_reports_execution() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainax");
    setup_rows(&client, &tbl).await;

    let rows = simple_query_rows(&client, &format!("EXPLAIN ANALYZE SELECT id FROM {tbl}"))
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "EXPLAIN ANALYZE must return execution output"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 22): DESCRIBE is rejected 0A000 — schema introspection is only reachable through the emulated pg_catalog SELECTs"]
async fn test_describe_lists_the_columns() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_describex");
    setup_rows(&client, &tbl).await;

    let rows = simple_query_rows(&client, &format!("DESCRIBE {tbl}"))
        .await
        .unwrap();
    let mut names: Vec<String> = rows
        .iter()
        .filter_map(|r| r[0].as_deref().map(|s| s.to_string()))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["id".to_string(), "v".to_string()],
        "DESCRIBE must list one row per column"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 6. MERGE INTO / upsert — row 25
// ============================================================================
//
// Prioritized gap #6. Tested in `sql_command_dml.rs` alongside INSERT, since a
// merge is DML and shares the shard-key routing constraints:
// `test_merge_into_currently_rejected` / `test_merge_into_updates_and_inserts`
// and the `ON CONFLICT` pair.

// ============================================================================
// 7. Indexes — row 15
// ============================================================================
//
// Prioritized gap #7. `DROP INDEX` is row 7 (see `sql_command_ddl.rs`).

#[tokio::test]
async fn test_create_index_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_index");
    setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idx");

    assert_unsupported(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)")).await;
    assert_unsupported(
        &client,
        &format!("CREATE UNIQUE INDEX {idx}_u ON {tbl} (id)"),
    )
    .await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 15): CREATE INDEX is rejected 0A000 — an index would have to be created on, and tracked for, every shard"]
async fn test_create_index_then_drop_index() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_indexx");
    let ids = setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idxx");

    execute(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)"))
        .await
        .unwrap();

    // Creating an index changes performance, not results.
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();

    drop_table(&client, &tbl).await;
}

// A UNIQUE index on the SHARD KEY is the one uniqueness constraint a sharded
// store can enforce without cross-shard coordination: equal keys always hash to
// the same shard, so per-shard enforcement is globally correct.
#[tokio::test]
#[ignore = "gap (row 15): CREATE UNIQUE INDEX is rejected 0A000, so nothing prevents a duplicate shard-key row"]
async fn test_unique_index_on_shard_key_is_enforced() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_uniqidx",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let idx = unique_table_name("un_uniqidx_i");

    execute(&client, &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (id)"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'first')"),
    )
    .await
    .unwrap();

    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'dup')"),
    )
    .await;
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 8. Schemas — row 17
// ============================================================================
//
// Prioritized gap #8. The coordinator's namespace is flat: `canonical_table_name`
// keeps only the LAST identifier part, so `schema_a.t` and `schema_b.t` collapse
// onto one catalog key. `identifier_rewrite.rs::test_schema_qualified_name_collision`
// documents that collision from the naming side; this is the statement side.

#[tokio::test]
async fn test_create_schema_currently_rejected() {
    let client = ready_client().await;
    let schema = unique_table_name("un_schema");

    assert_unsupported(&client, &format!("CREATE SCHEMA {schema}")).await;
    assert_unsupported(&client, &format!("CREATE SCHEMA IF NOT EXISTS {schema}")).await;
    // `ALTER SCHEMA` is not in sqlparser's PostgreSqlDialect at all: it fails at
    // parse (42601, "expected one of VIEW or TYPE or TABLE or INDEX …").
    assert_rejected(
        &client,
        &format!("ALTER SCHEMA {schema} RENAME TO {schema}_2"),
    )
    .await;
}

#[tokio::test]
#[ignore = "gap (row 17): CREATE SCHEMA is rejected 0A000 and the namespace is flat — two same-named tables in different schemas collide on one catalog key and one set of shards"]
async fn test_schemas_are_independent_namespaces() {
    let client = ready_client().await;
    let a = unique_table_name("un_ns_a");
    let b = unique_table_name("un_ns_b");

    execute(&client, &format!("CREATE SCHEMA {a}"))
        .await
        .unwrap();
    execute(&client, &format!("CREATE SCHEMA {b}"))
        .await
        .unwrap();

    for schema in [&a, &b] {
        execute(
            &client,
            &format!("CREATE TABLE {schema}.t (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
        )
        .await
        .unwrap();
    }

    execute(
        &client,
        &format!("INSERT INTO {a}.t (id, v) VALUES (1, 'in_a')"),
    )
    .await
    .unwrap();

    // The two tables are independent: the row written to a.t is not in b.t.
    assert_eq!(row_count(&client, &format!("{a}.t")).await, 1);
    assert_eq!(
        row_count(&client, &format!("{b}.t")).await,
        0,
        "a.t and b.t must not share storage"
    );

    execute(&client, &format!("DROP TABLE IF EXISTS {a}.t"))
        .await
        .unwrap();
    execute(&client, &format!("DROP TABLE IF EXISTS {b}.t"))
        .await
        .unwrap();
    execute(&client, &format!("DROP SCHEMA {a}")).await.unwrap();
    execute(&client, &format!("DROP SCHEMA {b}")).await.unwrap();
}

// ============================================================================
// 9. Sequences — row 19
// ============================================================================
//
// Prioritized gap #9. Also the blocker behind the SERIAL gap in
// `data_types_round_trips.rs`: SERIAL is defined in terms of a sequence.

#[tokio::test]
async fn test_create_sequence_currently_rejected() {
    let client = ready_client().await;
    let seq = unique_table_name("un_seq");

    assert_unsupported(&client, &format!("CREATE SEQUENCE {seq}")).await;
    assert_unsupported(&client, &format!("CREATE SEQUENCE {seq} START WITH 100")).await;
    // `ALTER SEQUENCE`, like `ALTER SCHEMA`, is not in sqlparser's
    // PostgreSqlDialect: parse rejection (42601).
    assert_rejected(&client, &format!("ALTER SEQUENCE {seq} RESTART WITH 1")).await;
}

// A distributed sequence has to hand out ids that are unique across shards, and
// `nextval()` in the shard-key position also requires routing a non-literal key.
#[tokio::test]
#[ignore = "gap (row 19): CREATE SEQUENCE is rejected 0A000 — there is no distributed sequence, and nextval() in the shard-key position is not a routable literal"]
async fn test_sequence_supplies_unique_ids() {
    let client = ready_client().await;
    let seq = unique_table_name("un_seqx");
    let tbl = create_table(
        &client,
        "un_seqx_t",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(&client, &format!("CREATE SEQUENCE {seq} START WITH 1"))
        .await
        .unwrap();

    for i in 0..3 {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES (nextval('{seq}'), 'row{i}')"),
        )
        .await
        .unwrap();
    }

    let rows = simple_query_rows(&client, &format!("SELECT DISTINCT id FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "each nextval() must yield a distinct id across shards"
    );

    execute(&client, &format!("DROP SEQUENCE {seq}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

// ============================================================================
// 10. VACUUM — row 36
// ============================================================================
//
// VACUUM's scope is the doc's one open question: it was listed both as prioritized
// gap #10 ("distributed vacuum management") and in the "intentionally out of scope"
// list. The doc now follows the prioritized ranking, and so does this section by
// keeping an xfail. If VACUUM is settled as out of scope instead, delete the xfail
// and move the rejection test down to section 14 — that test holds either way.
//
// VACUUM is also the one prioritized gap that fails at PARSE rather than
// classification: sqlparser's PostgreSqlDialect does not implement it, so support
// starts one layer lower than the other rows in this file.

#[tokio::test]
async fn test_vacuum_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_vacuum");
    setup_rows(&client, &tbl).await;

    assert_rejected(&client, "VACUUM").await;
    assert_rejected(&client, &format!("VACUUM {tbl}")).await;
    assert_rejected(&client, &format!("VACUUM ANALYZE {tbl}")).await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 36 / prioritized #10): VACUUM is rejected at parse (42601) — sqlparser's PostgreSqlDialect has no VACUUM, and per-shard storage maintenance is not exposed through the coordinator"]
async fn test_vacuum_is_accepted_and_preserves_rows() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_vacuumx");
    let ids = setup_rows(&client, &tbl).await;

    // Churn some rows so a vacuum has something to reclaim.
    execute(&client, &format!("DELETE FROM {tbl} WHERE id = {}", ids[0]))
        .await
        .unwrap();

    execute(&client, &format!("VACUUM {tbl}")).await.unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        (ids.len() - 1) as i64,
        "VACUUM must not change visible rows"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 11. PIVOT / UNPIVOT — rows 26 and 34
// ============================================================================
//
// Prioritized gap #11. DuckDB-only syntax, so unlike the rest of this file these
// fail at parse (`42601`) under `PostgreSqlDialect` rather than at classification:
// supporting them means teaching the parser the statement first.

#[tokio::test]
async fn test_pivot_and_unpivot_currently_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_pivot",
        &format!("(id INTEGER NOT NULL, cat VARCHAR, amt INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, cat, amt) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)"
        ),
    )
    .await
    .unwrap();

    assert_rejected(&client, &format!("PIVOT {tbl} ON cat USING SUM(amt)")).await;
    assert_rejected(
        &client,
        &format!("UNPIVOT {tbl} ON amt INTO NAME measure VALUE val"),
    )
    .await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 26): PIVOT is DuckDB-only syntax that sqlparser's PostgreSqlDialect does not parse (42601)"]
async fn test_pivot_reshapes_rows_into_columns() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_pivotx",
        &format!("(id INTEGER NOT NULL, cat VARCHAR, amt INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, cat, amt) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)"
        ),
    )
    .await
    .unwrap();

    // One row: category 'a' sums to 40, 'b' to 20.
    let rows = simple_query_rows(&client, &format!("PIVOT {tbl} ON cat USING SUM(amt)"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "PIVOT must collapse the rows into one");
    let mut values: Vec<String> = rows[0].iter().filter_map(|c| c.clone()).collect();
    values.sort();
    assert!(
        values.contains(&"40".to_string()) && values.contains(&"20".to_string()),
        "PIVOT must aggregate per category, got {values:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 34): UNPIVOT is DuckDB-only syntax that sqlparser's PostgreSqlDialect does not parse (42601)"]
async fn test_unpivot_reshapes_columns_into_rows() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_unpivotx",
        &format!("(id INTEGER NOT NULL, q1 INTEGER, q2 INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, q1, q2) VALUES (1, 10, 20)"),
    )
    .await
    .unwrap();

    // The single row becomes one row per unpivoted column.
    let rows = simple_query_rows(
        &client,
        &format!("UNPIVOT {tbl} ON q1, q2 INTO NAME quarter VALUE amount"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2, "UNPIVOT must emit one row per column");

    let mut amounts: Vec<String> = rows.iter().filter_map(|r| r[r.len() - 1].clone()).collect();
    amounts.sort();
    assert_eq!(amounts, vec!["10".to_string(), "20".to_string()]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 12. TRUNCATE
// ============================================================================
//
// Not a DuckDB overview row, but a standard PostgreSQL statement the doc names
// in its classification table, and one of the original fake-OK offenders: a
// client would believe the table was emptied while every row stayed in place.

#[tokio::test]
async fn test_truncate_currently_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_truncate");
    let ids = setup_rows(&client, &tbl).await;

    assert_unsupported(&client, &format!("TRUNCATE TABLE {tbl}")).await;
    assert_unsupported(&client, &format!("TRUNCATE {tbl}")).await;

    assert_eq!(
        row_count(&client, &tbl).await,
        ids.len() as i64,
        "a rejected TRUNCATE must leave every row in place"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap: TRUNCATE is rejected 0A000 — emptying the table means a broadcast DELETE across every shard under quorum"]
async fn test_truncate_empties_every_shard() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_truncatex");
    setup_rows(&client, &tbl).await;

    execute(&client, &format!("TRUNCATE TABLE {tbl}"))
        .await
        .unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "TRUNCATE must remove every shard's rows"
    );

    // The table itself survives and still accepts writes.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'after')"),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 13. Unranked ❌ statements — rejection contract only
// ============================================================================
//
// Rows the doc marks ❌ but does not rank in the prioritized list. They get no
// xfail: no target behavior has been specified for them yet, so all that is
// pinned is the honest failure. Add an xfail here when one is prioritized.
//
// The trailing code on each line is the rejection point observed against the e2e
// cluster: `0A000` means the statement parses and is refused by classification (so
// only routing/execution is missing), `42601` means sqlparser's PostgreSqlDialect
// does not know the syntax at all (so support starts one layer lower).

#[tokio::test]
async fn test_unranked_statements_are_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_unranked");
    setup_rows(&client, &tbl).await;

    let statements = [
        // row 9 — ANALYZE (no planner statistics surface)                 0A000
        format!("ANALYZE {tbl}"),
        // row 11 — CALL (no stored/table procedures)                      0A000
        "CALL my_procedure()".to_string(),
        // row 13 — COMMENT ON (no catalog comment storage)                0A000
        format!("COMMENT ON TABLE {tbl} IS 'a comment'"),
        // row 16 — CREATE MACRO (DuckDB-only)                             42601
        "CREATE MACRO one() AS 1".to_string(),
        // row 21 — CREATE TYPE (see also the ENUM gap in
        //          data_types_round_trips.rs)                             0A000
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')".to_string(),
        // row 32 — SUMMARIZE (DuckDB-only)                                42601
        format!("SUMMARIZE {tbl}"),
    ];

    for sql in &statements {
        assert_rejected(&client, sql).await;
    }

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 14. Intentionally out of scope — must STAY rejected
// ============================================================================
//
// The doc's closing list: single-node DuckDB concerns that do not map onto a
// sharded coordinator. These deliberately have no xfail — this test is the guard
// that they are never quietly accepted, since accepting one would mean it ran on
// an arbitrary single node.

#[tokio::test]
async fn test_out_of_scope_statements_stay_rejected() {
    let client = ready_client().await;

    // Same convention as section 13: the trailing code is the observed rejection
    // point. These are split across both layers, which is why the assertion is the
    // tolerant one — what matters is only that none of them is ever accepted.
    let statements = [
        // row 10 — ATTACH / DETACH (single-node DuckDB attachment)  0A000 / 42601
        "ATTACH 'other.db' AS other",
        "DETACH other",
        // row 24 — INSTALL / LOAD (per-node extensions)             42601 / 0A000
        "INSTALL httpfs",
        "LOAD httpfs",
        // row 12 — CHECKPOINT (per-shard storage concern)                   42601
        "CHECKPOINT",
        // row 23 — EXPORT / IMPORT DATABASE (whole-DB dump/load)            42601
        "EXPORT DATABASE '/tmp/vairedb_export'",
        "IMPORT DATABASE '/tmp/vairedb_export'",
        // row 35 — USE (no database switching; the namespace is flat)       0A000
        "USE other_db",
        // row 18 — CREATE SECRET (superseded by VaireDB's own anonymization
        // secret, written via INSERT INTO
        // vairedb_catalog.anonymization_secret)                             0A000
        "CREATE SECRET my_secret (TYPE S3, KEY_ID 'k', SECRET 's')",
    ];

    for sql in statements {
        assert_rejected(&client, sql).await;
    }
}
