mod common;
use common::*;
use tokio_postgres::Client;

// PostgreSQL -> DuckDB/DataFusion dialect gaps.
//
// Two execution backends apply DIFFERENT dialect translation:
//   * Writes/DDL go through `sql_compat::transform_to_duckdb`, which rewrites
//     TO_CHAR->STRFTIME (incl. PG format string -> strftime specifiers),
//     BYTEA->BLOB, JSONB->JSON.
//   * SELECTs are planned by DataFusion and get a narrower transform
//     (`transform_to_char_format_for_read`) that keeps the native `to_char` but
//     translates its PG format string to strftime specifiers.
//
// Tests assert the PostgreSQL-correct result. Cases that already work through
// DataFusion/DuckDB pass and guard against regressions; genuine gaps are #[ignore]'d
// xfails with a specific reason, converting to passing tests once the gap is closed.
//
// Cleanup safety: CREATE TABLE failures roll back partial DDL on written nodes
// (DROP TABLE IF EXISTS), so the unsupported-type xfails cannot poison the shared
// cluster. Every test still uses `unique_table_name` + a trailing DROP.

// Helper: a small table with a timestamp/text payload for read-path function tests.
async fn setup_events(client: &Client, tbl: &str) {
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        client,
        &format!(
            "CREATE TABLE {tbl} (id INTEGER NOT NULL, ts TIMESTAMP NOT NULL, name VARCHAR NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await
    .unwrap();
    execute(
        client,
        &format!(
            "INSERT INTO {tbl} (id, ts, name) VALUES \
             (1, '2024-01-15 08:30:00', 'Alice'), \
             (2, '2025-06-20 14:00:00', 'bob'), \
             (3, '2026-03-10 23:59:00', 'Carol')"
        ),
    )
    .await
    .unwrap();
}

// ---- Function translation gaps (read path) ----

// EXTRACT(YEAR FROM ts) is standard SQL that DataFusion supports.
#[tokio::test]
async fn test_extract_year() {
    let client = ready_client().await;
    let tbl = unique_table_name("dg_extract");
    setup_events(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT EXTRACT(YEAR FROM ts) FROM {tbl} ORDER BY id"),
    )
    .await
    .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![2024, 2025, 2026]);

    drop_table(&client, &tbl).await;
}

// date_trunc('day', ts) is supported by DataFusion.
#[tokio::test]
async fn test_date_trunc_day() {
    let client = ready_client().await;
    let tbl = unique_table_name("dg_datetrunc");
    setup_events(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT date_trunc('day', ts) FROM {tbl} WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_deref(),
        Some("2024-01-15 00:00:00"),
        "date_trunc('day') should zero the time component"
    );

    drop_table(&client, &tbl).await;
}

// ILIKE (case-insensitive LIKE) is a PostgreSQL-ism that DataFusion supports.
#[tokio::test]
async fn test_ilike_case_insensitive() {
    let client = ready_client().await;
    let tbl = unique_table_name("dg_ilike");
    setup_events(&client, &tbl).await;

    // Matches 'Alice' case-insensitively.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE name ILIKE 'alice'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

// String concatenation with `||`.
#[tokio::test]
async fn test_string_concat_operator() {
    let client = ready_client().await;
    let tbl = unique_table_name("dg_concat");
    setup_events(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT name || '!' FROM {tbl} WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("Alice!"));

    drop_table(&client, &tbl).await;
}

// TO_CHAR in a projection list. The read path keeps DataFusion's native `to_char`
// but translates the PG format string ('YYYY-MM-DD') to strftime specifiers
// ('%Y-%m-%d') via `transform_to_char_format_for_read`, so the projection formats
// correctly instead of emitting the template literally.
#[tokio::test]
async fn test_to_char_in_projection() {
    let client = ready_client().await;
    let tbl = unique_table_name("dg_tochar");
    setup_events(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT TO_CHAR(ts, 'YYYY-MM-DD') FROM {tbl} WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("2024-01-15"));

    drop_table(&client, &tbl).await;
}

// ---- Type translation gaps (write/DDL path) ----

// SERIAL is a PostgreSQL pseudo-type (auto-increment). It is not mapped to a DuckDB
// sequence/identity type, so the per-shard CREATE fails at DuckDB.
#[tokio::test]
#[ignore = "known gap: SERIAL is not mapped to a DuckDB sequence/identity column. Take a look to https://duckdb.org/docs/current/sql/statements/create_sequence."]
async fn test_serial_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dg_serial",
        &format!("(id INTEGER NOT NULL, seq SERIAL) {CREATE_OPTS}"),
    )
    .await;
    execute(&client, &format!("INSERT INTO {tbl} (id) VALUES (1)"))
        .await
        .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT seq FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("1"),
        "SERIAL should auto-assign 1"
    );

    drop_table(&client, &tbl).await;
}

// TIMESTAMPTZ (timestamp with time zone) is a PostgreSQL type. DuckDB supports
// TIMESTAMPTZ, but the catalog stores the raw PG type string and DataFusion's
// schema mapping may not handle it, so this guards the round trip.
#[tokio::test]
async fn test_timestamptz_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dg_tstz",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMPTZ NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, ts) VALUES (1, '2026-06-03 12:30:00+00')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the TIMESTAMPTZ row should read back");

    drop_table(&client, &tbl).await;
}

// PostgreSQL array column type (INTEGER[]). DuckDB stores this as a LIST; the PG
// `[]` DDL syntax passes through to DuckDB unchanged, and the catalog type string
// is modeled as an Arrow `List` so the column reads back as a PG array (`{1,2,3}`).
#[tokio::test]
async fn test_array_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dg_array",
        &format!("(id INTEGER NOT NULL, tags INTEGER[]) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, tags) VALUES (1, ARRAY[1, 2, 3])"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT tags FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the array row should read back");
    assert_eq!(
        rows[0][0].as_deref(),
        Some("{1,2,3}"),
        "INTEGER[] should read back in PostgreSQL array text form"
    );

    drop_table(&client, &tbl).await;
}
