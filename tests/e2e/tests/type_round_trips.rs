mod common;
use common::*;

// Values must survive the coordinator -> DuckDB -> coordinator round trip. The
// pgwire handler stringifies everything as TEXT, and write/read predicates are
// re-serialized to DuckDB via sqlparser's `to_string()`, so these tests guard a
// fragile encoding surface.

#[tokio::test]
async fn test_null_values_all_columns() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_null",
        &format!("(id INTEGER NOT NULL, name VARCHAR, amount INTEGER) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, name, amount) VALUES (1, NULL, NULL)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id, name, amount FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1], None, "name should round-trip as SQL NULL");
    assert_eq!(rows[0][2], None, "amount should round-trip as SQL NULL");

    // COUNT(*) counts the row; COUNT(col) excludes NULLs.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT COUNT(*), COUNT(name), COUNT(amount) FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("0"));
    assert_eq!(rows[0][2].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_decimal_precision() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_decimal",
        &format!("(id INTEGER NOT NULL, amount DECIMAL(10, 2)) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, amount) VALUES (1, 1234.56), (2, 0.01), (3, 9999.99)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT amount FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<f64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert!((got[0] - 1234.56).abs() < 0.001);
    assert!((got[1] - 0.01).abs() < 0.001);
    assert!((got[2] - 9999.99).abs() < 0.001);

    let rows = simple_query_rows(&client, &format!("SELECT SUM(amount) FROM {tbl}"))
        .await
        .unwrap();
    let sum: f64 = rows[0][0].as_deref().unwrap().parse().unwrap();
    assert!((sum - 11234.56).abs() < 0.01, "got sum {sum}");

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_bigint_boundaries() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bigint",
        &format!("(id INTEGER NOT NULL, big BIGINT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let max = i64::MAX;
    let min = i64::MIN;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, big) VALUES (1, {max}), (2, {min}), (3, 0)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT big FROM {tbl} ORDER BY big"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![min, 0, max]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_string_with_quotes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_quotes",
        &format!("(id INTEGER NOT NULL, name VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // Single quote (escaped), comma, and SQL LIKE wildcards.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, name) VALUES (1, 'O''Brien'), (2, 'a,b'), (3, '100%_off')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT name FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().to_string())
        .collect();
    assert_eq!(got, vec!["O'Brien", "a,b", "100%_off"]);

    // Point lookup whose predicate re-serializes the escaped quote to DuckDB.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE name = 'O''Brien'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_float_special_values() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_float",
        &format!("(id INTEGER NOT NULL, v DOUBLE PRECISION NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'inf'), (2, '-inf'), (3, 'nan')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("inf"));
    assert_eq!(rows[1][0].as_deref(), Some("-inf"));
    assert_eq!(rows[2][0].as_deref(), Some("NaN"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_timestamp_round_trip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_ts",
        &format!(
            "(id INTEGER NOT NULL, ts TIMESTAMP NOT NULL, d DATE NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, ts, d) VALUES (1, '2026-06-03 12:30:00', '2026-06-03')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT ts, d FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("2026-06-03 12:30:00"));
    assert_eq!(rows[0][1].as_deref(), Some("2026-06-03"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_jsonb_round_trip() {
    let client = ready_client().await;
    // JSONB is coerced to DuckDB JSON on the write path (transform_to_duckdb).
    let tbl = create_table(
        &client,
        "tr_jsonb",
        &format!("(id INTEGER NOT NULL, doc JSONB NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, doc) VALUES (1, '{{\"a\": 1, \"b\": [2, 3]}}')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT doc FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("{\"a\": 1, \"b\": [2, 3]}"));

    drop_table(&client, &tbl).await;
}
