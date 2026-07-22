mod common;
use common::*;

// PostgreSQL extended query protocol (Parse/Bind/Describe/Execute) — prepared
// statements and typed parameter binding. tokio-postgres's typed
// `query`/`execute` drive the EXTENDED protocol (unlike `simple_query`, used by
// most other tests).
//
// The coordinator binds parameters typed end to end: SELECTs cache a DataFusion
// LogicalPlan and bind via `replace_params_with_values`; writes route decoded
// values to DuckDB as prepared-statement parameters. No textual substitution is
// performed, so a literal `$N` inside a string is never clobbered.

#[tokio::test]
async fn test_prepared_statement_with_params() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_select",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2, 'b')"),
    )
    .await
    .unwrap();

    // Typed query → extended protocol with a bound $1 parameter.
    let id: i32 = 2;
    let rows = client
        .query(&format!("SELECT v FROM {tbl} WHERE id = $1"), &[&id])
        .await
        .expect("parameterized SELECT over the extended protocol should succeed");
    assert_eq!(rows.len(), 1, "expected exactly one matching row");
    let v: &str = rows[0].get(0);
    assert_eq!(v, "b", "bound parameter should select id = 2");

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_parameterized_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_insert",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Typed execute → extended protocol with bound $1, $2 parameters.
    let id: i32 = 7;
    let val = "seven";
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, v) VALUES ($1, $2)"),
            &[&id, &val],
        )
        .await
        .expect("parameterized INSERT over the extended protocol should succeed");
    assert_eq!(affected, 1, "INSERT should report one row");

    // The bound row must be readable back.
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = 7"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("seven"));

    drop_table(&client, &tbl).await;
}

// Regression for the old textual-substitution bug: a literal `$1` appearing
// inside a string value must NOT be replaced by the bound parameter. With true
// binding the stored string is verbatim.
#[tokio::test]
async fn test_dollar_token_inside_string_is_not_substituted() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_dollar",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // $1 binds to 100; the literal '$1 is not bound' must survive untouched.
    let id: i32 = 100;
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, v) VALUES ($1, '$1 is not bound')"),
            &[&id],
        )
        .await
        .expect("INSERT with a $-token string literal should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = 100"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_deref(),
        Some("$1 is not bound"),
        "string literal containing $1 must not be replaced by the bound value"
    );

    drop_table(&client, &tbl).await;
}

// Typed binding across several column types, including NULL.
#[tokio::test]
async fn test_parameterized_insert_typed_columns() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_typed",
        &format!(
            "(id INTEGER NOT NULL, amount DOUBLE PRECISION, flag BOOLEAN, note VARCHAR) {CREATE_OPTS}"
        ),
    )
    .await;

    let id: i32 = 11;
    let amount: f64 = 19.95;
    let flag: bool = true;
    let note: Option<&str> = None;
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, amount, flag, note) VALUES ($1, $2, $3, $4)"),
            &[&id, &amount, &flag, &note],
        )
        .await
        .expect("typed parameterized INSERT should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(
        &client,
        &format!("SELECT amount, flag, note FROM {tbl} WHERE id = 11"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("19.95"));
    assert_eq!(rows[0][1].as_deref(), Some("true"));
    assert_eq!(rows[0][2].as_deref(), None, "NULL note should round-trip");

    drop_table(&client, &tbl).await;
}

// The shard key itself bound as a parameter must route to the same shard the
// equivalent literal would, so the row is found on read-back.
#[tokio::test]
async fn test_shard_key_as_bound_parameter_routes_correctly() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_shardkey",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Insert several ids across shards, each via a bound shard-key parameter.
    for id in [3i32, 17, 42, 99, 128] {
        let affected = client
            .execute(
                &format!("INSERT INTO {tbl} (id, v) VALUES ($1, $2)"),
                &[&id, &format!("v{id}")],
            )
            .await
            .expect("parameterized INSERT should succeed");
        assert_eq!(affected, 1, "id {id} should insert exactly one row");
    }

    // Each id must be retrievable by a bound shard-key parameter (single-shard
    // routing) — proving the param hashes to the shard the row landed on.
    for id in [3i32, 17, 42, 99, 128] {
        let rows = client
            .query(&format!("SELECT v FROM {tbl} WHERE id = $1"), &[&id])
            .await
            .expect("parameterized point lookup should succeed");
        assert_eq!(rows.len(), 1, "id {id} should be found via bound key");
        let v: &str = rows[0].get(0);
        assert_eq!(v, format!("v{id}"));
    }

    drop_table(&client, &tbl).await;
}

// A float/double shard key bound as a parameter (ScalarValue Display "10")
// must hash to the same shard as the equivalent literal (sqlparser "10.0").
// Before canonicalization these diverged, so the row was written to one shard
// and never found on a literal point lookup.
#[tokio::test]
async fn test_float_shard_key_param_matches_literal() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_float_sk",
        "(k DOUBLE PRECISION NOT NULL, v VARCHAR) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'k')",
    )
    .await;

    // Insert whole-number doubles via a bound parameter (the value the client
    // sends decodes to ScalarValue::Float64, whose Display trims to "10").
    for k in [10.0f64, 200.0, 4096.0] {
        let affected = client
            .execute(
                &format!("INSERT INTO {tbl} (k, v) VALUES ($1, $2)"),
                &[&k, &format!("v{k}")],
            )
            .await
            .expect("parameterized float INSERT should succeed");
        assert_eq!(affected, 1, "k {k} should insert exactly one row");
    }

    // Read back via a *literal* point lookup — the literal renders as "10.0",
    // which must canonicalize to the same shard the param wrote to.
    for (k, lit) in [(10.0f64, "10.0"), (200.0, "200.00"), (4096.0, "4096")] {
        let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE k = {lit}"))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "literal {lit} must find the row written by param {k}"
        );
        assert_eq!(rows[0][0].as_deref(), Some(format!("v{k}").as_str()));
    }

    drop_table(&client, &tbl).await;
}

// Binary result format: tokio-postgres's typed `query` requests BINARY result
// encoding for types it has binary codecs for (INT4, INT8, FLOAT8, BOOL). The
// coordinator must encode result columns in the format the client asked for and
// advertise matching OIDs at Describe — otherwise the client fails to decode.
// (Before result encoding was routed through arrow-pg, rows were always text.)
#[tokio::test]
async fn test_binary_result_format_round_trip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "ext_binary",
        &format!(
            "(id INTEGER NOT NULL, big BIGINT, amount DOUBLE PRECISION, flag BOOLEAN, note VARCHAR) {CREATE_OPTS}"
        ),
    )
    .await;

    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, big, amount, flag, note) VALUES ($1, $2, $3, $4, $5)"),
            &[&7i32, &9_000_000_000i64, &19.5f64, &true, &"hello"],
        )
        .await
        .expect("typed INSERT should succeed");
    assert_eq!(affected, 1);

    // Typed getters force binary decoding of each column; a text/binary mismatch
    // or wrong OID would surface here as a decode error or panic.
    let rows = client
        .query(
            &format!("SELECT id, big, amount, flag, note FROM {tbl} WHERE id = $1"),
            &[&7i32],
        )
        .await
        .expect("typed binary SELECT should succeed");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    let big: i64 = rows[0].get(1);
    let amount: f64 = rows[0].get(2);
    let flag: bool = rows[0].get(3);
    let note: &str = rows[0].get(4);
    assert_eq!(id, 7);
    assert_eq!(big, 9_000_000_000);
    assert_eq!(amount, 19.5);
    assert!(flag);
    assert_eq!(note, "hello");

    drop_table(&client, &tbl).await;
}
