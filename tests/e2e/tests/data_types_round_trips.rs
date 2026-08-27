mod common;
use common::*;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use pg_interval::Interval;
use rust_decimal::Decimal;
use tokio_postgres::Client;
use tokio_postgres::types::Type;

// End-to-end data type map. One file for every type VaireDB can be asked to
// store, whether it works or not — the executable counterpart of
// docs/specs/gap-analysis-data-type.md.
//
// Values must survive the coordinator -> DuckDB -> coordinator round trip, and the
// advertised type must be the PostgreSQL type the client asked for. Both halves
// matter: the read path stringifies text cells itself and re-serializes write
// predicates to DuckDB via sqlparser's `to_string()`, while the *metadata* half
// (`parse_data_type` -> Arrow -> `arrow_pg::into_pg_type`) decides what the client
// is told a column is — and, because DataFusion plans over that Arrow type, how
// filters and ORDER BY are evaluated.
//
// Sections mirror the gap doc. Within each one:
//   * plain `#[tokio::test]` — the type works end to end today; the test guards
//     against regressions.
//   * `#[tokio::test]` + `#[ignore = "gap (<doc section>): ..."]` — a type the doc
//     classifies 🟡 (lossy/partial) or ❌ (broken). These assert the
//     PostgreSQL-correct behavior, so they fail by construction: they are the
//     specification of the target state, not a description of today's behavior.
//     Un-#[ignore] each one as its gap closes.
//
// `make e2e` runs only the passing set. To see the current gap map:
//
//     cd tests/e2e && cargo test --test data_types_round_trips -- --ignored --test-threads=1
//
// Three observation surfaces are used, matching the three places a type can be
// lost:
//   * `describe_result_types` — Describe/RowDescription OIDs (read path R3).
//     Catches "the value is right but the advertised type is text".
//   * `describe_param_types` — ParameterDescription OIDs (write path W4
//     precondition). Catches "$1 is described as text".
//   * typed `execute`/`query` with real PostgreSQL client types — the extended
//     protocol bind path (`scalar_to_write_param`) and binary result encoding.
//     `simple_query_rows` covers text-format values.
//
// Cleanup safety: a failed CREATE TABLE rolls back partial DDL on the nodes it
// reached (DROP TABLE IF EXISTS), and every table name is run-unique, so an xfail
// cannot poison the shared cluster for the next test or the next run.

/// Result-column PostgreSQL types as reported at Describe, without executing the
/// query. This is the read path's type-metadata surface: a wrong Arrow target in
/// `parse_data_type` shows up here as a wrong OID even when the values are fine.
async fn describe_result_types(client: &Client, sql: &str) -> Vec<Type> {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("Describe failed for `{sql}`: {e}"));
    stmt.columns().iter().map(|c| c.type_().clone()).collect()
}

/// Bind-parameter PostgreSQL types as reported at Describe. Parameter OIDs are
/// inferred from a DataFusion plan over the advertised schema, so they inherit
/// every `parse_data_type` error.
async fn describe_param_types(client: &Client, sql: &str) -> Vec<Type> {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("Describe failed for `{sql}`: {e}"));
    stmt.params().to_vec()
}

// ============================================================================
// NULLs — orthogonal to type, but the failure mode every gap below shares
// ============================================================================

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

// ============================================================================
// Boolean — doc section "Boolean"
// ============================================================================

// The type mapping is clean, so booleans survive and predicates evaluate as
// booleans (not strings).
#[tokio::test]
async fn test_boolean_round_trip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bool",
        &format!("(id INTEGER NOT NULL, flag BOOLEAN NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, flag) VALUES (1, true), (2, false)"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT flag FROM {tbl}")).await;
    assert_eq!(types, vec![Type::BOOL]);

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} WHERE flag"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "only the true row matches a bare predicate");
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    // Binary format carries the real bool, independently of the text rendering.
    let rows = client
        .query(
            &format!("SELECT flag FROM {tbl} WHERE id = $1 OR id = $2 ORDER BY id"),
            &[&1i32, &2i32],
        )
        .await
        .expect("binary-format BOOLEAN read should succeed");
    let got: Vec<bool> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![true, false]);

    drop_table(&client, &tbl).await;
}

// Text format diverges: `arrow_array_value_to_string` emits Rust's `true`/`false`
// where PostgreSQL's `boolout` emits `t`/`f`. Cosmetic (drivers accept both), but
// it is a wire-form difference a strict client can see.
#[tokio::test]
#[ignore = "gap (Boolean / Text vs binary divergence): text format renders `true`/`false` instead of PostgreSQL's `t`/`f`"]
async fn test_boolean_text_wire_form() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bool_text",
        &format!("(id INTEGER NOT NULL, flag BOOLEAN NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, flag) VALUES (1, true), (2, false)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT flag FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(got, vec!["t", "f"]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Integer — doc section "Integer"
// ============================================================================

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

// The narrow signed widths all have `parse_data_type` arms, so they are typed
// (not text) and therefore sort numerically.
#[tokio::test]
async fn test_signed_integer_widths() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_int_widths",
        &format!(
            "(id INTEGER NOT NULL, a TINYINT NOT NULL, b SMALLINT NOT NULL, c INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, a, b, c) VALUES (1, 127, 32767, 2147483647), (2, -128, -32768, -2147483648)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT a, b, c FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let row0: Vec<&str> = rows[0].iter().map(|c| c.as_deref().unwrap()).collect();
    let row1: Vec<&str> = rows[1].iter().map(|c| c.as_deref().unwrap()).collect();
    assert_eq!(row0, vec!["127", "32767", "2147483647"]);
    assert_eq!(row1, vec!["-128", "-32768", "-2147483648"]);

    // A typed column sorts numerically; a Utf8 fallback would sort -128 last.
    let rows = simple_query_rows(&client, &format!("SELECT a FROM {tbl} ORDER BY a"))
        .await
        .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(got, vec!["-128", "127"]);

    drop_table(&client, &tbl).await;
}

// All four unsigned widths are legal DuckDB and legal DataFusion, and arrow-pg
// maps every one of them (UInt8->int2, UInt16->int4, UInt32->int8,
// UInt64->numeric). They only fail because `parse_data_type` has no arms.
#[tokio::test]
#[ignore = "gap (Integer): UTINYINT/USMALLINT/UINTEGER/UBIGINT have no parse_data_type arms, so all four are advertised as text"]
async fn test_unsigned_integer_types() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_unsigned",
        &format!(
            "(id INTEGER NOT NULL, a UTINYINT NOT NULL, b USMALLINT NOT NULL, c UINTEGER NOT NULL, d UBIGINT NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    // Maximum value of each width.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, a, b, c, d) VALUES (1, 255, 65535, 4294967295, 18446744073709551615)"
        ),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT a, b, c, d FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::INT2, Type::INT4, Type::INT8, Type::NUMERIC],
        "unsigned widths must be advertised as the smallest PG type that holds them"
    );

    let rows = simple_query_rows(&client, &format!("SELECT a, b, c, d FROM {tbl}"))
        .await
        .unwrap();
    let got: Vec<&str> = rows[0].iter().map(|c| c.as_deref().unwrap()).collect();
    assert_eq!(
        got,
        vec!["255", "65535", "4294967295", "18446744073709551615"],
        "every unsigned maximum must round-trip exactly"
    );

    drop_table(&client, &tbl).await;
}

// The `Utf8` fallback is not merely cosmetic: DataFusion evaluates ORDER BY over
// the advertised type, so an unsigned column sorts digit-by-digit.
#[tokio::test]
#[ignore = "gap (Integer): a UBIGINT column is advertised as text, so ORDER BY sorts lexicographically (10, 100, 9) instead of numerically"]
async fn test_unsigned_integer_ordering_is_numeric() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_unsigned_ord",
        &format!("(id INTEGER NOT NULL, v UBIGINT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 9), (2, 10), (3, 100)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} ORDER BY v"))
        .await
        .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(
        got,
        vec!["9", "10", "100"],
        "ORDER BY on an unsigned column must be numeric"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Floating point — doc section "Floating point"
// ============================================================================

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

// REAL/FLOAT4 has its own `parse_data_type` arm, so single precision is not
// silently widened to double.
#[tokio::test]
async fn test_real_single_precision() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_real",
        &format!("(id INTEGER NOT NULL, v REAL NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 1.5), (2, -0.25)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<f32> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![1.5f32, -0.25f32]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Character — doc section "Character"
// ============================================================================

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

// VARCHAR, TEXT and STRING all have `parse_data_type` arms and are the same
// DuckDB type; multi-byte UTF-8 must survive the Utf8/Utf8View round trip.
#[tokio::test]
async fn test_varchar_aliases_and_unicode() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_varchar_alias",
        &format!(
            "(id INTEGER NOT NULL, a VARCHAR NOT NULL, b TEXT NOT NULL, c STRING NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, a, b, c) VALUES (1, 'ünïcodé', '日本語', '🎉')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT a, b, c FROM {tbl}"))
        .await
        .unwrap();
    let got: Vec<&str> = rows[0].iter().map(|c| c.as_deref().unwrap()).collect();
    assert_eq!(got, vec!["ünïcodé", "日本語", "🎉"]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Exact numeric — doc section "Exact numeric"
// ============================================================================

// Values inside DECIMAL(10,2) survive as numbers; only the rendered scale is
// wrong (see `test_numeric_declared_scale_is_preserved`), so this test parses to
// f64 rather than comparing text.
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

// Fault 1: the declared precision and scale are discarded and every decimal is
// advertised as Decimal128(38,10), so PostgreSQL's scale-faithful rendering is
// lost. PostgreSQL prints NUMERIC(10,2) 1.5 as `1.50`.
#[tokio::test]
#[ignore = "gap (Exact numeric, fault 1): parse_data_type widens every DECIMAL to Decimal128(38,10), so declared scale is lost and 1.5 renders 1.5000000000"]
async fn test_numeric_declared_scale_is_preserved() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_num_scale",
        &format!("(id INTEGER NOT NULL, amount NUMERIC(10,2) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, amount) VALUES (1, 1.5), (2, 1234.56), (3, 0.01)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT amount FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(
        got,
        vec!["1.50", "1234.56", "0.01"],
        "NUMERIC(10,2) must render at the declared scale, not at scale 10"
    );

    drop_table(&client, &tbl).await;
}

// Fault 2: the safe rescale from the stored Decimal128(38,0) to the hardcoded
// (38,10) target overflows above 28 integer digits, and a safe Arrow cast turns
// overflow into NULL — a wrong answer rather than an error.
#[tokio::test]
#[ignore = "gap (Exact numeric, fault 2): rescaling DECIMAL(38,0) to the hardcoded (38,10) overflows and the safe cast NULLifies every value above 28 integer digits; on this NOT NULL column the rebuild then fails the read with XX000 `declared as non-nullable but contains null values`"]
async fn test_numeric_38_digit_value_is_not_nullified() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_num_wide",
        &format!("(id INTEGER NOT NULL, big NUMERIC(38,0) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // 38 significant digits — the widest value DuckDB's DECIMAL can hold.
    let wide = "12345678901234567890123456789012345678";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, big) VALUES (1, {wide})"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT big FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0].as_deref(),
        Some(wide),
        "a 38-digit NUMERIC must read back exactly, not as NULL"
    );

    drop_table(&client, &tbl).await;
}

// Fault 3: `scalar_to_write_param` has no Decimal128 arm, so the parameter falls
// through `other => StringVal(ScalarValue::to_string())`, which renders the debug
// form `Some(150),10,2` and fails at DuckDB bind time. This is the single most
// ordinary parameterized write in PostgreSQL.
#[tokio::test]
#[ignore = "gap (write-path parameter defect): NUMERIC bind parameters reach write_router.rs:145 and are stringified as `Some(12345600000000),38,10`, which DuckDB cannot bind (42804)"]
async fn test_numeric_bind_parameter_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_num_param",
        &format!("(id INTEGER NOT NULL, amount NUMERIC(10,2) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let amount = Decimal::new(123_456, 2); // 1234.56
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, amount) VALUES ($1, $2)"),
            &[&1i32, &amount],
        )
        .await
        .expect("parameterized INSERT into a NUMERIC column should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT amount FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1234.56"));

    drop_table(&client, &tbl).await;
}

// Fault 4: arrow-pg encodes `numeric` through rust_decimal, whose 96-bit mantissa
// caps at 29 digits, so binary-format clients get SQLSTATE 22003 at the top of
// DuckDB's legal DECIMAL range. Text-format clients are unaffected because
// VaireDB renders text cells itself — the two wire formats disagree. Fault 2
// currently masks fault 4: the value is NULLified before the encoder ever runs, so
// today this test fails on the coercion rebuild rather than on 22003.
#[tokio::test]
#[ignore = "gap (Exact numeric, fault 4): binary-format NUMERIC encoding goes through rust_decimal, which caps at 29 digits and raises 22003 for wider DuckDB decimals (masked today by fault 2)"]
async fn test_numeric_38_digit_value_in_binary_format() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_num_bin",
        &format!("(id INTEGER NOT NULL, big NUMERIC(38,0) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let wide = "12345678901234567890123456789012345678";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, big) VALUES (1, {wide})"),
    )
    .await
    .unwrap();

    // Typed `query` requests binary encoding for NUMERIC, routing the cell
    // through arrow-pg's encoder rather than VaireDB's text formatter.
    let rows = client
        .query(&format!("SELECT big FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .unwrap_or_else(|e| {
            let sqlstate = e
                .as_db_error()
                .map(|d| d.code().code().to_string())
                .unwrap_or_else(|| "<client-side>".to_string());
            panic!("binary-format read of a 38-digit NUMERIC failed with {sqlstate}: {e}")
        });

    // rust_decimal backs BOTH arrow-pg's encoder and tokio-postgres's client
    // codec, so a server-side fix still leaves this client unable to materialize a
    // 38-digit value — a client limitation, not a VaireDB gap. What must not
    // happen is a server-side error or a NULLified cell.
    match rows[0].try_get::<_, Option<Decimal>>(0) {
        Ok(Some(big)) => assert_eq!(big.to_string(), wide),
        Ok(None) => panic!("a 38-digit NUMERIC must not read back as NULL"),
        Err(_) => { /* client-side 29-digit cap; the server half is correct */ }
    }

    drop_table(&client, &tbl).await;
}

// PostgreSQL NUMERIC supports up to 1000 digits of precision. VaireDB caps at
// DuckDB's 38, and the Arrow type that would carry more (Decimal256) has no
// arrow-pg mapping, so it cannot even be advertised.
#[tokio::test]
#[ignore = "gap (Exact numeric): NUMERIC(p,s) with p > 38 is rejected by DuckDB, and Decimal256 has no into_pg_type arm in arrow-pg 0.14 — needs upstream support in both duckdb-rs and arrow-pg"]
async fn test_numeric_precision_above_38() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_num_256",
        &format!("(id INTEGER NOT NULL, big NUMERIC(40,2) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let wide = "12345678901234567890123456789012345678.90";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, big) VALUES (1, {wide})"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT big FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some(wide));

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Temporal — doc section "Temporal"
// ============================================================================

// TIMESTAMP and DATE both have `parse_data_type` arms, and the encoder renders
// them in PostgreSQL's space-separated form (not ISO `T`), which libpq and JDBC
// require.
#[tokio::test]
async fn test_timestamp_round_trip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_ts",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMP NOT NULL, d DATE NOT NULL) {CREATE_OPTS}"),
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

// TIMESTAMP is 🟡, not ✅: the column type and OID are right and stored values
// project correctly, but DataFusion's SQL parser builds NANOSECOND timestamp
// literals, whose epoch only spans ~1677-09-21 .. 2262-04-11. A literal outside
// that window fails the whole query in the `simplify_expressions` optimizer rule
// even though DuckDB stores the data fine.
#[tokio::test]
#[ignore = "gap (Temporal): DataFusion timestamp literals are nanosecond-typed, so any literal outside 1677..2262 fails in the simplify_expressions optimizer rule"]
async fn test_timestamp_literal_outside_nanosecond_range() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_ts_range",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMP NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, ts) VALUES (1, '1500-01-01 00:00:00'), (2, '3000-01-01 00:00:00')"
        ),
    )
    .await
    .unwrap();

    // Projection alone already works today — DuckDB stores microseconds.
    let rows = simple_query_rows(&client, &format!("SELECT ts FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1500-01-01 00:00:00"));
    assert_eq!(rows[1][0].as_deref(), Some("3000-01-01 00:00:00"));

    // A predicate against an out-of-window literal is what breaks.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE ts < TIMESTAMP '1600-01-01 00:00:00'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "the year-1500 row must match");
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE ts > TIMESTAMP '2500-01-01 00:00:00'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "the year-3000 row must match");
    assert_eq!(rows[0][0].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

// TIMESTAMP bind parameters reach the `other` arm as
// ScalarValue::TimestampMicrosecond, whose Display is the raw microsecond count.
#[tokio::test]
#[ignore = "gap (write-path parameter defect): TIMESTAMP bind parameters are stringified as a bare microsecond count (e.g. `1780835400000000`), which DuckDB cannot bind"]
async fn test_timestamp_bind_parameter_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_ts_param",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMP NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let ts = NaiveDate::from_ymd_opt(2026, 6, 3)
        .unwrap()
        .and_hms_opt(12, 30, 0)
        .unwrap();
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (id, ts) VALUES ($1, $2)"),
            &[&1i32, &ts],
        )
        .await
        .expect("parameterized INSERT into a TIMESTAMP column should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT ts FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("2026-06-03 12:30:00"));

    drop_table(&client, &tbl).await;
}

// A TIMESTAMPTZ column stores and returns its row today (the catalog keeps the
// raw PG type string and DuckDB has the type), so the round trip itself is a
// regression guard. What is *not* right is its type and rendering — see the two
// tests below.
#[tokio::test]
async fn test_timestamptz_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_tstz",
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

// TIMESTAMPTZ has no `parse_data_type` arm, so it degrades to Utf8: the column is
// advertised `text`, and DuckDB's `Timestamp(µs, "UTC")` is stringified as
// `2026-06-03T12:30:00Z` — the ISO T/Z form encoding.rs deliberately avoids for
// real timestamps because libpq and JDBC reject it.
#[tokio::test]
#[ignore = "gap (Temporal): TIMESTAMPTZ has no parse_data_type arm, so it is advertised as text and rendered in ISO T/Z form instead of timestamptz"]
async fn test_timestamptz_type_and_value() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_tstz_type",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMPTZ NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, ts) VALUES (1, '2026-06-03 12:30:00+00')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT ts FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::TIMESTAMPTZ],
        "a TIMESTAMPTZ column must be advertised as timestamptz"
    );

    let rows = simple_query_rows(&client, &format!("SELECT ts FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("2026-06-03 12:30:00+00"),
        "text form must be PostgreSQL's space-separated offset form, not ISO T/Z"
    );

    // Binary format: a real timestamptz OID plus a real timestamptz encoding.
    let rows = client
        .query(&format!("SELECT ts FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .expect("binary-format TIMESTAMPTZ read should succeed");
    let got: DateTime<Utc> = rows[0].get(0);
    assert_eq!(got.to_rfc3339(), "2026-06-03T12:30:00+00:00");

    drop_table(&client, &tbl).await;
}

// Because the column degrades to Utf8, DataFusion evaluates the predicate over
// strings. The stored rendering is `2026-06-03T12:30:00Z` while the client sends
// `2026-06-03 12:30:00+00`; `T` (0x54) sorts after a space (0x20), so a `>`
// comparison is unconditionally true and the filter returns wrong rows.
#[tokio::test]
#[ignore = "gap (Temporal): TIMESTAMPTZ predicates compare lexicographically against DuckDB's ISO T/Z rendering, so `ts > <literal>` matches rows it must exclude"]
async fn test_timestamptz_predicate_is_chronological() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_tstz_pred",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMPTZ NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, ts) VALUES \
             (1, '2026-06-03 08:00:00+00'), \
             (2, '2026-06-03 20:00:00+00')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE ts > '2026-06-03 12:00:00+00' ORDER BY id"),
    )
    .await
    .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(
        got,
        vec!["2"],
        "only the 20:00 row is after noon; a lexicographic comparison returns both"
    );

    drop_table(&client, &tbl).await;
}

// TIMESTAMPTZ parameters are described as `text` today (the column degrades to
// Utf8), so a client sending a real timestamptz is rejected before the bind path
// is even reached.
#[tokio::test]
#[ignore = "gap (Temporal + write-path parameter defect): a TIMESTAMPTZ column advertises text, so $1 is described text and a timestamptz parameter is refused"]
async fn test_timestamptz_bind_parameter_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_tstz_param",
        &format!("(id INTEGER NOT NULL, ts TIMESTAMPTZ NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let sql = format!("INSERT INTO {tbl} (id, ts) VALUES ($1, $2)");
    let params = describe_param_types(&client, &sql).await;
    assert_eq!(
        params,
        vec![Type::INT4, Type::TIMESTAMPTZ],
        "$2 must be described as timestamptz"
    );

    let ts = NaiveDate::from_ymd_opt(2026, 6, 3)
        .unwrap()
        .and_hms_opt(12, 30, 0)
        .unwrap()
        .and_utc();
    let affected = client
        .execute(&sql, &[&1i32, &ts])
        .await
        .expect("parameterized INSERT into a TIMESTAMPTZ column should succeed");
    assert_eq!(affected, 1);

    drop_table(&client, &tbl).await;
}

// TIME has no `parse_data_type` arm. DuckDB returns exactly the Arrow type
// DataFusion wants (`Time64(µs)`) and arrow-pg maps it to `time`, so this is a
// pure mapping gap: the value is right, the advertised type is text.
#[tokio::test]
#[ignore = "gap (Temporal): TIME has no parse_data_type arm, so a TIME column is advertised as text instead of time (OID 1083)"]
async fn test_time_type_and_value() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_time",
        &format!("(id INTEGER NOT NULL, t TIME NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, t) VALUES (1, '12:34:56')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT t FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::TIME],
        "a TIME column must be advertised as time"
    );

    let rows = simple_query_rows(&client, &format!("SELECT t FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("12:34:56"));

    let rows = client
        .query(&format!("SELECT t FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .expect("binary-format TIME read should succeed");
    let got: NaiveTime = rows[0].get(0);
    assert_eq!(got, NaiveTime::from_hms_opt(12, 34, 56).unwrap());

    drop_table(&client, &tbl).await;
}

// TIME parameters work today only *because* the column degrades to Utf8 and the
// client is asked for text. Fixing `parse_data_type` without fixing
// `scalar_to_write_param` turns this into a hard failure — see the doc's
// "Sequencing warning".
#[tokio::test]
#[ignore = "gap (Temporal + write-path parameter defect): a TIME column advertises text, so $1 is described text; once TIME maps to Time64 the bind path stringifies it as a raw microsecond count"]
async fn test_time_bind_parameter_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_time_param",
        &format!("(id INTEGER NOT NULL, t TIME NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let sql = format!("INSERT INTO {tbl} (id, t) VALUES ($1, $2)");
    let params = describe_param_types(&client, &sql).await;
    assert_eq!(
        params,
        vec![Type::INT4, Type::TIME],
        "$2 must be described as time"
    );

    let t = NaiveTime::from_hms_opt(12, 34, 56).unwrap();
    let affected = client
        .execute(&sql, &[&1i32, &t])
        .await
        .expect("parameterized INSERT into a TIME column should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT t FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("12:34:56"));

    drop_table(&client, &tbl).await;
}

// INTERVAL has no `parse_data_type` arm either. Beyond the wrong OID, the Utf8
// fallback renders Arrow's plural form (`1 days`) instead of PostgreSQL's
// (`1 day`).
#[tokio::test]
#[ignore = "gap (Temporal): INTERVAL has no parse_data_type arm, so it is advertised as text and renders Arrow's `1 days` instead of PostgreSQL's `1 day`"]
async fn test_interval_type_and_value() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_interval",
        &format!("(id INTEGER NOT NULL, span INTERVAL NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, span) VALUES (1, INTERVAL '1 day')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT span FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::INTERVAL],
        "an INTERVAL column must be advertised as interval"
    );

    let rows = simple_query_rows(&client, &format!("SELECT span FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("1 day"),
        "PostgreSQL renders a one-day interval singular"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (Temporal + write-path parameter defect): an INTERVAL column advertises text, so $1 is described text; once INTERVAL maps to Interval(MonthDayNano) the bind path stringifies its Debug form"]
async fn test_interval_bind_parameter_insert() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_interval_param",
        &format!("(id INTEGER NOT NULL, span INTERVAL NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let sql = format!("INSERT INTO {tbl} (id, span) VALUES ($1, $2)");
    let params = describe_param_types(&client, &sql).await;
    assert_eq!(
        params,
        vec![Type::INT4, Type::INTERVAL],
        "$2 must be described as interval"
    );

    let span = Interval::new(0, 1, 0); // 1 day
    let affected = client
        .execute(&sql, &[&1i32, &span])
        .await
        .expect("parameterized INSERT into an INTERVAL column should succeed");
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT span FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1 day"));

    drop_table(&client, &tbl).await;
}

// TIMETZ loses its offset inside duckdb-rs's Arrow bridge (`12:34:56+02` becomes
// a bare `Time64(µs)` of `12:34:56`) — silent data loss upstream of the
// coordinator. The doc recommends restricting the type until that is fixed.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): duckdb-rs's Arrow bridge drops the TIMETZ offset, so the value is silently rewritten to local time"]
async fn test_timetz_preserves_offset() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_timetz",
        &format!("(id INTEGER NOT NULL, t TIMETZ NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, t) VALUES (1, '12:34:56+02')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT t FROM {tbl}")).await;
    assert_eq!(types, vec![Type::TIMETZ]);

    let rows = simple_query_rows(&client, &format!("SELECT t FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("12:34:56+02"),
        "the UTC offset must survive the round trip"
    );

    drop_table(&client, &tbl).await;
}

// DuckDB's sub-microsecond and coarse timestamp variants are all valid Arrow
// `Timestamp` units, so mapping them is cheap; today they fall through to text
// and render in ISO T-separated form.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): TIMESTAMP_S/_MS/_NS have no parse_data_type arm and read back as ISO T-separated text"]
async fn test_timestamp_precision_variants() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_ts_units",
        &format!(
            "(id INTEGER NOT NULL, s TIMESTAMP_S NOT NULL, ms TIMESTAMP_MS NOT NULL, ns TIMESTAMP_NS NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, s, ms, ns) VALUES \
             (1, '2026-06-03 12:30:00', '2026-06-03 12:30:00', '2026-06-03 12:30:00')"
        ),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT s, ms, ns FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::TIMESTAMP, Type::TIMESTAMP, Type::TIMESTAMP],
        "every TIMESTAMP precision variant must be advertised as timestamp"
    );

    let rows = simple_query_rows(&client, &format!("SELECT s, ms, ns FROM {tbl}"))
        .await
        .unwrap();
    for (i, label) in ["TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"]
        .iter()
        .enumerate()
    {
        assert_eq!(
            rows[0][i].as_deref(),
            Some("2026-06-03 12:30:00"),
            "{label} must render in PostgreSQL space-separated form"
        );
    }

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Binary — doc section "Binary"
// ============================================================================

// BYTEA is rewritten to DuckDB BLOB on the write path (`transform_to_duckdb`) and
// both spellings have a `parse_data_type` arm, so bytes survive intact and the
// column is advertised as bytea.
#[tokio::test]
async fn test_bytea_round_trip() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bytea",
        &format!("(id INTEGER NOT NULL, a BYTEA NOT NULL, b BLOB NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // Deliberately not valid UTF-8: a Binary -> Utf8 fallback would NULL these.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, a, b) VALUES (1, '\\xDE\\xAD\\xBE\\xEF', '\\xDE\\xAD\\xBE\\xEF')"
        ),
    )
    .await
    .unwrap();

    let rows = client
        .query(&format!("SELECT a, b FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .expect("binary-format bytea read should succeed");
    let a: Vec<u8> = rows[0].get(0);
    let b: Vec<u8> = rows[0].get(1);
    assert_eq!(a, vec![0xDE, 0xAD, 0xBE, 0xEF], "BYTEA must survive intact");
    assert_eq!(b, vec![0xDE, 0xAD, 0xBE, 0xEF], "BLOB must survive intact");

    drop_table(&client, &tbl).await;
}

// DuckDB's other BLOB aliases have no `parse_data_type` arm, so they degrade to
// Utf8 and the safe Binary->Utf8 cast sees invalid UTF-8: every non-ASCII value
// reads back NULL. Silent data loss on a type that works correctly under a
// different spelling.
#[tokio::test]
#[ignore = "gap (Binary / Alias gap): BINARY and VARBINARY have no parse_data_type arm, so non-UTF-8 bytes are NULLified by the safe Binary->Utf8 cast"]
async fn test_binary_aliases_preserve_bytes() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_binary_alias",
        &format!("(id INTEGER NOT NULL, a BINARY NOT NULL, b VARBINARY NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, a, b) VALUES (1, '\\xDE\\xAD\\xBE\\xEF', '\\xDE\\xAD\\xBE\\xEF')"
        ),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT a, b FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::BYTEA, Type::BYTEA],
        "BINARY and VARBINARY must be advertised as bytea"
    );

    let rows = client
        .query(&format!("SELECT a, b FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .expect("binary-format bytea read should succeed");
    let a: Vec<u8> = rows[0].get(0);
    let b: Vec<u8> = rows[0].get(1);
    assert_eq!(
        a,
        vec![0xDE, 0xAD, 0xBE, 0xEF],
        "BINARY must not be NULLified"
    );
    assert_eq!(
        b,
        vec![0xDE, 0xAD, 0xBE, 0xEF],
        "VARBINARY must not be NULLified"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Nested — doc section "Nested"
// ============================================================================

// PostgreSQL array column type (INTEGER[]). DuckDB stores this as a LIST; the PG
// `[]` DDL syntax passes through to DuckDB unchanged, and the catalog type string
// is modeled as an Arrow `List` of the element type, so the column reads back as a
// PG array (`{1,2,3}`) rather than as text.
#[tokio::test]
async fn test_array_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_array",
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

// `T[n]` lands as `FixedSizeList(T,n)` in DuckDB but is advertised as `List(T)`.
// The cast itself succeeds, yet it yields a list whose child field is unnamed, and
// `coerce_batch_to_schema`'s rebuild rejects that against the advertised schema —
// so every read of a fixed-length array column fails. The declared length is also
// dropped from the schema; DuckDB still enforces it on write, which must surface as
// a clean client error rather than an internal one. `FixedSizeList` must NOT be
// adopted as the advertised type: arrow-pg's encoder has no arm for it.
#[tokio::test]
#[ignore = "gap (Nested): every read of a T[n] column fails — the FixedSizeList -> List cast yields an unnamed child field and coerce_batch_to_schema reports `expected List(Int32) but found List(Int32, field: '')`"]
async fn test_fixed_length_array_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_fixed_array",
        &format!("(id INTEGER NOT NULL, tags INTEGER[3] NOT NULL) {CREATE_OPTS}"),
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
    assert_eq!(
        rows[0][0].as_deref(),
        Some("{1,2,3}"),
        "a fixed-length array must read back in PostgreSQL array text form"
    );

    // The declared length is part of the contract: a 2-element value is invalid.
    let err = execute_expect_err(
        &client,
        &format!("INSERT INTO {tbl} (id, tags) VALUES (2, ARRAY[1, 2])"),
    )
    .await;
    assert_ne!(
        err.code().code(),
        "XX000",
        "a wrong-length ARRAY insert must be a clean client error, not an internal one"
    );

    drop_table(&client, &tbl).await;
}

// MAP is accepted at CREATE TABLE and accepts INSERTs, then every SELECT fails:
// the column is advertised Utf8 and `Casting from Map(...) to Utf8` is not
// supported. Advertising `Map` would not help — arrow-pg has no Map arm at all,
// so this needs an upstream mapping or a DDL-time rejection.
#[tokio::test]
#[ignore = "gap (Nested): a MAP column is advertised Utf8, so every SELECT fails with `Casting from Map(...) to Utf8 not supported`; arrow-pg 0.14 has no Map arm either"]
async fn test_map_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_map",
        &format!("(id INTEGER NOT NULL, m MAP(INTEGER, VARCHAR) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, m) VALUES (1, map([1, 2], ['a', 'b']))"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT m FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the MAP row must read back");

    drop_table(&client, &tbl).await;
}

// STRUCT never reaches DuckDB intact: sqlparser re-renders
// `STRUCT(a INTEGER, b VARCHAR)` as `STRUCT(a, INTEGER, b, VARCHAR)`, which
// DuckDB rejects. Unlike MAP, STRUCT is viable once the DDL is fixed — arrow-pg
// maps it to `record` (OID 2249) and encodes it.
#[tokio::test]
#[ignore = "gap (Nested): sqlparser mangles the STRUCT field list on re-render (`STRUCT(a, INTEGER, b, VARCHAR)`), so the per-shard CREATE TABLE fails at DuckDB"]
async fn test_struct_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_struct",
        &format!("(id INTEGER NOT NULL, s STRUCT(i INTEGER, j VARCHAR) NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, s) VALUES (1, {{'i': 42, 'j': 'a'}})"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT s FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the STRUCT row must read back");

    drop_table(&client, &tbl).await;
}

// UNION hits the same re-render mangling as STRUCT, and there is no Arrow `Union`
// path through arrow-pg even with valid DDL — a restrict-at-DDL candidate.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): UNION's field list is mangled on re-render and arrow-pg has no Union path; it should be rejected at DDL time"]
async fn test_union_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_union",
        &format!("(id INTEGER NOT NULL, u UNION(num INTEGER, str VARCHAR)) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, u) VALUES (1, union_value(num := 2))"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT u FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the UNION row must read back");

    drop_table(&client, &tbl).await;
}

// VARIANT is unusable on the shard databases as deployed: DuckDB rejects the
// per-shard CREATE because the store predates VARIANT support. Even on a v1.5.0+
// store the read then fails inside duckdb-rs. Both failures are upstream of the
// coordinator, so the type should be rejected at DDL time with a clear message.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): the per-shard CREATE fails with `VARIANT columns are not supported in storage versions prior to v1.5.0`; even on a newer store duckdb-rs cannot decode Variant columns"]
async fn test_variant_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_variant",
        &format!("(id INTEGER NOT NULL, v VARIANT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 42)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the VARIANT row must read back");

    drop_table(&client, &tbl).await;
}

// ============================================================================
// DuckDB types to restrict — doc section "DuckDB types to restrict"
// ============================================================================

// duckdb-rs's Arrow bridge narrows 128-bit integers to Decimal128(38,0) BEFORE
// the coordinator sees the batch, so the 39th digit is lost silently. Not fixable
// in `parse_data_type`; needs Decimal256 in duckdb-rs and arrow-pg, or a
// `c::VARCHAR` projection in the shard scan.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): duckdb-rs narrows HUGEINT to Decimal128(38,0), silently truncating the 39th digit upstream of the coordinator"]
async fn test_hugeint_precision() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_hugeint",
        &format!("(id INTEGER NOT NULL, v HUGEINT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // i128::MAX — 39 digits.
    let max = "170141183460469231731687303715884105727";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, {max})"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some(max),
        "all 39 digits of HUGEINT max must survive"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): UHUGEINT max reads back as -1 after duckdb-rs narrows it to Decimal128(38,0)"]
async fn test_uhugeint_precision() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_uhugeint",
        &format!("(id INTEGER NOT NULL, v UHUGEINT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // u128::MAX — 39 digits.
    let max = "340282366920938463463374607431768211455";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, {max})"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some(max),
        "UHUGEINT max must survive, not read back as -1"
    );

    drop_table(&client, &tbl).await;
}

// DuckDB returns a packed bitstring as `Binary`; the Utf8 fallback's safe cast
// sees invalid UTF-8 and NULLifies every value.
#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): BIT is returned as Binary and NULLified by the safe Binary->Utf8 cast (on this NOT NULL column the rebuild then fails the read); PostgreSQL renders it as a 1/0 string"]
async fn test_bit_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bit",
        &format!("(id INTEGER NOT NULL, b BIT NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, b) VALUES (1, '101010')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT b FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("101010"),
        "a bitstring must read back as its 1/0 text form, not NULL"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (DuckDB types to restrict): BIGNUM is returned as Binary and NULLified by the safe Binary->Utf8 cast (on this NOT NULL column the rebuild then fails the read)"]
async fn test_bignum_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_bignum",
        &format!("(id INTEGER NOT NULL, v BIGNUM NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let big = "123456789012345678901234567890123456789012345";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, '{big}')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some(big),
        "a variable-length integer must read back exactly, not NULL"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Alias gap — doc section "The alias gap"
// ============================================================================

// `parse_data_type` matches the declared string, so an unrecognized alias of a
// fully-supported type degrades to text even though the canonical spelling works.
#[tokio::test]
#[ignore = "gap (Alias gap): LONG/SIGNED/SHORT/INT1/DATETIME/LOGICAL are missing from parse_data_type, so all six degrade to text"]
async fn test_duckdb_type_aliases() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_aliases",
        &format!(
            "(id INTEGER NOT NULL, a LONG NOT NULL, b SIGNED NOT NULL, c SHORT NOT NULL, \
              d INT1 NOT NULL, e DATETIME NOT NULL, f LOGICAL NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, a, b, c, d, e, f) VALUES \
             (1, 9000000000, 42, 7, 1, '2026-06-03 12:30:00', true)"
        ),
    )
    .await
    .unwrap();

    let types =
        describe_result_types(&client, &format!("SELECT a, b, c, d, e, f FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![
            Type::INT8,      // LONG     -> BIGINT
            Type::INT4,      // SIGNED   -> INTEGER
            Type::INT2,      // SHORT    -> SMALLINT
            Type::INT2,      // INT1     -> TINYINT (widened; PG has no int1)
            Type::TIMESTAMP, // DATETIME -> TIMESTAMP
            Type::BOOL,      // LOGICAL  -> BOOLEAN
        ],
        "every DuckDB alias must resolve to its canonical type"
    );

    let rows = simple_query_rows(&client, &format!("SELECT e FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("2026-06-03 12:30:00"),
        "DATETIME must render as a PostgreSQL timestamp, not ISO T-separated"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Type-metadata-only gaps — doc section "DataFusion-unsupported SQL types"
// ============================================================================

// JSONB is coerced to DuckDB JSON on the write path (transform_to_duckdb) and the
// document text round-trips exactly; DataFusion has no JSON type, so text is the
// correct value carrier. Only the advertised OID is wrong — see
// `test_json_column_type`.
#[tokio::test]
async fn test_jsonb_round_trip() {
    let client = ready_client().await;
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

#[tokio::test]
#[ignore = "gap (DataFusion-unsupported SQL types): JSON/JSONB columns are advertised as text instead of json (OID 114)"]
async fn test_json_column_type() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_json",
        &format!("(id INTEGER NOT NULL, a JSON NOT NULL, b JSONB NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, a, b) VALUES (1, '{{\"a\": 1}}', '{{\"b\": 2}}')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT a, b FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::JSON, Type::JSON],
        "JSON and JSONB columns must be advertised as json"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (DataFusion-unsupported SQL types): UUID columns are advertised as text instead of uuid (OID 2950)"]
async fn test_uuid_column_type() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_uuid",
        &format!("(id INTEGER NOT NULL, u UUID NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, u) VALUES (1, '{uuid}')"),
    )
    .await
    .unwrap();

    let types = describe_result_types(&client, &format!("SELECT u FROM {tbl}")).await;
    assert_eq!(
        types,
        vec![Type::UUID],
        "a UUID column must be advertised as uuid"
    );

    let rows = simple_query_rows(&client, &format!("SELECT u FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some(uuid));

    drop_table(&client, &tbl).await;
}

// ENUM values round-trip correctly through the Dictionary(UInt8, Utf8) -> text
// cast, but the cast also erases the enum's ordering: PostgreSQL and DuckDB both
// order an enum by DECLARATION order, while text orders alphabetically.
#[tokio::test]
#[ignore = "gap (DataFusion-unsupported SQL types): an ENUM column is cast to text, so ORDER BY uses alphabetical order instead of the enum's declaration order"]
async fn test_enum_column_ordering() {
    let client = ready_client().await;
    // Declaration order (low, medium, high) deliberately differs from
    // alphabetical order (high, low, medium).
    let tbl = create_table(
        &client,
        "tr_enum",
        &format!(
            "(id INTEGER NOT NULL, level ENUM('low', 'medium', 'high') NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, level) VALUES (1, 'high'), (2, 'low'), (3, 'medium')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT level FROM {tbl} ORDER BY level"))
        .await
        .unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r[0].as_deref().unwrap()).collect();
    assert_eq!(
        got,
        vec!["low", "medium", "high"],
        "ORDER BY on an enum must follow declaration order, not alphabetical order"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Pseudo-types
// ============================================================================

// SERIAL is a PostgreSQL pseudo-type (auto-increment). It is not mapped to a
// DuckDB sequence/identity type, so the per-shard CREATE fails at DuckDB.
#[tokio::test]
#[ignore = "known gap: SERIAL is not mapped to a DuckDB sequence/identity column. Take a look to https://duckdb.org/docs/current/sql/statements/create_sequence."]
async fn test_serial_column() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "tr_serial",
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
