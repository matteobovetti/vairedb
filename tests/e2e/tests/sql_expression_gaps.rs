mod common;
use common::*;
use tokio_postgres::Client;
use tokio_postgres::types::Type;

// The read path's expression surface: the executable counterpart of the operator,
// aggregate and window gap analyses, for the rows closed by the coordinator's
// PostgreSQL rewrite layer rather than by a change of engine.
//
// Every assertion here is the answer PostgreSQL gives. The layering is what makes
// that worth pinning: a client speaks PostgreSQL, DataFusion plans the statement and
// a core node executes the stage, and none of those three agrees with the other two
// on every operator. So each test names which seam it holds:
//
//   * a REWRITE — the operator has an exact DataFusion spelling, and what is pinned
//     is the PostgreSQL result, not the spelling;
//   * a REFUSAL — VaireDB does not implement what the expression asks for, and what
//     is pinned is that it says so instead of answering as if it did;
//   * a DISTRIBUTED case — the same expression over a sharded table, because a
//     function resolved on the coordinator still has to exist on the executor that
//     runs the stage, and a name that only the planner knows fails after the query
//     was accepted.
//
// Statement-level gaps live in `sql_command_unsupported.rs`; type mapping lives in
// `data_types_round_trips.rs`.

/// A three-row sharded table with the columns the read-path probes need: an integer
/// key, a float for the math functions, and text for the pattern operators.
async fn setup_probe_table(client: &Client, prefix: &str) -> String {
    let tbl = create_table(
        client,
        prefix,
        &format!(
            "(id INTEGER NOT NULL, val DOUBLE PRECISION NOT NULL, name VARCHAR NOT NULL) \
             {CREATE_OPTS}"
        ),
    )
    .await;
    execute(
        client,
        &format!(
            "INSERT INTO {tbl} (id, val, name) VALUES \
             (1, 1.5, 'Alice'), (2, -2.5, 'alpha'), (3, 3.5, 'Bob')"
        ),
    )
    .await
    .unwrap();
    tbl
}

/// The single scalar a `SELECT <expr>` returns, as the server rendered it.
async fn scalar(client: &Client, sql: &str) -> String {
    let rows = simple_query_rows(client, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"));
    assert_eq!(rows.len(), 1, "`{sql}` returns one row");
    rows[0][0]
        .clone()
        .unwrap_or_else(|| panic!("`{sql}` returned NULL"))
}

/// The same, parsed as a number, for expressions whose *value* is the contract and
/// whose result type is not: `power()` may answer `1024` or `1024.0` depending on how
/// DataFusion coerces its arguments, and both are the right answer to `2 ^ 10`.
async fn scalar_number(client: &Client, sql: &str) -> f64 {
    let text = scalar(client, sql).await;
    text.parse()
        .unwrap_or_else(|e| panic!("`{sql}` returned {text:?}, not a number: {e}"))
}

// ============================================================================
// 1. Operators PostgreSQL and DuckDB share and DataFusion has no node for
// ============================================================================

// REWRITE. `^` is exponentiation in PostgreSQL and in DuckDB, and DataFusion's
// planner reads it as bitwise XOR and then refuses the node. Before the rewrite this
// was the worse of the two failures — it silently answered `8`.
#[tokio::test]
async fn test_caret_is_exponentiation() {
    let client = ready_client().await;

    assert_eq!(scalar_number(&client, "SELECT 2 ^ 10").await, 1024.0);
    assert_eq!(scalar_number(&client, "SELECT 2 ^ 3 ^ 2").await, 64.0);
    assert_eq!(scalar_number(&client, "SELECT 9 ^ 0.5").await, 3.0);
}

// REWRITE. `^@` is PostgreSQL's prefix test and DuckDB answers it directly; only
// DataFusion's planner lacks it, so the read path was the one place it failed.
#[tokio::test]
async fn test_starts_with_operator() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_prefix").await;

    assert_eq!(scalar(&client, "SELECT 'abc' ^@ 'a'").await, "t");
    assert_eq!(scalar(&client, "SELECT 'abc' ^@ 'b'").await, "f");

    // DISTRIBUTED: over a sharded column, so the predicate is evaluated wherever the
    // rows are.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT name FROM {tbl} WHERE name ^@ 'A' ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "only 'Alice' starts with a capital A");
    assert_eq!(rows[0][0].as_deref(), Some("Alice"));

    drop_table(&client, &tbl).await;
}

// REWRITE. `&&` is overlap. PostgreSQL defines it for arrays, ranges and geometric
// types; arrays are the only one of the three VaireDB stores, so array overlap is the
// whole of what it can mean here.
#[tokio::test]
async fn test_array_overlap_operator() {
    let client = ready_client().await;

    assert_eq!(
        scalar(&client, "SELECT ARRAY[1, 2] && ARRAY[2, 3]").await,
        "t"
    );
    assert_eq!(
        scalar(&client, "SELECT ARRAY[1, 2] && ARRAY[3, 4]").await,
        "f"
    );
}

// REWRITE. DataFusion has no unary bitwise NOT, but `~x` is `x # -1` for every
// two's-complement width, so the value is exact. The result widens to `bigint`, which
// is the one visible difference from PostgreSQL and does not change the number.
#[tokio::test]
async fn test_unary_bitwise_not() {
    let client = ready_client().await;

    assert_eq!(scalar(&client, "SELECT ~5").await, "-6");
    assert_eq!(scalar(&client, "SELECT ~0").await, "-1");
    assert_eq!(scalar(&client, "SELECT ~(-1)").await, "0");
}

// ============================================================================
// 2. SIMILAR TO — a rewrite that changes an answer rather than a spelling
// ============================================================================

// REWRITE. `SIMILAR TO` is not a regex: `%` and `_` are its wildcards and every regex
// metacharacter outside its own small set is literal. Both DataFusion and DuckDB hand
// the pattern to a regex engine unchanged, so it used to be wrong in both directions
// at once — under-matching on `%`, over-matching on `.`. These are PostgreSQL's
// answers.
#[tokio::test]
async fn test_similar_to_uses_sql_wildcards_not_regex_syntax() {
    let client = ready_client().await;

    // `%` is the wildcard, which a regex engine reads as a literal percent sign.
    assert_eq!(scalar(&client, "SELECT 'abc' SIMILAR TO 'a%'").await, "t");
    assert_eq!(scalar(&client, "SELECT 'abc' SIMILAR TO 'a_c'").await, "t");
    // `.` is a literal, which a regex engine reads as "any character".
    assert_eq!(scalar(&client, "SELECT 'abc' SIMILAR TO 'a.*'").await, "f");
    assert_eq!(scalar(&client, "SELECT 'a.c' SIMILAR TO 'a.c'").await, "t");
    // The match is against the whole string, not a search within it.
    assert_eq!(scalar(&client, "SELECT 'abc' SIMILAR TO 'a'").await, "f");
    assert_eq!(scalar(&client, "SELECT 'abc' SIMILAR TO 'abc'").await, "t");
    // The metacharacters `SIMILAR TO` does define keep their meaning.
    assert_eq!(
        scalar(&client, "SELECT 'abc' SIMILAR TO '(a|z)bc'").await,
        "t"
    );
    assert_eq!(
        scalar(&client, "SELECT 'aaa' SIMILAR TO 'a{2,3}'").await,
        "t"
    );
    assert_eq!(
        scalar(&client, "SELECT 'abc' NOT SIMILAR TO 'z%'").await,
        "t"
    );
}

// REWRITE. The escape character is the only way to match a literal `%`, and `ESCAPE`
// chooses a different one.
#[tokio::test]
async fn test_similar_to_escape() {
    let client = ready_client().await;

    assert_eq!(
        scalar(&client, r"SELECT 'a%b' SIMILAR TO 'a\%b'").await,
        "t"
    );
    assert_eq!(
        scalar(&client, r"SELECT 'axb' SIMILAR TO 'a\%b'").await,
        "f"
    );
    assert_eq!(
        scalar(&client, "SELECT 'a%b' SIMILAR TO 'a#%b' ESCAPE '#'").await,
        "t"
    );
}

// REFUSAL. The pattern is translated into a regular expression before the query runs,
// so a pattern that is only known at run time cannot be translated — and passing it
// through would match by the wrong rules without saying so, which is the defect this
// whole test file exists to prevent.
#[tokio::test]
async fn test_similar_to_with_a_computed_pattern_is_refused() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_similar").await;

    assert_unsupported(
        &client,
        &format!("SELECT 1 FROM {tbl} WHERE name SIMILAR TO name"),
    )
    .await;

    // The escape hatch named in the refusal does work, so nothing is unreachable.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT name FROM {tbl} WHERE regexp_like(name, '^A') ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 3. Refusals — what would otherwise be accepted and half-ignored
// ============================================================================

// REFUSAL, with the exception that makes it safe. A `COLLATE` naming a real locale was
// parsed and thrown away, so `'B' COLLATE "en_US" < 'a'` answered `true` where
// PostgreSQL answers `false` — plausible rows in byte order, with nothing to tell the
// client the collation never applied. Byte order asked for by name is a different
// case: `C`, `POSIX`, `ucs_basic` and `default` describe what VaireDB already does, so
// they are accepted and dropped, which is also what keeps client introspection queries
// working.
#[tokio::test]
async fn test_collate_is_refused_unless_it_asks_for_byte_order() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_collate").await;

    let err = assert_sqlstate(
        &client,
        "SELECT 'B' COLLATE \"en_US\" < 'a'",
        SQLSTATE_FEATURE_NOT_SUPPORTED,
    )
    .await;
    assert!(
        err.message().contains("COLLATE"),
        "the refusal names the expression it refused: {}",
        err.message()
    );

    // Byte order, by each of its four names.
    assert_eq!(scalar(&client, "SELECT 'a' COLLATE \"C\"").await, "a");
    assert_eq!(
        scalar(&client, "SELECT 'B' COLLATE \"POSIX\" < 'a'").await,
        "t",
        "byte order really is what VaireDB does, which is why dropping the node is honest"
    );
    let rows = simple_query_rows(
        &client,
        &format!("SELECT name FROM {tbl} ORDER BY name COLLATE ucs_basic"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.iter().map(|r| r[0].as_deref()).collect::<Vec<_>>(),
        vec![Some("Alice"), Some("Bob"), Some("alpha")],
        "byte order sorts capitals before lower case"
    );
    // `pg_catalog.default` names the database's own collation, whatever that is; here
    // it is byte order, so it asks for nothing VaireDB does not do.
    assert_eq!(
        scalar(&client, "SELECT 'B' COLLATE pg_catalog.default < 'a'").await,
        "t"
    );

    drop_table(&client, &tbl).await;
}

// The `default` collation is not a curiosity: it is what `psql`'s `\d` sends, so
// refusing it broke table introspection for the reference client while every test in
// this suite stayed green. This is the shape of that query — a `pg_catalog.~` operator
// against a name pattern, collated `pg_catalog.default` — and finding the table by it
// is the whole assertion.
#[tokio::test]
async fn test_psql_describe_table_introspection() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_describe").await;

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT c.relname FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname OPERATOR(pg_catalog.~) '^({tbl})$' COLLATE pg_catalog.default \
             ORDER BY 1"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("`\\d {tbl}` introspection should answer: {e}"));

    assert_eq!(
        rows.iter().map(|r| r[0].as_deref()).collect::<Vec<_>>(),
        vec![Some(tbl.as_str())],
        "the table `psql \\d` asks about is the one it finds"
    );

    drop_table(&client, &tbl).await;
}

// A table the coordinator has is a table a client must be able to *find*, and that is a
// separate question from whether it can be read: the metadata is answered on the
// coordinator's local session context, and user tables had only ever been registered in
// the distributed one. So `pg_class` and `information_schema` listed nothing — `\dt`
// came back empty and `\d` had no columns to print — while every SELECT in this suite
// passed. Both halves are pinned here, the relation and its columns.
#[tokio::test]
async fn test_a_table_is_visible_to_the_metadata_a_client_lists() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_listed").await;

    // `\dt`: the relation, its schema, and a relkind that puts it among the tables.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT n.nspname, c.relname, c.relkind FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = '{tbl}'"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("`\\dt` introspection should answer: {e}"));
    assert_eq!(rows.len(), 1, "the table is listed exactly once");
    assert_eq!(rows[0][0].as_deref(), Some("public"));
    assert_eq!(rows[0][1].as_deref(), Some(tbl.as_str()));
    assert_eq!(
        rows[0][2].as_deref(),
        Some("r"),
        "an ordinary table, which is what makes `\\dt` show it"
    );

    // `\d <table>`: the columns, in declaration order.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT a.attname FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = '{tbl}' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("`\\d {tbl}` should list columns: {e}"));
    assert_eq!(
        rows.iter().map(|r| r[0].as_deref()).collect::<Vec<_>>(),
        vec![Some("id"), Some("val"), Some("name")]
    );

    // The SQL-standard spelling of the same two questions, which is what non-psql
    // clients and ORMs send.
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT count(*) FROM information_schema.tables WHERE table_name = '{tbl}'")
        )
        .await,
        "1"
    );
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT count(*) FROM information_schema.columns WHERE table_name = '{tbl}'")
        )
        .await,
        "3"
    );

    // And a dropped table stops being visible: both session contexts have to be
    // deregistered, or the metadata keeps advertising a table no read can reach.
    drop_table(&client, &tbl).await;
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT count(*) FROM pg_catalog.pg_class WHERE relname = '{tbl}'")
        )
        .await,
        "0",
        "a dropped table is gone from the metadata too"
    );
}

// REFUSAL, and the direct consequence of the visibility above. A user table is now
// resolvable in the local context — it has to be, for `pg_class` to list it — but not
// executable there: the per-shard scan its provider plans only means something once a
// core node holds it. Distributing the statement is no better, since the `pg_catalog`
// half is an in-memory provider on the coordinator. So the one statement that wants
// both is refused by name, rather than failing with an internal message about plan
// distribution.
#[tokio::test]
async fn test_joining_metadata_to_user_data_is_refused() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_mixed").await;

    for sql in [
        format!("SELECT c.relname, t.id FROM pg_catalog.pg_class c JOIN {tbl} t ON t.id = c.oid"),
        format!("SELECT relname FROM pg_class WHERE oid IN (SELECT id FROM {tbl})"),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains(&tbl),
            "the refusal names the table it cannot reach: {}",
            err.message()
        );
    }

    // Each half on its own still answers.
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT count(*) FROM pg_catalog.pg_class WHERE relname = '{tbl}'")
        )
        .await,
        "1"
    );
    assert_eq!(
        scalar(&client, &format!("SELECT count(*) FROM {tbl}")).await,
        "3"
    );

    drop_table(&client, &tbl).await;
}

// REFUSAL. `CAST(x AS VARCHAR(3))` was accepted and the length ignored, so a statement
// asking for truncation got its value back whole. The length is the only part refused:
// an unbounded character cast has nothing to enforce and stays accepted.
#[tokio::test]
async fn test_cast_length_is_refused_rather_than_discarded() {
    let client = ready_client().await;

    for sql in [
        "SELECT CAST('abcdef' AS VARCHAR(3))",
        "SELECT 'abcdef'::VARCHAR(3)",
        "SELECT CAST('abcdef' AS CHAR(3))",
        "SELECT CAST('abcdef' AS CHARACTER VARYING(3))",
    ] {
        let err = assert_sqlstate(&client, sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("substr()"),
            "the refusal names what to write instead: {}",
            err.message()
        );
    }

    assert_eq!(
        scalar(&client, "SELECT CAST('abcdef' AS VARCHAR)").await,
        "abcdef"
    );
    assert_eq!(
        scalar(&client, "SELECT substr('abcdef', 1, 3)").await,
        "abc"
    );
}

// ============================================================================
// 4. PostgreSQL aggregate spellings
// ============================================================================

// REWRITE. Three PostgreSQL aggregate names DataFusion does not answer to. `variance`
// and `every` are exact synonyms of `var_samp` and `bool_and`. `any_value` is defined
// as an arbitrary value *among the non-null inputs*, which `min` satisfies and
// DataFusion's `first_value` does not.
#[tokio::test]
async fn test_postgres_aggregate_spellings() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_agg",
        &format!("(id INTEGER NOT NULL, n INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, n) VALUES (1, 10), (2, NULL), (3, 30)"),
    )
    .await
    .unwrap();

    // var_samp over 10 and 30 is 200.
    assert_eq!(
        scalar_number(&client, &format!("SELECT variance(n) FROM {tbl}")).await,
        200.0
    );
    assert_eq!(
        scalar(&client, &format!("SELECT every(id > 0) FROM {tbl}")).await,
        "t"
    );
    assert_eq!(
        scalar(&client, &format!("SELECT every(n IS NOT NULL) FROM {tbl}")).await,
        "f"
    );
    // Non-null, which is the part of `any_value`'s contract that can be asserted: the
    // NULL row must not win.
    assert_eq!(
        scalar(&client, &format!("SELECT any_value(n) FROM {tbl}")).await,
        "10"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 5. PostgreSQL built-in functions DataFusion does not ship
// ============================================================================

// The `datafusion-pg-functions` compatibility layer, registered on the coordinator's
// planner *and* on every executor. Only its math family is implemented upstream at
// 0.1, so that is what is asserted; the rest of its categories are empty modules and
// enabling them would register nothing.
#[tokio::test]
async fn test_postgres_math_functions_resolve() {
    let client = ready_client().await;

    assert_eq!(scalar_number(&client, "SELECT ceiling(1.2)").await, 2.0);
    assert_eq!(scalar_number(&client, "SELECT sign(-3.0)").await, -1.0);
    assert_eq!(scalar_number(&client, "SELECT div(9, 4)").await, 2.0);
    assert_eq!(scalar_number(&client, "SELECT sind(90)").await, 1.0);
    assert_eq!(scalar_number(&client, "SELECT cosd(0)").await, 1.0);
    assert_eq!(
        scalar_number(&client, "SELECT width_bucket(5.0, 0.0, 10.0, 5)").await,
        3.0
    );
    // erf(0) is 0 and gamma(5) is 4! — enough to show the function ran rather than
    // resolved to something else.
    assert_eq!(scalar_number(&client, "SELECT erf(0.0)").await, 0.0);
    assert_eq!(scalar_number(&client, "SELECT gamma(5.0)").await, 24.0);
}

// DISTRIBUTED. The case the coordinator's own registration does not cover: a UDF
// crosses the wire as a name, so a function the planner knows and the executor does
// not resolves at planning time and then fails when the stage is deserialized on the
// core node — after the client's query was accepted. Running the same functions over
// a sharded column is what exercises that path.
#[tokio::test]
async fn test_postgres_math_functions_resolve_on_the_executors() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_pgmath").await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT ceiling(val), sign(val) FROM {tbl} ORDER BY id"),
    )
    .await
    .unwrap();
    let ceilings: Vec<f64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    let signs: Vec<f64> = rows
        .iter()
        .map(|r| r[1].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(ceilings, vec![2.0, -2.0, 4.0]);
    assert_eq!(signs, vec![1.0, -1.0, 1.0]);

    // Aggregated over the shards, so the function runs inside a stage rather than in
    // a projection the coordinator could have evaluated by itself.
    assert_eq!(
        scalar_number(&client, &format!("SELECT SUM(sign(val)) FROM {tbl}")).await,
        1.0
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 6. Ranking functions advertise the type PostgreSQL promises
// ============================================================================

// `row_number()`, `rank()` and `dense_rank()` are `bigint` in PostgreSQL. DataFusion
// types them `UInt64`, which has no PostgreSQL equivalent and which arrow-pg
// advertises as `numeric` — the one row most likely to break a conforming driver
// outright, since it fails on the column type before reading a value. Describe and
// Execute have to agree on the answer, so both halves are asserted.
#[tokio::test]
async fn test_ranking_functions_are_bigint() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_rank").await;

    let sql = format!(
        "SELECT row_number() OVER (ORDER BY id), rank() OVER (ORDER BY id), \
         dense_rank() OVER (ORDER BY id) FROM {tbl}"
    );

    assert_eq!(
        describe_result_types(&client, &sql).await,
        vec![Type::INT8, Type::INT8, Type::INT8],
        "a ranking column is bigint, not numeric"
    );

    // Reading the column as an i64 is what a driver does with that OID, so this fails
    // if Execute sends a numeric body under the int8 header Describe promised.
    let stmt = client.prepare(&sql).await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    let numbers: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
    assert_eq!(numbers, vec![1, 2, 3]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 7. Decimal literals are exact
// ============================================================================

// DataFusion read an unsuffixed decimal literal as `Float64`, so `0.1 + 0.2 = 0.3`
// answered **false** on the read path while the same literal reached DuckDB on the
// write path, which follows PostgreSQL and answers **true**. One database cannot hold
// both answers and the contract is PostgreSQL's, so the read path now parses the
// literal as an exact decimal.
#[tokio::test]
async fn test_decimal_literals_are_exact() {
    let client = ready_client().await;

    assert_eq!(scalar(&client, "SELECT 0.1 + 0.2 = 0.3").await, "t");
    assert_eq!(scalar(&client, "SELECT 0.1 + 0.2").await, "0.3");
    assert_eq!(scalar(&client, "SELECT 1.5 + 1.5").await, "3.0");
    // Still a float when a float is what was asked for.
    assert_eq!(
        scalar(&client, "SELECT 0.1::double + 0.2::double = 0.3::double").await,
        "f",
        "an explicit double is binary floating point, in PostgreSQL too"
    );
}

// The same setting governs integer literals too large for `i64`, which used to become
// `Float64` and lose their low digits with nothing said. They are now exact for as far
// as an exact type reaches: `Decimal128` holds 38 digits, and past that the literal is
// `Decimal256`, which has no PostgreSQL OID here — so it is refused rather than
// rounded, which is the same choice the rest of this file makes.
#[tokio::test]
async fn test_large_integer_literals_keep_their_digits() {
    let client = ready_client().await;

    assert_eq!(
        scalar(&client, "SELECT 123456789012345678901234567890").await,
        "123456789012345678901234567890",
        "30 digits, exact — this used to read back as 123456789012345680000000000000"
    );
    // 39 digits: one past what `Decimal128` can hold.
    assert_rejected(&client, "SELECT 123456789012345678901234567890123456789").await;
}

// ============================================================================
// 8. A scalar subquery in the select list is answered, not nulled
// ============================================================================

/// An orders/lines pair, each sharded on its own `id`, so a subquery relating the two
/// spans shards in both directions. Order 4 deliberately has no lines: the subquery has
/// to answer NULL there *because there is no row*, which is the answer the folding rule
/// used to give for every row and every order.
async fn setup_order_tables(client: &Client, prefix: &str) -> (String, String) {
    let orders = create_table(
        client,
        &format!("{prefix}_orders"),
        &format!("(id INTEGER NOT NULL, label VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    let lines = create_table(
        client,
        &format!("{prefix}_lines"),
        &format!(
            "(id INTEGER NOT NULL, oid INTEGER NOT NULL, amount INTEGER NOT NULL) \
                  {CREATE_OPTS}"
        ),
    )
    .await;
    execute(
        client,
        &format!(
            "INSERT INTO {orders} (id, label) VALUES \
             (1, 'a'), (2, 'b'), (3, 'c'), (4, 'empty')"
        ),
    )
    .await
    .unwrap();
    execute(
        client,
        &format!(
            "INSERT INTO {lines} (id, oid, amount) VALUES \
             (1, 1, 5), (2, 1, 15), (3, 2, 5), (4, 3, 30), (5, 3, 40)"
        ),
    )
    .await
    .unwrap();
    (orders, lines)
}

// The gap this closes was not a refusal but a wrong answer: `datafusion-pg-catalog`'s
// `RemoveSubqueryFromProjection` rule replaced a select-list subquery it read as
// correlated with a `NULL` literal, so the statement came back as a column of NULLs
// with nothing to say the subquery had never run. VaireDB now re-applies the same
// compatibility chain without that one rule for exactly those statements, so what the
// client gets is DataFusion's own answer — or DataFusion's own error, which is loud.
#[tokio::test]
async fn test_correlated_projection_subquery_is_answered() {
    let client = ready_client().await;
    let (orders, lines) = setup_order_tables(&client, "expr_sq").await;

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT o.id, (SELECT sum(l.amount) FROM {lines} l WHERE l.oid = o.id) \
             FROM {orders} o ORDER BY o.id"
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].as_deref(), r[1].as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (Some("1"), Some("20")),
            (Some("2"), Some("5")),
            (Some("3"), Some("70")),
            (Some("4"), None),
        ],
        "every row is the sum of its own lines, and NULL only where there are none"
    );

    drop_table(&client, &orders).await;
    drop_table(&client, &lines).await;
}

// Two shapes the rule folded that are not correlated at all: its test counts any `$N`
// placeholder as a reference to the outer row, and any `t.col` whose `t` is not an
// *aliased* table of the subquery's own FROM. Both are ordinary uncorrelated
// subqueries, and both used to come back NULL.
#[tokio::test]
async fn test_uncorrelated_projection_subquery_shapes_are_answered() {
    let client = ready_client().await;
    let (orders, lines) = setup_order_tables(&client, "expr_squ").await;

    // A qualified name whose table carries no alias.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT (SELECT count(*) FROM {lines} WHERE {lines}.oid = 3) \
                 FROM {orders} WHERE id = 1"
            )
        )
        .await,
        "2"
    );

    // A parameter, over the extended protocol, which is how a driver sends one.
    let stmt = client
        .prepare(&format!(
            "SELECT (SELECT sum(amount) FROM {lines} WHERE oid = $1) FROM {orders} WHERE id = 1"
        ))
        .await
        .unwrap();
    let rows = client.query(&stmt, &[&3i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, i64>(0),
        70,
        "the placeholder binds in the subquery, which runs"
    );

    drop_table(&client, &orders).await;
    drop_table(&client, &lines).await;
}

// The takeover is entered by recognising the shape the rule leaves behind, so a client
// that writes `NULL` itself must be unaffected — and the introspection queries the rule
// was written for must keep the NULL fallback that lets them answer at all.
#[tokio::test]
async fn test_a_client_written_null_is_left_alone() {
    let client = ready_client().await;

    let rows = simple_query_rows(&client, "SELECT NULL AS nothing")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], None);
}

// The second wrong answer from the same crate's rule set, and the same shape of fix.
// `RewriteArrayAnyAllOperation` turns `x = ANY (…)` into an `array_contains` call by
// looking at the operator alone, never at what the right-hand side is — so a *subquery*
// there became `array_contains(<subquery>, x)`, which no planner resolves. VaireDB now
// normalizes the two affected spellings to the `IN`/`NOT IN` forms that mean exactly the
// same thing, on the client's own AST and before the rule can see them.
//
// Membership over a subquery is the everyday PostgreSQL an ORM emits for a nested
// filter, and both sides span shards here: `lines` is sharded on `lines.id`, so the
// candidate list is gathered across all three shards before `orders` is probed.
#[tokio::test]
async fn test_any_and_all_over_a_subquery_are_membership_tests() {
    let client = ready_client().await;
    let (orders, lines) = setup_order_tables(&client, "expr_anysub").await;

    // Orders 1, 2 and 3 have lines; order 4 does not.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {orders} WHERE id = ANY (SELECT oid FROM {lines}) ORDER BY id"),
    )
    .await
    .unwrap_or_else(|e| panic!("`= ANY (subquery)` should be answerable: {e}"));
    assert_eq!(
        rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string())
        ],
        "`= ANY (subquery)` is membership"
    );

    // Its complement: the anti-join, which is the order with no lines.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {orders} WHERE id <> ALL (SELECT oid FROM {lines}) ORDER BY id"),
    )
    .await
    .unwrap_or_else(|e| panic!("`<> ALL (subquery)` should be answerable: {e}"));
    assert_eq!(
        rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec![Some("4".to_string())],
        "`<> ALL (subquery)` is non-membership"
    );

    // The same two questions in the spellings the normalization produces, so the
    // rewrite is pinned against the answer it claims to be equivalent to rather than
    // only against itself.
    for (any_all, in_form) in [
        (
            "id = ANY (SELECT oid FROM {lines})",
            "id IN (SELECT oid FROM {lines})",
        ),
        (
            "id <> ALL (SELECT oid FROM {lines})",
            "id NOT IN (SELECT oid FROM {lines})",
        ),
    ] {
        let subst = |p: &str| p.replace("{lines}", &lines);
        assert_eq!(
            scalar(
                &client,
                &format!("SELECT count(*) FROM {orders} WHERE {}", subst(any_all))
            )
            .await,
            scalar(
                &client,
                &format!("SELECT count(*) FROM {orders} WHERE {}", subst(in_form))
            )
            .await,
            "`{any_all}` must agree with `{in_form}`"
        );
    }

    drop_table(&client, &orders).await;
    drop_table(&client, &lines).await;
}

// The array forms are what that rule is *for*, and they have to keep working: a driver
// binding an "id in list" parameter sends `= ANY (ARRAY[…])`, which DataFusion answers
// through `array_contains`. The normalization must not have swept them in.
#[tokio::test]
async fn test_any_over_an_array_is_still_answered() {
    let client = ready_client().await;
    let (orders, lines) = setup_order_tables(&client, "expr_anyarr").await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {orders} WHERE id = ANY (ARRAY[2, 4]) ORDER BY id"),
    )
    .await
    .unwrap_or_else(|e| panic!("`= ANY (array)` should be answerable: {e}"));
    assert_eq!(
        rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec![Some("2".to_string()), Some("4".to_string())]
    );

    drop_table(&client, &orders).await;
    drop_table(&client, &lines).await;
}

// ============================================================================
// 9. Window clauses DataFusion parses and then drops
// ============================================================================

/// Six rows in two groups of three, so a window has partitions to keep apart and a
/// group has both a NULL and non-NULLs for the clauses below to visibly skip or not.
async fn setup_window_table(client: &Client, prefix: &str) -> String {
    let tbl = create_table(
        client,
        prefix,
        &format!("(id INTEGER NOT NULL, g INTEGER NOT NULL, n INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!(
            "INSERT INTO {tbl} (id, g, n) VALUES \
             (1, 1, 10), (2, 1, NULL), (3, 1, 30), (4, 2, 40), (5, 2, 50), (6, 2, NULL)"
        ),
    )
    .await
    .unwrap();
    tbl
}

// REFUSAL ×3. Three clauses PostgreSQL implements, DataFusion's parser accepts, and
// DataFusion's planner then discards — each of which turns a precise question into a
// plausible wrong answer with nothing to mark it. Each refusal is scoped to exactly the
// form that loses the clause, which is why the working neighbours are asserted in the
// same test: refusing the keyword instead of the form would have cost three queries
// that answer correctly today.
#[tokio::test]
async fn test_window_clauses_datafusion_discards_are_refused() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_wdw").await;

    // FILTER on a *windowed* aggregate: the filter does not cross into the window
    // expression, so every row of the window would be counted as though the client had
    // never written the condition.
    let err = assert_sqlstate(
        &client,
        &format!("SELECT count(*) FILTER (WHERE n > 20) OVER (PARTITION BY g) FROM {tbl}"),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
    )
    .await;
    assert!(
        err.message().to_lowercase().contains("filter"),
        "the refusal names the clause: {}",
        err.message()
    );

    // The same FILTER without OVER is an ordinary aggregate filter, and correct: 30 and
    // 40 and 50 pass, 10 and the two NULLs do not.
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT count(*) FILTER (WHERE n > 20) FROM {tbl}")
        )
        .await,
        "3",
        "FILTER without OVER is not refused and not wrong"
    );

    // A window specification that only *names* another window: the named window's own
    // PARTITION BY and ORDER BY are dropped, so the function would aggregate over the
    // whole result instead of over that window.
    for sql in [
        format!("SELECT sum(n) OVER (w) FROM {tbl} WINDOW w AS (PARTITION BY g ORDER BY id)"),
        format!("SELECT sum(n) OVER (w ORDER BY id) FROM {tbl} WINDOW w AS (PARTITION BY g)"),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains('w'),
            "the refusal names the window: {}",
            err.message()
        );
    }

    // The same lost clause declared in the WINDOW list instead of the OVER: `w2 AS (w1)`
    // drops w1's partition, so the sum covers the whole table.
    for sql in [
        format!(
            "SELECT sum(n) OVER w2 FROM {tbl} \
             WINDOW w1 AS (PARTITION BY g ORDER BY id), w2 AS (w1)"
        ),
        format!(
            "SELECT sum(n) OVER w2 FROM {tbl} \
             WINDOW w1 AS (PARTITION BY g), w2 AS (w1 ORDER BY id)"
        ),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("w1"),
            "the refusal names the inherited window: {}",
            err.message()
        );
    }

    // Two WINDOW definitions that inherit nothing from each other lose nothing, so both
    // are answered: the group's total beside the row's own.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT sum(n) OVER w1, sum(n) OVER w2 FROM {tbl} \
             WINDOW w1 AS (PARTITION BY g), w2 AS (PARTITION BY id) ORDER BY id LIMIT 1"
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some("40"),
        "an independent WINDOW definition is not refused"
    );
    assert_eq!(rows[0][1].as_deref(), Some("10"));

    // Referring to the window without parentheses keeps its clauses, and is the spelling
    // the refusal above suggests. Group 1 totals 40 and group 2 totals 90.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT g, sum(n) OVER w FROM {tbl} \
             WINDOW w AS (PARTITION BY g) ORDER BY g, id"
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].as_deref(), r[1].as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (Some("1"), Some("40")),
            (Some("1"), Some("40")),
            (Some("1"), Some("40")),
            (Some("2"), Some("90")),
            (Some("2"), Some("90")),
            (Some("2"), Some("90")),
        ],
        "OVER w without parentheses keeps the named window's PARTITION BY"
    );

    // IGNORE NULLS is discarded, so a NULL the client asked to skip would be returned as
    // the answer — the worst of the three, because the value looks like data.
    let err = assert_sqlstate(
        &client,
        &format!("SELECT last_value(n) IGNORE NULLS OVER (PARTITION BY g ORDER BY id) FROM {tbl}"),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
    )
    .await;
    assert!(
        err.message().to_lowercase().contains("ignore nulls"),
        "the refusal names the clause: {}",
        err.message()
    );

    // RESPECT NULLS asks for the behaviour DataFusion already has, so dropping it changes
    // nothing and it is not refused.
    assert_eq!(
        scalar(
            &client,
            &format!("SELECT last_value(n) RESPECT NULLS OVER (ORDER BY id) FROM {tbl} LIMIT 1")
        )
        .await,
        "10",
        "RESPECT NULLS is a no-op here, so it is accepted"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 10. A result column is labelled the way PostgreSQL labels it
// ============================================================================

// PostgreSQL names an unaliased result column after the function that produced it.
// DataFusion names it after the whole expression as it renders internally, which for a
// window function is a rendering of the frame the client never wrote — 99 bytes for the
// query below, past PostgreSQL's own 63-byte limit for a name. A driver that reads
// columns by name takes the label as the contract, so this is a wrong answer to a
// question the client did ask.
#[tokio::test]
async fn test_a_function_column_is_labelled_the_way_postgres_labels_it() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_label").await;

    assert_eq!(
        describe_result_labels(&client, &format!("SELECT sum(n) FROM {tbl}")).await,
        vec!["sum"]
    );
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT count(*) FROM {tbl}")).await,
        vec!["count"]
    );
    assert_eq!(
        describe_result_labels(
            &client,
            &format!("SELECT row_number() OVER (ORDER BY id) FROM {tbl}")
        )
        .await,
        vec!["row_number"],
        "not the 99-byte rendering of a frame the client never wrote"
    );

    // The client's own spelling, because the label is applied before the aggregate is
    // renamed to the DataFusion equivalent it is planned as.
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT variance(n) FROM {tbl}")).await,
        vec!["variance"],
        "labelled as written, not as var_samp"
    );

    // An alias the client wrote is never replaced.
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT sum(n) AS total FROM {tbl}")).await,
        vec!["total"]
    );

    // Two distinct functions each get their own label.
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT sum(n), count(*) FROM {tbl}")).await,
        vec!["sum", "count"]
    );

    // PostgreSQL would return two columns both called `sum` here; DataFusion refuses a
    // projection with two fields of one name outright, so the label is left off rather
    // than turning a working query into a planning error. Verbose, and answering.
    let labels =
        describe_result_labels(&client, &format!("SELECT sum(n), sum(g) FROM {tbl}")).await;
    assert_eq!(labels.len(), 2);
    assert_ne!(labels[0], labels[1], "the two labels stay distinct");

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 11. sum() and avg() widen the way PostgreSQL widens
// ============================================================================

// PostgreSQL widens as it sums: `sum(bigint)` is `numeric`, so a total never overflows
// the type of the column it came from. DataFusion accumulated `sum(Int64)` in `Int64`
// and wrapped silently — two rows of 4611686018427387904 summed to a *negative* number,
// advertised as bigint and reported without complaint. The read path now accumulates in
// a decimal wide enough that no row count can exhaust it.
#[tokio::test]
async fn test_sum_of_bigint_does_not_wrap() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_sumwide",
        &format!(
            "(id INTEGER NOT NULL, big BIGINT NOT NULL, small INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;
    // Two halves of i64::MAX, plus 3 — so the exact total is i64::MAX + 3 and wrapping is
    // the difference between a right answer and a negative one.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, big, small) VALUES \
             (1, 4611686018427387904, 1), (2, 4611686018427387906, 2)"
        ),
    )
    .await
    .unwrap();

    let sql = format!("SELECT sum(big) FROM {tbl}");
    assert_eq!(
        scalar(&client, &sql).await,
        "9223372036854775810",
        "the exact total, which does not fit in a bigint"
    );
    assert_eq!(
        describe_result_types(&client, &sql).await,
        vec![Type::NUMERIC],
        "and it is advertised as numeric, as PostgreSQL advertises it"
    );

    // The same aggregate as a window function, which DataFusion plans as a different node
    // over the same accumulator. Both spellings have to report the same type, or a client
    // gets `numeric` and `bigint` for the same total depending on how it was written.
    let running = format!("SELECT sum(big) OVER (ORDER BY id) FROM {tbl} ORDER BY id");
    assert_eq!(
        simple_query_rows(&client, &running)
            .await
            .unwrap()
            .iter()
            .map(|r| r[0].clone().unwrap())
            .collect::<Vec<String>>(),
        vec!["4611686018427387904", "9223372036854775810"],
        "the running total is exact through the row that overflows a bigint"
    );
    assert_eq!(
        describe_result_types(&client, &running).await,
        vec![Type::NUMERIC],
        "and the window spelling advertises what the grouped one does"
    );

    // `sum(integer)` is already the bigint PostgreSQL promises, so it must not be widened
    // — that would disagree with PostgreSQL in the other direction.
    let small = format!("SELECT sum(small) FROM {tbl}");
    assert_eq!(scalar(&client, &small).await, "3");
    assert_eq!(
        describe_result_types(&client, &small).await,
        vec![Type::INT8],
        "sum(integer) is bigint"
    );
    assert_eq!(
        describe_result_types(&client, &format!("SELECT sum(small) OVER () FROM {tbl}")).await,
        vec![Type::INT8],
        "and neither is its window spelling"
    );

    drop_table(&client, &tbl).await;
}

// PostgreSQL's `avg` is `numeric` over every integer width, and exact. DataFusion
// computed it in `float8`, which carries 53 bits of mantissa against `bigint`'s 63, so
// the average of 6148914691236517205 came back as 6148914691236517000 — the low digits
// replaced by zeros, advertised as `double precision`. Same rewrite as `sum`, one arm
// apart: `avg` widens from `integer` too, because PostgreSQL promises `numeric` there
// where it promises `bigint` for `sum(integer)`.
#[tokio::test]
async fn test_avg_of_an_integer_is_exact_numeric() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_avgwide",
        &format!(
            "(id INTEGER NOT NULL, big BIGINT NOT NULL, small INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;
    // Three rows whose average, 6148914691236517205, is past 2^53 and so unrepresentable
    // in a float — and whose `integer` column averages to a non-terminating 5/3.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, big, small) VALUES \
             (1, 6148914691236517204, 1), (2, 6148914691236517205, 2), \
             (3, 6148914691236517206, 2)"
        ),
    )
    .await
    .unwrap();

    let sql = format!("SELECT avg(big) FROM {tbl}");
    let text = scalar(&client, &sql).await;
    assert!(
        text.starts_with("6148914691236517205"),
        "the exact average, which a float rounds to ...000: got {text}"
    );
    assert_eq!(
        describe_result_types(&client, &sql).await,
        vec![Type::NUMERIC],
        "and advertised as numeric, as PostgreSQL advertises it"
    );

    // `avg(integer)` is `numeric` in PostgreSQL — the arm where this rewrite and the
    // `sum` one deliberately differ, since `sum(integer)` is `bigint`.
    let small = format!("SELECT avg(small) FROM {tbl}");
    assert_eq!(
        describe_result_types(&client, &small).await,
        vec![Type::NUMERIC],
        "avg(integer) is numeric, unlike sum(integer)"
    );
    // Non-terminating: PostgreSQL answers 1.6666666666666667. Ten decimal places is the
    // accumulator's scale, and the narrowing recorded in the gap analysis — what is
    // asserted here is that the digits present are right.
    let text = scalar(&client, &small).await;
    assert!(
        text.starts_with("1.6666666666"),
        "ten correct decimal places: got {text}"
    );

    // The window spelling reports what the grouped one reports, or a client gets two
    // types for one average depending on how it was written.
    assert_eq!(
        describe_result_types(&client, &format!("SELECT avg(big) OVER () FROM {tbl}")).await,
        vec![Type::NUMERIC],
    );
    assert_eq!(
        describe_result_types(&client, &format!("SELECT avg(small) OVER () FROM {tbl}")).await,
        vec![Type::NUMERIC],
    );

    // `avg(double precision)` is `double precision` in PostgreSQL, so it must not move.
    assert_eq!(
        describe_result_types(&client, &format!("SELECT avg(big::float8) FROM {tbl}")).await,
        vec![Type::FLOAT8],
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 12. A window survives being cut into distributed stages
// ============================================================================

// An equality predicate on the PARTITION BY column made the coordinator's optimizer
// conclude that column was constant — correctly — and therefore skip sorting it. Ballista
// then cut the plan at the shuffle, and the stage holding the window no longer had the
// filter that justified the conclusion, so the window failed at runtime with a
// DataFusion internal error about a PARTITION BY expression not being ordered. The
// coordinator now puts the partition columns back into the window's own sort, so the
// requirement is met by an ordering physically present inside that stage.
#[tokio::test]
async fn test_a_window_partitioned_by_a_filtered_column_answers() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_wsplit").await;

    // The failing shape: `g = 1` is an equality on the partition column.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT id, row_number() OVER (PARTITION BY g ORDER BY id) AS rn \
             FROM {tbl} WHERE g = 1 ORDER BY id"
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].as_deref(), r[1].as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (Some("1"), Some("1")),
            (Some("2"), Some("2")),
            (Some("3"), Some("3")),
        ],
        "the three rows of group 1, numbered in id order"
    );

    // Every ordering key constant, which leaves the plan with no sort to widen at all —
    // one has to be inserted instead.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT row_number() OVER (PARTITION BY g, id ORDER BY id) \
                 FROM {tbl} WHERE g = 1 AND id = 2"
            )
        )
        .await,
        "1"
    );

    // The neighbours that always worked, asserted so the repair is not hiding behind a
    // plan it changed for everyone: a non-equality predicate, and no predicate at all.
    assert_eq!(
        scalar(
            &client,
            &format!(
                "SELECT count(*) FROM (SELECT row_number() OVER (PARTITION BY g ORDER BY id) \
                 FROM {tbl} WHERE g > 0) s"
            )
        )
        .await,
        "6"
    );

    drop_table(&client, &tbl).await;
}

// `ntile(n)` distributes rows into n buckets as evenly as it can, putting the larger
// buckets first: 5 rows into 3 buckets is 2, 2, 1 and not 2, 2, 1 read backwards. The
// gap analysis recorded this as wrong on DataFusion 53; it is correct on 54.1, and the
// assertion is here so a regression is a test failure rather than a rediscovery.
#[tokio::test]
async fn test_ntile_fills_the_larger_buckets_first() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_ntile").await;

    let buckets = |sql: String| {
        let client = &client;
        async move {
            simple_query_rows(client, &sql)
                .await
                .unwrap()
                .iter()
                .map(|r| r[0].clone().unwrap())
                .collect::<Vec<String>>()
        }
    };

    // 6 rows into 3 buckets: even, 2 each.
    assert_eq!(
        buckets(format!(
            "SELECT ntile(3) OVER (ORDER BY id) FROM {tbl} ORDER BY id"
        ))
        .await,
        vec!["1", "1", "2", "2", "3", "3"]
    );
    // 5 rows into 3 buckets: 2, 2, 1 — the remainder goes to the *first* buckets.
    assert_eq!(
        buckets(format!(
            "SELECT ntile(3) OVER (ORDER BY id) FROM {tbl} WHERE id < 6 ORDER BY id"
        ))
        .await,
        vec!["1", "1", "2", "2", "3"]
    );
    // 5 rows into 2 buckets: 3, 2.
    assert_eq!(
        buckets(format!(
            "SELECT ntile(2) OVER (ORDER BY id) FROM {tbl} WHERE id < 6 ORDER BY id"
        ))
        .await,
        vec!["1", "1", "1", "2", "2"]
    );

    drop_table(&client, &tbl).await;
}

// A parameter the client leaves untyped, in the one place DataFusion's planner cannot
// type it from a column: against an aggregate. Untyped meant decoded as text, and the
// comparison became lexicographic — `'40' > '5'` is false because `'4' < '5'`, so a group
// that clears the threshold numerically was dropped from the answer. Wrong rows, no
// error. The type now comes from the aggregate beside the placeholder.
#[tokio::test]
async fn test_an_untyped_parameter_beside_an_aggregate_compares_as_a_number() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_hvp").await;

    // Group 1 sums to 40 (10 + NULL + 30) and group 2 to 90 (40 + 50 + NULL).
    let having = format!("SELECT g FROM {tbl} GROUP BY g HAVING sum(n) > $1 ORDER BY g");

    // Describe is where the type has to appear: it is what the client serializes the
    // bound value against. `tokio-postgres` declares no parameter OIDs at Parse, so
    // whatever comes back is the server's own inference.
    assert_eq!(
        describe_param_types(&client, &having).await,
        vec![Type::INT8],
        "the parameter is typed from the bigint aggregate it is compared against"
    );

    let groups = |threshold: i64| {
        let having = having.clone();
        let client = &client;
        async move {
            client
                .query(&having, &[&threshold])
                .await
                .unwrap_or_else(|e| panic!("`{having}` with ${threshold} failed: {e}"))
                .iter()
                .map(|r| r.get::<_, i32>(0))
                .collect::<Vec<i32>>()
        }
    };

    // 5 is the discriminating threshold: both totals clear it numerically, but as text
    // only `'90'` does. A lexicographic compare answers `[2]` here.
    assert_eq!(groups(5).await, vec![1, 2], "both totals are above 5");
    // And 100, where the text compare says both totals clear it and neither does.
    assert_eq!(
        groups(100).await,
        Vec::<i32>::new(),
        "neither total is above 100"
    );
    // A threshold between them, which both readings happen to agree on — kept so the
    // test would notice a fix that answered the two ends by dropping the comparison.
    assert_eq!(groups(50).await, vec![2]);

    drop_table(&client, &tbl).await;
}

// The other placeholder DataFusion cannot type, for the opposite reason: a row count
// stands alone, with no expression beside it to be inferred from. PostgreSQL's grammar
// admits only a row count there and declares the parameter `bigint`, so VaireDB does too.
#[tokio::test]
async fn test_a_row_count_parameter_is_a_bigint() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_lmp").await;

    let limit = format!("SELECT id FROM {tbl} ORDER BY id LIMIT $1");
    assert_eq!(
        describe_param_types(&client, &limit).await,
        vec![Type::INT8]
    );
    let ids: Vec<i32> = client
        .query(&limit, &[&2i64])
        .await
        .unwrap_or_else(|e| panic!("`{limit}` failed: {e}"))
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "the row count is read as a number, not as text"
    );

    let offset = format!("SELECT id FROM {tbl} ORDER BY id OFFSET $1");
    assert_eq!(
        describe_param_types(&client, &offset).await,
        vec![Type::INT8]
    );
    let ids: Vec<i32> = client
        .query(&offset, &[&4i64])
        .await
        .unwrap_or_else(|e| panic!("`{offset}` failed: {e}"))
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(ids, vec![5, 6]);

    drop_table(&client, &tbl).await;
}

// The neighbour that must not move: where the planner types the placeholder from a
// column, that type is the one to report, and this pass has no business widening a text
// comparison into a numeric one.
#[tokio::test]
async fn test_a_parameter_typed_from_a_column_keeps_the_columns_type() {
    let client = ready_client().await;
    let tbl = setup_probe_table(&client, "expr_colp").await;

    // TEXT and not VARCHAR because that is how the coordinator advertises a VARCHAR
    // column — a type-layer divergence recorded in the data-type analysis, and inherited
    // here rather than caused here.
    assert_eq!(
        describe_param_types(&client, &format!("SELECT id FROM {tbl} WHERE name = $1")).await,
        vec![Type::TEXT]
    );
    assert_eq!(
        describe_param_types(&client, &format!("SELECT id FROM {tbl} WHERE id = $1")).await,
        vec![Type::INT4]
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 13. The write path reads a predicate the way the read path does
// ============================================================================
//
// The two paths are two engines. A `SELECT` is planned by DataFusion and executed as
// Ballista stages; an `UPDATE`, `DELETE` or `INSERT` is rendered back to SQL text and
// executed *verbatim* by each shard's DuckDB. So a single predicate has two readers,
// and where they disagree there is no error to see — the statement runs, reports a row
// count, and the rows are not the ones the client asked for.
//
// Every test in this section therefore asserts the same predicate twice: once as the
// WHERE of a SELECT and once as the WHERE of an UPDATE, against the answer PostgreSQL
// gives. Two assertions that agree with each other but not with PostgreSQL would be a
// consistent product and still the wrong one, which is why the expected ids are written
// out rather than compared path against path.

/// A five-row sharded table whose text values separate the pattern operators: `alpha`
/// and `axb` are the rows a mis-read wildcard picks up, `a_b` is the row a literal
/// underscore should pick up alone, and `Alice` only matches case-insensitively.
async fn setup_pattern_table(client: &Client, prefix: &str) -> String {
    let tbl = create_table(
        client,
        prefix,
        &format!("(id INTEGER NOT NULL, name VARCHAR NOT NULL, flag INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!(
            "INSERT INTO {tbl} (id, name, flag) VALUES \
             (1, 'alpha', 0), (2, 'Alice', 0), (3, 'beta', 0), (4, 'a_b', 0), (5, 'axb', 0)"
        ),
    )
    .await
    .unwrap();
    tbl
}

/// The ids `predicate` selects on the **read path**: DataFusion plans it, a core node
/// runs the stage.
async fn ids_selected(client: &Client, tbl: &str, predicate: &str) -> Vec<i32> {
    let sql = format!("SELECT id FROM {tbl} WHERE {predicate} ORDER BY id");
    client
        .query(&sql, &[])
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"))
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect()
}

/// The ids `predicate` selects on the **write path**: the same text, rendered back to
/// SQL and run verbatim by each shard's DuckDB, recovered through the flag it set.
///
/// The UPDATE's own reported count is checked against what the flag says, because that
/// count is what a client believes — a predicate that matched the wrong rows and a
/// count that agreed with it is exactly the failure this section exists to catch.
async fn ids_updated(client: &Client, tbl: &str, predicate: &str) -> Vec<i32> {
    execute(client, &format!("UPDATE {tbl} SET flag = 0"))
        .await
        .unwrap();
    let sql = format!("UPDATE {tbl} SET flag = 1 WHERE {predicate}");
    let affected = execute(client, &sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be executable: {e}"));

    let ids = ids_selected(client, tbl, "flag = 1").await;
    assert_eq!(
        affected as usize,
        ids.len(),
        "`{sql}` reported {affected} rows and changed {}",
        ids.len()
    );
    ids
}

/// Assert both paths give PostgreSQL's answer to `predicate`.
async fn both_paths_select(client: &Client, tbl: &str, predicate: &str, expected: Vec<i32>) {
    assert_eq!(
        ids_selected(client, tbl, predicate).await,
        expected,
        "read path, `WHERE {predicate}`"
    );
    assert_eq!(
        ids_updated(client, tbl, predicate).await,
        expected,
        "write path, `WHERE {predicate}`"
    );
}

/// REWRITE. PostgreSQL's `~` is a *partial* match. DuckDB's is a full one, so an
/// unanchored-at-the-end pattern like `^a` matched nothing at all on a write and the
/// UPDATE reported zero rows — the most silent failure of the set, because zero rows is
/// also a perfectly ordinary answer.
#[tokio::test]
async fn test_the_regex_operators_match_partially_on_both_paths() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_rx").await;

    both_paths_select(&client, &tbl, "name ~ '^a'", vec![1, 4, 5]).await;
    both_paths_select(&client, &tbl, "name !~ '^a'", vec![2, 3]).await;
    // Case-insensitive, so `Alice` joins them.
    both_paths_select(&client, &tbl, "name ~* '^a'", vec![1, 2, 4, 5]).await;
    both_paths_select(&client, &tbl, "name !~* '^a'", vec![3]).await;
    // A pattern anchored at neither end, to show the partial match is not an artefact
    // of the `^`.
    both_paths_select(&client, &tbl, "name ~ 'lph'", vec![1]).await;

    drop_table(&client, &tbl).await;
}

/// REWRITE. `LIKE` escapes with `\` in PostgreSQL and with nothing at all in DuckDB, so
/// `'a\_%'` asked for a literal underscore and a shard read it as a wildcard: three rows
/// changed where one should have. This is the shape an ORM emits whenever it escapes
/// user input, which is why it matters more than its size suggests.
#[tokio::test]
async fn test_like_escapes_with_a_backslash_on_both_paths() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_lk").await;

    // The literal underscore: `a_b` alone, not `alpha` and `axb` with it.
    both_paths_select(&client, &tbl, "name LIKE 'a\\_%'", vec![4]).await;
    both_paths_select(&client, &tbl, "name NOT LIKE 'a\\_%'", vec![1, 2, 3, 5]).await;
    both_paths_select(&client, &tbl, "name ILIKE 'A\\_%'", vec![4]).await;
    // An unescaped `_` is still the wildcard it always was — the fix must not turn
    // every underscore literal.
    both_paths_select(&client, &tbl, "name LIKE 'a_b'", vec![4, 5]).await;
    // An escape the client chose is the client's.
    both_paths_select(&client, &tbl, "name LIKE 'a!_%' ESCAPE '!'", vec![4]).await;

    drop_table(&client, &tbl).await;
}

/// REWRITE. `SIMILAR TO` is neither `LIKE` nor a regex, and DuckDB hands the pattern
/// straight to a regex engine — wrong in both directions at once, missing the rows `%`
/// should have matched and matching rows a literal `.` should not have.
#[tokio::test]
async fn test_similar_to_is_the_same_language_on_both_paths() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_sim").await;

    // `%` is the wildcard. Read as a regex this pattern matches nothing.
    both_paths_select(&client, &tbl, "name SIMILAR TO 'a%'", vec![1, 4, 5]).await;
    // `_` is one character, and the whole string must match — `alpha` does not.
    both_paths_select(&client, &tbl, "name SIMILAR TO 'a_b'", vec![4, 5]).await;
    both_paths_select(&client, &tbl, "name NOT SIMILAR TO 'a%'", vec![2, 3]).await;
    // Regex metacharacters SIMILAR TO does share: alternation and a repeat count.
    both_paths_select(&client, &tbl, "name SIMILAR TO 'beta|Alice'", vec![2, 3]).await;
    // A literal `.`, which a regex engine would read as "any character" and match on.
    both_paths_select(&client, &tbl, "name SIMILAR TO 'a.b'", vec![]).await;

    drop_table(&client, &tbl).await;
}

/// A DELETE takes the same route as an UPDATE and gets the same predicate, so the rows
/// it removes are the rows a SELECT with that predicate returned.
#[tokio::test]
async fn test_a_delete_removes_the_rows_the_predicate_named() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_del").await;

    assert_eq!(
        ids_selected(&client, &tbl, "name ~ '^a'").await,
        vec![1, 4, 5]
    );
    let sql = format!("DELETE FROM {tbl} WHERE name ~ '^a'");
    assert_eq!(execute(&client, &sql).await.unwrap(), 3, "`{sql}`");
    assert_eq!(ids_selected(&client, &tbl, "1 = 1").await, vec![2, 3]);

    drop_table(&client, &tbl).await;
}

/// `/` between integers is integer division in PostgreSQL and floating-point division in
/// DuckDB, so `7 / 2` was `3` when a SELECT computed it and `3.5` when a shard stored
/// it. One database cannot hold both answers to one expression; the shards are
/// configured to follow PostgreSQL.
#[tokio::test]
async fn test_integer_division_truncates_on_both_paths() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_div",
        &format!("(id INTEGER NOT NULL, n INTEGER, d DOUBLE PRECISION) {CREATE_OPTS}"),
    )
    .await;
    execute(&client, &format!("INSERT INTO {tbl} (id) VALUES (1)"))
        .await
        .unwrap();

    assert_eq!(scalar_number(&client, "SELECT 7 / 2").await, 3.0);
    assert_eq!(scalar_number(&client, "SELECT -7 / 2").await, -3.0);
    // A write computing the same expression stores the same value.
    execute(&client, &format!("UPDATE {tbl} SET n = 7 / 2"))
        .await
        .unwrap();
    assert_eq!(
        scalar_number(&client, &format!("SELECT n FROM {tbl}")).await,
        3.0
    );

    // Only *integer* division truncates. One float operand and it is float division on
    // both paths, which is the half of PostgreSQL's rule a blanket setting could break.
    assert_eq!(scalar_number(&client, "SELECT 7.0 / 2").await, 3.5);
    execute(&client, &format!("UPDATE {tbl} SET d = 7.0 / 2"))
        .await
        .unwrap();
    assert_eq!(
        scalar_number(&client, &format!("SELECT d FROM {tbl}")).await,
        3.5
    );

    drop_table(&client, &tbl).await;
}

/// REFUSAL. What cannot be translated is refused on the write path too, and with the
/// same code the read path uses — an expression answered on a SELECT and refused on an
/// UPDATE would be its own kind of split, just a visible one.
#[tokio::test]
async fn test_the_untranslatable_write_expressions_are_refused_like_the_read_ones() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_wrej").await;

    for (read, write) in [
        (
            format!("SELECT id FROM {tbl} WHERE name COLLATE \"en_US\" < 'a'"),
            format!("UPDATE {tbl} SET flag = 1 WHERE name COLLATE \"en_US\" < 'a'"),
        ),
        (
            format!("SELECT CAST(name AS VARCHAR(3)) FROM {tbl}"),
            format!("UPDATE {tbl} SET name = CAST(name AS VARCHAR(3))"),
        ),
        (
            format!("SELECT id FROM {tbl} WHERE name SIMILAR TO name"),
            format!("UPDATE {tbl} SET flag = 1 WHERE name SIMILAR TO name"),
        ),
    ] {
        assert_sqlstate(&client, &read, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert_sqlstate(&client, &write, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
    }

    // And the refusal is a refusal, not a rollback: nothing was changed on the way to it.
    assert_eq!(
        ids_selected(&client, &tbl, "flag = 1").await,
        Vec::<i32>::new()
    );

    drop_table(&client, &tbl).await;
}

/// REWRITE. A byte-order collation is what VaireDB in fact applies, so it is accepted
/// and dropped on the way to a shard rather than passed through. Passing it through was
/// a shard-side `Catalog Error` for two of the four spellings on DuckDB 1.5.5 —
/// including `pg_catalog.default`, which is the one a driver sends — so this is a write
/// that used to be accepted by the coordinator and then fail on the node.
#[tokio::test]
async fn test_a_byte_order_collation_is_honoured_on_both_paths() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_coll").await;

    for collation in ["\"C\"", "\"POSIX\"", "ucs_basic", "pg_catalog.default"] {
        // Byte order puts every capital before every lowercase letter, so `Alice` is the
        // only row below `a` — the answer a case-insensitive collation would not give.
        both_paths_select(
            &client,
            &tbl,
            &format!("name COLLATE {collation} < 'a'"),
            vec![2],
        )
        .await;
    }

    drop_table(&client, &tbl).await;
}

/// Division by zero is `22012 division_by_zero`, not `XX000 internal_error`.
///
/// This test earns its place in the e2e suite rather than the unit suite because the
/// classification it checks cannot be reached in a single process. The error is raised
/// inside a Ballista executor, and the scheduler serializes it to text on the way back,
/// so the coordinator receives `Job <id> failed: … DataFusionError(Execution("ArrowError(DivideByZero)"))`
/// with no `ArrowError` left to match on. The typed classifier is blind there by
/// construction. `XX000` is the class that tells a driver the server broke and the
/// statement is worth retrying, which is the opposite of true for `1 / 0`.
#[tokio::test]
async fn test_division_by_zero_is_a_data_error_not_an_internal_error() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_dz").await;

    for sql in [
        // A literal is enough: even a constant expression is planned as a distributed
        // stage, so it takes the same route back as a division over a real column.
        "SELECT 1 / 0".to_string(),
        "SELECT 1.0 / 0".to_string(),
        // Over a column, which is the shape a client actually writes.
        format!("SELECT id / (id - id) FROM {tbl}"),
    ] {
        assert_sqlstate(&client, &sql, SQLSTATE_DIVISION_BY_ZERO).await;
    }

    drop_table(&client, &tbl).await;
}

/// REWRITE + DISTRIBUTED. The same divisor at `float8`, where the read path did not raise
/// at all.
///
/// `1 / 0` and `7.0 / 0` reach Arrow's integer and decimal kernels, which raise; IEEE 754
/// float division has no error to raise, so `1.0::float8 / 0` answered `inf`. That made the
/// divergence both silent *and* inconsistent inside one database — the type of a literal
/// decided whether a client was told about a bad divisor or handed a value that every
/// `sum`, `avg` and comparison above it carried up as if it were data.
///
/// So the coordinator rewrites every float division into a checked call that divides with
/// the same Arrow kernel and raises for the rows PostgreSQL raises for. This test is the
/// distributed half of that: the call is inserted by the coordinator's planner but resolved
/// by name on the core node running the stage, so a registration missing on either side
/// fails a query that was already accepted.
///
/// The three rows PostgreSQL does *not* raise for are asserted alongside, because a guard
/// that raised for them would trade a silent wrong answer for a loud one.
#[tokio::test]
async fn test_a_float_zero_divisor_raises_rather_than_answering_infinity() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_fdz",
        &format!(
            "(id INTEGER NOT NULL, v DOUBLE PRECISION, r REAL, z DOUBLE PRECISION NOT NULL) \
             {CREATE_OPTS}"
        ),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, v, r, z) VALUES \
             (1, 3.0, 3.0, 0.0), (2, 6.0, 6.0, 0.0), (3, NULL, NULL, 0.0), \
             (4, 'nan', 'nan', 0.0)"
        ),
    )
    .await
    .unwrap();

    for sql in [
        // The gap's own statement, both operands constant — folded at plan time, which is
        // also where PostgreSQL raises it.
        "SELECT 1.0::float8 / 0".to_string(),
        "SELECT 1.0::float8 / 0.0::float8".to_string(),
        // A column dividend over a column divisor: the row-by-row path, on a shard.
        format!("SELECT v / z FROM {tbl} WHERE id = 1"),
        // `real` is checked at its own width rather than widened into `double precision`.
        format!("SELECT r / 0 FROM {tbl} WHERE id = 1"),
        // A mixed float/integer division is a float division, so the integer zero is the
        // divisor of a checked call and not of Arrow's integer kernel.
        format!("SELECT v / (id - id) FROM {tbl} WHERE id = 1"),
        // The shape that made this Tier 1: an aggregate over a poisoned column, which
        // returned a single plausible `inf` with nothing in it to tell a client apart from
        // a real total.
        format!("SELECT sum(v / z) FROM {tbl} WHERE id <= 2"),
        format!("SELECT avg(v / z) OVER () FROM {tbl} WHERE id <= 2"),
        // And in a predicate, where the infinity used to decide which rows came back.
        format!("SELECT id FROM {tbl} WHERE v / z > 0"),
    ] {
        assert_sqlstate(&client, &sql, SQLSTATE_DIVISION_BY_ZERO).await;
    }

    // The carve-out: a NaN dividend over zero is NaN in PostgreSQL, not an error, because
    // the result is already the value that says "not a number".
    assert_eq!(
        scalar(&client, &format!("SELECT v / z FROM {tbl} WHERE id = 4")).await,
        "NaN"
    );

    // Division is strict, so a NULL dividend is NULL however bad the divisor is.
    let rows = simple_query_rows(&client, &format!("SELECT v / z FROM {tbl} WHERE id = 3"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], None);

    // Per row and not per statement: a zero divisor no row reaches raises nothing, which is
    // what makes an ordinary division over a real column cost nothing until it divides by
    // zero. Measured against PostgreSQL 17, where this returns no rows.
    let rows = simple_query_rows(&client, &format!("SELECT v / z FROM {tbl} WHERE id > 100"))
        .await
        .unwrap();
    assert!(rows.is_empty(), "no rows to divide, no error");

    // Nothing else moved. An ordinary float division answers what it answered before —
    // through the same rewritten call, on the same shards.
    assert_eq!(
        scalar_number(&client, &format!("SELECT v / 2 FROM {tbl} WHERE id = 2")).await,
        3.0
    );
    assert_eq!(
        scalar_number(
            &client,
            &format!("SELECT sum(v / 3) FROM {tbl} WHERE id <= 2")
        )
        .await,
        3.0
    );

    // And neither did the types a client reads off Describe. A rewrite that changed a
    // result OID would have traded a wrong value for a wrong type, which is the same class
    // of gap one row over: `real / real` is still `real`, and a float over an integer is
    // still `double precision`.
    assert_eq!(
        describe_result_types(&client, &format!("SELECT v / v FROM {tbl}")).await,
        vec![Type::FLOAT8]
    );
    assert_eq!(
        describe_result_types(&client, &format!("SELECT r / r FROM {tbl}")).await,
        vec![Type::FLOAT4]
    );
    assert_eq!(
        describe_result_types(&client, &format!("SELECT v / id FROM {tbl}")).await,
        vec![Type::FLOAT8]
    );

    drop_table(&client, &tbl).await;
}

/// REWRITE. A zero divisor on the **write path**. The read path above raises `22012`; a
/// shard answered **NULL** and the statement reported the rows it changed, so the client
/// was told a write succeeded that had stored a null where it asked for a number. It is
/// the one divergence the write path's own configuration created rather than inherited:
/// the `integer_division` setting that makes `7 / 2` store `3` is the same setting that
/// makes `7 / 0` store NULL, and DuckDB 1.5.5 has no second setting to separate them —
/// `7 / 0`, `7.0 / 0` and `7 % 0` are all NULL.
///
/// So the coordinator wraps every write-path division in a guard that raises, and the
/// core node classifies the guard's error back into `22012`. This test is the whole
/// round trip: the class a client reads, and the rows a client keeps.
#[tokio::test]
async fn test_a_zero_divisor_raises_on_the_write_path_too() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_wdz").await;

    for sql in [
        // An assignment: the NULL that used to be stored.
        format!("UPDATE {tbl} SET flag = id / 0"),
        format!("UPDATE {tbl} SET flag = id % 0"),
        // A float divisor divides by zero in PostgreSQL as well, and DuckDB answered
        // NULL for it on the same terms.
        format!("UPDATE {tbl} SET flag = CAST(id AS DOUBLE) / 0.0"),
        // A predicate, where the NULL used to make the comparison unknown and the
        // statement silently change nothing.
        format!("DELETE FROM {tbl} WHERE id / 0 = 1"),
        // A value row: the INSERT lane renders the same expression.
        format!("INSERT INTO {tbl} (id, name, flag) VALUES (6, 'zed', 1 / 0)"),
    ] {
        assert_sqlstate(&client, &sql, SQLSTATE_DIVISION_BY_ZERO).await;
    }

    // Nothing was written on the way to any of those errors: the flags are still 0, the
    // five rows are still five, and the INSERT's row is not there. A guard that raised
    // after storing would be no better than the NULL it replaced. Each divisor above is
    // zero for *every* row, so this holds shard by shard — a write is not atomic across
    // shards (gap analysis § 6.1.1), so a statement that raised on only one of them is a
    // separate question, asserted below.
    assert_eq!(
        ids_selected(&client, &tbl, "flag = 1").await,
        Vec::<i32>::new()
    );
    assert_eq!(
        ids_selected(&client, &tbl, "id > 0").await,
        vec![1, 2, 3, 4, 5]
    );

    // A divisor that is zero for one row only. The guard is evaluated per row rather than
    // folded at bind time, so reaching that row is what fails the statement — which is
    // also what makes an ordinary division over a real column cost nothing until it
    // divides by zero. What the shards holding the other rows did is not asserted: they
    // have no zero divisor and no reason to fail, and cross-shard atomicity is out of
    // scope for v0.2.
    assert_sqlstate(
        &client,
        &format!("UPDATE {tbl} SET flag = 1 / (id - 3)"),
        SQLSTATE_DIVISION_BY_ZERO,
    )
    .await;
    execute(&client, &format!("UPDATE {tbl} SET flag = 0"))
        .await
        .unwrap();

    // And an ordinary divisor is untouched by the guard, including the `integer_division`
    // truncation it wraps — `test_integer_division_truncates_on_both_paths` pins the
    // arithmetic; this pins that a *guarded* division still reaches a real column.
    let affected = execute(&client, &format!("UPDATE {tbl} SET flag = id / 2"))
        .await
        .unwrap();
    assert_eq!(affected, 5);
    assert_eq!(ids_selected(&client, &tbl, "flag = 1").await, vec![2, 3]);

    drop_table(&client, &tbl).await;
}

/// REFUSAL. The guard names the divisor twice, because DuckDB can only raise from an
/// expression through `error()` inside a `CASE` and has nowhere to bind the value once.
/// For an ordinary divisor that costs one extra evaluation; for a divisor that carries a
/// guard of its own it doubles that guard, and a right-nested chain doubles once per
/// level — a short statement that renders a very long one. That one shape is refused
/// rather than left as the silent NULL the guard exists to remove.
#[tokio::test]
async fn test_a_division_nested_in_a_divisor_is_refused_on_the_write_path() {
    let client = ready_client().await;
    let tbl = setup_pattern_table(&client, "expr_wdn").await;

    for sql in [
        format!("UPDATE {tbl} SET flag = id / (id / 2)"),
        format!("DELETE FROM {tbl} WHERE id / (1 + id % 2) = 1"),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("divisor is itself a division"),
            "the message names the shape, got: {}",
            err.message()
        );
    }

    // A division in the *dividend* is not that shape — only the divisor is duplicated —
    // so it is executed, and the read path agrees with it.
    execute(&client, &format!("UPDATE {tbl} SET flag = (id / 2) / 2"))
        .await
        .unwrap();
    assert_eq!(ids_selected(&client, &tbl, "flag = 1").await, vec![4, 5]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 14. The ordered-set aggregates answer exactly
// ============================================================================

/// Ten rows spread over every shard, so a percentile has to be gathered from all of
/// them rather than answered from one.
async fn setup_percentile_table(client: &Client, prefix: &str) -> String {
    let tbl = create_table(
        client,
        prefix,
        &format!("(id INTEGER NOT NULL, n INTEGER, f DOUBLE PRECISION) {CREATE_OPTS}"),
    )
    .await;
    // The ids are chosen per shard bucket; the values 1..10 are what the percentile is
    // taken over, and they deliberately do not follow the ids.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .flat_map(|b| ids_in_bucket(b, 4, 1))
        .take(10)
        .collect();
    assert_eq!(ids.len(), 10, "the percentile fixture needs ten rows");
    for (value, id) in (1..=10).zip(&ids) {
        execute(
            client,
            &format!("INSERT INTO {tbl} (id, n, f) VALUES ({id}, {value}, {value}.0)"),
        )
        .await
        .unwrap();
    }
    tbl
}

// DISTRIBUTED. `percentile_cont` is VaireDB's own aggregate, registered under the name
// DataFusion uses so it replaces DataFusion's — whose interpolation weight is floored to
// six decimals, answering 9.099999 for this exact query. The value is the contract, and
// it has to survive being computed as a partial pass per shard and merged on the
// coordinator, which is the reason this is an end-to-end test and not only a unit one.
#[tokio::test]
async fn test_percentile_cont_interpolates_exactly_across_shards() {
    let client = ready_client().await;
    let tbl = setup_percentile_table(&client, "expr_pctl_cont").await;

    for column in ["n", "f"] {
        assert_eq!(
            scalar_number(
                &client,
                &format!("SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY {column}) FROM {tbl}")
            )
            .await,
            9.1,
            "the 0.9 percentile of 1..10 over {column}"
        );
        assert_eq!(
            scalar_number(
                &client,
                &format!("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY {column}) FROM {tbl}")
            )
            .await,
            5.5
        );
        // Counted from the other end: the 0.9 percentile of a descending order.
        assert!(
            (scalar_number(
                &client,
                &format!(
                    "SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY {column} DESC) FROM {tbl}"
                )
            )
            .await
                - 1.9)
                .abs()
                < 1e-9
        );
    }

    // PostgreSQL answers `double precision` whatever it was given, including for the
    // integer column — the type a client's driver decodes with.
    assert_eq!(
        describe_result_types(
            &client,
            &format!("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FROM {tbl}")
        )
        .await,
        vec![Type::FLOAT8]
    );

    drop_table(&client, &tbl).await;
}

// DISTRIBUTED. `percentile_disc` DataFusion does not have at all. It returns one of the
// input values rather than a point between two, so its result keeps the ordered
// column's own type — `integer` here, not `double precision`.
#[tokio::test]
async fn test_percentile_disc_returns_an_input_value() {
    let client = ready_client().await;
    let tbl = setup_percentile_table(&client, "expr_pctl_disc").await;

    // ceil(fraction * 10) is the row, and never row 0.
    for (fraction, want) in [
        ("0.0", "1"),
        ("0.05", "1"),
        ("0.1", "1"),
        ("0.11", "2"),
        ("0.5", "5"),
        ("0.9", "9"),
        ("1.0", "10"),
    ] {
        assert_eq!(
            scalar(
                &client,
                &format!("SELECT percentile_disc({fraction}) WITHIN GROUP (ORDER BY n) FROM {tbl}")
            )
            .await,
            want,
            "percentile_disc({fraction}) over 1..10"
        );
    }

    assert_eq!(
        describe_result_types(
            &client,
            &format!("SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY n) FROM {tbl}")
        )
        .await,
        vec![Type::INT4]
    );

    drop_table(&client, &tbl).await;
}

// A fraction outside 0..1 is an error rather than a clamped answer, which is what
// PostgreSQL does — and an empty group is NULL, the way every aggregate answers one.
#[tokio::test]
async fn test_a_percentile_of_nothing_and_of_a_bad_fraction() {
    let client = ready_client().await;
    let tbl = setup_percentile_table(&client, "expr_pctl_edge").await;

    for sql in [
        format!("SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY n) FROM {tbl}"),
        format!("SELECT percentile_disc(-0.5) WITHIN GROUP (ORDER BY n) FROM {tbl}"),
    ] {
        // The `DbError`, not the `Error` wrapping it: a `tokio_postgres::Error` renders
        // as the bare word "db error" and keeps the server's message in its source, so
        // asserting on its `to_string()` would assert on nothing.
        let error = execute_expect_err(&client, &sql).await;
        assert!(
            error.message().contains("is not between 0 and 1"),
            "`{sql}` must say why: {}",
            error.message()
        );
        // A literal fraction is decided on the coordinator, so this is an argument
        // error and carries an argument error's SQLSTATE — not a failed job's.
        assert_eq!(
            error.code().code(),
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "`{sql}`: {}",
            error.message()
        );
    }

    for function in ["percentile_cont", "percentile_disc"] {
        let rows = simple_query_rows(
            &client,
            &format!("SELECT {function}(0.5) WITHIN GROUP (ORDER BY n) FROM {tbl} WHERE n > 1000"),
        )
        .await
        .unwrap_or_else(|e| panic!("`{function}` over an empty group must answer: {e}"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], None, "`{function}` over an empty group is NULL");
    }

    drop_table(&client, &tbl).await;
}

// DISTRIBUTED. A `pg_catalog` scalar function applied to a **column** rather than to a
// literal, which is the case that has to cross the wire: every one of these declares
// `Immutable` or `Stable` volatility, so a call whose arguments are all literals is
// folded into a constant before the plan is ever serialized. That is why
// `SELECT format_type(23, NULL)` always worked while `\gdesc` did not — psql's `\gdesc`
// falls back to a `VALUES` list plus `pg_catalog.format_type`, and the plan carrying it
// could be built and then not be decoded by the scheduler that had never heard the name.
//
// A UDF crosses the wire as a name and nothing else, so the fix is that the name resolves
// on all three registries, not a codec arm: see `vairedb_common::pg_udf`.
#[tokio::test]
async fn test_a_pg_catalog_function_over_a_column_survives_distribution() {
    let client = ready_client().await;

    // The `\gdesc` shape itself: a VALUES list, so there is no table to route by and the
    // whole statement is planned for distribution.
    let rows = simple_query_rows(
        &client,
        "SELECT format_type(oid, NULL) FROM (VALUES (23),(25)) t(oid) ORDER BY 1",
    )
    .await
    .unwrap_or_else(|e| panic!("`format_type` over a column must answer: {e}"));
    assert_eq!(
        rows.iter().map(|r| r[0].as_deref()).collect::<Vec<_>>(),
        vec![Some("integer"), Some("text")]
    );

    // And the same over a sharded table, where the projection really does run on a core
    // node rather than on the coordinator.
    let tbl = create_table(
        &client,
        "expr_fmt_type",
        &format!("(id INTEGER NOT NULL, name VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    execute(&client, &format!("INSERT INTO {tbl} VALUES (23, 'a b')"))
        .await
        .unwrap();

    assert_eq!(
        scalar(&client, &format!("SELECT format_type(id, NULL) FROM {tbl}")).await,
        "integer"
    );
    assert_eq!(
        scalar(&client, &format!("SELECT quote_ident(name) FROM {tbl}")).await,
        "\"a b\""
    );

    // The residue, pinned so the gap analysis's claim stays honest: what closed is the
    // serialization of the plan `\gdesc` builds, not `format_type`'s argument coercion. The
    // signature is `OneOf(Exact(Int32, Int32), … Exact(Int64, Int64))` with no `(Utf8, …)`
    // arm, so an untyped literal OID is refused where PostgreSQL coerces it to `oid`.
    // See `gap-analysis.md` § 4 items 18 and 23.
    assert_unsupported(&client, "SELECT format_type('23', 0)").await;

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 15. Literals and subscripts read the way PostgreSQL reads them
// ============================================================================
//
// Four forms where DataFusion and DuckDB agree with each other and not with PostgreSQL
// (§ 2.7 rows 2–5 of the gap analysis). Agreeing with each other is what made them
// dangerous: the split-brain probes above compare the two paths against each other, and
// these four answered the same wrong thing on both, so nothing flagged them. Each test
// therefore writes out PostgreSQL 17's answer, measured, and asserts it on the read path
// and on the write path separately.
//
// The read path's `::bytea` is rewritten to a UDF (`vaire_bytea_in`) that a *core node*
// runs, so these also cover the wire registration: a name only the coordinator knows
// plans fine and then fails on the executor.

/// The single scalar a `SELECT <expr>` returns, or `None` where it returned NULL — which
/// for the subscript rows is the answer itself, and is what [`scalar`] panics on.
async fn maybe_scalar(client: &Client, sql: &str) -> Option<String> {
    let rows = simple_query_rows(client, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"));
    assert_eq!(rows.len(), 1, "`{sql}` returns one row");
    rows[0][0].clone()
}

/// `0b101` is 5, `0o17` is 15 and `0x1F` is 31 — PostgreSQL 16's non-decimal integer
/// literals, which sqlparser has neither form of.
///
/// It did not fail to parse them, which is the whole problem. `0b101` tokenized as `0`
/// aliased `b101`, so the client got **`0`** under a column name it never wrote, and
/// `0x1F` tokenized as SQL's byte-string syntax, so a number came back as the *bytes*
/// `1f`. Both paths start from the same tokenizer, so both were wrong the same way and no
/// comparison between them could show it.
#[tokio::test]
async fn test_a_non_decimal_integer_literal_is_the_number_it_spells() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_radix",
        &format!("(id INTEGER NOT NULL, n BIGINT) {CREATE_OPTS}"),
    )
    .await;

    for (sql, want) in [
        ("SELECT 0b101", 5.0),
        ("SELECT 0o17", 15.0),
        ("SELECT 0x1F", 31.0),
        // Every prefix in both letter cases: only lowercase `0x` is a form the tokenizer
        // has an opinion about, and the other five arrive as a number beside a word.
        ("SELECT 0B101", 5.0),
        ("SELECT 0O17", 15.0),
        ("SELECT 0X1f", 31.0),
        // `_` groups digits, and the literal is an operand like any other.
        ("SELECT 0b1_0000_0000", 256.0),
        ("SELECT 0x1F + 1", 32.0),
        ("SELECT -0x10", -16.0),
    ] {
        assert_eq!(scalar_number(&client, sql).await, want, "`{sql}`");
    }

    // Typed exactly as the decimal spelling is: `bigint`'s largest value is a value, not an
    // overflow, so the conversion cannot be what decides a width.
    assert_eq!(
        scalar(&client, "SELECT 0x7fffffffffffffff").await,
        "9223372036854775807"
    );

    // The write path, where the same literal used to reach a shard as `X'1F'` and fail
    // there with `42804`: a VALUES row, an assignment, a predicate, and the shard key.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, n) VALUES (0b1, 0x1F)"),
    )
    .await
    .unwrap();
    assert_eq!(
        scalar_number(&client, &format!("SELECT n FROM {tbl} WHERE id = 1")).await,
        31.0
    );
    execute(&client, &format!("UPDATE {tbl} SET n = 0o17 WHERE id = 0x1"))
        .await
        .unwrap();
    assert_eq!(
        scalar_number(&client, &format!("SELECT n FROM {tbl} WHERE id = 1")).await,
        15.0
    );
    assert_eq!(
        execute(&client, &format!("DELETE FROM {tbl} WHERE n = 0b1111"))
            .await
            .unwrap(),
        1,
        "the DELETE predicate reads 0b1111 as 15"
    );

    // Over-reach, in three directions. Text that only looks like a literal is text; SQL's
    // own byte-string syntax shares a token with `0x…` and still means bytes (PostgreSQL
    // reads `x'1F'` as a bit string — a different row, in § 2.2); and a malformed literal is
    // refused rather than turned into some other number.
    assert_eq!(scalar(&client, "SELECT '0x1F'").await, "0x1F");
    assert_ne!(scalar(&client, "SELECT x'1F'").await, "31");
    for sql in [
        "SELECT 0b102",
        "SELECT 0o18",
        "SELECT 0x",
        "SELECT 0x1f_",
        "SELECT 0x1f.5",
    ] {
        assert_sqlstate(&client, sql, SQLSTATE_SYNTAX_ERROR).await;
    }
    // And the decimal digits a client actually sends are not rewritten at all.
    assert_eq!(scalar_number(&client, "SELECT 10").await, 10.0);
    assert_eq!(scalar_number(&client, "SELECT 0.5").await, 0.5);

    drop_table(&client, &tbl).await;
}

/// A subscript outside the array is NULL for an element and empty for a slice.
///
/// Both engines borrowed Python's rule instead — a negative index counts back from the
/// end — so `tags[-1]` read the **last element** where PostgreSQL reads nothing, and a
/// negative slice invented or dropped rows: `tags[-3:-1]` was the whole array and
/// `tags[-1:2]` was empty. The slice half is the worse one, because a wrong element is one
/// value and a wrong slice changes how many values there are.
#[tokio::test]
async fn test_an_out_of_range_subscript_is_null_and_an_empty_slice() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_subscript",
        &format!(
            "(id INTEGER NOT NULL, tags INTEGER[], v INTEGER, part INTEGER[]) {CREATE_OPTS}"
        ),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, tags) VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[10, 20, 30])"),
    )
    .await
    .unwrap();

    // An element, over a literal array and over a column.
    for subscript in ["-1", "0", "4"] {
        assert_eq!(
            maybe_scalar(&client, &format!("SELECT (ARRAY[10, 20, 30])[{subscript}]")).await,
            None,
            "(ARRAY[10, 20, 30])[{subscript}] is NULL in PostgreSQL"
        );
        assert_eq!(
            maybe_scalar(
                &client,
                &format!("SELECT tags[{subscript}] FROM {tbl} WHERE id = 1")
            )
            .await,
            None,
            "tags[{subscript}] is NULL in PostgreSQL"
        );
    }
    // The in-range index still reads its element: the clamp is a clamp, not a refusal.
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT tags[1] FROM {tbl} WHERE id = 1")).await,
        Some("10".to_string())
    );
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT tags[3] FROM {tbl} WHERE id = 1")).await,
        Some("30".to_string())
    );

    // A slice: a low lower bound is the start of the array, and an upper bound before the
    // start selects nothing.
    for (slice, want) in [
        ("2:3", "{20,30}"),
        ("-1:2", "{10,20}"),
        ("0:2", "{10,20}"),
        ("2:9", "{20,30}"),
        ("-3:-1", "{}"),
    ] {
        assert_eq!(
            maybe_scalar(&client, &format!("SELECT (ARRAY[10, 20, 30])[{slice}]")).await,
            Some(want.to_string()),
            "(ARRAY[10, 20, 30])[{slice}]"
        );
    }

    // The write path reads the same subscript the same way. `tags[-1]` in an assignment
    // stored `30`, and a predicate on it matched no rows where PostgreSQL matches every
    // row — a row count the client had no way to question.
    execute(
        &client,
        &format!("UPDATE {tbl} SET v = tags[-1], part = tags[-3:-1] WHERE id = 1"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("UPDATE {tbl} SET v = tags[1], part = tags[2:3] WHERE id = 2"),
    )
    .await
    .unwrap();
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT v FROM {tbl} WHERE id = 1")).await,
        None,
        "an out-of-range element stores NULL, not the last element"
    );
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT part FROM {tbl} WHERE id = 1")).await,
        Some("{}".to_string()),
        "an out-of-range slice stores an empty array, not the whole one"
    );
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT v FROM {tbl} WHERE id = 2")).await,
        Some("10".to_string())
    );
    assert_eq!(
        maybe_scalar(&client, &format!("SELECT part FROM {tbl} WHERE id = 2")).await,
        Some("{20,30}".to_string())
    );
    assert_eq!(
        execute(&client, &format!("DELETE FROM {tbl} WHERE tags[-1] IS NULL"))
            .await
            .unwrap(),
        2,
        "every row's tags[-1] is NULL, so the DELETE removes both"
    );

    drop_table(&client, &tbl).await;
}

/// `'\xDEADBEEF'::bytea` is four bytes.
///
/// PostgreSQL reads the text through its own `bytea` input conversion, which has two
/// formats: hex after a `\x` prefix, and backslash escapes otherwise. Neither engine did
/// — DataFusion cast `Utf8`→`Binary` bytewise and stored the **10 ASCII characters** of
/// the literal, and DuckDB's own `VARCHAR`→`BLOB` cast read `\xDE` as one byte and the
/// remaining `ADBEEF` as six characters, for seven. Three different answers to one cast,
/// none of them PostgreSQL's.
#[tokio::test]
async fn test_a_bytea_literal_is_decoded_the_way_postgres_decodes_it() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_bytea",
        &format!("(id INTEGER NOT NULL, a BYTEA, s VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // The read path, in binary format so the bytes are the assertion rather than a
    // rendering of them. The UDF runs on a core node, so this covers the registration on
    // both sides of the wire too.
    for (literal, want) in [
        ("'\\xDEADBEEF'", vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ("'\\xdeadbeef'", vec![0xDE, 0xAD, 0xBE, 0xEF]),
        // Whitespace between pairs is skipped, as PostgreSQL's decoder does.
        ("'\\xDE AD BE EF'", vec![0xDE, 0xAD, 0xBE, 0xEF]),
        // No `\x` prefix is the escape format: `\\` is one backslash and `\NNN` is octal.
        ("'a\\101b'", b"aAb".to_vec()),
        ("'a\\\\b'", b"a\\b".to_vec()),
        ("'\\000'", vec![0]),
        ("'abc'", b"abc".to_vec()),
        ("''", vec![]),
    ] {
        let sql = format!("SELECT {literal}::bytea");
        let rows = client
            .query(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"));
        let got: Vec<u8> = rows[0].get(0);
        assert_eq!(got, want, "`{sql}`");
        // And it is a `bytea` to a driver, not a string that happens to hold hex.
        assert_eq!(describe_result_types(&client, &sql).await, vec![Type::BYTEA]);
    }

    // `CAST(… AS BYTEA)` is the same cast written the other way.
    let rows = client
        .query("SELECT CAST('\\x41' AS BYTEA)", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, Vec<u8>>(0), vec![0x41]);

    // The write path stores the same four bytes — where a shard, handed the literal
    // verbatim, would have stored seven.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, a) VALUES (1, '\\xDEADBEEF'::bytea)"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("UPDATE {tbl} SET a = 'a\\101b'::bytea WHERE id = 1"),
    )
    .await
    .unwrap();
    let rows = client
        .query(&format!("SELECT a FROM {tbl} WHERE id = $1"), &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, Vec<u8>>(0), b"aAb".to_vec());

    // Over-reach: the forms the write path must keep accepting. A NULL cast, a parameter
    // bound as bytea, and a plain parameter into a declared BYTEA column are all left
    // alone — only a *string literal* is translated, and only a non-literal is refused.
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, a) VALUES (2, NULL::bytea)"),
    )
    .await
    .unwrap();
    client
        .execute(
            &format!("INSERT INTO {tbl} (id, a) VALUES ($1, $2)"),
            &[&3i32, &vec![0xDEu8, 0xAD]],
        )
        .await
        .expect("a bytea parameter is not a cast and must still be written");
    let rows = client
        .query(&format!("SELECT a FROM {tbl} WHERE id = $1"), &[&3i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, Vec<u8>>(0), vec![0xDE, 0xAD]);
    // And every other cast is still every other cast: only `bytea` is rewritten.
    assert_eq!(scalar(&client, "SELECT '41'::INTEGER").await, "41");
    execute(
        &client,
        &format!("UPDATE {tbl} SET s = 41::VARCHAR WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(&client, &format!("SELECT s FROM {tbl} WHERE id = 1")).await,
        "41"
    );

    drop_table(&client, &tbl).await;
}

/// A `bytea` literal that does not decode is refused with PostgreSQL's message *and*
/// PostgreSQL's SQLSTATE — and the same pair on both paths.
///
/// Two codes, because PostgreSQL raises two: `byteain` reports `22P02` for an escape it
/// cannot read and the hex decoder it calls reports `22023` for a bad hex body. Measured,
/// not inferred. The read path raises inside an executor, where the error is serialized to
/// text and loses its type, so keeping the code takes a named exception in the
/// coordinator's classifier — without it these arrive as `XX000`, which tells a driver the
/// server broke and invites a retry that cannot succeed.
#[tokio::test]
async fn test_a_bytea_literal_that_does_not_decode_is_refused() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_bytea_bad",
        &format!("(id INTEGER NOT NULL, a BYTEA) {CREATE_OPTS}"),
    )
    .await;

    // `message` is PostgreSQL's wording as the write path reports it, at parse time and
    // unescaped; `wording` is the part of it that survives being rendered into a
    // scheduler's error string, which is the only form the read path's failure arrives in.
    for (literal, sqlstate, message, wording) in [
        (
            "'\\xzz'",
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "invalid hexadecimal digit: \"z\"",
            "invalid hexadecimal digit",
        ),
        (
            "'\\xdeadbee'",
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "invalid hexadecimal data: odd number of digits",
            "invalid hexadecimal data: odd number of digits",
        ),
        (
            "'a\\1'",
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for type bytea",
            "invalid input syntax for type bytea",
        ),
        (
            "'a\\400'",
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for type bytea",
            "invalid input syntax for type bytea",
        ),
    ] {
        let read = format!("SELECT {literal}::bytea");
        let err = assert_sqlstate(&client, &read, sqlstate).await;
        assert!(
            err.message().contains(wording),
            "`{read}` should say {wording:?}: {}",
            err.message()
        );

        let write = format!("INSERT INTO {tbl} (id, a) VALUES (1, {literal}::bytea)");
        let err = assert_sqlstate(&client, &write, sqlstate).await;
        assert!(
            err.message().contains(message),
            "`{write}` should say {message:?}: {}",
            err.message()
        );
    }

    // The refusals are refusals, not partial writes.
    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}

/// REFUSAL. A `::bytea` the write path cannot decode itself is refused by name.
///
/// A write is executed verbatim by a shard, and only a *literal* can be decoded on the
/// coordinator. `s::bytea` over a column would reach DuckDB as its own `VARCHAR`→`BLOB`
/// cast, which reads a different escape syntax and stores different bytes — silently, and
/// per row, so no single value in the statement could be inspected to see it. Refusing is
/// the honest answer, and the read path answers the same expression, which is why the
/// message names the two ways to write it instead.
#[tokio::test]
async fn test_a_bytea_cast_the_write_path_cannot_decode_is_refused() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_bytea_rej",
        &format!("(id INTEGER NOT NULL, a BYTEA, s VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, s) VALUES (1, '\\x41')"),
    )
    .await
    .unwrap();

    for sql in [
        format!("UPDATE {tbl} SET a = s::bytea"),
        format!("UPDATE {tbl} SET a = CAST(s AS BYTEA) WHERE id = 1"),
        format!("UPDATE {tbl} SET a = (s || 'x')::bytea"),
        format!("INSERT INTO {tbl} (id, a) SELECT 2, s::bytea FROM {tbl}"),
        format!("DELETE FROM {tbl} WHERE a = s::bytea"),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("CAST to BYTEA"),
            "`{sql}` should name the cast it refuses: {}",
            err.message()
        );
    }

    // Over-reach, probed in the same test: the read path answers the expression that the
    // write path refuses, the same cast over a *literal* is written, and nothing above
    // changed a row on its way to being refused.
    assert_eq!(
        client
            .query(&format!("SELECT s::bytea FROM {tbl}"), &[])
            .await
            .expect("the read path decodes a column, so it must still answer")[0]
            .get::<_, Vec<u8>>(0),
        vec![0x41]
    );
    execute(
        &client,
        &format!("UPDATE {tbl} SET a = '\\x41'::bytea WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

/// A subscript and a `::bytea` are labelled the way PostgreSQL labels them.
///
/// PostgreSQL names an unaliased column after the thing it read — the array for a
/// subscript, the column for a cast — and both rewrites above put a function call where
/// that name came from, so without this the client would have received `vaire_bytea_in(s)`
/// or `t.a[nullif(greatest(1, 0), 0)]` as a column name. A driver reads columns by name,
/// so a rewrite that changes one is a wrong answer to a question the client did ask.
#[tokio::test]
async fn test_a_subscript_and_a_bytea_cast_keep_postgresqls_column_label() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_sub_label",
        &format!("(id INTEGER NOT NULL, tags INTEGER[], s VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    for (projection, want) in [
        ("tags[1]", "tags"),
        ("tags[1:2]", "tags"),
        ("s::bytea", "s"),
        ("CAST(s AS BYTEA)", "s"),
        ("'\\x41'::bytea", "bytea"),
        ("(ARRAY[1, 2, 3])[1]", "array"),
    ] {
        let sql = format!("SELECT {projection} FROM {tbl}");
        assert_eq!(
            describe_result_labels(&client, &sql).await,
            vec![want.to_string()],
            "`{sql}`"
        );
    }

    // An explicit alias wins, as always.
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT tags[1] AS first FROM {tbl}")).await,
        vec!["first".to_string()]
    );

    drop_table(&client, &tbl).await;
}

/// GAP. `length()` and `octet_length()` over `bytea` count bytes in PostgreSQL.
///
/// Found while closing the cast above, and pre-existing: neither function has a `Binary`
/// arm in DataFusion, so both refuse the argument — `octet_length` by name, and `length`
/// worse than that, by trying to coerce the bytes to `Utf8` and failing inside the
/// optimizer on any value that is not valid UTF-8. A refusal is not a wrong answer, so this
/// is not one of § 2.7's rows; it is recorded here because `length(a)` is how a client asks
/// a `bytea` column its size.
#[tokio::test]
#[ignore = "gap: length()/octet_length() have no Binary argument, so bytea sizes are unaskable"]
async fn test_the_length_of_a_bytea_is_its_byte_count() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "expr_bytea_len",
        &format!("(id INTEGER NOT NULL, a BYTEA) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, a) VALUES (1, '\\xDEADBEEF'::bytea)"),
    )
    .await
    .unwrap();

    assert_eq!(
        scalar_number(&client, &format!("SELECT length(a) FROM {tbl}")).await,
        4.0
    );
    assert_eq!(
        scalar_number(&client, &format!("SELECT octet_length(a) FROM {tbl}")).await,
        4.0
    );
    assert_eq!(
        scalar_number(&client, "SELECT length('\\xDEADBEEF'::bytea)").await,
        4.0
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 16. nth_value refuses the offset PostgreSQL refuses
// ============================================================================

// REFUSAL, DISTRIBUTED. `nth_value(x, 0)` — the narrowest row of § 2.7, and the one whose
// wrong answer is hardest to see: DataFusion finds that `0` names no row of the frame and
// returns NULL, for every row, in every partition. That is the same answer
// `nth_value(x, 4)` gives over a three-row frame, so a client cannot tell "you asked for
// nothing" from "there was nothing there". PostgreSQL raises `22016` before evaluating.
//
// Distributed because the guard runs inside the executor that evaluates the window, per
// partition — which is what keeps an empty input answering no rows instead of raising —
// so the SQLSTATE has to survive the trip back through the scheduler as text.
#[tokio::test]
async fn test_a_zero_nth_value_offset_raises_rather_than_answering_null() {
    let client = ready_client().await;
    let tbl = setup_window_table(&client, "expr_nth").await;

    // The gap. Both the partitioned and the whole-table window raise, and the message
    // names the function so a client reading it knows which argument to change.
    for sql in [
        format!("SELECT nth_value(n, 0) OVER (PARTITION BY g ORDER BY id) FROM {tbl}"),
        format!("SELECT nth_value(id, 0) OVER () FROM {tbl}"),
    ] {
        let err = assert_sqlstate(&client, &sql, SQLSTATE_INVALID_ARGUMENT_FOR_NTH_VALUE).await;
        assert!(
            err.message().contains("nth_value"),
            "the refusal names the function: {}",
            err.message()
        );
    }

    // A zero offset written at a width the planner reads as something other than a plain
    // integer literal reaches the same guard.
    assert_sqlstate(
        &client,
        &format!("SELECT nth_value(id, 0::bigint) OVER () FROM {tbl}"),
        SQLSTATE_INVALID_ARGUMENT_FOR_NTH_VALUE,
    )
    .await;

    // The over-reach probe, and the reason the guard is on the value and not on the
    // function: every offset PostgreSQL accepts still answers exactly what it answered
    // before. Over the whole partition ordered by id, group 1 is ids 1..3 and group 2 is
    // ids 4..6, so the first is 1 and 4 and the second is 2 and 5.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT g, \
             nth_value(id, 1) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), \
             nth_value(id, 2) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM {tbl} ORDER BY g, id"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("a positive offset should be answerable: {e}"));
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].as_deref(), r[1].as_deref(), r[2].as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (Some("1"), Some("1"), Some("2")),
            (Some("1"), Some("1"), Some("2")),
            (Some("1"), Some("1"), Some("2")),
            (Some("2"), Some("4"), Some("5")),
            (Some("2"), Some("4"), Some("5")),
            (Some("2"), Some("4"), Some("5")),
        ]
    );

    // An offset past the end of the frame is PostgreSQL's NULL, and stays one — the answer
    // a zero offset used to be indistinguishable from.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT nth_value(id, 4) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM {tbl} ORDER BY g, id LIMIT 1"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("an offset past the frame should be answerable: {e}"));
    assert_eq!(rows[0][0], None, "a 4th row of a 3-row frame is NULL");

    // The deliberate superset of § 5: a negative offset counts from the end of the frame.
    // PostgreSQL raises `22016` for this too, and VaireDB answers it on purpose — which is
    // why the guard fires on exactly zero and not on every offset below one.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT g, nth_value(id, -1) OVER (PARTITION BY g ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM {tbl} ORDER BY g, id LIMIT 4"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("a negative offset is a superset and must keep working: {e}"));
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].as_deref(), r[1].as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (Some("1"), Some("3")),
            (Some("1"), Some("3")),
            (Some("1"), Some("3")),
            (Some("2"), Some("6")),
        ]
    );

    // PostgreSQL evaluates the offset per partition, so a statement whose input has no
    // rows has nothing to raise about: no rows, no error. This is the reason the guard is
    // a window function and not a check on the plan — a planning-time refusal would raise
    // here, where PostgreSQL returns an empty result.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT nth_value(n, 0) OVER (PARTITION BY g) FROM {tbl} WHERE false"),
    )
    .await
    .unwrap_or_else(|e| panic!("an empty input raises nothing in PostgreSQL: {e}"));
    assert!(rows.is_empty(), "no rows, and no error");

    // `first_value` and `last_value` share DataFusion's implementation with `nth_value`
    // and take no offset, so neither is touched by the guard.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT first_value(id) OVER w, last_value(id) OVER w FROM {tbl} \
             WINDOW w AS (PARTITION BY g ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             ORDER BY g, id LIMIT 1"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("first_value/last_value should be answerable: {e}"));
    assert_eq!(
        (rows[0][0].as_deref(), rows[0][1].as_deref()),
        (Some("1"), Some("3"))
    );

    drop_table(&client, &tbl).await;
}
