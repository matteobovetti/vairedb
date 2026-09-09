mod common;
use common::*;
use tokio_postgres::Client;
use tokio_postgres::types::Type;

// The read path's join and set-operation surface: the executable counterpart of
// `docs/specs/gap-analysis-join.md`.
//
// This axis exists because a join is the one read whose answer depends on how the data is
// laid out. Every table here is created with `CREATE_OPTS` — three shards, hash-sharded on
// `id` — so a join on `id` is **co-located** (each shard can be joined where it lives) and
// a join on any other column is a **shuffle** (rows have to be repartitioned across the
// cluster before they can meet). Both have to give PostgreSQL's answer, and the two are
// different code paths, so each is measured.
//
// Every assertion is the answer PostgreSQL gives. Where VaireDB does not give it, the row
// carries two tests, as in `sql_command_unsupported.rs`: a **passing** one pinning today's
// behaviour, so a regression is visible, and an `#[ignore = "gap (row N): …"]` one
// asserting PostgreSQL's answer, which is the definition of done. `make e2e` therefore
// stays green while the gaps stay open:
//
//     cd tests/e2e && cargo test --test sql_join_gaps -- --ignored --test-threads=1
//
// Three seams recur, and each test says which one it holds:
//
//   * a DISTRIBUTED case — the same query would be answered by DataFusion in one process,
//     and here it is cut into stages and shipped, so a plan that cannot be serialized or
//     an operator whose semantics differ once repartitioned shows up only here;
//   * a THREE-VALUED case — the answer turns on a NULL, which is where a join operator and
//     PostgreSQL are most likely to disagree without either of them erroring;
//   * a REFUSAL — VaireDB will not answer, and what is pinned is that it says so with
//     `0A000` rather than answering a different question.

// ============================================================================
// Fixtures
// ============================================================================

/// `l(id, k, v)` and `r(id, k, w)`, the pair almost every test here joins.
///
/// The values are chosen so each expected answer is derivable by hand rather than looked
/// up: the two `id` sets overlap on `2, 3`, so an `id` join returns **two** rows; the two
/// `k` sets overlap only on `20`, so a `k` join returns **one**; and `l` holds a row whose
/// key is NULL, which is what every three-valued assertion turns on.
///
/// ```text
///   l: (1, 10, 'a')  (2, 20, 'b')  (3, 30, 'c')  (4, NULL, 'd')
///   r: (2, 20, 'x')  (3, 99, 'y')  (5, 50, 'z')
/// ```
async fn setup_pair(client: &Client, prefix: &str) -> (String, String) {
    let l = create_table(
        client,
        &format!("{prefix}_l"),
        &format!("(id INTEGER NOT NULL, k INTEGER, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let r = create_table(
        client,
        &format!("{prefix}_r"),
        &format!("(id INTEGER NOT NULL, k INTEGER, w VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!(
            "INSERT INTO {l} (id, k, v) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, NULL, 'd')"
        ),
    )
    .await
    .unwrap();
    execute(
        client,
        &format!("INSERT INTO {r} (id, k, w) VALUES (2, 20, 'x'), (3, 99, 'y'), (5, 50, 'z')"),
    )
    .await
    .unwrap();
    (l, r)
}

/// A third table for the three-way joins: `m(id, k, z)` = `(2, 20, 'b'), (5, 50, 'q')`.
///
/// `z = 'b'` on the row whose `id` is 2 is deliberate: it makes `l.v = m.z` a join on a
/// **text** key that returns exactly one row, so the text case is not vacuous.
async fn setup_third(client: &Client, prefix: &str) -> String {
    let m = create_table(
        client,
        &format!("{prefix}_m"),
        &format!("(id INTEGER NOT NULL, k INTEGER, z VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!("INSERT INTO {m} (id, k, z) VALUES (2, 20, 'b'), (5, 50, 'q')"),
    )
    .await
    .unwrap();
    m
}

/// `big(id, k BIGINT, d DOUBLE PRECISION)` = `(2, 20, 2.5), (7, 5000000000, 7.5)`.
///
/// One row's `k` matches `l.k` while fitting in `int4`, so a cross-type join key returns a
/// row; the other's does not fit, so a `UNION` with an `int4` branch has to widen or fail
/// rather than silently truncate.
async fn setup_wide(client: &Client, prefix: &str) -> String {
    let big = create_table(
        client,
        &format!("{prefix}_big"),
        &format!("(id INTEGER NOT NULL, k BIGINT, d DOUBLE PRECISION) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!("INSERT INTO {big} (id, k, d) VALUES (2, 20, 2.5), (7, 5000000000, 7.5)"),
    )
    .await
    .unwrap();
    big
}

/// A pair holding duplicates, which is the only fixture that can tell `INTERSECT` from
/// `INTERSECT ALL`: `dl.k = 1, 1, 1, 2` and `dr.k = 1, 1, 3`.
async fn setup_dupes(client: &Client, prefix: &str) -> (String, String) {
    let dl = create_table(
        client,
        &format!("{prefix}_dl"),
        &format!("(id INTEGER NOT NULL, k INTEGER) {CREATE_OPTS}"),
    )
    .await;
    let dr = create_table(
        client,
        &format!("{prefix}_dr"),
        &format!("(id INTEGER NOT NULL, k INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        client,
        &format!("INSERT INTO {dl} (id, k) VALUES (1, 1), (2, 1), (3, 1), (4, 2)"),
    )
    .await
    .unwrap();
    execute(
        client,
        &format!("INSERT INTO {dr} (id, k) VALUES (1, 1), (2, 1), (3, 3)"),
    )
    .await
    .unwrap();
    (dl, dr)
}

// ============================================================================
// Readers
// ============================================================================

/// The first column of every row `sql` answers, in the order the server sent them, with a
/// NULL rendered as the four characters `NULL` so an assertion can name one.
async fn column(client: &Client, sql: &str) -> Vec<String> {
    let rows = simple_query_rows(client, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"));
    rows.iter()
        .map(|row| row[0].clone().unwrap_or_else(|| "NULL".to_string()))
        .collect()
}

/// The first two columns of every row, rendered the same way.
async fn pairs(client: &Client, sql: &str) -> Vec<(String, String)> {
    let rows = simple_query_rows(client, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should be answerable: {e}"));
    rows.iter()
        .map(|row| {
            (
                row[0].clone().unwrap_or_else(|| "NULL".to_string()),
                row[1].clone().unwrap_or_else(|| "NULL".to_string()),
            )
        })
        .collect()
}

/// The single integer a `COUNT`-shaped query answers.
async fn count(client: &Client, sql: &str) -> i64 {
    let values = column(client, sql).await;
    assert_eq!(values.len(), 1, "`{sql}` answers one row");
    values[0]
        .parse()
        .unwrap_or_else(|e| panic!("`{sql}` answered {:?}, not a count: {e}", values[0]))
}

/// The same column, sorted as text, for a query whose row *set* is the contract and whose
/// row order is not — a set operation without an `ORDER BY`, for instance.
async fn sorted_column(client: &Client, sql: &str) -> Vec<String> {
    let mut values = column(client, sql).await;
    values.sort();
    values
}

// ============================================================================
// 1. Join types
// ============================================================================

// DISTRIBUTED. The five join types over the shard key, which is the co-located case: every
// shard holds the rows of both tables whose `id` hashes to it, so the join needs no
// reshuffle. What is pinned is that the outer joins produce the padding rows — a join that
// silently degraded to an inner join would pass an `INNER JOIN` test and fail these.
#[tokio::test]
async fn test_join_types_over_the_shard_key() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_types").await;

    // Inner: only the two ids both tables hold.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.w FROM {l} l JOIN {r} r ON l.id = r.id ORDER BY l.id")
        )
        .await,
        vec![("2".into(), "x".into()), ("3".into(), "y".into())]
    );

    // Left: every `l` row, padded where `r` has none.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.w FROM {l} l LEFT JOIN {r} r ON l.id = r.id ORDER BY l.id")
        )
        .await,
        vec![
            ("1".into(), "NULL".into()),
            ("2".into(), "x".into()),
            ("3".into(), "y".into()),
            ("4".into(), "NULL".into()),
        ]
    );

    // Right: every `r` row, padded where `l` has none.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT r.id, l.v FROM {l} l RIGHT JOIN {r} r ON l.id = r.id ORDER BY r.id")
        )
        .await,
        vec![
            ("2".into(), "b".into()),
            ("3".into(), "c".into()),
            ("5".into(), "NULL".into()),
        ]
    );

    // Full: the union of both, five rows for ids 1..5.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT COALESCE(l.id, r.id) AS id FROM {l} l FULL JOIN {r} r ON l.id = r.id \
                 ORDER BY id"
            )
        )
        .await,
        vec!["1", "2", "3", "4", "5"]
    );

    // The `OUTER` keyword is noise in all three spellings, as in PostgreSQL.
    for outer in ["LEFT OUTER", "RIGHT OUTER", "FULL OUTER"] {
        let sql = format!("SELECT COUNT(*) FROM {l} l {outer} JOIN {r} r ON l.id = r.id");
        let rows = count(&client, &sql).await;
        assert!(rows >= 3, "`{sql}` answered {rows}");
    }

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// DISTRIBUTED. The same joins over a column that is **not** the shard key, so the rows have
// to be repartitioned before they can meet. `l.k` and `r.k` overlap only on 20.
#[tokio::test]
async fn test_join_over_a_non_shard_key_shuffles() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_shuffle").await;

    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.id FROM {l} l JOIN {r} r ON l.k = r.k ORDER BY l.id")
        )
        .await,
        vec![("2".into(), "2".into())]
    );

    // THREE-VALUED. The NULL-keyed `l` row joins nothing — `NULL = NULL` is not true — but
    // it survives a left join, padded.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.id FROM {l} l LEFT JOIN {r} r ON l.k = r.k ORDER BY l.id")
        )
        .await,
        vec![
            ("1".into(), "NULL".into()),
            ("2".into(), "2".into()),
            ("3".into(), "NULL".into()),
            ("4".into(), "NULL".into()),
        ]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 2. Cross joins, and a join under an aggregate
// ============================================================================

// DISTRIBUTED. `COUNT(*)` over a join is the shape an analytical client writes first, and it
// is the one that used to fail: the aggregate needs **no column** from either side, so the
// per-shard scan is planned with an empty projection, and a scan that answered `SELECT *`
// for an empty projection returned three columns where the plan promised none
// (`XX000 … number of columns(3) must match number of fields(0) in schema`). A scan now
// answers an empty projection with the row count, so every count below is the product of
// the two row counts.
#[tokio::test]
async fn test_cross_join_under_an_aggregate() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_cross").await;

    // 4 × 3.
    assert_eq!(
        count(&client, &format!("SELECT COUNT(*) FROM {l} CROSS JOIN {r}")).await,
        12
    );
    // The comma spelling is the same join.
    assert_eq!(
        count(&client, &format!("SELECT COUNT(*) FROM {l}, {r}")).await,
        12
    );
    // A self cross join, 4 × 4 — the same table scanned twice in one plan.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} a CROSS JOIN {l} b")
        )
        .await,
        16
    );
    // Three relations, 4 × 3 × 3.
    assert_eq!(
        count(&client, &format!("SELECT COUNT(*) FROM {l}, {r} a, {r} b")).await,
        36
    );
    // A predicate on one side only, which is still a cross join: 1 × 3.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} CROSS JOIN {r} WHERE {l}.id = 2")
        )
        .await,
        3
    );
    // `COUNT(<column>)` needs one column of the four, which is the same defect one field
    // wider.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(l.id) FROM {l} l CROSS JOIN {r} r")
        )
        .await,
        12
    );
    // An equi self join under a count: `k` = 10, 20, 30 each match themselves and the NULL
    // matches nothing.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} a JOIN {l} b ON a.k = b.k")
        )
        .await,
        3
    );
    // The control: a count over a join whose key *is* projected never had the defect.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l JOIN {r} r ON l.id = r.id")
        )
        .await,
        2
    );
    // A cross join whose columns are projected, which always worked, still answers the
    // same rows.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.id FROM {l} l CROSS JOIN {r} r ORDER BY l.id, r.id LIMIT 4")
        )
        .await,
        vec![
            ("1".into(), "2".into()),
            ("1".into(), "3".into()),
            ("1".into(), "5".into()),
            ("2".into(), "2".into()),
        ]
    );
    // The comma-plus-`WHERE` spelling of an inner join, which is what old SQL writes.
    assert_eq!(
        column(
            &client,
            &format!("SELECT {l}.id FROM {l}, {r} WHERE {l}.id = {r}.id ORDER BY 1")
        )
        .await,
        vec!["2", "3"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 3. `USING` and `NATURAL`
// ============================================================================

// The two spellings that name the join key by column instead of by predicate. What
// distinguishes them from `ON` is the *shape* of `SELECT *`: `USING` merges the named
// column into one and `NATURAL` merges every common column, so the result is narrower than
// the two inputs and a client's column indices depend on it.
#[tokio::test]
async fn test_join_using_merges_the_key_column() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_using").await;

    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} JOIN {r} USING (id) ORDER BY id")
        )
        .await,
        vec!["2", "3"]
    );

    // One `id`, then both `k`s and both payload columns: `k` is common but not named, so it
    // is not merged.
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT * FROM {l} JOIN {r} USING (id)")).await,
        vec!["id", "k", "v", "k", "w"]
    );

    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} LEFT JOIN {r} USING (id) ORDER BY id")
        )
        .await,
        vec!["1", "2", "3", "4"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// `NATURAL JOIN` equates every column the two tables share — here `id` **and** `k` — so it
// returns only the row that agrees on both, and `SELECT *` shows each common column once.
#[tokio::test]
async fn test_natural_join() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_natural").await;

    assert_eq!(
        pairs(
            &client,
            &format!("SELECT id, k FROM {l} NATURAL JOIN {r} ORDER BY id")
        )
        .await,
        vec![("2".into(), "20".into())]
    );
    assert_eq!(
        describe_result_labels(&client, &format!("SELECT * FROM {l} NATURAL JOIN {r}")).await,
        vec!["id", "k", "v", "w"]
    );

    // A natural left join keeps every left row; the merged columns take the left value,
    // which is never NULL here.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT id, w FROM {l} NATURAL LEFT JOIN {r} ORDER BY id")
        )
        .await,
        vec![
            ("1".into(), "NULL".into()),
            ("2".into(), "x".into()),
            ("3".into(), "NULL".into()),
            ("4".into(), "NULL".into()),
        ]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// `USING` on the two join types where the two sides can disagree about the key. A full or
// right join has rows the left side does not have, and PostgreSQL's merged column is
// `COALESCE(left.c, right.c)` — so such a row reports *its own* key and not NULL. An inner
// or left join needs no merge, which is why the tests above pass without one.
//
// DISTRIBUTED, and deliberately so: the merge is a coordinator-side plan rewrite
// (`pg_using_join_merge`) applied before the plan is cut into stages, so this is what says
// the added projection survives being shipped to the cores and back.
#[tokio::test]
async fn test_full_join_using_merges_the_key_column() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_fullusing").await;

    // `5` exists only in `r`. The un-merged left column would report NULL for it, which a
    // client cannot tell from `l`'s genuine NULLs.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id FROM {l} FULL JOIN {r} USING (id)")
        )
        .await,
        vec!["1", "2", "3", "4", "5"]
    );
    // The right join keeps only `r`'s rows, and `5` is one of them.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id FROM {l} RIGHT JOIN {r} USING (id)")
        )
        .await,
        vec!["2", "3", "5"]
    );
    // `SELECT *` reaches the key through wildcard expansion rather than name resolution,
    // and its shape must not have moved: one `id`, then both `k`s.
    assert_eq!(
        describe_result_labels(
            &client,
            &format!("SELECT * FROM {l} FULL JOIN {r} USING (id)")
        )
        .await,
        vec!["id", "k", "v", "k", "w"]
    );
    // `NATURAL` merges every shared column, so `k` merges too: the `5` row's `k` is 50,
    // which only `r` has.
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT id, k FROM {l} NATURAL FULL JOIN {r} ORDER BY id, k")
        )
        .await,
        vec![
            ("1".into(), "10".into()),
            ("2".into(), "20".into()),
            ("3".into(), "30".into()),
            ("3".into(), "99".into()),
            ("4".into(), "NULL".into()),
            ("5".into(), "50".into()),
        ]
    );
    // A grouping key, a sort key and an aggregate argument each resolve the merged column
    // through a different part of the planner.
    assert_eq!(
        column(
            &client,
            &format!("SELECT max(id) FROM {l} FULL JOIN {r} USING (id)")
        )
        .await,
        vec!["5"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// What the merge costs, pinned so it is a recorded decision rather than a discovery: a key
// column reached by an **explicit qualifier**. PostgreSQL answers the raw per-side value
// there — `4` with a NULL beside it, and a NULL beside `5` — because `l.id` is the left
// side's own column and only the unqualified `id` is merged. VaireDB answers the merged
// value in both.
//
// It is not a corner that was skipped. PostgreSQL's join output has three addressable
// names here (`id`, `l.id`, `r.id`) and DataFusion's schema has two fields to hold them, so
// the merged column has to live in whichever fields every other consumer reads — which is
// both of them. The `ON` spelling below is the query that answers this question today.
#[tokio::test]
async fn test_a_qualified_using_key_reports_the_merged_value() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_fullusing_qual").await;

    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, r.id FROM {l} l FULL JOIN {r} r USING (id) ORDER BY l.id")
        )
        .await,
        vec![
            ("1".into(), "1".into()),
            ("2".into(), "2".into()),
            ("3".into(), "3".into()),
            ("4".into(), "4".into()),
            ("5".into(), "5".into()),
        ]
    );
    // The `ON` spelling is untouched by the merge and reports the raw sides, which is
    // PostgreSQL's answer for the query above.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, r.id FROM {l} l FULL JOIN {r} r ON l.id = r.id \
                 ORDER BY COALESCE(l.id, r.id)"
            )
        )
        .await,
        vec![
            ("1".into(), "NULL".into()),
            ("2".into(), "2".into()),
            ("3".into(), "3".into()),
            ("4".into(), "NULL".into()),
            ("NULL".into(), "5".into()),
        ]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// REFUSAL. The one clause that cannot reach the merged column: DataFusion plans a `WHERE`
// predicate without the join's `USING` set in hand, so two fields named `id` are ambiguous
// to it where PostgreSQL sees one merged column. It refuses rather than answering a
// different question, and it refuses on **every** `USING` and `NATURAL` join — inner
// included — so this is not a cost of the merge above. The qualified spelling works, and
// on a full join it now carries the merged value.
#[tokio::test]
async fn test_a_where_clause_on_a_using_key_is_refused() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_usingwhere").await;

    for sql in [
        format!("SELECT id FROM {l} FULL JOIN {r} USING (id) WHERE id > 2"),
        format!("SELECT id FROM {l} JOIN {r} USING (id) WHERE id > 2"),
        format!("SELECT id FROM {l} NATURAL FULL JOIN {r} WHERE id > 2"),
    ] {
        // `42703` undefined_column, not PostgreSQL's `42702` ambiguous_column — recorded
        // as observed, since PostgreSQL does not refuse this form at all.
        let err = assert_sqlstate(&client, &sql, "42703").await;
        assert!(
            err.message().contains("mbiguous"),
            "`{sql}` should be refused as ambiguous, got: {}",
            err.message()
        );
    }

    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT l.id FROM {l} l FULL JOIN {r} r USING (id) WHERE l.id > 2")
        )
        .await,
        vec!["3", "4", "5"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 4. Join shapes and predicates
// ============================================================================

// The `ON` clauses that are not a single equality, plus the shapes a query planner has to
// reassociate: a three-way join, a self join, a non-equi join and a disjunctive one. A
// distributed planner has more ways to get these wrong than a local one, because each
// changes which side can be partitioned.
#[tokio::test]
async fn test_join_predicates_and_shapes() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_shapes").await;
    let m = setup_third(&client, "jg_shapes").await;

    // Three-way over the shard key: only id 2 is in all three.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT l.id FROM {l} l JOIN {r} r ON l.id = r.id JOIN {m} m ON l.id = m.id \
                 ORDER BY l.id"
            )
        )
        .await,
        vec!["2"]
    );

    // A self join on a non-shard key: 10, 20, 30 each match themselves.
    assert_eq!(
        column(
            &client,
            &format!("SELECT a.id FROM {l} a JOIN {l} b ON a.k = b.k ORDER BY a.id")
        )
        .await,
        vec!["1", "2", "3"]
    );

    // Non-equi: 10 < {20, 99, 50}, 20 < {99, 50}, 30 < {99, 50} — and the NULL key
    // compares to nothing.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l JOIN {r} r ON l.k < r.k")
        )
        .await,
        7
    );

    // Disjunctive: id 2 matches on both halves, id 3 on the first only.
    assert_eq!(
        column(
            &client,
            &format!("SELECT l.id FROM {l} l JOIN {r} r ON l.id = r.id OR l.k = r.k ORDER BY l.id")
        )
        .await,
        vec!["2", "3"]
    );

    // A join key of a different width on each side: `int4` against `int8`.
    let big = setup_wide(&client, "jg_shapes").await;
    assert_eq!(
        pairs(
            &client,
            &format!("SELECT l.id, b.d FROM {l} l JOIN {big} b ON l.k = b.k ORDER BY l.id")
        )
        .await,
        vec![("2".into(), "2.5".into())]
    );

    // A text join key.
    assert_eq!(
        column(
            &client,
            &format!("SELECT l.id FROM {l} l JOIN {m} m ON l.v = m.z ORDER BY l.id")
        )
        .await,
        vec!["2"]
    );

    // An outer join's `ON` filters the *match*, a `WHERE` filters the *result* — the
    // distinction that decides whether an outer join stays outer. Here the extra condition
    // is in `ON`, so all four left rows survive and only one is matched.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, r.w FROM {l} l LEFT JOIN {r} r ON l.id = r.id AND r.w = 'x' \
                 ORDER BY l.id"
            )
        )
        .await,
        vec![
            ("1".into(), "NULL".into()),
            ("2".into(), "x".into()),
            ("3".into(), "NULL".into()),
            ("4".into(), "NULL".into()),
        ]
    );
    // The same condition in `WHERE` discards the padding rows, which is an inner join.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, r.w FROM {l} l LEFT JOIN {r} r ON l.id = r.id WHERE r.w = 'x' \
                 ORDER BY l.id"
            )
        )
        .await,
        vec![("2".into(), "x".into())]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
    drop_table(&client, &m).await;
    drop_table(&client, &big).await;
}

// DISTRIBUTED. A join that feeds something else: an aggregate, a `HAVING`, an `ORDER BY` and
// a `LIMIT`, a derived table and a CTE. Each adds a stage above the join, and the join's
// output partitioning has to satisfy what that stage needs.
#[tokio::test]
async fn test_join_feeding_aggregation_and_ordering() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_agg").await;

    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.v, COUNT(*) FROM {l} l JOIN {r} r ON l.id = r.id \
                 GROUP BY l.v HAVING COUNT(*) >= 1 ORDER BY l.v"
            )
        )
        .await,
        vec![("b".into(), "1".into()), ("c".into(), "1".into())]
    );

    assert_eq!(
        column(
            &client,
            &format!("SELECT l.id FROM {l} l JOIN {r} r ON l.id = r.id ORDER BY l.id DESC LIMIT 1")
        )
        .await,
        vec!["3"]
    );

    // A derived table on one side.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT l.id FROM {l} l JOIN (SELECT id FROM {r} WHERE id < 5) s ON l.id = s.id \
                 ORDER BY l.id"
            )
        )
        .await,
        vec!["2", "3"]
    );

    // The same thing as a CTE, which is where a view would land too.
    assert_eq!(
        column(
            &client,
            &format!(
                "WITH s AS (SELECT id FROM {r} WHERE id < 5) \
                 SELECT l.id FROM {l} l JOIN s ON l.id = s.id ORDER BY l.id"
            )
        )
        .await,
        vec!["2", "3"]
    );

    // A shard-key predicate on one side, which lets the planner prune shards on that side
    // only — the other side still has to be read in full.
    assert_eq!(
        column(
            &client,
            &format!("SELECT l.id FROM {l} l JOIN {r} r ON l.id = r.id WHERE l.id = 2")
        )
        .await,
        vec!["2"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// `LATERAL` — a subquery on the right of a join that may reference the left row. It is a
// correlated join, so it cannot be planned as one shuffle, and it is worth its own test for
// that reason.
#[tokio::test]
async fn test_lateral_join() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_lateral").await;

    let expected = vec![
        ("1".to_string(), "0".to_string()),
        ("2".to_string(), "1".to_string()),
        ("3".to_string(), "1".to_string()),
        ("4".to_string(), "0".to_string()),
    ];
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, s.n FROM {l} l \
                 JOIN LATERAL (SELECT COUNT(*) AS n FROM {r} r WHERE r.id = l.id) s ON true \
                 ORDER BY l.id"
            )
        )
        .await,
        expected
    );
    // `LEFT JOIN LATERAL` answers the same here: the aggregate always returns a row, so
    // there is nothing to pad.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, s.n FROM {l} l \
                 LEFT JOIN LATERAL (SELECT COUNT(*) AS n FROM {r} r WHERE r.id = l.id) s ON true \
                 ORDER BY l.id"
            )
        )
        .await,
        expected
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 5. Semi and anti joins
// ============================================================================

// The spellings that ask "is there a match?" rather than "join me to it". PostgreSQL has
// five and they plan to two joins — a semi and an anti — so what is pinned here is that all
// five agree.
#[tokio::test]
async fn test_semi_and_anti_join_spellings() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_semi").await;

    let present = vec!["2".to_string(), "3".to_string()];
    let absent = vec!["1".to_string(), "4".to_string()];

    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE id IN (SELECT id FROM {r}) ORDER BY id")
        )
        .await,
        present
    );
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE id NOT IN (SELECT id FROM {r}) ORDER BY id")
        )
        .await,
        absent
    );
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT l.id FROM {l} l WHERE EXISTS (SELECT 1 FROM {r} r WHERE r.id = l.id) \
                 ORDER BY l.id"
            )
        )
        .await,
        present
    );
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT l.id FROM {l} l WHERE NOT EXISTS (SELECT 1 FROM {r} r WHERE r.id = l.id) \
                 ORDER BY l.id"
            )
        )
        .await,
        absent
    );
    // `= ANY` is `IN` and `<> ALL` is `NOT IN`; both are rewritten to those spellings before
    // planning, because the compatibility rule that owns `ANY` over an *array* would
    // otherwise pass the subquery to `array_contains`.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE id = ANY (SELECT id FROM {r}) ORDER BY id")
        )
        .await,
        present
    );
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE id <> ALL (SELECT id FROM {r}) ORDER BY id")
        )
        .await,
        absent
    );

    // A correlated scalar subquery in the select list, which is the same question asked as
    // a value rather than as a predicate.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, (SELECT COUNT(*) FROM {r} r WHERE r.id = l.id) FROM {l} l \
                 ORDER BY l.id"
            )
        )
        .await,
        vec![
            ("1".into(), "0".into()),
            ("2".into(), "1".into()),
            ("3".into(), "1".into()),
            ("4".into(), "0".into()),
        ]
    );

    // An uncorrelated aggregate subquery, which is one value the planner may evaluate once.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE id = (SELECT MAX(id) FROM {r} WHERE id < 5)")
        )
        .await,
        vec!["3"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// DISTRIBUTED, and the widest defect this axis found — now closed. Every `EXISTS` in the
// test above correlates by an **equality**, which was for a while the only shape that
// worked: a semi or anti join whose subquery gives the planner no equijoin key is a
// `NestedLoopJoinExec`, and that operator decides a `LeftSemi` or `LeftAnti` result from a
// match bitmap over its *collected* side, emitted only once every probe partition has
// reported in. DataFusion counts those partitions down through one shared counter; Ballista
// runs each of them as a separate task in a separate process, so every task counted its own
// copy down by one, none ever reached zero, and the emission that is the entire result
// never happened. Both `EXISTS` and `NOT EXISTS` collapsed to a constant false.
//
// `scheduler::nested_loop_join_one_task` collects the probe side into one partition, so the
// join runs as a single task whose counter does reach zero. The cost is the probe's
// parallelism, and only these shapes pay it.
//
// Kept as its own test rather than folded into the one above because these are the
// spellings that need no equality — an existence check with a constant predicate, and an
// inequality correlation as a "has a later row" test — and because the last two assertions
// were right before the fix and have to stay right after it.
#[tokio::test]
async fn test_exists_without_an_equijoin_key() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_exists_nokey").await;

    let every = vec![
        "1".to_string(),
        "2".to_string(),
        "3".to_string(),
        "4".to_string(),
    ];
    let none = Vec::<String>::new();

    // The shortest reproduction, and ordinary PostgreSQL: "is that table non-empty?".
    // Answered as if `r` were empty, before the fix.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE EXISTS (SELECT 1 FROM {r}) ORDER BY id")
        )
        .await,
        every
    );
    // An uncorrelated subquery with a filter that does match.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE EXISTS (SELECT 1 FROM {r} WHERE k = 20) ORDER BY id"
            )
        )
        .await,
        every
    );
    // A non-equality correlation: every non-NULL key has something above it in `r`, whose
    // maximum is 99. The NULL key compares to nothing, so `r.k > l.k` is NULL for every
    // candidate and that row alone sees an empty subquery.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} l WHERE EXISTS \
                 (SELECT 1 FROM {r} r WHERE r.k > l.k) ORDER BY id"
            )
        )
        .await,
        vec!["1", "2", "3"]
    );
    // The negation of the same, which is the assertion that says the fix is not simply
    // "answer everything": exactly one row, and the two directions partition the table.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} l WHERE NOT EXISTS \
                 (SELECT 1 FROM {r} r WHERE r.k > l.k) ORDER BY id"
            )
        )
        .await,
        vec!["4"]
    );
    // An uncorrelated `NOT EXISTS` whose filter matches nothing: every row.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE NOT EXISTS (SELECT 1 FROM {r} WHERE k = 777) ORDER BY id"
            )
        )
        .await,
        every
    );
    // A correlation on a column that is NULL throughout `r` — the subquery is empty for
    // every left row, so the anti join returns all of them.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} l WHERE NOT EXISTS \
                 (SELECT 1 FROM {r} r WHERE r.k IS NULL AND r.k > l.k) ORDER BY id"
            )
        )
        .await,
        every
    );

    // The two spellings that were already right, pinned so the fix is seen to keep them.
    // A subquery the optimizer can prove empty is folded away before a join is planned.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE NOT EXISTS (SELECT 1 FROM {r} WHERE 1 = 0) ORDER BY id"
            )
        )
        .await,
        every
    );
    // And `r` is non-empty, so PostgreSQL answers nothing here too.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE NOT EXISTS (SELECT 1 FROM {r}) ORDER BY id")
        )
        .await,
        none
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// DISTRIBUTED. The same defect in its quieter form, and the shape that went unmeasured
// until the semi join was traced: a `LEFT` or `FULL` join with **no equijoin key** loses
// exactly its unmatched left rows. Those rows are the ones the outer join exists to
// produce, and they come from the same build-side bitmap emission a `LeftSemi` result is
// entirely made of — so where the semi join answered nothing at all, these answered the
// inner join and looked plausible.
//
// An `INNER` join, a `RIGHT` join and a `CROSS JOIN` over the identical plan shape were
// correct throughout: their rows are decided as the probe streams, with no final pass to
// coordinate. That contrast is what identified the operator, so it is pinned here.
#[tokio::test]
async fn test_outer_join_without_an_equijoin_key_keeps_its_unmatched_rows() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_outer_nokey").await;

    // `l.k = 10, 20, 30, NULL` against `r.k = 20, 99, 50`, matched by `r.k > l.k`: three
    // rows for 10, two each for 20 and 30, and the NULL key matches nothing. A LEFT join
    // pads that last one rather than dropping it — before the fix it was dropped.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, r.id FROM {l} l LEFT JOIN {r} r ON r.k > l.k ORDER BY l.id, r.id"
            )
        )
        .await,
        vec![
            ("1".into(), "2".into()),
            ("1".into(), "3".into()),
            ("1".into(), "5".into()),
            ("2".into(), "3".into()),
            ("2".into(), "5".into()),
            ("3".into(), "3".into()),
            ("3".into(), "5".into()),
            ("4".into(), "NULL".into()),
        ]
    );
    // A FULL join over the same predicate: every `r` row matches something, so the only
    // padding is the same left row — which makes this the LEFT answer, and any difference
    // between the two a lost row rather than a semantic one.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l FULL JOIN {r} r ON r.k > l.k")
        )
        .await,
        8
    );
    // A FULL join whose predicate leaves rows unmatched on *both* sides: `r.k > 60` holds
    // for 99 alone, so `l.k = 10` matches it and every other row on either side is padded.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT l.id, r.id FROM {l} l FULL JOIN {r} r ON r.k > 60 AND l.k = 10 \
                 ORDER BY l.id, r.id"
            )
        )
        .await,
        vec![
            ("1".into(), "3".into()),
            ("2".into(), "NULL".into()),
            ("3".into(), "NULL".into()),
            ("4".into(), "NULL".into()),
            ("NULL".into(), "2".into()),
            ("NULL".into(), "5".into()),
        ]
    );

    // The probe-driven join types over the identical shape, which never lost a row and must
    // not start paying for a fix they do not need.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l JOIN {r} r ON r.k > l.k")
        )
        .await,
        7
    );
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l RIGHT JOIN {r} r ON r.k > l.k")
        )
        .await,
        7
    );
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} l CROSS JOIN {r} r")
        )
        .await,
        12
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// THREE-VALUED. `NOT IN (subquery)` is where an anti join and PostgreSQL part company. In
// PostgreSQL the predicate is NULL — and so not true — as soon as the left value is NULL or
// any candidate is, and only an *empty* subquery makes it true unconditionally.
//
// DataFusion plans it as an anti join, and only its `HashJoinExec` carries the `null_aware`
// flag that reproduces those rules; under Ballista the anti join is a `SortMergeJoinExec`,
// which reads a NULL as merely unequal and keeps the row. Unfixed, the first two queries
// below measured `1, 3, 4` and `4` on this cluster.
//
// Turning `prefer_hash_join` on is not the fix and was measured not to be: a null-aware
// anti join is only correct as a broadcast, Ballista's planner refuses to broadcast it and
// swaps the sides instead, and `HashJoinExec` will not build a null-aware `RightAnti` — the
// job dies inside the scheduler and the client never hears about it. So the coordinator
// respells the predicate in the AST before it is planned, into a `NOT EXISTS` over a
// one-column derived table whose filter carries all three NULL rules; see
// `compat_rewrite::rewrite_not_in_subqueries` and
// `scheduler::with_postgres_sql_options`.
#[tokio::test]
async fn test_not_in_a_subquery_is_null_aware() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_notin").await;

    // The left NULL drops: `NULL NOT IN (20, 99, 50)` is NULL, not true.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k NOT IN (SELECT k FROM {r}) ORDER BY id")
        )
        .await,
        vec!["1", "3"]
    );
    // A NULL among the candidates makes every non-matching row NULL too, so a subquery
    // holding one answers nothing at all.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k NOT IN (SELECT k FROM {l}) ORDER BY id")
        )
        .await,
        Vec::<String>::new()
    );
    // An empty subquery has no NULL to poison the comparison, so every row is true —
    // including the one whose key is NULL.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE k NOT IN (SELECT k FROM {r} WHERE k > 1000) ORDER BY id"
            )
        )
        .await,
        vec!["1", "2", "3", "4"]
    );
    // `<> ALL` is the same predicate spelled the other way, so it answers the same.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k <> ALL (SELECT k FROM {r}) ORDER BY id")
        )
        .await,
        vec!["1", "3"]
    );
    // A NULL in a literal list poisons it the same way a NULL row does.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k NOT IN (10, NULL) ORDER BY id")
        )
        .await,
        Vec::<String>::new()
    );
    // `IN` needs no such care: a NULL candidate can only turn a false into a NULL, and
    // neither is returned.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k IN (SELECT k FROM {l}) ORDER BY id")
        )
        .await,
        vec!["1", "2", "3"]
    );

    // A rewritten predicate has to survive the contexts a predicate appears in, since a
    // lost conjunct or a wrong count would be as silent as the defect itself.
    //
    // In a conjunction: `k > 15 AND k NOT IN (20, 99, 50)` leaves `k = 30`.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE k > 15 AND k NOT IN (SELECT k FROM {r}) ORDER BY id"
            )
        )
        .await,
        vec!["3"]
    );
    // Under an aggregate, where the predicate's row count is the answer.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} WHERE k NOT IN (SELECT k FROM {r})")
        )
        .await,
        2
    );
    // With a left side that is not a bare column: `k + 10` is `20, 30, 40, NULL`, and only
    // `20` is among the candidates.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k + 10 NOT IN (SELECT k FROM {r}) ORDER BY id")
        )
        .await,
        vec!["2", "3"]
    );
    // In a `HAVING` clause, over the grouped column. The aggregate spelling of the same
    // predicate is the gap below: only a grouped *column* can be rewritten.
    assert_eq!(
        column(
            &client,
            &format!("SELECT k FROM {l} GROUP BY k HAVING k NOT IN (SELECT k FROM {r}) ORDER BY k")
        )
        .await,
        vec!["10", "30"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// THREE-VALUED, and the two shapes the rewrite above declines to enter. This test pins
// today's answer so a regression is visible;
// `test_not_in_over_an_aggregate_or_a_correlated_subquery` asserts PostgreSQL's.
//
// The rewrite is applied only where NULL and false are indistinguishable — a clause that
// keeps a row when the predicate is *true*, reached through `AND`/`OR` — and only to a left
// side it can move inside a subquery. That leaves two shapes:
//
// * under a `NOT`, where two-valued and three-valued differ. Measured on the cluster,
//   DataFusion's simplifier already recovers PostgreSQL's answer here by turning the double
//   negation into a semi join, so this one is pinned as **correct** rather than as a gap.
// * a left side holding an **aggregate**, i.e. `HAVING MAX(k) NOT IN (q)`. Every available
//   spelling puts that aggregate inside a subquery, and DataFusion cannot plan a correlated
//   subquery over an aggregate at all — a hand-written `NOT EXISTS (… WHERE r.k = MAX(l.k))`
//   fails the same way. Rewriting would trade a wrong answer for a plan error naming a
//   clause the client did not write, so the predicate is left as written.
//
// * a **correlated** subquery. A derived table cannot see the outer row without `LATERAL`,
//   so wrapping one would turn a wrong answer into a failure to resolve a column — worse for
//   a client whose query runs today.
#[tokio::test]
async fn test_not_in_over_an_aggregate_or_a_correlated_subquery_is_wrong() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_notin_gap").await;

    // Already PostgreSQL's answer, and not by way of the rewrite: `NOT (k NOT IN q)` is
    // simplified to a semi join before the null-awareness of the anti join can matter.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE NOT (k NOT IN (SELECT k FROM {r})) ORDER BY id")
        )
        .await,
        vec!["2"]
    );
    // The aggregate left side, which keeps the null-unaware answer: PostgreSQL drops the
    // NULL group and answers `10, 30`.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT k FROM {l} GROUP BY k HAVING MAX(k) NOT IN (SELECT k FROM {r}) \
                 ORDER BY k"
            )
        )
        .await,
        vec!["10", "30", "NULL"]
    );
    // The correlated subquery, which keeps it too: for id 4 the subquery holds `50` and the
    // left key is NULL, so PostgreSQL's predicate is NULL and the row drops — it answers
    // `1, 2, 3`.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE k NOT IN \
                 (SELECT k FROM {r} WHERE {r}.id > {l}.id) ORDER BY id"
            )
        )
        .await,
        vec!["1", "2", "3", "4"]
    );
    // `NOT EXISTS` expresses the row-level predicate correctly and is available today —
    // row 19. It is not available over an aggregate, which is what closes the corner above.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE NOT EXISTS \
                 (SELECT 1 FROM {r} WHERE {r}.k = {l}.k) ORDER BY id"
            )
        )
        .await,
        vec!["1", "3", "4"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

#[tokio::test]
#[ignore = "gap (row 23): NOT IN must be null-aware over an aggregate and over a correlated subquery"]
async fn test_not_in_over_an_aggregate_or_a_correlated_subquery() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_notin_want").await;

    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT k FROM {l} GROUP BY k HAVING MAX(k) NOT IN (SELECT k FROM {r}) \
                 ORDER BY k"
            )
        )
        .await,
        vec!["10", "30"]
    );
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE k NOT IN \
                 (SELECT k FROM {r} WHERE {r}.id > {l}.id) ORDER BY id"
            )
        )
        .await,
        vec!["1", "2", "3"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// REFUSAL. Every quantified comparison over a subquery except `= ANY` and `<> ALL` plans to
// a *mark* join, whose output column is called `mark` on both sides of the join above it.
// Serializing that plan for an executor fails — `Schema contains duplicate unqualified
// field name mark` — so before this refusal the query reached the client as `XX000`
// carrying a raw gRPC `Status { … }`. The refusal names the aggregate rewrite that works,
// and does not apply it: `x > (SELECT max(c) …)` differs from `x > ALL (SELECT c …)` on an
// empty subquery and on one containing a NULL.
#[tokio::test]
async fn test_quantified_subquery_forms_are_refused() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_quant").await;

    for op in ["> ALL", ">= ALL", "= ALL", "> ANY", "< SOME"] {
        let sql = format!("SELECT id FROM {l} WHERE k {op} (SELECT k FROM {r})");
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("aggregate") && err.message().contains("EXISTS"),
            "`{sql}` should name a rewrite that works: {}",
            err.message()
        );
    }

    // The rewrite the message names is available, and answers.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k > (SELECT MAX(k) FROM {r}) ORDER BY id")
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k > (SELECT MIN(k) FROM {r}) ORDER BY id")
        )
        .await,
        vec!["3"]
    );
    // The two spellings that are not mark joins keep working — see the semi/anti test.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM {l} WHERE id = ANY (SELECT id FROM {r})")
        )
        .await,
        2
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

#[tokio::test]
#[ignore = "gap (row 24): <op> ANY/ALL (subquery) must answer instead of being refused"]
async fn test_quantified_subquery_forms_answer() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_quant_gap").await;

    // `k > ALL (20, 99, 50)` is true for no row; `k > ANY (…)` is true for 30 alone.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k > ALL (SELECT k FROM {r}) ORDER BY id")
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} WHERE k > ANY (SELECT k FROM {r}) ORDER BY id")
        )
        .await,
        vec!["3"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 6. Set operations
// ============================================================================

// The four operations and what their `ALL`/`DISTINCT` qualifier does to duplicates.
#[tokio::test]
async fn test_set_operation_basics() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setops").await;

    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} UNION ALL SELECT id FROM {r} ORDER BY id")
        )
        .await,
        vec!["1", "2", "2", "3", "3", "4", "5"]
    );
    for spelling in ["UNION", "UNION DISTINCT"] {
        assert_eq!(
            column(
                &client,
                &format!("SELECT id FROM {l} {spelling} SELECT id FROM {r} ORDER BY id")
            )
            .await,
            vec!["1", "2", "3", "4", "5"],
            "`{spelling}` should remove the duplicates"
        );
    }
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} INTERSECT SELECT id FROM {r} ORDER BY id")
        )
        .await,
        vec!["2", "3"]
    );
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} EXCEPT SELECT id FROM {r} ORDER BY id")
        )
        .await,
        vec!["1", "4"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// THREE-VALUED. A set operation compares whole rows, and it treats two NULLs as **equal** —
// unlike a join predicate, which treats them as unknown. So the NULL-keyed row is one value
// among the others: `EXCEPT` keeps it when the right side has no NULL, and `INTERSECT`
// returns it when both sides do.
#[tokio::test]
async fn test_set_operations_and_nulls() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setnull").await;

    // `l.k` = 10, 20, 30, NULL minus `r.k` = 20, 99, 50 → 10, 30, NULL.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {l} EXCEPT SELECT k FROM {r}")
        )
        .await,
        vec!["10", "30", "NULL"]
    );
    // Both sides hold the NULL, so it survives an intersection with itself.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {l} INTERSECT SELECT k FROM {l}")
        )
        .await,
        vec!["10", "20", "30", "NULL"]
    );
    // Only one side holds it, so it does not.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {l} INTERSECT SELECT k FROM {r}")
        )
        .await,
        vec!["20"]
    );
    // A union folds the two NULLs into one row.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {l} UNION SELECT k FROM {l}")
        )
        .await,
        vec!["10", "20", "30", "NULL"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// `ORDER BY` and `LIMIT` over a set operation belong to the *whole* operation, and a
// parenthesized branch may carry its own. Getting the scope wrong is not an error, it is a
// different answer, which is why each spelling is pinned separately.
#[tokio::test]
async fn test_set_operation_ordering_and_limits() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setorder").await;

    // By ordinal, which is the only way to name a column of a set operation portably.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} UNION SELECT id FROM {r} ORDER BY 1 DESC")
        )
        .await,
        vec!["5", "4", "3", "2", "1"]
    );
    // By the first branch's output name, which is where a set operation's labels come from.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id AS n FROM {l} UNION SELECT id FROM {r} ORDER BY n")
        )
        .await,
        vec!["1", "2", "3", "4", "5"]
    );
    // A `LIMIT` after the operation applies to the result.
    assert_eq!(
        column(
            &client,
            &format!("SELECT id FROM {l} UNION SELECT id FROM {r} ORDER BY 1 LIMIT 2")
        )
        .await,
        vec!["1", "2"]
    );
    // A `LIMIT` inside a parenthesized branch applies to that branch: one row from each.
    assert_eq!(
        sorted_column(
            &client,
            &format!(
                "(SELECT id FROM {l} ORDER BY id LIMIT 1) UNION ALL \
                 (SELECT id FROM {r} ORDER BY id LIMIT 1)"
            )
        )
        .await,
        vec!["1", "2"]
    );
    // Chained operations associate left to right, so the `UNION` deduplicates what the
    // `UNION ALL` produced: ids 2 and 3 from `l`, then 2 and 3 again from `r`.
    assert_eq!(
        sorted_column(
            &client,
            &format!(
                "SELECT id FROM {l} WHERE id IN (2, 3) UNION ALL \
                 SELECT id FROM {r} WHERE id IN (2, 3) UNION \
                 SELECT id FROM {r} WHERE id IN (2, 3)"
            )
        )
        .await,
        vec!["2", "3"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// The type a set operation advertises is the type resolved over **every** branch, not the
// first one's. Reading it off the first branch is not a wrong type in the abstract — it is a
// wrong *value*: an `int4` column fed a bigint either raises `22003` or, for a float,
// truncates silently. The result type is asserted at Describe, before a row is fetched,
// because that is what a client binds its buffer from.
#[tokio::test]
async fn test_set_operation_type_resolution() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_settypes").await;
    let big = setup_wide(&client, "jg_settypes").await;

    // int4 ∪ int8 = int8, in either order, and the wide value survives.
    for sql in [
        format!("SELECT k FROM {l} UNION ALL SELECT k FROM {big}"),
        format!("SELECT k FROM {big} UNION ALL SELECT k FROM {l}"),
    ] {
        assert_eq!(
            describe_result_types(&client, &sql).await,
            vec![Type::INT8],
            "`{sql}` resolves to int8"
        );
        assert!(
            sorted_column(&client, &sql)
                .await
                .contains(&"5000000000".to_string()),
            "`{sql}` must keep the value int4 cannot hold"
        );
    }

    // int4 ∪ float8 = float8, in either order, and the fraction survives.
    for sql in [
        format!("SELECT k FROM {l} UNION ALL SELECT d FROM {big}"),
        format!("SELECT d FROM {big} UNION ALL SELECT k FROM {l}"),
    ] {
        assert_eq!(
            describe_result_types(&client, &sql).await,
            vec![Type::FLOAT8],
            "`{sql}` resolves to float8"
        );
        assert!(
            sorted_column(&client, &sql)
                .await
                .contains(&"2.5".to_string()),
            "`{sql}` must not truncate 2.5 to 2"
        );
    }

    // The label, unlike the type, does come from the first branch — PostgreSQL's rule.
    assert_eq!(
        describe_result_labels(
            &client,
            &format!("SELECT id AS first_name FROM {l} UNION SELECT id AS second_name FROM {r}")
        )
        .await,
        vec!["first_name"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
    drop_table(&client, &big).await;
}

// REFUSAL. A set operation whose branches do not line up. The column-count mismatch is
// refused at parse time, as in PostgreSQL. Incompatible *types* used to be the gap and are now
// refused too: PostgreSQL raises `42804` at plan time naming both types, because it resolves a
// common type reachable by **implicit** coercion and between two type categories there is none.
// DataFusion always finds one — everything casts to a string — so `int ∪ text` answered
// `1, 2, 3, 4, x, y, z` as text, rows PostgreSQL never returns, and an `ORDER BY` over them
// sorted `10` before `9`.
//
// The refusal is symmetric in the branch order, which the answer already was: resolution used
// to follow the *first* branch, so `int ∪ text` failed `22003` trying to read `'x'` as a number
// while `text ∪ int` rendered the integers as text. Resolving over both branches
// (`coerce_types` in `plan_select`) made it symmetric first; refusing it before coercion
// (`pg_set_op_types`) is what made it PostgreSQL's answer.
#[tokio::test]
async fn test_set_operation_branch_mismatch() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setmismatch").await;

    // Different widths: refused, and by the parser rather than the planner.
    assert_sqlstate(
        &client,
        &format!("SELECT id, k FROM {l} UNION SELECT id FROM {r}"),
        SQLSTATE_SYNTAX_ERROR,
    )
    .await;

    // Different types, either order, `ALL` or `DISTINCT`: PostgreSQL's `42804`, before a row
    // is read, and the message names both types so the client knows what to cast.
    for sql in [
        format!("SELECT id FROM {l} UNION ALL SELECT w FROM {r}"),
        format!("SELECT w FROM {r} UNION ALL SELECT id FROM {l}"),
        format!("SELECT id FROM {l} UNION SELECT w FROM {r}"),
    ] {
        let err = assert_sqlstate(&client, &sql, "42804").await;
        assert!(
            err.message().contains("int4") && err.message().contains("text"),
            "`{sql}` should name both types, got: {}",
            err.message()
        );
    }

    // A mismatch one level down is the same wrong answer, so it is refused there too.
    for sql in [
        format!("SELECT * FROM (SELECT id FROM {l} UNION ALL SELECT w FROM {r}) AS u"),
        format!("WITH u AS (SELECT id FROM {l} UNION ALL SELECT w FROM {r}) SELECT * FROM u"),
    ] {
        assert_sqlstate(&client, &sql, "42804").await;
    }

    // An explicit cast is the fix the message names, and it answers.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id::text FROM {l} UNION ALL SELECT w FROM {r}")
        )
        .await,
        vec!["1", "2", "3", "4", "x", "y", "z"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// What the refusal must NOT catch. PostgreSQL's `UNKNOWN` is a bare string literal or a bare
// `NULL` in a branch's own select list: it has no type yet and takes the one the other branches
// resolve to, so these are integer unions and not mismatches. Arrow has no `UNKNOWN` — the
// literal is already `Utf8` in the plan — so each of these would be refused by a check that
// went on types alone, and each is a query a client writes.
//
// The boundary is where it stops, and it is not where it looks like it should: a `VALUES`
// branch's literal is already `text` by the time the set operation sees it, in PostgreSQL as
// much as here. Every row below was byte-diffed against a PostgreSQL 16.15 oracle.
#[tokio::test]
async fn test_an_untyped_literal_branch_still_answers() {
    let client = ready_client().await;
    let (l, _r) = setup_pair(&client, "jg_setunknown").await;

    // A bare string literal, either order, and aliased. All three answer, which is what the
    // check has to leave alone. What the *result type* is, is a separate matter — see
    // `test_an_untyped_literal_branch_resolves_to_text` below.
    for sql in [
        format!("SELECT id FROM {l} UNION ALL SELECT '9'"),
        format!("SELECT '9' UNION ALL SELECT id FROM {l}"),
        format!("SELECT id FROM {l} UNION ALL SELECT '9' AS id"),
    ] {
        assert_eq!(
            sorted_column(&client, &sql).await,
            vec!["1", "2", "3", "4", "9"],
            "`{sql}` must answer rather than be refused"
        );
    }

    // A bare NULL, which is how a client pads a branch it has no value for.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id FROM {l} UNION ALL SELECT NULL")
        )
        .await,
        vec!["1", "2", "3", "4", "NULL"]
    );

    // …and where it stops. A `VALUES` list resolves its own columns first, so its literal is
    // text and the union is a mismatch — unlike the `SELECT '9'` spelling above.
    for sql in [
        format!("SELECT id FROM {l} UNION ALL VALUES ('9')"),
        format!("SELECT id FROM {l} UNION ALL SELECT * FROM (VALUES ('9')) AS v"),
    ] {
        assert_sqlstate(&client, &sql, "42804").await;
    }

    // A cast literal is not untyped either: `'9'::text` is text as much as a text column is.
    assert_sqlstate(
        &client,
        &format!("SELECT id FROM {l} UNION ALL SELECT '9'::text"),
        "42804",
    )
    .await;

    drop_table(&client, &l).await;
}

// GAP (row 43). Which type the untyped branch resolves *to*. PostgreSQL's `UNKNOWN` takes the
// other branch's type, so `SELECT id FROM l UNION ALL SELECT '9'` is an integer union and
// `pg_typeof` says `integer` — measured against a 16.15 oracle, for both the bare literal and
// the bare `NULL`. VaireDB answers the right rows but calls the column `text`, because Arrow has
// already made the literal `Utf8` and DataFusion's coercion then picks the common type of
// `Int32 ∪ Utf8`, which is `Utf8`.
//
// Recorded rather than fixed: matching it means rewriting the untyped branch's projection to
// cast to the resolved type before coercion runs — the refusal above only has to *recognize*
// the untyped branch, not retype it. The rows are correct either way; what a driver sees is the
// column's advertised OID, so `ORDER BY` over the result sorts lexicographically.
#[tokio::test]
async fn test_an_untyped_literal_branch_resolves_to_text() {
    let client = ready_client().await;
    let (l, _r) = setup_pair(&client, "jg_setunknowntype").await;

    for sql in [
        format!("SELECT id FROM {l} UNION ALL SELECT '9'"),
        format!("SELECT '9' UNION ALL SELECT id FROM {l}"),
    ] {
        assert_eq!(
            describe_result_types(&client, &sql).await,
            vec![Type::TEXT],
            "`{sql}` resolves to text where PostgreSQL resolves to int4"
        );
    }

    drop_table(&client, &l).await;
}

#[tokio::test]
#[ignore = "gap (row 43): an untyped literal branch must resolve to the typed branch's type"]
async fn test_an_untyped_literal_branch_resolves_to_the_typed_branch_type() {
    let client = ready_client().await;
    let (l, _r) = setup_pair(&client, "jg_setunknowntypegap").await;

    for sql in [
        format!("SELECT id FROM {l} UNION ALL SELECT '9'"),
        format!("SELECT '9' UNION ALL SELECT id FROM {l}"),
        format!("SELECT id FROM {l} UNION ALL SELECT NULL"),
    ] {
        assert_eq!(
            describe_result_types(&client, &sql).await,
            vec![Type::INT4],
            "`{sql}` must resolve to int4, as PostgreSQL does"
        );
    }

    drop_table(&client, &l).await;
}

// The same refusal reaching `INTERSECT` and `EXCEPT`, which is a harder case than `UNION` in
// two ways. It was the worse defect: neither reaches the check as a set operation — DataFusion
// lowers `INTERSECT` to a `LeftSemi` join and `EXCEPT` to a `LeftAnti` join — and neither was
// answering wrongly. Both were failing *mid-execution* with `XX000` and a leaked DataFusion
// debug string (`CastError("Cannot cast string 'x' to value of Int32 type")`), after the query
// had already been distributed to the cores.
//
// And the shape had to be told apart from a semi/anti join the client asked for, because
// PostgreSQL refuses a mismatched `IN (subquery)` as a missing operator (`42883`) instead — a
// different code, so catching both would put the wrong one on one of them. It is
// distinguishable: at the point this check runs an `IN`/`EXISTS` subquery is still an expression
// inside a `Filter`, not a join, and an explicit `LEFT SEMI JOIN` carries its equality in the
// join's filter rather than its keys. `test_semi_and_anti_join_spellings` pins the other side.
#[tokio::test]
async fn test_intersect_and_except_branch_mismatch() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setintersect").await;

    // `l.id` int4 against `r.w` text, both operators, both orders. The message names the
    // client's own operator — not "UNION" — because that is the operator it has to fix.
    for (sql, operator) in [
        (
            format!("SELECT id FROM {l} INTERSECT SELECT w FROM {r}"),
            "INTERSECT",
        ),
        (
            format!("SELECT w FROM {r} INTERSECT SELECT id FROM {l}"),
            "INTERSECT",
        ),
        (
            format!("SELECT id FROM {l} EXCEPT SELECT w FROM {r}"),
            "EXCEPT",
        ),
    ] {
        let err = assert_sqlstate(&client, &sql, "42804").await;
        let message = err.message();
        assert!(
            message.contains(operator),
            "`{sql}` should name {operator}: {message}"
        );
        for wanted in ["int4", "text", "cannot be matched"] {
            assert!(
                message.contains(wanted),
                "`{sql}` should name `{wanted}`: {message}"
            );
        }
    }

    // A mismatch in a later column of a wide set operation is found too, and named by column.
    let sql = format!("SELECT id, k FROM {l} INTERSECT SELECT id, w FROM {r}");
    let err = assert_sqlstate(&client, &sql, "42804").await;
    assert!(
        err.message().contains("\"k\""),
        "should name the second column: {}",
        err.message()
    );

    // The cast the message asks for answers: `l.id` = 1,2,3,4 as text against 'x','y','z' is
    // empty, and the difference is the whole left side.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id::text FROM {l} INTERSECT SELECT w FROM {r}")
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id::text FROM {l} EXCEPT SELECT w FROM {r}")
        )
        .await,
        vec!["1", "2", "3", "4"]
    );

    // And the `UNKNOWN` rule reaches these two as well: a bare literal branch takes the typed
    // branch's type, so this is an integer intersection and answers.
    assert_eq!(
        sorted_column(&client, &format!("SELECT id FROM {l} INTERSECT SELECT '2'")).await,
        vec!["2"]
    );
    assert_eq!(
        sorted_column(&client, &format!("SELECT id FROM {l} EXCEPT SELECT '2'")).await,
        vec!["1", "3", "4"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// REFUSAL. `INTERSECT ALL` and `EXCEPT ALL` count multiplicity: `INTERSECT ALL` keeps a row
// as many times as it appears on *both* sides, and `EXCEPT ALL` removes one left row per
// matching right row. Both were answered as the plain semi/anti join the `DISTINCT` forms
// are built from — measured `1, 1, 1` where PostgreSQL answers `1, 1`, and `2` where
// PostgreSQL answers `1, 2` — so both are refused rather than answered, because a row count
// is exactly what an analytical client goes on to aggregate.
#[tokio::test]
async fn test_multiplicity_set_operations_are_refused() {
    let client = ready_client().await;
    let (dl, dr) = setup_dupes(&client, "jg_mult").await;

    for op in ["INTERSECT ALL", "EXCEPT ALL"] {
        let sql = format!("SELECT k FROM {dl} {op} SELECT k FROM {dr}");
        let err = assert_sqlstate(&client, &sql, SQLSTATE_FEATURE_NOT_SUPPORTED).await;
        assert!(
            err.message().contains("as sets"),
            "`{sql}` should name the form that is correct: {}",
            err.message()
        );
    }

    // The `DISTINCT` forms the message points at are correct and stay available.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {dl} INTERSECT SELECT k FROM {dr}")
        )
        .await,
        vec!["1"]
    );
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {dl} EXCEPT SELECT k FROM {dr}")
        )
        .await,
        vec!["2"]
    );
    // `UNION ALL` is untouched: concatenating is all its `ALL` asks for.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM (SELECT k FROM {dl} UNION ALL SELECT k FROM {dr}) t")
        )
        .await,
        7
    );

    drop_table(&client, &dl).await;
    drop_table(&client, &dr).await;
}

#[tokio::test]
#[ignore = "gap (row 27): INTERSECT ALL / EXCEPT ALL must count how often a row appears"]
async fn test_multiplicity_set_operations_count_duplicates() {
    let client = ready_client().await;
    let (dl, dr) = setup_dupes(&client, "jg_mult_gap").await;

    // `dl.k` = 1, 1, 1, 2 and `dr.k` = 1, 1, 3.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {dl} INTERSECT ALL SELECT k FROM {dr}")
        )
        .await,
        vec!["1", "1"]
    );
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT k FROM {dl} EXCEPT ALL SELECT k FROM {dr}")
        )
        .await,
        vec!["1", "2"]
    );

    drop_table(&client, &dl).await;
    drop_table(&client, &dr).await;
}

// A set operation is a query, so it goes wherever a query goes: inside a derived table, a
// CTE, or one side of a join. Each puts a stage above the operation, which is where a plan
// that only works at the top level breaks.
#[tokio::test]
async fn test_set_operations_nested_in_a_query() {
    let client = ready_client().await;
    let (l, r) = setup_pair(&client, "jg_setnest").await;

    // Five distinct ids across the two tables.
    assert_eq!(
        count(
            &client,
            &format!("SELECT COUNT(*) FROM (SELECT id FROM {l} UNION SELECT id FROM {r}) t")
        )
        .await,
        5
    );
    assert_eq!(
        count(
            &client,
            &format!(
                "WITH u AS (SELECT id FROM {l} UNION SELECT id FROM {r}) SELECT COUNT(*) FROM u"
            )
        )
        .await,
        5
    );
    // A union on one side of a join.
    assert_eq!(
        column(
            &client,
            &format!(
                "SELECT u.id FROM (SELECT id FROM {l} UNION SELECT id FROM {r}) u \
                 JOIN {r} r ON u.id = r.id AND r.w = 'x' ORDER BY u.id"
            )
        )
        .await,
        vec!["2"]
    );
    // Branches that are themselves aggregates, so each side is a two-stage plan.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT COUNT(*) FROM {l} UNION ALL SELECT COUNT(*) + 1 FROM {l}")
        )
        .await,
        vec!["4", "5"]
    );
    // A `VALUES` branch, which reaches no shard at all.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id FROM {l} WHERE id = 1 UNION ALL VALUES (99)")
        )
        .await,
        vec!["1", "99"]
    );
    // A branch that is a bare NULL, which has to be typed from the other branch.
    assert_eq!(
        sorted_column(
            &client,
            &format!("SELECT id FROM {l} WHERE id = 1 UNION ALL SELECT NULL")
        )
        .await,
        vec!["1", "NULL"]
    );

    drop_table(&client, &l).await;
    drop_table(&client, &r).await;
}

// ============================================================================
// 7. Joining over a pseudonymized column
// ============================================================================

// A column in `anonymized_columns` stores the HMAC-SHA256 digest of its plaintext, not the
// plaintext. Equality survives that — equal values have equal digests — so an equi-join on
// such a column is the documented way to relate two tables by it, and it works. Every read
// that would report a property the hash does **not** preserve is refused instead of
// answered: an ordering join, an `ORDER BY`, and a comparison against plaintext, which
// would match nothing and say so nowhere.
#[tokio::test]
async fn test_join_on_an_anonymized_column() {
    let client = ready_client().await;

    let secret_id = unique_table_name("jg_secret");
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', 'a_join_gap_secret_key_long_enough')"
        ),
    )
    .await
    .unwrap();

    let anon_opts = format!(
        "WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
         anonymized_columns = [ email -> '{secret_id}' ])"
    );
    let users = create_table(
        &client,
        "jg_anon_users",
        &format!("(id INTEGER NOT NULL, email VARCHAR(64)) {anon_opts}"),
    )
    .await;
    let logins = create_table(
        &client,
        "jg_anon_logins",
        &format!("(id INTEGER NOT NULL, email VARCHAR(64), n INTEGER) {anon_opts}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {users} (id, email) VALUES (1, 'a@x.com'), (2, 'b@x.com')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!(
            "INSERT INTO {logins} (id, email, n) VALUES \
             (10, 'a@x.com', 5), (11, 'a@x.com', 7), (12, 'c@x.com', 1)"
        ),
    )
    .await
    .unwrap();

    // The digests match for `a@x.com`, so the join relates the two tables and the sum is
    // over the right rows. `b@x.com` has no login and `c@x.com` no user.
    assert_eq!(
        pairs(
            &client,
            &format!(
                "SELECT u.id, SUM(g.n) FROM {users} u JOIN {logins} g ON u.email = g.email \
                 GROUP BY u.id ORDER BY u.id"
            )
        )
        .await,
        vec![("1".into(), "12".into())]
    );

    // An ordering join would order by the digests, which say nothing about the plaintext.
    assert_unsupported(
        &client,
        &format!("SELECT COUNT(*) FROM {users} u JOIN {logins} g ON u.email < g.email"),
    )
    .await;
    assert_unsupported(&client, &format!("SELECT id FROM {users} ORDER BY email")).await;
    // Comparing against the plaintext would match no row.
    assert_unsupported(
        &client,
        &format!("SELECT id FROM {users} WHERE email = 'a@x.com'"),
    )
    .await;

    drop_table(&client, &users).await;
    drop_table(&client, &logins).await;
}
