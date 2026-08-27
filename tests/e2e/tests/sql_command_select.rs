mod common;
use common::*;
use tokio_postgres::Client;

// SELECT — row 1 of docs/specs/gap-analysis-command.md (✅ supported, "full read
// path"). One of four `sql_command_*` files that together make the command gap
// doc executable:
//
//   * `sql_command_select.rs`      — row 1 (SELECT)
//   * `sql_command_dml.rs`         — rows 2-4 (INSERT/UPDATE/DELETE) + row 25 (MERGE)
//   * `sql_command_ddl.rs`         — rows 5-7 (CREATE TABLE ✅, ALTER TABLE 🟡, DROP 🟡)
//   * `sql_command_unsupported.rs` — rows 8-36 (❌)
//
// Suite convention, shared by all four:
//   * plain `#[tokio::test]` — the command works end to end today; the test
//     guards against regressions.
//   * `#[tokio::test]` + `#[ignore = "gap (row N): ..."]` — asserts the
//     PostgreSQL-correct target state for a 🟡/❌ row, so it fails by
//     construction. It is the specification of where VaireDB is going, not a
//     description of today's behavior. Un-#[ignore] each one as its gap closes.
//
// `make e2e` runs only the passing set. To see the current gap map:
//
//     cd tests/e2e && cargo test --test sql_command_select -- --ignored --test-threads=1
//
// This file covers the read statement's operator surface: projection, filtering,
// ordering, LIMIT/OFFSET, DISTINCT, aggregates, joins, subqueries, set
// operations, window functions, CTEs, and NULL ordering. Every table uses 3
// shards, so each query exercises the distributed read path (per-shard scan +
// UnionExec + a top-level DataFusion operator) and the assertions are computed
// from the inserted data, independent of which node a row lands on.
//
// Because the doc classifies SELECT ✅ with no sub-restrictions, this file is
// all regression guard — there is no statement-level SELECT gap to xfail. The
// neighbouring axes have their own files:
//   * data types in a projection -> `data_types_round_trips.rs`, the executable
//     counterpart of docs/specs/gap-analysis-data-type.md;
//   * PG -> DataFusion/DuckDB expression translation (TO_CHAR, EXTRACT, ILIKE,
//     `||`, date_trunc) -> `data_types_dialect_gaps.rs`;
//   * shard-predicate routing (single-shard vs broadcast) -> `shard_routing.rs`;
//   * prepared statements / bind parameters -> `extended_protocol.rs`.

/// Table with a NULL in `amt` and three categories spread across shards — the
/// shared fixture for set operations, window functions, CTEs and NULL ordering.
async fn setup_op_table(client: &Client, tbl: &str) {
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        client,
        &format!(
            "CREATE TABLE {tbl} (id INTEGER NOT NULL, cat VARCHAR, amt INTEGER) {CREATE_OPTS}"
        ),
    )
    .await
    .unwrap();
    execute(
        client,
        &format!(
            "INSERT INTO {tbl} (id, cat, amt) VALUES \
             (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50),(6,'a',NULL)"
        ),
    )
    .await
    .unwrap();
}

fn ints(rows: &[Vec<Option<String>>], col: usize) -> Vec<i64> {
    rows.iter()
        .map(|r| r[col].as_deref().unwrap().parse().unwrap())
        .collect()
}

fn strings(rows: &[Vec<Option<String>>], col: usize) -> Vec<String> {
    rows.iter()
        .map(|r| r[col].as_deref().unwrap().to_string())
        .collect()
}

// ============================================================================
// Projection, filtering, ordering, LIMIT/OFFSET, DISTINCT (single table)
// ============================================================================

#[tokio::test]
async fn test_select_all() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_all",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id, name FROM {tbl} ORDER BY id"))
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("alice"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));
    assert_eq!(rows[2][0].as_deref(), Some("3"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_select_with_where() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_where",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT name FROM {tbl} WHERE id = 2"))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("bob"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_select_with_limit() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_limit",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} ORDER BY id LIMIT 2"),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_limit_offset() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_limit_offset",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} ORDER BY id LIMIT 3 OFFSET 4"),
    )
    .await
    .unwrap();

    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![5, 6, 7]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_order_by_desc() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_order_desc",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, name) VALUES \
             (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id DESC"))
        .await
        .unwrap();

    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![7, 6, 5, 4, 3, 2, 1]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_order_by_multi_key() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_order_multi",
        &format!(
            "(id INTEGER NOT NULL, category VARCHAR NOT NULL, amount INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category, amount) VALUES \
             (1, 'a', 30), (2, 'a', 10), (3, 'b', 20), (4, 'b', 40), (5, 'a', 20), (6, 'c', 5)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT category, amount FROM {tbl} ORDER BY category ASC, amount DESC"),
    )
    .await
    .unwrap();

    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().to_string(),
                r[1].as_deref().unwrap().parse().unwrap(),
            )
        })
        .collect();
    let expected = vec![
        ("a".to_string(), 30),
        ("a".to_string(), 20),
        ("a".to_string(), 10),
        ("b".to_string(), 40),
        ("b".to_string(), 20),
        ("c".to_string(), 5),
    ];
    assert_eq!(got, expected);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_where_range_scan() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_range",
        &format!("(id INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE id BETWEEN 4 AND 7 ORDER BY id"),
    )
    .await
    .unwrap();

    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![4, 5, 6, 7]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_distinct() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_distinct",
        &format!("(id INTEGER NOT NULL, category VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    // Same category repeated across rows that hash to different shards.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category) VALUES \
             (1, 'x'), (2, 'y'), (3, 'x'), (4, 'z'), (5, 'y'), (6, 'x'), (7, 'z')"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT DISTINCT category FROM {tbl} ORDER BY category"),
    )
    .await
    .unwrap();
    let got: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().to_string())
        .collect();
    assert_eq!(got, vec!["x", "y", "z"]);

    let rows = simple_query_rows(
        &client,
        &format!("SELECT COUNT(DISTINCT category) FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("3"));

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Aggregates: scalar, GROUP BY, HAVING
// ============================================================================

#[tokio::test]
async fn test_count() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_count",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, name) VALUES \
             (1, 'alice'), (2, 'bob'), (3, NULL), (4, 'dave'), (5, NULL)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("5"));

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(name) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("3"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_scalar_aggregates() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_scalar_agg",
        &format!("(id INTEGER NOT NULL, amount INTEGER NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, amount) VALUES \
             (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("150"));
    let avg: f64 = rows[0][1].as_deref().unwrap().parse().unwrap();
    assert!((avg - 30.0).abs() < 0.001);
    assert_eq!(rows[0][2].as_deref(), Some("10"));
    assert_eq!(rows[0][3].as_deref(), Some("50"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_group_by_multi_agg() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_multi_agg",
        &format!(
            "(id INTEGER NOT NULL, category VARCHAR NOT NULL, amount INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category, amount) VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'a', 50), (6, 'c', 60)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT category, SUM(amount), COUNT(*), MIN(amount), MAX(amount), AVG(amount) \
             FROM {tbl} GROUP BY category ORDER BY category"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 3);

    // category 'a': 10, 30, 50
    assert_eq!(rows[0][0].as_deref(), Some("a"));
    assert_eq!(rows[0][1].as_deref(), Some("90"));
    assert_eq!(rows[0][2].as_deref(), Some("3"));
    assert_eq!(rows[0][3].as_deref(), Some("10"));
    assert_eq!(rows[0][4].as_deref(), Some("50"));
    let avg_a: f64 = rows[0][5].as_deref().unwrap().parse().unwrap();
    assert!((avg_a - 30.0).abs() < 0.001);

    // category 'b': 20, 40
    assert_eq!(rows[1][0].as_deref(), Some("b"));
    assert_eq!(rows[1][1].as_deref(), Some("60"));
    assert_eq!(rows[1][2].as_deref(), Some("2"));
    assert_eq!(rows[1][3].as_deref(), Some("20"));
    assert_eq!(rows[1][4].as_deref(), Some("40"));

    // category 'c': 60
    assert_eq!(rows[2][0].as_deref(), Some("c"));
    assert_eq!(rows[2][1].as_deref(), Some("60"));
    assert_eq!(rows[2][2].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_group_by_having() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "q_having",
        &format!(
            "(id INTEGER NOT NULL, category VARCHAR NOT NULL, amount INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category, amount) VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'c', 50)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT category, COUNT(*) FROM {tbl} \
             GROUP BY category HAVING COUNT(*) > 1 ORDER BY category"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("a"));
    assert_eq!(rows[0][1].as_deref(), Some("2"));
    assert_eq!(rows[1][0].as_deref(), Some("b"));
    assert_eq!(rows[1][1].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Joins — the coordinator must combine unioned per-shard scans, so no
// single-shard shortcut can produce the right answer
// ============================================================================

#[tokio::test]
async fn test_self_join() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dq_self_join",
        &format!("(id INTEGER NOT NULL, category VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category) VALUES \
             (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c')"
        ),
    )
    .await
    .unwrap();

    // Count pairs (a.id, b.id) sharing a category with a.id < b.id.
    // category a: (1,2) -> 1 pair; b: (3,4) -> 1 pair; c: none. Total 2.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT COUNT(*) FROM {tbl} a JOIN {tbl} b \
             ON a.category = b.category AND a.id < b.id"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("2"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_two_table_join() {
    let client = ready_client().await;
    let users = create_table(
        &client,
        "dq_join_users",
        &format!("(id INTEGER NOT NULL, name VARCHAR NOT NULL) {CREATE_OPTS}"),
    )
    .await;
    let orders = create_table(
        &client,
        "dq_join_orders",
        &format!(
            "(id INTEGER NOT NULL, user_id INTEGER NOT NULL, total INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {users} (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!(
            "INSERT INTO {orders} (id, user_id, total) VALUES \
             (10, 1, 100), (11, 1, 200), (12, 2, 50), (13, 3, 75)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT u.name, SUM(o.total) FROM {users} u JOIN {orders} o \
             ON u.id = o.user_id GROUP BY u.name ORDER BY u.name"
        ),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].as_deref(), Some("alice"));
    assert_eq!(rows[0][1].as_deref(), Some("300"));
    assert_eq!(rows[1][0].as_deref(), Some("bob"));
    assert_eq!(rows[1][1].as_deref(), Some("50"));
    assert_eq!(rows[2][0].as_deref(), Some("carol"));
    assert_eq!(rows[2][1].as_deref(), Some("75"));

    drop_table(&client, &users).await;
    drop_table(&client, &orders).await;
}

#[tokio::test]
async fn test_left_outer_join() {
    let client = ready_client().await;
    let users = create_table(
        &client,
        "dq_lj_users",
        &format!("(id INTEGER NOT NULL, name VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let orders = create_table(
        &client,
        "dq_lj_orders",
        &format!("(id INTEGER NOT NULL, uid INTEGER, tot INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {users} (id, name) VALUES (1,'alice'),(2,'bob'),(3,'carol')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {orders} (id, uid, tot) VALUES (10,1,100),(11,1,200),(12,2,50)"),
    )
    .await
    .unwrap();

    // carol (id 3) has no orders -> one row with NULL total preserved.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT u.name, o.tot FROM {users} u LEFT JOIN {orders} o \
             ON u.id = o.uid ORDER BY u.name, o.tot"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 4, "every left row must appear at least once");
    let names = strings(&rows, 0);
    assert_eq!(names, vec!["alice", "alice", "bob", "carol"]);
    assert_eq!(
        rows[3][1], None,
        "carol has no matching order, so tot must be NULL"
    );

    drop_table(&client, &users).await;
    drop_table(&client, &orders).await;
}

// ============================================================================
// Subqueries, set operations, CTEs
// ============================================================================

#[tokio::test]
async fn test_subquery_in_where() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dq_subquery",
        &format!(
            "(id INTEGER NOT NULL, category VARCHAR NOT NULL, amount INTEGER NOT NULL) {CREATE_OPTS}"
        ),
    )
    .await;

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, category, amount) VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40), (5, 'b', 50)"
        ),
    )
    .await
    .unwrap();

    // Rows whose id is in the set of ids with amount >= 30: ids 3, 4, 5.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT id FROM {tbl} WHERE id IN \
             (SELECT id FROM {tbl} WHERE amount >= 30) ORDER BY id"
        ),
    )
    .await
    .unwrap();

    assert_eq!(ints(&rows, 0), vec![3, 4, 5]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_union_all() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_union_all");
    setup_op_table(&client, &tbl).await;

    // UNION ALL keeps duplicates: ids 1,2 emitted twice each.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT id FROM {tbl} WHERE id <= 2 \
             UNION ALL SELECT id FROM {tbl} WHERE id <= 2 ORDER BY id"
        ),
    )
    .await
    .unwrap();
    assert_eq!(ints(&rows, 0), vec![1, 1, 2, 2]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_union_distinct() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_union");
    setup_op_table(&client, &tbl).await;

    // UNION dedups across the two halves: categories a,b,c.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT cat FROM {tbl} WHERE id <= 3 \
             UNION SELECT cat FROM {tbl} WHERE id >= 3 ORDER BY cat"
        ),
    )
    .await
    .unwrap();
    assert_eq!(strings(&rows, 0), vec!["a", "b", "c"]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_intersect() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_intersect");
    setup_op_table(&client, &tbl).await;

    // {a,b} ∩ {b,c,a} (id>=3 has b,b,c,a) = {a,b}.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT cat FROM {tbl} WHERE id <= 3 \
             INTERSECT SELECT cat FROM {tbl} WHERE id >= 3 ORDER BY cat"
        ),
    )
    .await
    .unwrap();
    assert_eq!(strings(&rows, 0), vec!["a", "b"]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_except() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_except");
    setup_op_table(&client, &tbl).await;

    // {a,b} (id<=3) minus {c} (id=5) = {a,b}.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT cat FROM {tbl} WHERE id <= 3 \
             EXCEPT SELECT cat FROM {tbl} WHERE id = 5 ORDER BY cat"
        ),
    )
    .await
    .unwrap();
    assert_eq!(strings(&rows, 0), vec!["a", "b"]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_cte_simple() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_cte");
    setup_op_table(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("WITH c AS (SELECT id FROM {tbl} WHERE amt >= 30) SELECT id FROM c ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(ints(&rows, 0), vec![3, 4, 5]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// Window functions
// ============================================================================

#[tokio::test]
async fn test_window_row_number() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_rownum");
    setup_op_table(&client, &tbl).await;

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM {tbl} ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(ints(&rows, 0), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(ints(&rows, 1), vec![1, 2, 3, 4, 5, 6]);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_window_sum_partition() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_winsum");
    setup_op_table(&client, &tbl).await;

    // SUM(amt) per category (NULL ignored): a=30 (10+20+NULL), b=70, c=50.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id, SUM(amt) OVER (PARTITION BY cat) FROM {tbl} ORDER BY id"),
    )
    .await
    .unwrap();
    let sums = ints(&rows, 1);
    assert_eq!(sums, vec![30, 30, 70, 70, 50, 30]);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// NULL ordering
// ============================================================================

#[tokio::test]
async fn test_order_by_nulls_ordering() {
    let client = ready_client().await;
    let tbl = unique_table_name("dq_nulls");
    setup_op_table(&client, &tbl).await;

    // id 6 has amt NULL. NULLS FIRST puts it before the numeric values.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} ORDER BY amt NULLS FIRST"),
    )
    .await
    .unwrap();
    assert_eq!(ints(&rows, 0), vec![6, 1, 2, 3, 4, 5]);

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} ORDER BY amt NULLS LAST"),
    )
    .await
    .unwrap();
    assert_eq!(ints(&rows, 0), vec![1, 2, 3, 4, 5, 6]);

    drop_table(&client, &tbl).await;
}
