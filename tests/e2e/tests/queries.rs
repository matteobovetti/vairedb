mod common;
use common::*;

// Single-table read path: projection, filtering, ordering, limit/offset,
// distinct, and aggregates (scalar + GROUP BY / HAVING). Every table uses 3
// shards, so each query exercises the distributed read path (per-shard scan +
// UnionExec + top-level DataFusion operator) and the assertions are computed
// from the inserted data, independent of which node a row lands on.
//
// Multi-table joins, subqueries, set operations, window functions, CTEs, and
// NULL ordering live in `distributed_queries.rs`.

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
