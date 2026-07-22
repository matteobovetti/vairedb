mod common;
use common::*;
use tokio_postgres::Client;

// Query operators the coordinator must combine on top of unioned per-shard
// scans: joins (self, inner, left outer), subqueries, set operations
// (UNION/INTERSECT/EXCEPT), window functions, CTEs, and NULL ordering. All data
// spans multiple shards so a single-shard shortcut cannot produce the right
// answer.
//
// Single-table projection/filter/aggregate coverage lives in `queries.rs`.

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
