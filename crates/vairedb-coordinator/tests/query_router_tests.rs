use vairedb_coordinator::query_router::{
    QueryType, classify_statement, extract_select_table_name, extract_table_name,
};
use vairedb_coordinator::sql_compat;

#[test]
fn test_classify_select() {
    let stmts = sql_compat::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Select);
}

#[test]
fn test_classify_insert() {
    let stmts = sql_compat::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Insert);
}

#[test]
fn test_classify_update() {
    let stmts = sql_compat::parse_sql("UPDATE orders SET amount = 10 WHERE id = 1").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Update);
}

#[test]
fn test_classify_delete() {
    let stmts = sql_compat::parse_sql("DELETE FROM orders WHERE id = 1").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Delete);
}

#[test]
fn test_classify_create_table() {
    let stmts = sql_compat::parse_sql("CREATE TABLE t (id INT)").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::CreateTable);
}

#[test]
fn test_classify_alter_table() {
    let stmts = sql_compat::parse_sql("ALTER TABLE t ADD COLUMN x INT").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::AlterTable);
}

#[test]
fn test_classify_drop_table() {
    let stmts = sql_compat::parse_sql("DROP TABLE orders").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::DropTable);
}

#[test]
fn test_classify_other() {
    let stmts = sql_compat::parse_sql("EXPLAIN SELECT 1").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Other);
}

#[test]
fn test_extract_table_name_insert() {
    let stmts = sql_compat::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("orders".to_string()));
}

#[test]
fn test_extract_table_name_update() {
    let stmts = sql_compat::parse_sql("UPDATE users SET name = 'x' WHERE id = 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("users".to_string()));
}

#[test]
fn test_extract_table_name_delete() {
    let stmts = sql_compat::parse_sql("DELETE FROM events WHERE id = 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("events".to_string()));
}

#[test]
fn test_extract_table_name_create() {
    let stmts = sql_compat::parse_sql("CREATE TABLE metrics (id INT)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("metrics".to_string()));
}

#[test]
fn test_extract_table_name_drop() {
    let stmts = sql_compat::parse_sql("DROP TABLE old_data").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("old_data".to_string()));
}

#[test]
fn test_extract_table_name_select_returns_none() {
    let stmts = sql_compat::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_table_name_other_returns_none() {
    let stmts = sql_compat::parse_sql("EXPLAIN SELECT 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_table_name_schema_qualified() {
    let stmts = sql_compat::parse_sql("INSERT INTO myschema.orders (id) VALUES (1)").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

#[test]
fn test_extract_table_name_drop_multiple() {
    let stmts = sql_compat::parse_sql("DROP TABLE t1, t2").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("t1".to_string()));
}

// --- extract_select_table_name tests ---

#[test]
fn test_extract_select_table_name_basic() {
    let stmts = sql_compat::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_with_alias() {
    let stmts = sql_compat::parse_sql("SELECT o.id FROM orders AS o").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_schema_qualified() {
    let stmts = sql_compat::parse_sql("SELECT * FROM myschema.orders").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_non_select_returns_none() {
    let stmts = sql_compat::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_no_from_returns_none() {
    let stmts = sql_compat::parse_sql("SELECT 1").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_subquery_in_from_returns_none() {
    let stmts = sql_compat::parse_sql("SELECT * FROM (SELECT 1 AS x) AS sub").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_union_returns_none() {
    let stmts = sql_compat::parse_sql("SELECT 1 UNION SELECT 2").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_table_name_alter_table() {
    let stmts = sql_compat::parse_sql("ALTER TABLE orders ADD COLUMN status VARCHAR").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("orders".to_string()));
}

#[test]
fn test_extract_table_name_alter_table_schema_qualified() {
    let stmts = sql_compat::parse_sql("ALTER TABLE myschema.orders ADD COLUMN x INT").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}
