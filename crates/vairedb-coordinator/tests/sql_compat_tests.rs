use vairedb_coordinator::sql_compat::{
    extract_insert_row_shard_keys, extract_shard_key_value, parse_sql, rewrite_to_shard_local,
    split_insert_by_rows, statement_to_sql, transform_to_duckdb,
};

#[test]
fn test_parse_sql_single_statement() {
    let stmts = parse_sql("SELECT 1").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_parse_sql_multiple_statements() {
    let stmts = parse_sql("SELECT 1; SELECT 2").unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_parse_sql_invalid_syntax() {
    let result = parse_sql("NOT VALID SQL ???");
    assert!(result.is_err());
}

#[test]
fn test_shard_rewrite_insert() {
    let sql = "INSERT INTO orders (id, amount) VALUES (1, 100)";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard0");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard0"));
}

#[test]
fn test_shard_rewrite_select() {
    let sql = "SELECT * FROM orders WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard2");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard2"));
}

#[test]
fn test_shard_rewrite_update() {
    let sql = "UPDATE orders SET amount = 10 WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard1");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard1"));
}

#[test]
fn test_shard_rewrite_delete() {
    let sql = "DELETE FROM orders WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard3");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard3"));
}

#[test]
fn test_shard_rewrite_create_table() {
    let sql = "CREATE TABLE orders (id INT)";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard0");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard0"));
}

#[test]
fn test_transform_bytea_to_blob() {
    let sql = "CREATE TABLE t (data BYTEA)";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("BLOB"));
    assert!(!result.contains("BYTEA"));
}

#[test]
fn test_transform_jsonb_to_json() {
    let sql = "CREATE TABLE t (payload JSONB)";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("JSON"));
    assert!(!result.contains("JSONB"));
}

#[test]
fn test_transform_preserves_other_types() {
    let sql = "CREATE TABLE t (id INT, name VARCHAR)";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("INT"));
    assert!(result.contains("VARCHAR"));
}

#[test]
fn test_transform_to_char_becomes_strftime() {
    let sql = "UPDATE t SET col = TO_CHAR(ts, 'YYYY-MM-DD') WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"));
    assert!(!result.contains("TO_CHAR"));
}

#[test]
fn test_statement_to_sql_roundtrip() {
    let sql = "SELECT id, name FROM users WHERE active = true";
    let stmts = parse_sql(sql).unwrap();
    let output = statement_to_sql(&stmts[0]);
    assert!(output.contains("id"));
    assert!(output.contains("name"));
    assert!(output.contains("users"));
}

#[test]
fn test_extract_shard_key_from_insert() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (42, 100)";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("42".to_string()));
}

#[test]
fn test_extract_shard_key_from_insert_second_column() {
    let sql = "INSERT INTO orders (id, customer_id) VALUES (1, 99)";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("99".to_string()));
}

#[test]
fn test_extract_shard_key_from_insert_missing_column() {
    let sql = "INSERT INTO orders (id, amount) VALUES (1, 100)";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, None);
}

#[test]
fn test_extract_shard_key_from_update_where() {
    let sql = "UPDATE orders SET amount = 200 WHERE customer_id = 42";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("42".to_string()));
}

#[test]
fn test_extract_shard_key_from_delete_where() {
    let sql = "DELETE FROM orders WHERE customer_id = 42";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("42".to_string()));
}

#[test]
fn test_extract_shard_key_from_compound_where() {
    let sql = "DELETE FROM orders WHERE status = 'closed' AND customer_id = 7";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("7".to_string()));
}

#[test]
fn test_extract_shard_key_from_select_returns_none() {
    let sql = "SELECT * FROM orders WHERE customer_id = 42";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, None);
}

#[test]
fn test_extract_shard_key_from_delete_no_where() {
    let sql = "DELETE FROM orders";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, None);
}

#[test]
fn test_extract_equality_right_side() {
    let sql = "UPDATE orders SET amount = 0 WHERE 42 = customer_id";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("42".to_string()));
}

#[test]
fn test_transform_strips_with_clause() {
    let sql = "CREATE TABLE orders (id INTEGER NOT NULL, name VARCHAR(255)) WITH (shards = 3, replication_factor = 2, shard_by = 'HASH(id)')";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(!result.contains("WITH"));
    assert!(!result.contains("shards"));
    assert!(result.contains("CREATE TABLE"));
    assert!(result.contains("orders"));
}

// --- extract_insert_row_shard_keys tests ---

#[test]
fn test_extract_insert_row_shard_keys_single_row() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (42, 100)";
    let stmts = parse_sql(sql).unwrap();
    let result = extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]);
    assert_eq!(result, Some(vec![(0, "42".to_string())]));
}

#[test]
fn test_extract_insert_row_shard_keys_multiple_rows() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (10, 100), (20, 200), (30, 300)";
    let stmts = parse_sql(sql).unwrap();
    let result = extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]);
    assert_eq!(
        result,
        Some(vec![
            (0, "10".to_string()),
            (1, "20".to_string()),
            (2, "30".to_string()),
        ])
    );
}

#[test]
fn test_extract_insert_row_shard_keys_missing_column() {
    let sql = "INSERT INTO orders (id, amount) VALUES (1, 100)";
    let stmts = parse_sql(sql).unwrap();
    let result = extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]);
    assert_eq!(result, None);
}

#[test]
fn test_extract_insert_row_shard_keys_non_insert() {
    let sql = "SELECT * FROM orders";
    let stmts = parse_sql(sql).unwrap();
    let result = extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]);
    assert_eq!(result, None);
}

// --- split_insert_by_rows tests ---

#[test]
fn test_split_insert_by_rows_select_subset() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (10, 100), (20, 200), (30, 300)";
    let stmts = parse_sql(sql).unwrap();
    let split = split_insert_by_rows(&stmts[0], &[0, 2]).unwrap();
    let result = statement_to_sql(&split);
    assert!(result.contains("10"));
    assert!(result.contains("30"));
    assert!(!result.contains("20"));
}

#[test]
fn test_split_insert_by_rows_single_row() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (10, 100), (20, 200), (30, 300)";
    let stmts = parse_sql(sql).unwrap();
    let split = split_insert_by_rows(&stmts[0], &[1]).unwrap();
    let result = statement_to_sql(&split);
    assert!(result.contains("20"));
    assert!(result.contains("200"));
    assert!(!result.contains("10"));
    assert!(!result.contains("30"));
}

#[test]
fn test_split_insert_by_rows_empty_indices() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (10, 100)";
    let stmts = parse_sql(sql).unwrap();
    let result = split_insert_by_rows(&stmts[0], &[]);
    assert_eq!(result, None);
}

#[test]
fn test_split_insert_by_rows_out_of_bounds_indices() {
    let sql = "INSERT INTO orders (customer_id, amount) VALUES (10, 100)";
    let stmts = parse_sql(sql).unwrap();
    let result = split_insert_by_rows(&stmts[0], &[5, 10]);
    assert_eq!(result, None);
}

#[test]
fn test_split_insert_by_rows_non_insert() {
    let sql = "SELECT * FROM orders";
    let stmts = parse_sql(sql).unwrap();
    let result = split_insert_by_rows(&stmts[0], &[0]);
    assert_eq!(result, None);
}

// --- Additional transform_to_duckdb coverage ---

#[test]
fn test_transform_to_char_in_insert_values() {
    let sql = "INSERT INTO logs (ts) VALUES (TO_CHAR(NOW(), 'YYYY-MM-DD'))";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"));
    assert!(!result.contains("TO_CHAR"));
}

#[test]
fn test_transform_does_not_alter_select() {
    let sql = "SELECT TO_CHAR(ts, 'YYYY-MM-DD') FROM logs";
    let mut stmts = parse_sql(sql).unwrap();
    let before = statement_to_sql(&stmts[0]);
    transform_to_duckdb(&mut stmts[0]);
    let after = statement_to_sql(&stmts[0]);
    assert_eq!(before, after);
}

// --- Additional extract_shard_key_value edge cases ---

#[test]
fn test_extract_shard_key_update_without_where() {
    let sql = "UPDATE orders SET amount = 0";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, None);
}

#[test]
fn test_extract_shard_key_where_inequality_returns_none() {
    let sql = "DELETE FROM orders WHERE customer_id > 42";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, None);
}

#[test]
fn test_extract_shard_key_compound_where_first_match() {
    let sql = "UPDATE orders SET amount = 0 WHERE customer_id = 5 AND status = 'active'";
    let stmts = parse_sql(sql).unwrap();
    let val = extract_shard_key_value(&stmts[0], "customer_id", &[]);
    assert_eq!(val, Some("5".to_string()));
}

// --- ALTER TABLE transform_to_duckdb tests ---

#[test]
fn test_shard_rewrite_alter_table() {
    let sql = "ALTER TABLE orders ADD COLUMN status VARCHAR";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard0");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard0"));
    assert!(result.contains("ADD COLUMN"));
}

#[test]
fn test_transform_alter_table_add_column_bytea() {
    let sql = "ALTER TABLE t ADD COLUMN data BYTEA";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("BLOB"));
    assert!(!result.contains("BYTEA"));
}

#[test]
fn test_transform_alter_table_add_column_jsonb() {
    let sql = "ALTER TABLE t ADD COLUMN payload JSONB";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("JSON"));
    assert!(!result.contains("JSONB"));
}

#[test]
fn test_transform_alter_table_alter_column_type_bytea() {
    let sql = "ALTER TABLE t ALTER COLUMN data SET DATA TYPE BYTEA";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("BLOB"));
    assert!(!result.contains("BYTEA"));
}

#[test]
fn test_transform_alter_table_preserves_other_types() {
    let sql = "ALTER TABLE t ADD COLUMN name VARCHAR";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("VARCHAR"));
}

#[test]
fn test_shard_rewrite_alter_table_drop_column() {
    let sql = "ALTER TABLE orders DROP COLUMN status";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard2");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard2"));
    assert!(result.contains("DROP COLUMN"));
}

#[test]
fn test_shard_rewrite_alter_table_rename_column() {
    let sql = "ALTER TABLE orders RENAME COLUMN old_col TO new_col";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard1");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard1"));
    assert!(result.contains("RENAME COLUMN"));
}
