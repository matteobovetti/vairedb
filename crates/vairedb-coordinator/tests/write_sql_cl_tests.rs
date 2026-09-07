//! Write-path SQL translation (`write_sql_cl`). The shared parse and the
//! read-path rewrites are covered by `pgwire_parser_tests`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use vairedb_coordinator::pgwire_handler::parser::parse_sql;
use vairedb_coordinator::write_sql_cl::{
    ROWS_PER_STATEMENT, extract_insert_row_shard_keys, extract_shard_key_value,
    insert_omits_column_list, insert_source_is_query, insert_source_tables,
    insert_statements_from_batches, insert_template, insert_values_row_count,
    materialize_insert_columns, materialize_insert_columns_for_arity,
    rewrite_index_name_to_shard_local, rewrite_to_shard_local, split_insert_by_rows,
    statement_to_sql, transform_to_duckdb, validate_insert_shard_key,
};

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

// A quoted identifier must land the shard suffix INSIDE the quotes and emit a
// BARE physical name (no quote characters) so it matches shard_table_name and
// the storage node's unquoted `FROM {name}` splice.
#[test]
fn test_shard_rewrite_quoted_identifier_is_bare() {
    let sql = "INSERT INTO \"MyTable\" (id) VALUES (1)";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard0");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("MyTable_shard0"), "got: {result}");
    assert!(
        !result.contains('"'),
        "physical name must be bare (no quotes), got: {result}"
    );
}

// A schema-qualified relation folds the qualifier into one physical identifier:
// the storage node has a single flat namespace and splices the name in unquoted, so
// the name may carry neither a dot nor a quote — and `ident_schema.orders` must
// still be a different physical table from `orders`.
#[test]
fn test_shard_rewrite_schema_qualified_folds_the_schema_in() {
    let sql = "SELECT id FROM ident_schema.orders WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard2");
    let result = statement_to_sql(&stmts[0]);
    assert!(
        result.contains("ident_schema_orders_shard2"),
        "got: {result}"
    );
    assert!(
        !result.contains('.') && !result.contains('"'),
        "the physical name must be a plain identifier, got: {result}"
    );
}

// An unquoted mixed-case name is folded to lowercase (PG identifier semantics).
#[test]
fn test_shard_rewrite_unquoted_mixedcase_lowercased() {
    let sql = "INSERT INTO Orders (id) VALUES (1)";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard0");
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("orders_shard0"), "got: {result}");
    assert!(!result.contains("Orders_shard0"), "got: {result}");
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

// --- Write path: TO_CHAR format-string translation (PG template -> strftime) ---

#[test]
fn test_transform_to_char_translates_date_format() {
    let sql = "UPDATE t SET col = TO_CHAR(ts, 'YYYY-MM-DD') WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(result.contains("%Y-%m-%d"), "got: {result}");
    assert!(!result.contains("YYYY"), "got: {result}");
}

#[test]
fn test_transform_to_char_translates_datetime_format() {
    let sql = "INSERT INTO logs (v) VALUES (TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'))";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(result.contains("%Y-%m-%d %H:%M:%S"), "got: {result}");
}

// --- Write path: every expression of a write statement is reached ---

#[test]
fn test_transform_to_char_in_delete_where_clause() {
    let sql = "DELETE FROM logs WHERE day = TO_CHAR(ts, 'YYYY-MM-DD')";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(result.contains("%Y-%m-%d"), "got: {result}");
    assert!(!result.contains("TO_CHAR"), "got: {result}");
}

#[test]
fn test_transform_to_char_in_update_where_clause() {
    let sql = "UPDATE logs SET v = 'x' WHERE day = TO_CHAR(ts, 'YYYY-MM-DD')";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(!result.contains("TO_CHAR"), "got: {result}");
}

#[test]
fn test_transform_to_char_nested_inside_another_expression() {
    // The call is neither the assignment's nor the row's top-level expression;
    // a walker that only inspects the outermost node would leave it as TO_CHAR.
    let sql = "UPDATE logs SET v = UPPER(TO_CHAR(ts, 'YYYY-MM-DD')) || '!' WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(result.contains("%Y-%m-%d"), "got: {result}");
    assert!(!result.contains("TO_CHAR"), "got: {result}");
}

#[test]
fn test_transform_to_char_nested_in_insert_row() {
    let sql = "INSERT INTO logs (id, day) VALUES (1, CAST(TO_CHAR(ts, 'YYYY-MM-DD') AS VARCHAR))";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("STRFTIME"), "got: {result}");
    assert!(!result.contains("TO_CHAR"), "got: {result}");
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

// PostgreSQL array syntax (`INTEGER[]`) is valid DuckDB ARRAY/LIST syntax, so the
// transform must leave it intact — DuckDB accepts the column DDL verbatim.
#[test]
fn test_transform_preserves_array_column_type() {
    let sql = "CREATE TABLE t (id INTEGER, tags INTEGER[])";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    assert!(result.contains("INTEGER[]"), "got: {result}");
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

// --- CREATE INDEX: both names are shard-local, and PG-only options are stripped ---

// `CreateIndex.table_name` is a relation the shard rewrite reaches; `CreateIndex.name`
// is not, which is why it takes a second pass. Without it every shard on one node
// would create the same index name and all but the first would collide — N shards,
// one index.
#[test]
fn test_shard_rewrite_create_index_moves_the_index_name_too() {
    let sql = "CREATE INDEX idx_amount ON orders (amount)";
    let mut stmts = parse_sql(sql).unwrap();
    rewrite_to_shard_local(&mut stmts[0], "shard2");
    let table_only = statement_to_sql(&stmts[0]);
    assert!(table_only.contains("orders_shard2"), "got: {table_only}");
    assert!(
        table_only.contains("idx_amount ON"),
        "the relation rewrite must leave the index name alone: {table_only}"
    );

    rewrite_index_name_to_shard_local(&mut stmts[0], "shard2");
    let both = statement_to_sql(&stmts[0]);
    assert!(both.contains("idx_amount_shard2"), "got: {both}");
    assert!(both.contains("orders_shard2"), "got: {both}");
}

// The suffix must be recomputable from the logical name, because `DROP INDEX` only
// ever has the logical name to work from.
#[test]
fn test_index_name_rewrite_canonicalizes_like_every_other_name() {
    let mut stmts = parse_sql("CREATE INDEX IDX_Amount ON orders (amount)").unwrap();
    rewrite_index_name_to_shard_local(&mut stmts[0], "shard0");
    assert!(
        statement_to_sql(&stmts[0]).contains("idx_amount_shard0"),
        "got: {}",
        statement_to_sql(&stmts[0])
    );

    let mut quoted = parse_sql("CREATE INDEX \"IDX_Amount\" ON orders (amount)").unwrap();
    rewrite_index_name_to_shard_local(&mut quoted[0], "shard0");
    assert!(
        statement_to_sql(&quoted[0]).contains("IDX_Amount_shard0"),
        "got: {}",
        statement_to_sql(&quoted[0])
    );
}

// Anything but a CREATE INDEX must come through untouched: the rewrite runs on the
// whole DDL broadcast path.
#[test]
fn test_index_name_rewrite_is_a_no_op_for_other_statements() {
    let mut stmts = parse_sql("ALTER TABLE orders ADD COLUMN status VARCHAR").unwrap();
    let before = statement_to_sql(&stmts[0]);
    rewrite_index_name_to_shard_local(&mut stmts[0], "shard1");
    assert_eq!(statement_to_sql(&stmts[0]), before);
}

// DuckDB has one index type (ART) and none of PostgreSQL's index decorations.
// Every one of them describes how the index is built, not which rows it admits, so
// the statement has to reach the node without them rather than be refused. The
// exception — anything narrowing a UNIQUE index's scope — never gets this far; it
// is refused in the coordinator.
#[test]
fn test_transform_create_index_strips_postgresql_only_options() {
    let sql = "CREATE INDEX CONCURRENTLY idx ON orders USING gin (amount DESC NULLS LAST) \
               INCLUDE (id) WITH (fillfactor = 70) WHERE amount > 0";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_duckdb(&mut stmts[0]);
    let result = statement_to_sql(&stmts[0]);
    for absent in ["USING", "gin", "CONCURRENTLY", "INCLUDE", "WHERE", "DESC"] {
        assert!(
            !result.contains(absent),
            "`{absent}` must be gone: {result}"
        );
    }
    assert!(result.contains("(amount)"), "got: {result}");
}

// UNIQUE is the one thing that is not a decoration: it is the constraint the client
// asked for, so it must survive to the shards that enforce it.
#[test]
fn test_transform_create_index_keeps_unique() {
    let mut stmts = parse_sql("CREATE UNIQUE INDEX idx ON orders (id)").unwrap();
    transform_to_duckdb(&mut stmts[0]);
    assert!(
        statement_to_sql(&stmts[0]).contains("UNIQUE"),
        "got: {}",
        statement_to_sql(&stmts[0])
    );
}

// --- Positional INSERT: the implicit column list is resolved from the catalog ---

// `INSERT INTO t VALUES (…)` is PostgreSQL's default form and what many ORMs
// emit. Filling in the column list from the table's declaration order is what
// makes it routable: every later step locates the shard key by name.
#[test]
fn test_materialize_insert_columns_fills_the_declared_order() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 42, 100)").unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "customer_id", "amount"]).unwrap();

    let sql = statement_to_sql(&stmts[0]);
    assert!(
        sql.contains(r#"("id", "customer_id", "amount")"#),
        "got: {sql}"
    );
    // Routable now: the shard key is locatable by name.
    assert_eq!(
        extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]),
        Some(vec![(0, "42".to_string())])
    );
}

// A short row takes the FIRST n columns, as PostgreSQL does — the remaining
// columns fall to their defaults.
#[test]
fn test_materialize_insert_columns_short_row_takes_leading_columns() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 42)").unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "customer_id", "amount"]).unwrap();

    let sql = statement_to_sql(&stmts[0]);
    assert!(sql.contains(r#"("id", "customer_id")"#), "got: {sql}");
    assert!(!sql.contains("amount"), "got: {sql}");
}

// Every row of a multi-row positional INSERT is keyed off the same resolved list,
// so the rows still split by shard.
#[test]
fn test_materialize_insert_columns_multi_row_keeps_every_row_routable() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200)").unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "customer_id", "amount"]).unwrap();

    assert_eq!(
        extract_insert_row_shard_keys(&stmts[0], "customer_id", &[]),
        Some(vec![(0, "10".to_string()), (1, "20".to_string())])
    );
}

// The catalog stores canonical column names, so a name that kept its case through
// CREATE TABLE must be emitted quoted — unquoted, DuckDB would fold it and miss
// the column.
#[test]
fn test_materialize_insert_columns_quotes_a_case_sensitive_name() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 42)").unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "CustomerId"]).unwrap();

    let sql = statement_to_sql(&stmts[0]);
    assert!(sql.contains(r#""CustomerId""#), "got: {sql}");
    assert_eq!(
        extract_insert_row_shard_keys(&stmts[0], "CustomerId", &[]),
        Some(vec![(0, "42".to_string())])
    );
}

// More values than the table has columns cannot be mapped positionally at all;
// PostgreSQL reports it as a syntax error rather than truncating the row.
#[test]
fn test_materialize_insert_columns_rejects_too_many_values() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 42, 100)").unwrap();
    let err = materialize_insert_columns(&mut stmts[0], &["id", "customer_id"]).unwrap_err();
    assert!(
        err.contains("more expressions than target columns"),
        "got: {err}"
    );
}

// Rows of differing length have no single positional mapping.
#[test]
fn test_materialize_insert_columns_rejects_ragged_rows() {
    let mut stmts = parse_sql("INSERT INTO orders VALUES (1, 42), (2)").unwrap();
    let err = materialize_insert_columns(&mut stmts[0], &["id", "customer_id"]).unwrap_err();
    assert!(err.contains("same length"), "got: {err}");
}

// A client-written column list is authoritative: it must survive untouched, even
// when it names the columns in a different order than the table declares them.
#[test]
fn test_materialize_insert_columns_leaves_an_explicit_list_alone() {
    let sql = "INSERT INTO orders (amount, id) VALUES (100, 1)";
    let mut stmts = parse_sql(sql).unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "customer_id", "amount"]).unwrap();
    assert_eq!(statement_to_sql(&stmts[0]), sql);
}

// Nothing to map onto: a table whose catalog entry lists no columns, a non-VALUES
// source, and a non-INSERT statement all pass through unchanged, leaving the
// verdict to the shard-key check that reports a reason.
#[test]
fn test_materialize_insert_columns_is_a_noop_when_it_cannot_map() {
    for sql in [
        "INSERT INTO orders VALUES (1, 42)",
        "INSERT INTO orders SELECT * FROM other",
        "UPDATE orders SET amount = 1 WHERE id = 2",
    ] {
        let mut stmts = parse_sql(sql).unwrap();
        materialize_insert_columns(&mut stmts[0], &[]).unwrap();
        assert_eq!(statement_to_sql(&stmts[0]), sql, "changed: {sql}");
    }

    let mut stmts = parse_sql("INSERT INTO orders SELECT * FROM other").unwrap();
    materialize_insert_columns(&mut stmts[0], &["id", "customer_id"]).unwrap();
    assert!(!statement_to_sql(&stmts[0]).contains("customer_id"));
}

// The predicate that decides whether the statement has to be cloned at all.
#[test]
fn test_insert_omits_column_list_identifies_the_positional_form() {
    for (sql, want) in [
        ("INSERT INTO orders VALUES (1, 42)", true),
        ("INSERT INTO orders (id) VALUES (1)", false),
        ("UPDATE orders SET amount = 1", false),
        ("SELECT * FROM orders", false),
    ] {
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(insert_omits_column_list(&stmts[0]), want, "for: {sql}");
    }
}

// --- INSERT whose rows come from a query: classification, sources, re-emission ---

// The predicate that sends an INSERT down the materialize-then-re-emit lane
// instead of the ordinary VALUES lane. `DEFAULT VALUES` has no source at all and
// must not be mistaken for a query.
#[test]
fn test_insert_source_is_query_identifies_the_forms_that_need_materializing() {
    for (sql, want) in [
        ("INSERT INTO orders (id) SELECT id FROM staging", true),
        (
            "INSERT INTO orders (id) WITH w AS (SELECT 1 AS id) SELECT id FROM w",
            true,
        ),
        (
            "INSERT INTO orders (id) SELECT id FROM a UNION SELECT id FROM b",
            true,
        ),
        ("INSERT INTO orders (id) VALUES (1)", false),
        ("INSERT INTO orders DEFAULT VALUES", false),
        ("UPDATE orders SET amount = 1", false),
        ("SELECT * FROM orders", false),
    ] {
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(insert_source_is_query(&stmts[0]), want, "for: {sql}");
    }
}

// Inside a transaction block the buffered writes are invisible to a read, so the
// coordinator has to know *every* table the source touches — a join or a subquery
// reads its tables as much as the first `FROM` does.
#[test]
fn test_insert_source_tables_reports_every_relation_read() {
    let sql = "INSERT INTO orders (id) SELECT s.id FROM staging s \
               JOIN customers c ON c.id = s.id \
               WHERE s.id IN (SELECT id FROM pending)";
    let stmts = parse_sql(sql).unwrap();
    let mut tables = insert_source_tables(&stmts[0]);
    tables.sort();
    assert_eq!(tables, vec!["customers", "pending", "staging"]);
}

// Names are canonicalized and deduped: the caller compares them against the
// buffer's canonical table names, and a table read twice is one table.
#[test]
fn test_insert_source_tables_canonicalizes_and_dedupes() {
    let sql = r#"INSERT INTO orders (id) SELECT a.id FROM public."Staging" a JOIN STAGING b ON b.id = a.id"#;
    let stmts = parse_sql(sql).unwrap();
    // `"Staging"` keeps its case, unquoted `STAGING` folds — two distinct tables.
    assert_eq!(insert_source_tables(&stmts[0]), vec!["Staging", "staging"]);

    let stmts = parse_sql("INSERT INTO orders (id) SELECT a.id FROM staging a, staging b").unwrap();
    assert_eq!(insert_source_tables(&stmts[0]), vec!["staging"]);
}

// Nothing is read by a literal INSERT, so the transaction guard has nothing to
// check — and the target table is not a source.
#[test]
fn test_insert_source_tables_is_empty_without_a_source_query() {
    for sql in [
        "INSERT INTO orders (id) VALUES (1)",
        "INSERT INTO orders DEFAULT VALUES",
        "UPDATE orders SET amount = 1",
    ] {
        assert!(
            insert_source_tables(&parse_sql(sql).unwrap()[0]).is_empty(),
            "for: {sql}"
        );
    }
}

// An `INSERT ... SELECT` takes its arity from the source query's result schema,
// which is known before a row is fetched, so the same leading-columns mapping
// PostgreSQL applies to a positional VALUES row applies here.
#[test]
fn test_materialize_insert_columns_for_arity_fills_the_leading_columns() {
    let mut stmts = parse_sql("INSERT INTO orders SELECT id, customer_id FROM staging").unwrap();
    materialize_insert_columns_for_arity(&mut stmts[0], &["id", "customer_id", "amount"], 2)
        .unwrap();

    let sql = statement_to_sql(&stmts[0]);
    assert!(sql.contains(r#"("id", "customer_id")"#), "got: {sql}");
    assert!(!sql.contains(r#""amount""#), "got: {sql}");
}

// A source query producing more columns than the table has cannot be mapped
// positionally at all.
#[test]
fn test_materialize_insert_columns_for_arity_rejects_a_wider_source() {
    let mut stmts = parse_sql("INSERT INTO orders SELECT * FROM staging").unwrap();
    let err =
        materialize_insert_columns_for_arity(&mut stmts[0], &["id", "customer_id"], 3).unwrap_err();
    assert!(
        err.contains("more expressions than target columns"),
        "got: {err}"
    );
}

// Nothing to map onto — an explicit list, an unknown table, an empty result
// schema — leaves the statement alone and the verdict to the shard-key check.
#[test]
fn test_materialize_insert_columns_for_arity_is_a_noop_when_it_cannot_map() {
    for (sql, columns, arity) in [
        (
            "INSERT INTO orders (amount) SELECT amount FROM staging",
            &["id", "amount"][..],
            1,
        ),
        ("INSERT INTO orders SELECT * FROM staging", &[][..], 2),
        ("INSERT INTO orders SELECT * FROM staging", &["id"][..], 0),
        ("UPDATE orders SET amount = 1", &["id", "amount"][..], 1),
    ] {
        let mut stmts = parse_sql(sql).unwrap();
        materialize_insert_columns_for_arity(&mut stmts[0], columns, arity).unwrap();
        assert_eq!(statement_to_sql(&stmts[0]), sql, "changed: {sql}");
    }
}

// The whole point of re-emitting materialized rows as literals: what comes out is
// an ordinary multi-row INSERT that the rest of the write path handles unchanged —
// it passes the shard-key check, reports an exact row count, splits by shard, and
// renders as shard-local SQL. Rendering is unit-tested in `write_sql_cl::rows`;
// this pins the hand-off across modules.
#[test]
fn test_rows_re_emitted_from_a_source_query_route_like_any_other_insert() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("note", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let template = parse_sql("INSERT INTO orders (id, note) SELECT id, note FROM staging").unwrap();
    let stmts = insert_statements_from_batches(&template[0], &[batch], ROWS_PER_STATEMENT).unwrap();
    assert_eq!(stmts.len(), 1);
    let stmt = &stmts[0];

    // No longer refused for want of a hashable shard key, and the row count the
    // client is told is the number of rows actually shipped.
    validate_insert_shard_key(stmt, "id", &[]).expect("literal rows carry a shard key");
    assert_eq!(insert_values_row_count(stmt), Some(3));
    assert_eq!(
        extract_insert_row_shard_keys(stmt, "id", &[]),
        Some(vec![
            (0, "1".to_string()),
            (1, "2".to_string()),
            (2, "3".to_string()),
        ])
    );

    // Each shard receives only its own rows, as shard-local DuckDB SQL.
    let mut shard_stmt = split_insert_by_rows(stmt, &[0, 2]).expect("rows split by shard");
    rewrite_to_shard_local(&mut shard_stmt, "shard1");
    transform_to_duckdb(&mut shard_stmt);
    let sql = statement_to_sql(&shard_stmt);
    assert!(sql.contains("orders_shard1"), "got: {sql}");
    assert!(sql.contains("VALUES (1, 'a'), (3, 'c')"), "got: {sql}");
}

// The chunk size bounds one statement, not the write: every chunk has to be a
// routable INSERT of its own, and together they must account for every row.
#[test]
fn test_a_large_source_result_becomes_several_routable_statements() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from((0..2500).collect::<Vec<i32>>()))],
    )
    .unwrap();

    let template = parse_sql("INSERT INTO orders (id) SELECT id FROM staging").unwrap();
    let stmts = insert_statements_from_batches(&template[0], &[batch], ROWS_PER_STATEMENT).unwrap();

    assert_eq!(stmts.len(), 3);
    let total: usize = stmts
        .iter()
        .map(|stmt| {
            validate_insert_shard_key(stmt, "id", &[]).expect("every chunk must be routable");
            insert_values_row_count(stmt).expect("every chunk must count its rows") as usize
        })
        .sum();
    assert_eq!(total, 2500);
}

// --- Rows written into a table the statement itself created (CTAS, COPY FROM) ---

// A write with no client INSERT to start from needs one built for it: the
// template names the table and columns, and the rows replace its placeholder row.
#[test]
fn test_an_insert_template_carries_the_table_and_columns_it_was_built_for() {
    let template = insert_template("orders", &["id", "name"]).expect("a template is buildable");
    let sql = statement_to_sql(&template);
    assert!(sql.contains("\"orders\""), "got: {sql}");
    assert!(sql.contains("(\"id\", \"name\")"), "got: {sql}");
}

// Identifiers a query produced are not identifiers a client wrote: a result column
// can be named `order id` or contain a quote, and must survive into the INSERT.
#[test]
fn test_an_insert_template_quotes_identifiers_that_need_it() {
    let template = insert_template("my table", &["order id", "we\"ird"]).unwrap();
    let sql = statement_to_sql(&template);
    assert!(sql.contains("\"my table\""), "got: {sql}");
    assert!(sql.contains("\"order id\""), "got: {sql}");
    // A quote inside the name is doubled, not dropped — otherwise the statement
    // would be mis-parsed rather than rejected.
    assert!(sql.contains("\"we\"\"ird\""), "got: {sql}");
}

// Without a column list the rows would be positional against a table shape the
// caller never checked, so there is nothing safe to build.
#[test]
fn test_an_insert_template_needs_columns() {
    let reason = insert_template("orders", &[]).expect_err("no columns, no template");
    assert!(reason.contains("column list"), "got: {reason}");
}

// The whole point of the template: rows collected from a query become routable
// shard-local INSERTs for a table that had no client statement of its own.
#[test]
fn test_rows_written_through_a_template_route_like_any_other_insert() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .unwrap();

    let template = insert_template("summary", &["id", "name"]).unwrap();
    let stmts = insert_statements_from_batches(&template, &[batch], ROWS_PER_STATEMENT).unwrap();
    assert_eq!(stmts.len(), 1);

    validate_insert_shard_key(&stmts[0], "id", &[]).expect("the shard key is a literal");
    assert_eq!(insert_values_row_count(&stmts[0]), Some(2));

    let mut shard_stmt = split_insert_by_rows(&stmts[0], &[1]).expect("rows split by shard");
    rewrite_to_shard_local(&mut shard_stmt, "shard0");
    transform_to_duckdb(&mut shard_stmt);
    let sql = statement_to_sql(&shard_stmt);
    assert!(sql.contains("summary_shard0"), "got: {sql}");
    assert!(sql.contains("VALUES (2, NULL)"), "got: {sql}");
}
