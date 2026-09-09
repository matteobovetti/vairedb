use vairedb_coordinator::pgwire_handler::parser;
use vairedb_coordinator::pgwire_handler::query_router::{
    QueryType, classify_statement, extract_select_table_name, extract_table_name,
};

#[test]
fn test_classify_select() {
    let stmts = parser::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Select);
}

#[test]
fn test_classify_insert() {
    let stmts = parser::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Insert);
}

#[test]
fn test_classify_update() {
    let stmts = parser::parse_sql("UPDATE orders SET amount = 10 WHERE id = 1").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Update);
}

#[test]
fn test_classify_delete() {
    let stmts = parser::parse_sql("DELETE FROM orders WHERE id = 1").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Delete);
}

#[test]
fn test_classify_create_table() {
    let stmts = parser::parse_sql("CREATE TABLE t (id INT)").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::CreateTable);
}

#[test]
fn test_classify_alter_table() {
    let stmts = parser::parse_sql("ALTER TABLE t ADD COLUMN x INT").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::AlterTable);
}

#[test]
fn test_classify_drop_table() {
    let stmts = parser::parse_sql("DROP TABLE orders").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::DropTable);
}

// The `TABLE` keyword is optional in PostgreSQL, so both spellings must land on
// the same route — one of them classified as `Other` would be rejected as
// unsupported while the other emptied the table.
#[test]
fn test_classify_truncate_table_with_and_without_the_table_keyword() {
    for sql in ["TRUNCATE TABLE orders", "TRUNCATE orders"] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            classify_statement(&stmts[0]),
            QueryType::TruncateTable,
            "`{sql}` must classify as TRUNCATE"
        );
    }
}

#[test]
fn test_classify_create_index() {
    for sql in [
        "CREATE INDEX idx ON orders (amount)",
        "CREATE UNIQUE INDEX idx ON orders (id)",
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            classify_statement(&stmts[0]),
            QueryType::CreateIndex,
            "`{sql}` must classify as CREATE INDEX"
        );
    }
}

// `DROP INDEX`, `DROP VIEW` and `DROP SCHEMA` split off from every other `DROP`:
// each resolves through a namespace and a catalog lookup of its own. Every remaining kind must
// keep reaching the table path, which is what reports 42809 for a kind that names
// a table — a `DROP SEQUENCE t` classified as something else would report "does
// not exist" and leave the client thinking the table was gone.
#[test]
fn test_classify_drop_by_object_kind() {
    for (sql, want) in [
        ("DROP INDEX idx", QueryType::DropIndex),
        ("DROP VIEW v", QueryType::DropView),
        ("DROP TABLE t", QueryType::DropTable),
        ("DROP SCHEMA s", QueryType::DropSchema),
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            classify_statement(&stmts[0]),
            want,
            "wrong kind for `{sql}`"
        );
    }
}

// View DDL is neither a read nor a broadcast write: it is executed entirely in the
// coordinator, so it must not be prepared for DataFusion's planner — but its AST
// must still be the client's own, because the definition is what gets stored.
#[test]
fn test_view_ddl_is_not_write_path_but_wants_the_verbatim_ast() {
    for query_type in [
        QueryType::CreateView,
        QueryType::AlterView,
        QueryType::DropView,
    ] {
        assert!(
            !query_type.is_write_path(),
            "{query_type:?} reaches no shard"
        );
    }
    assert!(QueryType::CreateView.wants_verbatim_ast());
    assert!(QueryType::AlterView.wants_verbatim_ast());
}

// `CREATE VIEW`/`ALTER VIEW` name the view itself, unlike `CREATE INDEX`, which
// names the table the index is built on.
#[test]
fn test_extract_table_name_of_view_ddl_is_the_view() {
    for sql in [
        "CREATE VIEW public.Big_Orders AS SELECT 1",
        "ALTER VIEW Big_Orders AS SELECT 1",
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            extract_table_name(&stmts[0]).as_deref(),
            Some("big_orders"),
            "wrong target for `{sql}`"
        );
    }
}

// Both are DDL the write path broadcasts, not reads for the planner: classified
// as read-path they would be prepared for DataFusion and never reach a shard.
#[test]
fn test_index_ddl_is_write_path() {
    assert!(QueryType::CreateIndex.is_write_path());
    assert!(QueryType::DropIndex.is_write_path());
}

// `Other` is the fallback for a statement no subsystem claims, and it has to stay
// reachable: it is what turns an unhandled command into a refusal that names it
// rather than a fake `OK`.
#[test]
fn test_classify_other() {
    let stmts = parser::parse_sql("CALL p()").unwrap();
    assert_eq!(classify_statement(&stmts[0]), QueryType::Other);
}

// Every spelling of transaction control must reach the session handler. One of
// them falling through to `Other` would be rejected as unsupported, which is what
// the whole of this feature exists to stop.
#[test]
fn test_classify_transaction_control() {
    for sql in [
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN WORK",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "BEGIN READ ONLY",
        "START TRANSACTION",
        "COMMIT",
        "COMMIT WORK",
        "END",
        "ROLLBACK",
        "ROLLBACK TRANSACTION",
        "ABORT",
        "ROLLBACK TO SAVEPOINT sp",
        "SAVEPOINT sp",
        "RELEASE SAVEPOINT sp",
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            classify_statement(&stmts[0]),
            QueryType::TransactionControl,
            "`{sql}` must classify as transaction control"
        );
    }
}

#[test]
fn test_extract_table_name_insert() {
    let stmts = parser::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("orders".to_string()));
}

#[test]
fn test_extract_table_name_update() {
    let stmts = parser::parse_sql("UPDATE users SET name = 'x' WHERE id = 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("users".to_string()));
}

#[test]
fn test_extract_table_name_delete() {
    let stmts = parser::parse_sql("DELETE FROM events WHERE id = 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("events".to_string()));
}

#[test]
fn test_extract_table_name_create() {
    let stmts = parser::parse_sql("CREATE TABLE metrics (id INT)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("metrics".to_string()));
}

#[test]
fn test_extract_table_name_drop() {
    let stmts = parser::parse_sql("DROP TABLE old_data").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("old_data".to_string()));
}

// A `CREATE INDEX` resolves to the table the index is built on — the shard lookup
// and the error context both need that, not the index's own name.
#[test]
fn test_extract_table_name_create_index_is_the_indexed_table() {
    let stmts = parser::parse_sql("CREATE INDEX idx_amount ON myschema.orders (amount)").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

#[test]
fn test_extract_table_name_select_returns_none() {
    let stmts = parser::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_table_name_other_returns_none() {
    let stmts = parser::parse_sql("EXPLAIN SELECT 1").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), None);
}

// A schema-qualified name canonicalizes to `schema.relation`: the qualifier is part
// of the catalog key, so `myschema.orders` and `orders` are two relations.
#[test]
fn test_extract_table_name_schema_qualified() {
    let stmts = parser::parse_sql("INSERT INTO myschema.orders (id) VALUES (1)").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

// A quoted identifier keeps its case verbatim (no quote characters in the key).
#[test]
fn test_extract_table_name_quoted_preserves_case() {
    let stmts = parser::parse_sql("INSERT INTO \"MyTable\" (id) VALUES (1)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("MyTable".to_string()));
}

// An unquoted mixed-case name folds to lowercase (PG identifier semantics).
#[test]
fn test_extract_table_name_unquoted_mixedcase_lowercased() {
    let stmts = parser::parse_sql("INSERT INTO Orders (id) VALUES (1)").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("orders".to_string()));
}

#[test]
fn test_extract_table_name_drop_multiple() {
    let stmts = parser::parse_sql("DROP TABLE t1, t2").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("t1".to_string()));
}

#[test]
fn test_extract_table_name_truncate() {
    let stmts = parser::parse_sql("TRUNCATE TABLE myschema.orders").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

// The decorated forms carry the same target; losing the name here would empty
// nothing and report success.
#[test]
fn test_extract_table_name_truncate_with_inheritance_decorations() {
    for sql in ["TRUNCATE TABLE ONLY orders", "TRUNCATE TABLE orders *"] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert_eq!(
            extract_table_name(&stmts[0]),
            Some("orders".to_string()),
            "`{sql}` must resolve its target"
        );
    }
}

// --- extract_select_table_name tests ---

#[test]
fn test_extract_select_table_name_basic() {
    let stmts = parser::parse_sql("SELECT * FROM orders").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_with_alias() {
    let stmts = parser::parse_sql("SELECT o.id FROM orders AS o").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_schema_qualified() {
    let stmts = parser::parse_sql("SELECT * FROM myschema.orders").unwrap();
    assert_eq!(
        extract_select_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

#[test]
fn test_extract_select_table_name_non_select_returns_none() {
    let stmts = parser::parse_sql("INSERT INTO orders (id) VALUES (1)").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_no_from_returns_none() {
    let stmts = parser::parse_sql("SELECT 1").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_subquery_in_from_returns_none() {
    let stmts = parser::parse_sql("SELECT * FROM (SELECT 1 AS x) AS sub").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_select_table_name_union_returns_none() {
    let stmts = parser::parse_sql("SELECT 1 UNION SELECT 2").unwrap();
    assert_eq!(extract_select_table_name(&stmts[0]), None);
}

#[test]
fn test_extract_table_name_alter_table() {
    let stmts = parser::parse_sql("ALTER TABLE orders ADD COLUMN status VARCHAR").unwrap();
    assert_eq!(extract_table_name(&stmts[0]), Some("orders".to_string()));
}

#[test]
fn test_extract_table_name_alter_table_schema_qualified() {
    let stmts = parser::parse_sql("ALTER TABLE myschema.orders ADD COLUMN x INT").unwrap();
    assert_eq!(
        extract_table_name(&stmts[0]),
        Some("myschema.orders".to_string())
    );
}

// `is_write_path` decides which AST a statement gets from `parse_sql` (verbatim
// vs. pg-compat-rewritten), so a statement kind on the wrong side of it is
// prepared for a path that never runs it.
#[test]
fn test_is_write_path_covers_dml_and_ddl() {
    for sql in [
        "INSERT INTO orders (id) VALUES (1)",
        "UPDATE orders SET amount = 10 WHERE id = 1",
        "DELETE FROM orders WHERE id = 1",
        "CREATE TABLE orders (id INTEGER)",
        "ALTER TABLE orders ADD COLUMN status VARCHAR",
        "DROP TABLE orders",
        "TRUNCATE TABLE orders",
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert!(
            classify_statement(&stmts[0]).is_write_path(),
            "`{sql}` must be routed as a write"
        );
    }
}

// Transaction control is handled, but it is not a write path: `BEGIN` never
// reaches DuckDB, and the writes it releases at COMMIT were classified as DML
// themselves. Putting it on the write path would send it through the verbatim
// re-parse for nothing.
#[test]
fn test_is_write_path_excludes_reads_and_unclassified() {
    for sql in [
        "SELECT * FROM orders",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT sp",
        "SET timezone = 'UTC'",
    ] {
        let stmts = parser::parse_sql(sql).unwrap();
        assert!(
            !classify_statement(&stmts[0]).is_write_path(),
            "`{sql}` must not be routed as a write"
        );
    }
}

// Canonical folding is what makes catalog keys comparable to whatever case a
// client writes: unquoted names fold to lowercase, quoted names stay verbatim.
#[test]
fn test_canonicalize_ident_str_folds_like_an_identifier() {
    use vairedb_coordinator::pgwire_handler::query_router::canonicalize_ident_str;
    assert_eq!(canonicalize_ident_str("Customer_ID"), "customer_id");
    assert_eq!(canonicalize_ident_str("id"), "id");
    assert_eq!(canonicalize_ident_str("\"Customer_ID\""), "Customer_ID");
    assert_eq!(canonicalize_ident_str(""), "");
}
