//! The single SQL parse and the read-path AST rewrites (`pgwire_handler::parser`).
//! Write-path translation is covered by `write_sql_cl_tests`.

use vairedb_coordinator::error::CoordinatorError;
use vairedb_coordinator::pgwire_handler::parser::{
    collapse_schema_qualified_relations, parse_sql, transform_to_char_format_for_read,
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

// --- The write path must get the statement the client sent, verbatim ---
//
// The pg-compatibility parser rewrites a statement so DataFusion can plan it
// against an emulated `pg_catalog`. A write is never planned — it is rendered
// back to SQL and shipped to DuckDB — so those rewrites do not adapt a write,
// they change the data it stores. `parse_sql` therefore hands the write path a
// verbatim parse.

// `'users'::regclass` in a SELECT is a catalog lookup DataFusion cannot plan, so
// the compat parser drops the cast. In an INSERT the same rewrite silently turns
// an OID-typed value into the bare string, and DuckDB stores the string.
#[test]
fn test_write_path_keeps_regclass_cast_in_insert_values() {
    let stmts = parse_sql("INSERT INTO t (a) VALUES ('users'::regclass)").unwrap();
    let sql = stmts[0].to_string();
    assert!(
        sql.to_lowercase().contains("regclass"),
        "the cast the client sent must survive to DuckDB, got: {sql}"
    );
}

#[test]
fn test_write_path_keeps_oid_cast_in_insert_values() {
    let stmts = parse_sql("INSERT INTO t (a) VALUES (1::oid)").unwrap();
    let sql = stmts[0].to_string();
    assert!(
        sql.to_lowercase().contains("oid"),
        "the cast the client sent must survive to DuckDB, got: {sql}"
    );
}

#[test]
fn test_write_path_keeps_casts_in_update_assignments() {
    let stmts = parse_sql("UPDATE t SET a = 'users'::regclass WHERE id = 1").unwrap();
    let sql = stmts[0].to_string();
    assert!(sql.to_lowercase().contains("regclass"), "got: {sql}");
}

// `ANY(ARRAY[..])` is rewritten to `array_contains(..)` for DataFusion. Reversing
// the argument order and renaming the operator is a rewrite the write path never
// asked for; DuckDB understands `= ANY (...)` itself.
#[test]
fn test_write_path_keeps_any_array_in_delete_predicate() {
    let stmts = parse_sql("DELETE FROM t WHERE a = ANY(ARRAY[1, 2])").unwrap();
    let sql = stmts[0].to_string();
    assert!(
        !sql.to_lowercase().contains("array_contains"),
        "the write path must not be handed a read-path desugaring, got: {sql}"
    );
    assert!(sql.to_uppercase().contains("ANY"), "got: {sql}");
}

// The read path still gets the rewrites — that is what makes driver
// introspection queries planable.
#[test]
fn test_read_path_still_gets_the_compat_rewrites() {
    let stmts = parse_sql("SELECT 'users'::regclass").unwrap();
    let sql = stmts[0].to_string();
    assert!(
        !sql.to_lowercase().contains("regclass"),
        "the read path must keep its pg-compat rewrites, got: {sql}"
    );
}

// A batch mixing both kinds must prepare each statement for the path that runs
// it, not pick one AST for the whole batch.
#[test]
fn test_mixed_batch_prepares_each_statement_for_its_own_path() {
    let stmts = parse_sql("SELECT 'users'::regclass; INSERT INTO t (a) VALUES ('users'::regclass)")
        .unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(
        !stmts[0].to_string().to_lowercase().contains("regclass"),
        "the SELECT must keep its rewrite, got: {}",
        stmts[0]
    );
    assert!(
        stmts[1].to_string().to_lowercase().contains("regclass"),
        "the INSERT must be verbatim, got: {}",
        stmts[1]
    );
}

// DDL is shipped to DuckDB too, so it is write-path for parsing purposes.
#[test]
fn test_ddl_is_parsed_verbatim() {
    let stmts = parse_sql("CREATE TABLE t (a INTEGER DEFAULT 1::oid)").unwrap();
    let sql = stmts[0].to_string();
    assert!(sql.to_lowercase().contains("oid"), "got: {sql}");
}

// --- Read path: collapse a schema-qualified relation to its catalog key ---

// A schema is a coordinator-catalog namespace, not a DataFusion one: the collapse
// rewrites `schema.tbl` to the single quoted identifier `"schema.tbl"`, which is
// the name the relation is registered under for planning.
#[test]
fn test_collapse_schema_qualified_relation() {
    let sql = "SELECT id FROM ident_schema.orders WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    collapse_schema_qualified_relations(&mut stmts[0]);
    let result = stmts[0].to_string();
    assert!(result.contains("\"ident_schema.orders\""), "got: {result}");
}

#[test]
fn test_collapse_leaves_single_part_relation_untouched() {
    let sql = "SELECT id FROM orders WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    let before = stmts[0].to_string();
    collapse_schema_qualified_relations(&mut stmts[0]);
    let after = stmts[0].to_string();
    assert_eq!(before, after);
}

// The key keeps a quoted part's case, and the whole key is quoted so nothing folds
// it away again.
#[test]
fn test_collapse_preserves_quoted_last_part() {
    let sql = "SELECT id FROM ident_schema.\"MyTable\"";
    let mut stmts = parse_sql(sql).unwrap();
    collapse_schema_qualified_relations(&mut stmts[0]);
    let result = stmts[0].to_string();
    assert!(result.contains("\"ident_schema.MyTable\""), "got: {result}");
}

// --- Read path: keep to_char, translate only the format string ---

#[test]
fn test_read_transform_keeps_to_char_and_translates_format() {
    let sql = "SELECT TO_CHAR(ts, 'YYYY-MM-DD') FROM logs";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_char_format_for_read(&mut stmts[0]);
    let result = stmts[0].to_string();
    assert!(
        result.to_uppercase().contains("TO_CHAR"),
        "read path must keep to_char, got: {result}"
    );
    assert!(!result.contains("STRFTIME"), "got: {result}");
    assert!(result.contains("%Y-%m-%d"), "got: {result}");
    assert!(!result.contains("YYYY"), "got: {result}");
}

#[test]
fn test_read_transform_reaches_projection_and_where() {
    let sql =
        "SELECT TO_CHAR(a, 'YYYY') FROM t WHERE TO_CHAR(b, 'MM') = '01' ORDER BY TO_CHAR(c, 'DD')";
    let mut stmts = parse_sql(sql).unwrap();
    transform_to_char_format_for_read(&mut stmts[0]);
    let result = stmts[0].to_string();
    assert!(result.contains("%Y"), "projection not translated: {result}");
    assert!(result.contains("%m"), "WHERE not translated: {result}");
    assert!(result.contains("%d"), "ORDER BY not translated: {result}");
    assert!(!result.contains("'YYYY'") && !result.contains("'MM'") && !result.contains("'DD'"));
}

#[test]
fn test_read_transform_ignores_non_to_char() {
    let sql = "SELECT EXTRACT(YEAR FROM ts), name || '!' FROM logs WHERE id = 1";
    let mut stmts = parse_sql(sql).unwrap();
    let before = stmts[0].to_string();
    transform_to_char_format_for_read(&mut stmts[0]);
    let after = stmts[0].to_string();
    assert_eq!(before, after);
}

// --- COLLATE is decided during the parse, because that is where it still exists ---
//
// The compatibility parser deletes every `COLLATE` clause on its way past, so a read
// statement asking for a real locale would otherwise be answered in byte order with
// nothing said. `parse_sql` refuses it while the client's own text is still in reach.

#[test]
fn test_read_path_refuses_a_collation_that_is_not_byte_order() {
    let err = parse_sql(r#"SELECT name COLLATE "en_US" FROM t"#)
        .expect_err("a collation VaireDB cannot apply must be refused, not dropped");
    assert!(
        matches!(err, CoordinatorError::Unsupported(_)),
        "0A000, not a syntax error: the statement parses, VaireDB just cannot honour it          — got {err:?}"
    );
    assert!(err.to_string().contains("COLLATE"), "got: {err}");
}

// Byte order is what VaireDB applies, so the three names for it stay accepted — and
// come back stripped, which is what makes them plannable.
#[test]
fn test_read_path_accepts_byte_order_collations() {
    for sql in [
        r#"SELECT name COLLATE "C" FROM t"#,
        r#"SELECT 1 FROM t ORDER BY name COLLATE "POSIX""#,
        "SELECT name COLLATE ucs_basic FROM t",
    ] {
        let stmts = parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"));
        assert!(
            !stmts[0].to_string().to_uppercase().contains("COLLATE"),
            "the clause is dropped once accepted, got: {}",
            stmts[0]
        );
    }
}

// The keyword scan that decides whether to parse a second time errs towards yes, so a
// string that merely contains the word must still be answered rather than refused.
#[test]
fn test_the_word_collate_inside_a_literal_is_not_a_collation() {
    let stmts = parse_sql("SELECT 'collate me' AS s").unwrap();
    assert_eq!(stmts.len(), 1);
}

// A write is refused on the same terms as a read. It was not, once — the reasoning being
// that a write never reaches DataFusion and DuckDB has collations of its own, so the
// clause could be passed through intact. That is true and it is the problem: DuckDB
// applies one of *its* collations, so the same comparison was ordered by ICU rules on an
// UPDATE and by byte value on the SELECT that read the result back. The client asked for
// an ordering neither path was going to give it, and nothing said so.
#[test]
fn test_write_path_refuses_a_collation_it_does_not_implement() {
    let err = parse_sql(r#"UPDATE t SET a = 1 WHERE name COLLATE "en_US" < 'x'"#)
        .expect_err("a locale collation must be refused on a write");
    assert!(err.to_string().contains("COLLATE"), "got: {err}");
}

// The byte-order spellings are accepted instead — and stripped on the way to a shard,
// which `write_sql_cl_tests` covers.
#[test]
fn test_write_path_accepts_a_byte_order_collation() {
    for sql in [
        r#"UPDATE t SET a = 1 WHERE name COLLATE "C" < 'x'"#,
        "DELETE FROM t WHERE name COLLATE pg_catalog.default < 'x'",
    ] {
        parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"));
    }
}
