//! Refusal of every spelling of "let the database allocate my ids".
//!
//! VaireDB has no sequences, and this is a decided limitation rather than an
//! unimplemented feature (`docs/specs/gap-analysis-command.md`, *Decided
//! limitations → No sequences*). A sequence is one monotonic counter, and neither
//! implementation available to a shared-nothing cluster is worth having:
//! broadcasting `CREATE SEQUENCE` gives each shard its own counter, so the "unique"
//! ids collide across shards — and because replication is statement shipping, a
//! `nextval()` surviving into shard-local SQL evaluates *differently on each
//! replica* of the same shard; while a coordinator-allocated counter serializes
//! every insert through one allocator.
//!
//! `CREATE SEQUENCE` itself never classifies, so it is refused by
//! `unsupported_statement_error`. What this module catches is the same request
//! wearing table-DDL clothing — `SERIAL`, `DEFAULT nextval(…)`, `GENERATED … AS
//! IDENTITY` — plus `nextval()` written into a write statement directly. Without
//! it, `SERIAL` reaches the per-shard DuckDB and fails there, so the client sees a
//! storage-engine error about a type instead of the reason.

use std::ops::ControlFlow;

use pgwire::error::PgWireResult;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, DataType, Expr, ObjectName, Statement, visit_expressions,
};

use super::error_enrichment::make_vdb_error;

/// The sequence-manipulating functions. `nextval` is the one that matters;
/// the others are refused with it so the answer does not depend on which half of
/// the sequence API a client reaches for.
const SEQUENCE_FUNCTIONS: [&str; 4] = ["nextval", "currval", "lastval", "setval"];

/// The `SERIAL` family, as sqlparser hands it over: PostgreSQL's serial types are
/// not `DataType` variants, they arrive as `DataType::Custom("SERIAL")`.
const SERIAL_TYPES: [&str; 6] = [
    "serial",
    "bigserial",
    "smallserial",
    "serial2",
    "serial4",
    "serial8",
];

/// Refuse `stmt` if it asks the cluster to allocate ids from a sequence.
///
/// Called on both dispatch paths before the statement is executed, so the refusal
/// costs one AST walk and happens before any catalog or shard state is touched.
pub(super) fn reject_sequence_use(stmt: &Statement) -> PgWireResult<()> {
    if let Statement::CreateTable(create) = stmt {
        reject_sequence_columns(create)?;
    }

    // `nextval('s')` anywhere in a write: an INSERT value, an UPDATE assignment,
    // a WHERE clause. A SELECT is not walked — reads are planned by DataFusion,
    // which fails on its own with an unknown-function error, and this walk is
    // about what gets *shipped*.
    if matches!(
        stmt,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) | Statement::Copy { .. }
    ) {
        let mut found: Option<String> = None;
        let _ = visit_expressions(stmt, |expr| {
            if let Expr::Function(func) = expr
                && let Some(name) = sequence_function_name(&func.name)
            {
                found = Some(name);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        if let Some(name) = found {
            return Err(sequence_error(&format!("`{name}()`")));
        }
    }

    Ok(())
}

/// Refuse a `CREATE TABLE` whose column list asks for a server-allocated id, in
/// any of its three spellings.
fn reject_sequence_columns(create: &CreateTable) -> PgWireResult<()> {
    for col in &create.columns {
        if let Some(kind) = sequence_column_kind(col) {
            return Err(sequence_error(&format!(
                "{kind} on column \"{}\"",
                col.name.value
            )));
        }
    }
    Ok(())
}

/// Which sequence spelling `col` uses, phrased for the error message, or `None`
/// if the column allocates nothing.
fn sequence_column_kind(col: &ColumnDef) -> Option<&'static str> {
    if let DataType::Custom(name, _) = &col.data_type
        && let Some(part) = name.0.last()
        && let Some(ident) = part.as_ident()
        && SERIAL_TYPES.contains(&ident.value.to_ascii_lowercase().as_str())
    {
        return Some("the SERIAL family");
    }

    for option in &col.options {
        match &option.option {
            ColumnOption::Default(expr) => {
                let mut found = false;
                let _ = visit_expressions(expr, |e| {
                    if let Expr::Function(func) = e
                        && sequence_function_name(&func.name).is_some()
                    {
                        found = true;
                        return ControlFlow::Break(());
                    }
                    ControlFlow::Continue(())
                });
                if found {
                    return Some("a DEFAULT that draws from a sequence");
                }
            }
            // `GENERATED … AS IDENTITY` is a sequence with different syntax.
            // A *generated column* (`GENERATED ALWAYS AS (<expr>) STORED`) is a
            // different feature — it computes from the row, allocates nothing —
            // and is told apart by carrying a generation expression.
            ColumnOption::Generated {
                generation_expr, ..
            } if generation_expr.is_none() => {
                return Some("GENERATED ... AS IDENTITY");
            }
            _ => {}
        }
    }
    None
}

/// The lowercased function name if `name` is a sequence function, else `None`.
fn sequence_function_name(name: &ObjectName) -> Option<String> {
    let ident = name.0.last()?.as_ident()?;
    let lowered = ident.value.to_ascii_lowercase();
    SEQUENCE_FUNCTIONS
        .contains(&lowered.as_str())
        .then_some(lowered)
    // Qualified spellings (`pg_catalog.nextval`) match on the last part, which is
    // why only that part is compared.
}

/// The one refusal message, so every spelling gets the same explanation and the
/// same way out. `what` names the thing refused, as it appears in the statement.
pub(super) fn sequence_error(what: &str) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{what} is not supported by VaireDB: there are no sequences. A sequence is a single \
             counter, so on a sharded cluster it is either kept per shard — where every shard \
             hands out the same numbers and each replica of a shard advances it separately — or \
             kept in the coordinator, where every insert waits on one allocator. Generate ids in \
             the application instead (a UUID, a ULID, or a client-side snowflake): no \
             coordination, and they spread evenly over the shards"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    /// The message a statement is refused with, or `None` if it is allowed.
    fn rejection(sql: &str) -> Option<String> {
        reject_sequence_use(&parse_one(sql))
            .err()
            .map(|e| e.to_string())
    }

    #[test]
    fn serial_columns_are_refused_by_name() {
        for ty in ["SERIAL", "BIGSERIAL", "SMALLSERIAL", "serial4", "SERIAL8"] {
            let msg = rejection(&format!("CREATE TABLE t (id {ty}, v VARCHAR)"))
                .unwrap_or_else(|| panic!("{ty} should be refused"));
            assert!(msg.contains("SERIAL family"), "got: {msg}");
            assert!(msg.contains("\"id\""), "the column should be named: {msg}");
            // The way out is part of the contract, not decoration.
            assert!(msg.contains("UUID"), "got: {msg}");
        }
    }

    #[test]
    fn a_default_drawing_from_a_sequence_is_refused() {
        let msg = rejection("CREATE TABLE t (id INTEGER DEFAULT nextval('s'))").unwrap();
        assert!(msg.contains("DEFAULT"), "got: {msg}");
    }

    #[test]
    fn identity_columns_are_refused() {
        for sql in [
            "CREATE TABLE t (id INTEGER GENERATED ALWAYS AS IDENTITY)",
            "CREATE TABLE t (id INTEGER GENERATED BY DEFAULT AS IDENTITY)",
        ] {
            let msg = rejection(sql).unwrap_or_else(|| panic!("should be refused: {sql}"));
            assert!(msg.contains("IDENTITY"), "got: {msg}");
        }
    }

    // A generated column computes from the row rather than allocating an id, so it
    // is a different feature and must not be caught by this guard.
    #[test]
    fn a_computed_generated_column_is_not_a_sequence() {
        assert_eq!(
            rejection(
                "CREATE TABLE t (id INTEGER, double_id INTEGER GENERATED ALWAYS AS (id * 2) STORED)"
            ),
            None
        );
    }

    #[test]
    fn nextval_in_a_write_is_refused_wherever_it_appears() {
        for sql in [
            "INSERT INTO t (id, v) VALUES (nextval('s'), 'a')",
            "INSERT INTO t (id, v) VALUES (1, 'a'), (nextval('s'), 'b')",
            "UPDATE t SET v = 'x' WHERE id = currval('s')",
            "UPDATE t SET id = nextval('s') WHERE v = 'x'",
            "DELETE FROM t WHERE id = lastval()",
            "INSERT INTO t (id) VALUES (pg_catalog.nextval('s'))",
        ] {
            let msg = rejection(sql).unwrap_or_else(|| panic!("should be refused: {sql}"));
            assert!(
                msg.contains("no sequences"),
                "should explain why ({sql}): {msg}"
            );
        }
    }

    #[test]
    fn ordinary_writes_and_ddl_are_untouched() {
        for sql in [
            "CREATE TABLE t (id INTEGER, v VARCHAR)",
            "CREATE TABLE t (id INTEGER DEFAULT 1, ts TIMESTAMP DEFAULT now())",
            "INSERT INTO t (id, v) VALUES (1, 'a')",
            "UPDATE t SET v = 'x' WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
            // A column merely *named* like a sequence function is a name, not a call.
            "CREATE TABLE t (nextval INTEGER)",
            "INSERT INTO t (nextval) VALUES (1)",
        ] {
            assert_eq!(rejection(sql), None, "should be allowed: {sql}");
        }
    }
}
