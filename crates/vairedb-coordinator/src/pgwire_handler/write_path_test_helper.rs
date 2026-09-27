//! Handing a write-path module the statement a client sent, and reading back what the
//! client would be told.
//!
//! The write path has the same test/production seam [`super::read_path_test_helper`] closed on
//! the read path, and it opened in a quieter way: **thirteen** modules each defined a
//! `parse_one`, twelve of them over the real [`parser::parse_sql`] and one over
//! `sqlparser`'s parser directly. So the duplication is not the finding — the finding is
//! what the copies disagreed about.
//!
//! Two things:
//!
//! * **How many statements a fixture is allowed to be.** Seven copies took
//!   `.into_iter().next()`, which judges the first statement of `"A; B"` and discards the
//!   rest without saying so; five asserted a length of one. A fixture that silently means
//!   less than it says is the kind of test that passes for the wrong reason, so this
//!   asserts.
//! * **Whether the statement came from the write path at all.** `session_params` built a
//!   `PostgreSqlDialect` parser by hand and re-implemented one of `parse_sql`'s respellings
//!   beside it, so the write path's verbatim re-parse and its `reject_duckdb_divergent`
//!   check never ran and the statement under test was not the statement production hands
//!   the module. Migrated in iteration 6, and every expectation held — which is the useful
//!   half of the result: the hand-built parser was not buying the module anything the real
//!   one would have denied it.
//!
//! What is *not* here is anything that plans or dispatches. A write path module is reached
//! with a `Statement` and answers with a `PgWireError` or a catalog change, and those are
//! each module's own — unlike the read path, there is no eleven-pass chain whose order a
//! test could get wrong.

use pgwire::error::PgWireError;

use super::parser;
use crate::sqlparser::ast::{AlterTable, AlterTableOperation, Statement};

/// The one statement `sql` parses to, the way the write path parses it.
///
/// Panics with the statement's own text on a parse failure, on a `sql` that is more than
/// one statement, and on one that is none — a fixture is a claim about a single statement,
/// and each of those three is a fixture that does not make the claim it looks like.
pub(super) fn parse_one(sql: &str) -> Statement {
    let mut statements =
        parser::parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"));
    assert_eq!(
        statements.len(),
        1,
        "`{sql}` is {} statements; these helpers judge one",
        statements.len()
    );
    statements.remove(0)
}

/// The `ALTER TABLE` `sql` parses to.
///
/// `ALTER TABLE` is the statement the DDL cluster tests most, and five modules unwrapped it
/// by hand. One of the five — `table_meta_ops` — indexed `[0]` without checking the length
/// and cloned the operations out of a borrow, which is the lenient shape [`parse_one`]
/// exists to stop; it was missed when the `parse_one` copies were migrated because it is
/// spelled as its own helper rather than as a `parse_one` call.
pub(super) fn parse_alter(sql: &str) -> AlterTable {
    match parse_one(sql) {
        Statement::AlterTable(alter) => alter,
        other => panic!("`{sql}` is not an ALTER TABLE: {other:?}"),
    }
}

/// The operations of the `ALTER TABLE` `sql` parses to.
pub(super) fn parse_alter_ops(sql: &str) -> Vec<AlterTableOperation> {
    parse_alter(sql).operations
}

/// The one operation of the `ALTER TABLE` `sql` parses to.
///
/// Asserts the count for the reason [`parse_one`] asserts its own: `ALTER TABLE t A, B`
/// is a statement the coordinator treats differently from either half, so a helper that
/// silently took the first would test a claim the fixture does not make.
pub(super) fn parse_alter_op(sql: &str) -> AlterTableOperation {
    let mut operations = parse_alter_ops(sql);
    assert_eq!(
        operations.len(),
        1,
        "`{sql}` is {} operations; this helper judges one",
        operations.len()
    );
    operations.remove(0)
}

/// The SQLSTATE and message a client is shown for `err`.
///
/// Only a `UserError` counts. An internal error is also an `Err`, and reading one as a
/// refusal is how a test whose check stopped firing keeps passing — the same reason
/// [`super::read_path_test_helper::refusal`] insists on the variant.
pub(super) fn user_error(err: PgWireError) -> (String, String) {
    match err {
        PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
        other => panic!("expected a user-facing error, got {other:?}"),
    }
}
