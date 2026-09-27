//! Asking the read path for its verdict on a statement, the way a client does.
//!
//! Every `pg_*` pass used to be tested by planning the SQL with DataFusion directly and
//! then calling the one pass under test:
//!
//! ```ignore
//! let plan = ctx.state().create_logical_plan(sql).await.unwrap();
//! reject_null_unaware_not_in(&plan).map_err(|e| e.to_string())
//! ```
//!
//! That is not the read path. It skips [`parser::parse_sql`]'s AST rewrites and every
//! plan pass that runs before the one under test, so a pass could satisfy its own tests
//! and still misbehave in [`parser::plan_select`]'s chain — while the helpers' doc
//! comments described themselves as "the plan `plan_select` would hand the rest of the
//! read path". A pass whose real behaviour is unverified is where a ⛔ wrong answer
//! hides, which is why the invocation is shared and the hand-rolled approximation is not
//! allowed to come back.
//!
//! The *context* is shared for the same reason as the invocation, and this is the half
//! the first pass missed. A read-path context is two things at once — the session
//! settings of `scheduler::with_postgres_sql_options` and the function set of
//! `scheduler::register_postgres_functions` — and a `SessionContext::new()` is neither.
//! Without the settings an unsuffixed decimal literal arrives as `Float64` instead of
//! `numeric`, so a test reads a type production never produces; without the functions a
//! pass that rewrites to a UDF plans against a registry that does not have it. One module
//! had already rebuilt both by hand and the rest had neither, which is the same drift one
//! level up: the fixture was faithful about the statement and unfaithful about the engine
//! it was asked of. [`context`] is where that pairing lives now.
//!
//! The AST passes — the ones that run *inside* [`parser::parse_sql`] rather than on its
//! plan — cannot use [`plan`]: judging one of them means handing it the statement before
//! the rest of the chain has touched it. [`parse_verbatim`] is that parse, and
//! [`parse_rewritten`] is its counterpart for a pass or a guard that runs *after* the
//! chain. Both are here because the surrounding modules had written one of them out by
//! hand in twenty-one places, and not one of those copies decided the thing such a helper
//! must: how many statements a fixture may be. Twenty took the first of however many
//! parsed and one popped the last — that one spelling its `expect` "one statement" while
//! asserting only "at least one". A fixture that is accidentally two statements tested
//! half of itself, quietly, and in either half depending on the module. The count is
//! asserted once, here.
//!
//! What is *not* shared is the fixture data. Each module builds its own tables because
//! their schemas are the test's meaning: `pg_not_in_nulls` needs `id` non-nullable and
//! `k` nullable to reach both sides of its nullability check, and a common table would
//! quietly decide that for it.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use pgwire::error::{PgWireError, PgWireResult};

use super::error_enrichment::make_vdb_error;
use super::parser;
use crate::error::CoordinatorError;
use crate::scheduler::{register_postgres_functions, with_postgres_sql_options};
use crate::sqlparser::ast::Statement;

/// An empty context configured the way the read path's own contexts are.
///
/// Callers register their fixture tables on it. Deliberately not `with_information_schema`
/// and without the `pg_catalog` or `vairedb_catalog` schemas: those belong to `local_ctx`,
/// a third read context these helpers do not stand in for, and a pass tested against them
/// would be tested somewhere its statement never runs.
pub(super) fn context() -> SessionContext {
    let mut ctx = SessionContext::new_with_config(with_postgres_sql_options(SessionConfig::new()));
    register_postgres_functions(&mut ctx);
    ctx
}

/// The one statement `sql` parses to, exactly as the client wrote it.
///
/// This is [`parser::parse_sql`]'s own verbatim parse, so a pass that runs inside it is
/// handed what it is handed in production. Deliberately *not* the compat-rewritten AST:
/// a pass tested on its own output, or on a later pass's, is not the pass under test.
pub(super) fn parse_verbatim(sql: &str) -> Statement {
    let mut statements = parser::parse_verbatim(sql).expect("parses");
    assert_eq!(
        statements.len(),
        1,
        "`{sql}` is {} statements; these helpers judge one",
        statements.len()
    );
    statements.pop().expect("one statement")
}

/// The one statement `sql` parses to *after* [`parser::parse_sql`]'s compat rewrites.
///
/// [`parse_verbatim`]'s counterpart, for a pass or a guard that runs on the rewritten
/// AST rather than inside the rewrite chain. Three more modules had hand-rolled this
/// one, and took the first of however many statements parsed; the count is asserted
/// here for the same reason it is asserted there.
pub(super) fn parse_rewritten(sql: &str) -> Statement {
    let mut statements =
        parser::parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"));
    assert_eq!(
        statements.len(),
        1,
        "`{sql}` is {} statements; these helpers judge one",
        statements.len()
    );
    statements.pop().expect("one statement")
}

/// Plan `sql` through the whole read path: [`parser::parse_sql`]'s rewrites, then every
/// pass [`parser::plan_select`] runs, in its order, then coercion.
///
/// A parse-time refusal is converted the way `VaireQueryParser::parse_sql` converts it,
/// so a statement refused before the planner arrives here as the same error the client
/// would read rather than as a second error type callers have to match on.
pub(super) async fn plan(ctx: &SessionContext, sql: &str) -> PgWireResult<LogicalPlan> {
    let mut statements = parser::parse_sql(sql)
        .map_err(|e: CoordinatorError| make_vdb_error(e.vdb_error_code(), e.to_string()))?;
    assert_eq!(
        statements.len(),
        1,
        "`{sql}` is {} statements; these helpers judge one",
        statements.len()
    );
    let stmt = statements.pop().expect("one statement");

    // A scratch catalog rather than a shared one: `prepare_select_for_planning` reads it
    // for view expansion and anonymization, and an empty catalog is what a test that
    // registers its tables on the `SessionContext` means to present.
    let catalog = Arc::new(crate::catalog::catalog_test_helper::scratch_catalog(
        "read_path",
    ));
    parser::plan_select(ctx, &stmt, false, &catalog)
        .await
        .map(|(plan, _)| plan)
}

/// The read path's verdict on `sql`, with `Err` carrying the client-facing message.
pub(super) async fn verdict(ctx: &SessionContext, sql: &str) -> Result<(), String> {
    plan(ctx, sql).await.map(|_| ()).map_err(message_of)
}

/// The message a client is shown for a statement the read path refuses.
///
/// Refusals are `UserError`s, and only those count: a planner failure or an internal
/// error is a different event that also happens to be an `Err`, and reading one as a
/// refusal is how a test whose pass stopped firing keeps passing.
pub(super) async fn refusal(ctx: &SessionContext, sql: &str) -> String {
    match plan(ctx, sql).await {
        Err(PgWireError::UserError(info)) => info.message.clone(),
        Err(other) => panic!("`{sql}` failed without refusing the client: {other}"),
        Ok(_) => panic!("`{sql}` must be refused"),
    }
}

/// Assert the read path answers `sql`, which for a refusal pass is half the claim: a
/// check scoped too widely takes away a query that works.
pub(super) async fn accepted(ctx: &SessionContext, sql: &str) {
    if let Err(e) = verdict(ctx, sql).await {
        panic!("`{sql}` must be accepted, got: {e}");
    }
}

/// The client-facing text of a read-path error, whatever kind it is.
fn message_of(error: PgWireError) -> String {
    match error {
        PgWireError::UserError(info) => info.message,
        other => other.to_string(),
    }
}
