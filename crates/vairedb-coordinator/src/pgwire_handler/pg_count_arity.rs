//! Refuse `count(a, b)` — with or without `DISTINCT` — instead of answering a number
//! PostgreSQL has no function to produce.
//!
//! ```text
//! w(n int4, g text) = (1, 'a'), (1, 'a'), (2, 'b'), (NULL, 'c')
//!
//! SELECT count(n, g) FROM w
//! PostgreSQL  ERROR: 42883: function count(integer, text) does not exist
//! VaireDB     3                       -- before this refusal
//!
//! SELECT count(DISTINCT n, g) FROM w
//! PostgreSQL  ERROR: 42883: function count(integer, text) does not exist
//! VaireDB     Internal error: DISTINCT aggregate should have exactly one argument
//! ```
//!
//! PostgreSQL's `count` has exactly two forms, `count(*)` and `count(expression)`. There is
//! no multi-argument overload, and `count(DISTINCT a, b)` — which looks like it should count
//! distinct *pairs* — is refused at parse time. The spelling PostgreSQL does have for
//! distinct pairs is `count(DISTINCT (a, b))`, over a row constructor: one argument that
//! happens to be composite. § 5 of the gap analysis lists that as a superset VaireDB must
//! not "fix" into the comma form, and this module is the other side of the same decision.
//!
//! ## Why a refusal is the fix
//!
//! Because DataFusion's `count` is variadic, and what it means by a second argument is not
//! what anyone writing the comma form intends: it counts the rows where *every* argument is
//! non-null. So `count(n, g)` above answers `3` — a plausible number, arrived at by a rule
//! the client never asked for, with nothing in the result to say the pair was not what was
//! counted. A client migrating from PostgreSQL cannot hit this by accident, because their
//! query never worked there; a client writing VaireDB-first can, and would then find the
//! same query rejected by the PostgreSQL they migrate *to*. Either way the honest answer is
//! the one PostgreSQL gives.
//!
//! The `DISTINCT` form is worse only in presentation: `XX000` with the words "Internal
//! error" tells a client the server has a bug and invites a report against the wrong
//! component, when the statement is simply not one PostgreSQL has.
//!
//! ## Why `42883` and not `0A000`
//!
//! The two codes send a client to different places. `0A000` says PostgreSQL has this form and
//! VaireDB has not implemented it yet, so waiting for a release is rational. `42883` says
//! PostgreSQL does not have it either, so only editing the call helps. This is the second,
//! and it is the only refusal on the read path that is — which is why
//! [`vairedb_common::proto::vairedb::v1::VdbErrorCode::UndefinedFunction`] exists.
//!
//! ## Why the plan and not the AST
//!
//! For the message. PostgreSQL names the argument *types* it could not find an overload for —
//! `function count(integer, text) does not exist` — and a type is a property of a resolved
//! column, which the AST does not have. Running after the planner means the schema is there
//! to read the types off; running before the optimizer means the `count` is still spelled the
//! way the client spelled it. See [`super::parser::plan_select`].

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::pg_typeof::pg_type_name;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// Refuse every `count` in `plan` written with more than one argument.
///
/// Reads the plan and never rewrites it: the outcome is either the same plan or `42883`.
pub(crate) fn reject_multi_argument_count(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut visitor = CountArity { error: None };
    // `visit_with_subqueries`, so a `count(a, b)` inside a subquery or a CTE is refused too —
    // the number it would contribute is as wrong there as at the top level.
    let _ = plan.visit_with_subqueries(&mut visitor);
    match visitor.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

struct CountArity {
    error: Option<PgWireError>,
}

impl<'n> TreeNodeVisitor<'n> for CountArity {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n LogicalPlan) -> DFResult<TreeNodeRecursion> {
        // The two nodes a `count` can live in, each paired with the schema its arguments are
        // resolved against — which is the *input's*, since the aggregate's own output schema
        // holds the results rather than the operands.
        let (exprs, schema): (Vec<&Expr>, &DFSchema) = match node {
            LogicalPlan::Aggregate(aggregate) => (
                aggregate.aggr_expr.iter().collect(),
                aggregate.input.schema().as_ref(),
            ),
            LogicalPlan::Window(window) => (
                window.window_expr.iter().collect(),
                window.input.schema().as_ref(),
            ),
            _ => return Ok(TreeNodeRecursion::Continue),
        };

        for expr in exprs {
            if let Some(e) = check_expr(expr, schema) {
                self.error = Some(e);
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// The refusal for the first multi-argument `count` anywhere inside `expr`, if there is one.
///
/// The whole expression tree rather than its root, because an aggregate is not always the
/// outermost node: `count(a, b) + 1` and `coalesce(count(a, b), 0)` are the same call in a
/// position that hides it.
fn check_expr(expr: &Expr, schema: &DFSchema) -> Option<PgWireError> {
    let mut refusal = None;
    let _ = expr.apply(|expr| {
        let args = match expr {
            Expr::AggregateFunction(aggregate) if is_count(aggregate.func.name()) => {
                &aggregate.params.args
            }
            Expr::WindowFunction(window) if is_count(window.fun.name()) => &window.params.args,
            _ => return Ok(TreeNodeRecursion::Continue),
        };
        if args.len() > 1 {
            refusal = Some(undefined_count(args, schema));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    refusal
}

/// Whether the resolved function is `count`.
///
/// Compared against the resolved name rather than what the client typed, so `COUNT` and
/// `"count"` are the same function — which they are, since the planner has already done the
/// folding by this point.
fn is_count(name: &str) -> bool {
    name.eq_ignore_ascii_case("count")
}

/// `42883`, in PostgreSQL's own wording: the name, the argument types it was written with,
/// and the hint that says where to look.
///
/// A type that cannot be derived against this schema is reported as `unknown`, which is the
/// spelling PostgreSQL uses for an argument whose type it has not resolved either. The
/// alternative — declining to refuse because one type is unreadable — would answer the wrong
/// number for the sake of a better message.
fn undefined_count(args: &[Expr], schema: &DFSchema) -> PgWireError {
    let types = args
        .iter()
        .map(|arg| match arg.get_type(schema) {
            Ok(data_type) => pg_type_name(&data_type),
            Err(_) => "unknown".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    make_vdb_error(
        VdbErrorCode::UndefinedFunction,
        format!(
            "function count({types}) does not exist. \
             HINT: No function matches the given name and argument types. \
             count() takes one argument; write count(DISTINCT (a, b)) over a row constructor \
             to count distinct combinations."
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    /// `w(n int4, g text)`, matching the table the oracle values in the module doc were
    /// measured against.
    async fn context() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("g", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("c"),
                ])),
            ],
        )
        .expect("a batch");
        let ctx = SessionContext::new();
        ctx.register_table(
            "w",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("a table")),
        )
        .expect("registered");
        ctx
    }

    async fn refusal(sql: &str) -> Option<String> {
        let ctx = context().await;
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} did not plan: {e}"));
        reject_multi_argument_count(&plan).err().map(|e| match e {
            PgWireError::UserError(info) => info.message.clone(),
            other => panic!("{sql} was refused as {other:?}"),
        })
    }

    /// The two spellings PostgreSQL 16.15 refuses, with the types it names them by.
    #[tokio::test]
    async fn the_comma_form_is_refused_with_its_argument_types() {
        for sql in [
            "SELECT count(n, g) FROM w",
            "SELECT count(DISTINCT n, g) FROM w",
        ] {
            let message = refusal(sql)
                .await
                .unwrap_or_else(|| panic!("{sql} was allowed"));
            assert!(
                message.contains("function count(integer, text) does not exist"),
                "{sql} was refused as: {message}"
            );
        }
    }

    /// The hint names the spelling that does work, because that is what a client needs: the
    /// row-constructor form is not a workaround, it is PostgreSQL's own answer.
    #[tokio::test]
    async fn the_refusal_names_the_row_constructor_form() {
        let message = refusal("SELECT count(n, g) FROM w").await.expect("refused");
        assert!(message.contains("count(DISTINCT (a, b))"), "got: {message}");
    }

    /// The forms PostgreSQL has, each of which must keep working — the refusal is about
    /// arity and nothing else.
    #[tokio::test]
    async fn every_count_postgresql_has_is_still_allowed() {
        for sql in [
            "SELECT count(*) FROM w",
            "SELECT count(n) FROM w",
            "SELECT count(DISTINCT n) FROM w",
            "SELECT count(DISTINCT g) FROM w",
            "SELECT count(*) OVER () FROM w",
            "SELECT count(n) OVER (PARTITION BY g) FROM w",
            "SELECT count(n) FILTER (WHERE n > 1) FROM w",
            "SELECT g, count(*) FROM w GROUP BY g",
            // Two single-argument counts, not one two-argument count.
            "SELECT count(n), count(g) FROM w",
            // A composite argument: one argument that is a pair, which is the spelling
            // PostgreSQL has for counting distinct combinations.
            "SELECT count(DISTINCT (n, g)) FROM w",
        ] {
            assert_eq!(refusal(sql).await, None, "{sql} was wrongly refused");
        }
    }

    /// An aggregate is not always the outermost expression, and the count is as wrong inside
    /// an arithmetic expression, a subquery or a window as it is on its own.
    #[tokio::test]
    async fn the_call_is_found_wherever_it_is_written() {
        for sql in [
            "SELECT count(n, g) + 1 FROM w",
            "SELECT coalesce(count(n, g), 0) FROM w",
            "SELECT count(n, g) OVER () FROM w",
            "SELECT (SELECT count(n, g) FROM w) AS c",
            "WITH c AS (SELECT count(n, g) AS k FROM w) SELECT k FROM c",
            "SELECT g FROM w GROUP BY g HAVING count(n, g) > 0",
        ] {
            assert!(refusal(sql).await.is_some(), "{sql} was not refused");
        }
    }
}
