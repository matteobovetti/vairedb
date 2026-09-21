//! Refuse the `NOT IN (subquery)` shapes whose NULL rules the cluster cannot reproduce,
//! instead of answering one row too many.
//!
//! ```text
//! l(id int4 NOT NULL, k int4) = (1, 10), (2, 20), (3, 30), (4, NULL)
//! r(id int4 NOT NULL, k int4) = (2, 20), (3, 99), (5, 50)
//!
//! SELECT id FROM l WHERE k NOT IN (SELECT k FROM r WHERE r.id > l.id)
//!
//! PostgreSQL  1, 2, 3        -- for id 4 the predicate is NULL: NULL NOT IN (50)
//! VaireDB     1, 2, 3, 4     -- before this refusal
//! ```
//!
//! PostgreSQL reads `x NOT IN (q)` as `x <> q1 AND x <> q2 AND …`, so it is **NULL** — and
//! so not true — as soon as `x` is NULL or any candidate is NULL. A distributed anti join
//! reads a NULL as merely unequal and keeps the row, and the flag that would fix it
//! (`HashJoinExec`'s `null_aware`) cannot be shipped: see
//! [`crate::scheduler::scheduler`]'s `with_postgres_sql_options`, which records the
//! measurement. So the predicate is respelled before planning, by
//! [`super::compat_rewrite::rewrite_not_in_subqueries`], into shapes a *non*-null-aware
//! anti join or [`vairedb_common::not_in`] answers correctly.
//!
//! This module is the other half of that: what the respelling cannot enter, it refuses.
//! Both halves together are the invariant — **no `NOT IN (subquery)` can answer a row
//! PostgreSQL would not** — and only the pair of them can hold it, because neither the
//! rewrite nor a refusal alone can.
//!
//! ## What is left for it, and why each one is left
//!
//! * a **correlated** `q` — the shape above. Every respelling needs `q` as either a derived
//!   table or an aggregate subquery, and a correlated `q` can be neither: a derived table
//!   cannot see the outer row without `LATERAL`, which DataFusion does not have, and a
//!   correlated `array_agg` subquery is planned but not *serialized* — measured on the
//!   cluster as `Proto serialization error: outer_ref(l.id) is not yet supported`, since
//!   only an equality correlation is decorrelated before the plan is cut into stages.
//! * a `q` that projects a **wildcard**. The rewrite wraps `q` in a derived table with one
//!   column alias, and whether `SELECT *` has one column is a fact about the catalog rather
//!   than about the statement, so the rewrite cannot know and declines.
//! * a `NOT IN` inside a **`CASE`** or any other expression the rewrite does not descend
//!   into. `WHERE CASE WHEN k NOT IN (q) THEN true ELSE false END` was measured returning
//!   the extra row: the rewrite only rewrites the positions where NULL and false are
//!   indistinguishable, and a `CASE` is exactly a position where they are not.
//!
//! ## Why nullability decides, and not the shape alone
//!
//! Because for a `NOT IN` over columns that cannot be NULL, the anti join **is**
//! PostgreSQL's answer. Three-valuedness is the whole of the divergence: with no NULL on
//! either side the predicate is two-valued, and an empty `q` keeps every row in both
//! engines. So `WHERE id NOT IN (SELECT id FROM r WHERE r.g = l.g)` over `NOT NULL` columns
//! keeps working, correlated or not, and only the queries whose answer could actually
//! differ are refused.
//!
//! That is a decision this check can only make **on the plan**: nullability is a property of
//! a resolved column, and the AST has no types. It runs in
//! [`super::parser::plan_select`] alongside [`super::pg_set_op_types`], on the planned but
//! not yet optimized plan — before DataFusion's decorrelation turns the `InSubquery` into
//! the join whose NULL handling is the problem.
//!
//! ## The one exemption
//!
//! A `NOT IN` directly under a `NOT` is left alone, because it is already correct:
//! `NOT (k NOT IN (q))` is simplified to a **semi** join before the anti join's
//! null-unawareness can matter, which was measured on the cluster to answer exactly what
//! PostgreSQL answers. Refusing it would take away a query that works.
//!
//! ## What the client is told
//!
//! `0A000`, naming `NOT EXISTS` — with the caveat that it is not a mechanical substitution.
//! `NOT EXISTS` is two-valued, so for the example above it answers `1, 2, 3, 4`, not
//! PostgreSQL's `1, 2, 3`: the client has to add the `IS NULL` test that says what a NULL
//! key should mean. Saying so is the point of the message. A refusal that suggested an
//! inexact rewrite as though it were exact would move the wrong answer from the server to
//! the client.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::logical_expr::expr::InSubquery;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// Refuse every `NOT IN (subquery)` in `plan` that a NULL could reach.
///
/// Reads the plan and never rewrites it: the outcome is either the same plan or `0A000`.
pub(crate) fn reject_null_unaware_not_in(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut visitor = NotInNulls { error: None };
    // `visit_with_subqueries`, so a `NOT IN` one level down inside a subquery or a CTE is
    // judged too — it is the same extra row, contributed to whatever reads that subquery.
    let _ = plan.visit_with_subqueries(&mut visitor);
    match visitor.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

struct NotInNulls {
    error: Option<PgWireError>,
}

impl<'n> TreeNodeVisitor<'n> for NotInNulls {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n LogicalPlan) -> DFResult<TreeNodeRecursion> {
        // Only the two nodes whose expressions become a join: a `WHERE`, `HAVING` or
        // `QUALIFY` predicate is a `Filter`, and a join's `ON` is a `Join`. A select list is
        // not one of them, and does not need to be:
        // [`super::pg_projection_subqueries`] respells a `NOT IN` there into a three-valued
        // `CASE` over `count(*)` subqueries, which reproduces PostgreSQL's NULL rules exactly
        // rather than relying on a join to. So no anti join is built from that position and
        // there is nothing here to judge.
        let checked = match node {
            LogicalPlan::Filter(filter) => {
                check_expr(&filter.predicate, filter.input.schema().as_ref())
            }
            LogicalPlan::Join(join) => match &join.filter {
                Some(filter) => check_expr(filter, join.schema.as_ref()),
                None => Ok(()),
            },
            _ => Ok(()),
        };
        if let Err(e) = checked {
            self.error = Some(e);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Refuse the first `NOT IN (subquery)` anywhere in `predicate` that a NULL could reach.
///
/// The whole expression tree, not the top-level conjuncts: a `NOT IN` under a `CASE` is a
/// position the rewrite does not reach and the anti join answers wrongly all the same.
fn check_expr(predicate: &Expr, schema: &DFSchema) -> PgWireResult<()> {
    let mut refusal = None;
    // `apply` visits top-down, so `Jump` skips a subtree — which is how the double negation
    // below keeps its own `NOT IN` from being judged.
    let _ = predicate.apply(|expr| {
        match expr {
            // `NOT (x NOT IN (q))` is planned as a semi join and is already correct.
            Expr::Not(inner) if is_negated_in_subquery(inner.as_ref()) => {
                Ok(TreeNodeRecursion::Jump)
            }
            Expr::InSubquery(in_subquery) if in_subquery.negated => {
                if nulls_are_possible(in_subquery, schema) {
                    refusal = Some(unsupported(in_subquery));
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    });
    match refusal {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Whether `expr` is a `NOT IN (subquery)`, i.e. the operand of a double negation.
fn is_negated_in_subquery(expr: &Expr) -> bool {
    matches!(expr, Expr::InSubquery(in_subquery) if in_subquery.negated)
}

/// Whether a NULL can reach either side of the comparison, which is the only case where the
/// anti join and PostgreSQL differ.
///
/// Both questions err towards *yes*: a left expression whose nullability cannot be derived
/// against this schema (an outer reference the schema does not hold, say) is treated as
/// nullable, because the cost of being wrong that way is a refusal and the cost of being
/// wrong the other way is the silent extra row this module exists to prevent.
fn nulls_are_possible(in_subquery: &InSubquery, schema: &DFSchema) -> bool {
    let left_nullable = in_subquery.expr.nullable(schema).unwrap_or(true);
    let candidates = in_subquery.subquery.subquery.schema();
    let candidate_nullable = candidates
        .fields()
        .first()
        .map(|field| field.is_nullable())
        // No column to read means a shape this cannot judge, so it does not.
        .unwrap_or(true);
    left_nullable || candidate_nullable
}

/// `0A000`, naming the shape refused and the spelling that expresses it — including the part
/// of that spelling the client has to supply, since `NOT EXISTS` is two-valued and this
/// predicate is not.
fn unsupported(in_subquery: &InSubquery) -> PgWireError {
    let shape = if in_subquery.subquery.outer_ref_columns.is_empty() {
        "NOT IN (subquery) in this position"
    } else {
        "NOT IN (correlated subquery)"
    };
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{shape} is not supported when either side may be NULL: PostgreSQL's NOT IN is \
             NULL — and so not true — when the compared value or any candidate is NULL, and \
             the cluster's anti join reads a NULL as merely unequal, so it would return rows \
             PostgreSQL excludes. Spell it as NOT EXISTS (SELECT 1 FROM … WHERE … = …), \
             adding the IS NULL test that says what a NULL should mean there, since \
             NOT EXISTS is two-valued. An uncorrelated NOT IN, or one over NOT NULL columns, \
             is answered as written"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    /// `l(id int4 NOT NULL, k int4, v text)` and `r(id int4 NOT NULL, k int4, w text)` — the
    /// pair the e2e `NOT IN` tests use, with `id` non-nullable so the two-valued case is
    /// reachable.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for (name, key) in [("l", "v"), ("r", "w")] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
                Field::new(key, DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(Int32Array::from(vec![10, 20])),
                    Arc::new(StringArray::from(vec!["a", "b"])),
                ],
            )
            .unwrap();
            ctx.register_batch(name, batch).unwrap();
        }
        ctx
    }

    /// The verdict on `sql` at the point `plan_select` asks for it: planned, not yet
    /// coerced. `Err` carries the client-facing message.
    async fn verdict(sql: &str) -> Result<(), String> {
        let ctx = ctx();
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        reject_null_unaware_not_in(&plan).map_err(|e| e.to_string())
    }

    async fn refusal(sql: &str) -> String {
        verdict(sql)
            .await
            .expect_err(&format!("`{sql}` must be refused"))
    }

    async fn accepted(sql: &str) {
        if let Err(e) = verdict(sql).await {
            panic!("`{sql}` must be accepted, got: {e}");
        }
    }

    /// The gap: a correlated `NOT IN` over a nullable key, which answered one row too many.
    #[tokio::test]
    async fn a_correlated_not_in_over_a_nullable_key_is_refused() {
        let message =
            refusal("SELECT id FROM l WHERE k NOT IN (SELECT k FROM r WHERE r.id > l.id)").await;
        assert!(
            message.contains("correlated subquery"),
            "should name the shape: {message}"
        );
    }

    /// The message has to carry the rewrite *and* its caveat: `NOT EXISTS` alone is not the
    /// same predicate, and a client that took it as one would keep the wrong answer.
    #[tokio::test]
    async fn the_refusal_names_not_exists_and_the_null_test_it_needs() {
        let message =
            refusal("SELECT id FROM l WHERE k NOT IN (SELECT k FROM r WHERE r.id > l.id)").await;
        assert!(
            message.contains("NOT EXISTS"),
            "should name the spelling that works: {message}"
        );
        assert!(
            message.contains("IS NULL"),
            "should say the spelling needs a NULL test: {message}"
        );
    }

    /// Nullability and not shape: over `NOT NULL` columns the anti join is exactly
    /// PostgreSQL's answer, so the same correlated shape is answered as written.
    #[tokio::test]
    async fn a_correlated_not_in_over_non_nullable_columns_is_accepted() {
        accepted("SELECT id FROM l WHERE id NOT IN (SELECT id FROM r WHERE r.k = l.k)").await;
    }

    /// One nullable side is enough, on either side.
    #[tokio::test]
    async fn one_nullable_side_is_enough_to_refuse() {
        refusal("SELECT id FROM l WHERE id NOT IN (SELECT k FROM r WHERE r.id > l.id)").await;
        refusal("SELECT id FROM l WHERE k NOT IN (SELECT id FROM r WHERE r.id > l.id)").await;
    }

    /// A `CASE` is a position where NULL and false are distinguishable, so the AST rewrite
    /// leaves the predicate alone and this refuses it. Measured on the cluster returning the
    /// extra row before the refusal existed.
    #[tokio::test]
    async fn a_not_in_inside_a_case_is_refused() {
        refusal(
            "SELECT id FROM l WHERE (CASE WHEN k NOT IN (SELECT k FROM r) THEN true \
             ELSE false END)",
        )
        .await;
    }

    /// A wildcard subquery, whose column count the AST rewrite cannot know.
    #[tokio::test]
    async fn a_wildcard_subquery_is_refused() {
        refusal("SELECT id FROM l WHERE k NOT IN (SELECT * FROM (SELECT k FROM r) AS x)").await;
    }

    /// The exemption: a double negation is planned as a semi join, which is null-correct
    /// already. The inner `NOT IN` is over nullable columns, so only the exemption can keep
    /// this accepted.
    #[tokio::test]
    async fn a_not_in_under_a_not_is_accepted() {
        accepted("SELECT id FROM l WHERE NOT (k NOT IN (SELECT k FROM r))").await;
    }

    /// The positive `IN` is two-valued in PostgreSQL too — a NULL candidate can only turn a
    /// false into a NULL, and neither is returned — so it is never this check's business.
    #[tokio::test]
    async fn in_is_never_refused() {
        accepted("SELECT id FROM l WHERE k IN (SELECT k FROM r)").await;
        accepted("SELECT id FROM l WHERE k IN (SELECT k FROM r WHERE r.id > l.id)").await;
    }

    /// The shapes the AST rewrite does respell reach this check as `NOT EXISTS` and
    /// `count(*)`, with no `InSubquery` left to judge — so the two halves do not overlap.
    #[tokio::test]
    async fn a_respelled_not_in_has_nothing_left_to_refuse() {
        // `parse_sql` is where the respelling happens, so this is the statement the read
        // path would actually plan — an uncorrelated `NOT IN` over a nullable key, which
        // this check would refuse if the rewrite had not already taken it.
        let sql = "SELECT id FROM l WHERE k NOT IN (SELECT k FROM r)";
        let stmt = crate::pgwire_handler::parser::parse_sql(sql)
            .unwrap()
            .pop()
            .unwrap();
        assert!(
            !stmt.to_string().contains("NOT IN"),
            "`{sql}` should be respelled, got: {stmt}"
        );
        let plan = ctx()
            .state()
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                stmt,
            )))
            .await
            .unwrap();
        assert!(reject_null_unaware_not_in(&plan).is_ok());
    }

    /// A `NOT IN` in a select list is not this check's business either: it is answered, by
    /// [`super::super::pg_projection_subqueries`], with the NULL rules written into the
    /// expression rather than left to a join. Refusing it here would take away a query that
    /// works.
    #[tokio::test]
    async fn a_not_in_in_a_select_list_is_left_to_the_select_list_rewrite() {
        accepted("SELECT id, k NOT IN (SELECT k FROM r) FROM l").await;
    }
}
