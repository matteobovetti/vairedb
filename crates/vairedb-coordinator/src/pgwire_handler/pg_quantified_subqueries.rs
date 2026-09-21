//! Lower `x <op> ANY | SOME | ALL (subquery)` onto `EXISTS` / `NOT EXISTS`, the one shape
//! of it the cluster can both plan and ship.
//!
//! ```text
//! l(id int4 NOT NULL, k int4) = (1, 1), (2, 2), (3, NULL), (4, 4)
//! r(id int4 NOT NULL, k int4) = (2, 2), (3, NULL)
//!
//! SELECT id FROM l WHERE id > ANY (SELECT id FROM r)
//! PostgreSQL  3, 4
//! DataFusion  3, 4 in-process — but the optimized plan does not deserialize, so on a
//!             cluster the query fails: `Schema contains duplicate unqualified field
//!             name mark`
//! after       3, 4, and the plan round-trips
//!
//! SELECT id FROM l WHERE k > ALL (SELECT k FROM r)
//! PostgreSQL  (no rows)          -- 4 > 2 is true but 4 > NULL is NULL, so ALL is NULL
//! DataFusion  ProjectionPushdown internal error: `Input field name k does not match
//!             with the projection expression id`
//! after       (no rows)
//! ```
//!
//! ## Why lowering, and not the planner's own decorrelation
//!
//! DataFusion has an `Expr::SetComparison` and decorrelates it into three stacked
//! `LeftMark` joins — one for "some row compares true", one for "some row compares NULL",
//! one for the operand — which is the right three-valued reading and answers correctly for
//! `ANY` in process. Two things then go wrong, and both were measured rather than inferred:
//!
//! * **It does not survive serialization.** Every mark join contributes a column called
//!   `mark`. While the subquery keeps its `SubqueryAlias` those columns are distinct
//!   (`__correlated_sq_1.mark`, `__correlated_sq_2.mark`, …), but when a branch is provably
//!   empty — which is exactly what "the subquery has a NULL" is over a `NOT NULL` column —
//!   `propagate_empty_relation` replaces the alias with a bare `EmptyRelation` and the
//!   `mark` it adds loses its qualifier. The plan is still fine in memory; it is
//!   `logical_plan_from_bytes` that rejects it, and Ballista serializes the **optimized**
//!   logical plan. So the shape that breaks is not exotic: it is any quantified comparison
//!   whose subquery column is `NOT NULL`.
//! * **`ALL` does not execute.** `ProjectionPushdown` mis-maps the columns of a `LeftMark`
//!   join correlated by a filter rather than by an equality, and every `ALL` plan has one.
//!   The same bug is reachable without `ALL` at all — a plain non-equi correlated `EXISTS`
//!   under an `OR` hits it too — so it is not something a rewrite of this expression could
//!   route around in general.
//!
//! Lowering sidesteps both by never building a mark join. A single `LeftSemi` (for `ANY`)
//! or `LeftAnti` (for `ALL`) join carries no `mark` column to collide, and both shapes are
//! already the cluster's daily bread: they are what `INTERSECT` and `EXCEPT` lower to.
//!
//! ## Why the lowering is exact
//!
//! Not because `ANY` is two-valued — it is not — but because of **where** it is lowered. A
//! predicate is only lowered in a position that already reads NULL as "not true": a
//! `WHERE`, `HAVING`, `QUALIFY` or join-`ON` predicate, and there only as an operand of a
//! top-level `AND` chain. In such a position, for a subquery `q` over candidate `c`:
//!
//! ```text
//! x <op> ANY (q)  passes  ⟺  some row of q makes `x <op> c` true
//!                         ⟺  EXISTS (SELECT 1 FROM q WHERE (x <op> c) IS TRUE)
//!
//! x <op> ALL (q)  passes  ⟺  no row of q makes `x <op> c` anything but true
//!                         ⟺  NOT EXISTS (SELECT 1 FROM q WHERE (x <op> c) IS NOT TRUE)
//! ```
//!
//! Both equivalences hold for every comparison operator, for a NULL `x`, for a NULL
//! candidate and for an empty `q` — an empty `q` makes the `EXISTS` false, which is
//! PostgreSQL's `ANY`, and the `NOT EXISTS` true, which is PostgreSQL's `ALL`. The
//! `IS TRUE` / `IS NOT TRUE` guards are what carry the three-valued reading into a
//! two-valued join, and dropping them would be the classic mistake: `NOT EXISTS (… WHERE
//! NOT (x <op> c))` keeps a row PostgreSQL's `ALL` rejects whenever a candidate is NULL.
//!
//! One position inside that set is withheld all the same: a join `ON` predicate whose
//! left-hand side names its column through a **table alias**. The lowering is exact there
//! too, but the plan DataFusion builds from the `EXISTS` is not — see
//! [`aliased_relations`] — and a hand-written `EXISTS` fails the same way, so the shape is
//! refused by name rather than answered wrongly or left to a schema error.
//!
//! Outside such a position the collapse is not valid — under a `NOT`, inside a `CASE`, in a
//! select list, in `ORDER BY` — and there the expression is refused rather than lowered.
//! An `OR` is refused for a different reason: NULL and false *are* interchangeable under
//! one, but the plan DataFusion then builds is the mark join above, with the two defects
//! above, so allowing it would only move the failure later. See
//! [`reject_unlowered_quantified_subqueries`].
//!
//! ## What never reaches here
//!
//! `= ANY (subquery)` and `<> ALL (subquery)` — the two spellings that are just `IN` and
//! `NOT IN` — are respelled on the AST by
//! [`super::compat_rewrite::normalize_any_all_subqueries`], in every position, before the
//! planner sees them. So they keep working in a select list or a `CASE`, where this module
//! refuses; and the `NOT IN` NULL rules stay the business of
//! [`super::pg_not_in_nulls`]. What is left for this module is the ordering operators and
//! the two equality spellings that are not `IN`: `= ALL` and `<> ANY`.
//!
//! ## Where it runs
//!
//! In [`super::parser::plan_select`], on the planned but not yet optimized plan, and only
//! there: decorrelation is the pass that turns `SetComparison` into the mark joins, so
//! afterwards there is no `SetComparison` left to lower. It runs before `coerce_types` so
//! that the comparison it builds is type-checked by the analyzer like any other expression.

use std::sync::Arc;

use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion::common::{Column, DFSchema, Result as DFResult, TableReference};
use datafusion::logical_expr::expr::{Exists, SetComparison, SetQuantifier};
use datafusion::logical_expr::{
    BinaryExpr, Expr, Filter, Join, LogicalPlan, LogicalPlanBuilder, Operator, Subquery,
};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// Rewrite every quantified comparison in a NULL-insensitive predicate position of `plan`
/// into the equivalent `EXISTS` / `NOT EXISTS`.
///
/// Leaves every other occurrence untouched, for [`reject_unlowered_quantified_subqueries`]
/// to refuse.
pub(crate) fn lower_quantified_subqueries(plan: LogicalPlan) -> DFResult<LogicalPlan> {
    // `_with_subqueries`, so a quantified comparison inside a subquery or a CTE is lowered
    // too: it is the same plan defect wherever the predicate sits.
    plan.transform_down_with_subqueries(|node| match node {
        // The two nodes whose predicate is read as "keep the row if this is true", which is
        // the property the lowering rests on. A `WHERE`, `HAVING` and `QUALIFY` are all a
        // `Filter`; a join's `ON` is the `Join`'s own filter.
        LogicalPlan::Filter(filter) => {
            let Filter {
                predicate, input, ..
            } = filter;
            let lowered = lower_conjunction(predicate, input.schema(), &[])?;
            if !lowered.transformed {
                return Ok(Transformed::no(LogicalPlan::Filter(Filter::try_new(
                    lowered.data,
                    input,
                )?)));
            }
            Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                lowered.data,
                input,
            )?)))
        }
        LogicalPlan::Join(join) => {
            let Some(filter) = join.filter else {
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            };
            // The join's own schema, because an `ON` predicate may name a column of either
            // side and only the output schema holds both. The aliases are what the lowering
            // must *not* correlate through — see [`aliased_relations`].
            let mut aliased = aliased_relations(&join.left);
            aliased.append(&mut aliased_relations(&join.right));
            let lowered = lower_conjunction(filter, &join.schema, &aliased)?;
            let transformed = lowered.transformed;
            let join = LogicalPlan::Join(Join {
                filter: Some(lowered.data),
                ..join
            });
            Ok(if transformed {
                Transformed::yes(join)
            } else {
                Transformed::no(join)
            })
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
}

/// Lower the quantified comparisons among `predicate`'s top-level `AND` operands.
///
/// Descends through `AND` and nothing else. Every other operator either changes what a NULL
/// means (`NOT`, `CASE`) or leaves DataFusion's own decorrelation in charge (`OR`), and in
/// both cases the expression is left for the refusal to name.
fn lower_conjunction(
    predicate: Expr,
    schema: &DFSchema,
    aliased: &[TableReference],
) -> DFResult<Transformed<Expr>> {
    match predicate {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let left = lower_conjunction(*left, schema, aliased)?;
            let right = lower_conjunction(*right, schema, aliased)?;
            let transformed = left.transformed || right.transformed;
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left.data),
                Operator::And,
                Box::new(right.data),
            ));
            Ok(if transformed {
                Transformed::yes(expr)
            } else {
                Transformed::no(expr)
            })
        }
        Expr::SetComparison(comparison) => match lower_set_comparison(comparison, schema, aliased)?
        {
            Ok(exists) => Ok(Transformed::yes(exists)),
            Err(comparison) => Ok(Transformed::no(Expr::SetComparison(comparison))),
        },
        other => Ok(Transformed::no(other)),
    }
}

/// Build the `EXISTS` / `NOT EXISTS` equivalent of one quantified comparison.
///
/// Returns the comparison back, unchanged, for the shapes it cannot express — a subquery
/// that does not project exactly one column, a compared expression that itself holds a
/// subquery, a column this schema cannot type, or a column reached through one of
/// `aliased`. Each of those then reaches the client as a refusal rather than as a plan that
/// might answer wrongly.
fn lower_set_comparison(
    comparison: SetComparison,
    schema: &DFSchema,
    aliased: &[TableReference],
) -> DFResult<Result<Expr, SetComparison>> {
    let SetComparison {
        expr,
        subquery,
        op,
        quantifier,
    } = comparison;
    let candidates = subquery.subquery.schema();
    if candidates.fields().len() != 1 {
        return Ok(Err(SetComparison::new(expr, subquery, op, quantifier)));
    }
    // A subquery of its own inside the compared expression would have to be carried into the
    // `EXISTS` as an outer reference, which is not something to construct mechanically.
    if expr.exists(|e| Ok(is_subquery(e)))? {
        return Ok(Err(SetComparison::new(expr, subquery, op, quantifier)));
    }
    // A column reached through a table alias in a join input. See [`aliased_relations`].
    if expr.column_refs().iter().any(|column| {
        column
            .relation
            .as_ref()
            .is_some_and(|r| aliased.contains(r))
    }) {
        return Ok(Err(SetComparison::new(expr, subquery, op, quantifier)));
    }
    let Some((compared, outer_refs)) = outer_reference(expr.as_ref().clone(), schema) else {
        return Ok(Err(SetComparison::new(expr, subquery, op, quantifier)));
    };
    let candidate = Expr::Column(Column::from(candidates.qualified_field(0)));

    // The guard is the whole of the three-valued reading: `ANY` keeps a row on a candidate
    // that compares *true*, `ALL` rejects one on a candidate that compares anything else.
    let comparison = Expr::BinaryExpr(BinaryExpr::new(Box::new(compared), op, Box::new(candidate)));
    let (guard, negated) = match quantifier {
        SetQuantifier::Any => (comparison.is_true(), false),
        SetQuantifier::All => (comparison.is_not_true(), true),
    };

    let Subquery {
        subquery: plan,
        outer_ref_columns,
        spans,
    } = subquery;
    let plan = LogicalPlanBuilder::from(Arc::unwrap_or_clone(plan))
        .filter(guard)?
        .build()?;
    // The subquery's own correlations come first and are kept: the compared expression's
    // columns are additional outer references, not a replacement for them.
    let mut outer_ref_columns = outer_ref_columns;
    for reference in outer_refs {
        if !outer_ref_columns.contains(&reference) {
            outer_ref_columns.push(reference);
        }
    }
    Ok(Ok(Expr::Exists(Exists::new(
        Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns,
            spans,
        },
        negated,
    ))))
}

/// Restate `expr` as it must read from *inside* the subquery: every column of the outer
/// query becomes an outer reference, and the references are collected for the [`Subquery`].
///
/// `None` when a column cannot be typed against `schema`, which is the case for a reference
/// that is already correlated to a further-out query — a shape this does not rewrite.
fn outer_reference(expr: Expr, schema: &DFSchema) -> Option<(Expr, Vec<Expr>)> {
    let mut outer_refs = Vec::new();
    let rewritten = expr
        .transform_up(|e| match e {
            Expr::Column(column) => {
                let index = schema
                    .index_of_column(&column)
                    .map_err(|_| missing_column())?;
                let field = Arc::clone(&schema.fields()[index]);
                let reference = Expr::OuterReferenceColumn(field, column);
                if !outer_refs.contains(&reference) {
                    outer_refs.push(reference.clone());
                }
                Ok(Transformed::yes(reference))
            }
            other => Ok(Transformed::no(other)),
        })
        .data()
        .ok()?;
    Some((rewritten, outer_refs))
}

/// The sentinel that aborts [`outer_reference`]; never reaches a client.
fn missing_column() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Internal(
        "quantified comparison operand is not a column of the enclosing plan".to_string(),
    )
}

/// Every relation name introduced by a `SubqueryAlias` inside `plan` — a table alias, a
/// derived table's name, a CTE reference.
///
/// These are the qualifiers a quantified comparison in a join `ON` clause must not correlate
/// through, and the reason is upstream rather than here. DataFusion's
/// `decorrelate_predicate_subquery` rewrites a join filter by pushing the semi join it builds
/// *under* the aliased input while the filter it carries keeps the outer qualifier, so the
/// plan optimizes and then fails to execute:
///
/// ```text
/// SELECT a.id FROM l a JOIN r b ON a.id = b.id AND a.k < ANY (SELECT k FROM r)
///
/// Inner Join: a.id = b.id
///   SubqueryAlias: a
///     LeftSemi Join:  Filter: a.k < __correlated_sq_1.k IS TRUE   -- `a` is not in scope here
///       TableScan: l projection=[id]                             -- and `k` has been pruned
///
/// Schema error: No field named a.k. Valid fields are __correlated_sq_1.k.
/// ```
///
/// Not this module's defect: a **hand-written** correlated `EXISTS` in the same position
/// fails identically, and the same statement with the alias dropped answers. So the shape is
/// left unlowered for [`reject_unlowered_quantified_subqueries`] to refuse by name, which
/// turns a schema error that reads like a mistake in the client's SQL into `0A000` naming the
/// two spellings that work — drop the alias, or lift the comparison into the `WHERE` clause.
fn aliased_relations(plan: &LogicalPlan) -> Vec<TableReference> {
    let mut aliases = Vec::new();
    // Not `_with_subqueries`: a name introduced inside a subquery is not a qualifier the
    // join's `ON` predicate can reach.
    let _ = plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node {
            aliases.push(alias.alias.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    aliases
}

/// Whether `expr` carries a subquery of its own.
fn is_subquery(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_) | Expr::SetComparison(_)
    )
}

/// Refuse every quantified comparison [`lower_quantified_subqueries`] did not lower.
///
/// Reads the plan and never rewrites it: the outcome is either the same plan or `0A000`.
/// Run *after* the lowering, on the same plan, so that what it sees is exactly the residue.
pub(crate) fn reject_unlowered_quantified_subqueries(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut visitor = UnloweredQuantifier { error: None };
    let _ = plan.visit_with_subqueries(&mut visitor);
    match visitor.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

struct UnloweredQuantifier {
    error: Option<PgWireError>,
}

impl<'n> TreeNodeVisitor<'n> for UnloweredQuantifier {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n LogicalPlan) -> DFResult<TreeNodeRecursion> {
        // A residue on a `Join` is one shape and one shape only: a comparison correlated
        // through a table alias, which `lower_quantified_subqueries` leaves alone because the
        // plan it would build does not execute. It gets its own wording, because the general
        // message says a join `ON` *is* answered — and it is, without the alias.
        let in_join = matches!(node, LogicalPlan::Join(_));
        let mut found = None;
        node.apply_expressions(|expr| {
            let _ = expr.apply(|e| {
                if let Expr::SetComparison(comparison) = e {
                    found = Some(match in_join {
                        true => unsupported_through_an_alias(comparison),
                        false => unsupported(comparison),
                    });
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });
            Ok(match found {
                Some(_) => TreeNodeRecursion::Stop,
                None => TreeNodeRecursion::Continue,
            })
        })?;
        if let Some(e) = found {
            self.error = Some(e);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// `0A000`, naming the position that is answered and the `EXISTS` spelling that reaches it
/// from anywhere — including the `IS TRUE` guard, since a client that drops it gets a
/// different answer whenever a candidate is NULL.
fn unsupported(comparison: &SetComparison) -> PgWireError {
    let SetComparison { op, quantifier, .. } = comparison;
    let (equivalent, guard) = match quantifier {
        SetQuantifier::Any => ("EXISTS", "IS TRUE"),
        SetQuantifier::All => ("NOT EXISTS", "IS NOT TRUE"),
    };
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "x {op} {quantifier} (subquery) is not supported in this position: it is answered \
             as a WHERE, HAVING, QUALIFY or join ON predicate, and there as an operand of the \
             top-level AND chain, because only a position that already reads NULL as not-true \
             can be lowered to a join the cluster can ship. Spell it as \
             {equivalent} (SELECT 1 FROM … WHERE (x {op} c) {guard}) — keeping the {guard}, \
             which is what makes it agree with {quantifier} when a candidate is NULL — or \
             lift the comparison into a WHERE clause. = ANY (subquery) and \
             <> ALL (subquery) are answered in every position, as IN and NOT IN"
        ),
    )
}

/// `0A000` for a comparison in a join `ON` clause that names its operand through a table
/// alias — the one shape [`aliased_relations`] withholds from the lowering.
fn unsupported_through_an_alias(comparison: &SetComparison) -> PgWireError {
    let SetComparison { op, quantifier, .. } = comparison;
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "x {op} {quantifier} (subquery) is not supported in a join ON clause when its \
             left-hand side names a column through a table alias, a derived table or a CTE: \
             the correlated join that would answer it is built under that alias and cannot see \
             it. Two spellings answer — name the column through the table itself, dropping the \
             alias, or move the comparison out of the ON clause into the WHERE clause, which \
             filters the join's output and reads the same way for an inner join"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Extension;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    };
    use datafusion_proto::logical_plan::LogicalExtensionCodec;

    /// `l(id int4 NOT NULL, k int4)` and `r(id int4 NOT NULL, k int4)` — the pair the e2e
    /// quantified-subquery tests use.
    ///
    /// ```text
    /// l = (1, 1), (2, 2), (3, NULL), (4, 4)
    /// r = (2, 2), (3, NULL)
    /// ```
    ///
    /// `id` is non-nullable so the two-valued case is reachable, `k` nullable so the
    /// three-valued one is, and `r.k` holds a NULL so that "some candidate is NULL" — the
    /// case a two-valued rewrite gets wrong — is reachable from every comparison.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = || {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
            ]))
        };
        let l = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
            ],
        )
        .unwrap();
        let r = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3])),
                Arc::new(Int32Array::from(vec![Some(2), None])),
            ],
        )
        .unwrap();
        ctx.register_batch("l", l).unwrap();
        ctx.register_batch("r", r).unwrap();
        ctx
    }

    /// Plan `sql`, lower it, and refuse what is left — the two passes in the order
    /// [`super::super::parser::plan_select`] runs them.
    async fn lowered(ctx: &SessionContext, sql: &str) -> PgWireResult<LogicalPlan> {
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("statement plans");
        let plan = lower_quantified_subqueries(plan).expect("lowering does not fail");
        reject_unlowered_quantified_subqueries(&plan)?;
        Ok(plan)
    }

    /// The `id`s `sql` answers, sorted, with the whole read path's passes applied.
    async fn answer(sql: &str) -> Vec<i32> {
        let ctx = ctx();
        let plan = lowered(&ctx, sql).await.expect("not refused");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("plans physically")
            .collect()
            .await
            .expect("executes");
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is int4");
                (0..column.len())
                    .map(|i| column.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The `0A000` message `sql` is refused with.
    async fn refusal(sql: &str) -> String {
        let ctx = ctx();
        match lowered(&ctx, sql).await {
            Ok(plan) => panic!("expected a refusal, planned:\n{}", plan.display_indent()),
            Err(e) => e.to_string(),
        }
    }

    /// The optimized plan of `sql`, which is the plan Ballista serializes.
    async fn optimized(sql: &str) -> String {
        let ctx = ctx();
        let plan = lowered(&ctx, sql).await.expect("not refused");
        ctx.state()
            .optimize(&plan)
            .expect("optimizes")
            .display_indent()
            .to_string()
    }

    /// A codec for the in-memory tables these tests register, standing in for the
    /// [`crate::scheduler::logical_codec::VaireLogicalCodec`] the cluster ships plans with.
    /// Only the table provider matters: what is under test is whether the *plan's schema*
    /// survives a round trip.
    #[derive(Debug)]
    struct MemTableCodec;

    impl LogicalExtensionCodec for MemTableCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> DFResult<Extension> {
            unimplemented!("these plans carry no extension node")
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> DFResult<()> {
            unimplemented!("these plans carry no extension node")
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _table_ref: &TableReference,
            schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> DFResult<Arc<dyn TableProvider>> {
            Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            _node: Arc<dyn TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> DFResult<()> {
            Ok(())
        }
    }

    /// Whether the optimized plan of `sql` survives the serialization Ballista puts it
    /// through on its way to the scheduler.
    async fn round_trips(sql: &str) -> Result<(), String> {
        let ctx = ctx();
        let plan = lowered(&ctx, sql).await.expect("not refused");
        let optimized = ctx.state().optimize(&plan).map_err(|e| e.to_string())?;
        let bytes = logical_plan_to_bytes_with_extension_codec(&optimized, &MemTableCodec)
            .map_err(|e| e.to_string())?;
        logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &MemTableCodec)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    // ---- the plan the lowering produces -------------------------------------------------

    #[tokio::test]
    async fn any_becomes_a_semi_join_carrying_no_mark_column() {
        let plan = optimized("SELECT id FROM l WHERE k > ANY (SELECT k FROM r)").await;
        assert!(plan.contains("LeftSemi Join"), "{plan}");
        assert!(!plan.contains("mark"), "{plan}");
    }

    #[tokio::test]
    async fn all_becomes_an_anti_join_carrying_no_mark_column() {
        let plan = optimized("SELECT id FROM l WHERE k > ALL (SELECT k FROM r)").await;
        assert!(plan.contains("LeftAnti Join"), "{plan}");
        assert!(!plan.contains("mark"), "{plan}");
    }

    /// The regression this module exists for: over a `NOT NULL` subquery column the planner's
    /// own decorrelation produced a plan that failed to deserialize with `Schema contains
    /// duplicate unqualified field name mark`, so the query failed on a cluster while
    /// answering correctly in process.
    #[tokio::test]
    async fn the_optimized_plan_reaches_an_executor() {
        for sql in [
            "SELECT id FROM l WHERE id > ANY (SELECT id FROM r)",
            "SELECT id FROM l WHERE id > ALL (SELECT id FROM r)",
            "SELECT id FROM l WHERE id = ALL (SELECT id FROM r)",
            "SELECT id FROM l WHERE id <> ANY (SELECT id FROM r)",
            "SELECT id FROM l WHERE k > ANY (SELECT k FROM r)",
            "SELECT id FROM l WHERE k > ALL (SELECT k FROM r)",
            "SELECT id FROM l WHERE k > ANY (SELECT k FROM r WHERE false)",
        ] {
            assert_eq!(round_trips(sql).await, Ok(()), "{sql}");
        }
    }

    // ---- the answers, against PostgreSQL 17 ---------------------------------------------

    #[tokio::test]
    async fn any_is_null_when_no_candidate_matches_and_one_is_null() {
        // 1 > 2 is false and 1 > NULL is NULL, so ANY is NULL for every row but 4.
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ANY (SELECT k FROM r)").await,
            vec![4]
        );
    }

    #[tokio::test]
    async fn all_is_null_when_every_candidate_matches_but_one_is_null() {
        // 4 > 2 is true but 4 > NULL is NULL, so ALL is NULL and no row qualifies. A
        // two-valued rewrite — `NOT EXISTS (… WHERE NOT (k > c))` — answers `4` here.
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ALL (SELECT k FROM r)").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE k = ALL (SELECT k FROM r)").await,
            Vec::<i32>::new()
        );
    }

    #[tokio::test]
    async fn a_two_valued_comparison_answers_as_written() {
        assert_eq!(
            answer("SELECT id FROM l WHERE id > ANY (SELECT id FROM r)").await,
            vec![3, 4]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE id > ALL (SELECT id FROM r)").await,
            vec![4]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE id >= ALL (SELECT id FROM r)").await,
            vec![3, 4]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE id = ALL (SELECT id FROM r)").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE id <> ANY (SELECT id FROM r)").await,
            vec![1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn a_nulls_first_comparison_still_finds_the_true_candidate() {
        // `1 <> 2` is true, so the NULL candidate never has to be reached.
        assert_eq!(
            answer("SELECT id FROM l WHERE k <> ANY (SELECT k FROM r)").await,
            vec![1, 4]
        );
    }

    /// The case with no rewrite in terms of aggregates: an empty subquery is false for `ANY`
    /// and true for `ALL`, whatever the operand is — including a NULL one.
    #[tokio::test]
    async fn an_empty_subquery_is_false_for_any_and_true_for_all() {
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ANY (SELECT k FROM r WHERE false)").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ALL (SELECT k FROM r WHERE false)").await,
            vec![1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn a_correlated_subquery_is_answered() {
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ANY (SELECT k FROM r WHERE r.id < l.id)").await,
            vec![4]
        );
        // For ids 1 and 2 the subquery is empty, which `ALL` reads as true.
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ALL (SELECT k FROM r WHERE r.id < l.id)").await,
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn a_having_predicate_is_answered() {
        assert_eq!(
            answer("SELECT id FROM l GROUP BY id HAVING max(k) > ANY (SELECT k FROM r)").await,
            vec![4]
        );
    }

    #[tokio::test]
    async fn a_join_on_predicate_is_answered() {
        assert_eq!(
            answer(
                "SELECT l.id FROM l JOIN r AS r2 ON l.id > r2.id \
                 AND l.id > ANY (SELECT id FROM r)"
            )
            .await,
            vec![3, 4, 4]
        );
    }

    #[tokio::test]
    async fn a_join_on_predicate_naming_an_alias_is_refused() {
        // Both join flavours, and either input's alias: `decorrelate_predicate_subquery`
        // builds the semi join under the alias in every one of them.
        for sql in [
            "SELECT a.id FROM l a JOIN r b ON a.id = b.id AND a.k < ANY (SELECT k FROM r)",
            "SELECT a.id FROM l a JOIN r b ON a.id > b.id AND a.k < ANY (SELECT k FROM r)",
            "SELECT a.id FROM l a JOIN r b ON a.id = b.id AND b.k < ANY (SELECT k FROM l)",
        ] {
            let message = refusal(sql).await;
            assert!(message.contains("0A000"), "{sql}: {message}");
            assert!(message.contains("join ON clause"), "{sql}: {message}");
            assert!(message.contains("table alias"), "{sql}: {message}");
            assert!(message.contains("WHERE clause"), "{sql}: {message}");
        }
    }

    #[tokio::test]
    async fn the_alias_refusal_reaches_no_further_than_the_on_clause() {
        // The same alias in a `WHERE` is answered — that is one of the two spellings the
        // refusal names, so it has to work.
        assert_eq!(
            answer("SELECT a.id FROM l a WHERE a.k > ANY (SELECT k FROM r)").await,
            vec![4]
        );
        assert_eq!(
            answer(
                "SELECT a.id FROM l a JOIN r b ON a.id = b.id \
                 WHERE a.k > ANY (SELECT k FROM r)"
            )
            .await,
            Vec::<i32>::new()
        );
        // And so is the other: the join `ON` without the alias.
        assert_eq!(
            answer("SELECT l.id FROM l JOIN r ON l.id = r.id AND l.k > ANY (SELECT k FROM r)")
                .await,
            Vec::<i32>::new()
        );
        // An `ON` predicate that reaches no aliased column at all is still lowered even
        // though the *other* side of the join carries an alias.
        assert_eq!(
            answer(
                "SELECT l.id FROM l JOIN r AS r2 ON l.id = r2.id \
                 AND l.id > ANY (SELECT id FROM r)"
            )
            .await,
            vec![3]
        );
    }

    #[tokio::test]
    async fn a_conjunct_beside_the_comparison_is_kept() {
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ANY (SELECT k FROM r) AND id = 4").await,
            vec![4]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE k > ANY (SELECT k FROM r) AND id = 1").await,
            Vec::<i32>::new()
        );
    }

    // ---- what is refused, and what the refusal does not reach ---------------------------

    #[tokio::test]
    async fn a_select_list_comparison_is_refused() {
        let message = refusal("SELECT k > ANY (SELECT k FROM r) FROM l").await;
        assert!(message.contains("0A000"), "{message}");
        assert!(message.contains("> ANY (subquery)"), "{message}");
        assert!(message.contains("IS TRUE"), "{message}");
    }

    #[tokio::test]
    async fn an_order_by_comparison_is_refused() {
        let message = refusal("SELECT id FROM l ORDER BY (k > ANY (SELECT k FROM r))").await;
        assert!(message.contains("0A000"), "{message}");
    }

    /// Under a `NOT` the collapse this module relies on is invalid — `NOT NULL` is NULL and
    /// drops the row, while `NOT false` keeps it — so the shape is refused rather than
    /// lowered.
    #[tokio::test]
    async fn a_negated_comparison_is_refused() {
        let message = refusal("SELECT id FROM l WHERE NOT (k > ANY (SELECT k FROM r))").await;
        assert!(message.contains("0A000"), "{message}");
    }

    #[tokio::test]
    async fn a_comparison_inside_a_case_is_refused() {
        let message = refusal(
            "SELECT id FROM l WHERE CASE WHEN k > ANY (SELECT k FROM r) THEN true ELSE false END",
        )
        .await;
        assert!(message.contains("0A000"), "{message}");
    }

    /// A NULL and a false *are* interchangeable under an `OR`, but the plan the planner then
    /// builds is the mark join that does not ship, so allowing it would move the failure to
    /// the cluster.
    #[tokio::test]
    async fn a_disjunct_comparison_is_refused() {
        let message = refusal("SELECT id FROM l WHERE k > ANY (SELECT k FROM r) OR id = 1").await;
        assert!(message.contains("0A000"), "{message}");
        assert!(message.contains("AND chain"), "{message}");
    }

    /// The refusal must not spread to the neighbouring subquery forms, which lose no clause
    /// and are answered today.
    #[tokio::test]
    async fn the_refusal_does_not_reach_in_or_exists() {
        assert_eq!(
            answer("SELECT id FROM l WHERE k IN (SELECT k FROM r) OR id = 1").await,
            vec![1, 2]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.k = r.k) OR id = 1")
                .await,
            vec![1, 2]
        );
        assert_eq!(
            answer("SELECT id FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE l.k = r.k)").await,
            vec![1, 3, 4]
        );
    }

    /// A statement with no quantified comparison must come out of the pass byte-identical:
    /// the pass rebuilds every `Filter` and `Join` it walks, and a rebuild that changed a
    /// plan would change what Describe reports.
    #[tokio::test]
    async fn a_plan_without_a_quantified_comparison_is_unchanged() {
        let ctx = ctx();
        for sql in [
            "SELECT id FROM l WHERE k = 2",
            "SELECT l.id FROM l JOIN r ON l.id = r.id AND l.k > r.k",
            "SELECT id FROM l WHERE k IN (SELECT k FROM r)",
            "SELECT id, max(k) FROM l GROUP BY id HAVING max(k) > 1",
        ] {
            let before = ctx
                .state()
                .create_logical_plan(sql)
                .await
                .expect("statement plans");
            let after =
                lower_quantified_subqueries(before.clone()).expect("lowering does not fail");
            assert_eq!(
                before.display_indent_schema().to_string(),
                after.display_indent_schema().to_string(),
                "{sql}"
            );
        }
    }
}
