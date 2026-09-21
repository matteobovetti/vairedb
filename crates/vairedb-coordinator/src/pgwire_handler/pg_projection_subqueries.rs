//! Answer `EXISTS (q)` and `x IN (q)` in a **select list**, by respelling each one as the
//! `count(*)` scalar subquery that means the same thing.
//!
//! ```text
//! SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = t.n) FROM t
//!
//! PostgreSQL  a boolean column per row
//! VaireDB     XX000 Physical plan does not support logical expression Exists(…)
//! ```
//!
//! A reporting query that wants the membership test *as a column* rather than as a filter
//! writes exactly this, and PostgreSQL answers it. VaireDB did not, and the reason is that
//! nothing in the read path was ever prepared to:
//!
//! * `DecorrelatePredicateSubquery`, the rule that turns `EXISTS` and `IN` into a semi or anti
//!   join, only matches a `Filter`. In a projection the expression is left exactly as the
//!   planner built it.
//! * so it reaches the physical planner as an `Expr::Exists` / `Expr::InSubquery`, which has no
//!   physical form — `Physical plan does not support logical expression …`. On the cluster it
//!   does not even reach that far: datafusion-proto encodes neither variant
//!   (`to_proto.rs` lists both under "unsupported expr"), so the plan cannot be cut into
//!   stages.
//!
//! Both halves of that are about the *spelling*, not about the meaning. `Expr::ScalarSubquery`
//! **is** encoded by datafusion-proto, and `ScalarSubqueryToJoin` **does** rewrite a
//! correlated one in a projection into a left join. So the fix is to say the same thing with a
//! scalar subquery, before either pass runs.
//!
//! ## What it emits
//!
//! `EXISTS (q)` becomes `(SELECT count(*) FROM (q)) > 0`, and `NOT EXISTS (q)` the same with
//! `= 0`. `count(*)` with no `GROUP BY` returns a row for an empty input, so the comparison is
//! never NULL and the aggregate is above whatever `q` already is — a `LIMIT`, a `DISTINCT`, a
//! `GROUP BY … HAVING`, a `UNION` — which is why the wrapping cannot change the answer:
//! counting `q`'s rows is zero exactly when `q` has none.
//!
//! `x IN (q)` needs PostgreSQL's three-valued reading, which is `x = c1 OR x = c2 OR …`:
//!
//! * a candidate equal to `x` makes it **true**;
//! * otherwise a NULL anywhere in the disjunction makes it **NULL** — either because `x` is
//!   NULL and `q` returned at least one row, or because some candidate is NULL;
//! * otherwise **false**, including for an empty `q`, where the empty disjunction is false and
//!   a NULL `x` does not change that.
//!
//! so it becomes
//!
//! ```text
//! CASE WHEN (SELECT count(*) FROM (q) WHERE c = x) > 0            THEN true
//!      WHEN (SELECT count(*) FROM (q) WHERE c IS NULL) > 0
//!        OR (x IS NULL AND (SELECT count(*) FROM (q)) > 0)        THEN NULL
//!      ELSE false END
//! ```
//!
//! and `x NOT IN (q)` the same with `true` and `false` exchanged — which is where this module
//! also closes the select-list half of the `NOT IN` NULL rules that
//! [`super::pg_not_in_nulls`] holds for predicate positions.
//!
//! Two things about that shape are deliberate. The `x IS NULL` test is evaluated in the
//! **outer** query and not inside a subquery, and the null-candidate count carries no
//! correlation: a correlated `q1 IS NULL OR outer_ref(x) IS NULL` filter is not an equality,
//! so `ScalarSubqueryToJoin` cannot decorrelate it and the plan fails at physical planning.
//! Only the `c = x` filter is correlated, and an equality is exactly what decorrelation
//! accepts. And when nullability rules a branch out — a non-nullable `x`, a non-nullable
//! candidate, or both — that branch is not emitted at all, so the common case over `NOT NULL`
//! columns is one subquery and one comparison.
//!
//! ## What it refuses, and why each one
//!
//! * a `q` that is **already correlated by anything but an equality**. `count(*)` over
//!   `WHERE r.g <> t.id` is a correlated scalar subquery `ScalarSubqueryToJoin` declines, and
//!   what reaches the client then is `Physical plan does not support logical expression
//!   ScalarSubquery(…)` — the same error under a different name. Refusing it instead says so,
//!   and names the `LEFT JOIN … IS NOT NULL` spelling that does work.
//! * a correlated `q` whose own plan is more than a filtered projection — a `GROUP BY`, a
//!   `LIMIT`, a set operation, a subquery of its own. Measured: `count(*)` over a correlated
//!   grouped subquery optimizes and then fails, `Schema error: No field named
//!   __scalar_sq_1.g`. An **un**correlated `q` has no such limit and any shape is answered.
//! * a `q` that does not project exactly one column, for `IN`, since there is no single
//!   candidate to compare against.
//! * any position other than a select list. A correlated scalar subquery is rejected outright
//!   outside a `Projection`, `Filter` or `Aggregate` (`Correlated scalar subquery can only be
//!   used in Projection, Filter, Aggregate plan nodes`), and in a `GROUP BY` even an
//!   uncorrelated one fails at physical planning. So `ORDER BY EXISTS (q)` and
//!   `GROUP BY EXISTS (q)` stay refused, with the message naming the select-list alias that
//!   reaches both.
//!
//! [`reject_unlowered_projection_subqueries`] is the other half: whatever the lowering
//! declined reaches the client as `0A000` naming the workaround, rather than as the physical
//! planner's own wording.
//!
//! ## Where it runs
//!
//! In [`super::parser::plan_select`], on the planned but not yet optimized plan, after
//! [`super::pg_quantified_subqueries`] — which is the other pass that builds an `Expr::Exists`,
//! and builds it only in a predicate position this one leaves alone — and before
//! `coerce_types`, so that the comparisons and the `CASE` are type-checked by the analyzer
//! like any other expression.

use std::sync::Arc;

use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result as DFResult, ScalarValue};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator, Projection,
    Subquery, lit,
};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// Rewrite every `EXISTS (q)` and `x IN (q)` in a select list of `plan` into the equivalent
/// `count(*)` scalar subquery.
///
/// Leaves every other occurrence untouched — a predicate position, where DataFusion's own
/// decorrelation is in charge, and the shapes the respelling cannot preserve, which
/// [`reject_unlowered_projection_subqueries`] then names.
pub(crate) fn lower_projection_subqueries(plan: LogicalPlan) -> DFResult<LogicalPlan> {
    // `_with_subqueries`, so a select list one level down — inside a derived table, a CTE or
    // another subquery — is lowered too: it is the same expression with the same physical form
    // missing wherever it sits.
    plan.transform_down_with_subqueries(|node| match node {
        LogicalPlan::Projection(projection) => {
            let Projection { expr, input, .. } = projection;
            let schema = Arc::clone(input.schema());
            let mut transformed = false;
            let mut lowered = Vec::with_capacity(expr.len());
            for expr in expr {
                // The name the projection would have produced, kept across the rewrite: it is
                // the client's column label, and it is also what makes the projection's field
                // names stay unique — two lowered `EXISTS` in one select list both become
                // `count(*) > Int64(0)` and would collide.
                let name = expr.schema_name().to_string();
                let one = lower_expr(expr, &schema)?;
                transformed |= one.transformed;
                lowered.push(match one.transformed {
                    true => preserve_name(one.data, name),
                    false => one.data,
                });
            }
            let projection = LogicalPlan::Projection(Projection::try_new(lowered, input)?);
            Ok(match transformed {
                true => Transformed::yes(projection),
                false => Transformed::no(projection),
            })
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
}

/// Keep `expr` under the name the projection had before the rewrite.
fn preserve_name(expr: Expr, name: String) -> Expr {
    match expr {
        // Already named — by `column_labels::label_result_columns` for a result column, or by
        // the client's own `AS`. Aliasing again would only bury it.
        aliased @ Expr::Alias(_) => aliased,
        other => other.alias(name),
    }
}

/// Lower every `EXISTS` and `IN (subquery)` inside one select-list expression.
///
/// `schema` is the projection's *input* schema: it is what the compared expression of an `IN`
/// is resolved against when it becomes an outer reference.
fn lower_expr(expr: Expr, schema: &DFSchemaRef) -> DFResult<Transformed<Expr>> {
    expr.transform_up(|expr| match expr {
        Expr::Exists(exists) => Ok(match lower_exists(exists)? {
            Ok(lowered) => Transformed::yes(lowered),
            Err(exists) => Transformed::no(Expr::Exists(exists)),
        }),
        Expr::InSubquery(in_subquery) => Ok(match lower_in_subquery(in_subquery, schema)? {
            Ok(lowered) => Transformed::yes(lowered),
            Err(in_subquery) => Transformed::no(Expr::InSubquery(in_subquery)),
        }),
        other => Ok(Transformed::no(other)),
    })
}

/// `EXISTS (q)` as `(SELECT count(*) FROM (q)) > 0`, or the `NOT EXISTS` spelling as `= 0`.
///
/// Returns the `Exists` back for a `q` whose correlation the optimizer cannot lift.
fn lower_exists(exists: Exists) -> DFResult<Result<Expr, Exists>> {
    if !correlation_can_be_lifted(&exists.subquery) {
        return Ok(Err(exists));
    }
    let Exists { subquery, negated } = exists;
    let rows = row_count(subquery, None)?;
    Ok(Ok(match negated {
        false => rows.gt(lit(0_i64)),
        true => rows.eq(lit(0_i64)),
    }))
}

/// `x IN (q)` as the three-valued `CASE` this module's header spells out, or its `NOT IN`
/// mirror.
///
/// Returns the `InSubquery` back for a `q` that does not project exactly one column, a
/// compared expression that carries a subquery of its own or names a column this schema
/// cannot resolve, or a `q` whose correlation the optimizer cannot lift.
fn lower_in_subquery(
    in_subquery: InSubquery,
    schema: &DFSchemaRef,
) -> DFResult<Result<Expr, InSubquery>> {
    let candidates = in_subquery.subquery.subquery.schema();
    if candidates.fields().len() != 1 {
        return Ok(Err(in_subquery));
    }
    if !correlation_can_be_lifted(&in_subquery.subquery) {
        return Ok(Err(in_subquery));
    }
    // A subquery inside the compared expression would have to be carried into the count as an
    // outer reference, which is not something to construct mechanically.
    if in_subquery.expr.exists(|e| Ok(is_subquery(e)))? {
        return Ok(Err(in_subquery));
    }
    let Some((compared, outer_refs)) = outer_reference((*in_subquery.expr).clone(), schema) else {
        return Ok(Err(in_subquery));
    };
    let candidate = Expr::Column(Column::from(candidates.qualified_field(0)));
    let candidate_nullable = candidates.field(0).is_nullable();
    // Errs towards nullable: a compared expression this schema cannot type is treated as one a
    // NULL can reach, because the cost of being wrong that way is one extra subquery and the
    // cost of being wrong the other way is a false where PostgreSQL says NULL.
    let probe_nullable = in_subquery.expr.nullable(schema).unwrap_or(true);

    let InSubquery {
        expr,
        subquery,
        negated,
    } = in_subquery;
    let matched = row_count(
        with_outer_references(subquery.clone(), outer_refs),
        Some(candidate.clone().eq(compared)),
    )?
    .gt(lit(0_i64));

    // The NULL branch, assembled from only the disjuncts nullability leaves reachable. Over
    // two non-nullable sides there are none, and `IN` is the two-valued predicate the match
    // test alone already is.
    let mut unknown: Option<Expr> = None;
    if candidate_nullable {
        unknown = Some(row_count(subquery.clone(), Some(candidate.is_null()))?.gt(lit(0_i64)));
    }
    if probe_nullable {
        // Evaluated in the outer query, not inside the subquery: `IS NULL` on an outer
        // reference is not an equality, and a filter that is not an equality is one
        // `ScalarSubqueryToJoin` leaves correlated.
        let empty = (*expr)
            .clone()
            .is_null()
            .and(row_count(subquery, None)?.gt(lit(0_i64)));
        unknown = Some(match unknown {
            Some(candidate_is_null) => candidate_is_null.or(empty),
            None => empty,
        });
    }
    let (present, absent) = match negated {
        false => (true, false),
        true => (false, true),
    };
    Ok(Ok(match unknown {
        None => match negated {
            false => matched,
            true => Expr::Not(Box::new(matched)),
        },
        Some(unknown) => Expr::Case(datafusion::logical_expr::Case::new(
            None,
            vec![
                (Box::new(matched), Box::new(lit(present))),
                (Box::new(unknown), Box::new(lit(ScalarValue::Boolean(None)))),
            ],
            Some(Box::new(lit(absent))),
        )),
    }))
}

/// `(SELECT count(*) FROM (q))`, with `filter` applied to `q`'s rows first when there is one.
///
/// The aggregate goes *above* whatever `q` is, and so does the filter: `q` may carry a
/// `LIMIT`, a `DISTINCT` or a `GROUP BY`, and pushing either under one of those would count
/// different rows than `q` returns.
fn row_count(subquery: Subquery, filter: Option<Expr>) -> DFResult<Expr> {
    let Subquery {
        subquery: plan,
        outer_ref_columns,
        spans,
    } = subquery;
    let mut builder = LogicalPlanBuilder::from(Arc::unwrap_or_clone(plan));
    if let Some(filter) = filter {
        builder = builder.filter(filter)?;
    }
    let plan = builder
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64))])?
        .build()?;
    Ok(Expr::ScalarSubquery(Subquery {
        subquery: Arc::new(plan),
        outer_ref_columns,
        spans,
    }))
}

/// Add `outer_refs` to `subquery`'s declared outer references, keeping the ones it already
/// has: the compared expression's columns are additional correlations, not a replacement.
fn with_outer_references(subquery: Subquery, outer_refs: Vec<Expr>) -> Subquery {
    let Subquery {
        subquery: plan,
        mut outer_ref_columns,
        spans,
    } = subquery;
    for reference in outer_refs {
        if !outer_ref_columns.contains(&reference) {
            outer_ref_columns.push(reference);
        }
    }
    Subquery {
        subquery: plan,
        outer_ref_columns,
        spans,
    }
}

/// Restate `expr` as it must read from *inside* the subquery: every column of the outer query
/// becomes an outer reference, and the references are collected for the [`Subquery`].
///
/// `None` when a column cannot be typed against `schema`, which is the case for a reference
/// already correlated to a further-out query — a shape this does not rewrite.
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
        "IN (subquery) operand is not a column of the enclosing plan".to_string(),
    )
}

/// Whether `ScalarSubqueryToJoin` will be able to lift the correlation of the `count(*)`
/// subquery built over `subquery`.
///
/// An **un**correlated `q` is always fine: the count is uncorrelated too — or correlated only
/// by the equality this module adds — and any internal shape is answered.
///
/// A correlated `q` is fine only in the narrow shape decorrelation accepts: a filtered
/// projection whose correlations are all top-level equalities. Anything else was measured
/// failing, either at physical planning (`Physical plan does not support logical expression
/// ScalarSubquery`) or after the optimizer (`Schema error: No field named __scalar_sq_1.g`).
fn correlation_can_be_lifted(subquery: &Subquery) -> bool {
    if !is_correlated(&subquery.subquery) {
        return true;
    }
    let mut liftable = true;
    let _ = subquery.subquery.apply(|node| {
        let acceptable = match node {
            LogicalPlan::Filter(filter) => conjuncts(&filter.predicate)
                .into_iter()
                .all(|conjunct| !mentions_outer(conjunct) || is_outer_equality(conjunct)),
            // The nodes a filtered projection is made of. A `GROUP BY`, a `LIMIT`, a
            // `DISTINCT`, a set operation or a window between the correlation and the count is
            // what decorrelation cannot see past.
            LogicalPlan::Projection(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_) => {
                let mut clean = true;
                let _ = node.apply_expressions(|expr| {
                    if mentions_outer(expr) {
                        clean = false;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
                clean
            }
            _ => false,
        };
        // A subquery of its own inside a correlated `q` puts a second scope between the
        // correlation and the count, and whose scope an inner outer reference belongs to is
        // not something this can read off one node.
        let mut nested = false;
        let _ = node.apply_expressions(|expr| {
            if expr.exists(|e| Ok(is_subquery(e)))? {
                nested = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if !acceptable || nested {
            liftable = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    liftable
}

/// Whether anything in `plan`, including inside a subquery of its own, names a column of an
/// enclosing query.
fn is_correlated(plan: &LogicalPlan) -> bool {
    let mut correlated = false;
    let _ = plan.apply_with_subqueries(|node| {
        let _ = node.apply_expressions(|expr| {
            if mentions_outer(expr) {
                correlated = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(match correlated {
            true => TreeNodeRecursion::Stop,
            false => TreeNodeRecursion::Continue,
        })
    });
    correlated
}

/// Whether `expr` names a column of an enclosing query.
fn mentions_outer(expr: &Expr) -> bool {
    expr.exists(|e| Ok(matches!(e, Expr::OuterReferenceColumn(_, _))))
        .unwrap_or(true)
}

/// `expr`'s top-level `AND` operands.
fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let mut operands = conjuncts(left);
            operands.append(&mut conjuncts(right));
            operands
        }
        other => vec![other],
    }
}

/// Whether `expr` is an equality with the correlation on exactly one side — the one shape
/// `PullUpCorrelatedExpr` turns into a join key.
fn is_outer_equality(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => mentions_outer(left) != mentions_outer(right),
        _ => false,
    }
}

/// Whether `expr` carries a subquery.
fn is_subquery(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_) | Expr::SetComparison(_)
    )
}

/// Refuse every `EXISTS` and `IN (subquery)` outside a predicate position that
/// [`lower_projection_subqueries`] did not lower.
///
/// Reads the plan and never rewrites it: the outcome is either the same plan or `0A000`. Run
/// *after* the lowering, on the same plan, so that what it sees is exactly the residue.
pub(crate) fn reject_unlowered_projection_subqueries(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut visitor = UnloweredSubquery { error: None };
    let _ = plan.visit_with_subqueries(&mut visitor);
    match visitor.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

struct UnloweredSubquery {
    error: Option<PgWireError>,
}

impl<'n> TreeNodeVisitor<'n> for UnloweredSubquery {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n LogicalPlan) -> DFResult<TreeNodeRecursion> {
        // A `WHERE`, `HAVING`, `QUALIFY` or join `ON` is `DecorrelatePredicateSubquery`'s own
        // business and is answered as written, correlated or not. Nothing here to judge.
        if matches!(node, LogicalPlan::Filter(_) | LogicalPlan::Join(_)) {
            return Ok(TreeNodeRecursion::Continue);
        }
        // In a select list the residue is a subquery shape the respelling declined; anywhere
        // else it is the position itself, and the two get different wording because the
        // workarounds are different.
        let in_projection = matches!(node, LogicalPlan::Projection(_));
        let mut found = None;
        let _ = node.apply_expressions(|expr| {
            let _ = expr.apply(|e| {
                let shape = match e {
                    Expr::Exists(exists) => match exists.negated {
                        false => "EXISTS (subquery)",
                        true => "NOT EXISTS (subquery)",
                    },
                    Expr::InSubquery(in_subquery) => match in_subquery.negated {
                        false => "IN (subquery)",
                        true => "NOT IN (subquery)",
                    },
                    _ => return Ok(TreeNodeRecursion::Continue),
                };
                found = Some(match in_projection {
                    true => unsupported_shape(shape),
                    false => unsupported_position(shape),
                });
                Ok(TreeNodeRecursion::Stop)
            });
            Ok(match found {
                Some(_) => TreeNodeRecursion::Stop,
                None => TreeNodeRecursion::Continue,
            })
        });
        if let Some(e) = found {
            self.error = Some(e);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// `0A000` for a select-list subquery whose correlation cannot be lifted — naming the join
/// spelling that answers it, and the correlation shape that is answered as written.
fn unsupported_shape(shape: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{shape} is not supported in a select list when the subquery is correlated by \
             anything other than an equality, or when a correlated subquery carries a GROUP BY, \
             a LIMIT, a DISTINCT, a set operation or a subquery of its own: the boolean column \
             is computed by counting the subquery's rows per outer row, and only an equality \
             correlation can be lifted into the join that does that. Spell it as a \
             LEFT JOIN on the same predicate and test the joined key IS NOT NULL, or keep the \
             {shape} in a WHERE clause, where it is answered as written for every correlation. \
             An uncorrelated subquery, or one correlated only by =, is answered in a select \
             list"
        ),
    )
}

/// `0A000` for a subquery outside both a predicate and a select list — naming the alias that
/// moves it into one.
fn unsupported_position(shape: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{shape} is not supported in this position: it is answered in a select list, and as \
             a WHERE, HAVING, QUALIFY or join ON predicate, because those are the positions the \
             count that computes it can be planned in. Give it a name in the select list and \
             use that name — ORDER BY and GROUP BY both accept a result column's alias — or \
             compute it in a derived table and reference the column"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, BooleanArray, Int32Array};
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

    /// `l(id int4 NOT NULL, k int4)` and `r(id int4 NOT NULL, k int4)`.
    ///
    /// ```text
    /// l = (1, 10), (2, NULL), (3, 30), (4, 99)
    /// r = (1, 10), (2, NULL), (3, 30)
    /// ```
    ///
    /// `id` is non-nullable so the two-valued branch is reachable, `k` nullable on both sides
    /// so each of the three `IN` outcomes is: `l.k = 10` matches, `l.k = 99` does not but a
    /// candidate is NULL, and `l.k IS NULL` is the row where the probe itself is unknown.
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
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(99)])),
            ],
        )
        .unwrap();
        let r = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
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
        let plan = lower_projection_subqueries(plan).expect("lowering does not fail");
        reject_unlowered_projection_subqueries(&plan)?;
        Ok(plan)
    }

    /// The boolean column `sql` answers, one entry per `l.id` in order, with the read path's
    /// passes applied. Every test statement projects `id` first and the boolean second.
    async fn answer(sql: &str) -> Vec<Option<bool>> {
        let ctx = ctx();
        let plan = lowered(&ctx, sql).await.expect("not refused");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("plans physically")
            .collect()
            .await
            .expect("executes");
        let mut rows: Vec<(i32, Option<bool>)> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is int4");
                let flags = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("the lowered column is boolean");
                (0..ids.len())
                    .map(|i| {
                        (
                            ids.value(i),
                            match flags.is_null(i) {
                                true => None,
                                false => Some(flags.value(i)),
                            },
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        rows.sort_by_key(|(id, _)| *id);
        rows.into_iter().map(|(_, flag)| flag).collect()
    }

    /// The `0A000` message `sql` is refused with.
    async fn refusal(sql: &str) -> String {
        let ctx = ctx();
        match lowered(&ctx, sql).await {
            Ok(plan) => panic!("expected a refusal, planned:\n{}", plan.display_indent()),
            Err(e) => e.to_string(),
        }
    }

    /// A codec for the in-memory tables these tests register, standing in for the
    /// [`crate::scheduler::logical_codec::VaireLogicalCodec`] the cluster ships plans with.
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

    /// Whether the optimized plan of `sql` survives the serialization Ballista puts it through
    /// on its way to the scheduler — the half of this gap that never reached the physical
    /// planner at all.
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

    /// Nothing the physical planner and datafusion-proto have no form for is left behind.
    #[tokio::test]
    async fn no_exists_or_in_subquery_survives_a_select_list() {
        let ctx = ctx();
        for sql in [
            "SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = l.k) FROM l",
            "SELECT id, NOT EXISTS (SELECT 1 FROM r WHERE r.k = l.k) FROM l",
            "SELECT id, k IN (SELECT k FROM r) FROM l",
            "SELECT id, k NOT IN (SELECT k FROM r) FROM l",
            "SELECT id, id IN (SELECT id FROM r) FROM l",
        ] {
            let plan = lowered(&ctx, sql).await.expect("not refused");
            let mut left = false;
            plan.apply_with_subqueries(|node| {
                node.apply_expressions(|expr| {
                    left |= expr
                        .exists(|e| Ok(matches!(e, Expr::Exists(_) | Expr::InSubquery(_))))
                        .unwrap_or(false);
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            assert!(!left, "`{sql}` still carries a subquery expression");
        }
    }

    /// The other half of the gap: the plan has to reach an executor, and `Expr::Exists` and
    /// `Expr::InSubquery` are both on datafusion-proto's unsupported list.
    #[tokio::test]
    async fn the_optimized_plan_reaches_an_executor() {
        for sql in [
            "SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = l.k) AS e FROM l",
            "SELECT id, EXISTS (SELECT 1 FROM r) AS e FROM l",
            "SELECT id, k IN (SELECT k FROM r) AS i FROM l",
            "SELECT id, k NOT IN (SELECT k FROM r) AS i FROM l",
            "SELECT id, id IN (SELECT id FROM r) AS i FROM l",
        ] {
            assert_eq!(round_trips(sql).await, Ok(()), "{sql}");
        }
    }

    /// The rewritten expression keeps the name the projection had, so the client is told the
    /// same column label it would have been told before — `count(*) > Int64(0)` is not a name
    /// any client asked for, and two of them in one select list would not even be unique.
    #[tokio::test]
    async fn a_lowered_subquery_keeps_the_column_label() {
        let ctx = ctx();
        for sql in [
            "SELECT k IN (SELECT k FROM r) FROM l",
            "SELECT EXISTS (SELECT 1 FROM r) FROM l",
            "SELECT EXISTS (SELECT 1 FROM r) AS present FROM l",
        ] {
            let before = ctx
                .state()
                .create_logical_plan(sql)
                .await
                .expect("statement plans");
            let expected = before.schema().field(0).name().clone();
            let after = lowered(&ctx, sql).await.expect("not refused");
            assert_eq!(after.schema().field(0).name(), &expected, "`{sql}`");
            assert!(
                !expected.contains("count"),
                "`{sql}` should not be labelled by the rewrite: {expected}"
            );
        }
    }

    // ---- the answers, against PostgreSQL 17 ---------------------------------------------

    /// The gap's own statement.
    #[tokio::test]
    async fn a_correlated_exists_answers_a_boolean_per_row() {
        assert_eq!(
            answer("SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = l.k) FROM l").await,
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    /// `EXISTS` is two-valued: the NULL `l.k` row is false, not NULL, because `r.k = NULL` is
    /// merely never true.
    #[tokio::test]
    async fn exists_is_never_null() {
        assert_eq!(
            answer("SELECT id, NOT EXISTS (SELECT 1 FROM r WHERE r.k = l.k) FROM l").await,
            vec![Some(false), Some(true), Some(false), Some(true)]
        );
    }

    /// An uncorrelated `EXISTS` over an empty subquery, and over a non-empty one.
    #[tokio::test]
    async fn an_uncorrelated_exists_answers_the_same_value_for_every_row() {
        assert_eq!(
            answer("SELECT id, EXISTS (SELECT 1 FROM r) FROM l").await,
            vec![Some(true); 4]
        );
        assert_eq!(
            answer("SELECT id, EXISTS (SELECT 1 FROM r WHERE false) FROM l").await,
            vec![Some(false); 4]
        );
    }

    /// `count(*)` goes above whatever the subquery already is, so a `LIMIT`, a `DISTINCT`, a
    /// `GROUP BY … HAVING` and a set operation are all counted as the rows they return.
    #[tokio::test]
    async fn exists_reads_the_subquerys_own_clauses() {
        for (sql, expected) in [
            ("SELECT 1 FROM r LIMIT 0", false),
            ("SELECT 1 FROM r LIMIT 1", true),
            ("SELECT DISTINCT k FROM r WHERE false", false),
            ("SELECT k FROM r GROUP BY k HAVING count(*) > 1", false),
            ("SELECT k FROM r GROUP BY k HAVING count(*) >= 1", true),
            ("SELECT k FROM r WHERE false UNION SELECT k FROM r", true),
        ] {
            assert_eq!(
                answer(&format!("SELECT id, EXISTS ({sql}) FROM l")).await,
                vec![Some(expected); 4],
                "EXISTS ({sql})"
            );
        }
    }

    /// PostgreSQL's `IN` over nullable columns: true on a match, NULL when no candidate
    /// matches but one is NULL, NULL when the probe is NULL and the subquery returned a row.
    #[tokio::test]
    async fn in_is_three_valued_over_nullable_columns() {
        assert_eq!(
            answer("SELECT id, k IN (SELECT k FROM r) FROM l").await,
            vec![Some(true), None, Some(true), None]
        );
    }

    /// And `NOT IN` is its exact negation, which is the select-list half of the invariant
    /// [`super::super::pg_not_in_nulls`] holds for predicate positions.
    #[tokio::test]
    async fn not_in_is_the_negation_of_in() {
        assert_eq!(
            answer("SELECT id, k NOT IN (SELECT k FROM r) FROM l").await,
            vec![Some(false), None, Some(false), None]
        );
    }

    /// Over `NOT NULL` columns `IN` is two-valued, and the lowering emits the match test
    /// alone: no `CASE`, no null-candidate count.
    #[tokio::test]
    async fn in_over_non_nullable_columns_is_two_valued() {
        assert_eq!(
            answer("SELECT id, id IN (SELECT id FROM r) FROM l").await,
            vec![Some(true), Some(true), Some(true), Some(false)]
        );
        assert_eq!(
            answer("SELECT id, id NOT IN (SELECT id FROM r) FROM l").await,
            vec![Some(false), Some(false), Some(false), Some(true)]
        );
        let ctx = ctx();
        let plan = lowered(&ctx, "SELECT id, id IN (SELECT id FROM r) FROM l")
            .await
            .expect("not refused")
            .display_indent()
            .to_string();
        assert!(!plan.contains("CASE"), "{plan}");
    }

    /// An empty subquery makes `IN` false and `NOT IN` true for every row — including the row
    /// whose probe is NULL, because the empty disjunction is false whatever it compares.
    #[tokio::test]
    async fn an_empty_subquery_is_false_even_for_a_null_probe() {
        assert_eq!(
            answer("SELECT id, k IN (SELECT k FROM r WHERE false) FROM l").await,
            vec![Some(false); 4]
        );
        assert_eq!(
            answer("SELECT id, k NOT IN (SELECT k FROM r WHERE false) FROM l").await,
            vec![Some(true); 4]
        );
    }

    /// A subquery with no NULL candidate: only the probe can make the answer unknown.
    #[tokio::test]
    async fn a_null_probe_alone_is_unknown() {
        assert_eq!(
            answer("SELECT id, k IN (SELECT k FROM r WHERE r.k IS NOT NULL) FROM l").await,
            vec![Some(true), None, Some(true), Some(false)]
        );
    }

    /// A correlated `IN`, where both the subquery's own equality and the one the lowering adds
    /// have to be lifted at once.
    #[tokio::test]
    async fn a_correlated_in_carries_both_equalities() {
        assert_eq!(
            answer("SELECT id, k IN (SELECT k FROM r WHERE r.id = l.id) FROM l").await,
            vec![Some(true), None, Some(true), Some(false)]
        );
    }

    /// The subquery expression is lowered wherever it sits inside a select-list expression,
    /// not only when it is the whole of one.
    #[tokio::test]
    async fn a_subquery_nested_in_a_larger_expression_is_lowered() {
        assert_eq!(
            answer(
                "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM r WHERE r.k = l.k) THEN true \
                 ELSE false END FROM l"
            )
            .await,
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    /// A select list one level down, inside a derived table.
    #[tokio::test]
    async fn a_select_list_inside_a_derived_table_is_lowered() {
        assert_eq!(
            answer(
                "SELECT id, e FROM (SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = l.k) AS e \
                 FROM l) AS x"
            )
            .await,
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    // ---- what is refused, and what is left alone ----------------------------------------

    /// A correlation the optimizer cannot lift. Measured without the refusal as
    /// `Physical plan does not support logical expression ScalarSubquery(…)`, which reads like
    /// a defect rather than like a limit with a workaround.
    #[tokio::test]
    async fn a_non_equality_correlation_is_refused() {
        let message = refusal("SELECT id, EXISTS (SELECT 1 FROM r WHERE r.id > l.id) FROM l").await;
        assert!(message.contains("LEFT JOIN"), "{message}");
        assert!(message.contains("WHERE clause"), "{message}");
    }

    /// A correlated subquery with a `GROUP BY` between the correlation and the count.
    /// Measured without the refusal as `Schema error: No field named __scalar_sq_1.id`.
    #[tokio::test]
    async fn a_correlated_grouped_subquery_is_refused() {
        refusal("SELECT id, k IN (SELECT k FROM r WHERE r.id = l.id GROUP BY k) FROM l").await;
    }

    /// The same shape **un**correlated is answered: the limit is the lifting, not the clause.
    #[tokio::test]
    async fn an_uncorrelated_grouped_subquery_is_answered() {
        assert_eq!(
            answer("SELECT id, k IN (SELECT k FROM r GROUP BY k) FROM l").await,
            vec![Some(true), None, Some(true), None]
        );
    }

    /// A position no `count(*)` can be planned in, with the message naming the alias that
    /// moves it into one.
    #[tokio::test]
    async fn a_subquery_outside_a_select_list_is_refused() {
        for sql in [
            "SELECT id FROM l ORDER BY EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
            "SELECT count(*) FROM l GROUP BY EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
        ] {
            let message = refusal(sql).await;
            assert!(message.contains("select list"), "`{sql}`: {message}");
            assert!(message.contains("alias"), "`{sql}`: {message}");
        }
    }

    /// The alias the refusal names does reach both positions.
    #[tokio::test]
    async fn the_alias_the_refusal_names_is_answered() {
        assert_eq!(
            answer(
                "SELECT id, e FROM (SELECT id, EXISTS (SELECT 1 FROM r WHERE r.k = l.k) AS e \
                 FROM l) AS x ORDER BY e, id"
            )
            .await,
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    /// A predicate position is `DecorrelatePredicateSubquery`'s and is left exactly as it was
    /// — including the non-equality correlation a select list refuses, which a `WHERE` answers.
    #[tokio::test]
    async fn a_predicate_position_is_untouched() {
        let ctx = ctx();
        for sql in [
            "SELECT id FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.id > l.id)",
            "SELECT id FROM l WHERE k IN (SELECT k FROM r)",
            "SELECT l.id FROM l JOIN r ON l.id = r.id \
             AND EXISTS (SELECT 1 FROM r x WHERE x.id = l.id)",
            "SELECT id, count(*) FROM l GROUP BY id HAVING EXISTS (SELECT 1 FROM r WHERE r.id = l.id)",
        ] {
            let planned = ctx
                .state()
                .create_logical_plan(sql)
                .await
                .expect("statement plans");
            let lowered = lower_projection_subqueries(planned.clone()).expect("does not fail");
            assert_eq!(
                planned.display_indent().to_string(),
                lowered.display_indent().to_string(),
                "`{sql}` should be left alone"
            );
            reject_unlowered_projection_subqueries(&lowered).expect("and not refused");
        }
    }
}
