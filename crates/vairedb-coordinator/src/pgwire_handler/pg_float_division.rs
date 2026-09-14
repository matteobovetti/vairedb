//! Give floating-point division PostgreSQL's zero-divisor error instead of IEEE's
//! infinity.
//!
//! `1.0::float8 / 0` answers `inf` on the read path where PostgreSQL raises
//! `22012 division_by_zero`, and the same statement at another type — `7 / 0`, or
//! `7.0 / 0`, which the read path parses as a `numeric` — already raises `22012` from
//! Arrow's integer and decimal kernels. So the divergence is not only silent, it is
//! *inconsistent within one database*: the type of a literal decides whether a client is
//! told about a bad divisor or handed a value that poisons every aggregate above it.
//!
//! Arrow's float division is IEEE 754 and has no error to classify, so the fix is to stop
//! asking it the question. Every float division in the plan becomes a call of
//! [`vairedb_common::float_div`], which divides with the same Arrow kernel and raises for
//! the rows PostgreSQL raises for. That module owns the rule and the measurements behind
//! it; this one owns *which* divisions get it.
//!
//! ## Why the plan and not the AST
//!
//! Because the AST does not know the types. `a / b` is a zero-divisor error at `numeric`
//! and an infinity at `float8`, and the two are the same three tokens — the parse cannot
//! tell them apart, and rewriting both would take the already-correct `numeric` division
//! away from Arrow's decimal kernel for no reason. So this runs where a column's type is
//! known, on the logical plan, in [`super::parser::plan_select`], along
//! [`super::pg_aggregate_widening`] and [`super::pg_using_join_merge`].
//!
//! Its position in that sequence is load-bearing at both ends:
//!
//! * **After `pg_param_types::resolve_placeholder_types`**, because an untyped `$1` has no
//!   type to divide at, so `x / $1` would be skipped for want of one. Once the placeholder
//!   carries the type the client's value will be decoded as, the division is recognizable.
//! * **Before `coerce_types`**, so the call the analyzer then sees is an ordinary function
//!   call it type-checks like any other, rather than one inserted behind its back.
//!
//! ## Why the operands are cast explicitly
//!
//! A rewrite that changed a result type would trade a wrong *value* for a wrong *OID*,
//! which is the § 2 class of gap rather than a fix. So the coerced input types and the
//! result type are taken from [`BinaryTypeCoercer`] — the same component DataFusion's own
//! `TypeCoercion` consults for the operator being replaced — and each operand is cast to
//! the type the operator would have divided it at. `float4 / float4` therefore stays
//! `real`, `float8 / integer` stays `double precision`, and the guarded call advertises
//! exactly what the bare `/` advertised.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::binary::BinaryTypeCoercer;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator};

use vairedb_common::float_div::float_division_udf;

/// Rewrite every floating-point division in `plan` into the checked call.
///
/// A plan with no float division comes back unchanged, and applying this twice is the same
/// as applying it once — the second pass sees a function call, which is not an
/// [`Operator::Divide`]. `with_subqueries`, because a division inside a subquery poisons
/// the same aggregates.
pub(crate) fn guard_float_division(plan: LogicalPlan) -> Result<LogicalPlan> {
    // The same accounting `pg_aggregate_widening` needs and for the same reason: a
    // `LogicalPlan` caches its schema, so every node from the first rewrite upward is
    // rebuilt around a new child and would otherwise keep the cached copy. The rewrite
    // preserves both the name and the type of every expression it touches, so this cannot
    // change what the plan advertises — it is the *nullability* the call derives from its
    // arguments that has to be re-derived rather than inherited.
    let mut guarded_something = false;
    plan.transform_up_with_subqueries(|plan| {
        let rewritten = guard_divisions_in(plan)?;
        guarded_something |= rewritten.transformed;
        if !guarded_something {
            return Ok(rewritten);
        }
        rewritten.map_data(|plan| plan.recompute_schema())
    })
    .map(|t| t.data)
}

/// Guard every float division among one node's own expressions.
fn guard_divisions_in(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // The types of this node's expressions come from what its inputs produce.
    let schema = merge_schema(&plan.inputs());

    // A guarded division is named after the call, and the node above it still refers to
    // `a / b`. `NamePreserver` re-pins the original where the rewrite changed it.
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        // Bottom-up, so a nested division is guarded before the one containing it and the
        // call this inserts is never re-examined as a divisor.
        Ok(expr
            .transform_up(|expr| guard_division(expr, &schema))?
            .update_data(|expr| name.restore(expr)))
    })
}

/// One expression: a division whose result is `float4` or `float8` becomes the checked
/// call, and anything else is returned untouched.
fn guard_division(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Divide,
        right,
    }) = &expr
    else {
        return Ok(Transformed::no(expr));
    };

    // A type this plan cannot resolve yet is left alone rather than reported. Whatever
    // makes the operand unresolvable — a placeholder still without a type, a column a
    // later pass introduces — is DataFusion's own to complain about, and complaining here
    // instead would fail a statement this rewrite has no opinion on.
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return Ok(Transformed::no(expr));
    };
    let coercer = BinaryTypeCoercer::new(&left_type, &Operator::Divide, &right_type);
    let (Ok(result_type), Ok((left_input, right_input))) =
        (coercer.get_result_type(), coercer.get_input_types())
    else {
        return Ok(Transformed::no(expr));
    };
    // Only the two float widths. Integer and decimal division already raise `22012` from
    // Arrow's own kernels, and taking those away from the kernels would be a change with
    // no gap behind it.
    if !matches!(result_type, DataType::Float32 | DataType::Float64) {
        return Ok(Transformed::no(expr));
    }

    let Expr::BinaryExpr(BinaryExpr { left, right, .. }) = expr else {
        unreachable!("matched a BinaryExpr");
    };
    Ok(Transformed::yes(float_division_udf().call(vec![
        cast_to(*left, left_type, left_input),
        cast_to(*right, right_type, right_input),
    ])))
}

/// Cast `expr` to `target` unless it already has that type.
///
/// `from` is the type the caller already resolved, so this costs no second lookup — and
/// the cast it adds is the one `TypeCoercion` would have inserted around the operator.
fn cast_to(expr: Expr, from: DataType, target: DataType) -> Expr {
    if from == target {
        return expr;
    }
    Expr::Cast(Cast::new(Box::new(expr), target))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::array::{Float32Array, Float64Array, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;
    use vairedb_common::float_div::FLOAT_DIV_UDF_NAME;

    /// A context holding `t(f float8, g float8, r float4, n int4)` with one row per
    /// argument list, and the checked division registered the way every read-path context
    /// registers it.
    async fn context(
        f: Vec<Option<f64>>,
        g: Vec<Option<f64>>,
        r: Vec<Option<f32>>,
        n: Vec<Option<i32>>,
    ) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float64, true),
            Field::new("r", DataType::Float32, true),
            Field::new("n", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(f)),
                Arc::new(Float64Array::from(g)),
                Arc::new(Float32Array::from(r)),
                Arc::new(Int32Array::from(n)),
            ],
        )
        .expect("the test batch is well formed");
        // `parse_float_as_decimal`, because that is how the read path reads a literal —
        // without it `0.0` would be a `Float64` here and a `numeric` in production.
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        let mut ctx = SessionContext::new_with_config(config);
        vairedb_common::float_div::register_float_division(&mut ctx)
            .expect("registering the checked division");
        ctx.register_batch("t", batch)
            .expect("registering the test table");
        ctx
    }

    /// One row of `f`, `g` left at 1.0, and the other columns unused.
    async fn one_row(f: f64, g: f64) -> SessionContext {
        context(vec![Some(f)], vec![Some(g)], vec![Some(1.0)], vec![Some(1)]).await
    }

    /// Plan `sql`, guard it, and execute — the read path's own order, minus the passes
    /// that have nothing to do with division.
    async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = ctx.state().create_logical_plan(sql).await?;
        let plan = guard_float_division(plan)?;
        ctx.execute_logical_plan(plan).await?.collect().await
    }

    /// What the plan advertises for the one column of `sql`, read the way Describe reads
    /// it — off the guarded plan, before execution.
    async fn advertised_type(ctx: &SessionContext, sql: &str) -> DataType {
        let plan = ctx.state().create_logical_plan(sql).await.expect("planned");
        let plan = guard_float_division(plan).expect("guarded");
        plan.schema().field(0).data_type().clone()
    }

    /// The premise: without the guard this is `inf`, which is the gap.
    #[tokio::test]
    async fn a_float_division_by_zero_is_an_infinity_without_the_guard() {
        let ctx = one_row(1.0, 0.0).await;
        let batches = ctx
            .sql("SELECT f / g AS d FROM t")
            .await
            .expect("planned")
            .collect()
            .await
            .expect("executed");
        let answer = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float8")
            .value(0);
        assert!(answer.is_infinite(), "expected the gap, got {answer}");
    }

    /// And with it, the error PostgreSQL raises.
    #[tokio::test]
    async fn a_float_division_by_zero_raises() {
        let ctx = one_row(1.0, 0.0).await;
        let err = run(&ctx, "SELECT f / g AS d FROM t")
            .await
            .expect_err("should have raised");
        assert!(
            err.to_string().to_lowercase().contains("division by zero"),
            "the message is what error enrichment classifies into 22012: {err}"
        );
    }

    /// The row from the gap analysis, written as a client writes it: a cast literal over a
    /// literal zero, with no table at all. Both operands are constant, so DataFusion's
    /// simplifier folds the call — which is where the error comes from, and is also what
    /// PostgreSQL does with `SELECT 1.0::float8 / 0 WHERE false`.
    #[tokio::test]
    async fn the_gaps_own_statement_raises() {
        let ctx = one_row(1.0, 1.0).await;
        assert!(
            run(&ctx, "SELECT 1.0::float8 / 0 AS d")
                .await
                .is_err_and(|e| e.to_string().to_lowercase().contains("division by zero"))
        );
    }

    /// A zero divisor reaches the error through an aggregate too, which is the shape that
    /// made the gap Tier 1 — a poison value that a `sum` would have carried up silently.
    #[tokio::test]
    async fn a_zero_divisor_under_an_aggregate_raises() {
        let ctx = context(
            vec![Some(1.0), Some(2.0)],
            vec![Some(2.0), Some(0.0)],
            vec![Some(1.0), Some(1.0)],
            vec![Some(1), Some(1)],
        )
        .await;
        assert!(run(&ctx, "SELECT sum(f / g) AS s FROM t").await.is_err());
        assert!(
            run(&ctx, "SELECT sum(f / g) OVER () AS s FROM t")
                .await
                .is_err()
        );
    }

    /// A division in a `WHERE`, where the expression contributes no column to the schema
    /// and `NamePreserver` therefore saves no name.
    #[tokio::test]
    async fn a_zero_divisor_in_a_predicate_raises() {
        let ctx = one_row(1.0, 0.0).await;
        assert!(run(&ctx, "SELECT f FROM t WHERE f / g > 0").await.is_err());
    }

    /// And in a subquery, which is why the walk carries `with_subqueries`.
    #[tokio::test]
    async fn a_zero_divisor_in_a_subquery_raises() {
        let ctx = one_row(1.0, 0.0).await;
        assert!(
            run(&ctx, "SELECT f FROM t WHERE f > (SELECT min(f / g) FROM t)")
                .await
                .is_err()
        );
    }

    /// Nothing else changes: an ordinary division answers what it answered before, and the
    /// column keeps the name the client wrote it under.
    #[tokio::test]
    async fn an_ordinary_division_is_unchanged() {
        let ctx = one_row(3.0, 2.0).await;
        let batches = run(&ctx, "SELECT f / g FROM t").await.expect("executed");
        assert_eq!(
            batches[0].schema().field(0).name(),
            "t.f / t.g",
            "the name a client sees comes from the plan, so the guard has to preserve it"
        );
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float8")
                .value(0),
            1.5
        );
    }

    /// The three PostgreSQL rows that are *not* errors, over one batch each.
    #[tokio::test]
    async fn nan_and_null_follow_postgresql_rather_than_raising() {
        let nan = one_row(f64::NAN, 0.0).await;
        let batches = run(&nan, "SELECT f / g AS d FROM t")
            .await
            .expect("a NaN dividend over zero is NaN, not an error");
        assert!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float8")
                .value(0)
                .is_nan()
        );

        let null = context(vec![None], vec![Some(0.0)], vec![Some(1.0)], vec![Some(1)]).await;
        let batches = run(&null, "SELECT f / g AS d FROM t")
            .await
            .expect("division is strict, so a NULL dividend is NULL");
        assert!(batches[0].column(0).is_null(0));
    }

    /// A literal zero divisor over a column dividend, with no rows to divide: per row and
    /// not per statement, so this answers nothing rather than raising. Measured against
    /// PostgreSQL 17, where `SELECT f / 0 FROM <empty>` returns no rows.
    #[tokio::test]
    async fn an_empty_table_raises_nothing() {
        let ctx = context(vec![], vec![], vec![], vec![]).await;
        let batches = run(&ctx, "SELECT f / 0.0::float8 AS d FROM t")
            .await
            .expect("no rows, no error");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    /// The types the rewrite must not move. `float4 / float4` is `real`; a mixed division
    /// is what the operator itself would have coerced to.
    #[tokio::test]
    async fn the_advertised_type_is_the_one_the_operator_gave() {
        let ctx = one_row(1.0, 2.0).await;
        assert_eq!(
            advertised_type(&ctx, "SELECT r / r FROM t").await,
            DataType::Float32
        );
        assert_eq!(
            advertised_type(&ctx, "SELECT f / g FROM t").await,
            DataType::Float64
        );
        assert_eq!(
            advertised_type(&ctx, "SELECT f / n FROM t").await,
            DataType::Float64
        );
    }

    /// The narrower width is checked too, and keeps its own type through the error path.
    #[tokio::test]
    async fn a_float4_division_by_zero_raises() {
        let ctx = context(
            vec![Some(1.0)],
            vec![Some(1.0)],
            vec![Some(0.0)],
            vec![Some(1)],
        )
        .await;
        assert!(run(&ctx, "SELECT r / r AS d FROM t").await.is_err());
    }

    /// A mixed float/integer division is a float division: PostgreSQL raises for
    /// `f / 0::int` too, and the integer operand is cast rather than left to Arrow's
    /// integer kernel.
    #[tokio::test]
    async fn a_float_over_an_integer_zero_raises() {
        let ctx = context(
            vec![Some(1.0)],
            vec![Some(1.0)],
            vec![Some(1.0)],
            vec![Some(0)],
        )
        .await;
        assert!(run(&ctx, "SELECT f / n AS d FROM t").await.is_err());
    }

    /// The divisions this rewrite leaves alone, because Arrow's own kernels already raise
    /// `22012` for them — and taking a correct answer away from a kernel is a change with
    /// no gap behind it.
    #[tokio::test]
    async fn integer_and_decimal_division_are_left_to_arrow() {
        let ctx = one_row(1.0, 1.0).await;
        for sql in ["SELECT n / 0 AS d FROM t", "SELECT 7.0 / 0 AS d"] {
            let plan = ctx.state().create_logical_plan(sql).await.expect("planned");
            let guarded = guard_float_division(plan.clone()).expect("guarded");
            assert_eq!(
                format!("{guarded:?}"),
                format!("{plan:?}"),
                "{sql} should not have been rewritten"
            );
            assert!(
                run(&ctx, sql)
                    .await
                    .is_err_and(|e| e.to_string().to_lowercase().contains("zero")),
                "{sql} should already raise"
            );
        }
    }

    /// Idempotent, because a second pass sees a function call and not a `Divide`.
    #[tokio::test]
    async fn guarding_twice_is_the_same_as_guarding_once() {
        let ctx = one_row(1.0, 2.0).await;
        let plan = ctx
            .state()
            .create_logical_plan("SELECT f / g FROM t")
            .await
            .expect("planned");
        let once = guard_float_division(plan).expect("guarded");
        let twice = guard_float_division(once.clone()).expect("guarded twice");
        assert_eq!(format!("{once:?}"), format!("{twice:?}"));
    }

    /// A right-nested chain, which is the shape a `CASE`-shaped guard would have grown
    /// exponentially: each division is guarded exactly once and its operands appear once.
    #[tokio::test]
    async fn a_nested_division_is_guarded_once_per_operator() {
        let ctx = one_row(1.0, 2.0).await;
        let plan = ctx
            .state()
            .create_logical_plan("SELECT f / (g / (f / g)) FROM t")
            .await
            .expect("planned");
        // The displayed plan and not the `Debug` one: `Debug` renders a UDF as the struct
        // behind it, so the name to count only appears in the form `EXPLAIN` would print.
        let guarded = guard_float_division(plan)
            .expect("guarded")
            .display_indent()
            .to_string();
        assert_eq!(
            guarded.matches(FLOAT_DIV_UDF_NAME).count(),
            3,
            "one call per operator, and each operand once: {guarded}"
        );
    }
}
