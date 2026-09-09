//! Give `sum` and `avg` over an integer column the accumulator width PostgreSQL gives
//! them.
//!
//! PostgreSQL widens as it accumulates: `sum(integer)` is a `bigint`, `sum(bigint)` is a
//! `numeric`, and `avg` of **any** integer type is a `numeric`. So a total never
//! overflows the type of the column it came from and an average never loses the exact
//! integers it was computed over. DataFusion does neither:
//!
//! * `sum(Int64)` accumulates **in** `Int64` and wraps on overflow silently — two rows of
//!   `4611686018427387904` sum to `-9223372036854775802`, a negative total over positive
//!   values, advertised as `bigint` and reported without complaint.
//! * `avg(Int64)` computes in `Float64`, which holds 53 bits of mantissa against
//!   `bigint`'s 63. `avg` of `6148914691236517205` answers `6148914691236517000` — the
//!   last three digits replaced by zeros, advertised as `double precision` where
//!   PostgreSQL promises `numeric`.
//!
//! That is the failure this codebase refuses everywhere: a plausible wrong answer with
//! nothing to say it is wrong. Here neither has to be refused, because PostgreSQL's own
//! answer is reachable by casting the *argument* — a `Decimal128` argument makes
//! DataFusion accumulate in `Decimal128` and return one, which arrow-pg advertises as
//! `numeric`, exactly the type PostgreSQL promises.
//!
//! `sum(integer)` is left alone: DataFusion already accumulates that one in `Int64`,
//! which is the `bigint` PostgreSQL promises, so it is already right. `avg(integer)` is
//! **not** left alone, because PostgreSQL promises `numeric` there too — the asymmetry is
//! PostgreSQL's, not a special case, and [`widening_target`] is where it lives.
//!
//! `sum(x) OVER (…)` and `avg(x) OVER (…)` are the same aggregates reached through a
//! different plan node, and they lose precision identically, so they are widened too.
//! Leaving them out would have been worse than not doing this at all: `sum(big)` and
//! `sum(big) OVER ()` would then report different types for the same total, and the
//! window form is the one where the overflow is easier to reach, since every row of a
//! partition carries a running total of the rows before it.
//!
//! ## Where this runs
//!
//! On the **logical plan**, once, in [`crate::pgwire_handler::parser::plan_select`] — and
//! deliberately not as a registered [`datafusion::optimizer::AnalyzerRule`], which is
//! where the first version of this lived and why it failed.
//!
//! A rule registered on a session context runs inside `create_physical_plan`, after the
//! plan a client is told about has been settled. `statement_to_plan` returns an
//! *unanalyzed* plan, and it is that plan's schema — `df.schema()` — that VaireDB turns
//! into result-column type OIDs, both for Describe and for the rows themselves. So an
//! analyzer rule changes what executes while leaving the advertised type behind: the plan
//! promised `bigint`, execution produced the decimal, and the encoder's checked cast into
//! the promised type met the client as `[VDB-1019] column "sum" holds a value
//! PostgreSQL's Int64 cannot represent`. The wrong answer traded for a confusing error
//! rather than for a right one.
//!
//! Rewriting the plan instead means Describe and Execute cannot disagree — they are the
//! same plan — and the rewrite needs no help from the analyzer afterwards: `TypeCoercion`
//! accepts the decimal argument, and DataFusion's own `sum` over a `Decimal128(38, s)`
//! already returns `Decimal128(38, s)`.
//!
//! Acting before the analyzer is also what makes the rewrite *correct*, not merely
//! visible. PostgreSQL chooses between `sum(integer)` and `sum(bigint)` by the argument's
//! own type and never widens to pick an overload, so `sum(n)` over an `integer` column is
//! `bigint` while `sum(n::bigint)` is `numeric`. `TypeCoercion` erases exactly that
//! distinction — it rewrites `sum(n)` into `sum(CAST(n AS Int64))` to match DataFusion's
//! signature, after which the two are the same expression. Anything running later cannot
//! tell them apart and would widen both, getting `sum(integer)` wrong in the other
//! direction.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, WindowFunctionDefinition};

/// The accumulator `sum` over a `bigint` uses instead of `Int64`.
///
/// Scale zero, because the inputs are integers and PostgreSQL's own `numeric` result
/// carries no fractional part either. Precision 38 is the widest Arrow's 128-bit
/// decimal offers, and DataFusion's `sum` over a decimal already widens the precision
/// to 38, so this is the type it would have reached anyway.
const SUM_ACCUMULATOR: DataType = DataType::Decimal128(38, 0);

/// The accumulator `avg` over an integer uses instead of `Float64`.
///
/// Scale six, where `sum` needs none, because **`avg`'s result scale is derived from its
/// argument's**: measured against DataFusion 54.1, `avg(Decimal128(38, s))` returns
/// `Decimal128(38, s + 4)`. An average has a fractional part that the integers it was
/// computed over do not, so scale 0 would answer `1.6666` for PostgreSQL's
/// `1.6666666666666667` — right type, right magnitude, and visibly rounded off. Six buys
/// ten decimal places.
///
/// Six and not twelve because scale is bought out of the same 38 digits the accumulation
/// spends: `avg` sums into `Decimal128(38, s)`, so `N` rows of magnitude up to
/// `i64::MAX` need `N · 9.22 × 10¹⁸ · 10ˢ < 10³⁸`. At scale 6 that is ~10¹³ rows, past
/// any cluster; at 12 it is ~10⁷, which an analytical table reaches. And the failure is
/// loud rather than silent — DataFusion raises `Arithmetic Overflow in AvgAccumulator`
/// (unlike its `sum`, which wraps) — so the ceiling costs an error, not a wrong number.
const AVG_ACCUMULATOR: DataType = DataType::Decimal128(38, 6);

/// The type `name(arg)` should accumulate in, where DataFusion's own choice does not
/// match PostgreSQL's promise, and `None` where it already does.
///
/// The two aggregates disagree about `integer` and PostgreSQL is why. `sum(integer)` is a
/// `bigint`, which DataFusion already gives; `avg(integer)` is a `numeric`, which it does
/// not. So `avg` widens from every integer width and `sum` only from the one that
/// overflows.
fn widening_target(name: &str, arg: &DataType) -> Option<DataType> {
    match name {
        "sum" if *arg == DataType::Int64 => Some(SUM_ACCUMULATOR),
        "avg" if matches!(arg, DataType::Int64 | DataType::Int32 | DataType::Int16) => {
            Some(AVG_ACCUMULATOR)
        }
        _ => None,
    }
}

/// Rewrite every `sum` and `avg` in `plan` that PostgreSQL accumulates more widely than
/// DataFusion does.
///
/// A plan with no such aggregate comes back unchanged, and applying this twice is the
/// same as applying it once — the second pass sees a decimal argument, which
/// [`widening_target`] declines. `with_subqueries`, because an aggregate in a subquery is
/// the same aggregate over the same column type and loses precision the same way.
pub(crate) fn widen_bigint_aggregates(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Every node from the first rewrite upward needs its schema recomputed, not only the
    // ones whose own expressions changed. A `LogicalPlan` caches its schema, and
    // rebuilding a parent around a new child reuses that cache rather than deriving it
    // again — so a projection over a widened aggregate goes on advertising `bigint`, and
    // the optimizer later rejects the plan for the disagreement.
    //
    // `transform_up` visits children first and does not expose its own accumulated flag
    // to the closure, so "something below me changed" is tracked here. It is one flag for
    // the whole walk rather than one per subtree: recomputing a node whose inputs did not
    // change derives the schema it already had, so being generous costs a derivation and
    // risks nothing.
    let mut widened_something = false;
    plan.transform_up_with_subqueries(|plan| {
        let rewritten = widen_aggregates_in(plan)?;
        widened_something |= rewritten.transformed;
        if !widened_something {
            return Ok(rewritten);
        }
        rewritten.map_data(|plan| plan.recompute_schema())
    })
    .map(|t| t.data)
}

/// Widen every under-accumulated aggregate among one node's own expressions.
fn widen_aggregates_in(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // The types of this node's expressions come from what its inputs produce. Order does
    // not matter: the schema is only consulted to resolve a column's type.
    let schema = merge_schema(&plan.inputs());

    // A rewritten aggregate is named after its new argument, and the node above it still
    // refers to the old name. `NamePreserver` re-pins the original where a rewrite
    // changed it, and knows the plans whose names are not referred to at all.
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        Ok(widen_aggregate(expr, &schema)?.update_data(|expr| name.restore(expr)))
    })
}

/// One expression: `sum(<bigint>)` becomes `sum(<bigint> AS decimal)`, `avg(<integer>)`
/// becomes `avg(<integer> AS decimal)`, and anything else is returned untouched.
///
/// Both spellings DataFusion plans an aggregate under are matched. A grouped `sum(x)` is
/// an [`Expr::AggregateFunction`]; `sum(x) OVER (…)` is an [`Expr::WindowFunction`]
/// wrapping the very same UDF, and it accumulates identically. A running total is if
/// anything the more exposed of the two, since a partition's last row sees the whole
/// partition's total.
fn widen_aggregate(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Some((name, args)) = aggregate_call(&expr) else {
        return Ok(Transformed::no(expr));
    };
    // One argument, so `sum(DISTINCT x)`'s own shape is still a single argument while a
    // multi-argument aggregate that happened to be named `avg` is not this rewrite's.
    let [arg] = args.as_slice() else {
        return Ok(Transformed::no(expr));
    };
    // Also what makes the rewrite idempotent: a second pass sees a decimal argument, for
    // which there is no target.
    let Some(accumulator) = widening_target(name, &arg.get_type(schema)?) else {
        return Ok(Transformed::no(expr));
    };

    let mut expr = expr;
    let args = aggregate_args_mut(&mut expr).expect("just matched as an aggregate");
    let arg = args.remove(0);
    args.push(Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(arg),
        accumulator,
    )));

    Ok(Transformed::yes(expr))
}

/// The name and argument list of `expr` if it is an aggregate call, grouped or windowed.
fn aggregate_call(expr: &Expr) -> Option<(&str, &Vec<Expr>)> {
    match expr {
        Expr::AggregateFunction(agg) => Some((agg.func.name(), &agg.params.args)),
        Expr::WindowFunction(window) => match &window.fun {
            WindowFunctionDefinition::AggregateUDF(udf) => Some((udf.name(), &window.params.args)),
            _ => None,
        },
        _ => None,
    }
}

/// The argument list [`aggregate_call`] returned, to be rewritten in place.
fn aggregate_args_mut(expr: &mut Expr) -> Option<&mut Vec<Expr>> {
    match expr {
        Expr::AggregateFunction(agg) => Some(&mut agg.params.args),
        Expr::WindowFunction(window) => match &window.fun {
            WindowFunctionDefinition::AggregateUDF(_) => Some(&mut window.params.args),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::context::SessionContext;

    /// What `avg` over [`AVG_ACCUMULATOR`] returns: DataFusion adds four to the
    /// argument's scale. Written out rather than computed, so that a change to the rule
    /// upstream fails a test here instead of being absorbed by it.
    const AVG_ACCUMULATOR_RESULT: DataType = DataType::Decimal128(38, 10);

    /// One table: two `bigint` values whose exact sum is just past `i64::MAX`, and the
    /// same magnitudes as `integer`.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("big", DataType::Int64, false),
            Field::new("small", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    4611686018427387904,
                    4611686018427387906,
                ])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    /// The plan `plan_select` would hand the rest of the read path: planned, then
    /// widened, and not yet analyzed.
    async fn read_path_plan(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        widen_bigint_aggregates(plan).unwrap()
    }

    /// The first column of a one-row answer: the type and the rendered value.
    ///
    /// The type is taken from the **plan's** schema and then checked against the batch's,
    /// because the two disagreeing is the failure mode this rewrite exists to avoid.
    /// VaireDB puts the plan's schema on the wire as the column's type OID and casts the
    /// batch into it, so a plan that promises `bigint` over a decimal batch reaches the
    /// client as an out-of-range error rather than as the number it asked for.
    async fn one_row(sql: &str) -> (DataType, String) {
        let ctx = ctx();
        let plan = read_path_plan(&ctx, sql).await;
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let planned = df.schema().field(0).data_type().clone();
        let batches = df.collect().await.unwrap();
        let col = batches[0].column(0);
        assert_eq!(
            col.data_type(),
            &planned,
            "the plan's schema and the data have to agree for `{sql}`"
        );
        let text = datafusion::arrow::util::display::array_value_to_string(col, 0).unwrap();
        (planned, text)
    }

    // The defect: in `Int64` this total wraps to a negative number. The exact sum is
    // 9223372036854775810, which is `i64::MAX + 3`.
    #[tokio::test]
    async fn a_bigint_total_past_i64_max_is_exact() {
        let (dt, text) = one_row("SELECT sum(big) FROM t").await;
        assert_eq!(dt, SUM_ACCUMULATOR, "advertised as numeric, as PG does");
        assert_eq!(text, "9223372036854775810");
    }

    // `sum(integer)` already accumulates in the `bigint` PostgreSQL promises, so the
    // rewrite must not touch it — widening it would change the advertised type from
    // `int8` to `numeric` and disagree with PostgreSQL in the other direction. This is
    // what running before `TypeCoercion` buys: after it, the argument is an `Int64` cast
    // and indistinguishable from a `bigint` column.
    #[tokio::test]
    async fn an_integer_total_is_left_as_bigint() {
        let (dt, text) = one_row("SELECT sum(small) FROM t").await;
        assert_eq!(dt, DataType::Int64);
        assert_eq!(text, "3");
    }

    // A `bigint` the client asked for explicitly *is* `sum(bigint)` to PostgreSQL, which
    // resolves the overload on the cast's type and not on the column's. The pair with the
    // test above pins the distinction the rewrite has to see.
    #[tokio::test]
    async fn an_integer_cast_to_bigint_is_widened() {
        let (dt, text) = one_row("SELECT sum(small::bigint) FROM t").await;
        assert_eq!(dt, SUM_ACCUMULATOR);
        assert_eq!(text, "3");
    }

    // The window spelling of the same aggregate, over the same column, wrapping the same
    // way. The last row of the single partition holds the whole total, which is what the
    // `ORDER BY … DESC LIMIT 1` reads.
    #[tokio::test]
    async fn a_running_bigint_total_past_i64_max_is_exact() {
        let (dt, text) = one_row(
            "SELECT sum(big) OVER (ORDER BY big) FROM t ORDER BY 1 DESC NULLS LAST LIMIT 1",
        )
        .await;
        assert_eq!(dt, SUM_ACCUMULATOR, "the same type the grouped sum reports");
        assert_eq!(text, "9223372036854775810");
    }

    // And the window form of the case that must not move either, for the same reason: the
    // two spellings have to agree with PostgreSQL, which means agreeing with each other.
    #[tokio::test]
    async fn a_running_integer_total_is_left_as_bigint() {
        let (dt, _) = one_row("SELECT sum(small) OVER () FROM t").await;
        assert_eq!(dt, DataType::Int64);
    }

    // The rewrite renames the aggregate's own expression, so the column the plan above
    // it refers to has to keep its name. A query that reads the aggregate through an
    // outer projection is what breaks if it does not — and a subquery is also the case
    // that needs the rewrite to descend into one at all.
    #[tokio::test]
    async fn the_aggregate_keeps_the_column_name_the_plan_refers_to() {
        let (dt, text) = one_row("SELECT s FROM (SELECT sum(big) AS s FROM t) x WHERE s > 0").await;
        assert_eq!(dt, SUM_ACCUMULATOR);
        assert_eq!(text, "9223372036854775810");
    }

    // Idempotence. The plan is rewritten on the coordinator and then travels to the
    // scheduler, so a second pass over an already-widened plan must be a no-op rather
    // than a second cast that drifts the type.
    #[tokio::test]
    async fn widening_an_already_widened_plan_changes_nothing() {
        let ctx = ctx();
        let once = read_path_plan(&ctx, "SELECT sum(big) FROM t").await;
        let twice = widen_bigint_aggregates(once.clone()).unwrap();
        assert_eq!(format!("{once:?}"), format!("{twice:?}"));
    }

    // Other aggregates over the same column are not this rewrite's business.
    #[tokio::test]
    async fn leaves_other_aggregates_alone() {
        let (dt, _) = one_row("SELECT max(big) FROM t").await;
        assert_eq!(dt, DataType::Int64);
    }

    // The `avg` defect, which is a lost-precision one rather than an overflow: in `Float64`
    // this average is 4611686018427388000 — 53 bits of mantissa against 63 of `bigint`, so
    // the low digits are replaced by zeros. The exact average of the two rows is
    // 4611686018427387905, and PostgreSQL reports it as `numeric`.
    #[tokio::test]
    async fn a_bigint_average_past_the_float_mantissa_is_exact() {
        let (dt, text) = one_row("SELECT avg(big) FROM t").await;
        assert_eq!(
            dt, AVG_ACCUMULATOR_RESULT,
            "advertised as numeric, as PG does"
        );
        assert_eq!(text, "4611686018427387905.0000000000");
    }

    // Where `sum` must leave `integer` alone, `avg` must not: PostgreSQL's `avg` is
    // `numeric` over every integer width, so this is the arm where the two aggregates
    // deliberately disagree. The value was already right in `float8`; the type was not.
    #[tokio::test]
    async fn an_integer_average_is_widened_where_an_integer_total_is_not() {
        let (dt, text) = one_row("SELECT avg(small) FROM t").await;
        assert_eq!(dt, AVG_ACCUMULATOR_RESULT);
        assert_eq!(text, "1.5000000000");

        // The neighbour, restated here because the pair *is* the rule.
        let (dt, _) = one_row("SELECT sum(small) FROM t").await;
        assert_eq!(dt, DataType::Int64);
    }

    // The window spelling has to report the type the grouped one does, or `avg(big)` and
    // `avg(big) OVER ()` disagree about the same average.
    #[tokio::test]
    async fn a_windowed_average_reports_the_type_the_grouped_one_does() {
        let (dt, text) = one_row("SELECT avg(big) OVER () FROM t LIMIT 1").await;
        assert_eq!(dt, AVG_ACCUMULATOR_RESULT);
        assert_eq!(text, "4611686018427387905.0000000000");
    }

    // A non-terminating average is where the accumulator's *scale* is visible, and it is
    // the reason `avg` casts to scale 6 where `sum` casts to scale 0: ten decimal places
    // rather than four. PostgreSQL answers `1.6666666666666667` — this is that number
    // truncated, not a different one, and the remaining digits are the narrowing recorded
    // in the gap analysis.
    #[tokio::test]
    async fn a_non_terminating_average_keeps_ten_decimal_places() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 2]))],
        )
        .unwrap();
        ctx.register_batch("r", batch).unwrap();

        let plan = read_path_plan(&ctx, "SELECT avg(n) FROM r").await;
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        assert_eq!(df.schema().field(0).data_type(), &AVG_ACCUMULATOR_RESULT);
        let batches = df.collect().await.unwrap();
        let text = datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0)
            .unwrap();
        assert_eq!(text, "1.6666666666");
    }

    // `avg` over a type PostgreSQL answers in `double precision` must stay there. Widening
    // it would be this rewrite disagreeing with PostgreSQL in the other direction, the way
    // widening `sum(integer)` would.
    #[tokio::test]
    async fn a_float_average_stays_double_precision() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(datafusion::arrow::array::Float64Array::from(
                vec![1.5, 2.5],
            ))],
        )
        .unwrap();
        ctx.register_batch("f", batch).unwrap();
        let plan = read_path_plan(&ctx, "SELECT avg(f) FROM f").await;
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        assert_eq!(df.schema().field(0).data_type(), &DataType::Float64);
    }

    // Idempotence for the second aggregate too — the plan is widened on the coordinator
    // and then travels to the scheduler.
    #[tokio::test]
    async fn widening_an_already_widened_average_changes_nothing() {
        let ctx = ctx();
        let once = read_path_plan(&ctx, "SELECT avg(big), sum(big) FROM t").await;
        let twice = widen_bigint_aggregates(once.clone()).unwrap();
        assert_eq!(format!("{once:?}"), format!("{twice:?}"));
    }
}
