//! PostgreSQL's `nth_value`, which refuses an offset of zero where DataFusion answers
//! NULL for every row.
//!
//! `nth_value(x, 0)` is the narrowest row in § 2.7 of the gap analysis and one of the
//! worst-shaped: DataFusion computes the index of the row to read, finds that `0` names
//! no row, and returns NULL — for every row, in every partition, with no error and no
//! warning. A client reading that column cannot tell it apart from a window that
//! genuinely had no such row, which is the answer `nth_value(x, 4)` gives over a
//! three-row frame. PostgreSQL never lets the two be confused: it raises
//! `22016 invalid_argument_for_nth_value` before evaluating anything.
//!
//! ## Why zero and not every `n < 1`
//!
//! PostgreSQL's rule is `n >= 1`; DataFusion reads a *negative* `n` as an offset from the
//! end of the frame, so `nth_value(x, -1)` answers the last row. That is a **superset**
//! listed in § 5 of the gap analysis and it must keep working: it accepts a statement
//! PostgreSQL rejects, which no PostgreSQL-compatible client can be relying on, and it
//! is a documented VaireDB extension. Zero is not a superset. It is a *wrong answer* to
//! a statement PostgreSQL rejects, and the only one of the two that a client can read as
//! data. So the guard below fires on exactly `0`.
//!
//! ## Why a shadowing window function and not a planning-time refusal
//!
//! A check on the logical plan would raise for `SELECT nth_value(n, 0) OVER () FROM t`
//! whatever `t` holds — including when `t` is empty, where PostgreSQL answers **no rows
//! and no error**, because it evaluates the argument per partition and an empty input has
//! none. Registering a `WindowUDF` under DataFusion's own name (the same shadowing
//! [`crate::within_group`] does for `percentile_cont`) puts the check where PostgreSQL puts it:
//! in the construction of the per-partition evaluator, which `WindowAggExec` reaches only
//! after it has seen at least one row.
//!
//! Everything else — the evaluator, the declared field, the coercion, the simplifier
//! hook — is delegated to DataFusion's own `nth_value`, so no answer this function
//! returns is VaireDB's rather than the engine's. `reverse_expr` returns *this* function
//! and not DataFusion's, since a window the optimizer chooses to evaluate backwards must
//! keep the guard.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! | Input | PostgreSQL | Here |
//! |---|---|---|
//! | `nth_value(x, 0) OVER ()` | `22016` | `22016` — the gap; used to be a NULL per row |
//! | `nth_value(x, 1) OVER ()` | first row of the frame | unchanged, DataFusion's |
//! | `nth_value(x, 4) OVER ()`, 3-row frame | NULL | unchanged, DataFusion's |
//! | `nth_value(x, -1) OVER ()` | `22016` | **last row of the frame** — the § 5 superset |
//! | `nth_value(x, 0)` over an **empty** table | no rows, no error | no rows, no error |
//!
//! ## Why the message is PostgreSQL's wording
//!
//! A window runs inside a Ballista executor, and an error raised there reaches the
//! coordinator as text with its type gone (§ 1.3). The coordinator recognizes this one by
//! its message — [`error_code_of_message`] owns the wording so the two sides cannot drift
//! — and reports `22016` rather than the `XX000` that an unclassified execution failure
//! would get. `argument of nth_value must be greater than zero` is what PostgreSQL 17
//! writes, character for character.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::functions_window::nth_value::nth_value_udwf;
use datafusion::logical_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowFunctionSimplification, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Documentation, LimitEffect, PartitionEvaluator, ReversedUDWF, Signature, WindowUDF,
    WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::proto::vairedb::v1::VdbErrorCode;

/// PostgreSQL's own name, because this function replaces DataFusion's under it.
///
/// Unlike [`crate::float_div`], whose name is VaireDB's own, shadowing is the point
/// here: the statement a client writes is `nth_value`, and the rule PostgreSQL applies to
/// it has to be the one that runs.
pub const NTH_VALUE_UDWF_NAME: &str = "nth_value";

/// PostgreSQL 17's wording for the refusal, character for character.
///
/// Also the value the coordinator matches on to recover `22016` from an error that
/// crossed the Ballista boundary as text — see [`error_code_of_message`].
pub const NON_POSITIVE_OFFSET_MESSAGE: &str = "argument of nth_value must be greater than zero";

/// Register PostgreSQL's `nth_value` on `registry`, replacing DataFusion's.
///
/// Call this on every context that plans **or** executes a read. A window function
/// crosses the Ballista wire as a name with no definition attached, so one registered
/// only on the coordinator would be resolved back to DataFusion's own — and the guard
/// would vanish on precisely the nodes that evaluate the window.
pub fn register_nth_value(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udwf(pg_nth_value_udwf())?;
    Ok(())
}

/// The shared [`WindowUDF`] handle registered under `nth_value`.
pub fn pg_nth_value_udwf() -> Arc<WindowUDF> {
    static UDWF: OnceLock<Arc<WindowUDF>> = OnceLock::new();
    Arc::clone(UDWF.get_or_init(|| Arc::new(WindowUDF::from(PgNthValue::new()))))
}

/// The `VdbErrorCode` for a message raised by this function, or `None` if the message is
/// not one of ours.
///
/// The coordinator calls this on an error whose type is already gone, so the wording
/// lives here beside the code that raises it rather than being repeated there.
pub fn error_code_of_message(msg: &str) -> Option<VdbErrorCode> {
    msg.to_lowercase()
        .contains(&NON_POSITIVE_OFFSET_MESSAGE.to_lowercase())
        .then_some(VdbErrorCode::InvalidArgumentForNthValue)
}

/// DataFusion's `nth_value`, borrowed for the lifetime of the process.
///
/// A `static` `OnceLock` hands back a `&'static` reference, which is what lets the
/// delegating methods below return the borrowed `Signature` and `Documentation` DataFusion
/// owns instead of copies of them.
fn datafusion_nth_value() -> &'static Arc<dyn WindowUDFImpl> {
    static INNER: OnceLock<Arc<dyn WindowUDFImpl>> = OnceLock::new();
    INNER.get_or_init(|| Arc::clone(nth_value_udwf().inner()))
}

/// `nth_value(x, n)` — DataFusion's, with PostgreSQL's refusal of `n = 0` in front of it.
///
/// Private, because the only thing outside this module that has any use for it is the
/// registry, and [`pg_nth_value_udwf`] is what hands it one.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgNthValue;

impl PgNthValue {
    fn new() -> Self {
        Self
    }
}

/// Delegates, so it implements every method of the trait: a defaulted method left out
/// here would be DataFusion's default rather than DataFusion's `nth_value`, which is a
/// silent behaviour change in a function whose whole purpose is to keep them identical.
impl WindowUDFImpl for PgNthValue {
    fn name(&self) -> &str {
        NTH_VALUE_UDWF_NAME
    }

    fn signature(&self) -> &Signature {
        datafusion_nth_value().signature()
    }

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        datafusion_nth_value().expressions(expr_args)
    }

    /// The guard, and the whole of what this function adds.
    ///
    /// `WindowAggExec` builds an evaluator per partition and only after the input has
    /// produced a row, so raising here raises for exactly the statements PostgreSQL
    /// raises for — and not for a statement that selected nothing.
    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        reject_zero_offset(partition_evaluator_args.input_exprs())?;
        datafusion_nth_value().partition_evaluator(partition_evaluator_args)
    }

    fn aliases(&self) -> &[String] {
        datafusion_nth_value().aliases()
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        datafusion_nth_value().simplify()
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        datafusion_nth_value().field(field_args)
    }

    fn sort_options(&self) -> Option<SortOptions> {
        datafusion_nth_value().sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        datafusion_nth_value().coerce_types(arg_types)
    }

    /// Reversed into **this** function and not DataFusion's.
    ///
    /// DataFusion's `nth_value` reverses into `nth_value` with the sign of `n` flipped;
    /// naming its own function here would hand the optimizer a way to plan the window
    /// backwards and lose the guard on the way.
    fn reverse_expr(&self) -> ReversedUDWF {
        ReversedUDWF::Reversed(pg_nth_value_udwf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        datafusion_nth_value().documentation()
    }

    fn limit_effect(&self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        datafusion_nth_value().limit_effect(args)
    }
}

/// Raise PostgreSQL's `22016` when the offset argument is the literal `0`.
///
/// Only the value `0` refuses. A **negative** literal is the § 5 superset — DataFusion's
/// offset from the end of the frame — and answers as it always has.
///
/// `is_reversed` needs no attention: DataFusion negates `n` for a reversed window, and
/// the negation of zero is zero.
fn reject_zero_offset(input_exprs: &[Arc<dyn PhysicalExpr>]) -> Result<()> {
    if literal_integer_offset(input_exprs) == Some(0) {
        return exec_err!("{NON_POSITIVE_OFFSET_MESSAGE}");
    }
    Ok(())
}

/// The offset argument of `nth_value(x, n)`, when it is an integer literal.
///
/// That is the only shape the rule above reads, because every other one is DataFusion's to
/// refuse and answering `None` here leaves it to DataFusion:
///
/// * the offset is **absent** for `nth_value(x)`, which is malformed;
/// * a **non-literal** offset is something DataFusion itself rejects a moment later
///   (its evaluator needs a constant `n`), so its error is left to it;
/// * a **non-integer** literal is likewise DataFusion's to refuse, and reading `0.4` as
///   zero here would raise "must be greater than zero" about a number that is;
/// * a literal too wide for an `i64`, or a NULL one, is no more a zero than it is a row
///   number DataFusion can use.
fn literal_integer_offset(input_exprs: &[Arc<dyn PhysicalExpr>]) -> Option<i64> {
    let offset: &dyn Any = input_exprs.get(1)?.as_ref();
    let literal = offset.downcast_ref::<Literal>()?;
    if !literal.value().data_type().is_integer() {
        return None;
    }
    match literal.value().cast_to(&DataType::Int64) {
        Ok(ScalarValue::Int64(Some(offset))) => Some(offset),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::col;
    use std::ops::Range;

    /// The column every case below reads its answer out of.
    fn values() -> Vec<ArrayRef> {
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))]
    }

    /// The one-column input the window is evaluated over.
    fn input_schema() -> Schema {
        Schema::new(vec![Field::new("x", DataType::Int32, true)])
    }

    /// Build the evaluator the way a `WindowAggExec` does, over the given arguments.
    fn evaluator_over(
        input_exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let input_fields = input_schema().fields().to_vec();
        PgNthValue::new().partition_evaluator(PartitionEvaluatorArgs::new(
            input_exprs,
            &input_fields,
            false,
            false,
        ))
    }

    /// Build the evaluator over `nth_value(x, n)` for a literal offset.
    fn evaluator(n: ScalarValue) -> Result<Box<dyn PartitionEvaluator>> {
        evaluator_over(&[
            col("x", &input_schema()).expect("column"),
            Arc::new(Literal::new(n)),
        ])
    }

    /// The answer `nth_value(x, n)` gives over the whole three-row frame.
    fn nth_value_over_three_rows(n: i64) -> Result<ScalarValue> {
        let mut evaluator = evaluator(ScalarValue::Int64(Some(n)))?;
        evaluator.evaluate(&values(), &Range { start: 0, end: 3 })
    }

    /// The gap itself: this used to build an evaluator that answered NULL for every row.
    ///
    /// At every integer width, because `nth_value(x, 0)` is an `Int64` zero only if the
    /// planner happened to type it that way — and with the message every time, because the
    /// message is what the coordinator recovers `22016` from.
    #[test]
    fn a_zero_offset_raises_instead_of_answering_null() {
        for zero in [
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(0)),
        ] {
            let err = evaluator(zero.clone()).expect_err("should have raised");
            assert!(
                err.to_string().contains(NON_POSITIVE_OFFSET_MESSAGE),
                "for {zero:?}: {err}"
            );
        }
    }

    /// The offsets PostgreSQL accepts still answer exactly what they answered before,
    /// which is the over-reach probe: a guard that refused these would be a worse gap
    /// than the one it closed.
    #[test]
    fn a_positive_offset_still_answers_its_row() {
        assert_eq!(
            nth_value_over_three_rows(1).expect("should not raise"),
            ScalarValue::Int32(Some(10))
        );
        assert_eq!(
            nth_value_over_three_rows(3).expect("should not raise"),
            ScalarValue::Int32(Some(30))
        );
        // An offset past the end of the frame is PostgreSQL's NULL, and stays one — the
        // answer `nth_value(x, 0)` used to be indistinguishable from.
        assert_eq!(
            nth_value_over_three_rows(4).expect("should not raise"),
            ScalarValue::Int32(None)
        );
    }

    /// The § 5 superset: a negative offset counts from the end of the frame. PostgreSQL
    /// raises `22016` for this too, and VaireDB deliberately does not.
    #[test]
    fn a_negative_offset_still_counts_from_the_end() {
        assert_eq!(
            nth_value_over_three_rows(-1).expect("should not raise"),
            ScalarValue::Int32(Some(30))
        );
        assert_eq!(
            nth_value_over_three_rows(-3).expect("should not raise"),
            ScalarValue::Int32(Some(10))
        );
    }

    /// The refusal end to end through the planner, which is the shape a client meets: the
    /// statement reaches the registered function by name and the message that classifies as
    /// `22016` is the one it gets back.
    ///
    /// The unit cases above call [`WindowUDFImpl::partition_evaluator`] themselves; only this
    /// one proves the guard is still there once DataFusion has resolved the name, coerced the
    /// arguments and built the window.
    #[tokio::test]
    async fn a_zero_offset_raises_through_the_planner() {
        let mut ctx = SessionContext::new();
        register_nth_value(&mut ctx).expect("registration failed");
        let batch = RecordBatch::try_new(
            Arc::new(input_schema()),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .expect("batch");
        ctx.register_batch("t", batch).expect("register");

        let err = ctx
            .sql("SELECT nth_value(x, 0) OVER () FROM t")
            .await
            .expect("should plan")
            .collect()
            .await
            .expect_err("should have raised");
        assert!(
            err.to_string().contains(NON_POSITIVE_OFFSET_MESSAGE),
            "the message is what classifies as 22016: {err}"
        );
    }

    /// A non-integer offset is DataFusion's to refuse, and `0.4` must not be read as a
    /// zero — "must be greater than zero" about `0.4` would be a false statement.
    #[test]
    fn a_non_integer_offset_is_left_to_datafusion() {
        let err = evaluator(ScalarValue::Float64(Some(0.4))).expect_err("should have raised");
        assert!(
            !err.to_string().contains(NON_POSITIVE_OFFSET_MESSAGE),
            "not ours to refuse: {err}"
        );
    }

    /// A non-literal offset likewise: DataFusion needs a constant `n` and says so.
    #[test]
    fn a_non_literal_offset_is_left_to_datafusion() {
        let schema = input_schema();
        let err = evaluator_over(&[
            col("x", &schema).expect("column"),
            col("x", &schema).expect("column"),
        ])
        .expect_err("should have raised");
        assert!(
            !err.to_string().contains(NON_POSITIVE_OFFSET_MESSAGE),
            "not ours to refuse: {err}"
        );
    }

    /// The advertised type is DataFusion's, so shadowing the function cannot move the
    /// OID a driver binds its receive buffer from.
    #[test]
    fn the_declared_field_is_datafusions() {
        let input_fields = vec![Arc::new(Field::new("x", DataType::Int32, false)) as FieldRef];
        let ours = PgNthValue::new()
            .field(WindowUDFFieldArgs::new(&input_fields, "nth_value(x,1)"))
            .expect("field");
        let theirs = nth_value_udwf()
            .field(WindowUDFFieldArgs::new(&input_fields, "nth_value(x,1)"))
            .expect("field");
        assert_eq!(ours, theirs);
    }

    /// A reversed window keeps the guard, which it would not if this reversed into
    /// DataFusion's own `nth_value`.
    #[test]
    fn the_reverse_of_this_function_is_this_function() {
        match PgNthValue::new().reverse_expr() {
            ReversedUDWF::Reversed(udwf) => assert!(udwf.inner().is::<PgNthValue>()),
            ReversedUDWF::Identical => panic!("nth_value is not its own reverse"),
            ReversedUDWF::NotSupported => panic!("nth_value is reversible"),
        }
    }

    /// The wire carries only the name, so the name has to resolve to *ours* after
    /// registration — a context that resolved DataFusion's would answer NULL again. Twice,
    /// because a context reached by two registration paths registers twice.
    #[test]
    fn the_name_resolves_to_this_function_after_registration() {
        let mut ctx = SessionContext::new();
        let before = ctx
            .udwf(NTH_VALUE_UDWF_NAME)
            .expect("datafusion registers one");
        assert!(!before.inner().is::<PgNthValue>());

        register_nth_value(&mut ctx).expect("first registration failed");
        register_nth_value(&mut ctx).expect("second registration failed");
        let after = ctx.udwf(NTH_VALUE_UDWF_NAME).expect("ours");
        assert!(after.inner().is::<PgNthValue>());
    }

    /// `first_value` and `last_value` share DataFusion's implementation with `nth_value`
    /// and are untouched by the registration — neither of them takes an offset.
    #[test]
    fn first_value_and_last_value_are_left_alone() {
        let mut ctx = SessionContext::new();
        register_nth_value(&mut ctx).expect("registration failed");
        for name in ["first_value", "last_value"] {
            assert!(
                !ctx.udwf(name).expect(name).inner().is::<PgNthValue>(),
                "{name} was replaced"
            );
        }
    }

    /// The message the coordinator recovers `22016` from, in prose and in the `Debug`
    /// rendering a Ballista stage failure arrives as.
    #[test]
    fn the_message_classifies_as_the_nth_value_code() {
        assert_eq!(
            error_code_of_message(NON_POSITIVE_OFFSET_MESSAGE),
            Some(VdbErrorCode::InvalidArgumentForNthValue)
        );
        assert_eq!(
            error_code_of_message(&format!(
                "Job 3QdcFzH failed: Job failed due to stage 1 failed: Task failed due to \
                 runtime execution error: DataFusionError(Execution(\"{NON_POSITIVE_OFFSET_MESSAGE}\"))"
            )),
            Some(VdbErrorCode::InvalidArgumentForNthValue)
        );
        // And the error this function actually raises is recognized by its own rule.
        let err = nth_value_over_three_rows(0).expect_err("should have raised");
        assert_eq!(
            error_code_of_message(&err.to_string()),
            Some(VdbErrorCode::InvalidArgumentForNthValue)
        );
        // Nothing else is claimed.
        assert_eq!(error_code_of_message("division by zero"), None);
    }
}
