//! PostgreSQL's `ntile`, which is an `integer` where DataFusion's is a `UInt64`.
//!
//! `ntile(n)` is the one ranking function PostgreSQL does **not** answer in `bigint`. Its
//! result is a bucket number between 1 and `n`, and `n` is an `int4` argument, so the
//! result is `int4` too — while `row_number()`, `rank()` and `dense_rank()` are all
//! `int8`. DataFusion types all four `UInt64`, a type PostgreSQL has no OID for at all.
//!
//! Three of the four are already right on the wire: [`crate`]'s consumer widens a
//! top-level `UInt64` column to `Int64` before encoding it, which is exactly the `bigint`
//! those three promise. `ntile` is the one the widening gets wrong, because it lands on
//! `int8` where PostgreSQL says `int4` — a driver that bound an `int4` receive buffer from
//! the OID reads four bytes of an eight-byte body, and one that reflects on the column
//! reports the wrong type for a value that would always have fitted.
//!
//! ## Why a shadowing window function
//!
//! The advertised type is read off the **one** logical plan the coordinator's pgwire query
//! path builds for a statement: its schema is the Arrow field the column's type OID is
//! derived from, and Describe and the row encoder both take it from there — so the type has
//! to change in the plan, not after it. A plan rewrite cannot do
//! it: a window function may only appear in a `Window` node's expression list, so wrapping
//! the call in a `CAST` there would make the node malformed, and moving the cast up into
//! the projection above means rewriting every reference to the window's output column.
//!
//! Registering a `WindowUDF` under DataFusion's own name (the same shadowing
//! [`crate::nth_value`] does, and [`crate::within_group`] does for `percentile_cont`) changes the
//! declared field in place, and the expression stays the `ntile(…)` a client wrote.
//!
//! Everything except the declared type is delegated to DataFusion's own `ntile`,
//! including the bucket arithmetic — the rule that the larger buckets come first is the
//! engine's, and the only thing added here is the narrowing of what it produced. The cast
//! is unsafe on purpose: a bucket number past `i32::MAX` needs a partition of more than
//! two billion rows, and PostgreSQL cannot express that query at all (its `ntile`
//! argument is an `int4`), so the honest answer is an error and not a silent NULL.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! | Query | PostgreSQL | Here |
//! |---|---|---|
//! | `ntile(3) OVER (…)` | `int4` | `int4` — the gap; used to be `int8` |
//! | `row_number() OVER (…)` | `int8` | `int8`, unchanged |
//! | buckets of 5 rows into 3 | `1,1,2,2,3` | unchanged, DataFusion's |

use std::sync::{Arc, OnceLock};

use arrow::array::ArrayRef;
use arrow::compute::kernels::cast::cast_with_options;
use arrow::compute::{CastOptions, SortOptions};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::functions_window::ntile::ntile_udwf;
use datafusion::logical_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowFunctionSimplification, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Documentation, LimitEffect, PartitionEvaluator, ReversedUDWF, Signature, WindowUDF,
    WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;

/// PostgreSQL's own name, because this function replaces DataFusion's under it.
pub const NTILE_UDWF_NAME: &str = "ntile";

/// The type PostgreSQL gives a bucket number: `int4`.
const NTILE_RESULT: DataType = DataType::Int32;

/// Register PostgreSQL's `ntile` on `registry`, replacing DataFusion's.
///
/// Call this on every context that plans **or** executes a read. The type matters on the
/// coordinator, which advertises it; the evaluator matters on the executor, which produces
/// the array the declared type describes. A registry that had one and not the other would
/// promise `int4` and hand back eight bytes.
pub fn register_ntile(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udwf(pg_ntile_udwf())?;
    Ok(())
}

/// The shared [`WindowUDF`] handle registered under `ntile`.
pub fn pg_ntile_udwf() -> Arc<WindowUDF> {
    static UDWF: OnceLock<Arc<WindowUDF>> = OnceLock::new();
    Arc::clone(UDWF.get_or_init(|| Arc::new(WindowUDF::from(PgNtile::new()))))
}

/// DataFusion's `ntile`, borrowed for the lifetime of the process.
fn datafusion_ntile() -> &'static Arc<dyn WindowUDFImpl> {
    static INNER: OnceLock<Arc<dyn WindowUDFImpl>> = OnceLock::new();
    INNER.get_or_init(|| Arc::clone(ntile_udwf().inner()))
}

/// `ntile(n)` — DataFusion's buckets, narrowed to the `integer` PostgreSQL returns.
///
/// Private, because the only thing outside this module that has any use for it is the
/// registry, and [`pg_ntile_udwf`] is what hands it one.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgNtile;

impl PgNtile {
    fn new() -> Self {
        Self
    }
}

/// Delegates, so it implements every method of the trait: a defaulted method left out here
/// would be DataFusion's default rather than DataFusion's `ntile`, which is a silent
/// behaviour change in a function whose whole purpose is to keep them identical.
impl WindowUDFImpl for PgNtile {
    fn name(&self) -> &str {
        NTILE_UDWF_NAME
    }

    fn signature(&self) -> &Signature {
        datafusion_ntile().signature()
    }

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        datafusion_ntile().expressions(expr_args)
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NarrowedNtile {
            inner: datafusion_ntile().partition_evaluator(partition_evaluator_args)?,
        }))
    }

    fn aliases(&self) -> &[String] {
        datafusion_ntile().aliases()
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        datafusion_ntile().simplify()
    }

    /// The whole of what this function adds: the field DataFusion declares, retyped.
    ///
    /// The name and the nullability stay DataFusion's, because they are what the plan
    /// above this window refers to the column by.
    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let declared = datafusion_ntile().field(field_args)?;
        Ok(Arc::new(Field::new(
            declared.name(),
            NTILE_RESULT,
            declared.is_nullable(),
        )))
    }

    fn sort_options(&self) -> Option<SortOptions> {
        datafusion_ntile().sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        datafusion_ntile().coerce_types(arg_types)
    }

    /// Reversed into **this** function and not DataFusion's, so a window the optimizer
    /// chooses to evaluate backwards keeps the narrowed type rather than going back to
    /// producing a `UInt64` under an `int4` header.
    fn reverse_expr(&self) -> ReversedUDWF {
        match datafusion_ntile().reverse_expr() {
            ReversedUDWF::Reversed(_) => ReversedUDWF::Reversed(pg_ntile_udwf()),
            other => other,
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        datafusion_ntile().documentation()
    }

    fn limit_effect(&self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        datafusion_ntile().limit_effect(args)
    }
}

/// DataFusion's `ntile` evaluator with its answer narrowed to [`NTILE_RESULT`].
#[derive(Debug)]
struct NarrowedNtile {
    inner: Box<dyn PartitionEvaluator>,
}

impl PartitionEvaluator for NarrowedNtile {
    fn memoize(
        &mut self,
        state: &mut datafusion::logical_expr::window_state::WindowAggState,
    ) -> Result<()> {
        self.inner.memoize(state)
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<std::ops::Range<usize>> {
        self.inner.get_range(idx, n_rows)
    }

    fn is_causal(&self) -> bool {
        self.inner.is_causal()
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        narrow(self.inner.evaluate_all(values, num_rows)?)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        // `ScalarValue::cast_to` refuses an unrepresentable value rather than substituting a
        // NULL, which is the same choice `narrow` makes explicitly for an array.
        self.inner.evaluate(values, range)?.cast_to(&NTILE_RESULT)
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[std::ops::Range<usize>],
    ) -> Result<ArrayRef> {
        narrow(
            self.inner
                .evaluate_all_with_rank(num_rows, ranks_in_partition)?,
        )
    }

    fn supports_bounded_execution(&self) -> bool {
        self.inner.supports_bounded_execution()
    }

    fn uses_window_frame(&self) -> bool {
        self.inner.uses_window_frame()
    }

    fn include_rank(&self) -> bool {
        self.inner.include_rank()
    }
}

/// Narrow one bucket-number array to the declared type.
///
/// `safe: false` so a bucket number that does not fit is an error and not a NULL. It
/// cannot happen for a query PostgreSQL could have written — its `ntile` argument is an
/// `int4` — and a wrong answer is worse than a refusal for one that could not.
fn narrow(values: ArrayRef) -> Result<ArrayRef> {
    if values.data_type() == &NTILE_RESULT {
        return Ok(values);
    }
    Ok(cast_with_options(
        &values,
        &NTILE_RESULT,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::Literal;

    /// The field the bucket count arrives as.
    fn input_field() -> FieldRef {
        Arc::new(Field::new("n", DataType::Int64, false))
    }

    /// Build the evaluator the way a `WindowAggExec` does, over `ntile(n)`.
    fn evaluator(n: i64) -> Result<Box<dyn PartitionEvaluator>> {
        let input_exprs: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(n))))];
        let input_fields = vec![input_field()];
        PgNtile::new().partition_evaluator(PartitionEvaluatorArgs::new(
            &input_exprs,
            &input_fields,
            false,
            false,
        ))
    }

    /// The buckets `ntile(n)` assigns to `rows` rows.
    fn buckets(n: i64, rows: usize) -> Int32Array {
        let values = evaluator(n)
            .expect("ntile takes a positive literal")
            .evaluate_all(&[], rows)
            .expect("buckets");
        values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the buckets are an int4 array")
            .clone()
    }

    /// The gap: the declared type is PostgreSQL's `int4` and not DataFusion's `UInt64`.
    #[test]
    fn the_declared_field_is_an_integer() {
        let input_fields = vec![input_field()];
        let ours = PgNtile::new()
            .field(WindowUDFFieldArgs::new(&input_fields, "ntile(3)"))
            .expect("field");
        assert_eq!(ours.data_type(), &DataType::Int32);

        // The name and nullability are DataFusion's, because the plan above the window
        // refers to the column by them.
        let theirs = ntile_udwf()
            .field(WindowUDFFieldArgs::new(&input_fields, "ntile(3)"))
            .expect("field");
        assert_eq!(ours.name(), theirs.name());
        assert_eq!(ours.is_nullable(), theirs.is_nullable());
        assert_eq!(theirs.data_type(), &DataType::UInt64, "what it used to be");
    }

    /// The buckets themselves are DataFusion's, and the array they arrive in is the one
    /// the declared field describes — a mismatch there is a schema error at execution.
    #[test]
    fn the_buckets_are_datafusions_in_an_integer_array() {
        assert_eq!(
            buckets(3, 6),
            Int32Array::from(vec![1, 1, 2, 2, 3, 3]),
            "6 rows into 3 buckets is even"
        );
        assert_eq!(
            buckets(3, 5),
            Int32Array::from(vec![1, 1, 2, 2, 3]),
            "the remainder goes to the first buckets"
        );
        assert_eq!(buckets(2, 5), Int32Array::from(vec![1, 1, 1, 2, 2]));
        assert_eq!(
            buckets(4, 2),
            Int32Array::from(vec![1, 2]),
            "more buckets than rows"
        );
    }

    /// Delegating means the argument checks are still DataFusion's.
    #[test]
    fn a_non_positive_bucket_count_is_still_refused() {
        assert!(evaluator(0).is_err(), "ntile(0) names no bucket");
    }

    /// The narrowing is unsafe on purpose: an unrepresentable bucket number is an error
    /// rather than the NULL a safe cast would substitute.
    #[test]
    fn a_bucket_number_that_does_not_fit_is_refused() {
        let too_wide: ArrayRef = Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1]));
        assert!(narrow(too_wide).is_err());
    }

    /// The wire carries only the name, so the name has to resolve to *ours* after
    /// registration — a context that resolved DataFusion's would advertise `int8` again.
    /// Twice, because a context reached by two registration paths registers twice.
    #[test]
    fn the_name_resolves_to_this_function_after_registration() {
        let mut ctx = SessionContext::new();
        let before = ctx.udwf(NTILE_UDWF_NAME).expect("datafusion registers one");
        assert!(!before.inner().is::<PgNtile>());

        register_ntile(&mut ctx).expect("first registration failed");
        register_ntile(&mut ctx).expect("second registration failed");
        let after = ctx.udwf(NTILE_UDWF_NAME).expect("ours");
        assert!(after.inner().is::<PgNtile>());
    }

    /// The three ranking functions PostgreSQL *does* answer in `bigint` are untouched:
    /// `ntile` is `int4` because its own result is, not because ranking functions are.
    #[test]
    fn the_bigint_ranking_functions_are_left_alone() {
        let mut ctx = SessionContext::new();
        register_ntile(&mut ctx).expect("registration failed");
        for name in ["row_number", "rank", "dense_rank"] {
            assert!(
                !ctx.udwf(name).expect(name).inner().is::<PgNtile>(),
                "{name} was replaced"
            );
        }
    }

    /// End to end through the planner: the plan's schema is what VaireDB turns into the
    /// column's type OID, so it is the plan and not only the field that has to say `int4`.
    #[tokio::test]
    async fn a_planned_ntile_column_is_typed_int4() {
        use arrow::array::RecordBatch;
        use arrow::datatypes::Schema;

        let mut ctx = SessionContext::new();
        register_ntile(&mut ctx).expect("registration failed");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let df = ctx
            .sql("SELECT ntile(3) OVER (ORDER BY id) AS bucket FROM t")
            .await
            .unwrap();
        assert_eq!(df.schema().field(0).data_type(), &DataType::Int32);

        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].column(0).data_type(), &DataType::Int32);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![1, 1, 2, 2, 3])
        );
    }
}
