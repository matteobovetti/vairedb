//! PostgreSQL's ordered-set aggregates: `percentile_cont` and `percentile_disc`.
//!
//! Both are registered on **every** context that plans or executes a read, which is
//! why they live here rather than in the coordinator: a Ballista stage crosses the
//! wire naming its aggregate, and the executor resolves that name in its own
//! registry. One implementation in one crate is what keeps the two sides from
//! answering the same query differently.
//!
//! ## Why `percentile_cont` is shadowed rather than used
//!
//! DataFusion ships a `percentile_cont`, and its distribution is right, but it
//! quantizes the interpolation weight to six decimal places and *floors* it
//! (`(fraction * 1_000_000.0) as usize`). Interpolating between two neighbours whose
//! weight lands just under a representable boundary therefore loses the last part of
//! the step: `percentile_cont(0.9)` over the integers 1..10 answers `9.099999` where
//! PostgreSQL answers `9.1`, because `0.9 * 9` is `8.099999999999999644` in binary
//! floating point and the weight `0.099999999999999644` floors to `0.099999`.
//! Registering a UDAF under the same name replaces it; interpolating at full `f64`
//! precision is the whole of the fix.
//!
//! `percentile_disc` DataFusion does not have at all.
//!
//! ## Why one array-shaped accumulator instead of a generic per-type one
//!
//! `percentile_disc` returns **a value from the input**, so its result type is the
//! ordered column's type — any type an `ORDER BY` accepts, not just a numeric one.
//! Accumulating Arrow arrays and sorting them with `sort_to_indices` gives every such
//! type one code path, and lets `percentile_cont` reuse it after a cast to `float8`
//! (which is what PostgreSQL returns for every input it accepts here).
//!
//! Both are exact, so both hold the whole group in memory — the same trade DataFusion's
//! own `percentile_cont` makes, and the reason `approx_percentile_cont` exists.

use std::any::Any;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, new_empty_array};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, cast, concat, sort_to_indices};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err, not_impl_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, Signature, TypeSignatureClass,
    Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

/// Register the ordered-set aggregates on `registry`, replacing DataFusion's
/// `percentile_cont` (and its `quantile_cont` alias) with the exact one.
///
/// Call this on every context that plans **or** executes a read; see the module doc.
pub fn register_ordered_set_aggregates(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udaf(Arc::new(AggregateUDF::from(PercentileCont::new())))?;
    registry.register_udaf(Arc::new(AggregateUDF::from(PercentileDisc::new())))?;
    Ok(())
}

/// How a percentile that falls between two rows is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Interpolation {
    /// `percentile_cont`: interpolate linearly between the neighbours, in `float8`.
    Continuous,
    /// `percentile_disc`: return one of the input values, unchanged.
    Discrete,
}

/// `percentile_cont(fraction) WITHIN GROUP (ORDER BY expr)` — PostgreSQL's continuous
/// percentile, returning `double precision`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PercentileCont {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileCont {
    pub fn new() -> Self {
        // Both arguments are `float8` to PostgreSQL, and any numeric input is cast to
        // it — which is also why the result is always `float8`, never the narrower
        // input type DataFusion's own version hands back for a `float4` column.
        let float8 = || {
            Coercion::new_implicit(
                TypeSignatureClass::Native(logical_float64()),
                vec![TypeSignatureClass::Numeric],
                NativeType::Float64,
            )
        };
        Self {
            signature: Signature::coercible(vec![float8(), float8()], Volatility::Immutable),
            // Kept so shadowing DataFusion's function does not take its alias away.
            aliases: vec![String::from("quantile_cont")],
        }
    }
}

impl AggregateUDFImpl for PercentileCont {
    fn name(&self) -> &str {
        "percentile_cont"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        PercentileAccumulator::open(Interpolation::Continuous, self.name(), args)
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// `percentile_disc(fraction) WITHIN GROUP (ORDER BY expr)` — PostgreSQL's discrete
/// percentile, returning one of the input values with its own type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PercentileDisc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PercentileDisc {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileDisc {
    pub fn new() -> Self {
        Self {
            // The ordered value keeps its own type — PostgreSQL's signature is
            // `(float8) WITHIN GROUP (ORDER BY anyelement) -> anyelement` — so nothing
            // may be coerced here. The fraction is read as a literal instead, which
            // also copes with it arriving as `numeric` under `parse_float_as_decimal`.
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("quantile_disc")],
        }
    }
}

impl AggregateUDFImpl for PercentileDisc {
    fn name(&self) -> &str {
        "percentile_disc"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(ordered) => Ok(ordered.clone()),
            None => internal_err!("percentile_disc was called without an ordered value"),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        PercentileAccumulator::open(Interpolation::Discrete, self.name(), args)
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// The intermediate state of both aggregates: every value seen, as a list.
///
/// A partial aggregate cannot be summarised into anything smaller — an exact
/// percentile needs the whole distribution — so the state is the values themselves,
/// which is what lets the final aggregate merge partials from every shard.
fn state_fields(function: &str, args: &StateFieldsArgs) -> Result<Vec<FieldRef>> {
    let Some(ordered) = args.input_fields.first() else {
        return internal_err!("{function} was called without an ordered value");
    };
    let element = Field::new_list_field(ordered.data_type().clone(), true);
    Ok(vec![
        Field::new(
            format!("{}[{function}]", args.name),
            DataType::List(Arc::new(element)),
            true,
        )
        .into(),
    ])
}

/// Accumulates the group's values and answers the percentile once it has them all.
#[derive(Debug)]
struct PercentileAccumulator {
    kind: Interpolation,
    /// The fraction, already resolved from its literal argument.
    fraction: f64,
    /// `ORDER BY … DESC`, which counts the percentile from the other end.
    descending: bool,
    /// The ordered column's type, needed to describe an empty group's state.
    ordered_type: DataType,
    /// The aggregate's own result type.
    result_type: DataType,
    /// One entry per batch seen, concatenated only when the answer is needed.
    values: Vec<ArrayRef>,
}

impl PercentileAccumulator {
    fn open(
        kind: Interpolation,
        function: &str,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            // PostgreSQL has no `DISTINCT` in an ordered-set aggregate's direct
            // arguments, and de-duplicating the *ordered* values would answer a
            // different question than the one asked.
            return not_impl_err!("DISTINCT is not supported for {function}");
        }
        let Some(ordered) = args.expr_fields.first() else {
            return internal_err!("{function} was called without an ordered value");
        };
        Ok(Box::new(Self {
            kind,
            fraction: fraction_argument(args.exprs, function)?,
            descending: args
                .order_bys
                .first()
                .map(|sort| sort.options.descending)
                .unwrap_or(false),
            ordered_type: ordered.data_type().clone(),
            result_type: args.return_type().clone(),
            values: Vec::new(),
        }))
    }

    /// Every value seen so far, in one array.
    fn accumulated(&self) -> Result<ArrayRef> {
        match self.values.len() {
            0 => Ok(new_empty_array(&self.ordered_type)),
            1 => Ok(Arc::clone(&self.values[0])),
            _ => {
                let parts: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();
                Ok(concat(&parts)?)
            }
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(ordered) = values.first() else {
            return internal_err!("a percentile needs an ordered value to accumulate");
        };
        if !ordered.is_empty() {
            self.values.push(Arc::clone(ordered));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(state) = states.first() else {
            return internal_err!("a percentile needs its own state to merge");
        };
        let partials = state.as_list::<i32>();
        for partial in partials.iter().flatten() {
            if !partial.is_empty() {
                self.values.push(partial);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = self.accumulated()?;
        let element = Field::new_list_field(values.data_type().clone(), true);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
        let list = ListArray::new(Arc::new(element), offsets, values, None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        percentile_of(
            &self.accumulated()?,
            self.fraction,
            self.descending,
            self.kind,
            &self.result_type,
        )
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .values
                .iter()
                .map(|values| values.get_array_memory_size())
                .sum::<usize>()
    }
}

/// The percentile of `values`, ignoring nulls the way every aggregate does.
///
/// Kept separate from the accumulator so the distribution rules — which are the part
/// that has to match PostgreSQL exactly — can be tested on an array alone.
fn percentile_of(
    values: &ArrayRef,
    fraction: f64,
    descending: bool,
    kind: Interpolation,
    result_type: &DataType,
) -> Result<ScalarValue> {
    // `percentile_cont` answers in `float8` whatever it was given. Coercion normally
    // has already cast the input, so this is usually a no-op.
    let values = match kind {
        Interpolation::Continuous if values.data_type() != &DataType::Float64 => {
            cast(values, &DataType::Float64)?
        }
        _ => Arc::clone(values),
    };

    // An empty group is a null, not an error — and nulls do not count towards the
    // distribution, so `sort_to_indices` is asked to park them past the end.
    let rows = values.len() - values.null_count();
    if rows == 0 {
        return ScalarValue::try_from(result_type);
    }
    let order = sort_to_indices(
        &values,
        Some(SortOptions {
            descending,
            nulls_first: false,
        }),
        None,
    )?;
    let row = |rank: usize| order.value(rank) as usize;

    match kind {
        Interpolation::Discrete => {
            // PostgreSQL returns the first value whose cumulative distribution is at
            // least the fraction. The k-th of n rows has distribution k/n, so that is
            // row ceil(fraction * n) — and row 1 when the fraction is 0.
            let rank = ((fraction * rows as f64).ceil() as usize).clamp(1, rows) - 1;
            ScalarValue::try_from_array(&values, row(rank))
        }
        Interpolation::Continuous => {
            let floats = values.as_primitive::<Float64Type>();
            let position = fraction * (rows - 1) as f64;
            let below = position.floor();
            let above = position.ceil();
            let low = floats.value(row(below as usize));
            if below == above {
                return Ok(ScalarValue::Float64(Some(low)));
            }
            let high = floats.value(row(above as usize));
            // The weight is used at full precision. Rounding it to a fixed number of
            // decimals here is exactly the upstream defect this function exists to
            // avoid — see the module doc.
            Ok(ScalarValue::Float64(Some(
                low + (high - low) * (position - below),
            )))
        }
    }
}

/// Read the fraction out of the aggregate's second argument.
///
/// It has to be a literal — the fraction is fixed for the whole group, so there is no
/// row to evaluate an expression against — and it is read leniently, because
/// `parse_float_as_decimal` makes `0.9` arrive as `numeric` rather than `float8`.
fn fraction_argument(args: &[Arc<dyn PhysicalExpr>], function: &str) -> Result<f64> {
    let Some(argument) = args.get(1) else {
        return plan_err!("{function} requires a percentile fraction");
    };
    // Upcast rather than call `as_any()`: Arrow's `Array` is in scope in this module and
    // owns that method name too.
    let argument: &dyn Any = argument.as_ref();
    let Some(literal) = argument.downcast_ref::<Literal>() else {
        return plan_err!("the percentile fraction for {function} must be a literal");
    };
    let fraction = match literal.value().cast_to(&DataType::Float64) {
        Ok(ScalarValue::Float64(Some(fraction))) => fraction,
        _ => {
            return plan_err!(
                "the percentile fraction for {function} must be a number between 0 and 1, not {}",
                literal.value()
            );
        }
    };
    if !(0.0..=1.0).contains(&fraction) {
        // PostgreSQL: "percentile value 1.5 is not between 0 and 1".
        return exec_err!("percentile value {fraction} is not between 0 and 1");
    }
    Ok(fraction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};

    fn floats(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn cont(values: &ArrayRef, fraction: f64) -> f64 {
        match percentile_of(
            values,
            fraction,
            false,
            Interpolation::Continuous,
            &DataType::Float64,
        )
        .unwrap()
        {
            ScalarValue::Float64(Some(answer)) => answer,
            other => panic!("percentile_cont must answer a float8, got {other:?}"),
        }
    }

    fn disc(values: &ArrayRef, fraction: f64) -> ScalarValue {
        percentile_of(
            values,
            fraction,
            false,
            Interpolation::Discrete,
            values.data_type(),
        )
        .unwrap()
    }

    // The defect this module exists for. DataFusion's own `percentile_cont` answers
    // 9.099999 here, because it floors the interpolation weight to six decimals.
    #[test]
    fn the_interpolation_weight_is_not_quantized() {
        let values = floats(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        assert_eq!(cont(&values, 0.9), 9.1);
        assert_eq!(cont(&values, 0.25), 3.25);
        assert_eq!(cont(&values, 0.5), 5.5);
        assert_eq!(cont(&values, 0.33), 3.97);
    }

    #[test]
    fn a_fraction_that_lands_on_a_row_needs_no_interpolation() {
        let values = floats(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        assert_eq!(cont(&values, 0.0), 10.0);
        assert_eq!(cont(&values, 0.25), 20.0);
        assert_eq!(cont(&values, 1.0), 50.0);
    }

    // Order matters, and only through the ORDER BY: the input arrives in whatever
    // order the shards produced it.
    #[test]
    fn the_input_order_does_not_change_the_answer() {
        let sorted = floats(&[1.0, 2.0, 3.0, 4.0]);
        let shuffled = floats(&[3.0, 1.0, 4.0, 2.0]);
        assert_eq!(cont(&sorted, 0.4), cont(&shuffled, 0.4));
    }

    #[test]
    fn a_descending_order_counts_from_the_other_end() {
        let values = floats(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let descending = |fraction| match percentile_of(
            &values,
            fraction,
            true,
            Interpolation::Continuous,
            &DataType::Float64,
        )
        .unwrap()
        {
            ScalarValue::Float64(Some(answer)) => answer,
            other => panic!("expected a float8, got {other:?}"),
        };
        assert_eq!(descending(0.0), 10.0);
        assert_eq!(descending(1.0), 1.0);
        // Not asserted to the last bit: interpolating downwards from 2 towards 1 rounds
        // one ULP away from where interpolating upwards from 1 towards 2 lands, and
        // PostgreSQL's own `float8` arithmetic does the same. What matters is that the
        // answer is the far end's percentile, not that the two spellings agree bitwise.
        assert!(
            (descending(0.9) - 1.9).abs() < 1e-12,
            "the 0.9 percentile of a descending order is 1.9, got {}",
            descending(0.9)
        );
    }

    // PostgreSQL's rule: the first value whose cumulative distribution reaches the
    // fraction, i.e. row ceil(fraction * n), and never row 0.
    #[test]
    fn the_discrete_percentile_returns_a_value_from_the_input() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        assert_eq!(disc(&values, 0.0), ScalarValue::Int32(Some(1)));
        assert_eq!(disc(&values, 0.05), ScalarValue::Int32(Some(1)));
        assert_eq!(disc(&values, 0.1), ScalarValue::Int32(Some(1)));
        assert_eq!(disc(&values, 0.11), ScalarValue::Int32(Some(2)));
        assert_eq!(disc(&values, 0.5), ScalarValue::Int32(Some(5)));
        assert_eq!(disc(&values, 0.9), ScalarValue::Int32(Some(9)));
        assert_eq!(disc(&values, 1.0), ScalarValue::Int32(Some(10)));
    }

    // The reason the accumulator keeps Arrow arrays rather than numbers: PostgreSQL's
    // `percentile_disc` orders by anything an ORDER BY accepts.
    #[test]
    fn the_discrete_percentile_orders_text_too() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["delta", "alpha", "charlie"]));
        assert_eq!(disc(&values, 0.5), ScalarValue::from("charlie"));
        assert_eq!(disc(&values, 0.0), ScalarValue::from("alpha"));
        assert_eq!(disc(&values, 1.0), ScalarValue::from("delta"));
    }

    #[test]
    fn nulls_are_ignored_rather_than_ordered() {
        let with_nulls: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            None,
            Some(3.0),
        ]));
        assert_eq!(cont(&with_nulls, 0.5), 2.0);
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2), Some(3)]));
        assert_eq!(disc(&ints, 0.5), ScalarValue::Int32(Some(2)));
    }

    #[test]
    fn a_group_with_no_values_is_null() {
        let empty = floats(&[]);
        assert_eq!(
            percentile_of(
                &empty,
                0.5,
                false,
                Interpolation::Continuous,
                &DataType::Float64
            )
            .unwrap(),
            ScalarValue::Float64(None)
        );
        let all_null: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        assert_eq!(
            percentile_of(
                &all_null,
                0.5,
                false,
                Interpolation::Discrete,
                &DataType::Int32
            )
            .unwrap(),
            ScalarValue::Int32(None)
        );
    }

    #[test]
    fn one_value_is_its_own_every_percentile() {
        let single = floats(&[42.0]);
        assert_eq!(cont(&single, 0.0), 42.0);
        assert_eq!(cont(&single, 0.37), 42.0);
        assert_eq!(cont(&single, 1.0), 42.0);
        assert_eq!(disc(&single, 0.37), ScalarValue::Float64(Some(42.0)));
    }
}
