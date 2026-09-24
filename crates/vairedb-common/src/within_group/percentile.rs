//! `percentile_cont` and `percentile_disc` — PostgreSQL's two percentiles, which differ in
//! nothing but how a fraction that falls between two rows is answered.
//!
//! ## Why `percentile_cont` is shadowed rather than used
//!
//! DataFusion ships a `percentile_cont`, and its distribution is right, but it quantizes the
//! interpolation weight to six decimal places and *floors* it
//! (`(fraction * 1_000_000.0) as usize`). Interpolating between two neighbours whose weight
//! lands just under a representable boundary therefore loses the last part of the step:
//! `percentile_cont(0.9)` over the integers 1..10 answers `9.099999` where PostgreSQL
//! answers `9.1`, because `0.9 * 9` is `8.099999999999999644` in binary floating point and
//! the weight `0.099999999999999644` floors to `0.099999`. Registering a UDAF under the same
//! name replaces it; interpolating at full `f64` precision is the whole of the fix.
//!
//! `percentile_disc` DataFusion does not have at all.
//!
//! ## Why the two are one aggregate
//!
//! PostgreSQL's two functions differ in their interpolation, in the type they answer in — and
//! in nothing else: same direct argument, same group, same overload, same order. So
//! [`Interpolation`] is the difference, all of it, and both are the same
//! [`AggregateUDFImpl`] reading it. A second struct would have been a second place for the
//! signature, the state and the overload to drift, and drifting on the *order* is a wrong
//! answer no client can detect.
//!
//! ## Why one array-shaped accumulator instead of a generic per-type one
//!
//! `percentile_disc` returns **a value from the input**, so its result type is the ordered
//! column's type — any type an `ORDER BY` accepts, not just a numeric one. Accumulating
//! Arrow arrays and ranking them with [`SortedRows`] gives every such type one code path,
//! and lets `percentile_cont` reuse it after a cast to `float8` (which is what PostgreSQL
//! returns for every input it accepts here).
//!
//! Both are exact, so both hold the whole group in memory — the same trade DataFusion's own
//! `percentile_cont` makes, and the reason `approx_percentile_cont` exists.
//!
//! PostgreSQL's array-of-fractions overload changes none of that, which is why it is not
//! here: see [`super::fractions`] for the direct argument, and note that the group is read
//! once however many fractions are asked of it.

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef, Float64Type};
use datafusion::common::types::{NativeType, logical_float64};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Coercion, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};

use super::clause;
use super::fractions::{self, Fractions};
use super::group::{self, Grouped, SortedRows};

/// The two aggregates this file owns, for [`super::register_within_group_aggregates`].
pub(super) fn udafs() -> Vec<Arc<AggregateUDF>> {
    Interpolation::BOTH
        .into_iter()
        .map(|kind| Arc::new(AggregateUDF::from(Percentile::new(kind))))
        .collect()
}

/// How a percentile that falls between two rows is answered — and so, since they differ in
/// nothing else, which of PostgreSQL's two percentiles this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Interpolation {
    /// `percentile_cont`: interpolate linearly between the neighbours, in `float8`.
    Continuous,
    /// `percentile_disc`: return one of the input values, unchanged.
    Discrete,
}

impl Interpolation {
    const BOTH: [Self; 2] = [Self::Continuous, Self::Discrete];

    /// What the client writes, which is what every message names.
    fn function(self) -> &'static str {
        match self {
            Self::Continuous => "percentile_cont",
            Self::Discrete => "percentile_disc",
        }
    }

    /// The alias DataFusion gives its own `percentile_cont`, kept so that shadowing the
    /// function does not take its alias away — and given to `percentile_disc` too, so the
    /// pair is spelled the same way.
    fn alias(self) -> &'static str {
        match self {
            Self::Continuous => "quantile_cont",
            Self::Discrete => "quantile_disc",
        }
    }

    /// What each accepts, which is the one place their signatures differ.
    ///
    /// `percentile_cont` takes two `float8`s to PostgreSQL, and any numeric input is cast to
    /// it — which is also why its result is always `float8`, never the narrower input type
    /// DataFusion's own version hands back for a `float4` column. `percentile_disc` keeps the
    /// ordered value's own type, because PostgreSQL's signature is
    /// `(float8) WITHIN GROUP (ORDER BY anyelement) -> anyelement`, so nothing may be coerced
    /// for it; its fraction is read as a literal instead, which also copes with the value
    /// arriving as `numeric` under `parse_float_as_decimal`.
    fn signature(self) -> Signature {
        let float8 = || {
            Coercion::new_implicit(
                TypeSignatureClass::Native(logical_float64()),
                vec![TypeSignatureClass::Numeric],
                NativeType::Float64,
            )
        };
        match self {
            // Two branches, tried in order: the scalar fraction, coerced to `float8` like
            // PostgreSQL's own signature; and the array-of-fractions overload, whose direct
            // argument is left exactly as it arrives so that `Fractions` can read the list
            // literal itself. A scalar always matches the first branch, so the second only
            // ever sees what the first could not coerce.
            Self::Continuous => Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![float8(), float8()]),
                    TypeSignature::Coercible(vec![
                        float8(),
                        Coercion::new_exact(TypeSignatureClass::Any),
                    ]),
                ],
                Volatility::Immutable,
            ),
            Self::Discrete => Signature::any(2, Volatility::Immutable),
        }
    }

    /// The type one fraction is answered in: `float8` for the continuous percentile whatever
    /// it was given, and the ordered column's own type for the discrete one, which returns a
    /// value of the input — PostgreSQL's `anyelement`.
    fn answer_type(self, arg_types: &[DataType]) -> Result<DataType> {
        match self {
            Self::Continuous => Ok(DataType::Float64),
            Self::Discrete => match arg_types.first() {
                Some(ordered) => Ok(ordered.clone()),
                None => internal_err!("{} was called without an ordered value", self.function()),
            },
        }
    }
}

/// `percentile_cont(fraction)` and `percentile_disc(fraction) WITHIN GROUP (ORDER BY expr)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Percentile {
    kind: Interpolation,
    signature: Signature,
    aliases: Vec<String>,
}

impl Percentile {
    fn new(kind: Interpolation) -> Self {
        Self {
            kind,
            signature: kind.signature(),
            aliases: vec![String::from(kind.alias())],
        }
    }
}

impl AggregateUDFImpl for Percentile {
    fn name(&self) -> &str {
        self.kind.function()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let answer = self.kind.answer_type(arg_types)?;
        // The overload answers one element per fraction, so an array argument answers an
        // array — of `float8` for the one, of the ordered column's type for the other.
        Ok(match fractions::is_list(arg_types.get(1)) {
            true => fractions::list_of(answer),
            false => answer,
        })
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        group::state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        PercentileAccumulator::open(self.kind, args)
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// Accumulates the group's values and answers the percentile once it has them all.
#[derive(Debug)]
struct PercentileAccumulator {
    fractions: Fractions,
    distribution: Distribution,
    group: Grouped,
}

impl PercentileAccumulator {
    fn open(kind: Interpolation, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let function = kind.function();
        let ordered = clause::ordered_column(function, &args)?;
        let fractions = Fractions::of(args.exprs, function)?;
        Ok(Box::new(Self {
            distribution: Distribution {
                kind,
                descending: clause::descending(&args),
                answer_type: answer_type(&fractions, args.return_type()),
            },
            fractions,
            group: Grouped::new(ordered, function),
        }))
    }
}

/// The type one answer wears, which the overload wraps in a list rather than replaces: the
/// aggregate's declared return type is `element[]` in the array case, and the element is what
/// each fraction is answered in.
fn answer_type(fractions: &Fractions, declared: &DataType) -> DataType {
    match (fractions.is_array(), declared) {
        (true, DataType::List(element) | DataType::LargeList(element)) => {
            element.data_type().clone()
        }
        (_, other) => other.clone(),
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.group.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.group.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.group.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Read once whatever the number of fractions: reading the group once for all of them
        // is the whole point of PostgreSQL's overload.
        let values = self.group.accumulated()?;
        let mut answers = Vec::with_capacity(self.fractions.values().len());
        for fraction in self.fractions.values() {
            answers.push(self.distribution.percentile_of(&values, *fraction)?);
        }
        if !self.fractions.is_array() {
            return match answers.into_iter().next() {
                Some(answer) => Ok(answer),
                None => internal_err!("a percentile needs a fraction to answer"),
            };
        }
        Ok(ScalarValue::List(ScalarValue::new_list(
            &answers,
            &self.distribution.answer_type,
            true,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.group.size()
    }
}

/// The rule that turns a fraction into an answer: which interpolation, which end of the
/// clause's order to count from, and the type the answer wears.
///
/// All three are fixed by the call, so this is built once when the accumulator opens. It is
/// kept apart from the accumulator so the distribution — the part that has to match
/// PostgreSQL exactly — can be tested on an array alone.
#[derive(Debug)]
struct Distribution {
    kind: Interpolation,
    descending: bool,
    answer_type: DataType,
}

impl Distribution {
    /// The percentile of `values` at `fraction`, ignoring nulls the way every aggregate does.
    fn percentile_of(&self, values: &ArrayRef, fraction: f64) -> Result<ScalarValue> {
        // `percentile_cont` answers in `float8` whatever it was given. Coercion normally has
        // already cast the input, so this is usually a no-op.
        let values = match self.kind {
            Interpolation::Continuous if values.data_type() != &DataType::Float64 => {
                cast(values, &DataType::Float64)?
            }
            _ => Arc::clone(values),
        };
        // An empty group is a null, not an error — and nulls do not count towards the
        // distribution, which is the rule `SortedRows` owns.
        let Some(sorted) = SortedRows::of(&values, self.descending)? else {
            return ScalarValue::try_from(&self.answer_type);
        };
        match self.kind {
            Interpolation::Discrete => discrete(&values, &sorted, fraction),
            Interpolation::Continuous => Ok(continuous(&values, &sorted, fraction)),
        }
    }
}

/// PostgreSQL's `percentile_disc`: the first value whose cumulative distribution reaches
/// `fraction`.
///
/// The k-th of n rows has distribution k/n, so that is row ceil(fraction * n) — and row 1
/// when the fraction is 0, which is why the rank is clamped rather than allowed to reach 0.
fn discrete(values: &ArrayRef, sorted: &SortedRows, fraction: f64) -> Result<ScalarValue> {
    let rows = sorted.rows();
    let rank = ((fraction * rows as f64).ceil() as usize).clamp(1, rows) - 1;
    ScalarValue::try_from_array(values, sorted.row(rank))
}

/// PostgreSQL's `percentile_cont`: `fraction` of the way along the n - 1 steps from the first
/// value to the last, interpolated between the two neighbours it lands between.
///
/// `values` is `float8` by the time this is reached, which is what makes the answer one type
/// for every input PostgreSQL accepts here.
fn continuous(values: &ArrayRef, sorted: &SortedRows, fraction: f64) -> ScalarValue {
    let floats = values.as_primitive::<Float64Type>();
    let position = fraction * (sorted.rows() - 1) as f64;
    let below = position.floor();
    let above = position.ceil();
    let low = floats.value(sorted.row(below as usize));
    if below == above {
        return ScalarValue::Float64(Some(low));
    }
    let high = floats.value(sorted.row(above as usize));
    // The weight is used at full precision. Rounding it to a fixed number of decimals here
    // is exactly the upstream defect this file exists to avoid — see the module doc.
    ScalarValue::Float64(Some(low + (high - low) * (position - below)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};

    fn floats(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    /// `percentile_cont(fraction) WITHIN GROUP (ORDER BY …)`, ascending unless said otherwise.
    fn cont_at(values: &ArrayRef, fraction: f64, descending: bool) -> f64 {
        let distribution = Distribution {
            kind: Interpolation::Continuous,
            descending,
            answer_type: DataType::Float64,
        };
        match distribution.percentile_of(values, fraction).unwrap() {
            ScalarValue::Float64(Some(answer)) => answer,
            other => panic!("percentile_cont must answer a float8, got {other:?}"),
        }
    }

    fn cont(values: &ArrayRef, fraction: f64) -> f64 {
        cont_at(values, fraction, false)
    }

    /// `percentile_disc(fraction) WITHIN GROUP (ORDER BY …)`, which answers in the ordered
    /// column's own type.
    fn disc(values: &ArrayRef, fraction: f64) -> ScalarValue {
        let distribution = Distribution {
            kind: Interpolation::Discrete,
            descending: false,
            answer_type: values.data_type().clone(),
        };
        distribution.percentile_of(values, fraction).unwrap()
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
        assert_eq!(cont(&sorted, 0.4), 2.2);
        assert_eq!(cont(&shuffled, 0.4), 2.2);
    }

    #[test]
    fn a_descending_order_counts_from_the_other_end() {
        let values = floats(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        assert_eq!(cont_at(&values, 0.0, true), 10.0);
        assert_eq!(cont_at(&values, 1.0, true), 1.0);
        // Not asserted to the last bit: interpolating downwards from 2 towards 1 rounds
        // one ULP away from where interpolating upwards from 1 towards 2 lands, and
        // PostgreSQL's own `float8` arithmetic does the same. What matters is that the
        // answer is the far end's percentile, not that the two spellings agree bitwise.
        let answer = cont_at(&values, 0.9, true);
        assert!(
            (answer - 1.9).abs() < 1e-12,
            "the 0.9 percentile of a descending order is 1.9, got {answer}"
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
            Distribution {
                kind: Interpolation::Continuous,
                descending: false,
                answer_type: DataType::Float64,
            }
            .percentile_of(&empty, 0.5)
            .unwrap(),
            ScalarValue::Float64(None)
        );
        let all_null: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        assert_eq!(disc(&all_null, 0.5), ScalarValue::Int32(None));
    }

    #[test]
    fn one_value_is_its_own_every_percentile() {
        let single = floats(&[42.0]);
        assert_eq!(cont(&single, 0.0), 42.0);
        assert_eq!(cont(&single, 0.37), 42.0);
        assert_eq!(cont(&single, 1.0), 42.0);
        assert_eq!(disc(&single, 0.37), ScalarValue::Float64(Some(42.0)));
    }

    /// The two answer in different types for the same group, which is the whole of what a
    /// client chooses between: `float8` for the one, the column's own type for the other.
    #[test]
    fn the_two_percentiles_answer_in_the_types_postgresql_declares() {
        let ints: Vec<DataType> = vec![DataType::Int32, DataType::Float64];
        assert_eq!(
            Percentile::new(Interpolation::Continuous)
                .return_type(&ints)
                .unwrap(),
            DataType::Float64,
            "percentile_cont answers float8 for an int column"
        );
        assert_eq!(
            Percentile::new(Interpolation::Discrete)
                .return_type(&ints)
                .unwrap(),
            DataType::Int32,
            "percentile_disc answers a value of the input"
        );
    }

    /// PostgreSQL's overload answers `float8[]` and `anyarray`, and the fraction argument's
    /// shape is the whole of what decides it.
    #[test]
    fn an_array_of_fractions_answers_an_array_of_answers() {
        let with_list = vec![DataType::Int32, fractions::list_of(DataType::Float64)];
        assert_eq!(
            Percentile::new(Interpolation::Continuous)
                .return_type(&with_list)
                .unwrap(),
            fractions::list_of(DataType::Float64)
        );
        assert_eq!(
            Percentile::new(Interpolation::Discrete)
                .return_type(&with_list)
                .unwrap(),
            fractions::list_of(DataType::Int32)
        );
    }

    /// Both are registered under the names a client writes, and both keep the `quantile_`
    /// alias — `percentile_cont` because shadowing DataFusion's function must not take its
    /// alias away.
    #[test]
    fn both_percentiles_are_registered_under_postgresqls_names() {
        let names: Vec<(String, Vec<String>)> = udafs()
            .iter()
            .map(|udaf| (udaf.name().to_string(), udaf.aliases().to_vec()))
            .collect();
        assert_eq!(
            names,
            vec![
                (
                    String::from("percentile_cont"),
                    vec![String::from("quantile_cont")]
                ),
                (
                    String::from("percentile_disc"),
                    vec![String::from("quantile_disc")]
                ),
            ]
        );
    }
}
