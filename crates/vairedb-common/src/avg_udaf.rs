//! PostgreSQL's `avg` over an integer column, at PostgreSQL's own sixteen decimal places.
//!
//! `avg(smallint)`, `avg(integer)` and `avg(bigint)` are all `numeric` in PostgreSQL, and
//! the value is exact: the sum of the integers divided by their count, rounded half away
//! from zero. How many decimal places that division keeps is chosen **per value** —
//! `select_div_scale` gives sixteen significant digits after the point for a quotient below
//! `10⁴`, and fewer as the quotient grows — so `avg` of `1, 2, 2` is
//! `1.6666666666666667` and `avg` of two `bigint`s near `i64::MAX` is an integer with no
//! fractional part at all.
//!
//! An Arrow column has **one** scale for every row in it, so a per-value scale cannot be
//! reproduced and the only question is which constant to pick. Sixteen places is the
//! answer here, because it is the one PostgreSQL prints for the values a client is most
//! likely to be diffing — the sixteen-place case is the *whole* of what
//! `NUMERIC_MIN_SIG_DIGITS` promises, and a large average is a rendering with more zeros
//! after it rather than a different number.
//!
//! ## Why an aggregate of VaireDB's own and not `avg` with a wider argument
//!
//! The read path used to reach these sixteen places the way it still reaches `sum`'s exact
//! total: by casting the *argument*, since DataFusion's `avg` over a `Decimal128(38, s)`
//! accumulates in one. That cannot get past ten places without paying for them, and the
//! price is set by DataFusion's own arithmetic rather than by the accumulation:
//!
//! * `avg(Decimal128(38, s))` returns `Decimal128(38, s + 4)`, so sixteen places need an
//!   argument at scale 12 — and the accumulated total then carries those twelve zeros on
//!   every row, which is twelve digits of the 38 spent before a value is even added.
//! * `DecimalAvgAccumulator` divides **after** scaling: it evaluates
//!   `Σuᵢ · 10^(result scale − argument scale) / n`, so the product has to fit an `i128`
//!   *before* the division brings it back down. At ten places that bounds `Σ|xᵢ|` at
//!   `1.7 × 10²⁸`; at sixteen it would bound it at `1.7 × 10²²`, which is a billion rows of
//!   `10¹³` — reachable, and reported as `XX000` because the overflow is raised as
//!   `DataFusionError::Internal`.
//!
//! So the scale is applied here at the **end**, to the quotient and not to the total.
//! `n` and `Σuᵢ` are kept exactly (`u64` and `i128` over the `bigint`s themselves, which is
//! the narrowest total that cannot lose a digit), and the answer is
//! `⌊Σuᵢ · 10¹⁶ / n⌉` computed as a whole part plus a scaled remainder so that no
//! intermediate product is wider than the two operands were. The average of a `bigint`
//! column is at most `9.22 × 10¹⁸`, which needs 19 digits before the point and 16 after —
//! 35 of the 38 a `numeric` on this wire carries — so **no integer column has an average
//! this cannot report**, and the total itself only overflows past `10³⁸`, which is `10¹⁹`
//! rows of `i64::MAX`.
//!
//! Being an aggregate rather than a cast is also what keeps the choice of which calls are
//! exact where it belongs. PostgreSQL resolves `avg` on the argument's **own** type —
//! `avg(n)` over an `integer` column and `avg(n::numeric)` are different functions — and
//! the plan rewrite that emits this call is the one thing in the read path that still sees
//! the argument's own type, before `TypeCoercion` erases the difference. A `numeric` or
//! `double precision` argument never reaches here at all: it stays DataFusion's `avg`,
//! answered exactly as it was, so nothing this module does can narrow the range of a column
//! whose values are already wider than an integer's.
//!
//! ## Where it has to be registered
//!
//! On every context that plans **or** executes a read, like every other function in this
//! crate: an aggregate crosses the Ballista wire as a name, so a registry that misses this
//! one fails a stage the coordinator already accepted. See
//! [`register_exact_average`].
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! | Query over `n integer` = 1, 2, 2 | PostgreSQL | Here |
//! |---|---|---|
//! | `avg(n)` | `numeric` `1.6666666666666667` | `numeric` `1.6666666666666667` |
//! | `avg(n) OVER ()` | same | same |
//! | `avg(DISTINCT n)` | `1.5000000000000000` | `1.5000000000000000` |
//! | `avg(n)` over no rows | NULL | NULL |
//! | `avg(4611686018427387904::bigint), avg(…906)` | `4611686018427387905` | the same digits, then `.0000000000000000` |
//! | `avg(n::float8)` | `float8` | unchanged, DataFusion's |

use std::collections::HashSet;
use std::mem::size_of_val;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Decimal128Builder, Int64Array,
    ListArray, UInt64Array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type, UInt64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    Signature, Volatility,
};

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name the read path emits and every node resolves the call by.
///
/// Prefixed, and deliberately not a shadow of `avg`: this aggregate answers integers only,
/// and which arguments are integers is a question the plan rewrite settles before type
/// coercion runs. A function registered under `avg` would be asked the question afterwards,
/// when a `bigint` and a `numeric` argument have become the same expression.
pub const EXACT_AVG_UDAF_NAME: &str = "vaire_avg";

/// The precision the answer is reported at: Arrow's widest 128-bit decimal, which is what
/// `sum`'s exact total and [`crate::stats_udaf`] report too.
const RESULT_PRECISION: u8 = 38;

/// The decimal places of the answer — PostgreSQL's own sixteen, for the reason in the
/// module doc.
const RESULT_SCALE: i8 = 16;

/// `10¹⁶`, the factor a quotient is scaled by to become the unscaled integer of a
/// `Decimal128(38, 16)`.
const RESULT_MULTIPLIER: u128 = 10u128.pow(RESULT_SCALE as u32);

/// The magnitude a running total must stay under, so that it is a value the
/// `Decimal128(38, 0)` it crosses the wire as can hold.
const TOTAL_CEILING: i128 = 10i128.pow(RESULT_PRECISION as u32);

/// Register VaireDB's exact integer average on `registry`.
///
/// Call this on every context that plans **or** executes a read. The result type matters on
/// the coordinator, which advertises it; the accumulator matters on the executor, which
/// produces the value the advertised type describes. A registry holding one and not the
/// other would promise `numeric` and fail the stage that had to compute it.
pub fn register_exact_average(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udaf(exact_average_udaf())?;
    Ok(())
}

/// The shared [`AggregateUDF`] handle, for the read-path rewrite that builds the call.
///
/// One instance, because the rewrite clones it into every expression it replaces and the
/// function holds nothing but its signature.
pub fn exact_average_udaf() -> Arc<AggregateUDF> {
    static UDAF: OnceLock<Arc<AggregateUDF>> = OnceLock::new();
    Arc::clone(UDAF.get_or_init(|| Arc::new(AggregateUDF::from(ExactAverage::new()))))
}

/// `vaire_avg(<integer>)` — the exact average of an integer column, as `numeric(38, 16)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExactAverage {
    signature: Signature,
}

impl Default for ExactAverage {
    fn default() -> Self {
        Self::new()
    }
}

impl ExactAverage {
    pub fn new() -> Self {
        Self {
            // User-defined, because the only coercion this aggregate admits is the widening
            // of one integer to another — see [`Self::coerce_types`], which is also what
            // refuses every type the rewrite does not send here.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ExactAverage {
    fn name(&self) -> &str {
        EXACT_AVG_UDAF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Every integer width PostgreSQL answers `avg` in `numeric` for, widened to `bigint`.
    ///
    /// The cast is exact for all three — a `smallint` is a `bigint` — so the accumulator
    /// has one input type rather than three, and the totals mean the same thing whichever
    /// width the column was. Anything else is a plan error: this aggregate is reached from
    /// [`vairedb_coordinator`]'s aggregate rewrite, which sends it the integer arguments
    /// only, and answering something it cannot answer exactly would defeat the point of
    /// having it.
    ///
    /// [`vairedb_coordinator`]: https://docs.rs/vairedb-coordinator
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{EXACT_AVG_UDAF_NAME} takes exactly one argument, got {}",
                arg_types.len()
            );
        };
        match arg {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(vec![DataType::Int64]),
            other => plan_err!(
                "{EXACT_AVG_UDAF_NAME} averages smallint, integer and bigint exactly, not \
                 {other} — use avg({other})"
            ),
        }
    }

    /// `numeric(38, 16)`, always: [`Self::coerce_types`] admits nothing else.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let state = |suffix: &str, data_type: DataType| {
            Arc::new(Field::new(
                format!("{}[{suffix}]", args.name),
                data_type,
                true,
            )) as FieldRef
        };
        if args.is_distinct {
            // The values themselves, because whether a value counts towards the total
            // depends on every value before it — see [`ExactAverageAccumulator::distinct`].
            return Ok(vec![state(
                "distinct",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            )]);
        }
        // What a shard sends a final aggregate: `n` and `Σuᵢ`. Merging is two additions,
        // and the total crosses at scale 0 — the scale of the answer is applied to the
        // quotient, so no digit of it is spent on the wire.
        Ok(vec![
            state("count", DataType::UInt64),
            state("sum", DataType::Decimal128(RESULT_PRECISION, 0)),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExactAverageAccumulator::new(args.is_distinct)))
    }

    /// The same accumulator: it can retract, so a sliding window frame needs no second one.
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }

    /// Yes — unlike [`crate::stats_udaf`], whose `i256` totals have no vectorised form.
    ///
    /// `avg` is the aggregate a grouped analytical query reaches for most often, so the
    /// exactness is not bought with the throughput of `GROUP BY`: two totals per group are
    /// exactly what DataFusion's own decimal average keeps, and
    /// [`ExactAverageGroupsAccumulator`] keeps them in the same shape this aggregate's
    /// state fields describe.
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.is_distinct {
            return internal_err!(
                "{EXACT_AVG_UDAF_NAME} has no groups accumulator for DISTINCT values"
            );
        }
        Ok(Box::new(ExactAverageGroupsAccumulator::default()))
    }

    /// An average does not depend on the order its values arrive in, so a reversed window
    /// frame computes the same number.
    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

/// The out-of-range refusal, tagged so it survives the trip from an executor.
///
/// This runs on the node that evaluates the aggregate, and a `DataFusionError` raised there
/// reaches the coordinator as text with its variant gone (§ 1.3 of the gap analysis).
/// Without the tag the client would be told `XX000 internal_error` — that the server broke
/// and the statement is worth retrying — where the truth is `22003`: the answer does not
/// fit, and it will not fit on a retry either.
fn out_of_range() -> DataFusionError {
    DataFusionError::Execution(tagged_message(
        VdbErrorCode::NumericValueOutOfRange,
        format!(
            "the exact average of these values does not fit \
             numeric({RESULT_PRECISION}, {RESULT_SCALE})"
        ),
    ))
}

/// `n` and `Σuᵢ` over the `bigint`s of one group, and everything that can be said about a
/// total without knowing how many of them there are.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Totals {
    count: u64,
    sum: i128,
}

impl Totals {
    /// Fold `count` values summing to `sum` in, `None` if the total left the range a
    /// `Decimal128(38, 0)` can carry to another node.
    fn merge(&mut self, count: u64, sum: i128) -> Option<()> {
        self.count = self.count.checked_add(count)?;
        self.sum = self.sum.checked_add(sum)?;
        match self.sum.checked_abs()? < TOTAL_CEILING {
            true => Some(()),
            false => None,
        }
    }

    /// Take one value back out, for a sliding window frame.
    fn remove(&mut self, value: i64) -> Option<()> {
        self.count = self.count.checked_sub(1)?;
        self.sum = self.sum.checked_sub(i128::from(value))?;
        Some(())
    }
}

/// The unscaled integer of the `Decimal128(38, 16)` average of `totals`, or `None` for a
/// group PostgreSQL answers NULL for.
///
/// `Σuᵢ · 10¹⁶ / n` rounded half away from zero, which is PostgreSQL's own rule for a
/// `numeric` division, computed as a whole part plus a scaled remainder so that the widest
/// intermediate is `(n − 1) · 10¹⁶` rather than `Σuᵢ · 10¹⁶`. The sign is carried outside
/// the arithmetic: away from zero and up are then the same rounding.
fn exact_average(totals: Totals) -> std::result::Result<Option<i128>, OutOfRange> {
    if totals.count == 0 {
        return Ok(None);
    }
    let count = u128::from(totals.count);
    let magnitude = totals.sum.unsigned_abs();

    let whole = magnitude / count;
    let remainder = magnitude % count;
    let scaled_remainder = remainder.checked_mul(RESULT_MULTIPLIER).ok_or(OutOfRange)?;
    let unscaled = whole
        .checked_mul(RESULT_MULTIPLIER)
        .ok_or(OutOfRange)?
        .checked_add(scaled_remainder / count)
        .ok_or(OutOfRange)?;
    // `remainder ≥ count − remainder` rather than `2 · remainder ≥ count`, so that the
    // comparison cannot overflow: `0 ≤ remainder < count`.
    let remainder = scaled_remainder % count;
    let unscaled = match remainder >= count - remainder {
        true => unscaled.checked_add(1).ok_or(OutOfRange)?,
        false => unscaled,
    };

    // The last step, and the one that keeps a plausible wrong number off the wire: a value
    // wider than the declared precision is not reported at a lower one. No integer column
    // reaches it — a `bigint` average needs 35 of the 38 digits — so this fires only for a
    // total that was already past `10³⁸`.
    let unscaled = i128::try_from(unscaled).map_err(|_| OutOfRange)?;
    if unscaled >= TOTAL_CEILING {
        return Err(OutOfRange);
    }
    Ok(Some(match totals.sum.is_negative() {
        true => -unscaled,
        false => unscaled,
    }))
}

/// The answer does not fit `Decimal128(38, 16)`, or a total outgrew the range it crosses the
/// wire in.
///
/// Kept as a unit type rather than a `DataFusionError` so the arithmetic above has no
/// opinion about how a refusal is reported.
#[derive(Debug, PartialEq, Eq)]
struct OutOfRange;

/// Accumulates `n` and `Σuᵢ` and answers their exact quotient.
#[derive(Debug, Default)]
struct ExactAverageAccumulator {
    totals: Totals,
    /// The values seen, for `DISTINCT`.
    ///
    /// `Some` only for `avg(DISTINCT x)` beside another aggregate: the optimizer turns a
    /// lone `DISTINCT` aggregate into a group-by and this stays `None`.
    distinct: Option<HashSet<i64>>,
}

impl ExactAverageAccumulator {
    fn new(is_distinct: bool) -> Self {
        Self {
            totals: Totals::default(),
            distinct: is_distinct.then(HashSet::new),
        }
    }

    /// The values in `array`, checked to be the `bigint`s the accumulator was opened for.
    fn integers<'a>(&self, array: &'a ArrayRef) -> Result<&'a Int64Array> {
        match array.data_type() {
            DataType::Int64 => Ok(array.as_primitive::<Int64Type>()),
            other => internal_err!("{EXACT_AVG_UDAF_NAME} accumulates a bigint, not {other}"),
        }
    }

    /// The totals to answer from, which for `DISTINCT` are folded only now.
    fn totals(&self) -> Result<Totals> {
        let Some(seen) = &self.distinct else {
            return Ok(self.totals);
        };
        let mut totals = Totals::default();
        for value in seen {
            totals
                .merge(1, i128::from(*value))
                .ok_or_else(out_of_range)?;
        }
        Ok(totals)
    }
}

impl Accumulator for ExactAverageAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return internal_err!("{EXACT_AVG_UDAF_NAME} needs a value to accumulate");
        };
        let values = self.integers(values)?;
        if let Some(seen) = &mut self.distinct {
            seen.extend(values.iter().flatten());
            return Ok(());
        }
        for value in values.iter().flatten() {
            self.totals
                .merge(1, i128::from(value))
                .ok_or_else(out_of_range)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return internal_err!("{EXACT_AVG_UDAF_NAME} needs a value to retract");
        };
        if self.distinct.is_some() {
            return internal_err!("{EXACT_AVG_UDAF_NAME} cannot retract a DISTINCT value");
        }
        let values = self.integers(values)?;
        for value in values.iter().flatten() {
            self.totals.remove(value).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{EXACT_AVG_UDAF_NAME} retracted a value it had not accumulated"
                ))
            })?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(seen) = &mut self.distinct {
            let Some(state) = states.first() else {
                return internal_err!(
                    "{EXACT_AVG_UDAF_NAME} needs its own DISTINCT state to merge"
                );
            };
            for partial in state.as_list::<i32>().iter().flatten() {
                match partial.data_type() {
                    DataType::Int64 => {
                        seen.extend(partial.as_primitive::<Int64Type>().iter().flatten())
                    }
                    other => {
                        return internal_err!(
                            "{EXACT_AVG_UDAF_NAME} cannot merge a DISTINCT list of {other}"
                        );
                    }
                }
            }
            return Ok(());
        }
        let [counts, sums] = states else {
            return internal_err!(
                "{EXACT_AVG_UDAF_NAME} merges two totals, got {}",
                states.len()
            );
        };
        let counts = counts.as_primitive::<UInt64Type>();
        let sums = sums.as_primitive::<Decimal128Type>();
        for row in 0..counts.len() {
            // A group that saw no values contributes nothing rather than refusing: a
            // partial aggregate over an empty partition is a legitimate state to merge.
            let count = match counts.is_null(row) {
                true => 0,
                false => counts.value(row),
            };
            let sum = match sums.is_null(row) {
                true => 0,
                false => sums.value(row),
            };
            self.totals.merge(count, sum).ok_or_else(out_of_range)?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if let Some(seen) = &self.distinct {
            let values = Int64Array::from_iter_values(seen.iter().copied());
            let element = Field::new_list_field(DataType::Int64, true);
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
            let list = ListArray::new(Arc::new(element), offsets, Arc::new(values), None);
            return Ok(vec![ScalarValue::List(Arc::new(list))]);
        }
        Ok(vec![
            ScalarValue::UInt64(Some(self.totals.count)),
            ScalarValue::Decimal128(Some(self.totals.sum), RESULT_PRECISION, 0),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let answer = exact_average(self.totals()?).map_err(|OutOfRange| out_of_range())?;
        Ok(ScalarValue::Decimal128(
            answer,
            RESULT_PRECISION,
            RESULT_SCALE,
        ))
    }

    fn supports_retract_batch(&self) -> bool {
        self.distinct.is_none()
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .distinct
                .as_ref()
                .map(|seen| seen.capacity() * size_of_val(&0i64))
                .unwrap_or(0)
    }
}

/// The same two totals, one pair per group.
///
/// The vectorised shape of the accumulator above, and the reason a grouped `avg` over an
/// integer column costs no more than DataFusion's own. A group that has seen no value has
/// `count == 0`, which is what makes it NULL on the way out — there is no separate null
/// state to keep, because a count of zero already says everything one would.
#[derive(Debug, Default)]
struct ExactAverageGroupsAccumulator {
    totals: Vec<Totals>,
}

impl ExactAverageGroupsAccumulator {
    /// Make room for `total_num_groups`, which grows as new groups are seen.
    fn widen(&mut self, total_num_groups: usize) {
        self.totals.resize(total_num_groups, Totals::default());
    }

    /// Fold a partial total into one group.
    fn merge(&mut self, group: usize, count: u64, sum: i128) -> Result<()> {
        let Some(totals) = self.totals.get_mut(group) else {
            return internal_err!(
                "{EXACT_AVG_UDAF_NAME} was given group {group} of {}",
                self.totals.len()
            );
        };
        totals.merge(count, sum).ok_or_else(out_of_range)
    }

    /// The groups `emit_to` asks for, taken out of the state.
    fn emit(&mut self, emit_to: EmitTo) -> Vec<Totals> {
        emit_to.take_needed(&mut self.totals)
    }
}

/// Whether row `row` of a batch is one the aggregate sees.
///
/// A filter that is NULL for a row excludes it, which is what `FILTER (WHERE …)` means for
/// a predicate that did not evaluate to true.
fn included(opt_filter: Option<&BooleanArray>, row: usize) -> bool {
    match opt_filter {
        None => true,
        Some(filter) => filter.is_valid(row) && filter.value(row),
    }
}

impl GroupsAccumulator for ExactAverageGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let Some(values) = values.first() else {
            return internal_err!("{EXACT_AVG_UDAF_NAME} needs a value to accumulate");
        };
        let DataType::Int64 = values.data_type() else {
            return internal_err!(
                "{EXACT_AVG_UDAF_NAME} accumulates a bigint, not {}",
                values.data_type()
            );
        };
        let values = values.as_primitive::<Int64Type>();
        self.widen(total_num_groups);
        for (row, group) in group_indices.iter().enumerate() {
            if values.is_null(row) || !included(opt_filter, row) {
                continue;
            }
            self.merge(*group, 1, i128::from(values.value(row)))?;
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [counts, sums] = values else {
            return internal_err!(
                "{EXACT_AVG_UDAF_NAME} merges two totals, got {}",
                values.len()
            );
        };
        let counts = counts.as_primitive::<UInt64Type>();
        let sums = sums.as_primitive::<Decimal128Type>();
        self.widen(total_num_groups);
        for (row, group) in group_indices.iter().enumerate() {
            if !included(opt_filter, row) {
                continue;
            }
            let count = match counts.is_null(row) {
                true => 0,
                false => counts.value(row),
            };
            let sum = match sums.is_null(row) {
                true => 0,
                false => sums.value(row),
            };
            self.merge(*group, count, sum)?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emitted = self.emit(emit_to);
        let mut answers = Decimal128Builder::with_capacity(emitted.len());
        for totals in emitted {
            match exact_average(totals).map_err(|OutOfRange| out_of_range())? {
                Some(answer) => answers.append_value(answer),
                None => answers.append_null(),
            }
        }
        let answers = answers
            .finish()
            .with_precision_and_scale(RESULT_PRECISION, RESULT_SCALE)?;
        Ok(Arc::new(answers))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emitted = self.emit(emit_to);
        let counts = UInt64Array::from_iter_values(emitted.iter().map(|totals| totals.count));
        let sums = Decimal128Array::from_iter_values(emitted.iter().map(|totals| totals.sum))
            .with_precision_and_scale(RESULT_PRECISION, 0)?;
        Ok(vec![Arc::new(counts), Arc::new(sums)])
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.totals.capacity() * size_of_val(&Totals::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::Schema;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{Expr, col};

    /// The totals of a set of `bigint`s.
    fn totals(values: &[i64]) -> Totals {
        let mut totals = Totals::default();
        for value in values {
            totals
                .merge(1, i128::from(*value))
                .expect("no overflow in a test fixture");
        }
        totals
    }

    /// One average over integer values, rendered the way the wire renders it.
    fn average(values: &[i64]) -> Option<String> {
        let unscaled = exact_average(totals(values)).expect("in range")?;
        let scalar = ScalarValue::Decimal128(Some(unscaled), RESULT_PRECISION, RESULT_SCALE);
        let array = scalar.to_array().expect("one value");
        Some(arrow::util::display::array_value_to_string(&array, 0).expect("rendered"))
    }

    /// The non-terminating case, which is the whole of the gap this module closes:
    /// PostgreSQL answers `1.6666666666666667` and so does this.
    #[test]
    fn a_non_terminating_average_carries_postgresqls_sixteen_places() {
        assert_eq!(average(&[1, 2, 2]).as_deref(), Some("1.6666666666666667"));
    }

    /// Rounding is half away from zero, PostgreSQL's rule for a `numeric` division, and it
    /// is the same rule on both sides of zero. `2/3` is `0.666…` rounded **up** in the last
    /// place; `−2/3` is the same digits with a sign.
    #[test]
    fn a_repeating_average_rounds_half_away_from_zero() {
        assert_eq!(average(&[0, 0, 2]).as_deref(), Some("0.6666666666666667"));
        assert_eq!(average(&[0, 0, -2]).as_deref(), Some("-0.6666666666666667"));
        // Exactly a half in the seventeenth place, which is the boundary the rule decides:
        // 1/2 of 10⁻¹⁶ rounds away from zero rather than to even.
        assert_eq!(average(&[1, 2]).as_deref(), Some("1.5000000000000000"));
        assert_eq!(average(&[-1, -2]).as_deref(), Some("-1.5000000000000000"));
    }

    /// The two `bigint`s whose exact average is past a `f64`'s 53 bits of mantissa. The
    /// digits are PostgreSQL's; the sixteen zeros after them are the fixed scale, which
    /// PostgreSQL would not print because it chooses its own per value.
    #[test]
    fn a_bigint_average_uses_nineteen_digits_and_still_fits() {
        assert_eq!(
            average(&[4611686018427387904, 4611686018427387906]).as_deref(),
            Some("4611686018427387905.0000000000000000")
        );
        // And the widest average an integer column has at all, which is the value that
        // decides whether sixteen places fit: 19 digits before the point, 16 after, 3 to
        // spare.
        assert_eq!(
            average(&[i64::MAX]).as_deref(),
            Some("9223372036854775807.0000000000000000")
        );
        assert_eq!(
            average(&[i64::MIN]).as_deref(),
            Some("-9223372036854775808.0000000000000000")
        );
    }

    /// No rows is NULL, not zero — PostgreSQL's answer for an average over nothing.
    #[test]
    fn an_average_over_no_rows_is_null() {
        assert_eq!(exact_average(Totals::default()), Ok(None));
    }

    /// The total is refused rather than wrapped, and the refusal is the one a client can
    /// act on. `10³⁸` needs `10¹⁹` rows of `i64::MAX`, so this is reachable only by
    /// constructing the total directly.
    #[test]
    fn a_total_past_the_wire_ceiling_is_refused() {
        let mut totals = Totals {
            count: 1,
            sum: TOTAL_CEILING - 1,
        };
        assert!(totals.merge(1, 1).is_none(), "one past the ceiling refuses");
        assert!(
            out_of_range().message().contains("numeric(38, 16)"),
            "the refusal names the type the value did not fit: {}",
            out_of_range().message()
        );
    }

    /// Retracting is what a sliding window frame does, and it has to leave the totals
    /// exactly where they were before the value arrived.
    #[test]
    fn retracting_a_value_undoes_accumulating_it() {
        let mut totals = totals(&[10, 20, 30]);
        totals.remove(30).expect("accumulated");
        assert_eq!(totals, super::tests::totals(&[10, 20]));
        assert_eq!(exact_average(totals), Ok(Some(15 * 10i128.pow(16))));
    }

    /// One table: two `bigint` values whose exact average is past `2⁵³`, and the same
    /// magnitudes as `integer`.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("big", DataType::Int64, true),
            Field::new("small", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(4611686018427387904),
                    Some(4611686018427387906),
                    None,
                ])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(2)])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let mut ctx = ctx;
        register_exact_average(&mut ctx).unwrap();
        ctx
    }

    /// The first column of a one-row answer: the type the plan promised and the value, with
    /// the two checked against each other — a plan that promises `numeric(38, 16)` over a
    /// column of another type reaches the client as an error rather than as a number.
    async fn one_row(sql: &str) -> (DataType, String) {
        let ctx = ctx();
        let df = ctx.sql(sql).await.unwrap();
        let planned = df.schema().field(0).data_type().clone();
        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);
        assert_eq!(
            column.data_type(),
            &planned,
            "the plan's schema and the data have to agree for `{sql}`"
        );
        let text = arrow::util::display::array_value_to_string(column, 0).unwrap();
        (planned, text)
    }

    /// End to end through a session: the advertised type is the decimal, and the value is
    /// the exact average of the two rows that are not NULL.
    #[tokio::test]
    async fn a_bigint_average_is_exact_and_advertised_as_numeric() {
        let (dt, text) = one_row("SELECT vaire_avg(big) FROM t").await;
        assert_eq!(dt, DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE));
        assert_eq!(text, "4611686018427387905.0000000000000000");
    }

    /// A narrower integer column is the same aggregate: the coercion widens it, and the
    /// answer keeps the same sixteen places.
    #[tokio::test]
    async fn an_integer_average_is_the_same_aggregate() {
        let (dt, text) = one_row("SELECT vaire_avg(small) FROM t").await;
        assert_eq!(dt, DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE));
        assert_eq!(text, "1.6666666666666667");
    }

    /// NULLs are skipped rather than counted, which is what makes the average above
    /// `…905` and not two thirds of it.
    #[tokio::test]
    async fn a_column_of_only_nulls_averages_to_null() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT vaire_avg(big) FROM t WHERE big IS NULL")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(batches[0].column(0).is_null(0), "no values, no average");
    }

    /// The grouped path, which is [`ExactAverageGroupsAccumulator`] rather than the
    /// accumulator above, and has to answer the same numbers.
    #[tokio::test]
    async fn the_grouped_average_answers_what_the_ungrouped_one_does() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT small, vaire_avg(small) FROM t GROUP BY small \
                 ORDER BY small",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rendered = |batch: &RecordBatch, column: usize, row: usize| {
            arrow::util::display::array_value_to_string(batch.column(column), row).unwrap()
        };
        assert_eq!(rendered(&batches[0], 1, 0), "1.0000000000000000");
        assert_eq!(rendered(&batches[0], 1, 1), "2.0000000000000000");
    }

    /// The window spelling, over a frame that slides — so the accumulator retracts as well
    /// as accumulates, and the running average of the first row is that row alone.
    #[tokio::test]
    async fn a_windowed_average_over_a_sliding_frame_is_exact() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT vaire_avg(small) OVER (ORDER BY small \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let text = arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap();
        assert_eq!(text, "1.0000000000000000");
    }

    /// `DISTINCT` beside another aggregate, which is the shape the optimizer cannot rewrite
    /// into a group-by and so hands to the accumulator with its own de-duplication. The
    /// three `integer` rows are 1, 2, 2, so the distinct average is 1.5 and the plain one
    /// is 5/3.
    #[tokio::test]
    async fn a_distinct_average_counts_each_value_once() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT vaire_avg(DISTINCT small), vaire_avg(small) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rendered = |column: usize| {
            arrow::util::display::array_value_to_string(batches[0].column(column), 0).unwrap()
        };
        assert_eq!(rendered(0), "1.5000000000000000");
        assert_eq!(rendered(1), "1.6666666666666667");
    }

    /// Every type PostgreSQL does not answer `avg` in `numeric` for is refused by name
    /// rather than answered in a type this aggregate cannot compute exactly.
    #[tokio::test]
    async fn a_non_integer_argument_is_refused_at_planning() {
        let ctx = ctx();
        let err = ctx
            .sql("SELECT vaire_avg(big::float8) FROM t")
            .await
            .expect_err("a float has no exact average");
        assert!(
            err.message().contains("use avg("),
            "the refusal names the function that does answer it: {}",
            err.message()
        );
    }

    /// The registration is idempotent, because the two contexts that hold this aggregate
    /// are built by code that may run more than once.
    #[test]
    fn registering_twice_is_registering_once() {
        let mut ctx = SessionContext::new();
        register_exact_average(&mut ctx).expect("first registration failed");
        register_exact_average(&mut ctx).expect("second registration failed");
        assert!(
            ctx.state()
                .aggregate_functions()
                .contains_key(EXACT_AVG_UDAF_NAME)
        );
    }

    /// The expression the rewrite builds resolves to this aggregate, which is the contract
    /// between the two halves of the fix: the coordinator names it and the executor
    /// resolves the name.
    #[test]
    fn the_shared_handle_is_named_what_the_rewrite_emits() {
        let udaf = exact_average_udaf();
        assert_eq!(udaf.name(), EXACT_AVG_UDAF_NAME);
        let call: Expr = udaf.call(vec![col("big")]);
        assert!(
            call.schema_name()
                .to_string()
                .starts_with(EXACT_AVG_UDAF_NAME),
            "the call renders under its own name: {call}"
        );
    }
}
