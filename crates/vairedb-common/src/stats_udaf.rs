//! PostgreSQL's variance and standard deviation over an exact input, computed exactly.
//!
//! PostgreSQL's rule for this family is the same one it has for `avg`: an **exact** input
//! gets an exact answer. `var_pop`, `var_samp`, `variance`, `stddev`, `stddev_pop` and
//! `stddev_samp` all return `numeric` for a `smallint`, `integer`, `bigint` or `numeric`
//! column, and `double precision` only for `real` and `double precision`. DataFusion has
//! one path for all of them: its signature is `Signature::exact(vec![Float64])` and its
//! accumulator is Welford's algorithm in `f64`, so every one of these answers
//! `double precision`.
//!
//! Two things are wrong with that, and the type is the smaller one:
//!
//! * A client that reflects on the column, or binds a receive buffer from the OID, is told
//!   `float8` where PostgreSQL promises `numeric` — the § 4 Tier 2 row this module closes.
//! * The value is a `f64` computed over integers that do not fit one. `var_pop` of two
//!   `bigint`s near `i64::MAX` is a number with 38 significant digits, and Welford's
//!   algorithm carries 53 bits of mantissa: the answer is plausible, wrong, and has
//!   nothing about it to say so.
//!
//! ## Why a shadowing aggregate and not a plan rewrite
//!
//! [`crate`]'s coordinator fixes `sum` by casting its *argument*, because DataFusion's `sum`
//! over a `Decimal128` already accumulates in one. That does not work here: DataFusion's
//! `return_type` for all four functions is a hardcoded `Float64` behind a signature that
//! admits nothing but `Float64`, so a decimal argument is cast straight back — and it is not
//! how `avg` is fixed either, which is [`crate::avg_udaf`], an aggregate like this one.
//! Registering an `AggregateUDF` under DataFusion's own names replaces
//! them — the same shadowing [`crate::udaf`] does for `percentile_cont` and
//! [`crate::nth_value`] does for `nth_value` — and it is registered under **every** alias
//! DataFusion registers, or a client writing `var_samp` would reach the float version while
//! `var` reached this one.
//!
//! A float input is still DataFusion's to answer, in full: the signature coercion, the
//! state fields, the accumulator, the groups accumulator and the sliding accumulator are
//! all delegated for it. `stddev(double precision)` therefore keeps answering exactly what
//! it answered before, which is what PostgreSQL answers too.
//!
//! ## The arithmetic
//!
//! An exact variance needs no floating point at all. With every value read as the integer
//! `uᵢ` its `Decimal128(38, s)` representation already is — so `xᵢ = uᵢ / 10ˢ` — the
//! accumulator keeps three exact totals, `n`, `Σuᵢ` and `Σuᵢ²`, and
//!
//! ```text
//!            n·Σuᵢ² − (Σuᵢ)²                              SS
//!   var_pop = ──────────────      var_samp = ─────────────────────────────
//!               n² · 10²ˢ                        n(n−1) · 10²ˢ
//! ```
//!
//! is a ratio of integers, evaluated once at the end. `SS = n·Σuᵢ² − (Σuᵢ)²` is the
//! textbook sum of squared deviations multiplied by `n`, and it is never negative. The
//! totals are `i256` because `Σuᵢ²` over `bigint`s outgrows `i128` after two rows, and they
//! are what crosses the wire between a partial and a final aggregate — so a shard's
//! contribution is summarised into three integers rather than shipped as values, and
//! merging shards is three additions.
//!
//! `stddev` is the square root of the same ratio, taken by integer Newton's method at the
//! result's own scale, so no digit of the answer comes from a `f64` either.
//!
//! ## Sixteen decimal places, and the ceiling
//!
//! The result is `Decimal128(38, 16)` — the same type and the same sixteen decimal places
//! [`crate::avg_udaf`] reports, for the same reason: PostgreSQL's own scale for these
//! aggregates is chosen per value (`var_samp` over 1..10 is `9.1666666666666667`, sixteen
//! places) and an Arrow column has one scale for every row in it. Sixteen is what
//! PostgreSQL prints for the values a client is most likely to be diffing — it is the whole
//! of what `NUMERIC_MIN_SIG_DIGITS` promises — and one scale across `avg`, `var` and
//! `stddev` means a client reads the same `numeric(38, 16)` from all of them.
//!
//! 38 digits with 16 after the point leaves 22 before it, which is where the ceiling is: a
//! variance past `10²²` cannot be reported, and neither can a `Σuᵢ²` past `10⁷⁶`. Both are
//! **refused** with `22003` rather than rounded, which is the reason the accumulation is
//! checked at every step. The six places gained here are six digits paid for at that end:
//! a variance of `10²²` needs values around `10¹¹`, where the ten-place version reached
//! `10¹⁴`. It is a fixed-scale answer either way, so the trade is between two visible
//! refusals rather than between an answer and a refusal — and it is `stddev` that a client
//! reaching those magnitudes reads, whose ceiling is the square root of the same bound and
//! so past every value a `bigint` column can produce.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! | Query over `n integer` = 10, 30 | PostgreSQL | Here |
//! |---|---|---|
//! | `var_samp(n)` | `numeric` `200` | `numeric` `200.0000000000000000` |
//! | `var_pop(n)` | `numeric` `100` | `numeric` `100.0000000000000000` |
//! | `stddev(n)` | `numeric` `14.1421356237309505` | `numeric` `14.1421356237309505` |
//! | `stddev(n::float8)` | `float8` `14.142135623730951` | unchanged, DataFusion's |
//! | `var_samp(n)` over one row | NULL | NULL |
//! | `var_pop(n)` over one row | `0` | `0.0000000000000000` |

use std::collections::HashSet;
use std::mem::size_of_val;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, UInt64Type, i256,
};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, GroupsAccumulator, Signature,
    Volatility,
};

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The precision every exact answer is reported at: Arrow's widest 128-bit decimal, which
/// is also the one [`crate`]'s `sum` and `avg` widening reports.
const RESULT_PRECISION: u8 = 38;

/// The decimal places of an exact answer — `avg`'s sixteen, for the reason in the module
/// doc.
const RESULT_SCALE: i8 = 16;

/// The precision the running totals are carried at.
///
/// 76 is the widest `Decimal256` that Arrow will validate a value into, and the totals are
/// serialised as one between a partial and a final aggregate. `Σuᵢ²` past this is refused
/// rather than wrapped.
const TOTAL_PRECISION: u8 = 76;

/// The widest input scale accumulated exactly. Arrow's `Decimal128` holds 38 digits, so a
/// scale past it names no representable value; a negative scale is not a PostgreSQL
/// `numeric` and is left to the float path.
const MAX_INPUT_SCALE: i8 = 38;

/// Register the exact statistics aggregates on `registry`, replacing DataFusion's `var`,
/// `var_pop`, `stddev` and `stddev_pop` — and every alias DataFusion registers them under.
///
/// Call this on every context that plans **or** executes a read. The result type matters on
/// the coordinator, which advertises it; the accumulator matters on the executor, which
/// produces the value the advertised type describes. A registry holding one and not the
/// other would promise `numeric` and hand back a float.
pub fn register_statistics_aggregates(registry: &mut dyn FunctionRegistry) -> Result<()> {
    for kind in Statistic::ALL {
        registry.register_udaf(Arc::new(AggregateUDF::from(ExactStatistic::new(kind))))?;
    }
    Ok(())
}

/// Which of the four statistics an [`ExactStatistic`] is.
///
/// The pair of distinctions is all that separates them: a *sample* statistic divides by
/// `n(n−1)` where a *population* one divides by `n²`, and a standard deviation is the
/// square root of the variance beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Statistic {
    VarSamp,
    VarPop,
    StddevSamp,
    StddevPop,
}

impl Statistic {
    /// In the order [`Statistic::delegate`] indexes.
    const ALL: [Statistic; 4] = [
        Statistic::VarSamp,
        Statistic::VarPop,
        Statistic::StddevSamp,
        Statistic::StddevPop,
    ];

    /// DataFusion's own name for this function, which is the name it is registered under.
    fn name(self) -> &'static str {
        match self {
            // Not `var_samp`: DataFusion's primary name is `var` and `var_samp` is one of
            // its aliases. Registering under the primary name and the same aliases is what
            // makes this replace it under all three spellings.
            Statistic::VarSamp => "var",
            Statistic::VarPop => "var_pop",
            Statistic::StddevSamp => "stddev",
            Statistic::StddevPop => "stddev_pop",
        }
    }

    /// Every other name DataFusion answers to for this function.
    ///
    /// Kept identical to DataFusion's list rather than extended: `register_udaf` inserts a
    /// function under its name **and** each alias, so a spelling missing here would go on
    /// resolving to the float version and answer a different type for the same query.
    /// PostgreSQL's own extra spellings — `variance` and `stddev_samp` — reach this through
    /// `var_samp` and `stddev_samp`, the second of which is already in DataFusion's list.
    fn aliases(self) -> Vec<String> {
        match self {
            Statistic::VarSamp => vec![String::from("var_sample"), String::from("var_samp")],
            Statistic::VarPop => vec![String::from("var_population")],
            Statistic::StddevSamp => vec![String::from("stddev_samp")],
            Statistic::StddevPop => Vec::new(),
        }
    }

    /// DataFusion's function of the same name, borrowed for the lifetime of the process.
    ///
    /// A `static` `OnceLock` hands back a `&'static` reference, which is what lets the
    /// delegating methods below return the `Documentation` DataFusion owns rather than a
    /// copy of it.
    fn delegate(self) -> &'static Arc<AggregateUDF> {
        static DELEGATES: OnceLock<[Arc<AggregateUDF>; 4]> = OnceLock::new();
        let delegates = DELEGATES.get_or_init(|| {
            [
                var_samp_udaf(),
                var_pop_udaf(),
                stddev_udaf(),
                stddev_pop_udaf(),
            ]
        });
        let index = Statistic::ALL
            .iter()
            .position(|kind| *kind == self)
            .expect("every Statistic is in ALL");
        &delegates[index]
    }

    /// Whether the denominator is `n(n−1)` rather than `n²`.
    fn is_sample(self) -> bool {
        matches!(self, Statistic::VarSamp | Statistic::StddevSamp)
    }

    /// Whether the answer is the square root of the variance.
    fn is_deviation(self) -> bool {
        matches!(self, Statistic::StddevSamp | Statistic::StddevPop)
    }

    /// The smallest group that has an answer at all.
    ///
    /// A sample statistic divides by `n−1`, so one row has no sample variance and
    /// PostgreSQL answers NULL; a population statistic over one row is `0`. Both answer
    /// NULL for a group with no rows in it.
    fn minimum_rows(self) -> u64 {
        if self.is_sample() { 2 } else { 1 }
    }
}

/// One of PostgreSQL's four statistics: exact over an exact input, DataFusion's over a
/// float one.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExactStatistic {
    kind: Statistic,
    signature: Signature,
    aliases: Vec<String>,
}

impl ExactStatistic {
    fn new(kind: Statistic) -> Self {
        Self {
            kind,
            // User-defined, because the choice of path *is* the coercion: an exact input is
            // coerced to a decimal and answered here, and everything else is coerced to
            // `Float64` and answered by DataFusion. See [`coercion_target`].
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: kind.aliases(),
        }
    }
}

impl AggregateUDFImpl for ExactStatistic {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{} takes exactly one argument, got {}",
                self.name(),
                arg_types.len()
            );
        };
        Ok(vec![coercion_target(arg)])
    }

    /// `numeric` for an exact input, `double precision` otherwise — PostgreSQL's rule.
    ///
    /// Called with the types [`Self::coerce_types`] chose, so a `Decimal128` here is
    /// exactly the exact path.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Decimal128(_, _)) => {
                Ok(DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE))
            }
            _ => Ok(DataType::Float64),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let Some(input) = args.input_fields.first() else {
            return internal_err!("{} was called without an argument", self.name());
        };
        let Some(scale) = exact_scale(args.return_field.data_type(), input.data_type())? else {
            return self.kind.delegate().state_fields(args);
        };
        let state = |suffix: &str, data_type: DataType| {
            Arc::new(Field::new(
                format!("{}[{suffix}]", args.name),
                data_type,
                true,
            )) as FieldRef
        };
        if args.is_distinct {
            // The values themselves, because de-duplicating them is not something three
            // running totals can do — see [`ExactStatistics::distinct`].
            return Ok(vec![state(
                "distinct",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Decimal128(RESULT_PRECISION, scale),
                    true,
                ))),
            )]);
        }
        // What a shard sends a final aggregate: `n`, `Σuᵢ` and `Σuᵢ²`. Merging is three
        // additions, and no value crosses the wire twice.
        Ok(vec![
            state("count", DataType::UInt64),
            state("sum", DataType::Decimal256(TOTAL_PRECISION, 0)),
            state("sum_squares", DataType::Decimal256(TOTAL_PRECISION, 0)),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.open(args, false)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.open(args, true)
    }

    /// Not on the exact path.
    ///
    /// A grouped exact statistic uses one [`ExactStatistics`] per group instead, which is
    /// the slower of the two shapes DataFusion offers and the only one that can hold `i256`
    /// totals without a second implementation of the same arithmetic. The float path keeps
    /// DataFusion's groups accumulator, so nothing that had it loses it.
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        match exact_input_scale(&args) {
            Ok(Some(_)) => false,
            Ok(None) => self.kind.delegate().groups_accumulator_supported(args),
            Err(_) => false,
        }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if exact_input_scale(&args)?.is_some() {
            return internal_err!(
                "{} has no groups accumulator on the exact path",
                self.name()
            );
        }
        self.kind.delegate().create_groups_accumulator(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.kind.delegate().documentation()
    }
}

impl ExactStatistic {
    /// The accumulator for one call, exact or DataFusion's.
    ///
    /// `sliding` picks which of DataFusion's two the float path gets; the exact one is
    /// already able to retract, so there is only one of it.
    fn open(&self, args: AccumulatorArgs, sliding: bool) -> Result<Box<dyn Accumulator>> {
        let Some(scale) = exact_input_scale(&args)? else {
            return match sliding {
                true => self.kind.delegate().create_sliding_accumulator(args),
                false => self.kind.delegate().accumulator(args),
            };
        };
        Ok(Box::new(ExactStatistics::new(
            self.kind,
            scale,
            args.is_distinct,
        )))
    }
}

/// The type `name(arg)` should be coerced to.
///
/// The exact types PostgreSQL answers in `numeric` become a `Decimal128` whose scale is the
/// input's, so the cast that gets them there cannot lose a digit; everything else becomes
/// `Float64`, which is both what PostgreSQL answers for it and what DataFusion's own
/// signature asks for.
fn coercion_target(arg: &DataType) -> DataType {
    match arg {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Decimal128(RESULT_PRECISION, 0),
        DataType::Decimal128(_, scale) if (0..=MAX_INPUT_SCALE).contains(scale) => {
            DataType::Decimal128(RESULT_PRECISION, *scale)
        }
        // `Decimal256` included: it has no PostgreSQL OID on this wire, so an exact answer
        // over it could not be sent even if it were computed. Left where it already was.
        _ => DataType::Float64,
    }
}

/// The input scale to accumulate at, or `None` when this call is DataFusion's to answer.
///
/// Read from the **return** type rather than the input's, so the accumulator and the type
/// the plan advertises cannot disagree: a decimal result is this module's promise, and it
/// is the only thing that puts the exact accumulator in play.
fn exact_scale(return_type: &DataType, input: &DataType) -> Result<Option<i8>> {
    if !matches!(return_type, DataType::Decimal128(_, _)) {
        return Ok(None);
    }
    match input {
        DataType::Decimal128(_, scale) if (0..=MAX_INPUT_SCALE).contains(scale) => Ok(Some(*scale)),
        other => internal_err!(
            "an exact statistic promised a decimal over an input of {other}, which it \
             cannot accumulate exactly"
        ),
    }
}

/// [`exact_scale`] for the accumulator's own view of a call.
fn exact_input_scale(args: &AccumulatorArgs) -> Result<Option<i8>> {
    let Some(input) = args.expr_fields.first() else {
        return internal_err!("a statistic was called without an argument");
    };
    exact_scale(args.return_type(), input.data_type())
}

/// `n`, `Σuᵢ` and `Σuᵢ²` over the unscaled integers of a `Decimal128` column.
///
/// Everything the four statistics need, and nothing that depends on which of them is being
/// computed — the choice is made once, in [`exact_statistic`], out of these three numbers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Totals {
    count: u64,
    sum: i256,
    sum_squares: i256,
}

impl Totals {
    /// Fold one value in, `None` if a total outgrew `i256`.
    fn add(&mut self, value: i128) -> Option<()> {
        let value = i256::from_i128(value);
        self.count = self.count.checked_add(1)?;
        self.sum = self.sum.checked_add(value)?;
        self.sum_squares = self.sum_squares.checked_add(value.checked_mul(value)?)?;
        Some(())
    }

    /// Take one value back out, for a sliding window frame.
    fn remove(&mut self, value: i128) -> Option<()> {
        let value = i256::from_i128(value);
        self.count = self.count.checked_sub(1)?;
        self.sum = self.sum.checked_sub(value)?;
        self.sum_squares = self.sum_squares.checked_sub(value.checked_mul(value)?)?;
        Some(())
    }

    /// Fold another shard's totals in.
    fn merge(&mut self, other: Totals) -> Option<()> {
        self.count = self.count.checked_add(other.count)?;
        self.sum = self.sum.checked_add(other.sum)?;
        self.sum_squares = self.sum_squares.checked_add(other.sum_squares)?;
        Some(())
    }
}

/// Accumulates the three totals and answers one of the four statistics exactly.
#[derive(Debug)]
struct ExactStatistics {
    kind: Statistic,
    /// The scale of the `Decimal128` input, which fixes what the totals mean.
    input_scale: i8,
    totals: Totals,
    /// The unscaled values seen, for `DISTINCT`.
    ///
    /// `Some` only for `stddev(DISTINCT x)` and friends, where the totals cannot be kept
    /// incrementally: whether a value counts depends on every value before it. The
    /// optimizer turns a lone `DISTINCT` aggregate into a group-by and this stays `None`;
    /// it is the mixed `stddev(DISTINCT n), sum(m)` that reaches here.
    distinct: Option<HashSet<i128>>,
}

impl ExactStatistics {
    fn new(kind: Statistic, input_scale: i8, is_distinct: bool) -> Self {
        Self {
            kind,
            input_scale,
            totals: Totals::default(),
            distinct: is_distinct.then(HashSet::new),
        }
    }

    /// The out-of-range refusal, tagged so it survives the trip from an executor.
    ///
    /// This runs on the node that evaluates the aggregate, and a `DataFusionError` raised
    /// there reaches the coordinator as text with its variant gone (§ 1.3). Without the tag
    /// the client would be told `XX000 internal_error` — that the server broke and the
    /// statement is worth retrying — where the truth is `22003`: the answer does not fit,
    /// and it will not fit on a retry either.
    fn out_of_range(&self) -> DataFusionError {
        DataFusionError::Execution(tagged_message(
            VdbErrorCode::NumericValueOutOfRange,
            format!(
                "the exact {} of these values does not fit numeric({RESULT_PRECISION}, \
                 {RESULT_SCALE})",
                self.kind.name()
            ),
        ))
    }

    /// The values in `array`, checked to be the decimals the accumulator was opened for.
    fn decimals<'a>(&self, array: &'a ArrayRef) -> Result<&'a Decimal128Array> {
        match array.data_type() {
            DataType::Decimal128(_, scale) if *scale == self.input_scale => {
                Ok(array.as_primitive::<Decimal128Type>())
            }
            other => internal_err!(
                "{} accumulates a Decimal128 of scale {}, not {other}",
                self.kind.name(),
                self.input_scale
            ),
        }
    }

    /// The totals to answer from, which for `DISTINCT` are folded only now.
    fn totals(&self) -> Result<Totals> {
        let Some(seen) = &self.distinct else {
            return Ok(self.totals);
        };
        let mut totals = Totals::default();
        for value in seen {
            totals.add(*value).ok_or_else(|| self.out_of_range())?;
        }
        Ok(totals)
    }
}

impl Accumulator for ExactStatistics {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return internal_err!("a statistic needs a value to accumulate");
        };
        let values = self.decimals(values)?;
        if let Some(seen) = &mut self.distinct {
            seen.extend(values.iter().flatten());
            return Ok(());
        }
        for value in values.iter().flatten() {
            if self.totals.add(value).is_none() {
                return Err(self.out_of_range());
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return internal_err!("a statistic needs a value to retract");
        };
        if self.distinct.is_some() {
            return internal_err!("{} cannot retract a DISTINCT value", self.kind.name());
        }
        let values = self.decimals(values)?;
        for value in values.iter().flatten() {
            if self.totals.remove(value).is_none() {
                return internal_err!(
                    "{} retracted a value it had not accumulated",
                    self.kind.name()
                );
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(seen) = &mut self.distinct {
            let Some(state) = states.first() else {
                return internal_err!("a DISTINCT statistic needs its own state to merge");
            };
            for partial in state.as_list::<i32>().iter().flatten() {
                match partial.data_type() {
                    DataType::Decimal128(_, _) => {
                        seen.extend(partial.as_primitive::<Decimal128Type>().iter().flatten())
                    }
                    other => {
                        return internal_err!(
                            "a DISTINCT statistic cannot merge a list of {other}"
                        );
                    }
                }
            }
            return Ok(());
        }
        let [counts, sums, sums_of_squares] = states else {
            return internal_err!(
                "{} merges three totals, got {}",
                self.kind.name(),
                states.len()
            );
        };
        let counts = counts.as_primitive::<UInt64Type>();
        let sums = sums.as_primitive::<Decimal256Type>();
        let sums_of_squares = sums_of_squares.as_primitive::<Decimal256Type>();
        for row in 0..counts.len() {
            if counts.is_null(row) {
                continue;
            }
            let partial = Totals {
                count: counts.value(row),
                sum: sums.value(row),
                sum_squares: sums_of_squares.value(row),
            };
            self.totals
                .merge(partial)
                .ok_or_else(|| self.out_of_range())?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if let Some(seen) = &self.distinct {
            let values = Decimal128Array::from_iter_values(seen.iter().copied())
                .with_precision_and_scale(RESULT_PRECISION, self.input_scale)?;
            let element = Field::new_list_field(values.data_type().clone(), true);
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
            let list = ListArray::new(Arc::new(element), offsets, Arc::new(values), None);
            return Ok(vec![ScalarValue::List(Arc::new(list))]);
        }
        // Both totals are validated against `TOTAL_PRECISION` when Arrow builds the array,
        // and its message would name a precision no client asked about; refusing here says
        // what actually happened.
        let ceiling = pow10(u32::from(TOTAL_PRECISION)).expect("10^76 fits an i256");
        for total in [self.totals.sum, self.totals.sum_squares] {
            let magnitude = total.checked_abs().ok_or_else(|| self.out_of_range())?;
            if magnitude >= ceiling {
                return Err(self.out_of_range());
            }
        }
        Ok(vec![
            ScalarValue::UInt64(Some(self.totals.count)),
            ScalarValue::Decimal256(Some(self.totals.sum), TOTAL_PRECISION, 0),
            ScalarValue::Decimal256(Some(self.totals.sum_squares), TOTAL_PRECISION, 0),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let totals = self.totals()?;
        let answer = match exact_statistic(self.kind, totals, self.input_scale) {
            Ok(answer) => answer,
            Err(OutOfRange) => return Err(self.out_of_range()),
        };
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
                .map(|seen| seen.capacity() * size_of_val(&0i128))
                .unwrap_or(0)
    }
}

/// The answer does not fit `Decimal128(38, 16)`, or a total outgrew `i256` on the way.
///
/// Kept as a unit type rather than a `DataFusionError` so the arithmetic below can be a set
/// of free functions with no opinion about how a refusal is reported.
#[derive(Debug, PartialEq, Eq)]
struct OutOfRange;

/// One of the four statistics as the unscaled integer of a `Decimal128(38, 16)`, or `None`
/// for a group PostgreSQL answers NULL for.
///
/// Every step is exact and every step is checked. `input_scale` is the scale of the column
/// the totals were read from, which is what makes `Σuᵢ` an integer at all.
fn exact_statistic(
    kind: Statistic,
    totals: Totals,
    input_scale: i8,
) -> std::result::Result<Option<i128>, OutOfRange> {
    if totals.count < kind.minimum_rows() {
        return Ok(None);
    }
    let count = i256::from_i128(i128::from(totals.count));

    // SS = n·Σuᵢ² − (Σuᵢ)², the sum of squared deviations times n. Never negative — the
    // Cauchy–Schwarz inequality is what says so — and computed this way rather than from a
    // mean so that no division happens before the end.
    let squared_sum = totals.sum.checked_mul(totals.sum).ok_or(OutOfRange)?;
    let scaled_squares = count.checked_mul(totals.sum_squares).ok_or(OutOfRange)?;
    let sum_of_squares = scaled_squares
        .checked_sub(squared_sum)
        .ok_or(OutOfRange)?
        .max(i256::ZERO);

    let denominator = match kind.is_sample() {
        true => count.checked_mul(count.checked_sub(i256::ONE).ok_or(OutOfRange)?),
        false => count.checked_mul(count),
    }
    .ok_or(OutOfRange)?;

    // The `10²ˢ` of the two formulas in the module doc, and the `10^R` the result is scaled
    // by, folded into one power on whichever side of the division needs it. Dividing by a
    // smaller number than `den · 10²ˢ` is what keeps a wide input inside `i256`.
    let two_scale = 2 * u32::try_from(input_scale).map_err(|_| OutOfRange)?;
    let target = match kind.is_deviation() {
        // A square root halves the scale, so it is taken at twice the result's.
        true => 2 * u32::from(RESULT_SCALE.unsigned_abs()),
        false => u32::from(RESULT_SCALE.unsigned_abs()),
    };
    let (power, denominator) = match target >= two_scale {
        true => (target - two_scale, denominator),
        false => (
            0,
            denominator
                .checked_mul(pow10(two_scale - target).ok_or(OutOfRange)?)
                .ok_or(OutOfRange)?,
        ),
    };

    let unscaled = match kind.is_deviation() {
        true => rounded_sqrt(sum_of_squares, denominator, power),
        false => rounded_div(sum_of_squares, denominator, power),
    }
    .ok_or(OutOfRange)?;

    // The last step, and the one that keeps a plausible wrong number off the wire: a value
    // wider than the declared precision is not reported at a lower one.
    let unscaled = unscaled.to_i128().ok_or(OutOfRange)?;
    match unscaled.checked_abs().ok_or(OutOfRange)? < pow10_i128(u32::from(RESULT_PRECISION)) {
        true => Ok(Some(unscaled)),
        false => Err(OutOfRange),
    }
}

/// `10ᵏ` as an `i256`, or `None` past its range.
fn pow10(k: u32) -> Option<i256> {
    i256::from_i128(10).checked_pow(k)
}

/// `10ᵏ` as an `i128`, for `k` small enough that it has one.
fn pow10_i128(k: u32) -> i128 {
    10i128.pow(k)
}

/// `⌊value · 10^power / den⌋` and the remainder over `den`, for `value ≥ 0` and `den > 0`.
///
/// Long division one digit at a time, rather than `value · 10^power` divided once, because
/// the product is what overflows first: a `Σuᵢ²` that fits `i256` need not still fit it
/// after ten zeros are appended, while the quotient — the answer — comfortably does.
fn scaled_div(value: i256, den: i256, power: u32) -> Option<(i256, i256)> {
    let ten = i256::from_i128(10);
    let mut quotient = value.checked_div(den)?;
    let mut remainder = value.checked_rem(den)?;
    for _ in 0..power {
        quotient = quotient.checked_mul(ten)?;
        remainder = remainder.checked_mul(ten)?;
        quotient = quotient.checked_add(remainder.checked_div(den)?)?;
        remainder = remainder.checked_rem(den)?;
    }
    Some((quotient, remainder))
}

/// `value · 10^power / den` rounded half away from zero, for `value ≥ 0` and `den > 0`.
///
/// Half **up** is PostgreSQL's own rule for a `numeric` division, and both operands are
/// non-negative here, so away from zero and up are the same thing.
fn rounded_div(value: i256, den: i256, power: u32) -> Option<i256> {
    let (quotient, remainder) = scaled_div(value, den, power)?;
    // `remainder ≥ den − remainder` rather than `2·remainder ≥ den`: the doubling is the
    // one thing here that could overflow, and the subtraction cannot, since
    // `0 ≤ remainder < den`.
    match remainder >= den.checked_sub(remainder)? {
        true => quotient.checked_add(i256::ONE),
        false => Some(quotient),
    }
}

/// `√(value · 10^power / den)` rounded half up, for `value ≥ 0` and `den > 0`.
///
/// `power` is twice the result's scale, since a square root halves it.
fn rounded_sqrt(value: i256, den: i256, power: u32) -> Option<i256> {
    let (quotient, remainder) = scaled_div(value, den, power)?;
    let root = integer_sqrt(quotient);

    // Round up when `√(quotient + remainder/den) ≥ root + ½`, which — squaring both sides
    // and multiplying by `4·den` — is `4·(den·(quotient − root² − root) + remainder) ≥ den`.
    // Written that way because `quotient − root² − root` is at most `root + 1`, so every
    // product here stays near `den·root` rather than growing to the square of the value.
    let excess = quotient
        .checked_sub(root.checked_mul(root)?)?
        .checked_sub(root)?;
    let half = den.checked_mul(excess)?.checked_add(remainder)?;
    let round_up = match half.checked_mul(i256::from_i128(4)) {
        Some(quadrupled) => quadrupled >= den,
        // Overflowing `i256` in the positive direction is unambiguously past `den`.
        None => !half.is_negative(),
    };
    match round_up {
        true => root.checked_add(i256::ONE),
        false => Some(root),
    }
}

/// `⌊√value⌋` for `value ≥ 0`.
///
/// Newton's method in integers, seeded from the bit length so the first guess is already
/// above the root — which is what makes the iteration monotonically decreasing and its own
/// termination test. The two correction loops cost nothing and mean the answer is checked
/// rather than argued for.
fn integer_sqrt(value: i256) -> i256 {
    if value <= i256::ONE {
        return value.max(i256::ZERO);
    }
    // `value` is positive, so its bit length is at most 255 and this shift is at most 128 —
    // both inside an `i256`, and `2^⌈bits/2⌉ > √value`.
    let bits = 256 - value.leading_zeros();
    let mut root = i256::ONE << u8::try_from(bits.div_ceil(2)).expect("at most 128");
    loop {
        let next = (root + value / root) / i256::from_i128(2);
        if next >= root {
            break;
        }
        root = next;
    }
    // Too high — including a `root` whose square does not fit an `i256` at all.
    while !matches!(root.checked_mul(root), Some(square) if square <= value) {
        root -= i256::ONE;
    }
    // And too low, which Newton's method does not produce but which costs one comparison
    // to rule out.
    while matches!(
        (root + i256::ONE).checked_mul(root + i256::ONE),
        Some(square) if square <= value
    ) {
        root += i256::ONE;
    }
    root
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::Schema;
    use datafusion::execution::context::SessionContext;

    /// The totals of a set of integers read as `Decimal128(38, scale)`.
    fn totals(values: &[i128]) -> Totals {
        let mut totals = Totals::default();
        for value in values {
            totals.add(*value).expect("no overflow in a test fixture");
        }
        totals
    }

    /// One statistic over integer values, rendered the way the wire renders it.
    fn statistic(kind: Statistic, values: &[i128], scale: i8) -> Option<String> {
        let unscaled = exact_statistic(kind, totals(values), scale).expect("in range")?;
        let scalar = ScalarValue::Decimal128(Some(unscaled), RESULT_PRECISION, RESULT_SCALE);
        let array = scalar.to_array().expect("one value");
        Some(arrow::util::display::array_value_to_string(&array, 0).expect("rendered"))
    }

    /// The four statistics of 10 and 30, which PostgreSQL answers 200, 100,
    /// 14.1421356237309505 and 10.
    #[test]
    fn the_four_statistics_of_two_integers() {
        assert_eq!(
            statistic(Statistic::VarSamp, &[10, 30], 0).as_deref(),
            Some("200.0000000000000000")
        );
        assert_eq!(
            statistic(Statistic::VarPop, &[10, 30], 0).as_deref(),
            Some("100.0000000000000000")
        );
        assert_eq!(
            statistic(Statistic::StddevSamp, &[10, 30], 0).as_deref(),
            Some("14.1421356237309505")
        );
        assert_eq!(
            statistic(Statistic::StddevPop, &[10, 30], 0).as_deref(),
            Some("10.0000000000000000")
        );
    }

    /// 1..10, where every one of the four is non-terminating or irrational. PostgreSQL 17
    /// answers 9.1666666666666667, 8.25, 3.0276503540974917 and 2.8722813232690143; these
    /// are those numbers digit for digit, which is the whole of the claim.
    #[test]
    fn the_four_statistics_of_one_to_ten() {
        let values: Vec<i128> = (1..=10).collect();
        assert_eq!(
            statistic(Statistic::VarSamp, &values, 0).as_deref(),
            Some("9.1666666666666667"),
            "the sixteen places PostgreSQL prints, digit for digit"
        );
        assert_eq!(
            statistic(Statistic::VarPop, &values, 0).as_deref(),
            Some("8.2500000000000000")
        );
        assert_eq!(
            statistic(Statistic::StddevSamp, &values, 0).as_deref(),
            Some("3.0276503540974917"),
            "3.0276503540974917 rounded up at the tenth place"
        );
        assert_eq!(
            statistic(Statistic::StddevPop, &values, 0).as_deref(),
            Some("2.8722813232690143"),
            "2.8722813232690143 rounded up at the tenth place"
        );
    }

    /// The defect this module exists for: over `bigint`s this size Welford's algorithm in
    /// `f64` has 53 bits of mantissa against the 38 digits of the answer. PostgreSQL's
    /// `var_pop` of these two is exactly 2.25, because they differ by 3.
    #[test]
    fn a_variance_of_two_huge_bigints_is_exact() {
        let values = [4611686018427387904i128, 4611686018427387907];
        assert_eq!(
            statistic(Statistic::VarPop, &values, 0).as_deref(),
            Some("2.2500000000000000")
        );
        assert_eq!(
            statistic(Statistic::VarSamp, &values, 0).as_deref(),
            Some("4.5000000000000000")
        );
        assert_eq!(
            statistic(Statistic::StddevPop, &values, 0).as_deref(),
            Some("1.5000000000000000")
        );
    }

    /// A scaled input: the values are `1.5` and `2.5` as `Decimal128(38, 1)`, whose sample
    /// variance is exactly `0.5`.
    #[test]
    fn a_decimal_input_is_read_at_its_own_scale() {
        assert_eq!(
            statistic(Statistic::VarSamp, &[15, 25], 1).as_deref(),
            Some("0.5000000000000000")
        );
        assert_eq!(
            statistic(Statistic::StddevSamp, &[15, 25], 1).as_deref(),
            Some("0.7071067811865475"),
            "√0.5 = 0.70710678118654752, rounded down at the sixteenth place"
        );
    }

    /// An input scale past the sixteen places of the result truncates rather than failing —
    /// the same narrowing `avg` has, and the reason it is recorded in the gap analysis.
    #[test]
    fn an_input_finer_than_the_result_scale_rounds_to_it() {
        // 0.000000000001 and 0.000000000003 at scale 12: the population variance is
        // 1e-24, which sixteen decimal places cannot show at all.
        assert_eq!(
            statistic(Statistic::VarPop, &[1, 3], 12).as_deref(),
            Some("0.0000000000000000")
        );
    }

    /// PostgreSQL's rule for a group too small to have the statistic: a sample one is NULL
    /// over a single row where a population one is zero, and both are NULL over none.
    #[test]
    fn a_group_too_small_for_the_statistic() {
        for kind in Statistic::ALL {
            assert_eq!(statistic(kind, &[], 0), None, "{} of no rows", kind.name());
        }
        assert_eq!(statistic(Statistic::VarSamp, &[7], 0), None);
        assert_eq!(statistic(Statistic::StddevSamp, &[7], 0), None);
        assert_eq!(
            statistic(Statistic::VarPop, &[7], 0).as_deref(),
            Some("0.0000000000000000")
        );
        assert_eq!(
            statistic(Statistic::StddevPop, &[7], 0).as_deref(),
            Some("0.0000000000000000")
        );
    }

    /// Identical values have no spread at all, which is the case an inexact algorithm is
    /// most likely to report as a small negative number and then fail to take a root of.
    #[test]
    fn identical_values_have_no_spread() {
        let values = [i128::from(i64::MAX); 5];
        for kind in Statistic::ALL {
            assert_eq!(
                statistic(kind, &values, 0).as_deref(),
                Some("0.0000000000000000"),
                "{} of five identical values",
                kind.name()
            );
        }
    }

    /// The ceiling, refused rather than rounded. The population variance of ±10²⁰ is 10⁴⁰,
    /// which does not fit 22 digits before the point — and neither does 10²², which is where
    /// the sixteen decimal places put the edge exactly.
    #[test]
    fn a_variance_past_the_declared_precision_is_refused() {
        let huge = pow10_i128(20);
        assert_eq!(
            exact_statistic(Statistic::VarPop, totals(&[huge, -huge]), 0),
            Err(OutOfRange)
        );
        // Either side of the edge: ±10¹¹ has a population variance of 10²², refused, while
        // the row below it is answered. This is the six digits the last six places cost.
        let edge = pow10_i128(11);
        assert_eq!(
            exact_statistic(Statistic::VarPop, totals(&[edge, -edge]), 0),
            Err(OutOfRange)
        );
        assert_eq!(
            statistic(Statistic::VarPop, &[edge / 10, -edge / 10], 0).as_deref(),
            Some("100000000000000000000.0000000000000000")
        );
        // Its square root is 10²⁰, which does fit — a standard deviation reaches further
        // than the variance it is the root of, and is not refused for the variance's sake.
        assert_eq!(
            statistic(Statistic::StddevPop, &[huge, -huge], 0).as_deref(),
            Some("100000000000000000000.0000000000000000")
        );
    }

    /// The totals are exact over a great many rows, which is the property that makes the
    /// distributed merge the same answer as a single pass.
    #[test]
    fn merging_partial_totals_is_one_pass() {
        let all: Vec<i128> = (1..=100).collect();
        let mut merged = Totals::default();
        for shard in all.chunks(7) {
            merged.merge(totals(shard)).expect("no overflow");
        }
        assert_eq!(merged, totals(&all));
        assert_eq!(
            exact_statistic(Statistic::VarSamp, merged, 0),
            exact_statistic(Statistic::VarSamp, totals(&all), 0)
        );
    }

    /// Retracting is what a sliding window frame needs, and it has to leave the totals
    /// where they would have been had the value never arrived.
    #[test]
    fn retracting_a_value_undoes_it() {
        let mut running = totals(&[1, 2, 3, 4]);
        running.remove(4).expect("accumulated");
        assert_eq!(running, totals(&[1, 2, 3]));
    }

    #[test]
    fn the_integer_square_root_is_the_floor_of_the_root() {
        for value in [0i128, 1, 2, 3, 4, 8, 9, 10, 15, 16, 17, 99, 100, 101] {
            let root = integer_sqrt(i256::from_i128(value));
            let root = root.to_i128().expect("small");
            assert!(
                root * root <= value && (root + 1) * (root + 1) > value,
                "√{value} is not {root}"
            );
        }
        // The widest square an `i256` holds, and its neighbours.
        let big = i256::from_i128(i128::MAX);
        let square = big.checked_mul(big).expect("fits");
        assert_eq!(integer_sqrt(square), big);
        assert_eq!(
            integer_sqrt(square.checked_sub(i256::ONE).unwrap()),
            big - i256::ONE
        );
        assert!(
            integer_sqrt(i256::MAX)
                .checked_mul(integer_sqrt(i256::MAX))
                .is_some()
        );
    }

    /// The long division is the one that must not lose a digit to an intermediate product:
    /// `value` here already has 70 digits, so `value · 10¹⁰` would not fit an `i256`.
    #[test]
    fn the_long_division_outlives_the_product_it_avoids() {
        let value = pow10(70).unwrap();
        assert!(
            value.checked_mul(pow10(10).unwrap()).is_none(),
            "the fixture is only interesting if the product overflows"
        );
        // 10^70 / 10^60, scaled by 10^10, is 10^20.
        assert_eq!(
            rounded_div(value, pow10(60).unwrap(), 10),
            Some(pow10(20).unwrap())
        );
    }

    /// Half rounds up, in the direction PostgreSQL's `numeric` division rounds.
    #[test]
    fn a_half_rounds_up() {
        let three = i256::from_i128(3);
        let two = i256::from_i128(2);
        assert_eq!(rounded_div(three, two, 0), Some(two), "1.5 → 2");
        assert_eq!(
            rounded_div(i256::from_i128(5), two, 0),
            Some(three),
            "2.5 → 3"
        );
        assert_eq!(
            rounded_div(i256::ONE, three, 0),
            Some(i256::ZERO),
            "0.33 → 0"
        );
    }

    /// Which types take the exact path, and which stay DataFusion's. The pair *is* the
    /// PostgreSQL rule: exact in, exact out.
    #[test]
    fn the_types_postgresql_answers_in_numeric_take_the_exact_path() {
        for exact in [
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Decimal128(20, 4),
        ] {
            assert!(
                matches!(coercion_target(&exact), DataType::Decimal128(_, _)),
                "{exact} is exact to PostgreSQL"
            );
        }
        for float in [
            DataType::Float32,
            DataType::Float64,
            DataType::Null,
            DataType::Utf8,
            DataType::Decimal256(50, 2),
            DataType::Decimal128(20, -2),
        ] {
            assert_eq!(
                coercion_target(&float),
                DataType::Float64,
                "{float} is DataFusion's to answer"
            );
        }
        // The scale of a decimal input is kept, or the cast that gets it there would lose
        // the digits the exactness is about.
        assert_eq!(
            coercion_target(&DataType::Decimal128(20, 4)),
            DataType::Decimal128(38, 4)
        );
    }

    /// The names and aliases have to be DataFusion's exactly: `register_udaf` inserts under
    /// each of them, so one left out goes on resolving to the float version and a client
    /// gets a different type for `var_samp(n)` than for `var(n)`.
    #[test]
    fn every_spelling_datafusion_answers_to_is_replaced() {
        let mut ctx = SessionContext::new();
        register_statistics_aggregates(&mut ctx).expect("registration failed");
        for name in [
            "var",
            "var_samp",
            "var_sample",
            "var_pop",
            "var_population",
            "stddev",
            "stddev_samp",
            "stddev_pop",
        ] {
            let udaf = ctx.udaf(name).unwrap_or_else(|_| panic!("{name} resolves"));
            assert!(
                udaf.inner().is::<ExactStatistic>(),
                "{name} still resolves to DataFusion's"
            );
        }
    }

    /// Registering twice is what a context reached by two registration paths does.
    #[test]
    fn registering_twice_is_idempotent() {
        let mut ctx = SessionContext::new();
        register_statistics_aggregates(&mut ctx).expect("first registration failed");
        register_statistics_aggregates(&mut ctx).expect("second registration failed");
        assert!(
            ctx.udaf("var_samp")
                .expect("ours")
                .inner()
                .is::<ExactStatistic>()
        );
    }

    /// A table of two `bigint`s whose exact variance no `f64` holds, and the same values as
    /// `integer` and `double precision`.
    fn ctx() -> SessionContext {
        let mut ctx = SessionContext::new();
        register_statistics_aggregates(&mut ctx).expect("registration failed");
        let schema = Arc::new(Schema::new(vec![
            Field::new("big", DataType::Int64, false),
            Field::new("small", DataType::Int32, false),
            Field::new("f", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    4611686018427387904,
                    4611686018427387907,
                ])),
                Arc::new(Int32Array::from(vec![10, 30])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 30.0])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    /// The type the plan advertises and the value it produces, for one query.
    ///
    /// The plan's schema is checked against the batch's, because the two disagreeing is
    /// the failure this whole module has to avoid: VaireDB puts the plan's type on the wire
    /// as the column's OID and casts the batch into it.
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

    /// The gap, end to end through the planner: `numeric` over an integer column.
    #[tokio::test]
    async fn an_integer_statistic_is_planned_as_numeric() {
        for function in [
            "var",
            "var_samp",
            "var_sample",
            "var_pop",
            "var_population",
            "stddev",
            "stddev_samp",
            "stddev_pop",
        ] {
            let (dt, _) = one_row(&format!("SELECT {function}(small) FROM t")).await;
            assert_eq!(
                dt,
                DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE),
                "{function} over an integer column is numeric to PostgreSQL"
            );
        }
    }

    /// And the values, over a column no `f64` can hold the variance of.
    #[tokio::test]
    async fn a_bigint_variance_past_the_float_mantissa_is_exact() {
        assert_eq!(
            one_row("SELECT var_pop(big) FROM t").await.1,
            "2.2500000000000000"
        );
        assert_eq!(
            one_row("SELECT stddev_pop(big) FROM t").await.1,
            "1.5000000000000000"
        );
    }

    /// A float column stays `double precision`, with the value DataFusion always gave it.
    /// Widening it would be this module disagreeing with PostgreSQL in the other direction.
    #[tokio::test]
    async fn a_float_statistic_stays_double_precision() {
        for function in ["var_samp", "var_pop", "stddev", "stddev_pop"] {
            let (dt, _) = one_row(&format!("SELECT {function}(f) FROM t")).await;
            assert_eq!(dt, DataType::Float64, "{function} over float8");
        }
        assert_eq!(one_row("SELECT var_samp(f) FROM t").await.1, "200.0");
    }

    /// The window spelling reaches the same accumulator through a different plan node, and
    /// has to report the type the grouped one does — or `stddev(n)` and `stddev(n) OVER ()`
    /// disagree about the same number.
    #[tokio::test]
    async fn the_window_spelling_reports_the_grouped_type() {
        let (dt, text) = one_row("SELECT stddev(small) OVER () FROM t LIMIT 1").await;
        assert_eq!(dt, DataType::Decimal128(RESULT_PRECISION, RESULT_SCALE));
        assert_eq!(text, "14.1421356237309505");
    }

    /// `FILTER (WHERE …)` narrows the group before the accumulator sees it, and a group of
    /// one row has no sample variance.
    #[tokio::test]
    async fn a_filtered_aggregate_narrows_the_group() {
        let (_, text) = one_row("SELECT var_pop(small) FILTER (WHERE small > 20) FROM t").await;
        assert_eq!(text, "0.0000000000000000", "one row has no spread");
        let (_, text) = one_row("SELECT var_samp(small) FILTER (WHERE small > 20) FROM t").await;
        assert_eq!(text, "", "and no sample variance at all — a NULL");
    }

    /// `DISTINCT`, in both shapes it reaches an accumulator: alone, where the optimizer
    /// turns it into a group-by, and beside another aggregate, where it does not.
    #[tokio::test]
    async fn a_distinct_statistic_counts_each_value_once() {
        let ctx = ctx();
        // 10, 10, 30 — the distinct values are 10 and 30, whose sample variance is 200.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int32, false),
                Field::new("m", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![10, 10, 30])),
                Arc::new(Int32Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        ctx.register_batch("d", batch).unwrap();

        let alone = ctx
            .sql("SELECT var_samp(DISTINCT n) FROM d")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            arrow::util::display::array_value_to_string(alone[0].column(0), 0).unwrap(),
            "200.0000000000000000"
        );

        // Beside a second aggregate, which is what stops the optimizer rewriting it into a
        // group-by and sends `is_distinct` all the way to the accumulator.
        let mixed = ctx
            .sql("SELECT var_samp(DISTINCT n), sum(m) FROM d")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            arrow::util::display::array_value_to_string(mixed[0].column(0), 0).unwrap(),
            "200.0000000000000000"
        );
    }

    /// Grouped, which is the shape that goes through a partial and a final aggregate and
    /// therefore through the three-total state.
    #[tokio::test]
    async fn a_grouped_statistic_merges_its_partial_totals() {
        let ctx = ctx();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int32, false),
                Field::new("n", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int32Array::from(vec![10, 30, 1, 2])),
            ],
        )
        .unwrap();
        ctx.register_batch("g", batch).unwrap();
        let rows = ctx
            .sql("SELECT g, var_samp(n) FROM g GROUP BY g ORDER BY g")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let variance =
            |row| arrow::util::display::array_value_to_string(rows[0].column(1), row).unwrap();
        assert_eq!(variance(0), "200.0000000000000000");
        assert_eq!(variance(1), "0.5000000000000000");
    }
}
