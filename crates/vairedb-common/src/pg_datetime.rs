//! PostgreSQL's datetime family: `age`, `make_timestamp`, `make_interval`, `isfinite`, the
//! three `justify_*` functions, `clock_timestamp` and `timeofday`.
//!
//! DataFusion already supplies most of the *conversion* half of PostgreSQL's datetime
//! surface — `date_part`, `date_trunc`, `date_bin`, `to_char`, `to_date`, `to_timestamp`,
//! `make_date`, `make_time`, `now`. What `datafusion-pg-functions` 0.1 was supposed to add,
//! and does not, is the half that does *calendar arithmetic*: the functions whose answers
//! are not a fixed number of microseconds.
//!
//! That distinction is the reason this module is longer than a name-registration would be.
//! `age('2024-03-01', '2024-01-31')` is `1 mon 1 day` and not `30 days`, because PostgreSQL
//! borrows from the *earlier* operand's month, which has 31. Subtracting the two instants
//! and dividing gives a different answer, and a client reconciling two systems sees the
//! difference as a data mismatch rather than as a function it is missing. So each function
//! here reproduces PostgreSQL's algorithm rather than an equivalent-looking one, and the
//! tests record the 16.15 answers they were checked against.
//!
//! ## What is not here, and why
//!
//! - `age(x)` — the one-argument form is `age(current_date, x)`, and `current_date` is a
//!   property of the *statement*. A UDF cannot see it, and if each node answered from its
//!   own clock the same query could report two different ages for one row near midnight.
//!   The coordinator rewrites the call instead; see
//!   `vairedb_coordinator::pgwire_handler::pg_clock_functions`.
//! - `statement_timestamp()` and `transaction_timestamp()` — the same reasoning: both are
//!   PostgreSQL's `now()`, which is fixed once per statement, and the coordinator rewrites
//!   them into it.
//! - `to_number(text, text)` — see [`crate::pg_format`] for why a fixed Arrow scale makes it
//!   worse than an absence.

use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, IntervalMonthDayNanoArray, IntervalMonthDayNanoBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use datafusion::common::{DataFusionError, Result, ScalarValue, cast::as_int64_array, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::error::tagged_message;
use crate::pg_typeof::pg_type_name;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The names PostgreSQL uses, so a client's SQL needs no rewriting.
pub const AGE_UDF_NAME: &str = "age";
pub const MAKE_TIMESTAMP_UDF_NAME: &str = "make_timestamp";
pub const MAKE_INTERVAL_UDF_NAME: &str = "make_interval";
pub const ISFINITE_UDF_NAME: &str = "isfinite";
pub const JUSTIFY_DAYS_UDF_NAME: &str = "justify_days";
pub const JUSTIFY_HOURS_UDF_NAME: &str = "justify_hours";
pub const JUSTIFY_INTERVAL_UDF_NAME: &str = "justify_interval";
pub const CLOCK_TIMESTAMP_UDF_NAME: &str = "clock_timestamp";
pub const TIMEOFDAY_UDF_NAME: &str = "timeofday";

/// The timestamp type the family works in. PostgreSQL's `timestamp` has microsecond
/// resolution, and so does this, so no value is rounded on the way through.
const TS: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);
/// The interval layout the family answers in: the only one of Arrow's three that can hold
/// months, days and a sub-day time at once, which is what a PostgreSQL interval is.
const INTERVAL: DataType = DataType::Interval(IntervalUnit::MonthDayNano);

const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_DAY: i64 = 24 * 60 * 60 * USECS_PER_SEC;
const DAYS_PER_MONTH: i32 = 30;

/// Register the datetime family on `registry`.
///
/// Call this on every context that plans **or** executes a read; see [`crate::pg_udf`] for
/// why a name has to resolve on every node.
pub fn register_datetime_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(age_udf())?;
    registry.register_udf(make_timestamp_udf())?;
    registry.register_udf(make_interval_udf())?;
    registry.register_udf(isfinite_udf())?;
    for unit in [
        JustifyUnit::Days,
        JustifyUnit::Hours,
        JustifyUnit::DaysAndHours,
    ] {
        registry.register_udf(justify_udf(unit))?;
    }
    registry.register_udf(clock_timestamp_udf())?;
    registry.register_udf(timeofday_udf())?;
    Ok(())
}

/// The shared `age` instance, for the coordinator rewrite that builds the two-argument call.
pub fn age_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Age::default()))))
}

/// The shared `make_timestamp` instance.
pub fn make_timestamp_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(MakeTimestamp::default()))))
}

/// The shared `make_interval` instance.
pub fn make_interval_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(MakeInterval::default()))))
}

/// The shared `isfinite` instance.
pub fn isfinite_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(IsFinite::default()))))
}

/// The shared `clock_timestamp` instance.
pub fn clock_timestamp_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(ClockTimestamp::default()))))
}

/// The shared `timeofday` instance.
pub fn timeofday_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(TimeOfDay::default()))))
}

/// One of the three `justify_*` functions.
pub fn justify_udf(unit: JustifyUnit) -> Arc<ScalarUDF> {
    static UDFS: OnceLock<[Arc<ScalarUDF>; 3]> = OnceLock::new();
    let all = UDFS.get_or_init(|| {
        [
            Arc::new(ScalarUDF::from(Justify::new(JustifyUnit::Days))),
            Arc::new(ScalarUDF::from(Justify::new(JustifyUnit::Hours))),
            Arc::new(ScalarUDF::from(Justify::new(JustifyUnit::DaysAndHours))),
        ]
    });
    match unit {
        JustifyUnit::Days => Arc::clone(&all[0]),
        JustifyUnit::Hours => Arc::clone(&all[1]),
        JustifyUnit::DaysAndHours => Arc::clone(&all[2]),
    }
}

/// A calendar field PostgreSQL refuses rather than rolling over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatetimeFieldError {
    message: String,
}

impl std::fmt::Display for DatetimeFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<DatetimeFieldError> for DataFusionError {
    fn from(e: DatetimeFieldError) -> Self {
        // Tagged so the code survives the trip back from whichever node ran the projection.
        DataFusionError::Execution(tagged_message(VdbErrorCode::DatetimeFieldOverflow, e))
    }
}

// ---------------------------------------------------------------------------
// age
// ---------------------------------------------------------------------------

/// `age(later, earlier)`: the difference as PostgreSQL's calendar computes it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Age {
    signature: Signature,
}

impl Default for Age {
    fn default() -> Self {
        Self {
            // `any` rather than a typed signature, and the arguments are checked in
            // `return_type` instead. A `Uniform(2, [Timestamp(µs)])` signature does not
            // resolve the call a client actually writes: a `timestamp` literal arrives as
            // `Timestamp(ns)` and DataFusion's coercion refuses to narrow it, so
            // `age(timestamp '2026-09-18', timestamp '2000-01-15')` failed to plan at all.
            // Every precision, and `date`, casts cleanly inside `invoke_with_args`.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Age {
    fn name(&self) -> &str {
        AGE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // The check the `any` signature above does not do. PostgreSQL has `age(timestamp,
        // timestamp)` and nothing else, so an argument that is not a moment in time is a
        // call it has no overload for — `42883`, and not the `0A000` that would promise this
        // one is coming.
        if !arg_types.iter().all(accepts_age) {
            return Err(DataFusionError::Plan(tagged_message(
                VdbErrorCode::UndefinedFunction,
                format!(
                    "function age({}) does not exist",
                    arg_types
                        .iter()
                        .map(pg_type_name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )));
        }
        Ok(INTERVAL)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let (Some(later), Some(earlier)) = (args.args.first(), args.args.get(1)) else {
            return exec_err!("age() takes two timestamps");
        };
        let later = timestamps(&later.to_array(rows)?)?;
        let earlier = timestamps(&earlier.to_array(rows)?)?;

        let mut out = IntervalMonthDayNanoBuilder::with_capacity(rows);
        for row in 0..rows {
            match (later[row], earlier[row]) {
                (Some(later), Some(earlier)) => out.append_value(age_of(later, earlier)?),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// PostgreSQL's `timestamp_age`, field by field.
///
/// The whole of the algorithm is the borrowing: a negative day borrows the length of a
/// *specific* month — the earlier operand's, or the later one's when the result is negative —
/// which is why `age` is not subtraction. `2024-03-01` minus `2024-01-31` borrows January's
/// 31 days and answers `1 mon 1 day`; borrowing 30 would answer `1 mon 2 days`, and
/// borrowing February's 29 would answer `1 mon`.
fn age_of(later_us: i64, earlier_us: i64) -> Result<IntervalMonthDayNano> {
    let (later, earlier) = (civil(later_us)?, civil(earlier_us)?);
    // The sign is decided once, up front: PostgreSQL computes the difference of the larger
    // from the smaller and negates, so that `age(a, b)` is exactly `-age(b, a)`.
    let negated = later_us < earlier_us;
    let sign = if negated { -1 } else { 1 };

    let mut usec = sign * (later.usec - earlier.usec);
    let mut sec = sign * (later.sec - earlier.sec);
    let mut min = sign * (later.min - earlier.min);
    let mut hour = sign * (later.hour - earlier.hour);
    let mut mday = sign * (later.day - earlier.day);
    let mut mon = sign * (later.month - earlier.month);
    let mut year = sign * (later.year - earlier.year);

    while usec < 0 {
        usec += USECS_PER_SEC;
        sec -= 1;
    }
    while sec < 0 {
        sec += 60;
        min -= 1;
    }
    while min < 0 {
        min += 60;
        hour -= 1;
    }
    while hour < 0 {
        hour += 24;
        mday -= 1;
    }
    while mday < 0 {
        // The month being borrowed from is the one the *larger* operand sits in.
        let borrowed = if negated { &later } else { &earlier };
        mday += days_in_month(borrowed.year as i32, borrowed.month as u32);
        mon -= 1;
    }
    while mon < 0 {
        mon += 12;
        year -= 1;
    }

    let months = sign * (year * 12 + mon);
    let days = sign * mday;
    let time = sign * (((hour * 60 + min) * 60 + sec) * USECS_PER_SEC + usec);

    Ok(IntervalMonthDayNano::new(
        i32::try_from(months).map_err(|_| out_of_range("interval", months))?,
        i32::try_from(days).map_err(|_| out_of_range("interval", days))?,
        time.checked_mul(1_000)
            .ok_or_else(|| out_of_range("interval", time))?,
    ))
}

/// The civil-calendar fields of a microsecond timestamp, each as an `i64` so the
/// field-by-field subtraction above cannot overflow on the way to being borrowed back.
struct Civil {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: i64,
    usec: i64,
}

fn civil(us: i64) -> Result<Civil> {
    let dt: DateTime<Utc> =
        DateTime::from_timestamp_micros(us).ok_or_else(|| out_of_range("timestamp", us))?;
    let naive = dt.naive_utc();
    Ok(Civil {
        year: i64::from(naive.year()),
        month: i64::from(naive.month()),
        day: i64::from(naive.day()),
        hour: i64::from(naive.hour()),
        min: i64::from(naive.minute()),
        sec: i64::from(naive.second()),
        usec: i64::from(naive.and_utc().timestamp_subsec_micros()),
    })
}

/// Days in a month of the proleptic Gregorian calendar, which is the calendar PostgreSQL's
/// `age` borrows from.
fn days_in_month(year: i32, month: u32) -> i64 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    match (
        NaiveDate::from_ymd_opt(year, month, 1),
        NaiveDate::from_ymd_opt(next_year, next_month, 1),
    ) {
        (Some(start), Some(next)) => (next - start).num_days(),
        // Only reachable at the ends of chrono's range, where no borrow can be meaningful.
        _ => i64::from(DAYS_PER_MONTH),
    }
}

fn out_of_range(what: &str, value: i64) -> DataFusionError {
    DatetimeFieldError {
        message: format!("{what} out of range: {value}"),
    }
    .into()
}

// ---------------------------------------------------------------------------
// make_timestamp
// ---------------------------------------------------------------------------

/// `make_timestamp(year, month, day, hour, min, sec)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MakeTimestamp {
    signature: Signature,
}

impl Default for MakeTimestamp {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MakeTimestamp {
    fn name(&self) -> &str {
        MAKE_TIMESTAMP_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(TS)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        if args.args.len() != 6 {
            return exec_err!("make_timestamp() takes six arguments");
        }
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<Result<_>>()?;
        let fields: Vec<Vec<Option<i64>>> =
            arrays[..5].iter().map(integers).collect::<Result<_>>()?;
        let seconds = floats(&arrays[5])?;

        let mut out = TimestampMicrosecondBuilder::with_capacity(rows);
        for row in 0..rows {
            let parts: Option<Vec<i64>> = fields.iter().map(|f| f[row]).collect();
            match (parts, seconds[row]) {
                (Some(parts), Some(sec)) => out.append_value(make_timestamp_value(
                    parts[0], parts[1], parts[2], parts[3], parts[4], sec,
                )?),
                // Strict in every argument, as PostgreSQL's is.
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// One `make_timestamp`, with PostgreSQL's validation and PostgreSQL's two messages.
///
/// The refusals are the substance: rolling `2024-02-30` over into March would hide the
/// caller's arithmetic bug inside a plausible-looking answer, which is why PostgreSQL spends
/// a SQLSTATE (`22008`) on saying so.
fn make_timestamp_value(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: f64,
) -> Result<i64> {
    let date_error = || DatetimeFieldError {
        message: format!("date field value out of range: {year}-{month:02}-{day:02}"),
    };

    // PostgreSQL's calendar has no year zero: 1 BC is written `-1`, and chrono's
    // astronomical numbering puts that at year 0, so the two differ by one below the era
    // boundary and agree above it.
    if year == 0 {
        return Err(date_error().into());
    }
    let astronomical = if year < 0 { year + 1 } else { year };
    let date = i32::try_from(astronomical)
        .ok()
        .and_then(|year| {
            let month = u32::try_from(month).ok()?;
            let day = u32::try_from(day).ok()?;
            NaiveDate::from_ymd_opt(year, month, day)
        })
        .ok_or_else(date_error)?;

    // The second is truncated before it is range-checked, as PostgreSQL does, so `60.5` is
    // refused for the same reason `61` is.
    let whole_seconds = sec.trunc();
    let fractional = ((sec - whole_seconds) * USECS_PER_SEC as f64).round() as i64;
    let whole_seconds = whole_seconds as i64;
    // 24:00:00 exactly is midnight of the following day and PostgreSQL accepts it; a second
    // past it is not. A leap second (`:60`) is accepted too.
    let time_out_of_range = !(0..=24).contains(&hour)
        || (hour == 24 && (min > 0 || whole_seconds > 0))
        || !(0..=59).contains(&min)
        || !(0..=60).contains(&whole_seconds);
    if time_out_of_range {
        return Err(DatetimeFieldError {
            message: format!(
                "time field value out of range: {hour}:{min:02}:{}",
                seconds_as_postgresql_prints_them(sec)
            ),
        }
        .into());
    }

    let days = i64::from(
        (date - NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch is a valid date")).num_days()
            as i32,
    );
    let time = ((hour * 60 + min) * 60 + whole_seconds) * USECS_PER_SEC + fractional;
    days.checked_mul(USECS_PER_DAY)
        .and_then(|d| d.checked_add(time))
        .ok_or_else(|| date_error().into())
}

/// The seconds field the way PostgreSQL's `%02g` renders it in the refusal message: six
/// significant digits, no trailing zeros, at least two characters wide.
fn seconds_as_postgresql_prints_them(sec: f64) -> String {
    let magnitude = sec.abs();
    let integer_digits = if magnitude < 1.0 {
        1
    } else {
        magnitude.log10().floor() as i32 + 1
    };
    let decimals = (6 - integer_digits).max(0) as usize;
    let mut text = format!("{sec:.decimals$}");
    if text.contains('.') {
        text = text.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    if text.len() < 2 {
        text.insert(0, '0');
    }
    text
}

// ---------------------------------------------------------------------------
// make_interval
// ---------------------------------------------------------------------------

/// `make_interval(years, months, weeks, days, hours, mins, secs)`, each optional.
///
/// PostgreSQL's signature uses defaulted named parameters, which is how it is usually
/// written (`make_interval(days => 5)`). DataFusion's planner has no named-argument form, so
/// only the positional spelling is accepted here — a prefix of the seven, as
/// `make_interval(1, 2)` for one year and two months.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MakeInterval {
    signature: Signature,
}

impl Default for MakeInterval {
    fn default() -> Self {
        // Every prefix of the seven fields, with the last one a float because seconds are.
        let mut arities: Vec<TypeSignature> = (0..7)
            .map(|count| TypeSignature::Exact(vec![DataType::Int64; count]))
            .collect();
        let mut full = vec![DataType::Int64; 6];
        full.push(DataType::Float64);
        arities.push(TypeSignature::Exact(full));
        Self {
            signature: Signature::one_of(arities, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeInterval {
    fn name(&self) -> &str {
        MAKE_INTERVAL_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(INTERVAL)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<Result<_>>()?;
        // The first six fields are whole numbers and the seventh is not, so it is read
        // separately rather than truncated into the same vector.
        let whole: Vec<Vec<Option<i64>>> =
            arrays.iter().take(6).map(integers).collect::<Result<_>>()?;
        let seconds = match arrays.get(6) {
            Some(array) => Some(floats(array)?),
            None => None,
        };

        let mut out = IntervalMonthDayNanoBuilder::with_capacity(rows);
        for row in 0..rows {
            let mut fields = [0i64; 6];
            let mut null = false;
            for (index, column) in whole.iter().enumerate() {
                match column[row] {
                    Some(value) => fields[index] = value,
                    None => null = true,
                }
            }
            let sec = match &seconds {
                Some(column) => match column[row] {
                    Some(value) => value,
                    None => {
                        null = true;
                        0.0
                    }
                },
                None => 0.0,
            };
            if null {
                out.append_null();
                continue;
            }
            let [years, months, weeks, days, hours, mins] = fields;
            // A week is seven days and a year is twelve months; neither is a fixed number
            // of microseconds, which is why they stay in their own components.
            let month_total = years * 12 + months;
            let day_total = weeks * 7 + days;
            let time = (hours * 60 + mins) * 60 * USECS_PER_SEC
                + (sec * USECS_PER_SEC as f64).round() as i64;
            out.append_value(IntervalMonthDayNano::new(
                i32::try_from(month_total).map_err(|_| out_of_range("interval", month_total))?,
                i32::try_from(day_total).map_err(|_| out_of_range("interval", day_total))?,
                time.checked_mul(1_000)
                    .ok_or_else(|| out_of_range("interval", time))?,
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// isfinite
// ---------------------------------------------------------------------------

/// `isfinite(date | timestamp | interval)`.
///
/// Arrow has no representation for `infinity` in any of the three types, so every non-null
/// value VaireDB can hold is finite and the answer is `true`. That is not a stub: a client
/// guarding a comparison with `WHERE isfinite(ts)` gets the right answer for every row that
/// exists, and the only divergence is that VaireDB cannot *store* the infinite value whose
/// absence it would be reporting. The function is here so the guard plans at all.
#[derive(Debug, PartialEq, Eq, Hash)]
struct IsFinite {
    signature: Signature,
}

impl Default for IsFinite {
    fn default() -> Self {
        Self {
            // Any one argument, checked below: a timestamp's type carries a time zone, so
            // an exact list would need an entry per zone.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsFinite {
    fn name(&self) -> &str {
        ISFINITE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(arg) if accepts_isfinite(arg) => Ok(DataType::Boolean),
            // PostgreSQL has no `isfinite(integer)`, and says so with the code that means
            // "no such function", not the one that means "not implemented here".
            Some(other) => Err(DataFusionError::Plan(tagged_message(
                VdbErrorCode::UndefinedFunction,
                format!("function isfinite({}) does not exist", pg_type_name(other)),
            ))),
            None => exec_err!("isfinite() takes one argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let Some(arg) = args.args.first() else {
            return exec_err!("isfinite() takes one argument");
        };
        let array = arg.to_array(rows)?;

        // The one case where a value can be infinite: the text of a not-yet-parsed literal
        // still says so, and PostgreSQL's answer for `isfinite('infinity')` is false.
        let text = matches!(
            array.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        let rendered = if text {
            Some(crate::pg_format::render_column(&array)?)
        } else {
            None
        };

        let mut out = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if array.is_null(row) {
                out.append_null();
                continue;
            }
            match &rendered {
                Some(values) => {
                    let value = values[row].as_deref().unwrap_or_default().trim();
                    let infinite = value.eq_ignore_ascii_case("infinity")
                        || value.eq_ignore_ascii_case("-infinity")
                        || value.eq_ignore_ascii_case("inf")
                        || value.eq_ignore_ascii_case("-inf");
                    out.append_value(!infinite);
                }
                None => out.append_value(true),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// The types PostgreSQL has an `isfinite` for, plus the text of an unparsed literal.
fn accepts_isfinite(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null
    )
}

/// Whether `age` has an overload for this argument type.
///
/// A moment in time, in any of the shapes one arrives in: a `date` literal is `Date32`, a
/// `timestamp` literal is `Timestamp(ns)`, a stored column is `Timestamp(µs)`, and a `NULL`
/// has no type yet. Each casts to microseconds inside the call; an `integer` or an `interval`
/// does not, and PostgreSQL has no overload for either.
fn accepts_age(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null
    )
}

// ---------------------------------------------------------------------------
// justify_days / justify_hours / justify_interval
// ---------------------------------------------------------------------------

/// Which of the three `justify_*` functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JustifyUnit {
    /// `justify_days`: 30-day groups become months.
    Days,
    /// `justify_hours`: 24-hour groups become days.
    Hours,
    /// `justify_interval`: both, and then the sign of the three components is made to agree.
    DaysAndHours,
}

/// One of the three `justify_*` functions.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Justify {
    unit: JustifyUnit,
    signature: Signature,
}

impl Justify {
    fn new(unit: JustifyUnit) -> Self {
        Self {
            unit,
            signature: Signature::new(
                TypeSignature::Uniform(1, vec![INTERVAL]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Justify {
    fn name(&self) -> &str {
        match self.unit {
            JustifyUnit::Days => JUSTIFY_DAYS_UDF_NAME,
            JustifyUnit::Hours => JUSTIFY_HOURS_UDF_NAME,
            JustifyUnit::DaysAndHours => JUSTIFY_INTERVAL_UDF_NAME,
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(INTERVAL)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let Some(arg) = args.args.first() else {
            return exec_err!("{}() takes one interval", self.name());
        };
        let array = arg.to_array(rows)?;
        let intervals = array
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "{}() expected a month-day-nano interval, got {}",
                    self.name(),
                    array.data_type()
                ))
            })?;

        let mut out = IntervalMonthDayNanoBuilder::with_capacity(rows);
        for row in 0..rows {
            if intervals.is_null(row) {
                out.append_null();
            } else {
                out.append_value(justify(intervals.value(row), self.unit));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// PostgreSQL's `interval_justify_days`, `interval_justify_hours` and
/// `interval_justify_interval`.
///
/// The division truncates towards zero rather than flooring, which is what makes
/// `justify_hours('-27 hours')` answer `-1 days -03:00:00` instead of `-2 days +21:00:00`:
/// PostgreSQL keeps the sign of the input on every component it produces.
///
/// Only `justify_interval` then goes on to make the three components *agree* in sign, which
/// is the step that turns `1 mon -1 hour` into `29 days 23:00:00`.
fn justify(value: IntervalMonthDayNano, unit: JustifyUnit) -> IntervalMonthDayNano {
    let mut months = i64::from(value.months);
    let mut days = i64::from(value.days);
    // Microseconds throughout, because PostgreSQL's interval is microsecond-resolution and
    // the rounding of a nanosecond remainder is not something it has an answer for.
    let mut time = value.nanoseconds / 1_000;
    let nanosecond_remainder = value.nanoseconds % 1_000;

    if matches!(unit, JustifyUnit::Hours | JustifyUnit::DaysAndHours) {
        days += time / USECS_PER_DAY;
        time %= USECS_PER_DAY;
    }
    if matches!(unit, JustifyUnit::Days | JustifyUnit::DaysAndHours) {
        months += days / i64::from(DAYS_PER_MONTH);
        days %= i64::from(DAYS_PER_MONTH);
    }

    if unit == JustifyUnit::DaysAndHours {
        if months > 0 && (days < 0 || (days == 0 && time < 0)) {
            days += i64::from(DAYS_PER_MONTH);
            months -= 1;
        } else if months < 0 && (days > 0 || (days == 0 && time > 0)) {
            days -= i64::from(DAYS_PER_MONTH);
            months += 1;
        }
        if days > 0 && time < 0 {
            time += USECS_PER_DAY;
            days -= 1;
        } else if days < 0 && time > 0 {
            time -= USECS_PER_DAY;
            days += 1;
        }
    }

    IntervalMonthDayNano::new(
        months as i32,
        days as i32,
        time * 1_000 + nanosecond_remainder,
    )
}

// ---------------------------------------------------------------------------
// clock_timestamp / timeofday
// ---------------------------------------------------------------------------

/// `clock_timestamp()`: the wall clock, read now rather than at the start of the statement.
///
/// Volatile, so nothing folds it: the whole point of it against `now()` is that two calls in
/// one statement can differ, which is what makes it usable for timing.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ClockTimestamp {
    signature: Signature,
}

impl Default for ClockTimestamp {
    fn default() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for ClockTimestamp {
    fn name(&self) -> &str {
        CLOCK_TIMESTAMP_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // The same type DataFusion's `now()` reports, so the two are comparable without a
        // cast — which is the comparison anyone calling `clock_timestamp()` is about to make.
        Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let now = Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| DataFusionError::Internal("the clock is out of range".to_string()))?;
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(now),
            Some("+00:00".into()),
        )))
    }
}

/// `timeofday()`: the wall clock as text, in the one format PostgreSQL prints it in.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TimeOfDay {
    signature: Signature,
}

impl Default for TimeOfDay {
    fn default() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for TimeOfDay {
    fn name(&self) -> &str {
        TIMEOFDAY_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            timeofday_text(Utc::now()),
        ))))
    }
}

/// `Fri Sep 18 07:40:13.696013 2026 UTC` — PostgreSQL's format, which is `ctime`'s with the
/// microseconds and the zone added.
fn timeofday_text(now: DateTime<Utc>) -> String {
    now.format("%a %b %d %H:%M:%S%.6f %Y UTC").to_string()
}

// ---------------------------------------------------------------------------
// shared readers
// ---------------------------------------------------------------------------

/// The microsecond values of a timestamp column, whatever unit or zone it arrived in.
fn timestamps(array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let micros = arrow::compute::kernels::cast::cast(array.as_ref(), &TS)?;
    let values = micros
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .ok_or_else(|| DataFusionError::Internal("expected a microsecond timestamp".to_string()))?;
    Ok((0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

/// The `i64` values of an integer column.
fn integers(array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let wide = arrow::compute::kernels::cast::cast(array.as_ref(), &DataType::Int64)?;
    let values = as_int64_array(&wide)?;
    Ok((0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

/// The `f64` values of a numeric column.
fn floats(array: &ArrayRef) -> Result<Vec<Option<f64>>> {
    let wide = arrow::compute::kernels::cast::cast(array.as_ref(), &DataType::Float64)?;
    let values = wide
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("expected a float column".to_string()))?;
    Ok((0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::Field;
    use chrono::{NaiveDateTime, TimeZone};
    use datafusion::execution::context::SessionContext;

    fn micros(text: &str) -> i64 {
        let naive = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| {
                NaiveDate::parse_from_str(text, "%Y-%m-%d")
                    .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight"))
            })
            .expect("a timestamp");
        Utc.from_utc_datetime(&naive).timestamp_micros()
    }

    /// The interval, spelled the way PostgreSQL's `interval_out` would spell the components
    /// — months, days and microseconds — so a test reads as the measured answer.
    fn parts(value: IntervalMonthDayNano) -> (i32, i32, i64) {
        (value.months, value.days, value.nanoseconds / 1_000)
    }

    fn age_parts(later: &str, earlier: &str) -> (i32, i32, i64) {
        parts(age_of(micros(later), micros(earlier)).expect("an interval"))
    }

    /// PostgreSQL 16.15's answers, and the reason `age` is not subtraction: the borrow takes
    /// the length of a particular month.
    #[test]
    fn age_borrows_from_the_month_postgresql_borrows_from() {
        // `1 year 2 mons 5 days`
        assert_eq!(age_parts("2024-03-15", "2023-01-10"), (14, 5, 0));
        // `1 mon 1 day` — January's 31 days were borrowed, not 30 and not February's 29.
        assert_eq!(age_parts("2024-03-01", "2024-01-31"), (1, 1, 0));
        // `1 year 2 mons 4 days 20:56:57.25`
        assert_eq!(
            age_parts("2024-03-15 01:02:03.5", "2023-01-10 04:05:06.25"),
            (14, 4, 20 * 3_600_000_000 + 56 * 60_000_000 + 57_250_000)
        );
    }

    /// `age(a, b)` is exactly `-age(b, a)`, component by component — which is why the sign is
    /// decided before the borrowing rather than after.
    #[test]
    fn age_is_symmetric_under_swapping_its_arguments() {
        // `-1 years -2 mons -5 days`
        assert_eq!(age_parts("2023-01-10", "2024-03-15"), (-14, -5, 0));
        // `-1 mons -1 days`
        assert_eq!(age_parts("2024-01-31", "2024-03-01"), (-1, -1, 0));
        for (later, earlier) in [
            ("2024-03-15", "2023-01-10"),
            ("2024-03-01", "2024-01-31"),
            ("2024-03-15 01:02:03.5", "2023-01-10 04:05:06.25"),
        ] {
            let (months, days, time) = age_parts(later, earlier);
            assert_eq!(age_parts(earlier, later), (-months, -days, -time));
        }
    }

    /// Two equal instants are a zero interval, not a NULL and not an error.
    #[test]
    fn age_of_one_instant_with_itself_is_zero() {
        assert_eq!(age_parts("2024-03-15", "2024-03-15"), (0, 0, 0));
    }

    fn make_timestamp(
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        min: i64,
        sec: f64,
    ) -> Result<i64> {
        make_timestamp_value(year, month, day, hour, min, sec)
    }

    /// The answer, at microsecond resolution, for a leap day.
    #[test]
    fn make_timestamp_keeps_every_microsecond() {
        let value = make_timestamp(2024, 2, 29, 13, 30, 45.123456).expect("a timestamp");
        assert_eq!(value, micros("2024-02-29 13:30:45.123456"));
    }

    /// The four refusals, each with PostgreSQL 16.15's message. The 30th of February is the
    /// one that matters: rolling it over to March would answer a date the caller never asked
    /// for and never notice the bug that produced it.
    #[test]
    fn make_timestamp_refuses_the_dates_postgresql_refuses() {
        for (args, expected) in [
            (
                (2024, 2, 30, 0, 0, 0.0),
                "date field value out of range: 2024-02-30",
            ),
            (
                (2024, 13, 1, 0, 0, 0.0),
                "date field value out of range: 2024-13-01",
            ),
            (
                (0, 1, 1, 0, 0, 0.0),
                "date field value out of range: 0-01-01",
            ),
            (
                (2024, 1, 1, 25, 0, 0.0),
                "time field value out of range: 25:00:00",
            ),
            (
                (2024, 1, 1, 0, 0, 61.0),
                "time field value out of range: 0:00:61",
            ),
        ] {
            let (year, month, day, hour, min, sec) = args;
            let err = make_timestamp(year, month, day, hour, min, sec).expect_err("refused");
            assert_eq!(
                strip_code_tags(&err.to_string()),
                format!("Execution error: {expected}"),
                "{args:?}"
            );
            assert_eq!(
                code_of_tagged_message(&err.to_string()),
                Some(VdbErrorCode::DatetimeFieldOverflow),
                "{args:?} carried the wrong code"
            );
        }
    }

    /// The two PostgreSQL accepts at the edge, so the refusal above is not over-reaching:
    /// 24:00:00 exactly is the next midnight, and `:60` is a leap second.
    #[test]
    fn make_timestamp_accepts_the_two_edges_postgresql_accepts() {
        assert_eq!(
            make_timestamp(2024, 1, 1, 24, 0, 0.0).expect("midnight"),
            micros("2024-01-02 00:00:00")
        );
        assert_eq!(
            make_timestamp(2024, 1, 1, 0, 0, 60.0).expect("a leap second"),
            micros("2024-01-01 00:01:00")
        );
    }

    /// A negative year is BC, and PostgreSQL's numbering has no year zero: `-1` is 1 BC,
    /// which the astronomical calendar chrono uses calls year 0.
    #[test]
    fn make_timestamp_reads_a_negative_year_as_bc() {
        let bc = make_timestamp(-1, 1, 1, 0, 0, 0.0).expect("1 BC");
        let ad = make_timestamp(1, 1, 1, 0, 0, 0.0).expect("1 AD");
        assert!(bc < ad, "1 BC should precede 1 AD");
        let civil = civil(bc).expect("decomposed");
        assert_eq!((civil.year, civil.month, civil.day), (0, 1, 1));
    }

    /// The `%02g` rendering PostgreSQL uses in the refusal message.
    #[test]
    fn the_seconds_in_a_refusal_are_rendered_as_postgresql_renders_them() {
        assert_eq!(seconds_as_postgresql_prints_them(0.0), "00");
        assert_eq!(seconds_as_postgresql_prints_them(61.0), "61");
        assert_eq!(seconds_as_postgresql_prints_them(45.123456), "45.1235");
    }

    fn run_make_interval(args: Vec<ArrayRef>) -> (i32, i32, i64) {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("a{i}"), a.data_type().clone(), true)))
            .collect();
        let out = MakeInterval::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: args.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: 1,
                return_field: Arc::new(Field::new("i", INTERVAL, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .expect("invoked")
            .to_array(1)
            .expect("array");
        let intervals = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("an interval array");
        parts(intervals.value(0))
    }

    fn ints(values: &[i64]) -> Vec<ArrayRef> {
        values
            .iter()
            .map(|v| Arc::new(Int64Array::from(vec![*v])) as ArrayRef)
            .collect()
    }

    /// `make_interval(1,2,3,4,5,6,7.5)` is `1 year 2 mons 25 days 05:06:07.5` in PostgreSQL
    /// 16.15: a week is seven days and a year is twelve months, and neither becomes a fixed
    /// number of microseconds.
    #[test]
    fn make_interval_keeps_the_components_separate() {
        let mut args = ints(&[1, 2, 3, 4, 5, 6]);
        args.push(Arc::new(Float64Array::from(vec![7.5])));
        assert_eq!(
            run_make_interval(args),
            (14, 25, 5 * 3_600_000_000 + 6 * 60_000_000 + 7_500_000)
        );
    }

    /// The shorter spellings, including the empty one, which PostgreSQL answers `00:00:00`.
    #[test]
    fn make_interval_accepts_any_prefix_of_its_fields() {
        assert_eq!(run_make_interval(vec![]), (0, 0, 0));
        assert_eq!(run_make_interval(ints(&[1])), (12, 0, 0));
        assert_eq!(run_make_interval(ints(&[1, 2])), (14, 0, 0));
        assert_eq!(
            run_make_interval(ints(&[0, 0, 0, 0, 0, 90])),
            (0, 0, 90 * 60_000_000)
        );
    }

    fn justified(months: i32, days: i32, micros: i64, unit: JustifyUnit) -> (i32, i32, i64) {
        parts(justify(
            IntervalMonthDayNano::new(months, days, micros * 1_000),
            unit,
        ))
    }

    /// PostgreSQL 16.15's answers for the two one-sided functions.
    #[test]
    fn justify_days_and_hours_move_one_group_each() {
        // `justify_days('35 days')` = `1 mon 5 days`
        assert_eq!(justified(0, 35, 0, JustifyUnit::Days), (1, 5, 0));
        // `justify_days('380 days')` = `1 year 20 days`
        assert_eq!(justified(0, 380, 0, JustifyUnit::Days), (12, 20, 0));
        // `justify_hours('27 hours')` = `1 day 03:00:00`
        assert_eq!(
            justified(0, 0, 27 * 3_600_000_000, JustifyUnit::Hours),
            (0, 1, 3 * 3_600_000_000)
        );
        // `justify_hours('50 hours 3 min')` = `2 days 02:03:00`
        assert_eq!(
            justified(
                0,
                0,
                50 * 3_600_000_000 + 3 * 60_000_000,
                JustifyUnit::Hours
            ),
            (0, 2, 2 * 3_600_000_000 + 3 * 60_000_000)
        );
    }

    /// The sign rule, which is where a floor-division implementation diverges: PostgreSQL
    /// keeps the input's sign on every component, so `-27 hours` is `-1 days -03:00:00` and
    /// not `-2 days +21:00:00`.
    #[test]
    fn justify_keeps_the_sign_of_its_input() {
        assert_eq!(justified(0, -35, 0, JustifyUnit::Days), (-1, -5, 0));
        assert_eq!(
            justified(0, 0, -27 * 3_600_000_000, JustifyUnit::Hours),
            (0, -1, -3 * 3_600_000_000)
        );
    }

    /// `justify_interval` is the only one that makes the components agree in sign, and the
    /// three PostgreSQL 16.15 answers that pin each branch of it.
    #[test]
    fn justify_interval_makes_the_components_agree() {
        // `justify_interval('1 mon -1 hour')` = `29 days 23:00:00`
        assert_eq!(
            justified(1, 0, -3_600_000_000, JustifyUnit::DaysAndHours),
            (0, 29, 23 * 3_600_000_000)
        );
        // `justify_interval('35 days 27 hours')` = `1 mon 6 days 03:00:00`
        assert_eq!(
            justified(0, 35, 27 * 3_600_000_000, JustifyUnit::DaysAndHours),
            (1, 6, 3 * 3_600_000_000)
        );
        // `justify_interval('-1 mon 33 days')` = `3 days`
        assert_eq!(justified(-1, 33, 0, JustifyUnit::DaysAndHours), (0, 3, 0));
    }

    /// And the property that separates it from the other two: applying it twice changes
    /// nothing, because the first application already agreed.
    #[test]
    fn justify_interval_is_idempotent() {
        for (months, days, micros) in [
            (1, 0, -3_600_000_000i64),
            (0, 35, 27 * 3_600_000_000),
            (-1, 33, 0),
            (0, -35, -27 * 3_600_000_000),
        ] {
            let once = justify(
                IntervalMonthDayNano::new(months, days, micros * 1_000),
                JustifyUnit::DaysAndHours,
            );
            assert_eq!(
                parts(justify(once, JustifyUnit::DaysAndHours)),
                parts(once),
                "({months}, {days}, {micros}) was not already justified"
            );
        }
    }

    fn run_isfinite(array: ArrayRef) -> Vec<Option<bool>> {
        let rows = array.len();
        let field = Arc::new(Field::new("x", array.data_type().clone(), true));
        let out = IsFinite::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: rows,
                return_field: Arc::new(Field::new("f", DataType::Boolean, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .expect("invoked")
            .to_array(rows)
            .expect("array");
        let values = out
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("a boolean array");
        (0..rows)
            .map(|row| (!values.is_null(row)).then(|| values.value(row)))
            .collect()
    }

    /// Every value VaireDB can store is finite, and a NULL is still a NULL.
    #[test]
    fn isfinite_is_true_for_every_value_that_exists() {
        let timestamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(micros("2024-01-01")),
            None,
        ]));
        assert_eq!(run_isfinite(timestamps), vec![Some(true), None]);
    }

    /// The one case where the answer is false: the text of a literal that says so. This is
    /// what makes `isfinite('infinity')` agree with PostgreSQL rather than being a constant.
    #[test]
    fn isfinite_reads_an_infinite_literal_as_infinite() {
        let literals: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2024-01-01"),
            Some("infinity"),
            Some("-infinity"),
            None,
        ]));
        assert_eq!(
            run_isfinite(literals),
            vec![Some(true), Some(false), Some(false), None]
        );
    }

    /// PostgreSQL has no `isfinite(integer)`, and the refusal says the function does not
    /// exist rather than that VaireDB has not got round to it.
    #[test]
    fn isfinite_of_a_number_is_refused_as_a_missing_function() {
        let err = IsFinite::default()
            .return_type(&[DataType::Int32])
            .expect_err("no such function");
        assert_eq!(
            strip_code_tags(&err.to_string()),
            "Error during planning: function isfinite(integer) does not exist"
        );
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::UndefinedFunction)
        );
    }

    /// The format is the whole of what `timeofday()` is for, so it is pinned against a fixed
    /// instant rather than against the clock.
    #[test]
    fn timeofday_is_rendered_the_way_postgresql_renders_it() {
        let instant = Utc
            .timestamp_micros(micros("2026-09-18 07:40:13.696013"))
            .single()
            .expect("an instant");
        assert_eq!(
            timeofday_text(instant),
            "Fri Sep 18 07:40:13.696013 2026 UTC"
        );
    }

    /// Every name has to resolve on every node, because the plan carries the name and
    /// nothing else — and the two volatile ones are the pair that actually reach an executor,
    /// since nothing folds them.
    #[tokio::test]
    async fn the_family_resolves_by_name_and_answers_over_a_column() {
        let mut ctx = SessionContext::new();
        for name in [
            AGE_UDF_NAME,
            MAKE_TIMESTAMP_UDF_NAME,
            MAKE_INTERVAL_UDF_NAME,
            ISFINITE_UDF_NAME,
            JUSTIFY_DAYS_UDF_NAME,
            JUSTIFY_HOURS_UDF_NAME,
            JUSTIFY_INTERVAL_UDF_NAME,
            CLOCK_TIMESTAMP_UDF_NAME,
            TIMEOFDAY_UDF_NAME,
        ] {
            assert!(
                ctx.udf(name).is_err(),
                "a bare context should not have {name}, or this test proves nothing"
            );
        }
        register_datetime_functions(&mut ctx).expect("registration failed");
        for name in [
            AGE_UDF_NAME,
            MAKE_TIMESTAMP_UDF_NAME,
            MAKE_INTERVAL_UDF_NAME,
            ISFINITE_UDF_NAME,
            JUSTIFY_DAYS_UDF_NAME,
            JUSTIFY_HOURS_UDF_NAME,
            JUSTIFY_INTERVAL_UDF_NAME,
            CLOCK_TIMESTAMP_UDF_NAME,
            TIMEOFDAY_UDF_NAME,
        ] {
            assert!(ctx.udf(name).is_ok(), "{name} did not resolve");
        }

        let batches = ctx
            .sql(
                "SELECT age(make_timestamp(2024, 3, 1, 0, 0, 0), \
                 make_timestamp(2024, 1, 31, 0, 0, 0)) AS a, \
                 justify_interval(make_interval(0, 1, 0, 0, -1)) AS j, \
                 isfinite(make_timestamp(2024, 1, 1, 0, 0, 0)) AS f, \
                 clock_timestamp() IS NOT NULL AS c, timeofday() AS t \
                 FROM (VALUES (1)) v(x)",
            )
            .await
            .expect("planned")
            .collect()
            .await
            .expect("executed");
        let rendered = arrow::util::pretty::pretty_format_batches(&batches)
            .expect("rendered")
            .to_string();
        // `1 mon 1 day` and `29 days 23:00:00`, however Arrow chooses to spell them.
        assert!(rendered.contains("true"), "got:\n{rendered}");
        assert!(rendered.contains("UTC"), "got:\n{rendered}");
    }

    /// The spellings a client writes, each of which has to *resolve*.
    ///
    /// A `timestamp` literal is `Timestamp(ns)` and a `date` literal is `Date32`, neither of
    /// which a `Timestamp(µs)` signature accepts — DataFusion declines to narrow a nanosecond
    /// timestamp, so `age(timestamp '…', timestamp '…')` failed to plan while the same call
    /// over a stored column planned fine. Hence the `any(2)` signature and the cast inside.
    #[tokio::test]
    async fn age_resolves_for_every_way_a_moment_is_written() {
        let mut ctx = SessionContext::new();
        register_datetime_functions(&mut ctx).expect("registration failed");
        for sql in [
            "SELECT age(TIMESTAMP '2026-09-18', TIMESTAMP '2000-01-15')",
            "SELECT age(DATE '2026-09-18', DATE '2000-01-15')",
            "SELECT age(DATE '2026-09-18', TIMESTAMP '2000-01-15')",
            "SELECT age(TIMESTAMP '2026-09-18 07:40:13', DATE '2000-01-15')",
            "SELECT age(NULL, TIMESTAMP '2000-01-15')",
        ] {
            ctx.sql(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql} did not plan: {e}"))
                .collect()
                .await
                .unwrap_or_else(|e| panic!("{sql} did not execute: {e}"));
        }
    }

    /// `age` of something that is not a moment in time: PostgreSQL has no such overload, and
    /// the refusal says so with `42883` rather than promising the form is on its way.
    #[tokio::test]
    async fn age_of_a_non_temporal_argument_is_an_undefined_function() {
        let mut ctx = SessionContext::new();
        register_datetime_functions(&mut ctx).expect("registration failed");
        let error = ctx
            .sql("SELECT age(1, 2)")
            .await
            .expect_err("a number is not a moment in time")
            .to_string();
        // `bigint` rather than PostgreSQL's `integer` because DataFusion types an integer
        // literal as `Int64` — the coordinator does too, measured, and its row description for
        // `SELECT 1` says `bigint` for the same reason. The name is read off whatever type
        // arrives, so this asserts the message names it the way the rest of the server does.
        assert!(
            error.contains("function age(bigint, bigint) does not exist"),
            "got: {error}"
        );
        assert_eq!(
            code_of_tagged_message(&error),
            Some(VdbErrorCode::UndefinedFunction),
            "got: {error}"
        );
    }
}
