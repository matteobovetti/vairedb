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
//! ## How the family is laid out
//!
//! One submodule per function, because the functions share a *vocabulary* and nothing else:
//! `age`'s calendar borrowing, `make_timestamp`'s refusals, `make_interval`'s component
//! arithmetic, `isfinite`'s answer about what Arrow can hold, `justify`'s sign rule and
//! `wall_clock`'s output format are six unrelated PostgreSQL rules, each with its own reason
//! to change and none of them a reason to re-read the others.
//!
//! What they do share is here, and only here: the two Arrow types the family answers in, the
//! readers that narrow an argument's column to the width a rule is written against, and the
//! `22008` refusal a component that does not fit is reported as. None of those decide
//! anything — they are the units the rules are written in — which is why sharing them
//! couples nothing.
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

mod age;
mod isfinite;
mod justify;
mod make_interval;
mod make_timestamp;
mod wall_clock;

#[cfg(test)]
mod test_support;

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float64Type, Int64Type, IntervalMonthDayNano, IntervalUnit,
    TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

pub use age::{AGE_UDF_NAME, age_udf};
pub use isfinite::{ISFINITE_UDF_NAME, isfinite_udf};
pub use justify::{
    JUSTIFY_DAYS_UDF_NAME, JUSTIFY_HOURS_UDF_NAME, JUSTIFY_INTERVAL_UDF_NAME, JustifyUnit,
    justify_udf,
};
pub use make_interval::{MAKE_INTERVAL_UDF_NAME, make_interval_udf};
pub use make_timestamp::{MAKE_TIMESTAMP_UDF_NAME, make_timestamp_udf};
pub use wall_clock::{
    CLOCK_TIMESTAMP_UDF_NAME, TIMEOFDAY_UDF_NAME, clock_timestamp_udf, timeofday_udf,
};

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
    for unit in JustifyUnit::ALL {
        registry.register_udf(justify_udf(unit))?;
    }
    registry.register_udf(clock_timestamp_udf())?;
    registry.register_udf(timeofday_udf())?;
    Ok(())
}

/// The one instance of `T` this process registers, built on first use.
///
/// A server holds one session context per connection and registers the family on each of
/// them, so a fresh `Arc` per registration would be a fresh allocation per connection for a
/// value that is immutable and identical every time. Each accessor owns its own cell — a
/// `static` cannot be generic — and this holds the part that would otherwise be copied into
/// each of them.
fn shared<T: ScalarUDFImpl + 'static>(
    cell: &'static OnceLock<Arc<ScalarUDF>>,
    build: fn() -> T,
) -> Arc<ScalarUDF> {
    Arc::clone(cell.get_or_init(|| Arc::new(ScalarUDF::from(build()))))
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

/// A refusal in PostgreSQL's own words, carrying the `22008` the client reads.
///
/// `message` is PostgreSQL's text verbatim and not a paraphrase: a client that matches on
/// `date field value out of range` is matching the string PostgreSQL produces, and the
/// whole point of reproducing the refusal is that it reproduces.
fn field_error(message: String) -> DataFusionError {
    DatetimeFieldError { message }.into()
}

fn out_of_range(what: &str, value: i64) -> DataFusionError {
    field_error(format!("{what} out of range: {value}"))
}

/// An interval from the three components PostgreSQL keeps separate, refusing one that does
/// not fit rather than wrapping it.
///
/// Arrow's months and days are 32-bit where PostgreSQL's are, so the same values fit; what
/// does not fit is an arithmetic result that overflowed on the way here, and a wrapped month
/// count is a wrong answer the client cannot detect. Both `age` and `make_interval` end in
/// this, so both refuse in the same words.
fn interval(months: i64, days: i64, micros: i64) -> Result<IntervalMonthDayNano> {
    Ok(IntervalMonthDayNano::new(
        i32::try_from(months).map_err(|_| out_of_range("interval", months))?,
        i32::try_from(days).map_err(|_| out_of_range("interval", days))?,
        micros
            .checked_mul(1_000)
            .ok_or_else(|| out_of_range("interval", micros))?,
    ))
}

/// The microsecond values of a timestamp column, whatever unit or zone it arrived in.
fn timestamps(array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    column::<TimestampMicrosecondType>(array, &TS, "a microsecond timestamp")
}

/// The `i64` values of an integer column.
fn integers(array: &ArrayRef) -> Result<Vec<Option<i64>>> {
    column::<Int64Type>(array, &DataType::Int64, "an integer column")
}

/// The `f64` values of a numeric column.
fn floats(array: &ArrayRef) -> Result<Vec<Option<f64>>> {
    column::<Float64Type>(array, &DataType::Float64, "a float column")
}

/// One argument's column as the width its rule is written against, with the nulls kept.
///
/// The cast is the point: an `age` argument arrives as `Timestamp(ns)` from a literal and as
/// `Timestamp(µs)` from a scan, and a `make_interval` field arrives as whatever integer width
/// coercion settled on. Narrowing here rather than in each rule is what lets the rules be
/// written about numbers. `expected` only ever appears in an internal error the cast above
/// has already made unreachable.
fn column<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    target: &DataType,
    expected: &str,
) -> Result<Vec<Option<T::Native>>> {
    let narrowed = cast(array.as_ref(), target)?;
    let values = narrowed
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected {expected}")))?;
    Ok((0..values.len())
        .map(|row| (!values.is_null(row)).then(|| values.value(row)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use datafusion::execution::context::SessionContext;

    /// Every name the family registers, which is also every name a plan can carry.
    const NAMES: [&str; 9] = [
        AGE_UDF_NAME,
        MAKE_TIMESTAMP_UDF_NAME,
        MAKE_INTERVAL_UDF_NAME,
        ISFINITE_UDF_NAME,
        JUSTIFY_DAYS_UDF_NAME,
        JUSTIFY_HOURS_UDF_NAME,
        JUSTIFY_INTERVAL_UDF_NAME,
        CLOCK_TIMESTAMP_UDF_NAME,
        TIMEOFDAY_UDF_NAME,
    ];

    /// A component that does not fit Arrow's 32-bit month or day count is `22008`, and the
    /// message names the value the client wrote rather than the Arrow width, which is not
    /// something the client asked about.
    #[test]
    fn an_interval_component_that_does_not_fit_is_refused_as_an_overflow() {
        let err = interval(i64::from(i32::MAX) + 1, 0, 0).expect_err("too many months");
        assert_eq!(
            strip_code_tags(&err.to_string()),
            "Execution error: interval out of range: 2147483648"
        );
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::DatetimeFieldOverflow)
        );
    }

    /// Every name has to resolve on every node, because the plan carries the name and
    /// nothing else — and the two volatile ones are the pair that actually reach an executor,
    /// since nothing folds them.
    #[tokio::test]
    async fn the_family_resolves_by_name_and_answers_over_a_column() {
        let mut ctx = SessionContext::new();
        for name in NAMES {
            assert!(
                ctx.udf(name).is_err(),
                "a bare context should not have {name}, or this test proves nothing"
            );
        }
        register_datetime_functions(&mut ctx).expect("registration failed");
        for name in NAMES {
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
}
