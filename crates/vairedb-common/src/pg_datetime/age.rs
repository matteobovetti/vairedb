//! `age(later, earlier)`: the difference two instants make on PostgreSQL's calendar.
//!
//! The rule this file owns is the *borrowing*, and it is the only reason `age` is not
//! subtraction. A negative day count borrows the length of a **specific** month rather than
//! an average one, so the answer depends on which month the larger operand sits in.
//! `age('2024-03-01', '2024-01-31')` is `1 mon 1 day` because January has 31 days; borrowing
//! 30 would answer `1 mon 2 days` and borrowing February's 29 would answer `1 mon`. A client
//! reconciling VaireDB against PostgreSQL reads any of those three as a data mismatch, not as
//! a rounding choice, which is why this reproduces PostgreSQL's `timestamp_age` field by
//! field instead of dividing a microsecond difference.

use std::sync::{Arc, OnceLock};

use arrow::array::IntervalMonthDayNanoBuilder;
use arrow::datatypes::{DataType, IntervalMonthDayNano};
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{DAYS_PER_MONTH, INTERVAL, USECS_PER_SEC, interval, out_of_range, shared, timestamps};
use crate::error::tagged_message;
use crate::pg_typeof::pg_type_name;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name PostgreSQL uses, so a client's SQL needs no rewriting.
pub const AGE_UDF_NAME: &str = "age";

/// The shared `age` instance.
pub fn age_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, Age::default)
}

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

/// PostgreSQL's `timestamp_age`.
fn age_of(later_us: i64, earlier_us: i64) -> Result<IntervalMonthDayNano> {
    let (later, earlier) = (civil(later_us)?, civil(earlier_us)?);
    // The sign is decided once, up front: PostgreSQL computes the difference of the larger
    // from the smaller and negates, so that `age(a, b)` is exactly `-age(b, a)`.
    let negated = later_us < earlier_us;
    let sign = if negated { -1 } else { 1 };

    let mut difference = Difference::of(&later, &earlier, sign);
    // The month being borrowed from is the one the *larger* operand sits in.
    difference.borrow_from(if negated { &later } else { &earlier });

    interval(
        sign * (difference.year * 12 + difference.month),
        sign * difference.day,
        sign * difference.time_of_day(),
    )
}

/// The civil-calendar fields of two instants subtracted one field at a time, before any of
/// them has been borrowed back into range.
///
/// Each is an `i64` — a field's difference is small, but `year * 12` on the way out is not,
/// and the intermediate value has to survive being negative.
struct Difference {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: i64,
    usec: i64,
}

impl Difference {
    /// `sign * (later - earlier)`, field by field, so that what follows always works towards
    /// a positive answer and the caller negates once at the end.
    fn of(later: &Civil, earlier: &Civil, sign: i64) -> Self {
        Self {
            year: sign * (later.year - earlier.year),
            month: sign * (later.month - earlier.month),
            day: sign * (later.day - earlier.day),
            hour: sign * (later.hour - earlier.hour),
            min: sign * (later.min - earlier.min),
            sec: sign * (later.sec - earlier.sec),
            usec: sign * (later.usec - earlier.usec),
        }
    }

    /// Carry every negative field up into the next one, borrowing the days of `month_of`.
    ///
    /// This is the whole of the algorithm. Every step but one borrows a constant — 60, 60,
    /// 24, 12 — and the day step borrows the length of a *particular* month, which is what
    /// makes `age` a calendar operation rather than a division.
    fn borrow_from(&mut self, month_of: &Civil) {
        while self.usec < 0 {
            self.usec += USECS_PER_SEC;
            self.sec -= 1;
        }
        while self.sec < 0 {
            self.sec += 60;
            self.min -= 1;
        }
        while self.min < 0 {
            self.min += 60;
            self.hour -= 1;
        }
        while self.hour < 0 {
            self.hour += 24;
            self.day -= 1;
        }
        while self.day < 0 {
            self.day += days_in_month(month_of.year as i32, month_of.month as u32);
            self.month -= 1;
        }
        while self.month < 0 {
            self.month += 12;
            self.year -= 1;
        }
    }

    /// The four sub-day fields as the one microsecond count an Arrow interval holds.
    fn time_of_day(&self) -> i64 {
        ((self.hour * 60 + self.min) * 60 + self.sec) * USECS_PER_SEC + self.usec
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::code_of_tagged_message;
    use crate::pg_datetime::register_datetime_functions;
    use crate::pg_datetime::test_support::{micros, parts};
    use datafusion::execution::context::SessionContext;

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
