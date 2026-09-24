//! `make_timestamp(year, month, day, hour, min, sec)`: six numbers into one instant.
//!
//! The rule this file owns is the *refusals*, and they are the substance of the function.
//! Rolling `2024-02-30` over into `2024-03-01` would answer a date the caller never asked for
//! and hide the arithmetic bug that produced it, which is why PostgreSQL spends a SQLSTATE
//! (`22008`) on saying no instead. Two edges are accepted rather than refused — `24:00:00`
//! exactly, which is the following midnight, and a `:60` leap second — so the refusal has to
//! be exact in both directions, and the message has to be PostgreSQL's own text because a
//! client matching on it is matching the string PostgreSQL produces.

use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, TimestampMicrosecondBuilder};
use arrow::datatypes::DataType;
use chrono::NaiveDate;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{TS, USECS_PER_DAY, USECS_PER_SEC, field_error, floats, integers, shared};

/// The name PostgreSQL uses, so a client's SQL needs no rewriting.
pub const MAKE_TIMESTAMP_UDF_NAME: &str = "make_timestamp";

/// The shared `make_timestamp` instance.
pub fn make_timestamp_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, MakeTimestamp::default)
}

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
        let whole: Vec<Vec<Option<i64>>> =
            arrays[..5].iter().map(integers).collect::<Result<_>>()?;
        let seconds = floats(&arrays[5])?;

        let mut out = TimestampMicrosecondBuilder::with_capacity(rows);
        for row in 0..rows {
            match Fields::at(row, &whole, &seconds) {
                Some(fields) => out.append_value(fields.to_micros()?),
                // Strict in every argument, as PostgreSQL's is.
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// One row's six arguments, which travel together and are only meaningful together: the
/// refusal message for a bad day names the year and the month beside it, the way PostgreSQL's
/// does.
struct Fields {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: f64,
}

impl Fields {
    /// The six columns read at `row`, or `None` when any of them is NULL there.
    fn at(row: usize, whole: &[Vec<Option<i64>>], seconds: &[Option<f64>]) -> Option<Self> {
        // Five, checked by the arity guard above before the columns were read.
        let parts: Vec<i64> = whole
            .iter()
            .map(|field| field[row])
            .collect::<Option<_>>()?;
        Some(Self {
            year: parts[0],
            month: parts[1],
            day: parts[2],
            hour: parts[3],
            min: parts[4],
            sec: seconds[row]?,
        })
    }

    /// The instant, or PostgreSQL's refusal.
    ///
    /// The date is validated before the time, because that is the order PostgreSQL reports
    /// them in: `make_timestamp(2024, 2, 30, 25, 0, 0)` is a date error and not a time one.
    fn to_micros(&self) -> Result<i64> {
        let date = self.date()?;
        let time = self.time_of_day()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch is a valid date");
        let days = i64::from((date - epoch).num_days() as i32);
        days.checked_mul(USECS_PER_DAY)
            .and_then(|midnight| midnight.checked_add(time))
            .ok_or_else(|| self.date_error())
    }

    fn date(&self) -> Result<NaiveDate> {
        // PostgreSQL's calendar has no year zero: 1 BC is written `-1`, and chrono's
        // astronomical numbering puts that at year 0, so the two differ by one below the era
        // boundary and agree above it.
        if self.year == 0 {
            return Err(self.date_error());
        }
        let astronomical = if self.year < 0 {
            self.year + 1
        } else {
            self.year
        };
        i32::try_from(astronomical)
            .ok()
            .and_then(|year| {
                let month = u32::try_from(self.month).ok()?;
                let day = u32::try_from(self.day).ok()?;
                NaiveDate::from_ymd_opt(year, month, day)
            })
            .ok_or_else(|| self.date_error())
    }

    /// The microseconds since midnight, or PostgreSQL's refusal.
    fn time_of_day(&self) -> Result<i64> {
        // The second is truncated before it is range-checked, as PostgreSQL does, so `60.5` is
        // refused for the same reason `61` is.
        let whole_seconds = self.sec.trunc();
        let fractional = ((self.sec - whole_seconds) * USECS_PER_SEC as f64).round() as i64;
        let whole_seconds = whole_seconds as i64;
        // 24:00:00 exactly is midnight of the following day and PostgreSQL accepts it; a second
        // past it is not. A leap second (`:60`) is accepted too.
        let refused = !(0..=24).contains(&self.hour)
            || (self.hour == 24 && (self.min > 0 || whole_seconds > 0))
            || !(0..=59).contains(&self.min)
            || !(0..=60).contains(&whole_seconds);
        if refused {
            return Err(self.time_error());
        }
        Ok(((self.hour * 60 + self.min) * 60 + whole_seconds) * USECS_PER_SEC + fractional)
    }

    fn date_error(&self) -> DataFusionError {
        field_error(format!(
            "date field value out of range: {}-{:02}-{:02}",
            self.year, self.month, self.day
        ))
    }

    fn time_error(&self) -> DataFusionError {
        field_error(format!(
            "time field value out of range: {}:{:02}:{}",
            self.hour,
            self.min,
            seconds_as_postgresql_prints_them(self.sec)
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use crate::pg_datetime::test_support::micros;
    use crate::proto::vairedb::v1::VdbErrorCode;
    use chrono::DateTime;

    fn make_timestamp(
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        min: i64,
        sec: f64,
    ) -> Result<i64> {
        Fields {
            year,
            month,
            day,
            hour,
            min,
            sec,
        }
        .to_micros()
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
        assert_eq!(
            DateTime::from_timestamp_micros(bc)
                .expect("in chrono's range")
                .naive_utc()
                .date(),
            NaiveDate::from_ymd_opt(0, 1, 1).expect("year zero is 1 BC")
        );
    }

    /// The `%02g` rendering PostgreSQL uses in the refusal message.
    #[test]
    fn the_seconds_in_a_refusal_are_rendered_as_postgresql_renders_them() {
        assert_eq!(seconds_as_postgresql_prints_them(0.0), "00");
        assert_eq!(seconds_as_postgresql_prints_them(61.0), "61");
        assert_eq!(seconds_as_postgresql_prints_them(45.123456), "45.1235");
    }
}
