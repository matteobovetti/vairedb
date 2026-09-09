//! Turn a decoded bind parameter into a [`WriteParam`] the shard's engine can bind.
//!
//! A parameterized write crosses one narrow boundary: the `WriteParam` oneof, which
//! carries `is_null | bool | int64 | double | string | bytes`. Everything DuckDB has no
//! dedicated bind value for travels as a **string** and is cast by DuckDB on bind — which
//! works, because every type in question has an unambiguous SQL literal form, and because
//! DuckDB's cast from text is the same one it applies to an inline literal in the simple
//! protocol. The same statement therefore means the same thing whichever protocol carried
//! it.
//!
//! What that string is has to be chosen deliberately, and the reason this module exists is
//! that it once was not. The old code ended in
//!
//! ```ignore
//! other => write_param::Value::StringVal(other.to_string()),
//! ```
//!
//! and [`ScalarValue`]'s `Display` is a *diagnostic* rendering, not a literal. A
//! `NUMERIC(10,2)` parameter — the most ordinary parameterized write in PostgreSQL —
//! arrived at the shard as `Some(12345600000000),38,10` and failed on bind with
//! `42804 Could not convert string "Some(12345600000000),38,10" to DECIMAL(10,2)`. A
//! `TIMESTAMP` arrived as a bare microsecond count, a `TIME` as a bare microsecond count,
//! and an `INTERVAL` as its `Debug` form. Two variants — `Date32` and `UInt64` — happened
//! to render correctly, which is why the defect survived: the shapes people test worked.
//!
//! So every variant that reaches the string form is now rendered explicitly, and a variant
//! with no rendering is **refused** rather than stringified. A refusal names the type and
//! the position, which is a diagnosis; a wrong string is a bind error from inside the
//! engine, quoting a rendering the client never wrote.
//!
//! ## Where the renderings come from
//!
//! Not from hand-written arithmetic. Arrow's own [`ArrayFormatter`] already renders every
//! one of these types in the ISO-8601-and-decimal-point form DuckDB parses: a decimal at
//! its declared scale, a date as `2024-01-01`, a time as `12:34:56.789`, a timestamp as
//! `2024-01-01T12:34:56.789`, and a zoned timestamp with its offset. Re-deriving the scale
//! placement or the epoch arithmetic here would be a second implementation of something
//! Arrow already gets right, and it is the kind of code that is wrong only for the values
//! nobody tests — a leap day, a negative epoch, a scale of zero.
//!
//! Intervals are the exception, and they are hand-rendered because Arrow's form is not
//! parseable: `IntervalMonthDayNano` formats as `0 years 1 mons 2 days 0 hours 0 mins
//! 0.000003 secs`, and `mons` is not a unit DuckDB accepts. The components are rendered
//! instead, in units both engines name the same way — by
//! [`interval_sql_literal`](crate::pgwire_handler::encoding::interval_sql_literal), the
//! same function the read path renders an interval cell with, so a value written as a
//! parameter and one re-emitted from a `SELECT` are the same string.
//!
//! ## What stays typed
//!
//! Booleans, the signed integers up to 64 bits, the unsigned integers up to 32 bits,
//! floats, strings and byte strings keep their typed variants. `UInt64` does not: it does
//! not fit `int64`, and a value above `i64::MAX` would arrive negative, so it is rendered
//! as digits and cast.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::scalar::ScalarValue;

use vairedb_common::proto::vairedb::v1::{WriteParam, write_param};

use crate::error::{CoordinatorError, Result};
use crate::pgwire_handler::encoding::interval_sql_literal as interval_literal;

/// Nanoseconds in a microsecond, the unit an interval's sub-day part is rendered in.
const NANOS_PER_MICRO: i64 = 1_000;

/// Convert the decoded bind parameter at `position` (1-based, as the client numbers it)
/// into a `WriteParam`.
///
/// A missing parameter, and any NULL, maps to `is_null`: a client that bound fewer values
/// than the statement references has already been rejected by the protocol handler, so a
/// gap here is a NULL rather than an error to raise a second time.
///
/// Fails for a value with no SQL literal form — a list, a struct, a map, a dictionary —
/// rather than sending the engine a rendering it cannot bind.
pub(crate) fn scalar_to_write_param(
    scalar: Option<&ScalarValue>,
    position: usize,
) -> Result<WriteParam> {
    let value = match scalar {
        None => write_param::Value::IsNull(true),
        Some(s) if s.is_null() => write_param::Value::IsNull(true),
        Some(s) => typed_value(s, position)?,
    };
    Ok(WriteParam { value: Some(value) })
}

/// The non-NULL half of [`scalar_to_write_param`].
fn typed_value(scalar: &ScalarValue, position: usize) -> Result<write_param::Value> {
    use write_param::Value;

    let value = match scalar {
        ScalarValue::Boolean(Some(b)) => Value::BoolVal(*b),
        ScalarValue::Int8(Some(v)) => Value::IntVal(*v as i64),
        ScalarValue::Int16(Some(v)) => Value::IntVal(*v as i64),
        ScalarValue::Int32(Some(v)) => Value::IntVal(*v as i64),
        ScalarValue::Int64(Some(v)) => Value::IntVal(*v),
        ScalarValue::UInt8(Some(v)) => Value::IntVal(*v as i64),
        ScalarValue::UInt16(Some(v)) => Value::IntVal(*v as i64),
        ScalarValue::UInt32(Some(v)) => Value::IntVal(*v as i64),
        // Does not fit an i64: above `i64::MAX` it would arrive negative.
        ScalarValue::UInt64(Some(v)) => Value::StringVal(v.to_string()),
        ScalarValue::Float16(Some(v)) => Value::DoubleVal(f32::from(*v) as f64),
        ScalarValue::Float32(Some(v)) => Value::DoubleVal(*v as f64),
        ScalarValue::Float64(Some(v)) => Value::DoubleVal(*v),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Value::StringVal(v.clone()),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v)) => Value::BytesVal(v.clone()),

        // Rendered as a SQL literal and cast by DuckDB on bind.
        ScalarValue::Decimal128(..)
        | ScalarValue::Decimal256(..)
        | ScalarValue::Date32(_)
        | ScalarValue::Date64(_)
        | ScalarValue::Time32Second(_)
        | ScalarValue::Time32Millisecond(_)
        | ScalarValue::Time64Microsecond(_)
        | ScalarValue::Time64Nanosecond(_)
        | ScalarValue::TimestampSecond(..)
        | ScalarValue::TimestampMillisecond(..)
        | ScalarValue::TimestampMicrosecond(..)
        | ScalarValue::TimestampNanosecond(..) => Value::StringVal(sql_literal(scalar, position)?),

        ScalarValue::IntervalYearMonth(Some(months)) => {
            Value::StringVal(interval_literal(*months, 0, 0))
        }
        ScalarValue::IntervalDayTime(Some(v)) => Value::StringVal(interval_literal(
            0,
            v.days,
            v.milliseconds as i64 * NANOS_PER_MICRO * NANOS_PER_MICRO,
        )),
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
            Value::StringVal(interval_literal(months, days, nanos))
        }

        other => return Err(unbindable(other.data_type(), position)),
    };
    Ok(value)
}

/// `scalar` in the literal form DuckDB parses, via Arrow's own formatter.
///
/// The formatter is asked for a one-element array because that is the only interface Arrow
/// exposes to its display logic; the cost is one allocation per parameter, on a path that
/// is already sending an RPC.
fn sql_literal(scalar: &ScalarValue, position: usize) -> Result<String> {
    let array: ArrayRef = scalar
        .to_array()
        .map_err(|e| internal(scalar.data_type(), position, e.to_string()))?;
    // No null case to configure: a NULL never reaches here.
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(array.as_ref(), &options)
        .map_err(|e| internal(scalar.data_type(), position, e.to_string()))?;
    Ok(formatter.value(0).to_string())
}

/// A parameter whose type has no SQL literal form.
fn unbindable(data_type: DataType, position: usize) -> CoordinatorError {
    let what = match data_type {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            "an array".to_string()
        }
        DataType::Struct(_) => "a struct".to_string(),
        DataType::Map(..) => "a map".to_string(),
        other => format!("a {other}"),
    };
    CoordinatorError::Unsupported(format!(
        "${position} is {what} bind parameter, which a write cannot carry: the shard's engine \
         binds it from a SQL literal and this type has none. Write the value inline in the \
         statement instead"
    ))
}

/// A rendering that failed inside Arrow — not a client error, and not something to paper
/// over with a wrong literal.
fn internal(data_type: DataType, position: usize, detail: String) -> CoordinatorError {
    CoordinatorError::Internal(format!(
        "failed to render bind parameter ${position} of type {data_type} as a SQL literal: \
         {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};

    /// The string a parameter is carried as, or the error message it is refused with.
    fn rendered(scalar: ScalarValue) -> std::result::Result<String, String> {
        match scalar_to_write_param(Some(&scalar), 1).map_err(|e| e.to_string())? {
            WriteParam {
                value: Some(write_param::Value::StringVal(s)),
            } => Ok(s),
            other => Err(format!("expected a string parameter, got {other:?}")),
        }
    }

    // The defect this module was written for: the literal DuckDB could not bind.
    #[test]
    fn a_decimal_is_rendered_at_its_declared_scale() {
        assert_eq!(
            rendered(ScalarValue::Decimal128(Some(123456), 10, 2)).unwrap(),
            "1234.56"
        );
        // Scale 0 must not gain a decimal point, and a negative value keeps its sign.
        assert_eq!(
            rendered(ScalarValue::Decimal128(Some(-42), 5, 0)).unwrap(),
            "-42"
        );
        // 38 digits: the widest decimal DuckDB has, and the one the old `Display`
        // rendering mangled most visibly.
        let wide = "9".repeat(38).parse::<i128>().unwrap();
        assert_eq!(
            rendered(ScalarValue::Decimal128(Some(wide), 38, 0)).unwrap(),
            "9".repeat(38)
        );
    }

    #[test]
    fn a_timestamp_is_rendered_as_a_timestamp_not_as_a_count_of_microseconds() {
        // 2024-01-01 11:00:00 UTC.
        let micros = 1_704_106_800_000_000;
        let literal = rendered(ScalarValue::TimestampMicrosecond(Some(micros), None)).unwrap();
        assert!(
            literal.starts_with("2024-01-01T11:00:00"),
            "unexpected rendering: {literal}"
        );
        // A zoned value keeps the zone, so the engine stores the instant the client meant.
        let zoned = rendered(ScalarValue::TimestampMicrosecond(
            Some(micros),
            Some("+02:00".into()),
        ))
        .unwrap();
        assert!(
            zoned.starts_with("2024-01-01T13:00:00") && zoned.contains("+02:00"),
            "unexpected rendering: {zoned}"
        );
    }

    #[test]
    fn a_time_is_rendered_as_a_clock_time() {
        // 12:34:56.
        let literal = rendered(ScalarValue::Time64Microsecond(Some(45_296_000_000))).unwrap();
        assert!(
            literal.starts_with("12:34:56"),
            "unexpected rendering: {literal}"
        );
    }

    #[test]
    fn a_date_is_rendered_iso() {
        // Date32's own `Display` was already ISO, so this pins the behavior that used to
        // work by luck and must keep working now that it goes through the formatter.
        assert_eq!(
            rendered(ScalarValue::Date32(Some(19_723))).unwrap(),
            "2024-01-01"
        );
    }

    #[test]
    fn an_interval_is_rendered_in_units_duckdb_names() {
        assert_eq!(
            rendered(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(2, 3, 4_000)
            )))
            .unwrap(),
            "2 months 3 days 4 microseconds"
        );
        // The identity interval still needs all three components spelled out.
        assert_eq!(
            rendered(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 0, 0)
            )))
            .unwrap(),
            "0 months 0 days 0 microseconds"
        );
        assert_eq!(
            rendered(ScalarValue::IntervalYearMonth(Some(14))).unwrap(),
            "14 months 0 days 0 microseconds"
        );
        // A day-time interval's milliseconds become microseconds, not nanoseconds.
        assert_eq!(
            rendered(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                1, 5
            ))))
            .unwrap(),
            "0 months 1 days 5000 microseconds"
        );
    }

    #[test]
    fn a_u64_above_the_i64_range_keeps_its_value() {
        assert_eq!(
            rendered(ScalarValue::UInt64(Some(u64::MAX))).unwrap(),
            u64::MAX.to_string()
        );
    }

    // The typed variants, which must not have been broadened into strings.
    #[test]
    fn the_types_duckdb_binds_natively_stay_typed() {
        let cases = [
            (ScalarValue::Boolean(Some(true)), "BoolVal"),
            (ScalarValue::Int64(Some(7)), "IntVal"),
            (ScalarValue::UInt32(Some(7)), "IntVal"),
            (ScalarValue::Float64(Some(1.5)), "DoubleVal"),
            (ScalarValue::Utf8(Some("x".into())), "StringVal"),
            (ScalarValue::Binary(Some(vec![0, 159])), "BytesVal"),
        ];
        for (scalar, expected) in cases {
            let param = scalar_to_write_param(Some(&scalar), 1).unwrap();
            let actual = match param.value.unwrap() {
                write_param::Value::BoolVal(_) => "BoolVal",
                write_param::Value::IntVal(_) => "IntVal",
                write_param::Value::DoubleVal(_) => "DoubleVal",
                write_param::Value::StringVal(_) => "StringVal",
                write_param::Value::BytesVal(_) => "BytesVal",
                write_param::Value::IsNull(_) => "IsNull",
            };
            assert_eq!(actual, expected, "wrong variant for {scalar}");
        }
    }

    #[test]
    fn a_null_and_a_missing_parameter_are_both_null() {
        for scalar in [None, Some(&ScalarValue::Int64(None))] {
            let param = scalar_to_write_param(scalar, 1).unwrap();
            assert!(matches!(
                param.value,
                Some(write_param::Value::IsNull(true))
            ));
        }
        // Including a typed NULL of a type whose non-NULL form is refused: the NULL check
        // comes first, so a client binding NULL to an array column is not refused for the
        // type it did not send a value of.
        let empty_list = ScalarValue::List(std::sync::Arc::new(
            datafusion::arrow::array::ListArray::from_iter_primitive::<
                datafusion::arrow::datatypes::Int32Type,
                _,
                _,
            >(vec![None::<Vec<Option<i32>>>]),
        ));
        assert!(matches!(
            scalar_to_write_param(Some(&empty_list), 1).unwrap().value,
            Some(write_param::Value::IsNull(true))
        ));
    }

    // A type with no literal form is refused by name, and the message says what to do.
    #[test]
    fn a_parameter_with_no_literal_form_is_refused_rather_than_stringified() {
        let list = ScalarValue::List(std::sync::Arc::new(
            datafusion::arrow::array::ListArray::from_iter_primitive::<
                datafusion::arrow::datatypes::Int32Type,
                _,
                _,
            >(vec![Some(vec![Some(1), Some(2)])]),
        ));
        let err = scalar_to_write_param(Some(&list), 3)
            .expect_err("an array parameter has no literal form")
            .to_string();
        assert!(err.contains("$3"), "the position must be named: {err}");
        assert!(err.contains("array"), "the type must be named: {err}");
        assert!(
            err.contains("inline"),
            "the message must say what to do instead: {err}"
        );
    }
}
