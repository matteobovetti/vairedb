//! What the family's tests share: a way to write an instant, a way to read an interval, and
//! one invocation of a scalar function.
//!
//! These hold no rule. Each function's own rules are asserted next to the function, and this
//! is only the scaffolding those assertions are written on — which used to be copied into
//! every test module that needed to call an `invoke_with_args`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, IntervalMonthDayNanoArray};
use arrow::datatypes::{DataType, Field, IntervalMonthDayNano};
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

/// A timestamp written the way a test reads best — `2024-03-15` or
/// `2024-03-15 01:02:03.5` — as the microseconds the family works in.
pub(super) fn micros(text: &str) -> i64 {
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
pub(super) fn parts(value: IntervalMonthDayNano) -> (i32, i32, i64) {
    (value.months, value.days, value.nanoseconds / 1_000)
}

/// One row of an interval column.
pub(super) fn interval_at(array: &ArrayRef, row: usize) -> IntervalMonthDayNano {
    array
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .expect("an interval array")
        .value(row)
}

/// One invocation of `udf` over `args`, with the surrounding fields DataFusion would hand it
/// derived from the arguments' own types.
///
/// An empty argument list is one row, because that is what a nullary call over a single-row
/// input is; anything else is as many rows as the first column has.
pub(super) fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ArrayRef>, returns: DataType) -> ArrayRef {
    let rows = args.first().map_or(1, |array| array.len());
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(index, array)| {
            Arc::new(Field::new(
                format!("a{index}"),
                array.data_type().clone(),
                true,
            ))
        })
        .collect();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: args.into_iter().map(ColumnarValue::Array).collect(),
        arg_fields,
        number_rows: rows,
        return_field: Arc::new(Field::new("out", returns, true)),
        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
    })
    .expect("invoked")
    .to_array(rows)
    .expect("array")
}
