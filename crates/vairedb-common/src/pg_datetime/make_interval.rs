//! `make_interval(years, months, weeks, days, hours, mins, secs)`: seven numbers into one
//! interval, each of them optional.
//!
//! The rule this file owns is which component each field lands in. A week is seven *days* and
//! a year is twelve *months*, and neither is a fixed number of microseconds — a day is 23 or
//! 25 hours across a DST boundary, and a month is 28 to 31 days — so collapsing them into a
//! microsecond count would make `make_interval(weeks => 1) + timestamp` answer a different
//! instant than PostgreSQL does. The three components stay apart for exactly as long as
//! PostgreSQL keeps them apart, which is until something adds the interval to a date.

use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, IntervalMonthDayNanoBuilder};
use arrow::datatypes::{DataType, IntervalMonthDayNano};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{INTERVAL, USECS_PER_SEC, floats, integers, interval, shared};

/// The name PostgreSQL uses, so a client's SQL needs no rewriting.
pub const MAKE_INTERVAL_UDF_NAME: &str = "make_interval";

/// The shared `make_interval` instance.
pub fn make_interval_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, MakeInterval::default)
}

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
        let seconds = arrays.get(6).map(floats).transpose()?;

        let mut out = IntervalMonthDayNanoBuilder::with_capacity(rows);
        for row in 0..rows {
            match fields_at(row, &whole, seconds.as_deref()) {
                Some((whole, secs)) => out.append_value(interval_of(whole, secs)?),
                // Strict in every argument, as PostgreSQL's is.
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// The seven fields at `row`, or `None` when any argument is NULL there.
///
/// A call that spelled fewer than seven leaves the rest at zero, which is the default
/// PostgreSQL's named parameters carry — so `make_interval(1, 2)` is one year and two months
/// and not one year, two months and an unspecified number of weeks.
fn fields_at(
    row: usize,
    whole: &[Vec<Option<i64>>],
    seconds: Option<&[Option<f64>]>,
) -> Option<([i64; 6], f64)> {
    let mut fields = [0i64; 6];
    for (field, column) in fields.iter_mut().zip(whole) {
        *field = column[row]?;
    }
    let secs = match seconds {
        Some(column) => column[row]?,
        None => 0.0,
    };
    Some((fields, secs))
}

/// The six whole fields and the seconds, each landing in the component PostgreSQL puts it in.
fn interval_of(fields: [i64; 6], secs: f64) -> Result<IntervalMonthDayNano> {
    let [years, months, weeks, days, hours, mins] = fields;
    interval(
        years * 12 + months,
        weeks * 7 + days,
        (hours * 60 + mins) * 60 * USECS_PER_SEC + (secs * USECS_PER_SEC as f64).round() as i64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_datetime::test_support::{interval_at, invoke, parts};
    use arrow::array::{Float64Array, Int64Array};

    fn run_make_interval(args: Vec<ArrayRef>) -> (i32, i32, i64) {
        let out = invoke(&MakeInterval::default(), args, INTERVAL);
        parts(interval_at(&out, 0))
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
}
