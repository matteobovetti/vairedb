//! `justify_days`, `justify_hours` and `justify_interval`: moving an interval's own
//! components around without changing what it means.
//!
//! The rule this file owns is the *sign*. The division truncates towards zero rather than
//! flooring, which is what makes `justify_hours('-27 hours')` answer `-1 days -03:00:00`
//! instead of `-2 days +21:00:00`: PostgreSQL keeps the sign of the input on every component
//! it produces, and a floor-division implementation reading like an obvious simplification
//! answers the second. Both spellings denote the same duration, so a client comparing the
//! rendered text against PostgreSQL's is the one who notices.
//!
//! The three are one implementation because they differ only in which of the two truncating
//! steps runs. `justify_interval` then adds the step neither of the others has: making the
//! three components *agree* in sign, which is what turns `1 mon -1 hour` into
//! `29 days 23:00:00`.

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, IntervalMonthDayNanoArray, IntervalMonthDayNanoBuilder};
use arrow::datatypes::{DataType, IntervalMonthDayNano};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use super::{DAYS_PER_MONTH, INTERVAL, USECS_PER_DAY};

/// The names PostgreSQL uses, so a client's SQL needs no rewriting.
pub const JUSTIFY_DAYS_UDF_NAME: &str = "justify_days";
pub const JUSTIFY_HOURS_UDF_NAME: &str = "justify_hours";
pub const JUSTIFY_INTERVAL_UDF_NAME: &str = "justify_interval";

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

impl JustifyUnit {
    /// The three, in the order the shared instances below are built in — which is also the
    /// order they are registered in.
    pub(super) const ALL: [Self; 3] = [Self::Days, Self::Hours, Self::DaysAndHours];

    fn position(self) -> usize {
        match self {
            Self::Days => 0,
            Self::Hours => 1,
            Self::DaysAndHours => 2,
        }
    }
}

/// One of the three `justify_*` functions.
///
/// One cell for all three rather than three cells, because they are one type and the variant
/// is a field: a per-variant accessor would be the same `OnceLock` written out three times.
pub fn justify_udf(unit: JustifyUnit) -> Arc<ScalarUDF> {
    static UDFS: OnceLock<[Arc<ScalarUDF>; 3]> = OnceLock::new();
    let all = UDFS
        .get_or_init(|| JustifyUnit::ALL.map(|unit| Arc::new(ScalarUDF::from(Justify::new(unit)))));
    Arc::clone(&all[unit.position()])
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
                continue;
            }
            out.append_value(justify(intervals.value(row), self.unit));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// PostgreSQL's `interval_justify_days`, `interval_justify_hours` and
/// `interval_justify_interval`; see the module doc for the sign rule the truncation encodes.
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
        (months, days, time) = agree_in_sign(months, days, time);
    }

    IntervalMonthDayNano::new(
        months as i32,
        days as i32,
        time * 1_000 + nanosecond_remainder,
    )
}

/// The step only `justify_interval` takes: borrow across a component whose sign disagrees
/// with the one above it, until all three agree.
///
/// It is why `justify_interval('1 mon -1 hour')` is `29 days 23:00:00` rather than the
/// literally-true `1 mon -01:00:00` — the interval PostgreSQL would render as `1 mon -1 hour`
/// is the one it was handed, and justifying it means not leaving a negative hour hanging off
/// a positive month.
fn agree_in_sign(mut months: i64, mut days: i64, mut time: i64) -> (i64, i64, i64) {
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
    (months, days, time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_datetime::test_support::parts;

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

    /// The three names are the three variants, and the registration order the shared
    /// instances are handed out in has to match — a `justify_days` answering the
    /// `justify_hours` rule is a wrong answer with a right-looking type.
    #[test]
    fn each_variant_answers_under_its_own_postgresql_name() {
        for unit in JustifyUnit::ALL {
            assert_eq!(justify_udf(unit).name(), Justify::new(unit).name());
        }
    }
}
