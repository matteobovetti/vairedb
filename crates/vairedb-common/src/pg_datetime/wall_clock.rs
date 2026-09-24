//! `clock_timestamp()` and `timeofday()`: the two datetime functions that read a clock, and
//! the only two of PostgreSQL's six that may.
//!
//! The rule this file owns is the volatility, and it is the same rule for both. PostgreSQL's
//! other four clock forms are fixed once per statement or per transaction, so on a distributed
//! engine they have to be resolved on the coordinator — three shards reading three system
//! clocks answer three different times for one query, and nothing in the result says so. See
//! `vairedb_coordinator::pgwire_handler::pg_clock_functions` for that half. These two are
//! *defined* as answering per call, so being evaluated on whichever node runs the projection
//! is not a divergence but the specification: `Volatile` keeps the planner from folding one
//! call and reusing the answer, which is what makes the pair usable for timing at all.

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::shared;

/// The names PostgreSQL uses, so a client's SQL needs no rewriting.
pub const CLOCK_TIMESTAMP_UDF_NAME: &str = "clock_timestamp";
pub const TIMEOFDAY_UDF_NAME: &str = "timeofday";

/// The shared `clock_timestamp` instance.
pub fn clock_timestamp_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, ClockTimestamp::default)
}

/// The shared `timeofday` instance.
pub fn timeofday_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, TimeOfDay::default)
}

/// Neither takes an argument and both answer from the clock, which is the whole of what their
/// signatures have to say.
fn reads_the_clock() -> Signature {
    Signature::nullary(Volatility::Volatile)
}

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
            signature: reads_the_clock(),
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
            signature: reads_the_clock(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_datetime::test_support::micros;
    use chrono::TimeZone;

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

    /// Both are `Volatile`, which is the one property that cannot be read off the answer: a
    /// `Stable` one would be folded to a single literal per statement, and
    /// `clock_timestamp() - clock_timestamp()` — the reason either exists — would measure
    /// exactly zero.
    #[test]
    fn both_read_the_clock_on_every_call() {
        assert_eq!(
            ClockTimestamp::default().signature().volatility,
            Volatility::Volatile
        );
        assert_eq!(
            TimeOfDay::default().signature().volatility,
            Volatility::Volatile
        );
    }

    /// `clock_timestamp()` reports the type DataFusion's `now()` reports, so comparing the two
    /// needs no cast — which is the comparison anyone calling it is about to write.
    #[test]
    fn clock_timestamp_reports_the_type_now_reports() {
        assert_eq!(
            ClockTimestamp::default()
                .return_type(&[])
                .expect("a return type"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        );
    }
}
