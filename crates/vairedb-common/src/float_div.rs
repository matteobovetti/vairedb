//! PostgreSQL's floating-point division, which raises `22012` for a zero divisor where
//! IEEE 754 answers an infinity.
//!
//! `1.0::float8 / 0` is `inf` in Arrow and an error in PostgreSQL, and that divergence is
//! the worst-behaved kind this codebase has: no error, no warning, and a *poison* value
//! that every aggregate above it propagates, so one bad row turns a whole report into
//! plausible nonsense. It is also inconsistent with VaireDB's own answer for the same
//! statement at another type — `7 / 0` and `7.0 / 0` (a `numeric` here, since the read
//! path parses an unsuffixed decimal literal as one) already raise `22012` from Arrow's
//! integer and decimal kernels.
//!
//! So this is not a feature PostgreSQL has and DataFusion lacks; it is one operator whose
//! result differs at one input. The read path rewrites float division to a call of this
//! function ([`vairedb_coordinator::pgwire_handler::pg_float_division`]), and the rule
//! PostgreSQL applies lives here, once, in one place both the coordinator and the
//! executors resolve.
//!
//! ## Why a function and not a `CASE` guard
//!
//! The write path guards a zero divisor with `CASE WHEN d = 0 THEN error('division by
//! zero') ELSE n / d END`, because DuckDB's `error()` is the only way to raise from an
//! expression there. DataFusion has no `error()` at all, so the same shape would have to
//! raise by *dividing an integer by zero* in the `THEN` branch — a trick, and one that
//! only raises where the branch is not constant-folded first. It would also mention the
//! divisor twice, which makes a right-nested chain of divisions grow exponentially, and it
//! would need a second copy of the NaN rule below written in SQL.
//!
//! A function evaluates each operand once, states the rule as Rust, and needs no carve-out
//! for a nested divisor.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! `float8div`/`float4div` raise `division_by_zero` when the divisor is zero **and the
//! dividend is not NaN**, and are otherwise strict. Every branch below was run against
//! PostgreSQL rather than inferred from its source:
//!
//! | Input | PostgreSQL | Why it is not simply "divisor = 0 raises" |
//! |---|---|---|
//! | `1.0::float8 / 0` | `22012` | the case this module exists for |
//! | `0::float8 / 0` | `22012` | IEEE would answer NaN |
//! | `'inf'::float8 / 0` | `22012` | an infinite dividend is still an error |
//! | `1.0::float8 / -0.0` | `22012` | negative zero is zero |
//! | `'nan'::float8 / 0` | **`NaN`** | the carve-out; NaN propagates instead of raising |
//! | `null::float8 / 0` | **NULL** | strict, so a NULL dividend never reaches the check |
//! | `f / 0` over an **empty** table | no rows, no error | per row, not per statement |
//!
//! The last two rows are why the check is a scan over the pair of arrays rather than a
//! test of the divisor alone: a row whose dividend is NULL or NaN does not raise even
//! though its divisor is zero, and a batch with no rows raises nothing at all. A statement
//! whose operands are *both* constant is folded by DataFusion's simplifier before
//! execution and therefore raises at planning time — which is also what PostgreSQL does
//! with `SELECT 1.0::float8 / 0 WHERE false`.
//!
//! ## Why the message is PostgreSQL's wording
//!
//! `division by zero` is what [`vairedb_common::error`] and the coordinator's error
//! enrichment already classify into `22012`, in prose and in the `Debug` rendering a
//! Ballista executor's failure arrives as. Raising it with those words means a zero
//! divisor reported from an executor reaches the client as the same SQLSTATE as one the
//! coordinator evaluated itself, without the structured error code § 1.3 of the gap
//! analysis is still missing.

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::kernels::numeric::div;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// The name the read path emits and every node resolves the call by.
///
/// Prefixed, and not spelled as an operator or as a PostgreSQL function name, because it
/// is VaireDB's own: nothing a client writes produces this name, and shadowing a
/// PostgreSQL name with different behaviour is what this module exists to avoid.
pub const FLOAT_DIV_UDF_NAME: &str = "vaire_float_div";

/// Register PostgreSQL's float division on `registry`.
///
/// Call this on every context that plans **or** executes a read. A scalar function
/// crosses the Ballista wire as a name with no definition attached, so one registered
/// only where the query is planned fails on the scheduler that decodes the logical plan
/// and on the executor that decodes the stage — see [`crate::pg_udf`], which is the same
/// invariant found the hard way.
pub fn register_float_division(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(float_division_udf())?;
    Ok(())
}

/// The shared [`ScalarUDF`] handle, for the read-path rewrite that builds the call.
///
/// One instance, because [`ScalarUDF::call`] clones it into every expression it builds and
/// the function holds nothing but its signature.
pub fn float_division_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(FloatDivision::new()))))
}

/// `vaire_float_div(dividend, divisor)` — `dividend / divisor` for `float4` and `float8`,
/// raising `division by zero` where PostgreSQL raises it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FloatDivision {
    signature: Signature,
}

impl Default for FloatDivision {
    fn default() -> Self {
        Self::new()
    }
}

impl FloatDivision {
    pub fn new() -> Self {
        Self {
            // Uniform, so both arguments arrive at the same width and the result is that
            // width: `float4 / float4` stays `real` and does not silently widen to
            // `double precision`. The caller has already cast both operands to the type
            // DataFusion's own coercion would have divided them at, so this signature
            // accepts what it is given rather than choosing for it.
            signature: Signature::uniform(
                2,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FloatDivision {
    fn name(&self) -> &str {
        FLOAT_DIV_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The argument width, which the signature has already made the same for both.
    ///
    /// This is the type the plain `/` would have produced, so replacing the operator with
    /// this call cannot change the OID a driver binds its receive buffer from.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Float64, DataType::Float64] => Ok(DataType::Float64),
            [DataType::Float32, DataType::Float32] => Ok(DataType::Float32),
            other => {
                exec_err!("{FLOAT_DIV_UDF_NAME} divides two floats of one width, got {other:?}")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [dividend, divisor] = args.args.as_slice() else {
            return exec_err!(
                "{FLOAT_DIV_UDF_NAME} takes a dividend and a divisor, got {} arguments",
                args.args.len()
            );
        };
        // Materialized against `number_rows` rather than with `values_to_arrays`, because
        // a batch of no rows has to stay a batch of no rows: two literal operands would
        // otherwise expand to one row each and raise for a statement that selected
        // nothing. `SELECT f / 0 FROM <empty>` answers no rows in PostgreSQL.
        let dividend = dividend.to_array(args.number_rows)?;
        let divisor = divisor.to_array(args.number_rows)?;

        reject_zero_divisor(&dividend, &divisor)?;

        // Arrow's own kernel for every row that survives the check, so the value a client
        // gets is bit-for-bit the one the plain `/` gave before this rewrite existed —
        // including the infinities and NaNs IEEE 754 produces for inputs that are not a
        // zero divisor.
        Ok(ColumnarValue::Array(div(&dividend, &divisor)?))
    }
}

/// Raise `division by zero` if any row divides a non-NULL, non-NaN dividend by zero.
fn reject_zero_divisor(dividend: &ArrayRef, divisor: &ArrayRef) -> Result<()> {
    let raises = match (dividend.data_type(), divisor.data_type()) {
        (DataType::Float64, DataType::Float64) => divides_by_zero::<Float64Type>(dividend, divisor),
        (DataType::Float32, DataType::Float32) => divides_by_zero::<Float32Type>(dividend, divisor),
        // Unreachable through the signature, which coerces both arguments to one float
        // width. Reported rather than asserted, since a panic in an executor loses the
        // statement instead of failing it.
        (dividend, divisor) => {
            return exec_err!(
                "{FLOAT_DIV_UDF_NAME} divides two floats of one width, got {dividend} / {divisor}"
            );
        }
    };
    if raises {
        return exec_err!("division by zero");
    }
    Ok(())
}

/// Whether one row of the pair is PostgreSQL's error case: a zero divisor under a
/// dividend that is neither NULL nor NaN.
///
/// `T::Native: Into<f64>` covers both float widths with one comparison, and widening
/// `f32` to `f64` is exact — so neither the zero test nor the NaN test can change answer
/// on the way.
fn divides_by_zero<T: ArrowPrimitiveType>(dividend: &ArrayRef, divisor: &ArrayRef) -> bool
where
    T::Native: Into<f64>,
{
    let dividend = dividend.as_primitive::<T>();
    let divisor = divisor.as_primitive::<T>();
    dividend
        .iter()
        .zip(divisor.iter())
        .any(|(dividend, divisor)| match (dividend, divisor) {
            // `== 0.0` and not `is_zero`, because IEEE's negative zero is a zero divisor
            // to PostgreSQL too — `1.0::float8 / -0.0` raises.
            (Some(dividend), Some(divisor)) => divisor.into() == 0.0 && !dividend.into().is_nan(),
            // A NULL on either side is PostgreSQL's strict NULL, not an error.
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array};
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    /// Invoke the function the way a physical expression does, over whole columns.
    fn divide(dividend: Vec<Option<f64>>, divisor: Vec<Option<f64>>) -> Result<Vec<Option<f64>>> {
        let rows = dividend.len();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(dividend))),
                ColumnarValue::Array(Arc::new(Float64Array::from(divisor))),
            ],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(arrow::datatypes::Field::new("d", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = FloatDivision::new().invoke_with_args(args)?;
        let out = out.to_array(rows)?;
        Ok(out.as_primitive::<Float64Type>().iter().collect())
    }

    /// The gap itself: the answer used to be `inf`.
    #[test]
    fn a_zero_divisor_raises_instead_of_answering_an_infinity() {
        let err = divide(vec![Some(1.0)], vec![Some(0.0)]).expect_err("should have raised");
        assert!(
            err.to_string().to_lowercase().contains("division by zero"),
            "the message is what classifies as 22012: {err}"
        );
    }

    /// Measured against PostgreSQL 17: negative zero is a zero divisor.
    #[test]
    fn a_negative_zero_divisor_raises() {
        assert!(divide(vec![Some(1.0)], vec![Some(-0.0)]).is_err());
    }

    /// Measured against PostgreSQL 17: `0::float8 / 0` raises rather than answering NaN,
    /// and an infinite dividend raises too.
    #[test]
    fn a_zero_or_infinite_dividend_over_zero_raises() {
        assert!(divide(vec![Some(0.0)], vec![Some(0.0)]).is_err());
        assert!(divide(vec![Some(f64::INFINITY)], vec![Some(0.0)]).is_err());
    }

    /// Measured against PostgreSQL 17, and the one carve-out: `'nan'::float8 / 0` is NaN.
    #[test]
    fn a_nan_dividend_over_zero_answers_nan() {
        let answer = divide(vec![Some(f64::NAN)], vec![Some(0.0)]).expect("should not raise");
        assert!(answer[0].expect("not null").is_nan());
    }

    /// Measured against PostgreSQL 17: division is strict, so a NULL dividend over a zero
    /// divisor is NULL and not an error.
    #[test]
    fn a_null_operand_is_null_and_not_an_error() {
        assert_eq!(
            divide(vec![None, Some(1.0)], vec![Some(0.0), None]).expect("should not raise"),
            vec![None, None]
        );
    }

    /// Per row, not per statement: the zero divisor in one row raises for the whole
    /// batch, and a batch without one answers every row.
    #[test]
    fn only_a_batch_holding_a_zero_divisor_raises() {
        assert_eq!(
            divide(vec![Some(1.0), Some(3.0)], vec![Some(2.0), Some(-2.0)])
                .expect("should not raise"),
            vec![Some(0.5), Some(-1.5)]
        );
        assert!(divide(vec![Some(1.0), Some(3.0)], vec![Some(2.0), Some(0.0)]).is_err());
    }

    /// A batch of no rows raises nothing, which is what makes `SELECT f / 0` over an
    /// empty table answer no rows the way PostgreSQL does.
    #[test]
    fn an_empty_batch_raises_nothing() {
        assert_eq!(divide(vec![], vec![]).expect("should not raise"), vec![]);
    }

    /// Two scalars still divide, and still raise — this is the shape DataFusion's
    /// simplifier evaluates when it folds a constant expression.
    #[test]
    fn two_scalar_operands_are_divided_and_checked() {
        let scalars = |dividend: f64, divisor: f64| ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(dividend))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(divisor))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(arrow::datatypes::Field::new("d", DataType::Float64, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let answer = FloatDivision::new()
            .invoke_with_args(scalars(3.0, 2.0))
            .expect("should not raise")
            .to_array(1)
            .expect("one row");
        assert_eq!(answer.as_primitive::<Float64Type>().value(0), 1.5);
        assert!(
            FloatDivision::new()
                .invoke_with_args(scalars(3.0, 0.0))
                .is_err()
        );
    }

    /// `float4` keeps its own width, so the rewrite cannot turn a `real` column into a
    /// `double precision` one — and the zero check reaches it too.
    #[test]
    fn float4_divides_at_float4_and_is_checked() {
        let args = |divisor: f32| ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![Some(1.0f32)]))),
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![Some(divisor)]))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(arrow::datatypes::Field::new("d", DataType::Float32, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let answer = FloatDivision::new()
            .invoke_with_args(args(2.0))
            .expect("should not raise")
            .to_array(1)
            .expect("one row");
        assert_eq!(answer.data_type(), &DataType::Float32);
        assert_eq!(answer.as_primitive::<Float32Type>().value(0), 0.5);
        assert!(FloatDivision::new().invoke_with_args(args(0.0)).is_err());
    }

    /// The declared type is the argument type at both widths, since that is the type the
    /// operator it replaces would have produced.
    #[test]
    fn the_result_type_is_the_argument_type() {
        let f = FloatDivision::new();
        assert_eq!(
            f.return_type(&[DataType::Float64, DataType::Float64])
                .expect("float8"),
            DataType::Float64
        );
        assert_eq!(
            f.return_type(&[DataType::Float32, DataType::Float32])
                .expect("float4"),
            DataType::Float32
        );
    }

    /// The wire carries only the name, so the name has to resolve after registration.
    #[test]
    fn the_function_resolves_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        assert!(ctx.udf(FLOAT_DIV_UDF_NAME).is_err());
        register_float_division(&mut ctx).expect("registration failed");
        assert!(ctx.udf(FLOAT_DIV_UDF_NAME).is_ok());
    }

    /// Registering twice is what a context reached by two registration paths does.
    #[test]
    fn registering_twice_is_idempotent() {
        let mut ctx = SessionContext::new();
        register_float_division(&mut ctx).expect("first registration failed");
        register_float_division(&mut ctx).expect("second registration failed");
        assert!(ctx.udf(FLOAT_DIV_UDF_NAME).is_ok());
    }
}
