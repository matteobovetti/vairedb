//! `isfinite(date | timestamp | interval)`.
//!
//! Arrow has no representation for `infinity` in any of the three types, so every non-null
//! value VaireDB can hold is finite and the answer is `true`. That is not a stub: a client
//! guarding a comparison with `WHERE isfinite(ts)` gets the right answer for every row that
//! exists, and the only divergence is that VaireDB cannot *store* the infinite value whose
//! absence it would be reporting. The function is here so the guard plans at all.
//!
//! The rule this file owns is therefore a narrow one, and it is the exception: the text of a
//! literal that has not been parsed into a timestamp yet still *says* `infinity`, and
//! PostgreSQL answers `isfinite('infinity')` with false. Reading the text is what makes the
//! function agree with PostgreSQL rather than being a constant `true`.

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, BooleanBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::shared;
use crate::error::tagged_message;
use crate::pg_typeof::pg_type_name;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name PostgreSQL uses, so a client's SQL needs no rewriting.
pub const ISFINITE_UDF_NAME: &str = "isfinite";

/// The shared `isfinite` instance.
pub fn isfinite_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    shared(&UDF, IsFinite::default)
}

/// `isfinite(date | timestamp | interval)`; see the module doc for why the answer is what it
/// is.
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
        let rendered = match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Some(crate::pg_format::render_column(&array)?)
            }
            _ => None,
        };

        let mut out = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if array.is_null(row) {
                out.append_null();
                continue;
            }
            match &rendered {
                Some(values) => {
                    let value = values[row].as_deref().unwrap_or_default();
                    out.append_value(!says_infinity(value));
                }
                None => out.append_value(true),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// The four spellings of an infinite literal PostgreSQL's datetime input accepts, in either
/// case and either sign.
fn says_infinity(text: &str) -> bool {
    let text = text.trim();
    text.eq_ignore_ascii_case("infinity")
        || text.eq_ignore_ascii_case("-infinity")
        || text.eq_ignore_ascii_case("inf")
        || text.eq_ignore_ascii_case("-inf")
}

/// The types PostgreSQL has an `isfinite` for, plus the text of an unparsed literal.
///
/// Deliberately not written in terms of the set `age` accepts, which it strictly contains:
/// the two sets are two independent facts about PostgreSQL's catalogue — `isfinite(interval)`
/// exists and `age(interval, interval)` does not — and deriving one from the other would make
/// a future correction to either silently change both.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use crate::pg_datetime::test_support::{invoke, micros};
    use arrow::array::{ArrayRef, BooleanArray, StringArray, TimestampMicrosecondArray};

    fn run_isfinite(array: ArrayRef) -> Vec<Option<bool>> {
        let rows = array.len();
        let out = invoke(&IsFinite::default(), vec![array], DataType::Boolean);
        let values = out
            .as_any()
            .downcast_ref::<BooleanArray>()
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
}
