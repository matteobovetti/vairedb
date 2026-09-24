//! The check behind `::json` and `::jsonb`, and the two UDFs that carry it.
//!
//! ## Why a validating function and not a cast
//!
//! Both types are **stored as text**: [`crate::proto`]-side there is no JSON logical type,
//! and `vairedb_coordinator::column_types` maps a `JSON`/`JSONB` column to Arrow `Utf8`,
//! because that is what a shard's DuckDB hands back and what a client is told the column is.
//! So `::json` changes no representation — it *checks* one, which is exactly what
//! PostgreSQL's `json_in` does and what Arrow has no cast for. DataFusion's
//! `convert_data_type` has no arm for the type name at all, so the cast does not even plan;
//! the read path rewrites it into a call of [`JSON_IN_UDF_NAME`] the same way it rewrites
//! `::bytea` into [`crate::bytea_in`], and for the same reason: a scalar function crosses
//! the Ballista wire as a name, so one implementation serves the coordinator that folds a
//! literal and the executor that decodes a column.
//!
//! ## What `::jsonb` does here, and what PostgreSQL does with it
//!
//! PostgreSQL's `jsonb` is a *parsed* representation, so a `::jsonb` cast normalizes:
//! insignificant whitespace goes, duplicate keys collapse to the last one, and object keys
//! come back sorted. Here `::jsonb` validates and keeps the text as written. That is a
//! deliberate choice rather than an oversight — a `JSONB` **column** already keeps the text
//! as written, because the value is stored by DuckDB and read back as text, so a normalizing
//! cast would disagree with the column it is usually written beside:
//!
//! ```text
//! INSERT INTO t (j) VALUES ('{"b":1,"a":2}');   -- a jsonb column: kept as written
//! SELECT '{"b":1,"a":2}'::jsonb;                -- PostgreSQL: {"a": 2, "b": 1}
//!                                               -- here:       {"b":1,"a":2}
//! ```
//!
//! The two spellings are still distinct in every way a client can act on: they validate the
//! same text, they carry their own name in the error message, and they keep their own column
//! label. What is missing is the normalization, and it is recorded as such in
//! `docs/specs/gap-analysis.md` rather than hidden here.

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::value::RawValue;

use crate::columns::strings;
use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// `x::json` — validate and keep.
pub const JSON_IN_UDF_NAME: &str = "vaire_json_in";
/// `x::jsonb` — the same check under its own name, so the error and the label say `jsonb`.
pub const JSONB_IN_UDF_NAME: &str = "vaire_jsonb_in";

/// Which of the two type names a message should say.
///
/// The only difference between them here — see the module doc — and it is not cosmetic: a
/// client that wrote `::jsonb` and is told its `json` is invalid has to guess which of the
/// two casts in its statement failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsonType {
    Json,
    Jsonb,
}

impl JsonType {
    /// The type name PostgreSQL prints, which is also the name of the UDF.
    fn type_name(self) -> &'static str {
        match self {
            JsonType::Json => "json",
            JsonType::Jsonb => "jsonb",
        }
    }

    fn udf_name(self) -> &'static str {
        match self {
            JsonType::Json => JSON_IN_UDF_NAME,
            JsonType::Jsonb => JSONB_IN_UDF_NAME,
        }
    }

    /// This type's `22P02`.
    ///
    /// Built here rather than at each `raise` site so that the cast and the accessors cannot
    /// drift over which of the two names the client is told.
    pub(super) fn invalid_input(self) -> JsonInputError {
        JsonInputError {
            type_name: self.type_name(),
        }
    }
}

/// Text that is not JSON — `22P02`, PostgreSQL's `invalid_text_representation`.
///
/// Without the offending text, as PostgreSQL's own `json_in` reports it: the value goes into
/// a `DETAIL` line PostgreSQL sends separately, and a JSON document is often large.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonInputError {
    pub type_name: &'static str,
}

impl fmt::Display for JsonInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid input syntax for type {}", self.type_name)
    }
}

impl std::error::Error for JsonInputError {}

impl JsonInputError {
    /// PostgreSQL's SQLSTATE for it, carried in the message by
    /// [`crate::error::tagged_message`] so it survives the Ballista boundary — the accessors
    /// run wherever the projection runs, and a `DataFusionError` variant does not cross.
    pub fn error_code(&self) -> VdbErrorCode {
        VdbErrorCode::InvalidTextRepresentation
    }
}

impl From<JsonInputError> for DataFusionError {
    fn from(e: JsonInputError) -> Self {
        DataFusionError::Execution(tagged_message(e.error_code(), e))
    }
}

/// Whether `text` is a JSON document PostgreSQL's `json_in` would accept.
///
/// The one implementation of the check, called by the cast's UDF and by every accessor
/// before it navigates.
pub fn validate(text: &str, json_type: JsonType) -> std::result::Result<(), JsonInputError> {
    match serde_json::from_str::<&RawValue>(text) {
        // `from_str` into a borrowed `RawValue` still parses the whole document, so this
        // rejects trailing garbage and unbalanced braces rather than only the first value.
        Ok(_) => Ok(()),
        Err(_) => Err(json_type.invalid_input()),
    }
}

/// Whether a type is one a json document can arrive as.
///
/// Every text layout, because coercion narrows all of them to `Utf8`, and `Null`, because a
/// bare `NULL` plans as that and has to stay a NULL rather than be refused. Shared with the
/// accessors, whose document argument is the same json this validates.
pub(super) fn is_text_input(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
    )
}

/// Register the two casts on `registry`.
pub(super) fn register_casts(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(json_in_udf(JsonType::Json))?;
    registry.register_udf(json_in_udf(JsonType::Jsonb))?;
    Ok(())
}

/// The shared [`ScalarUDF`] handle for one of the two casts.
///
/// One handle per type name for the whole process: a context is built per session and both
/// nodes register on every one of them, so the alternative is two `ScalarUDF` allocations
/// per session for two values that never differ.
fn json_in_udf(json_type: JsonType) -> Arc<ScalarUDF> {
    static JSON: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    static JSONB: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    let cell = match json_type {
        JsonType::Json => &JSON,
        JsonType::Jsonb => &JSONB,
    };
    Arc::clone(cell.get_or_init(|| Arc::new(ScalarUDF::from(JsonIn::new(json_type)))))
}

/// `vaire_json_in(text)` / `vaire_jsonb_in(text)` — PostgreSQL's `::json` cast of a string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonIn {
    json_type: JsonType,
    signature: Signature,
}

impl JsonIn {
    fn new(json_type: JsonType) -> Self {
        Self {
            json_type,
            // User-defined, so the accepted types are PostgreSQL's rather than Arrow's: a
            // built-in string signature would coerce an integer to text and make
            // `1::json` succeed, where PostgreSQL raises `42846`.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonIn {
    fn name(&self) -> &str {
        self.json_type.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A string, or nothing at all.
    ///
    /// PostgreSQL casts to `json` from `text` (and from the other JSON type, which is a
    /// string here too) and from no other type: `to_json(x)` is what turns a value into
    /// JSON, and it is a different question — `1::json` is `42846`, not `1`.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{} takes one argument, got {}",
                self.name(),
                arg_types.len()
            );
        };
        if !is_text_input(arg) {
            return plan_err!("cannot cast type {arg} to {}", self.json_type.type_name());
        }
        Ok(vec![DataType::Utf8])
    }

    /// `Utf8`, which is what this codebase stores both JSON types as — see the module doc.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!(
                "{} takes one argument, got {}",
                self.name(),
                args.args.len()
            );
        };
        // Against `number_rows`, so a batch of no rows stays one: a literal argument would
        // otherwise expand to a single row and raise for a statement that selected nothing.
        let text = strings(&arg.to_array(args.number_rows)?)?;
        for value in text.iter().flatten() {
            validate(value, self.json_type)?;
        }
        Ok(ColumnarValue::Array(Arc::new(text)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_pg::invoking::{invoke, text_column};

    /// `'…'::json`, or the message PostgreSQL would have printed.
    fn checked(text: &str, json_type: JsonType) -> std::result::Result<(), String> {
        validate(text, json_type).map_err(|e| e.to_string())
    }

    #[test]
    fn a_json_document_is_accepted_whatever_shape_it_is() {
        for text in [
            "{\"a\":1}",
            "[1,2,3]",
            "\"a string\"",
            "1",
            "-1.5e10",
            "true",
            "null",
            "  {\n \"a\" : [1, {}] }  ",
            "{}",
            "[]",
            // Duplicate keys are valid JSON, and PostgreSQL's `json` keeps both.
            "{\"a\":1,\"a\":2}",
        ] {
            assert_eq!(checked(text, JsonType::Json), Ok(()), "`{text}`");
        }
    }

    /// The whole document is parsed, not only its first value: trailing garbage is what a
    /// validator that stopped at the first complete value would let through.
    #[test]
    fn text_that_is_not_json_is_invalid_input_syntax() {
        for text in [
            "",
            "notjson",
            "{",
            "{\"a\":}",
            "{\"a\":1} trailing",
            "[1,2",
            "'single quoted'",
            "{a:1}",
            "01",
        ] {
            assert_eq!(
                checked(text, JsonType::Json),
                Err("invalid input syntax for type json".to_string()),
                "`{text}`"
            );
        }
    }

    /// The one difference between the two casts a client can see, and the reason there are
    /// two names: the message says which of them failed.
    #[test]
    fn the_two_type_names_report_themselves() {
        assert_eq!(
            checked("oops", JsonType::Jsonb),
            Err("invalid input syntax for type jsonb".to_string())
        );
    }

    /// The SQLSTATE has to survive the executor boundary, which it does by being written
    /// into the message rather than carried by the variant.
    #[test]
    fn the_error_carries_its_sqlstate_in_the_message() {
        let e = DataFusionError::from(JsonInputError { type_name: "json" });
        assert_eq!(
            crate::error::code_of_tagged_message(&e.to_string()),
            Some(VdbErrorCode::InvalidTextRepresentation)
        );
    }

    /// A whole column at once, with a NULL that must stay NULL rather than raise — the cast
    /// is strict, like PostgreSQL's, and a NULL never reaches the parser.
    #[test]
    fn a_column_is_validated_row_by_row_and_keeps_nulls() {
        let f = JsonIn::new(JsonType::Json);
        assert_eq!(
            invoke(
                &f,
                vec![text_column(vec![Some("{\"a\":1}"), None, Some("[]")])],
                3
            )
            .expect("valid"),
            vec![Some("{\"a\":1}".to_string()), None, Some("[]".to_string())]
        );
        let err = invoke(&f, vec![text_column(vec![Some("{}"), Some("oops")])], 2)
            .expect_err("one bad row fails the batch");
        assert!(
            err.to_string()
                .contains("invalid input syntax for type json"),
            "got: {err}"
        );
        // A batch of no rows raises nothing, so `SELECT s::json` over an empty table
        // answers no rows rather than failing on a folded literal.
        assert_eq!(
            invoke(&f, vec![text_column(vec![])], 0).expect("valid"),
            vec![]
        );
    }

    /// The types each function accepts, and the refusal for the rest in PostgreSQL's words.
    #[test]
    fn only_a_string_can_be_cast_to_json() {
        let f = JsonIn::new(JsonType::Jsonb);
        for accepted in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Null,
        ] {
            assert_eq!(
                f.coerce_types(std::slice::from_ref(&accepted))
                    .expect("accepted"),
                vec![DataType::Utf8],
                "{accepted}"
            );
        }
        let err = f
            .coerce_types(&[DataType::Int64])
            .expect_err("an integer has no cast to jsonb");
        assert!(
            err.to_string().contains("cannot cast type Int64 to jsonb"),
            "got: {err}"
        );
        // Reachable by writing the rewritten name out, which a client is free to do.
        let err = f
            .coerce_types(&[DataType::Utf8, DataType::Utf8])
            .expect_err("the cast takes one argument");
        assert!(
            err.to_string()
                .contains("vaire_jsonb_in takes one argument, got 2"),
            "got: {err}"
        );
        assert_eq!(
            f.return_type(&[DataType::Utf8]).expect("text"),
            DataType::Utf8
        );
    }
}
