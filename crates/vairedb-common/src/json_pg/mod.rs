//! PostgreSQL's `json` and `jsonb`: the input conversion behind `::json` and `::jsonb`, and
//! the four accessors `->`, `->>`, `#>` and `#>>`.
//!
//! ```text
//! '{"a":1}'::json          PostgreSQL  {"a":1}    before  0A000 Unsupported SQL type JSON
//! 'notjson'::json          PostgreSQL  22P02      before  0A000 Unsupported SQL type JSON
//! '{"a":{"b":2}}'::json -> 'a'   PostgreSQL  {"b":2}   before  0A000, and unreachable
//!                                                              while the cast fails
//! ```
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
//!
//! ## The accessors
//!
//! `->` and `#>` return **json**, `->>` and `#>>` return **text** — which over a text-typed
//! json is the same Arrow type and a different value: the `_text` pair unwraps a JSON string
//! to its characters (`"x"` → `x`) and answers SQL NULL for a JSON `null`, where the other
//! pair returns the four characters `null`.
//!
//! Extraction is by [`RawValue`], so a returned object or array is the client's **own
//! text**, byte for byte: key order, spacing and number spelling are not rewritten on the
//! way out, which is what PostgreSQL's `json` does too. Only the `_text` forms decode, and
//! only the one string they return.
//!
//! A key that is not there is SQL NULL, and so is an index past the end (negative indexes
//! count from it, as in PostgreSQL). A value whose *shape* cannot answer the question — a
//! field of an array, an index into an object, anything at all of a scalar — is also NULL,
//! which is `jsonb`'s reading of it; PostgreSQL's `json` raises for some of those cases and
//! is the narrower of the two behaviours. Text that is not JSON at all is `22P02`, since
//! that is the client's own error rather than a missing value.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray, StringArray, StringBuilder};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::value::RawValue;

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// `x::json` — validate and keep.
pub const JSON_IN_UDF_NAME: &str = "vaire_json_in";
/// `x::jsonb` — the same check under its own name, so the error and the label say `jsonb`.
pub const JSONB_IN_UDF_NAME: &str = "vaire_jsonb_in";
/// `j -> k` — a field or an element, as json.
pub const JSON_GET_UDF_NAME: &str = "vaire_json_get";
/// `j ->> k` — a field or an element, as text.
pub const JSON_GET_TEXT_UDF_NAME: &str = "vaire_json_get_text";
/// `j #> p` — the value at a path, as json.
pub const JSON_PATH_UDF_NAME: &str = "vaire_json_path";
/// `j #>> p` — the value at a path, as text.
pub const JSON_PATH_TEXT_UDF_NAME: &str = "vaire_json_path_text";

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
        Err(_) => Err(JsonInputError {
            type_name: json_type.type_name(),
        }),
    }
}

/// What a JSON document is, read from its first character alone.
///
/// Enough to decide whether a question is answerable: an object answers a key, an array
/// answers an index, and a scalar answers neither. Reading the character rather than parsing
/// is what keeps a scalar from being deserialized into a container type only to fail.
fn shape(text: &str) -> Option<Shape> {
    match text.trim_start().as_bytes().first()? {
        b'{' => Some(Shape::Object),
        b'[' => Some(Shape::Array),
        _ => Some(Shape::Scalar),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Object,
    Array,
    Scalar,
}

/// The field of `doc` called `key`, as its own verbatim text.
///
/// `None` for a document that is not an object and for a key it does not hold — the two are
/// the same answer to a client, SQL NULL, and PostgreSQL's `jsonb` does not distinguish
/// them either. Duplicate keys resolve to the last, which is what `jsonb` keeps.
fn field(
    doc: &str,
    key: &str,
    json_type: JsonType,
) -> std::result::Result<Option<String>, JsonInputError> {
    let invalid = || JsonInputError {
        type_name: json_type.type_name(),
    };
    match shape(doc).ok_or_else(invalid)? {
        Shape::Object => {
            let members: HashMap<String, &RawValue> =
                serde_json::from_str(doc).map_err(|_| invalid())?;
            Ok(members.get(key).map(|value| value.get().to_string()))
        }
        // Still validated, so `'oops' -> 'a'` is an error and not a NULL: the text is the
        // client's, and a silent NULL would hide a typo in it.
        _ => {
            validate(doc, json_type)?;
            Ok(None)
        }
    }
}

/// The element of `doc` at `index`, as its own verbatim text.
///
/// A negative index counts from the end, as in PostgreSQL: `-1` is the last element.
fn element(
    doc: &str,
    index: i64,
    json_type: JsonType,
) -> std::result::Result<Option<String>, JsonInputError> {
    let invalid = || JsonInputError {
        type_name: json_type.type_name(),
    };
    match shape(doc).ok_or_else(invalid)? {
        Shape::Array => {
            let elements: Vec<&RawValue> = serde_json::from_str(doc).map_err(|_| invalid())?;
            let position = if index < 0 {
                match elements.len().checked_sub(index.unsigned_abs() as usize) {
                    Some(position) => position,
                    // Further back than the array is long, which is past its start.
                    None => return Ok(None),
                }
            } else {
                index as usize
            };
            Ok(elements.get(position).map(|value| value.get().to_string()))
        }
        _ => {
            validate(doc, json_type)?;
            Ok(None)
        }
    }
}

/// The value of `doc` at `path`, as its own verbatim text.
///
/// Each step is a key or, over an array, an index — which is what PostgreSQL does with a
/// `text[]` path over a mixed document. An empty path is the document itself, as in
/// PostgreSQL.
fn at_path(
    doc: &str,
    path: &[String],
    json_type: JsonType,
) -> std::result::Result<Option<String>, JsonInputError> {
    let mut current = doc.to_string();
    for step in path {
        let next = match shape(&current) {
            Some(Shape::Array) => match step.parse::<i64>() {
                Ok(index) => element(&current, index, json_type)?,
                // PostgreSQL answers NULL for a non-integer step into an array rather than
                // raising: the path is data, not syntax.
                Err(_) => None,
            },
            _ => field(&current, step, json_type)?,
        };
        match next {
            Some(next) => current = next,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// The `->>` reading of an extracted value: a JSON string becomes its characters, a JSON
/// `null` becomes SQL NULL, and everything else keeps its own text.
fn as_text(raw: Option<String>) -> Option<String> {
    let raw = raw?;
    match raw.trim_start().as_bytes().first() {
        Some(b'"') => serde_json::from_str::<String>(&raw).ok(),
        _ if raw.trim() == "null" => None,
        _ => Some(raw),
    }
}

/// Read PostgreSQL's `text[]` path, in either spelling a client can write it.
///
/// `#>` takes a `text[]`, and the two ways to write one reach here as different Arrow types:
/// `ARRAY['a','b']` is a `List(Utf8)` and `'{a,b}'` is the `Utf8` PostgreSQL's array input
/// syntax — which is what a client writes far more often, and which nothing coerces to a
/// list here because the operator's other operand is a string too.
fn path_of_text(text: &str) -> Vec<String> {
    let body = text
        .trim()
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .unwrap_or(text.trim());
    if body.is_empty() {
        return vec![];
    }
    body.split(',')
        .map(|step| {
            let step = step.trim();
            step.strip_prefix('"')
                .and_then(|step| step.strip_suffix('"'))
                .unwrap_or(step)
                .to_string()
        })
        .collect()
}

/// Register the `json` input conversion and the four accessors on `registry`.
///
/// Call this on every context that plans **or** executes a read: the read path rewrites
/// `::json` and each operator into a call, and only the name crosses the wire.
pub fn register_json_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(json_in_udf(JsonType::Json))?;
    registry.register_udf(json_in_udf(JsonType::Jsonb))?;
    for accessor in [
        Accessor::Field { text: false },
        Accessor::Field { text: true },
        Accessor::Path { text: false },
        Accessor::Path { text: true },
    ] {
        registry.register_udf(Arc::new(ScalarUDF::from(JsonAccessor::new(accessor))))?;
    }
    Ok(())
}

/// The shared [`ScalarUDF`] handle for one of the two casts.
pub fn json_in_udf(json_type: JsonType) -> Arc<ScalarUDF> {
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
pub struct JsonIn {
    json_type: JsonType,
    signature: Signature,
}

impl JsonIn {
    pub fn new(json_type: JsonType) -> Self {
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
        match arg {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(vec![DataType::Utf8])
            }
            other => plan_err!("cannot cast type {other} to {}", self.json_type.type_name()),
        }
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

/// Which accessor a [`JsonAccessor`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Accessor {
    /// `->` and `->>`: one key or one index.
    Field { text: bool },
    /// `#>` and `#>>`: a path of them.
    Path { text: bool },
}

impl Accessor {
    fn udf_name(self) -> &'static str {
        match self {
            Accessor::Field { text: false } => JSON_GET_UDF_NAME,
            Accessor::Field { text: true } => JSON_GET_TEXT_UDF_NAME,
            Accessor::Path { text: false } => JSON_PATH_UDF_NAME,
            Accessor::Path { text: true } => JSON_PATH_TEXT_UDF_NAME,
        }
    }

    /// Whether the result is the `->>` reading — text rather than json.
    fn is_text(self) -> bool {
        matches!(
            self,
            Accessor::Field { text: true } | Accessor::Path { text: true }
        )
    }
}

/// `vaire_json_get(json, key)` and the three operators beside it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonAccessor {
    accessor: Accessor,
    signature: Signature,
}

impl JsonAccessor {
    fn new(accessor: Accessor) -> Self {
        Self {
            accessor,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonAccessor {
    fn name(&self) -> &str {
        self.accessor.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A json document, and a key that is a string, an integer or a path.
    ///
    /// An integer only for `->`/`->>`: PostgreSQL's `#>` takes a `text[]`, whose steps are
    /// strings even where they index an array.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [doc, key] = arg_types else {
            return plan_err!(
                "{} takes two arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        if !matches!(
            doc,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            return plan_err!("operator does not exist: {doc} {}", self.operator());
        }
        let key = match (self.accessor, key) {
            (_, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null) => {
                DataType::Utf8
            }
            (
                Accessor::Field { .. },
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64,
            ) => DataType::Int64,
            // `ARRAY['a','b']`, the other spelling of a `text[]` path. Kept as the list it
            // is rather than cast to text: Arrow has no `List` → `Utf8` cast, so asking for
            // one here would refuse the spelling at planning time instead of reading it.
            (Accessor::Path { .. }, list @ (DataType::List(_) | DataType::LargeList(_))) => {
                list.clone()
            }
            (_, other) => {
                return plan_err!("operator does not exist: json {} {other}", self.operator());
            }
        };
        Ok(vec![DataType::Utf8, key])
    }

    /// `Utf8` for all four: json is text here, and so is text.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [doc, key] = args.args.as_slice() else {
            return exec_err!(
                "{} takes two arguments, got {}",
                self.name(),
                args.args.len()
            );
        };
        let doc = doc.to_array(args.number_rows)?;
        let key = key.to_array(args.number_rows)?;
        let docs = strings(&doc)?;
        let mut out = StringBuilder::with_capacity(docs.len(), docs.value_data().len());
        // The integer form of `->`, which is the only one whose key is not a string.
        if matches!(key.data_type(), DataType::Int64) {
            let indexes = key.as_primitive::<Int64Type>();
            for row in 0..docs.len() {
                if docs.is_null(row) || indexes.is_null(row) {
                    out.append_null();
                    continue;
                }
                let found = element(docs.value(row), indexes.value(row), JsonType::Json)?;
                append(&mut out, found, self.accessor.is_text());
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        // The `ARRAY['a','b']` spelling of a path, whose steps arrive as a list per row.
        if let DataType::List(_) | DataType::LargeList(_) = key.data_type() {
            for row in 0..docs.len() {
                if docs.is_null(row) || key.is_null(row) {
                    out.append_null();
                    continue;
                }
                let steps = strings(&list_row(&key, row)?)?;
                // A NULL step cannot be matched by any key, so the whole path misses — the
                // same answer PostgreSQL gives `#>` over an array holding a NULL.
                let Some(steps) = steps
                    .iter()
                    .map(|step| step.map(str::to_string))
                    .collect::<Option<Vec<String>>>()
                else {
                    out.append_null();
                    continue;
                };
                let found = at_path(docs.value(row), &steps, JsonType::Json)?;
                append(&mut out, found, self.accessor.is_text());
            }
            return Ok(ColumnarValue::Array(Arc::new(out.finish())));
        }
        let keys = strings(&key)?;
        for row in 0..docs.len() {
            if docs.is_null(row) || keys.is_null(row) {
                out.append_null();
                continue;
            }
            let (doc, key) = (docs.value(row), keys.value(row));
            let found = match self.accessor {
                Accessor::Field { .. } => field(doc, key, JsonType::Json)?,
                Accessor::Path { .. } => at_path(doc, &path_of_text(key), JsonType::Json)?,
            };
            append(&mut out, found, self.accessor.is_text());
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// One row of a list column, as an array of its elements.
fn list_row(list: &ArrayRef, row: usize) -> Result<ArrayRef> {
    match list.data_type() {
        DataType::List(_) => Ok(list.as_list::<i32>().value(row)),
        DataType::LargeList(_) => Ok(list.as_list::<i64>().value(row)),
        other => exec_err!("a #> path must be a text array, got {other}"),
    }
}

impl JsonAccessor {
    /// The operator this stands for, for a message that names what the client wrote rather
    /// than what it was rewritten into.
    fn operator(&self) -> &'static str {
        match self.accessor {
            Accessor::Field { text: false } => "->",
            Accessor::Field { text: true } => "->>",
            Accessor::Path { text: false } => "#>",
            Accessor::Path { text: true } => "#>>",
        }
    }
}

/// Append one extracted value, in the reading the accessor asks for.
fn append(out: &mut StringBuilder, found: Option<String>, is_text: bool) {
    let value = if is_text { as_text(found) } else { found };
    match value {
        Some(value) => out.append_value(value),
        None => out.append_null(),
    }
}

/// One argument as a `Utf8` array.
///
/// `cast` rather than a downcast, so a `Utf8View` handed over despite [`coerce_types`] still
/// reads — the same guard [`crate::bytea_in`] keeps.
fn strings(array: &ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().clone()),
        _ => Ok(cast(array, &DataType::Utf8)?.as_string::<i32>().clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::execution::context::SessionContext;

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
        assert_eq!(
            JsonInputError { type_name: "json" }.error_code(),
            VdbErrorCode::InvalidTextRepresentation
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

    fn got(doc: &str, key: &str) -> Option<String> {
        field(doc, key, JsonType::Json).expect("valid json")
    }

    /// `->` returns the field's **own text**: key order and spacing inside it are the
    /// client's, which is what PostgreSQL's `json` does.
    #[test]
    fn a_field_comes_back_verbatim() {
        assert_eq!(got("{\"a\":1}", "a"), Some("1".to_string()));
        assert_eq!(
            got("{\"a\":{\"c\":1,\"b\":2}}", "a"),
            Some("{\"c\":1,\"b\":2}".to_string())
        );
        assert_eq!(got("{\"a\":\"x\"}", "a"), Some("\"x\"".to_string()));
        assert_eq!(got("{\"a\":null}", "a"), Some("null".to_string()));
        // A key that is not there, and a document that has no keys at all.
        assert_eq!(got("{\"a\":1}", "b"), None);
        assert_eq!(got("[1,2]", "a"), None);
        assert_eq!(got("1", "a"), None);
        // Duplicates resolve to the last, which is the one `jsonb` keeps.
        assert_eq!(got("{\"a\":1,\"a\":2}", "a"), Some("2".to_string()));
    }

    #[test]
    fn an_index_counts_from_either_end() {
        let at = |doc: &str, index: i64| element(doc, index, JsonType::Json).expect("valid json");
        assert_eq!(at("[10,20,30]", 0), Some("10".to_string()));
        assert_eq!(at("[10,20,30]", 2), Some("30".to_string()));
        assert_eq!(at("[10,20,30]", -1), Some("30".to_string()));
        assert_eq!(at("[10,20,30]", -3), Some("10".to_string()));
        assert_eq!(at("[10,20,30]", 3), None);
        assert_eq!(at("[10,20,30]", -4), None);
        assert_eq!(at("{\"a\":1}", 0), None);
    }

    /// A shape that cannot answer is NULL, but text that is not JSON is still an error: the
    /// first is a missing value and the second is a mistake in the client's own literal.
    #[test]
    fn a_broken_document_raises_where_a_missing_key_does_not() {
        assert!(field("oops", "a", JsonType::Json).is_err());
        assert!(element("oops", 0, JsonType::Json).is_err());
        assert!(at_path("oops", &["a".to_string()], JsonType::Json).is_err());
        assert_eq!(got("{\"a\":1}", "zzz"), None);
    }

    #[test]
    fn a_path_walks_objects_and_arrays_alike() {
        let walk = |doc: &str, path: &str| {
            at_path(doc, &path_of_text(path), JsonType::Json).expect("valid json")
        };
        let doc = "{\"a\":{\"b\":[10,{\"c\":\"deep\"}]}}";
        assert_eq!(walk(doc, "{a,b,1,c}"), Some("\"deep\"".to_string()));
        assert_eq!(walk(doc, "{a,b,0}"), Some("10".to_string()));
        assert_eq!(walk(doc, "{a,b,-1,c}"), Some("\"deep\"".to_string()));
        assert_eq!(walk(doc, "{a,zzz}"), None);
        assert_eq!(walk(doc, "{a,b,9}"), None);
        // A non-integer step into an array is a miss, not an error.
        assert_eq!(walk(doc, "{a,b,x}"), None);
        // An empty path is the document itself.
        assert_eq!(walk("{\"a\":1}", "{}"), Some("{\"a\":1}".to_string()));
    }

    /// Both spellings of a `text[]`, since the string form is the one a client writes.
    #[test]
    fn a_path_is_read_in_postgresqls_array_syntax() {
        assert_eq!(
            path_of_text("{a,b}"),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            path_of_text(" { a , b } "),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            path_of_text("{\"a b\",c}"),
            vec!["a b".to_string(), "c".to_string()]
        );
        assert_eq!(path_of_text("{}"), Vec::<String>::new());
        // A bare step, which is what a single-element `ARRAY['a']` renders as once cast.
        assert_eq!(path_of_text("a"), vec!["a".to_string()]);
    }

    /// The `->>` reading: a string loses its quotes, a `null` becomes SQL NULL, everything
    /// else keeps its json text.
    #[test]
    fn the_text_forms_unwrap_a_string_and_null_a_json_null() {
        assert_eq!(as_text(Some("\"x\"".to_string())), Some("x".to_string()));
        assert_eq!(
            as_text(Some("\"a\\nb\"".to_string())),
            Some("a\nb".to_string())
        );
        assert_eq!(as_text(Some("1".to_string())), Some("1".to_string()));
        assert_eq!(
            as_text(Some("{\"a\":1}".to_string())),
            Some("{\"a\":1}".to_string())
        );
        assert_eq!(as_text(Some("null".to_string())), None);
        assert_eq!(as_text(None), None);
    }

    /// Invoke a UDF the way a physical expression does, over a whole column.
    fn invoke(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        rows: usize,
    ) -> Result<Vec<Option<String>>> {
        let out = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(arrow::datatypes::Field::new("v", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })?
            .to_array(rows)?;
        Ok(out
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect())
    }

    fn text_column(values: Vec<Option<&str>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(values)))
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

    #[test]
    fn the_accessors_answer_over_a_column() {
        let doc = || text_column(vec![Some("{\"a\":{\"b\":\"x\"}}"), Some("{}"), None]);
        assert_eq!(
            invoke(
                &JsonAccessor::new(Accessor::Field { text: false }),
                vec![doc(), text_column(vec![Some("a"), Some("a"), Some("a")])],
                3
            )
            .expect("valid"),
            vec![Some("{\"b\":\"x\"}".to_string()), None, None]
        );
        assert_eq!(
            invoke(
                &JsonAccessor::new(Accessor::Path { text: true }),
                vec![doc(), text_column(vec![Some("{a,b}"), Some("{a,b}"), None])],
                3
            )
            .expect("valid"),
            vec![Some("x".to_string()), None, None]
        );
        assert_eq!(
            invoke(
                &JsonAccessor::new(Accessor::Field { text: true }),
                vec![
                    text_column(vec![Some("[\"first\",2]"), Some("[\"first\",2]")]),
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(0), Some(9)]))),
                ],
                2
            )
            .expect("valid"),
            vec![Some("first".to_string()), None]
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
        assert_eq!(
            f.return_type(&[DataType::Utf8]).expect("text"),
            DataType::Utf8
        );
    }

    #[test]
    fn an_index_is_only_a_key_for_the_arrow_operators() {
        let arrow = JsonAccessor::new(Accessor::Field { text: false });
        assert_eq!(
            arrow
                .coerce_types(&[DataType::Utf8, DataType::Int32])
                .expect("an index is a key for ->"),
            vec![DataType::Utf8, DataType::Int64]
        );
        let path = JsonAccessor::new(Accessor::Path { text: false });
        let err = path
            .coerce_types(&[DataType::Utf8, DataType::Int32])
            .expect_err("#> takes a text[] path");
        assert!(err.to_string().contains("#>"), "got: {err}");
        // A list path, the `ARRAY['a','b']` spelling — kept as the list it is, because Arrow
        // has no `List` → `Utf8` cast to ask for.
        let list = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        assert_eq!(
            path.coerce_types(&[DataType::Utf8, list.clone()])
                .expect("a list is a path"),
            vec![DataType::Utf8, list]
        );
        // And a document that is not json at all names the operator, not the UDF.
        let err = arrow
            .coerce_types(&[DataType::Int64, DataType::Utf8])
            .expect_err("an integer has no -> operator");
        assert!(
            err.to_string().contains("operator does not exist"),
            "got: {err}"
        );
    }

    /// The wire carries only the name, so every one of the six has to resolve after
    /// registration, and registering twice is what a context reached by two paths does.
    #[test]
    fn the_functions_resolve_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        register_json_functions(&mut ctx).expect("registration failed");
        register_json_functions(&mut ctx).expect("second registration failed");
        for name in [
            JSON_IN_UDF_NAME,
            JSONB_IN_UDF_NAME,
            JSON_GET_UDF_NAME,
            JSON_GET_TEXT_UDF_NAME,
            JSON_PATH_UDF_NAME,
            JSON_PATH_TEXT_UDF_NAME,
        ] {
            assert!(ctx.udf(name).is_ok(), "{name} should resolve");
        }
    }
}
