//! The four accessors `->`, `->>`, `#>` and `#>>`.
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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::value::RawValue;

use super::input::{JsonInputError, JsonType, is_text_input, validate};
use crate::columns::{list_row, strings};

/// What a `#>`/`#>>` path argument has to be, for the refusal when it is not.
const PATH_MUST_BE_TEXT_ARRAY: &str = "a #> path must be a text array";

/// `j -> k` — a field or an element, as json.
pub const JSON_GET_UDF_NAME: &str = "vaire_json_get";
/// `j ->> k` — a field or an element, as text.
pub const JSON_GET_TEXT_UDF_NAME: &str = "vaire_json_get_text";
/// `j #> p` — the value at a path, as json.
pub const JSON_PATH_UDF_NAME: &str = "vaire_json_path";
/// `j #>> p` — the value at a path, as text.
pub const JSON_PATH_TEXT_UDF_NAME: &str = "vaire_json_path_text";

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

/// Whether `doc` is a `container` and so can answer the question asked of it.
///
/// `false` for a document of any other shape, which is SQL NULL to a client and `jsonb`'s
/// reading of it. Text that is not JSON at all is the client's own mistake rather than a
/// missing value, so it is validated on the way past: `'oops' -> 'a'` is `22P02` and not a
/// silent NULL that would hide a typo in the client's own literal.
fn can_answer(
    doc: &str,
    container: Shape,
    json_type: JsonType,
) -> std::result::Result<bool, JsonInputError> {
    let Some(shape) = shape(doc) else {
        // No first character at all, so not a document either.
        return Err(json_type.invalid_input());
    };
    if shape == container {
        return Ok(true);
    }
    validate(doc, json_type).map(|()| false)
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
    if !can_answer(doc, Shape::Object, json_type)? {
        return Ok(None);
    }
    let members: HashMap<String, &RawValue> =
        serde_json::from_str(doc).map_err(|_| json_type.invalid_input())?;
    Ok(members.get(key).map(|value| value.get().to_string()))
}

/// The element of `doc` at `index`, as its own verbatim text.
///
/// A negative index counts from the end, as in PostgreSQL: `-1` is the last element.
fn element(
    doc: &str,
    index: i64,
    json_type: JsonType,
) -> std::result::Result<Option<String>, JsonInputError> {
    if !can_answer(doc, Shape::Array, json_type)? {
        return Ok(None);
    }
    let elements: Vec<&RawValue> =
        serde_json::from_str(doc).map_err(|_| json_type.invalid_input())?;
    let found = position(index, elements.len()).and_then(|position| elements.get(position));
    Ok(found.map(|value| value.get().to_string()))
}

/// Where `index` lands in an array of `len` elements.
///
/// `None` for a negative index further back than the array is long, which is past its start
/// and so no element at all — an index past its *end* is the same answer, and the caller
/// gets it from the lookup that follows rather than from here.
fn position(index: i64, len: usize) -> Option<usize> {
    if index < 0 {
        return len.checked_sub(index.unsigned_abs() as usize);
    }
    Some(index as usize)
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

/// Whether a type is one PostgreSQL would let index an array.
///
/// Every integer width, because `->` takes an `int` and what reaches here is whatever width
/// the client's driver sent the literal or the column as.
fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Register the four accessors on `registry`.
pub(super) fn register_accessors(registry: &mut dyn FunctionRegistry) -> Result<()> {
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

    /// The operator this stands for, for a message that names what the client wrote rather
    /// than what it was rewritten into.
    fn operator(self) -> &'static str {
        match self {
            Accessor::Field { text: false } => "->",
            Accessor::Field { text: true } => "->>",
            Accessor::Path { text: false } => "#>",
            Accessor::Path { text: true } => "#>>",
        }
    }

    /// The reading this accessor gives an extracted value: `->>` and `#>>` decode the one
    /// string they return and answer SQL NULL for a JSON `null`, where `->` and `#>` hand
    /// back the document's own text — the four characters `null` included.
    fn read(self, found: Option<String>) -> Option<String> {
        match self {
            Accessor::Field { text: true } | Accessor::Path { text: true } => as_text(found),
            _ => found,
        }
    }
}

/// One row's key, in whichever form the client wrote it.
enum Key<'a> {
    /// A key of an object, or the `'{a,b}'` spelling of a whole path.
    Name(&'a str),
    /// An index into an array, the only key that is not a string.
    Index(i64),
    /// The steps of an `ARRAY['a','b']` path.
    Steps(Vec<String>),
}

/// The key column, in whichever of the three Arrow layouts a client's spelling produced.
///
/// One layout per arm of [`JsonAccessor::key_type`], read once for the batch rather than
/// re-decided per row: the layout is the same for every row, and only the value changes.
enum Keys {
    Name(StringArray),
    Index(Int64Array),
    Steps(ArrayRef),
}

impl Keys {
    /// Recognize the layout, and narrow the string one the way every argument is narrowed.
    fn read(key: &ArrayRef) -> Result<Self> {
        match key.data_type() {
            DataType::Int64 => Ok(Keys::Index(key.as_primitive::<Int64Type>().clone())),
            DataType::List(_) | DataType::LargeList(_) => Ok(Keys::Steps(Arc::clone(key))),
            _ => Ok(Keys::Name(strings(key)?)),
        }
    }

    /// One row's key, or `None` for a NULL one.
    ///
    /// A path holding a NULL step is `None` too: no key can match SQL NULL, so the whole
    /// path misses — the same answer PostgreSQL gives `#>` over an array holding a NULL.
    fn at(&self, row: usize) -> Result<Option<Key<'_>>> {
        match self {
            Keys::Name(names) => Ok(names.is_valid(row).then(|| Key::Name(names.value(row)))),
            Keys::Index(indexes) => Ok(indexes
                .is_valid(row)
                .then(|| Key::Index(indexes.value(row)))),
            Keys::Steps(lists) if lists.is_null(row) => Ok(None),
            Keys::Steps(lists) => {
                let steps = strings(&list_row(lists, row, PATH_MUST_BE_TEXT_ARRAY)?)?;
                Ok(steps
                    .iter()
                    .map(|step| step.map(str::to_string))
                    .collect::<Option<Vec<String>>>()
                    .map(Key::Steps))
            }
        }
    }
}

/// `vaire_json_get(json, key)` and the three operators beside it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonAccessor {
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

    /// The type one row's key has to arrive as.
    ///
    /// An integer only for `->`/`->>`: PostgreSQL's `#>` takes a `text[]`, whose steps are
    /// strings even where they index an array.
    fn key_type(&self, key: &DataType) -> Result<DataType> {
        if is_text_input(key) {
            return Ok(DataType::Utf8);
        }
        match (self.accessor, key) {
            (Accessor::Field { .. }, integer) if is_integer(integer) => Ok(DataType::Int64),
            // `ARRAY['a','b']`, the other spelling of a `text[]` path. Kept as the list it
            // is rather than cast to text: Arrow has no `List` → `Utf8` cast, so asking for
            // one here would refuse the spelling at planning time instead of reading it.
            (Accessor::Path { .. }, list @ (DataType::List(_) | DataType::LargeList(_))) => {
                Ok(list.clone())
            }
            (_, other) => plan_err!(
                "operator does not exist: json {} {other}",
                self.accessor.operator()
            ),
        }
    }

    /// One row's answer, before the reading its operator gives it.
    ///
    /// A NULL document is where the row stops, so the key beside it is never read; a NULL
    /// key misses for the reason [`Keys::at`] gives.
    fn answer(&self, docs: &StringArray, keys: &Keys, row: usize) -> Result<Option<String>> {
        if docs.is_null(row) {
            return Ok(None);
        }
        let Some(key) = keys.at(row)? else {
            return Ok(None);
        };
        Ok(self.select(docs.value(row), key)?)
    }

    /// The question this accessor asks of one document with one key.
    ///
    /// An index reaches here only from `->`/`->>` and a list of steps only from `#>`/`#>>`,
    /// because [`Self::key_type`] accepts each for one pair alone — so those two forms are
    /// the question, and only a string key still has to ask which operator wrote it.
    fn select(
        &self,
        doc: &str,
        key: Key<'_>,
    ) -> std::result::Result<Option<String>, JsonInputError> {
        match (self.accessor, key) {
            (_, Key::Index(index)) => element(doc, index, JsonType::Json),
            (_, Key::Steps(steps)) => at_path(doc, &steps, JsonType::Json),
            (Accessor::Field { .. }, Key::Name(key)) => field(doc, key, JsonType::Json),
            (Accessor::Path { .. }, Key::Name(path)) => {
                at_path(doc, &path_of_text(path), JsonType::Json)
            }
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
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [doc, key] = arg_types else {
            return plan_err!(
                "{} takes two arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        if !is_text_input(doc) {
            return plan_err!(
                "operator does not exist: {doc} {}",
                self.accessor.operator()
            );
        }
        Ok(vec![DataType::Utf8, self.key_type(key)?])
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
        let docs = strings(&doc.to_array(args.number_rows)?)?;
        let keys = Keys::read(&key.to_array(args.number_rows)?)?;
        let mut out = StringBuilder::with_capacity(docs.len(), docs.value_data().len());
        for row in 0..docs.len() {
            match self.accessor.read(self.answer(&docs, &keys, row)?) {
                Some(value) => out.append_value(value),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_pg::invoking::{invoke, text_column};
    use arrow::array::ListBuilder;

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
        // Empty text has no first character, and is no more a document than `oops` is.
        assert!(field("", "a", JsonType::Json).is_err());
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

    /// The `ARRAY['a','b']` spelling, whose steps arrive as a list per row rather than as
    /// the string form — and the two NULLs in it that miss instead of raising.
    #[test]
    fn a_list_path_is_walked_step_by_step() {
        let mut steps = ListBuilder::new(StringBuilder::new());
        for step in ["a", "b"] {
            steps.values().append_value(step);
        }
        steps.append(true);
        // A NULL step, which no key matches, so the path misses.
        steps.values().append_value("a");
        steps.values().append_null();
        steps.append(true);
        // And a NULL path, which is a NULL answer.
        steps.append(false);
        let path = ColumnarValue::Array(Arc::new(steps.finish()));
        assert_eq!(
            invoke(
                &JsonAccessor::new(Accessor::Path { text: false }),
                vec![text_column(vec![Some("{\"a\":{\"b\":\"x\"}}"); 3]), path],
                3
            )
            .expect("valid"),
            vec![Some("\"x\"".to_string()), None, None]
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
}
