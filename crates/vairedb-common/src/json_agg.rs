//! PostgreSQL's `json_agg` and `jsonb_agg` — a group's values as one JSON array.
//!
//! ```text
//! SELECT json_agg(v) FROM t                  -->  [1, 2, null]
//! SELECT json_agg(v ORDER BY v DESC) FROM t   -->  [2, 1, null]
//! SELECT json_agg(payload::json) FROM t       -->  [{"a": 1}, {"a": 2}]
//! ```
//!
//! ## Why this is a scalar function and not an aggregate
//!
//! The hard part of a `json_agg` is not the rendering, it is everything around it: the
//! in-aggregate `ORDER BY` PostgreSQL allows, `DISTINCT`, `FILTER`, and — the part that
//! only a distributed engine has to care about — keeping that order across a *partial*
//! aggregate on each shard and a *final* aggregate that merges them in whatever order the
//! partitions arrive. DataFusion's `array_agg` already carries all of it: its accumulator
//! keeps the ordering columns beside the values in its own state so the merge can re-sort,
//! which is machinery worth reusing rather than writing twice.
//!
//! So the read path rewrites the aggregate into a composition — the coordinator's side of
//! it lives in its `pgwire_handler::pg_operators`, which this crate cannot link to because
//! nothing here depends on the coordinator:
//!
//! ```text
//! json_agg(v ORDER BY v DESC)  -->  vaire_json_array(array_agg(v ORDER BY v DESC))
//! ```
//!
//! and what is left here is one scalar function that turns a list into JSON text. Every
//! modifier rides along on the inner `array_agg` untouched, and `json_agg` keeps its
//! output column label because `column_labels` reads the query before this rewrite runs.
//!
//! `array_agg` respects nulls unless a query says `IGNORE NULLS`, which is what makes the
//! composition faithful: PostgreSQL renders a NULL input row as a JSON `null` rather than
//! skipping it, so the nulls have to survive as far as the rendering. A group with no rows
//! at all is a SQL NULL — `array_agg` answers NULL there and so does this.
//!
//! ## Quoting versus embedding
//!
//! `json_agg('{"a":1}'::text)` is the JSON *string* `"{\"a\":1}"`; `json_agg('{"a":1}'::json)`
//! is the object `{"a":1}`. Both are Arrow `Utf8` here — see [`crate::json_pg`] for why the
//! JSON types are stored as text — so the difference cannot be read off the value, only off
//! the expression that produced it. Hence two functions: [`JSON_ARRAY_UDF_NAME`] quotes each
//! value and [`JSON_ARRAY_DOCS_UDF_NAME`] splices it in verbatim, and the read path picks
//! between them by looking at what the argument *is* — a `::json`/`::jsonb` cast or a `->`
//! or `#>` accessor produces a document, anything else produces a value.
//!
//! The residue that leaves is a bare column: `json_agg(payload)` over a column *declared*
//! `JSONB` quotes, because by the time the aggregate is planned the column is text like any
//! other and nothing distinguishes it. `json_agg(payload::jsonb)` is the spelling that
//! embeds. This is recorded in `docs/specs/gap-analysis.md` rather than guessed at.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::json::writer::{EncoderOptions, make_encoder};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::columns::list_row;
use crate::json_pg::{JsonType, validate};

/// What the aggregated column has to be, for the refusal when it is not: the rewrite wraps
/// an `array_agg`, so anything else means the plan the executor received is not the one the
/// read path builds.
const AGGREGATE_NEEDS_A_LIST: &str = "a json aggregate needs a list to render";

/// The name the read path emits for `json_agg`/`jsonb_agg` over ordinary values.
pub const JSON_ARRAY_UDF_NAME: &str = "vaire_json_array";
/// The name it emits when the aggregated values are already JSON documents.
pub const JSON_ARRAY_DOCS_UDF_NAME: &str = "vaire_json_array_docs";

/// The aggregate name PostgreSQL spells for `json` — recognized by the read-path rewrite.
pub const JSON_AGG_NAME: &str = "json_agg";
/// The same for `jsonb`. Both render identically here; see the module doc.
pub const JSONB_AGG_NAME: &str = "jsonb_agg";

/// What the aggregated elements are — the whole difference between the two renderings, and
/// the one thing this module is told rather than works out. See "Quoting versus embedding"
/// in the module doc.
///
/// The distinction cannot be read off the values, because both arrive as Arrow text; only
/// the expression that produced them says which it is. So the read path decides it once,
/// records the decision in the name it emits, and the UDF registered under that name carries
/// it from there down to the element that gets written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Elements {
    /// Ordinary values, each encoded as JSON: text comes out quoted and escaped.
    Values,
    /// JSON documents already, each spliced into the array verbatim.
    Documents,
}

impl Elements {
    /// The name the read path emits for this rendering, which is also the UDF's own name.
    fn udf_name(self) -> &'static str {
        match self {
            Elements::Values => JSON_ARRAY_UDF_NAME,
            Elements::Documents => JSON_ARRAY_DOCS_UDF_NAME,
        }
    }
}

/// Register both renderings on `registry`.
///
/// Call this on every context that plans **or** executes a read: the inner `array_agg`
/// crosses the wire as a name and so does the function wrapped around it.
pub fn register_json_aggregates(registry: &mut dyn FunctionRegistry) -> Result<()> {
    for elements in [Elements::Values, Elements::Documents] {
        registry.register_udf(Arc::new(ScalarUDF::from(JsonArray::new(elements))))?;
    }
    Ok(())
}

/// `vaire_json_array(list)` — a list rendered as a JSON array, in text.
///
/// A [`ScalarUDFImpl`] and not an
/// [`AggregateUDFImpl`](datafusion::logical_expr::AggregateUDFImpl): the aggregate is the
/// `array_agg` the read path wraps in this, which is what keeps `ORDER BY`, `DISTINCT` and
/// `FILTER` — and the distributed merge that has to preserve them — out of here. See "Why
/// this is a scalar function and not an aggregate" in the module doc.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArray {
    elements: Elements,
    signature: Signature,
}

impl JsonArray {
    fn new(elements: Elements) -> Self {
        Self {
            elements,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// One JSON array per row of a list column — which is one per group.
    fn render_groups(&self, lists: &ArrayRef) -> Result<ArrayRef> {
        let mut out = StringBuilder::with_capacity(lists.len(), lists.len() * 32);
        for row in 0..lists.len() {
            // A group that aggregated no rows: `array_agg` answers NULL and so does
            // PostgreSQL's `json_agg`.
            if lists.is_null(row) {
                out.append_null();
                continue;
            }
            let elements = list_row(lists, row, AGGREGATE_NEEDS_A_LIST)?;
            out.append_value(render(&elements, self.elements)?);
        }
        Ok(Arc::new(out.finish()))
    }
}

impl ScalarUDFImpl for JsonArray {
    fn name(&self) -> &str {
        self.elements.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A list, kept as the list it is: the element type is what gets rendered, and Arrow
    /// has no cast that would help here.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{} takes one argument, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let element = match arg {
            DataType::List(element) | DataType::LargeList(element) => element.data_type(),
            // What a literal NULL aggregates to; rendered as a SQL NULL.
            DataType::Null => return Ok(vec![DataType::Null]),
            other => return plan_err!("{} takes a list, got {other}", self.name()),
        };
        if self.elements == Elements::Documents
            && !matches!(
                element,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        {
            // Only reachable if the rewrite decided an expression produced json and the
            // engine disagreed about its type, which is a bug rather than a user error.
            return plan_err!("cannot aggregate {element} as json documents");
        }
        Ok(vec![arg.clone()])
    }

    /// Text, which is how both JSON types are stored — see [`crate::json_pg`].
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
        // Against `number_rows`, so a batch of no rows stays one.
        let lists = arg.to_array(args.number_rows)?;
        // What a literal NULL aggregates to: there is no list to render, only SQL NULLs.
        if lists.data_type() == &DataType::Null {
            return Ok(ColumnarValue::Array(arrow::array::new_null_array(
                &DataType::Utf8,
                args.number_rows,
            )));
        }
        Ok(ColumnarValue::Array(self.render_groups(&lists)?))
    }
}

/// `elements` as one JSON array, in PostgreSQL's `, `-separated spelling.
///
/// Kept separate from the function so the rendering — the part that has to match
/// PostgreSQL — can be tested on an array alone.
fn render(elements: &ArrayRef, kind: Elements) -> Result<String> {
    match kind {
        Elements::Values => render_values(elements),
        Elements::Documents => render_documents(elements),
    }
}

/// Each element encoded as JSON, which is what quotes and escapes a text element.
fn render_values(elements: &ArrayRef) -> Result<String> {
    let field: FieldRef = Arc::new(Field::new(
        "element",
        elements.data_type().clone(),
        elements.is_nullable(),
    ));
    // `explicit_nulls` so a null *inside* a struct element stays a `"key": null` member
    // rather than vanishing, which is how PostgreSQL renders a composite.
    let options = EncoderOptions::default().with_explicit_nulls(true);
    let mut encoder = make_encoder(&field, elements.as_ref(), &options)?;
    // The field, the options and the encoder all have to outlive the loop below, which is
    // why they are built here rather than behind their own function.
    json_array(elements.len(), |row, out| {
        match encoder.is_null(row) {
            // A NULL row is the JSON `null`, not a skipped element — see the module doc.
            true => out.extend_from_slice(b"null"),
            false => encoder.encode(row, out),
        }
        Ok(())
    })
}

/// Each element spliced in verbatim, which is what makes `json_agg(x::json)` answer an array
/// of objects rather than an array of strings.
fn render_documents(elements: &ArrayRef) -> Result<String> {
    let text = elements.as_string::<i32>();
    json_array(text.len(), |row, out| {
        if text.is_null(row) {
            // A NULL row is the JSON `null`, not a skipped element — see the module doc.
            out.extend_from_slice(b"null");
            return Ok(());
        }
        let document = text.value(row);
        // By construction these came from a `::json` cast or an accessor, both of which
        // already validated. Checking again costs one parse and keeps a malformed document
        // from being spliced into an answer that would then not parse as JSON at all.
        validate(document, JsonType::Json)?;
        out.extend_from_slice(document.as_bytes());
        Ok(())
    })
}

/// `count` elements, each written by `element`, in the array spelling PostgreSQL prints:
/// square brackets, `, ` between elements.
///
/// The frame lives here rather than in each rendering because the spacing is the contract —
/// a client reads the bytes — and two copies of it are two things to keep in step.
fn json_array(
    count: usize,
    mut element: impl FnMut(usize, &mut Vec<u8>) -> Result<()>,
) -> Result<String> {
    let mut out: Vec<u8> = Vec::with_capacity(count * 8 + 2);
    out.push(b'[');
    for row in 0..count {
        if row > 0 {
            out.extend_from_slice(b", ");
        }
        element(row, &mut out)?;
    }
    out.push(b']');
    // Every element above was written by an encoder or was a validated document, both of
    // which are UTF-8 by construction.
    String::from_utf8(out).map_err(|e| {
        datafusion::common::DataFusionError::Internal(format!(
            "a json aggregate rendered invalid UTF-8: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
        new_null_array,
    };
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::execution::context::SessionContext;

    fn json_of(elements: ArrayRef) -> String {
        render(&elements, Elements::Values).expect("rendering failed")
    }

    /// The shape PostgreSQL prints: square brackets, `, ` between elements.
    #[test]
    fn a_group_renders_as_a_comma_separated_json_array() {
        assert_eq!(
            json_of(Arc::new(Int64Array::from(vec![1, 2, 3]))),
            "[1, 2, 3]"
        );
        assert_eq!(
            json_of(Arc::new(StringArray::from(vec!["a", "b"]))),
            r#"["a", "b"]"#
        );
        assert_eq!(
            json_of(Arc::new(BooleanArray::from(vec![true, false]))),
            "[true, false]"
        );
        assert_eq!(json_of(Arc::new(Float64Array::from(vec![1.5]))), "[1.5]");
        // An empty group still renders a group; the SQL NULL for "no rows at all" is the
        // list being null, which `invoke_with_args` handles.
        assert_eq!(json_of(Arc::new(Int64Array::from(Vec::<i64>::new()))), "[]");
    }

    /// The reason the composition keeps `array_agg`'s nulls: PostgreSQL renders a NULL row
    /// as the JSON `null`, it does not drop the row.
    #[test]
    fn a_null_row_is_a_json_null_rather_than_a_missing_element() {
        assert_eq!(
            json_of(Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))),
            "[1, null, 3]"
        );
        assert_eq!(
            json_of(Arc::new(StringArray::from(vec![None, Some("b")]))),
            r#"[null, "b"]"#
        );
        assert_eq!(json_of(new_null_array(&DataType::Int32, 2)), "[null, null]");
    }

    /// Text is quoted and escaped as a JSON string — the whole difference between the two
    /// renderings.
    #[test]
    fn text_is_quoted_and_escaped() {
        assert_eq!(
            json_of(Arc::new(StringArray::from(vec![r#"{"a":1}"#]))),
            r#"["{\"a\":1}"]"#
        );
        assert_eq!(
            json_of(Arc::new(StringArray::from(vec![
                "line\nbreak",
                "tab\there"
            ]))),
            r#"["line\nbreak", "tab\there"]"#
        );
    }

    /// The document rendering splices instead, which is what makes `json_agg(x::json)`
    /// answer an array of objects rather than an array of strings.
    #[test]
    fn a_document_is_spliced_rather_than_quoted() {
        let documents: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"a":1}"#),
            None,
            Some("[1,2]"),
        ]));
        assert_eq!(
            render(&documents, Elements::Documents).expect("rendering failed"),
            r#"[{"a":1}, null, [1,2]]"#
        );
    }

    /// A document that is not JSON would make the whole answer unparseable, so it raises
    /// `22P02` rather than being spliced.
    #[test]
    fn a_document_that_is_not_json_raises() {
        let broken: ArrayRef = Arc::new(StringArray::from(vec!["{oops"]));
        let err = render(&broken, Elements::Documents).expect_err("not json");
        assert!(
            err.to_string()
                .contains("invalid input syntax for type json"),
            "got: {err}"
        );
    }

    /// The one-row call a physical expression makes, over the list column `lists`.
    fn args_over(lists: ArrayRef) -> ScalarFunctionArgs {
        let data_type = lists.data_type().clone();
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(lists)],
            arg_fields: vec![Arc::new(Field::new("l", data_type, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("j", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        }
    }

    /// Build a one-row list column and render it the way a physical expression does.
    fn invoke(elements: ArrayRef, kind: Elements) -> Result<Vec<Option<String>>> {
        let field = Field::new_list_field(elements.data_type().clone(), true);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, elements.len() as i32]));
        let list = ListArray::new(Arc::new(field), offsets, elements, None);
        let out = JsonArray::new(kind)
            .invoke_with_args(args_over(Arc::new(list)))?
            .to_array(1)?;
        Ok(out
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect())
    }

    #[test]
    fn a_list_column_renders_one_answer_per_group() {
        assert_eq!(
            invoke(
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Elements::Values
            )
            .expect("valid"),
            vec![Some("[1, null]".to_string())]
        );
        assert_eq!(
            invoke(
                Arc::new(StringArray::from(vec![r#"{"a":1}"#])),
                Elements::Documents
            )
            .expect("valid"),
            vec![Some(r#"[{"a":1}]"#.to_string())]
        );
    }

    /// A group that aggregated no rows at all: `array_agg` answers NULL, and PostgreSQL's
    /// `json_agg` answers NULL rather than `[]`.
    #[test]
    fn a_group_with_no_rows_is_null_not_an_empty_array() {
        let element = Field::new_list_field(DataType::Int64, true);
        let list = ListArray::new_null(Arc::new(element), 1);
        let out = JsonArray::new(Elements::Values)
            .invoke_with_args(args_over(Arc::new(list)))
            .expect("valid")
            .to_array(1)
            .expect("array");
        assert!(out.is_null(0), "an empty group must be NULL, got {out:?}");
    }

    #[test]
    fn only_a_list_can_be_rendered() {
        let f = JsonArray::new(Elements::Values);
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        assert_eq!(
            f.coerce_types(std::slice::from_ref(&list)).expect("a list"),
            vec![list.clone()]
        );
        let err = f
            .coerce_types(&[DataType::Int64])
            .expect_err("a bare value is not a group");
        assert!(err.to_string().contains("takes a list"), "got: {err}");
        assert_eq!(
            f.coerce_types(&[DataType::Null]).expect("a null"),
            vec![DataType::Null]
        );
        // The document rendering needs text elements, because a document is text here.
        let err = JsonArray::new(Elements::Documents)
            .coerce_types(&[list])
            .expect_err("integers are not documents");
        assert!(err.to_string().contains("as json documents"), "got: {err}");
        assert_eq!(
            f.return_type(&[DataType::Null]).expect("text"),
            DataType::Utf8
        );
    }

    /// The wire carries only the name, so both have to resolve after registration.
    #[test]
    fn the_functions_resolve_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        for name in [JSON_ARRAY_UDF_NAME, JSON_ARRAY_DOCS_UDF_NAME] {
            assert!(ctx.udf(name).is_err(), "{name} was already registered");
        }
        register_json_aggregates(&mut ctx).expect("registration failed");
        register_json_aggregates(&mut ctx).expect("second registration failed");
        for name in [JSON_ARRAY_UDF_NAME, JSON_ARRAY_DOCS_UDF_NAME] {
            assert!(ctx.udf(name).is_ok(), "{name} did not resolve");
        }
    }

    /// The composition rests on `array_agg` respecting nulls and honouring an in-aggregate
    /// `ORDER BY`. Both are upstream behaviour rather than ours, so both are pinned here:
    /// if a DataFusion upgrade changes either, `json_agg` starts answering PostgreSQL's
    /// question wrongly and this is the test that says so.
    #[tokio::test]
    async fn array_agg_keeps_nulls_and_honours_an_in_aggregate_order_by() {
        let ctx = SessionContext::new();
        register_json_aggregates(&mut { ctx.clone() }).expect("registration failed");
        let answer = |sql: String| {
            let ctx = ctx.clone();
            async move {
                let batches = ctx.sql(&sql).await.expect("planning").collect().await;
                let batches = batches.expect("execution");
                let column = batches[0].column(0);
                column.as_string::<i32>().value(0).to_string()
            }
        };
        let source = "(VALUES (2), (NULL), (1)) AS t(v)";
        assert_eq!(
            answer(format!(
                "SELECT {JSON_ARRAY_UDF_NAME}(array_agg(v ORDER BY v)) FROM {source}"
            ))
            .await,
            "[1, 2, null]"
        );
        assert_eq!(
            answer(format!(
                "SELECT {JSON_ARRAY_UDF_NAME}(array_agg(v ORDER BY v DESC)) FROM {source}"
            ))
            .await,
            "[null, 2, 1]"
        );
    }
}
