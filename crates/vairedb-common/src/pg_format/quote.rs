//! The two quoting rules — one for a literal, one for an identifier — and the
//! `quote_literal`/`quote_nullable` functions the first of them is exposed through.
//!
//! Both rules answer the same shaped question, "how does this text read back when it is
//! pasted into SQL", and they answer it differently enough to be written out twice: a
//! literal doubles `'` *and* `\` and sometimes carries the `E` prefix, an identifier doubles
//! `"` and is only wrapped at all when leaving it bare would change which identifier it is.
//! [`format()`](super::format_udf)'s `%L` and `%I` are these same two rules, which is why
//! they live beside the functions rather than inside either one.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_pg_catalog::pg_catalog::quote_ident_udf;

use super::{QUOTE_LITERAL_UDF_NAME, QUOTE_NULLABLE_UDF_NAME, render_column};

/// PostgreSQL's `quote_literal` of an already-rendered string.
///
/// The `E` prefix is not decoration: without it a backslash in the value would be read back
/// under `standard_conforming_strings = off` as an escape, so PostgreSQL emits the doubled
/// backslash *and* the prefix that makes the doubling meaningful.
pub fn quote_literal_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    if text.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for c in text.chars() {
        if c == '\'' || c == '\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// PostgreSQL's `quote_ident` of `text`.
///
/// The reserved-word question is answered by `datafusion-pg-catalog`'s `quote_ident`, which
/// carries the list. The *folding* question is answered here, because that implementation
/// does not: `Mixed` comes back from it unquoted, and `Mixed` read back unquoted is `mixed`,
/// a different identifier. PostgreSQL quotes anything that is not already lower-case for
/// exactly that reason, and `%I` exists to be pasted back into SQL.
fn quote_identifier(text: &str) -> Result<String> {
    Ok(match shared_quote_ident_text(text)? {
        // Already quoted by the shared implementation: a special character or a reserved
        // word, and both of those subsume the folding question.
        quoted if quoted != text => quoted,
        // Left alone by it, so the only thing that can still need quoting is a character
        // whose unquoted spelling would fold to a different one.
        plain if plain.chars().any(char::is_uppercase) => {
            let mut out = String::with_capacity(plain.len() + 2);
            out.push('"');
            for c in plain.chars() {
                if c == '"' {
                    out.push(c);
                }
                out.push(c);
            }
            out.push('"');
            out
        }
        plain => plain,
    })
}

/// The identifiers [`quote_identifier`] has already answered for, so a low-cardinality
/// column costs one answer per *distinct* identifier rather than one per row.
///
/// Answering one costs an invocation of the shared `quote_ident` — see
/// [`shared_quote_ident_text`], which builds a one-row array and the arguments around it —
/// and a column of schema and table names is mostly repeats. The map lives for one call of
/// `format()` and is dropped with it, which is why it needs no bound.
#[derive(Default)]
pub(super) struct IdentifierCache {
    answers: HashMap<String, String>,
}

impl IdentifierCache {
    pub(super) fn quote(&mut self, text: &str) -> Result<String> {
        if let Some(hit) = self.answers.get(text) {
            return Ok(hit.clone());
        }
        let quoted = quote_identifier(text)?;
        self.answers.insert(text.to_string(), quoted.clone());
        Ok(quoted)
    }
}

/// `quote_ident(text)` as `datafusion-pg-catalog` implements it.
///
/// Through the UDF and not through the function behind it, because the function behind it —
/// the reserved-word list and the character test that make up its answer — is private to
/// that crate. The one-row array this builds is the price of not carrying a second copy of
/// the list.
fn shared_quote_ident_text(text: &str) -> Result<String> {
    let input: ArrayRef = Arc::new(StringArray::from(vec![text.to_string()]));
    let out = shared_quote_ident()
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(Field::new("i", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("q", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?
        .to_array(1)?;
    render_column(&out)?
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| DataFusionError::Internal("quote_ident answered NULL".to_string()))
}

/// One `quote_ident`, built once. Constructing it per row would dominate the cost of `%I`.
fn shared_quote_ident() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(quote_ident_udf::create_quote_ident_udf())))
}

/// The one thing `quote_literal` and `quote_nullable` disagree about: what a NULL becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum NullAnswer {
    /// `quote_literal`, which is strict: a NULL propagates, so a client interpolating the
    /// result gets `NULL` out of the concatenation too.
    Null,
    /// `quote_nullable`: the four characters `NULL`, which is what makes it safe inside
    /// `format('%L')`.
    Keyword,
}

/// `quote_literal(x)` and `quote_nullable(x)`, which differ only over NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct Quote {
    nulls: NullAnswer,
    signature: Signature,
}

impl Quote {
    pub(super) fn new(nulls: NullAnswer) -> Self {
        Self {
            nulls,
            // PostgreSQL has a `text` overload and an `anyelement` one; accepting any type
            // and rendering it is the same thing with one entry.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Quote {
    fn name(&self) -> &str {
        match self.nulls {
            NullAnswer::Null => QUOTE_LITERAL_UDF_NAME,
            NullAnswer::Keyword => QUOTE_NULLABLE_UDF_NAME,
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let Some(arg) = args.args.first() else {
            return exec_err!("{} requires one argument", self.name());
        };
        let rendered = render_column(&arg.to_array(rows)?)?;

        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        for value in rendered {
            match value {
                Some(text) => out.append_value(quote_literal_text(&text)),
                None if self.nulls == NullAnswer::Keyword => out.append_value("NULL"),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, NullArray};

    fn run_quote(nulls: NullAnswer, array: ArrayRef) -> Vec<Option<String>> {
        let rows = array.len();
        let field = Arc::new(Field::new("x", array.data_type().clone(), true));
        let out = Quote::new(nulls)
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: rows,
                return_field: Arc::new(Field::new("q", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .expect("invoked")
            .to_array(rows)
            .expect("array");
        render_column(&out).expect("rendered")
    }

    /// The backslash rule: doubled *and* prefixed, because the prefix is what makes the
    /// doubling mean a backslash when the string is read back.
    #[test]
    fn a_backslash_gets_the_escape_string_prefix() {
        assert_eq!(quote_literal_text(r"a\b"), r"E'a\\b'");
        assert_eq!(quote_literal_text("plain"), "'plain'");
        assert_eq!(quote_literal_text("it's"), "'it''s'");
    }

    /// Why the folding rule is applied here rather than left to the shared `quote_ident`:
    /// that implementation checks the characters and the reserved-word list and stops, so
    /// `Mixed` comes back from it unquoted — and `Mixed` pasted into SQL unquoted is
    /// `mixed`, which is a different table. If the first assertion ever starts failing, the
    /// rule has become redundant and can go.
    #[test]
    fn the_shared_quote_ident_is_the_reason_the_folding_rule_is_here() {
        assert_eq!(
            shared_quote_ident_text("Mixed").expect("quoted"),
            "Mixed",
            "the dependency now folds too, so the local rule is redundant"
        );
        assert_eq!(
            shared_quote_ident_text("select").expect("quoted"),
            r#""select""#,
            "the reserved-word list is what this module borrows"
        );
        assert_eq!(quote_identifier("Mixed").expect("quoted"), r#""Mixed""#);
    }

    /// The one difference between the two functions, which is the reason to have both.
    #[test]
    fn quote_literal_is_strict_and_quote_nullable_is_not() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        assert_eq!(
            run_quote(NullAnswer::Null, values.clone()),
            vec![Some("'a'".to_string()), None]
        );
        assert_eq!(
            run_quote(NullAnswer::Keyword, values),
            vec![Some("'a'".to_string()), Some("NULL".to_string())]
        );
    }

    /// An **untyped** NULL, which is what a bare `NULL` in a statement arrives as. A
    /// `NullArray` carries no physical null buffer, so the row has to be recognized through
    /// `logical_nulls`; read the other way it renders as the empty string, and
    /// `quote_nullable(NULL)` answered `''` where PostgreSQL answers `NULL`.
    #[test]
    fn an_untyped_null_is_still_a_null() {
        let untyped: ArrayRef = Arc::new(NullArray::new(1));
        assert_eq!(render_column(&untyped).expect("rendered"), vec![None]);
        assert_eq!(
            run_quote(NullAnswer::Keyword, untyped.clone()),
            vec![Some("NULL".to_string())]
        );
        assert_eq!(run_quote(NullAnswer::Null, untyped), vec![None]);
    }

    /// Both accept any type, because PostgreSQL's `anyelement` overload does.
    #[test]
    fn a_non_text_argument_is_rendered_before_it_is_quoted() {
        assert_eq!(
            run_quote(NullAnswer::Null, Arc::new(Int32Array::from(vec![42]))),
            vec![Some("'42'".to_string())]
        );
    }
}
