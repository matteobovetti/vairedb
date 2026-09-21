//! PostgreSQL's string-building family: `format()`, `quote_literal()` and
//! `quote_nullable()`.
//!
//! These three are what PostgreSQL code uses to build SQL text — the `format('%I.%I', s, t)`
//! idiom in every migration tool, and the `quote_literal()` in every hand-written dynamic
//! statement. `datafusion-pg-functions` 0.1 declares a `format` category and compiles nothing
//! into it, so before this module the whole family was simply absent, and a client got
//! `Invalid function 'format'` — a message that reads as "no such function anywhere" when the
//! function is in fact one of the most-used in PostgreSQL.
//!
//! ## What `%I` reuses, and why
//!
//! `%I` is `quote_ident()` applied to the argument, and whether an identifier needs quoting
//! depends on PostgreSQL's reserved-word list. That list already ships in
//! `datafusion-pg-catalog`, whose `quote_ident` VaireDB registers (see [`crate::pg_udf`]), so
//! `%I` calls *that* function rather than carrying a second copy of the list which could
//! disagree with it. The call is made once per argument column, not once per row.
//!
//! ## What is deliberately left out
//!
//! `to_number(text, text)` is the one member of the format family this module does not add.
//! Arrow's `Decimal128` carries a fixed scale, so the natural implementation of
//! `to_number('1234', '9999')` would have to pick one — and any choice renders as
//! `1234.000…` where PostgreSQL prints `1234`. A function that answers a *differently
//! spelled* number is worse than one that is absent, because the absence is visible to the
//! client and the wrong scale is not.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_pg_catalog::pg_catalog::quote_ident_udf;

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The names PostgreSQL uses, so a client's SQL needs no rewriting.
pub const FORMAT_UDF_NAME: &str = "format";
pub const QUOTE_LITERAL_UDF_NAME: &str = "quote_literal";
pub const QUOTE_NULLABLE_UDF_NAME: &str = "quote_nullable";

/// Register the format family on `registry`.
///
/// Call this on every context that plans **or** executes a read; see [`crate::pg_udf`] for
/// why a name has to resolve on every node.
pub fn register_format_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(format_udf())?;
    registry.register_udf(quote_literal_udf())?;
    registry.register_udf(quote_nullable_udf())?;
    Ok(())
}

/// The shared `format` instance.
pub fn format_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Format::default()))))
}

/// The shared `quote_literal` instance.
pub fn quote_literal_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Quote::new(false)))))
}

/// The shared `quote_nullable` instance.
pub fn quote_nullable_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Quote::new(true)))))
}

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

/// The text representation PostgreSQL's `text` cast of a value produces, for every row of
/// `array`. `None` is a NULL, kept distinct because the three conversions disagree about it.
pub(crate) fn render_column(array: &ArrayRef) -> Result<Vec<Option<String>>> {
    // Arrow's default renders a timestamp as `2024-01-01T00:00:00`; PostgreSQL's `text` cast
    // writes a space, and a client interpolating the result into SQL would be quoting the
    // wrong string.
    let options = FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"));
    let formatter = ArrayFormatter::try_new(array.as_ref(), &options)?;
    // `logical_nulls` and not `is_null`, because a `NullArray` — which is what an untyped
    // `NULL` literal arrives as — has no *physical* null buffer, so `is_null` answers false
    // for every row of it and the formatter then renders the empty string. That made
    // `quote_nullable(NULL)` answer `''` where PostgreSQL answers `NULL`, and
    // `format('%I', NULL)` answer `""` where PostgreSQL raises. The same call is what makes a
    // dictionary-encoded null read correctly.
    let nulls = array.logical_nulls();
    (0..array.len())
        .map(|row| {
            if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                Ok(None)
            } else {
                Ok(Some(formatter.value(row).try_to_string()?))
            }
        })
        .collect()
}

/// PostgreSQL's `quote_ident` of `text`, memoized per call so a low-cardinality column
/// costs one invocation of the shared implementation.
///
/// The reserved-word question is answered by `datafusion-pg-catalog`'s `quote_ident`, which
/// carries the list. The *folding* question is answered here, because that implementation
/// does not: `Mixed` comes back from it unquoted, and `Mixed` read back unquoted is `mixed`,
/// a different identifier. PostgreSQL quotes anything that is not already lower-case for
/// exactly that reason, and `%I` exists to be pasted back into SQL.
fn quote_identifier(text: &str, cache: &mut HashMap<String, String>) -> Result<String> {
    if let Some(hit) = cache.get(text) {
        return Ok(hit.clone());
    }
    let quoted = match shared_quote_ident_text(text)? {
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
    };
    cache.insert(text.to_string(), quoted.clone());
    Ok(quoted)
}

/// `quote_ident(text)` as `datafusion-pg-catalog` implements it.
fn shared_quote_ident_text(text: &str) -> Result<String> {
    let udf = shared_quote_ident();
    let input: ArrayRef = Arc::new(StringArray::from(vec![text.to_string()]));
    let field = Arc::new(arrow::datatypes::Field::new("i", DataType::Utf8, true));
    let out = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![field],
            number_rows: 1,
            return_field: Arc::new(arrow::datatypes::Field::new("q", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
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

/// A `format()` string that PostgreSQL would refuse.
///
/// Each carries the SQLSTATE PostgreSQL reports, because the distinction matters to a
/// client: `22004` is one row's NULL and `22023` is the format string itself, which no
/// data will fix.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FormatError {
    /// A `%` at the end of the string, with nothing to convert.
    Unterminated,
    /// A conversion character that is none of `s`, `I`, `L`, `%`.
    UnrecognizedSpecifier(char),
    /// More conversions than arguments.
    TooFewArguments,
    /// `%0$s` — PostgreSQL numbers arguments from one.
    ZeroPosition,
    /// A NULL reaching `%I`, which has no spelling for one.
    NullIdentifier,
    /// A `*` width whose argument is not an integer.
    NonIntegerWidth(String),
}

impl FormatError {
    fn error_code(&self) -> VdbErrorCode {
        match self {
            // The one that is about the data rather than the query.
            FormatError::NullIdentifier => VdbErrorCode::NullValueNotAllowed,
            _ => VdbErrorCode::InvalidParameterValue,
        }
    }
}

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatError::Unterminated => write!(f, "unterminated format() type specifier"),
            FormatError::UnrecognizedSpecifier(c) => {
                write!(f, "unrecognized format() type specifier \"{c}\"")
            }
            FormatError::TooFewArguments => write!(f, "too few arguments for format()"),
            FormatError::ZeroPosition => write!(
                f,
                "format specifies argument 0, but arguments are numbered from 1"
            ),
            FormatError::NullIdentifier => {
                write!(f, "null values cannot be formatted as an SQL identifier")
            }
            FormatError::NonIntegerWidth(text) => {
                write!(f, "invalid input syntax for type integer: \"{text}\"")
            }
        }
    }
}

impl From<FormatError> for DataFusionError {
    fn from(e: FormatError) -> Self {
        // Tagged so the code survives the trip back from whichever node ran the projection.
        DataFusionError::Execution(tagged_message(e.error_code(), e))
    }
}

/// `format(fmt, args...)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Format {
    signature: Signature,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            // Any argument types: PostgreSQL's is `format(text, VARIADIC "any")`, and `%s`
            // renders whatever it is given. The arity floor is checked in `return_type`,
            // where the message can name the function.
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Format {
    fn name(&self) -> &str {
        FORMAT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return plan_err!("format() requires at least the format string");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<Result<_>>()?;
        let Some((format_strings, values)) = arrays.split_first() else {
            return exec_err!("format() requires at least the format string");
        };

        let formats = render_column(format_strings)?;
        let rendered: Vec<Vec<Option<String>>> = values
            .iter()
            .map(render_column)
            .collect::<Result<Vec<_>>>()?;

        let mut idents = HashMap::new();
        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        for (row, format) in formats.iter().enumerate() {
            // A NULL format string is a NULL answer, not an empty one: PostgreSQL's
            // `format` is strict in its first argument only.
            match format {
                None => out.append_null(),
                Some(fmt) => {
                    let text = format_row(fmt, row, &rendered, &mut idents)?;
                    out.append_value(text);
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Render one row's `format()` output.
///
/// The grammar is PostgreSQL's whole grammar, not a subset of it:
/// `%[position$][-][width|*]type`, where `type` is `s`, `I`, `L` or `%`.
fn format_row(
    fmt: &str,
    row: usize,
    args: &[Vec<Option<String>>],
    idents: &mut HashMap<String, String>,
) -> Result<String> {
    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::with_capacity(fmt.len());
    let mut at = 0usize;
    // PostgreSQL keeps one cursor for the arguments consumed positionally; an explicit
    // `n$` reads an argument without moving it.
    let mut cursor = 0usize;

    while at < chars.len() {
        if chars[at] != '%' {
            out.push(chars[at]);
            at += 1;
            continue;
        }
        at += 1;
        if at >= chars.len() {
            return Err(FormatError::Unterminated.into());
        }
        if chars[at] == '%' {
            out.push('%');
            at += 1;
            continue;
        }

        // `[position$]`, distinguished from a width only by the `$` that follows it.
        let position = read_position(&chars, &mut at)?;

        let mut left_justify = false;
        while at < chars.len() && chars[at] == '-' {
            left_justify = true;
            at += 1;
        }

        // `[width|*]`. A `*` takes the width from an argument, and a negative one there
        // means the same as the `-` flag.
        let mut width: i64 = 0;
        if at < chars.len() && chars[at] == '*' {
            at += 1;
            let star_position = read_position(&chars, &mut at)?;
            let value = take_argument(args, row, star_position, &mut cursor)?;
            width = match value {
                // PostgreSQL treats a NULL width as no width at all.
                None => 0,
                Some(text) => text
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| FormatError::NonIntegerWidth(text.clone()))?,
            };
            if width < 0 {
                left_justify = true;
                width = -width;
            }
        } else {
            let start = at;
            while at < chars.len() && chars[at].is_ascii_digit() {
                at += 1;
            }
            if at > start {
                let digits: String = chars[start..at].iter().collect();
                width = digits.parse::<i64>().unwrap_or(0);
            }
        }

        if at >= chars.len() {
            return Err(FormatError::Unterminated.into());
        }
        let conversion = chars[at];
        at += 1;

        let value = take_argument(args, row, position, &mut cursor)?;
        let converted = match conversion {
            // A NULL renders as nothing at all, which is what makes `format('%s', x)`
            // usable for building a comma-separated list.
            's' => value.unwrap_or_default(),
            'L' => match value {
                None => "NULL".to_string(),
                Some(text) => quote_literal_text(&text),
            },
            'I' => match value {
                None => return Err(FormatError::NullIdentifier.into()),
                Some(text) => quote_identifier(&text, idents)?,
            },
            other => return Err(FormatError::UnrecognizedSpecifier(other).into()),
        };

        pad(&mut out, &converted, width, left_justify);
    }

    Ok(out)
}

/// Read a `[digits$]` prefix, leaving `at` where it was if there is no `$`.
///
/// Those digits are otherwise a width, so the `$` is the only thing that tells the two
/// apart and the scan has to be undone when it is missing.
fn read_position(chars: &[char], at: &mut usize) -> Result<Option<usize>> {
    let start = *at;
    let mut end = start;
    while end < chars.len() && chars[end].is_ascii_digit() {
        end += 1;
    }
    if end == start || end >= chars.len() || chars[end] != '$' {
        return Ok(None);
    }
    let digits: String = chars[start..end].iter().collect();
    let position = digits
        .parse::<usize>()
        .map_err(|_| DataFusionError::from(FormatError::TooFewArguments))?;
    if position == 0 {
        return Err(FormatError::ZeroPosition.into());
    }
    *at = end + 1;
    Ok(Some(position))
}

/// The argument a conversion reads: the one `position` names, or the next one in order.
///
/// There is one cursor, and an explicit position moves it — measured against PostgreSQL
/// 16.15, where `format('%1$s-%1$s-%s', 'a', 'b')` is `a-a-b` and not `a-a-a`. So an
/// explicit position is "read from here", not "read this one and carry on where I was".
fn take_argument(
    args: &[Vec<Option<String>>],
    row: usize,
    position: Option<usize>,
    cursor: &mut usize,
) -> Result<Option<String>> {
    let index = match position {
        Some(position) => position - 1,
        None => *cursor,
    };
    *cursor = index + 1;
    let column = args
        .get(index)
        .ok_or_else(|| DataFusionError::from(FormatError::TooFewArguments))?;
    Ok(column[row].clone())
}

/// Append `text` to `out`, padded to `width` characters.
///
/// Width counts characters and not bytes, because PostgreSQL's does — a format string
/// aligning a column of names would otherwise ragged out on the first non-ASCII one.
fn pad(out: &mut String, text: &str, width: i64, left_justify: bool) {
    let length = text.chars().count() as i64;
    let padding = (width - length).max(0) as usize;
    if left_justify {
        out.push_str(text);
        out.extend(std::iter::repeat_n(' ', padding));
    } else {
        out.extend(std::iter::repeat_n(' ', padding));
        out.push_str(text);
    }
}

/// `quote_literal(x)` and `quote_nullable(x)`, which differ only over NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Quote {
    /// `true` for `quote_nullable`: a NULL becomes the four characters `NULL`.
    nullable: bool,
    signature: Signature,
}

impl Quote {
    fn new(nullable: bool) -> Self {
        Self {
            nullable,
            // PostgreSQL has a `text` overload and an `anyelement` one; accepting any type
            // and rendering it is the same thing with one entry.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Quote {
    fn name(&self) -> &str {
        if self.nullable {
            QUOTE_NULLABLE_UDF_NAME
        } else {
            QUOTE_LITERAL_UDF_NAME
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
        let array = arg.to_array(rows)?;
        let rendered = render_column(&array)?;

        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        for value in rendered {
            match value {
                Some(text) => out.append_value(quote_literal_text(&text)),
                // The whole difference between the two functions: `quote_literal` is
                // strict, so a NULL propagates and a client interpolating the result
                // gets `NULL` from the concatenation too — `quote_nullable` writes the
                // keyword instead, which is what makes it safe inside `format('%L')`.
                None if self.nullable => out.append_value("NULL"),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use arrow::array::{Int32Array, NullArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::execution::context::SessionContext;

    fn run_format(args: Vec<ArrayRef>) -> Result<Vec<Option<String>>> {
        let rows = args.first().map(|a| a.len()).unwrap_or(1);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("a{i}"), a.data_type().clone(), true)))
            .collect();
        let out = Format::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: args.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: rows,
                return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })?
            .to_array(rows)?;
        render_column(&out)
    }

    fn text(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(
            values.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
        ))
    }

    fn one(fmt: &str, args: Vec<ArrayRef>) -> Result<String> {
        let mut all = vec![text(&[fmt])];
        all.extend(args);
        Ok(run_format(all)?
            .into_iter()
            .next()
            .flatten()
            .unwrap_or_default())
    }

    /// The three conversions, in the shape a migration tool writes them.
    #[test]
    fn the_three_conversions_render_as_postgresql_does() {
        assert_eq!(
            one("%I.%I", vec![text(&["public"]), text(&["my table"])]).expect("valid"),
            r#"public."my table""#
        );
        assert_eq!(
            one("insert into t values (%L)", vec![text(&["O'Brien"])]).expect("valid"),
            "insert into t values ('O''Brien')"
        );
        assert_eq!(
            one("%s items", vec![Arc::new(Int32Array::from(vec![3]))]).expect("valid"),
            "3 items"
        );
    }

    /// `%%` is the escape, and nothing else in the string is touched.
    #[test]
    fn a_doubled_percent_is_one_percent() {
        assert_eq!(one("100%%", vec![]).expect("valid"), "100%");
        assert_eq!(
            one("%s%% of %s", vec![text(&["50"]), text(&["them"])]).expect("valid"),
            "50% of them"
        );
    }

    /// A NULL means three different things to the three conversions, and the differences are
    /// the reason all three exist: `%s` builds a list, `%L` builds SQL, and `%I` cannot.
    #[test]
    fn each_conversion_has_its_own_answer_for_null() {
        let null: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>]));
        assert_eq!(one("[%s]", vec![null.clone()]).expect("valid"), "[]");
        assert_eq!(one("[%L]", vec![null.clone()]).expect("valid"), "[NULL]");

        let err = one("[%I]", vec![null]).expect_err("a null identifier");
        assert_eq!(
            strip_code_tags(&err.to_string()).contains("null values cannot be formatted"),
            true,
            "got: {err}"
        );
    }

    /// The SQLSTATE is the point of the refusal: `22004` tells the client one row's value
    /// was null, which is a different fix from a bad format string.
    #[test]
    fn a_null_identifier_carries_the_data_error_code() {
        let null: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>]));
        let err = one("%I", vec![null]).expect_err("a null identifier");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::NullValueNotAllowed)
        );
    }

    /// Position specifiers, and the rule that is easy to get backwards: there is one cursor
    /// and an explicit position moves it, so the `%s` after a `%1$s` reads the *second*
    /// argument. All three of these are PostgreSQL 16.15's answers.
    #[test]
    fn an_explicit_position_moves_the_cursor_it_reads_from() {
        assert_eq!(
            one("%1$s-%1$s-%s", vec![text(&["a"]), text(&["b"])]).expect("valid"),
            "a-a-b"
        );
        assert_eq!(
            one("%s-%1$s-%s", vec![text(&["a"]), text(&["b"])]).expect("valid"),
            "a-a-b"
        );
        assert_eq!(
            one("%2$s-%1$s", vec![text(&["a"]), text(&["b"])]).expect("valid"),
            "b-a"
        );
        // And the consequence, which is what makes the rule observable rather than
        // academic: the cursor is past the end, so there is no next argument.
        let err = one("%2$s-%s", vec![text(&["a"]), text(&["b"])]).expect_err("past the end");
        assert!(
            strip_code_tags(&err.to_string()).contains("too few arguments for format()"),
            "got: {err}"
        );
    }

    /// An argument the format string never names is ignored, not an error.
    #[test]
    fn an_unused_argument_is_ignored() {
        assert_eq!(
            one("%s%s", vec![text(&["a"]), text(&["b"]), text(&["c"])]).expect("valid"),
            "ab"
        );
    }

    /// Width pads, `-` pads the other side, and `*` takes the width from an argument.
    #[test]
    fn width_pads_on_the_side_the_flag_selects() {
        assert_eq!(one("[%5s]", vec![text(&["ab"])]).expect("valid"), "[   ab]");
        assert_eq!(
            one("[%-5s]", vec![text(&["ab"])]).expect("valid"),
            "[ab   ]"
        );
        assert_eq!(
            one(
                "[%*s]",
                vec![Arc::new(Int32Array::from(vec![4])), text(&["ab"])]
            )
            .expect("valid"),
            "[  ab]"
        );
        // A negative `*` width means what the `-` flag means.
        assert_eq!(
            one(
                "[%*s]",
                vec![Arc::new(Int32Array::from(vec![-4])), text(&["ab"])]
            )
            .expect("valid"),
            "[ab  ]"
        );
    }

    /// Width counts characters, not bytes: a column of names aligned by `format` would
    /// otherwise go ragged at the first accented one.
    #[test]
    fn width_counts_characters() {
        assert_eq!(one("[%4s]", vec![text(&["é"])]).expect("valid"), "[   é]");
    }

    /// The three refusals that are about the format string rather than the data. Each is
    /// PostgreSQL's message, because a client matching on it is matching on that text.
    #[test]
    fn a_bad_format_string_is_refused_the_way_postgresql_refuses_it() {
        for (fmt, args, expected) in [
            (
                "%d",
                vec![text(&["x"])],
                "unrecognized format() type specifier",
            ),
            (
                "%s %s",
                vec![text(&["x"])],
                "too few arguments for format()",
            ),
            ("100%", vec![], "unterminated format() type specifier"),
            ("%0$s", vec![text(&["x"])], "arguments are numbered from 1"),
        ] {
            let err = one(fmt, args).expect_err(fmt);
            let message = strip_code_tags(&err.to_string());
            assert!(message.contains(expected), "{fmt} gave: {message}");
            assert_eq!(
                code_of_tagged_message(&err.to_string()),
                Some(VdbErrorCode::InvalidParameterValue),
                "{fmt} carried the wrong code"
            );
        }
    }

    /// A NULL format string answers NULL. Only the first argument is strict.
    #[test]
    fn a_null_format_string_answers_null() {
        let fmt: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>]));
        assert_eq!(
            run_format(vec![fmt, text(&["x"])]).expect("valid"),
            vec![None]
        );
    }

    /// `%I` has to answer what PostgreSQL's `quote_ident` answers, whatever the identifier
    /// is: an all-lower-case word needs nothing, a reserved word needs quotes even though
    /// its characters are ordinary, and a space or an embedded quote needs them too.
    #[test]
    fn an_identifier_conversion_answers_what_postgresql_answers() {
        let mut cache = HashMap::new();
        for (input, expected) in [
            ("simple", "simple"),
            ("with_underscore_1", "with_underscore_1"),
            ("select", r#""select""#),
            ("Mixed", r#""Mixed""#),
            ("with space", r#""with space""#),
            ("with\"quote", r#""with""quote""#),
        ] {
            assert_eq!(
                quote_identifier(input, &mut cache).expect("quoted"),
                expected,
                "quote_identifier disagreed for {input}"
            );
            assert_eq!(
                one("%I", vec![text(&[input])]).expect("valid"),
                expected,
                "%I disagreed for {input}"
            );
        }
    }

    /// Why the folding rule is applied here rather than left to the shared `quote_ident`:
    /// that implementation checks the characters and the reserved-word list and stops, so
    /// `Mixed` comes back from it unquoted — and `Mixed` pasted into SQL unquoted is
    /// `mixed`, which is a different table. If this ever starts failing, the rule above has
    /// become redundant and can go.
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
    }

    /// The backslash rule: doubled *and* prefixed, because the prefix is what makes the
    /// doubling mean a backslash when the string is read back.
    #[test]
    fn a_backslash_gets_the_escape_string_prefix() {
        assert_eq!(quote_literal_text(r"a\b"), r"E'a\\b'");
        assert_eq!(quote_literal_text("plain"), "'plain'");
        assert_eq!(quote_literal_text("it's"), "'it''s'");
    }

    fn run_quote(nullable: bool, array: ArrayRef) -> Vec<Option<String>> {
        let rows = array.len();
        let field = Arc::new(Field::new("x", array.data_type().clone(), true));
        let out = Quote::new(nullable)
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: rows,
                return_field: Arc::new(Field::new("q", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .expect("invoked")
            .to_array(rows)
            .expect("array");
        render_column(&out).expect("rendered")
    }

    /// The one difference between the two functions, which is the reason to have both.
    #[test]
    fn quote_literal_is_strict_and_quote_nullable_is_not() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        assert_eq!(
            run_quote(false, values.clone()),
            vec![Some("'a'".to_string()), None]
        );
        assert_eq!(
            run_quote(true, values),
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
            run_quote(true, untyped.clone()),
            vec![Some("NULL".to_string())]
        );
        assert_eq!(run_quote(false, untyped), vec![None]);
    }

    /// And through `format`, where the three conversions each have their own answer for a
    /// NULL: nothing at all, the keyword, and a refusal.
    #[test]
    fn format_reads_an_untyped_null_as_a_null() {
        let untyped: ArrayRef = Arc::new(NullArray::new(1));
        assert_eq!(
            run_format(vec![
                Arc::new(StringArray::from(vec!["[%s]"])),
                Arc::clone(&untyped)
            ])
            .expect("rendered"),
            vec![Some("[]".to_string())]
        );
        assert_eq!(
            run_format(vec![
                Arc::new(StringArray::from(vec!["[%L]"])),
                Arc::clone(&untyped)
            ])
            .expect("rendered"),
            vec![Some("[NULL]".to_string())]
        );
        let err = run_format(vec![
            Arc::new(StringArray::from(vec!["[%I]"])),
            Arc::clone(&untyped),
        ])
        .expect_err("a NULL cannot be an identifier");
        assert!(
            err.to_string().contains("identifier"),
            "got: {}",
            err.to_string()
        );
    }

    /// Both accept any type, because PostgreSQL's `anyelement` overload does.
    #[test]
    fn a_non_text_argument_is_rendered_before_it_is_quoted() {
        assert_eq!(
            run_quote(false, Arc::new(Int32Array::from(vec![42]))),
            vec![Some("'42'".to_string())]
        );
    }

    /// All three names have to resolve on every node, because the plan carries the name and
    /// nothing else.
    #[tokio::test]
    async fn the_family_resolves_by_name_and_answers_over_a_column() {
        let mut ctx = SessionContext::new();
        for name in [
            FORMAT_UDF_NAME,
            QUOTE_LITERAL_UDF_NAME,
            QUOTE_NULLABLE_UDF_NAME,
        ] {
            assert!(
                ctx.udf(name).is_err(),
                "a bare context should not have {name}, or this test proves nothing"
            );
        }
        register_format_functions(&mut ctx).expect("registration failed");

        let batches = ctx
            .sql(
                "SELECT format('%I = %L', c, c) AS f, quote_literal(c) AS l, \
                 quote_nullable(c) AS n FROM (VALUES ('a b')) v(c)",
            )
            .await
            .expect("planned")
            .collect()
            .await
            .expect("executed");
        let rendered = arrow::util::pretty::pretty_format_batches(&batches)
            .expect("rendered")
            .to_string();
        assert!(rendered.contains(r#""a b" = 'a b'"#), "got:\n{rendered}");
    }
}
