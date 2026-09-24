//! The `format()` function and the conversion grammar it walks:
//! `%[position$][-][width|*]type`, where `type` is `s`, `I`, `L` or `%`. That is
//! PostgreSQL's whole grammar and not a subset of it.
//!
//! The grammar is small enough to walk by hand and too irregular to parse any other way —
//! `12` is a width and `12$` a position, so the digits have to be scanned and sometimes
//! given back. [`Scanner`] is that cursor, [`Padding`] the `[-][width|*]` part it reads,
//! and [`RowArguments`] the one cursor PostgreSQL keeps over the arguments.
//!
//! ## Why the walk is not hoisted out of the row loop
//!
//! Parsing a format string once and rendering it for every row would be faster, and would
//! change which error a client sees. The refusals are raised in the order the string is
//! walked and interleaved with the argument reads, so `format('%s %d', 'x')` is "too few
//! arguments" — from the `%s` that ran out — and not the unrecognized `%d` further along.
//! Each of those is a real PostgreSQL message; which one arrives is observable, so the walk
//! stays where the rendering is.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::quote::{IdentifierCache, quote_literal_text};
use super::{FORMAT_UDF_NAME, render_column};
use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

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
pub(super) struct Format {
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

        let mut idents = IdentifierCache::default();
        let mut out = StringBuilder::with_capacity(rows, rows * 16);
        for (row, format) in formats.iter().enumerate() {
            // A NULL format string is a NULL answer, not an empty one: PostgreSQL's
            // `format` is strict in its first argument only.
            match format {
                None => out.append_null(),
                Some(fmt) => {
                    let arguments = RowArguments::new(&rendered, row);
                    out.append_value(format_row(fmt, arguments, &mut idents)?);
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Render one row's `format()` output, one conversion of the module's grammar at a time.
fn format_row(
    fmt: &str,
    mut args: RowArguments<'_>,
    idents: &mut IdentifierCache,
) -> Result<String> {
    let mut scanner = Scanner::new(fmt);
    let mut out = String::with_capacity(fmt.len());
    while let Some(c) = scanner.take() {
        if c != '%' {
            out.push(c);
            continue;
        }
        // `%%` is the escape, and it takes no argument and no width.
        if scanner.take_if('%') {
            out.push('%');
            continue;
        }
        let position = scanner.take_position()?;
        let padding = Padding::read(&mut scanner, &mut args)?;
        let conversion = scanner.take().ok_or(FormatError::Unterminated)?;
        let value = args.take(position)?;
        padding.append(&mut out, &convert(conversion, value, idents)?);
    }
    Ok(out)
}

/// What one conversion character does with the argument it has already read.
///
/// Already read, and that is the order to keep: a format string that is *both* short of
/// arguments and misspelled — `format('%d')` — is refused for the argument it could not
/// find, and a client matching on the message sees whichever refusal comes first.
fn convert(
    conversion: char,
    value: Option<String>,
    idents: &mut IdentifierCache,
) -> Result<String> {
    match conversion {
        // A NULL renders as nothing at all, which is what makes `format('%s', x)`
        // usable for building a comma-separated list.
        's' => Ok(value.unwrap_or_default()),
        'L' => Ok(match value {
            None => "NULL".to_string(),
            Some(text) => quote_literal_text(&text),
        }),
        'I' => match value {
            None => Err(FormatError::NullIdentifier.into()),
            Some(text) => idents.quote(&text),
        },
        other => Err(FormatError::UnrecognizedSpecifier(other).into()),
    }
}

/// A cursor over one format string's characters.
///
/// Over a `Vec<char>` and not the `&str`, because the grammar looks ahead and then backs
/// up: the digits of `12` are a width and the digits of `12$` a position, and only the
/// character after them says which, so the scan has to be undone when it is missing.
struct Scanner {
    chars: Vec<char>,
    at: usize,
}

impl Scanner {
    fn new(fmt: &str) -> Self {
        Self {
            chars: fmt.chars().collect(),
            at: 0,
        }
    }

    /// The next character, consumed.
    fn take(&mut self) -> Option<char> {
        let next = self.chars.get(self.at).copied()?;
        self.at += 1;
        Some(next)
    }

    /// Consume the next character if it is `expected`.
    fn take_if(&mut self, expected: char) -> bool {
        if self.chars.get(self.at) != Some(&expected) {
            return false;
        }
        self.at += 1;
        true
    }

    /// Consume every `expected` at the cursor, answering whether there was one.
    fn take_all(&mut self, expected: char) -> bool {
        let mut seen = false;
        while self.take_if(expected) {
            seen = true;
        }
        seen
    }

    /// The digits at the cursor, as the number they spell.
    ///
    /// `None` for no digits *and* for digits too wide for an `i64` — a width no output could
    /// reach, which this reads as no width at all rather than refusing.
    fn take_digits(&mut self) -> Option<i64> {
        let end = self.end_of_digits();
        if end == self.at {
            return None;
        }
        let digits: String = self.chars[self.at..end].iter().collect();
        self.at = end;
        digits.parse::<i64>().ok()
    }

    /// Read a `[digits$]` prefix, leaving the cursor where it was if there is no `$`.
    fn take_position(&mut self) -> Result<Option<usize>> {
        let end = self.end_of_digits();
        if end == self.at || self.chars.get(end) != Some(&'$') {
            return Ok(None);
        }
        let digits: String = self.chars[self.at..end].iter().collect();
        // A position too wide for a `usize` is a position there is no argument at.
        let position = digits
            .parse::<usize>()
            .map_err(|_| DataFusionError::from(FormatError::TooFewArguments))?;
        if position == 0 {
            return Err(FormatError::ZeroPosition.into());
        }
        self.at = end + 1;
        Ok(Some(position))
    }

    fn end_of_digits(&self) -> usize {
        let mut end = self.at;
        while self.chars.get(end).is_some_and(char::is_ascii_digit) {
            end += 1;
        }
        end
    }
}

/// The field a converted value is placed in: how wide, and which side the spaces go.
struct Padding {
    width: i64,
    left_justify: bool,
}

impl Padding {
    /// Read `[-][width|*]`.
    ///
    /// A `*` takes the width from an argument — read *before* the one the conversion itself
    /// reads, because that is the order PostgreSQL consumes them in — and a negative width
    /// there means what the `-` flag means.
    fn read(scanner: &mut Scanner, args: &mut RowArguments<'_>) -> Result<Self> {
        let flagged = scanner.take_all('-');
        let width = if scanner.take_if('*') {
            Self::argument_width(scanner, args)?
        } else {
            scanner.take_digits().unwrap_or(0)
        };
        Ok(Self {
            width: width.abs(),
            left_justify: flagged || width < 0,
        })
    }

    fn argument_width(scanner: &mut Scanner, args: &mut RowArguments<'_>) -> Result<i64> {
        let position = scanner.take_position()?;
        match args.take(position)? {
            // PostgreSQL treats a NULL width as no width at all.
            None => Ok(0),
            Some(text) => text.trim().parse::<i64>().map_err(|_| {
                // The spelling as it arrived, spaces and all, which is what PostgreSQL
                // quotes back.
                DataFusionError::from(FormatError::NonIntegerWidth(text.clone()))
            }),
        }
    }

    /// Append `text` to `out`, padded to the width.
    ///
    /// Width counts characters and not bytes, because PostgreSQL's does — a format string
    /// aligning a column of names would otherwise ragged out on the first non-ASCII one.
    fn append(&self, out: &mut String, text: &str) {
        let padding = (self.width - text.chars().count() as i64).max(0) as usize;
        if self.left_justify {
            out.push_str(text);
            out.extend(std::iter::repeat_n(' ', padding));
            return;
        }
        out.extend(std::iter::repeat_n(' ', padding));
        out.push_str(text);
    }
}

/// The arguments one row's conversions read, and the cursor they read them in order from.
///
/// There is one cursor, and an explicit position moves it — measured against PostgreSQL
/// 16.15, where `format('%1$s-%1$s-%s', 'a', 'b')` is `a-a-b` and not `a-a-a`. So an
/// explicit position is "read from here", not "read this one and carry on where I was".
struct RowArguments<'a> {
    /// One already-rendered column per argument, in the order they were passed.
    columns: &'a [Vec<Option<String>>],
    row: usize,
    cursor: usize,
}

impl<'a> RowArguments<'a> {
    fn new(columns: &'a [Vec<Option<String>>], row: usize) -> Self {
        Self {
            columns,
            row,
            cursor: 0,
        }
    }

    /// The argument a conversion reads: the one `position` names, or the next one in order.
    fn take(&mut self, position: Option<usize>) -> Result<Option<String>> {
        let index = position.map_or(self.cursor, |position| position - 1);
        self.cursor = index + 1;
        let column = self
            .columns
            .get(index)
            .ok_or_else(|| DataFusionError::from(FormatError::TooFewArguments))?;
        Ok(column[self.row].clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{code_of_tagged_message, strip_code_tags};
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::Field;

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
    ///
    /// The SQLSTATE is the point of that last refusal: `22004` tells the client one row's
    /// value was null, which is a different fix from a bad format string.
    #[test]
    fn each_conversion_has_its_own_answer_for_null() {
        let null: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>]));
        assert_eq!(one("[%s]", vec![null.clone()]).expect("valid"), "[]");
        assert_eq!(one("[%L]", vec![null.clone()]).expect("valid"), "[NULL]");

        let err = one("[%I]", vec![null]).expect_err("a null identifier");
        assert!(
            strip_code_tags(&err.to_string()).contains("null values cannot be formatted"),
            "got: {err}"
        );
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

    /// The four refusals that are about the format string rather than the data. Each is
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

    /// A `*` width whose argument is not an integer is the integer input function's
    /// refusal, with the spelling that reached it.
    #[test]
    fn a_non_integer_star_width_is_refused_as_an_integer_would_be() {
        let err = one("[%*s]", vec![text(&["wide"]), text(&["ab"])]).expect_err("not a width");
        assert!(
            strip_code_tags(&err.to_string())
                .contains("invalid input syntax for type integer: \"wide\""),
            "got: {err}"
        );
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
        for (input, expected) in [
            ("simple", "simple"),
            ("with_underscore_1", "with_underscore_1"),
            ("select", r#""select""#),
            ("Mixed", r#""Mixed""#),
            ("with space", r#""with space""#),
            ("with\"quote", r#""with""quote""#),
        ] {
            assert_eq!(
                one("%I", vec![text(&[input])]).expect("valid"),
                expected,
                "%I disagreed for {input}"
            );
        }
        // The same identifier twice reads the memoized answer the second time, which has to
        // be the same answer.
        assert_eq!(
            one("%I.%I", vec![text(&["Mixed"]), text(&["Mixed"])]).expect("valid"),
            r#""Mixed"."Mixed""#
        );
    }
}
