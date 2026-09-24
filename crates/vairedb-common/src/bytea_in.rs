//! PostgreSQL's `bytea` input conversion — the one that turns the *text* `\xDEADBEEF`
//! into the four bytes `DE AD BE EF`.
//!
//! `'\xDEADBEEF'::bytea` is four bytes in PostgreSQL and **ten** in VaireDB, because
//! neither engine underneath reads a string into a byte string the way PostgreSQL does:
//! Arrow's `Utf8` → `Binary` cast copies the characters, and DuckDB's `VARCHAR` → `BLOB`
//! cast reads its own escape syntax, where `\xDE` is one byte and the trailing `ADBEEF`
//! is six characters. Measured on PostgreSQL 17, DataFusion 54.1 and DuckDB 1.5.5:
//!
//! | expression | PostgreSQL | DataFusion | DuckDB |
//! |---|---|---|---|
//! | `'\xDEADBEEF'::bytea` | 4 bytes, `\xdeadbeef` | 10 bytes, the text | 7 bytes |
//! | `'a\101b'::bytea` | 3 bytes, `aAb` | 6 bytes, the text | 6 bytes |
//! | `'abc'::bytea` | 3 bytes, `abc` | 3 bytes, `abc` | 3 bytes |
//!
//! The third row is why this is the worst kind of divergence rather than an obvious
//! breakage: the plain form agrees, so a client that round-trips ASCII sees nothing wrong,
//! and only the hex form — the one every driver and every `pg_dump` uses — silently stores
//! or returns the wrong bytes. Nothing in the answer says so.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! `byteain` reads one of two formats, chosen by the first two characters:
//!
//! * **hex**, when the text starts with `\x` — pairs of hexadecimal digits, where
//!   whitespace is allowed *between* pairs but not inside one. `'\x de ad'` is two bytes
//!   and `'\x d e'` is an error. Only space, newline, tab and carriage return are skipped:
//!   a form feed is not, and neither is a non-breaking space. `'\x'` is the empty
//!   `bytea`, and the `x` must be lowercase — `'\X41'` is not the hex format at all.
//! * **escape**, otherwise — every byte is itself, except `\\`, which is one backslash,
//!   and `\NNN`, which is the byte with that three-digit octal value. Nothing else after
//!   a backslash is valid: `\12` (two digits), `\400` (out of range), `\x41` (inside the
//!   escape format) and a trailing `\` are all errors.
//!
//! The two error classes are PostgreSQL's own and are not the same SQLSTATE, which is why
//! [`ByteaInputError`] carries the code rather than only the message: a malformed escape
//! is `22P02` and a malformed hex body is `22023`.
//!
//! ## Why a shared decoder and not two
//!
//! Both paths need the same answer and neither engine can produce it:
//!
//! * The **read path** rewrites `x::bytea` into a call of [`BYTEA_IN_UDF_NAME`]
//!   ([`vairedb_coordinator::pgwire_handler::pg_operators`]), so the rule runs as Rust on
//!   whichever node executes the projection.
//! * The **write path** renders SQL text for a shard's DuckDB, which has no function that
//!   reads PostgreSQL's format. So the coordinator decodes the literal itself and emits
//!   `unhex('…')` — DuckDB's `X'…'` is a `VARCHAR`, not a `BLOB`, so `unhex` is the only
//!   spelling that produces bytes — and refuses what it cannot decode
//!   ([`vairedb_coordinator::write_sql_cl`]).
//!
//! One decoder means the two paths cannot disagree about the same literal, which is the
//! property that matters here: a value written by an `INSERT` and read back by a `SELECT`
//! goes through both.

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::columns::strings;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name the read path emits and every node resolves the call by.
///
/// Prefixed rather than spelled `byteain`, on the same rule as the rest of
/// [`crate::float_div`]: nothing a client writes produces this name, so it cannot shadow a
/// PostgreSQL function with different behaviour.
pub const BYTEA_IN_UDF_NAME: &str = "vaire_bytea_in";

/// Why a string is not a `bytea`, in PostgreSQL's own words and with PostgreSQL's own
/// SQLSTATE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteaInputError {
    /// The escape format could not be read — `22P02`, PostgreSQL's
    /// `invalid_text_representation`. Deliberately without the offending text, because
    /// PostgreSQL leaves it out too: a `bytea` input is often a secret.
    Syntax,
    /// A character in the hex body that is not a hexadecimal digit — `22023`.
    HexDigit(char),
    /// A hex body with a digit left over — `22023`.
    OddDigits,
}

impl ByteaInputError {
    /// The `VdbErrorCode` whose SQLSTATE PostgreSQL raises for this input.
    ///
    /// Two codes and not one, because PostgreSQL raises two: `byteain` itself reports
    /// `22P02` and the hex decoder it calls reports `22023`. Measured, not inferred —
    /// `'\xzz'::bytea` is `22023` and `'a\12'::bytea` is `22P02`.
    pub fn error_code(&self) -> VdbErrorCode {
        match self {
            ByteaInputError::Syntax => VdbErrorCode::InvalidTextRepresentation,
            ByteaInputError::HexDigit(_) | ByteaInputError::OddDigits => {
                VdbErrorCode::InvalidParameterValue
            }
        }
    }
}

impl fmt::Display for ByteaInputError {
    /// PostgreSQL 17's exact wording, which is what
    /// `vairedb_coordinator::pgwire_handler::error_enrichment` classifies an error raised
    /// inside an executor by — the variant does not survive the Ballista boundary but the
    /// rendered message does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteaInputError::Syntax => write!(f, "invalid input syntax for type bytea"),
            ByteaInputError::HexDigit(c) => write!(f, "invalid hexadecimal digit: \"{c}\""),
            ByteaInputError::OddDigits => {
                write!(f, "invalid hexadecimal data: odd number of digits")
            }
        }
    }
}

impl std::error::Error for ByteaInputError {}

/// How a decode failure leaves this module: an `Execution` error whose message is
/// PostgreSQL's wording and nothing else.
///
/// Deliberately **untagged**, where [`crate::uuid_in`] and [`crate::json_pg`] wrap their
/// message in [`crate::error::tagged_message`] so the code travels as a `[VDB-…]` prefix.
/// Here the wording *is* the transport: [`error_code_of_message`] below reads the code back
/// out of it and is registered as this family's classifier in
/// [`crate::distributed_functions`], which pins the exact messages on both sides of the
/// Ballista boundary. A tag would add nothing the classifier does not already recover, and
/// would change the text those tests — and the coordinator's — read.
///
/// One impl rather than a `map_err` at each call site, so the choice is made once and is
/// documented where the type is.
impl From<ByteaInputError> for DataFusionError {
    fn from(e: ByteaInputError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

/// The `VdbErrorCode` for a decode failure recognized by its *message*, or `None` if `msg`
/// is not one of them.
///
/// The read path's UDF runs inside a Ballista executor, and an error raised there reaches
/// the coordinator as text: the scheduler renders the whole failure into a string and the
/// variant is gone. So the only way for a `SELECT '\xzz'::bytea` to carry the same SQLSTATE
/// as the `INSERT` the write path refuses at parse time is to read the message back — which
/// is why this lives here, beside the [`fmt::Display`] impl that writes it, rather than as a
/// third copy of the wording in the coordinator's classifier.
///
/// Matching is `contains`, because the message arrives wrapped in several layers of
/// `Debug` (`… Execution("DataFusionError(Execution(\"invalid hexadecimal digit: …\"))")`),
/// and case-insensitive, because nothing guarantees the wrappers leave the case alone.
pub fn error_code_of_message(msg: &str) -> Option<VdbErrorCode> {
    let lower = msg.to_lowercase();
    for candidate in [
        ByteaInputError::HexDigit('0'),
        ByteaInputError::OddDigits,
        ByteaInputError::Syntax,
    ] {
        // The hex-digit message ends in the offending character, so its stable part is
        // everything up to the quote.
        let wording = candidate.to_string();
        let stable = wording.split(':').next().unwrap_or(&wording).to_lowercase();
        if lower.contains(&stable) {
            return Some(candidate.error_code());
        }
    }
    None
}

/// Decode `text` into the bytes PostgreSQL's `byteain` would produce.
///
/// The one implementation of the rule, called by the UDF below on the read path and by the
/// write path's literal translation.
pub fn decode(text: &str) -> std::result::Result<Vec<u8>, ByteaInputError> {
    match text.strip_prefix("\\x") {
        Some(hex) => decode_hex(hex),
        None => decode_escape(text.as_bytes()),
    }
}

/// The hex format's body — everything after the `\x`.
///
/// Whitespace is skipped only where a pair *begins*, which is what makes `'\x de ad'` two
/// bytes and `'\x d e'` an error. Both were measured; PostgreSQL's decoder skips
/// whitespace once per byte rather than once per digit.
fn decode_hex(body: &str) -> std::result::Result<Vec<u8>, ByteaInputError> {
    let mut out = Vec::with_capacity(body.len() / 2);
    let mut chars = body.chars();
    loop {
        // Only these four. A form feed and a vertical tab are *not* skipped by
        // PostgreSQL, and neither is any non-ASCII space — measured, because
        // `char::is_whitespace` would have accepted all three.
        let Some(high) = chars.find(|c| !matches!(c, ' ' | '\n' | '\t' | '\r')) else {
            return Ok(out);
        };
        let high = hex_digit(high)?;
        let Some(low) = chars.next() else {
            return Err(ByteaInputError::OddDigits);
        };
        out.push((high << 4) | hex_digit(low)?);
    }
}

/// One hexadecimal digit's value, in either letter case.
fn hex_digit(c: char) -> std::result::Result<u8, ByteaInputError> {
    c.to_digit(16)
        .map(|d| d as u8)
        .ok_or(ByteaInputError::HexDigit(c))
}

/// The escape format: `\\` for one backslash, `\NNN` for an octal byte, everything else
/// as itself.
///
/// Over bytes and not characters, because that is what PostgreSQL does: a multi-byte
/// character is stored as its UTF-8 bytes, so `'é'::bytea` is two bytes.
fn decode_escape(bytes: &[u8]) -> std::result::Result<Vec<u8>, ByteaInputError> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    while let Some((&byte, tail)) = rest.split_first() {
        if byte != b'\\' {
            out.push(byte);
            rest = tail;
            continue;
        }
        let (escaped, width) = escaped_byte(tail)?;
        out.push(escaped);
        rest = &tail[width..];
    }
    Ok(out)
}

/// One escape sequence, read from the bytes that *follow* a backslash: the byte it stands
/// for and how many of them it spans.
///
/// Every spelling other than these two is an error, which is what makes `\12` (two digits),
/// `\400` (out of range), `\x41` (the hex format's marker inside the escape format) and a
/// trailing `\` all `22P02` in PostgreSQL.
fn escaped_byte(after_backslash: &[u8]) -> std::result::Result<(u8, usize), ByteaInputError> {
    match *after_backslash {
        [b'\\', ..] => Ok((b'\\', 1)),
        // Three octal digits, the first no higher than `3` so the value fits a byte.
        // `\400` is an error in PostgreSQL rather than a wrapped `\000`.
        [high @ b'0'..=b'3', mid @ b'0'..=b'7', low @ b'0'..=b'7', ..] => {
            let byte = (high - b'0') << 6 | (mid - b'0') << 3 | (low - b'0');
            Ok((byte, 3))
        }
        // Including a trailing backslash, which is the empty slice here.
        _ => Err(ByteaInputError::Syntax),
    }
}

/// Register PostgreSQL's `bytea` input conversion on `registry`.
///
/// Call this on every context that plans **or** executes a read, for the reason
/// [`crate::float_div::register_float_division`] spells out: a scalar function crosses the
/// Ballista wire as a name with no definition attached.
pub fn register_bytea_in(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(bytea_in_udf())?;
    Ok(())
}

/// The one [`ScalarUDF`] handle, built once however many contexts register it.
///
/// Not public: the read path builds the call from [`BYTEA_IN_UDF_NAME`] and every node
/// resolves it from its own registry, so nothing outside needs the handle itself.
fn bytea_in_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(ByteaIn::new()))))
}

/// `vaire_bytea_in(text)` — PostgreSQL's `::bytea` cast of a string.
///
/// Private, like every other UDF type in this crate: a caller reaches the function through
/// [`register_bytea_in`] and the name, never through the type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ByteaIn {
    signature: Signature,
}

impl ByteaIn {
    fn new() -> Self {
        Self {
            // User-defined, so [`ScalarUDFImpl::coerce_types`] can accept exactly what
            // PostgreSQL accepts and refuse the rest with PostgreSQL's own words. A
            // built-in signature would either coerce a number to a string — making
            // `1::bytea` succeed where PostgreSQL raises `42846` — or reject a `bytea`
            // argument, which PostgreSQL passes through.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ByteaIn {
    fn name(&self) -> &str {
        BYTEA_IN_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The two argument types this reduces to: a string to decode, or bytes to pass
    /// through.
    ///
    /// `bytea` is what PostgreSQL casts to itself — `col::bytea` on a `bytea` column is
    /// valid and does nothing — and everything else is refused, because PostgreSQL has no
    /// cast to `bytea` from any other type.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{BYTEA_IN_UDF_NAME} takes one argument, got {}",
                arg_types.len()
            );
        };
        match arg {
            // `Null` is a bare `NULL::bytea`, which is a NULL `bytea`; decoding a column
            // of nothing gives a column of nothing.
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(vec![DataType::Utf8])
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(vec![DataType::Binary])
            }
            other => plan_err!("cannot cast type {other} to bytea"),
        }
    }

    /// Always `bytea`, which this codebase stores as Arrow `Binary`.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!(
                "{BYTEA_IN_UDF_NAME} takes one argument, got {}",
                args.args.len()
            );
        };
        // Against `number_rows`, so a batch of no rows stays one: a literal argument would
        // otherwise expand to a single row and raise for a statement that selected nothing.
        let arg = arg.to_array(args.number_rows)?;
        match arg.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(decode_array(&arg)?)),
            // Already `bytea`. PostgreSQL's `bytea::bytea` is the value unchanged.
            DataType::Binary => Ok(ColumnarValue::Array(arg)),
            other => exec_err!("cannot cast type {other} to bytea"),
        }
    }
}

/// Decode every row of a text array, keeping NULLs NULL.
///
/// Strict, like PostgreSQL's cast: a NULL never reaches the decoder, so a column with one
/// NULL and no bad values raises nothing.
///
/// The narrowing to `Utf8` is [`strings`]: `coerce_types` has already asked for `Utf8`, but
/// that is not enough on its own to make a view or dictionary layout unreachable here.
fn decode_array(text: &ArrayRef) -> Result<ArrayRef> {
    let text = strings(text)?;
    let mut out = BinaryBuilder::with_capacity(text.len(), text.value_data().len());
    for value in text.iter() {
        match value {
            // `?`, so the wording reaching the client is the one the [`From`] impl above
            // chose — the wording is what maps back to PostgreSQL's SQLSTATE once the error
            // crosses the wire as text.
            Some(value) => out.append_value(decode(value)?),
            None => out.append_null(),
        }
    }
    Ok(Arc::new(out.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray};
    use datafusion::execution::context::SessionContext;

    /// Decode, or the message PostgreSQL would have printed.
    fn decoded(text: &str) -> std::result::Result<Vec<u8>, String> {
        decode(text).map_err(|e| e.to_string())
    }

    /// The gap itself, in the spelling every driver and `pg_dump` uses: four bytes, not
    /// the ten characters of the text.
    #[test]
    fn the_hex_format_decodes_to_bytes() {
        assert_eq!(decoded("\\xDEADBEEF"), Ok(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(decoded("\\xdeadbeef"), Ok(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(decoded("\\x"), Ok(vec![]));
        assert_eq!(decoded(""), Ok(vec![]));
    }

    /// Measured against PostgreSQL 17: whitespace between pairs is skipped, whitespace
    /// inside one is an error, and only four whitespace characters count.
    #[test]
    fn hex_whitespace_is_skipped_only_between_pairs() {
        assert_eq!(decoded("\\x de ad"), Ok(vec![0xDE, 0xAD]));
        assert_eq!(decoded("\\x41\n"), Ok(vec![0x41]));
        assert_eq!(decoded("\\x\t41\r\n"), Ok(vec![0x41]));
        assert_eq!(
            decoded("\\x d e"),
            Err("invalid hexadecimal digit: \" \"".to_string())
        );
        assert_eq!(
            decoded("\\x4\u{c}1"),
            Err("invalid hexadecimal digit: \"\u{c}\"".to_string())
        );
        assert_eq!(
            decoded("\\x\u{a0}41"),
            Err("invalid hexadecimal digit: \"\u{a0}\"".to_string())
        );
    }

    /// Measured against PostgreSQL 17, including the two messages and which of them is
    /// which — the odd-digit check fires only after a valid first digit.
    #[test]
    fn a_malformed_hex_body_reports_postgresqls_message() {
        assert_eq!(
            decoded("\\xdeadbee"),
            Err("invalid hexadecimal data: odd number of digits".to_string())
        );
        assert_eq!(
            decoded("\\xzz"),
            Err("invalid hexadecimal digit: \"z\"".to_string())
        );
        assert_eq!(
            decoded("\\x4z"),
            Err("invalid hexadecimal digit: \"z\"".to_string())
        );
        assert_eq!(
            ByteaInputError::OddDigits.error_code(),
            VdbErrorCode::InvalidParameterValue
        );
        assert_eq!(
            ByteaInputError::HexDigit('z').error_code(),
            VdbErrorCode::InvalidParameterValue
        );
    }

    /// The `x` has to be lowercase: `'\X41'` is not the hex format, and `\X` is then not a
    /// valid escape either.
    #[test]
    fn an_uppercase_x_is_not_the_hex_format() {
        assert_eq!(
            decoded("\\X41"),
            Err("invalid input syntax for type bytea".to_string())
        );
    }

    /// The escape format, where the plain case is the one that agrees with both engines
    /// and hides the rest.
    #[test]
    fn the_escape_format_reads_backslashes_and_octal() {
        assert_eq!(decoded("abc"), Ok(b"abc".to_vec()));
        assert_eq!(decoded("a\\\\b"), Ok(b"a\\b".to_vec()));
        assert_eq!(decoded("a\\101b"), Ok(b"aAb".to_vec()));
        assert_eq!(decoded("\\000"), Ok(vec![0]));
        assert_eq!(decoded("\\377"), Ok(vec![0xFF]));
        // Not characters: a multi-byte character is its UTF-8 bytes, as in PostgreSQL.
        assert_eq!(decoded("é"), Ok(vec![0xC3, 0xA9]));
    }

    /// Measured against PostgreSQL 17: every one of these is `22P02`, and the message
    /// carries no part of the input.
    #[test]
    fn a_malformed_escape_is_a_syntax_error() {
        for text in ["a\\400", "a\\12", "a\\", "a\\x41", "\\8", "a\\1x1"] {
            assert_eq!(
                decoded(text),
                Err("invalid input syntax for type bytea".to_string()),
                "`{text}`"
            );
        }
        assert_eq!(
            ByteaInputError::Syntax.error_code(),
            VdbErrorCode::InvalidTextRepresentation
        );
    }

    /// The same two codes, recovered from a message that has crossed the Ballista boundary
    /// and come back wrapped in `Debug` layers — which is the only form the coordinator
    /// gets for an error the read path's UDF raised inside an executor.
    #[test]
    fn a_transported_message_still_carries_its_code() {
        let transported = |text: &str| {
            format!(
                "Job abc failed: … runtime execution error: \
                 DataFusionError(Execution(\"DataFusionError(Execution(\\\"{text}\\\"))\"))"
            )
        };
        assert_eq!(
            error_code_of_message(&transported("invalid hexadecimal digit: \\\"z\\\"")),
            Some(VdbErrorCode::InvalidParameterValue)
        );
        assert_eq!(
            error_code_of_message(&transported(
                "invalid hexadecimal data: odd number of digits"
            )),
            Some(VdbErrorCode::InvalidParameterValue)
        );
        assert_eq!(
            error_code_of_message(&transported("invalid input syntax for type bytea")),
            Some(VdbErrorCode::InvalidTextRepresentation)
        );
        // Every message the decoder can write is recognized, whatever the input was.
        for text in ["\\xzz", "\\xdeadbee", "a\\12"] {
            let e = decode(text).expect_err("must not decode");
            assert_eq!(
                error_code_of_message(&e.to_string()),
                Some(e.error_code()),
                "`{text}`"
            );
        }
        // And nothing else is claimed.
        assert_eq!(error_code_of_message("division by zero"), None);
        assert_eq!(
            error_code_of_message("Job abc failed: stage 1 failed"),
            None
        );
    }

    /// Invoke the function the way a physical expression does, over a whole column.
    fn invoke_column(column: ArrayRef) -> Result<ArrayRef> {
        let rows = column.len();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(column)],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(arrow::datatypes::Field::new("b", DataType::Binary, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        ByteaIn::new().invoke_with_args(args)?.to_array(rows)
    }

    /// The decoded bytes of a column of text, row by row.
    fn invoke(text: Vec<Option<&str>>) -> Result<Vec<Option<Vec<u8>>>> {
        let out = invoke_column(Arc::new(StringArray::from(text)))?;
        Ok(out
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Binary")
            .iter()
            .map(|v| v.map(<[u8]>::to_vec))
            .collect())
    }

    /// A whole column at once: a NULL stays NULL rather than raise, one bad row fails the
    /// batch with the message that classifies back into PostgreSQL's SQLSTATE, and a batch
    /// of no rows raises nothing — so `SELECT s::bytea` over an empty table answers no rows
    /// the way PostgreSQL does.
    #[test]
    fn a_column_decodes_row_by_row_and_keeps_nulls() {
        assert_eq!(
            invoke(vec![Some("\\x41"), None, Some("b")]).expect("should not raise"),
            vec![Some(vec![0x41]), None, Some(b"b".to_vec())]
        );
        let err = invoke(vec![Some("\\x41"), Some("\\xzz")]).expect_err("should have raised");
        assert!(
            err.to_string().contains("invalid hexadecimal digit"),
            "got: {err}"
        );
        assert_eq!(invoke(vec![]).expect("should not raise"), vec![]);
    }

    /// `bytea::bytea` is the value unchanged, which is what PostgreSQL does with it.
    #[test]
    fn bytes_pass_through_unchanged() {
        let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0xDEu8, 0xAD][..]), None]));
        let out = invoke_column(Arc::clone(&bytes)).expect("should not raise");
        assert_eq!(&out, &bytes);
    }

    /// The types PostgreSQL accepts, and the refusal for the rest in PostgreSQL's words.
    #[test]
    fn only_a_string_or_bytes_can_be_cast_to_bytea() {
        let f = ByteaIn::new();
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
        for accepted in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ] {
            assert_eq!(
                f.coerce_types(std::slice::from_ref(&accepted))
                    .expect("accepted"),
                vec![DataType::Binary],
                "{accepted}"
            );
        }
        let err = f
            .coerce_types(&[DataType::Int64])
            .expect_err("an integer has no cast to bytea");
        assert!(err.to_string().contains("cannot cast type"), "got: {err}");
        assert_eq!(
            f.return_type(&[DataType::Utf8]).expect("bytea"),
            DataType::Binary
        );
    }

    /// The wire carries only the name, so the name has to resolve after registration, and
    /// registering twice is what a context reached by two paths does.
    #[test]
    fn the_function_resolves_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        assert!(ctx.udf(BYTEA_IN_UDF_NAME).is_err());
        register_bytea_in(&mut ctx).expect("registration failed");
        register_bytea_in(&mut ctx).expect("second registration failed");
        assert!(ctx.udf(BYTEA_IN_UDF_NAME).is_ok());
    }
}
