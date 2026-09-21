//! PostgreSQL's `uuid` input conversion — the one behind `'a0ee…'::uuid`.
//!
//! ```text
//! spelling                                        PostgreSQL              before
//! 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid    lower case, hyphenated  0A000
//! 'a0eebc999c0b4ef8bb6d6bb9bd380a11'::uuid        hyphens added           0A000
//! 'notauuid'::uuid                                22P02                   0A000
//! ```
//!
//! A `UUID` **column** already works: `vairedb_coordinator::column_types` reads DuckDB's
//! `UUID` back as Arrow `Utf8` in the canonical spelling, and a client is told `text`. So —
//! exactly as for [`crate::json_pg`] — the cast changes no representation and only checks
//! and canonicalizes one, which is what PostgreSQL's `uuid_in` does and Arrow has no cast
//! for. DataFusion's `convert_data_type` has no arm for the type name, so the cast does not
//! plan at all; the read path rewrites it into a call of [`UUID_IN_UDF_NAME`] the way it
//! rewrites `::bytea` and `::json`, and the same registration rule applies — every context
//! that plans or executes.
//!
//! ## The accepted spellings, read from PostgreSQL's own `string_to_uuid`
//!
//! Wider than the canonical form and narrower than "any 32 hex digits with punctuation":
//!
//! * an optional `{`…`}` around the whole value — and, if it opens, it must close;
//! * 32 hexadecimal digits in either case;
//! * a hyphen allowed after any **even** number of bytes except the last — so after 4, 8,
//!   12, 16, 20, 24 and 28 digits. The canonical form's four hyphens are four of those
//!   positions, `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`; every hyphen is independently
//!   optional, so the unhyphenated form is accepted too, and so are the mixed forms
//!   PostgreSQL accepts and nobody writes.
//!
//! Everything else is `22P02` — including a hyphen in an odd position, which is the rule a
//! "strip the hyphens and count" reading would get wrong.
//!
//! The result is always the canonical rendering: lower case, hyphens in the standard four
//! places. That is what makes the cast useful beyond validation — two spellings of one UUID
//! compare equal after it, which is the whole point of the type.

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray, StringArray, StringBuilder};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name the read path emits for `::uuid` and every node resolves the call by.
pub const UUID_IN_UDF_NAME: &str = "vaire_uuid_in";

/// A string that is not a UUID — `22P02`, PostgreSQL's `invalid_text_representation`.
///
/// With the offending text, because PostgreSQL prints it here: a UUID is an identifier
/// rather than a secret, and the whole message is `invalid input syntax for type uuid:
/// "notauuid"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UuidInputError {
    pub text: String,
}

impl fmt::Display for UuidInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid input syntax for type uuid: \"{}\"", self.text)
    }
}

impl std::error::Error for UuidInputError {}

impl UuidInputError {
    /// PostgreSQL's SQLSTATE for it.
    pub fn error_code(&self) -> VdbErrorCode {
        VdbErrorCode::InvalidTextRepresentation
    }
}

impl From<UuidInputError> for DataFusionError {
    fn from(e: UuidInputError) -> Self {
        // Tagged, so the code survives being rendered to text by the Ballista scheduler:
        // this runs wherever the projection runs. See [`crate::error::tagged_message`].
        DataFusionError::Execution(tagged_message(e.error_code(), e))
    }
}

/// Canonicalize `text` the way PostgreSQL's `uuid_in` does, or say why it is not a UUID.
///
/// The one implementation of the rule; see the module doc for the spellings.
pub fn canonicalize(text: &str) -> std::result::Result<String, UuidInputError> {
    let invalid = || UuidInputError {
        text: text.to_string(),
    };
    let braced = text.starts_with('{');
    let body = if braced {
        text.strip_prefix('{')
            .and_then(|body| body.strip_suffix('}'))
            .ok_or_else(invalid)?
    } else {
        text
    };

    let mut digits = String::with_capacity(32);
    let mut rest = body.chars();
    for byte in 0..16 {
        let (high, low) = (rest.next(), rest.next());
        let (Some(high), Some(low)) = (high, low) else {
            return Err(invalid());
        };
        if !high.is_ascii_hexdigit() || !low.is_ascii_hexdigit() {
            return Err(invalid());
        }
        digits.push(high.to_ascii_lowercase());
        digits.push(low.to_ascii_lowercase());
        // A hyphen may follow every second byte, but not the last one — which is where
        // PostgreSQL allows it, and only there. Optional at each such position.
        if byte % 2 == 1 && byte < 15 {
            let mut lookahead = rest.clone();
            if lookahead.next() == Some('-') {
                rest = lookahead;
            }
        }
    }
    if rest.next().is_some() {
        return Err(invalid());
    }

    // The canonical rendering, whatever the input's punctuation was.
    let mut out = String::with_capacity(36);
    for (position, digit) in digits.chars().enumerate() {
        if matches!(position, 8 | 12 | 16 | 20) {
            out.push('-');
        }
        out.push(digit);
    }
    Ok(out)
}

/// Register PostgreSQL's `uuid` input conversion on `registry`.
///
/// Call this on every context that plans **or** executes a read; see the module doc.
pub fn register_uuid_in(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(uuid_in_udf())?;
    Ok(())
}

/// The shared [`ScalarUDF`] handle, for the read-path rewrite that builds the call.
pub fn uuid_in_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(UuidIn::new()))))
}

/// `vaire_uuid_in(text)` — PostgreSQL's `::uuid` cast of a string.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UuidIn {
    signature: Signature,
}

impl Default for UuidIn {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidIn {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UuidIn {
    fn name(&self) -> &str {
        UUID_IN_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A string, or nothing at all — PostgreSQL has no cast to `uuid` from any other type.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return plan_err!(
                "{UUID_IN_UDF_NAME} takes one argument, got {}",
                arg_types.len()
            );
        };
        match arg {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                Ok(vec![DataType::Utf8])
            }
            other => plan_err!("cannot cast type {other} to uuid"),
        }
    }

    /// `Utf8`, which is what a `UUID` column reads back as — see the module doc.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!(
                "{UUID_IN_UDF_NAME} takes one argument, got {}",
                args.args.len()
            );
        };
        // Against `number_rows`, so a batch of no rows stays one.
        let text = strings(&arg.to_array(args.number_rows)?)?;
        let mut out = StringBuilder::with_capacity(text.len(), text.len() * 36);
        for value in text.iter() {
            match value {
                Some(value) => out.append_value(canonicalize(value)?),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// One argument as a `Utf8` array, tolerating a view type [`ScalarUDFImpl::coerce_types`]
/// asked to be rewritten away.
fn strings(array: &ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().clone()),
        _ => Ok(cast(array, &DataType::Utf8)?.as_string::<i32>().clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    const CANONICAL: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

    fn read(text: &str) -> std::result::Result<String, String> {
        canonicalize(text).map_err(|e| e.to_string())
    }

    /// Every spelling PostgreSQL accepts comes back as the one canonical form, which is what
    /// makes two of them compare equal.
    #[test]
    fn the_accepted_spellings_all_canonicalize() {
        for text in [
            CANONICAL,
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "{a0eebc999c0b4ef8bb6d6bb9bd380a11}",
            // A hyphen is allowed after any even byte count, which is wider than the
            // canonical four positions — measured from PostgreSQL's `string_to_uuid`.
            "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
            "a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11",
        ] {
            assert_eq!(read(text), Ok(CANONICAL.to_string()), "`{text}`");
        }
        assert_eq!(
            read("00000000-0000-0000-0000-000000000000"),
            Ok("00000000-0000-0000-0000-000000000000".to_string())
        );
    }

    /// The message is PostgreSQL's, value included, and the class is `22P02`.
    #[test]
    fn a_string_that_is_not_a_uuid_is_invalid_input_syntax() {
        for text in [
            "notauuid",
            "",
            // One digit short, one digit long.
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111",
            // A hyphen in an odd position, which a "strip the hyphens" reading would accept.
            "a0e-ebc999c0b4ef8bb6d6bb9bd380a11",
            // An unbalanced brace, and a brace in the middle.
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            // A non-hex digit, and a non-ASCII one `char::is_alphanumeric` would allow.
            "z0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1٢",
        ] {
            assert_eq!(
                read(text),
                Err(format!("invalid input syntax for type uuid: \"{text}\"")),
                "`{text}`"
            );
        }
        assert_eq!(
            UuidInputError {
                text: "x".to_string()
            }
            .error_code(),
            VdbErrorCode::InvalidTextRepresentation
        );
    }

    /// The SQLSTATE has to survive the executor boundary, which it does by being written
    /// into the message.
    #[test]
    fn the_error_carries_its_sqlstate_in_the_message() {
        let e = DataFusionError::from(UuidInputError {
            text: "oops".to_string(),
        });
        assert_eq!(
            crate::error::code_of_tagged_message(&e.to_string()),
            Some(VdbErrorCode::InvalidTextRepresentation)
        );
        assert!(e.to_string().contains("\"oops\""), "got: {e}");
    }

    /// Invoke the function the way a physical expression does, over a whole column.
    fn invoke(text: Vec<Option<&str>>) -> Result<Vec<Option<String>>> {
        let rows = text.len();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(text)))],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(arrow::datatypes::Field::new("u", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = UuidIn::new().invoke_with_args(args)?.to_array(rows)?;
        Ok(out
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect())
    }

    /// A whole column at once, with a NULL that must stay NULL rather than raise.
    #[test]
    fn a_column_is_read_row_by_row_and_keeps_nulls() {
        assert_eq!(
            invoke(vec![Some("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"), None]).expect("valid"),
            vec![Some(CANONICAL.to_string()), None]
        );
        let err = invoke(vec![Some(CANONICAL), Some("oops")]).expect_err("one bad row fails");
        assert!(
            err.to_string()
                .contains("invalid input syntax for type uuid"),
            "got: {err}"
        );
        assert_eq!(invoke(vec![]).expect("valid"), vec![]);
    }

    #[test]
    fn only_a_string_can_be_cast_to_uuid() {
        let f = UuidIn::new();
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
            .expect_err("an integer has no cast to uuid");
        assert!(
            err.to_string().contains("cannot cast type Int64 to uuid"),
            "got: {err}"
        );
        assert_eq!(
            f.return_type(&[DataType::Utf8]).expect("text"),
            DataType::Utf8
        );
    }

    /// The wire carries only the name, so the name has to resolve after registration, and
    /// registering twice is what a context reached by two paths does.
    #[test]
    fn the_function_resolves_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        assert!(ctx.udf(UUID_IN_UDF_NAME).is_err());
        register_uuid_in(&mut ctx).expect("registration failed");
        register_uuid_in(&mut ctx).expect("second registration failed");
        assert!(ctx.udf(UUID_IN_UDF_NAME).is_ok());
    }
}
