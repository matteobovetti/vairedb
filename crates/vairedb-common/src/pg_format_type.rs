//! PostgreSQL's `format_type(oid, typemod)`, with the argument spellings PostgreSQL
//! accepts and `datafusion-pg-catalog`'s own signature does not.
//!
//! ```text
//! spelling                    PostgreSQL                  before
//! format_type(23, NULL)       integer                     integer
//! format_type('23', 0)        integer                     42883, at planning time
//! format_type('23', '-1')     integer                     42883, at planning time
//! format_type('abc', 0)       22P02                       42883, at planning time
//! ```
//!
//! Upstream's `FormatTypeUDF` declares `Signature::one_of` over four `Exact` arms —
//! `(Int32|Int64, Int32|Int64)` — and DataFusion has no implicit string-to-integer
//! coercion, so a string argument fails to resolve the function at all. PostgreSQL has
//! no such arm either: `format_type` is declared `(oid, integer)` once, and `'23'` reaches
//! it because an *unknown-typed literal* is resolved to the parameter's type. The
//! difference is whose job the conversion is, and in PostgreSQL it is the type system's.
//!
//! A client meets this through a tool rather than by writing it: a driver that builds an
//! introspection query by string interpolation quotes its OIDs, and `format_type('23', 0)`
//! is what arrives. The value it wants back is the one upstream already computes correctly,
//! which is why this is a widening of the signature and not a second implementation —
//! `FormatType` converts the arguments and delegates every rendering decision to
//! upstream's `format_type` UDF.
//!
//! ## Why the conversion is not left to DataFusion's cast
//!
//! [`ScalarUDFImpl::coerce_types`] could ask for `Int32`, and DataFusion would insert a
//! cast; a string that is not a number would then fail with Arrow's message
//! (`Cannot cast string 'abc' to value of Int32 type`) under the generic SQLSTATE.
//! PostgreSQL fails it as `22P02 invalid input syntax for type oid: "abc"`, and that is
//! the error a client can act on, so the parse happens here and the SQLSTATE travels in
//! the message the way every other input conversion in this crate carries it — see
//! [`crate::error::tagged_message`], and [`crate::uuid_in`] for the same shape.
//!
//! ## Registration
//!
//! `format_type` is the function whose *absence* on the executor this crate's
//! [`crate::pg_udf`] exists for, so the widened one has to be the one in that set — it is,
//! and the set is the only place either version is named. The one extra step is on the
//! coordinator: `setup_pg_catalog` registers upstream's `format_type` under the same name
//! *after* `register_postgres_functions` has run, so
//! `vairedb_coordinator::scheduler::setup_pg_catalog_schema` puts this one back.

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, Int32Array, Int32Builder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_pg_catalog::pg_catalog::format_type::create_format_type_udf;

use crate::columns::strings;
use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The name, which is PostgreSQL's and upstream's both: this shadows rather than adds.
pub const FORMAT_TYPE_UDF_NAME: &str = "format_type";

/// The shared [`ScalarUDF`] handle, for the registration seams that name it.
pub fn format_type_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(FormatType::new()))))
}

/// Register the widened `format_type` on `registry`, replacing any `format_type` already
/// there.
///
/// Nothing in production calls this, and that is not an oversight: `format_type` reaches a
/// node *inside* [`crate::pg_udf`]'s `pg_catalog` set, which is the family
/// [`crate::distributed_functions`] lists, and the coordinator puts it back by hand after
/// `setup_pg_catalog` replaces it. So this name already travels the one list, one row
/// further down than its own module — giving it a row of its own would give one function
/// two registration paths, which is the thing that list exists to prevent. The seam is kept
/// because it is how the tests below register this one function in isolation, and because
/// every sibling function module offers the same shape.
pub fn register_format_type(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(format_type_udf())?;
    Ok(())
}

/// Which PostgreSQL type an argument is declared as, and so how a string spelling of it
/// is read and what a bad one is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Argument {
    /// `oid`, PostgreSQL's unsigned 32-bit object identifier.
    Oid,
    /// `integer`, the type modifier.
    Integer,
}

impl Argument {
    /// PostgreSQL's name for the type, which is the name its error message uses.
    fn type_name(self) -> &'static str {
        match self {
            Argument::Oid => "oid",
            Argument::Integer => "integer",
        }
    }

    /// Read one string spelling the way PostgreSQL's input function for the type does.
    ///
    /// `oid` is unsigned in PostgreSQL and signed in the `i32` upstream renders from, and
    /// both spellings of the same identifier have to be accepted: `4294967295` is what
    /// PostgreSQL prints and `-1` is what a driver that treats an OID as an `int4` sends.
    /// `integer` takes neither, because `2147483648` is out of its range in PostgreSQL too.
    fn read(self, text: &str) -> Result<i32> {
        let trimmed = text.trim();
        if let Ok(value) = trimmed.parse::<i32>() {
            return Ok(value);
        }
        if self == Argument::Oid
            && let Ok(value) = trimmed.parse::<u32>()
        {
            return Ok(value as i32);
        }
        Err(DataFusionError::Execution(tagged_message(
            VdbErrorCode::InvalidTextRepresentation,
            format!(
                "invalid input syntax for type {}: \"{text}\"",
                self.type_name()
            ),
        )))
    }

    /// The Arrow type an argument of this kind is asked for, given the type it arrives as.
    ///
    /// An integer width is kept as it is, since upstream reads both; a string is kept a
    /// string, because [`Argument::read`] and not a cast is what converts it; and `Null` —
    /// which is what `format_type(23, NULL)` passes, the spelling `\gdesc` uses — is asked
    /// for as `Int32`, an all-null column upstream already reads as "no type modifier".
    fn coerced(self, arrived_as: &DataType) -> Option<DataType> {
        match arrived_as {
            DataType::Int32 | DataType::Int64 => Some(arrived_as.clone()),
            DataType::Null => Some(DataType::Int32),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(DataType::Utf8),
            _ => None,
        }
    }

    /// One whole argument as the integer column upstream reads, converting a string
    /// spelling of it the way PostgreSQL's input function for this type would.
    fn column(self, value: &ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        match value.data_type() {
            DataType::Int32 | DataType::Int64 => Ok(value.clone()),
            // Only reachable when this is invoked without `coerce_types` having run, which
            // the unit tests below do: an all-NULL column is an argument that carries no
            // value, and `Int32` is the width upstream reads that as.
            DataType::Null => Ok(ColumnarValue::Array(Arc::new(Int32Array::new_null(rows)))),
            _ => {
                let text = strings(&value.to_array(rows)?)?;
                let mut out = Int32Builder::with_capacity(text.len());
                for spelling in text.iter() {
                    match spelling {
                        Some(spelling) => out.append_value(self.read(spelling)?),
                        None => out.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }
        }
    }
}

/// `format_type(oid, integer)` — upstream's renderer behind PostgreSQL's own signature.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatType {
    signature: Signature,
    /// Upstream's UDF, which owns every rendering decision: this type only converts
    /// arguments, so the answer cannot drift from the one `\d` already gets.
    inner: ScalarUDF,
}

impl FormatType {
    fn new() -> Self {
        Self {
            // `Stable`, as upstream declares it: a call whose arguments are all literals is
            // then folded before the plan is serialized, which is what keeps `\gdesc`'s
            // `format_type(23, NULL)` from having to resolve anywhere else.
            signature: Signature::user_defined(Volatility::Stable),
            inner: create_format_type_udf(),
        }
    }
}

impl ScalarUDFImpl for FormatType {
    fn name(&self) -> &str {
        FORMAT_TYPE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Two arguments, each an integer, a string or NULL. Anything else is refused at
    /// planning time and named the way PostgreSQL names an unresolved call, because a
    /// `format_type(interval, integer)` does not exist there either.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [oid, typemod] = arg_types else {
            return plan_err!(
                "function {FORMAT_TYPE_UDF_NAME} takes two arguments, got {}",
                arg_types.len()
            );
        };
        let coerced = Argument::Oid
            .coerced(oid)
            .zip(Argument::Integer.coerced(typemod));
        match coerced {
            Some((oid, typemod)) => Ok(vec![oid, typemod]),
            None => plan_err!("function {FORMAT_TYPE_UDF_NAME}({oid}, {typemod}) does not exist"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: values,
            number_rows,
            return_field,
            config_options,
            ..
        } = args;
        let [oid, typemod] = values.as_slice() else {
            return exec_err!(
                "{FORMAT_TYPE_UDF_NAME} takes two arguments, got {}",
                values.len()
            );
        };
        let oid = Argument::Oid.column(oid, number_rows)?;
        let typemod = Argument::Integer.column(typemod, number_rows)?;
        self.inner.invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![arg_field(&oid), arg_field(&typemod)],
            args: vec![oid, typemod],
            number_rows,
            return_field,
            config_options,
        })
    }
}

/// The field upstream is handed beside a converted argument. It reads only the argument's
/// type from it, and this keeps the two agreeing after the conversion.
fn arg_field(value: &ColumnarValue) -> FieldRef {
    Arc::new(Field::new("arg", value.data_type(), true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::type_coercion::functions::fields_with_udf;

    /// The argument types a call of `udf` resolves to, asked the way the planner asks it.
    ///
    /// Not `ScalarUDFImpl::coerce_types` directly: that is only the hook a *user-defined*
    /// signature answers through, so calling it would prove nothing about upstream's
    /// `one_of` arms and everything about which of the two implementations is being tested.
    fn resolved(udf: &ScalarUDF, types: &[DataType]) -> Result<Vec<DataType>> {
        let fields: Vec<FieldRef> = types
            .iter()
            .map(|dt| Arc::new(Field::new("arg", dt.clone(), true)))
            .collect();
        Ok(fields_with_udf(&fields, udf)?
            .iter()
            .map(|f| f.data_type().clone())
            .collect())
    }

    /// Invoke the function the way a physical expression does, over whole columns.
    fn invoke(oid: ColumnarValue, typemod: ColumnarValue, rows: usize) -> Result<Vec<String>> {
        let args = ScalarFunctionArgs {
            arg_fields: vec![arg_field(&oid), arg_field(&typemod)],
            args: vec![oid, typemod],
            number_rows: rows,
            return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = FormatType::new().invoke_with_args(args)?.to_array(rows)?;
        Ok(out
            .as_string::<i32>()
            .iter()
            .map(|v| v.unwrap_or("NULL").to_string())
            .collect())
    }

    fn strings_of(values: &[&str]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(values.to_vec())))
    }

    fn int32s_of(values: &[i32]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int32Array::from(values.to_vec())))
    }

    /// The premise of the module: upstream's signature has integer arms only, so a string
    /// argument is not a call it has. If this ever fails because upstream widened its own
    /// signature, the shadowing below is no longer needed.
    #[test]
    fn upstream_has_no_arm_for_a_string_argument() {
        let upstream = create_format_type_udf();
        assert!(
            resolved(&upstream, &[DataType::Utf8, DataType::Int32]).is_err(),
            "a string OID is what a client tool sends and upstream cannot resolve"
        );
    }

    /// The headline: the string spellings now resolve, and to the same types upstream reads.
    #[test]
    fn a_string_argument_resolves() {
        let f = ScalarUDF::from(FormatType::new());
        for (oid, typemod, expected) in [
            (
                DataType::Utf8,
                DataType::Int32,
                [DataType::Utf8, DataType::Int32],
            ),
            (
                DataType::Utf8,
                DataType::Utf8,
                [DataType::Utf8, DataType::Utf8],
            ),
            (
                DataType::Int64,
                DataType::Utf8,
                [DataType::Int64, DataType::Utf8],
            ),
            // `format_type(23, NULL)` — the spelling `\gdesc` sends.
            (
                DataType::Int64,
                DataType::Null,
                [DataType::Int64, DataType::Int32],
            ),
            // The view and large string layouts collapse to `Utf8`, which is what the
            // conversion reads.
            (
                DataType::Utf8View,
                DataType::LargeUtf8,
                [DataType::Utf8, DataType::Utf8],
            ),
        ] {
            assert_eq!(
                resolved(&f, &[oid.clone(), typemod.clone()]).expect("resolved"),
                expected.to_vec(),
                "({oid}, {typemod})"
            );
        }
        assert_eq!(
            f.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }

    /// And a type PostgreSQL has no `format_type` for is still refused, at planning time and
    /// naming the call — the over-reach the widening must not have.
    #[test]
    fn a_type_postgres_has_no_arm_for_is_still_refused() {
        let f = ScalarUDF::from(FormatType::new());
        for types in [
            vec![
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                DataType::Int32,
            ],
            vec![DataType::Int32, DataType::Boolean],
            vec![DataType::Float64, DataType::Int32],
        ] {
            let err =
                resolved(&f, &types).expect_err(&format!("{types:?} is not a format_type call"));
            assert!(err.to_string().contains("does not exist"), "got: {err}");
        }
        // And the arity, which PostgreSQL also resolves by.
        assert!(resolved(&f, &[DataType::Int32]).is_err());
        assert!(resolved(&f, &[DataType::Int32, DataType::Int32, DataType::Int32]).is_err());
    }

    /// The values themselves, which are upstream's: the conversion must change the argument
    /// and nothing about the rendering.
    #[test]
    fn a_string_oid_renders_what_the_integer_one_renders() {
        assert_eq!(
            invoke(
                strings_of(&["23", "25", "1700", "1043"]),
                int32s_of(&[-1, -1, -1, -1]),
                4
            )
            .expect("valid"),
            ["integer", "text", "numeric", "character varying"]
        );
        // Both arguments as strings, including a type modifier that is read rather than
        // ignored: `varchar(10)` is typemod 14.
        assert_eq!(
            invoke(strings_of(&["1043"]), strings_of(&["14"]), 1).expect("valid"),
            ["character varying(10)"]
        );
        // Whitespace around a spelling, which PostgreSQL's input functions also skip.
        assert_eq!(
            invoke(strings_of(&[" 23 "]), strings_of(&[" -1 "]), 1).expect("valid"),
            ["integer"]
        );
    }

    /// An OID is unsigned in PostgreSQL and signed in what upstream renders from, so both
    /// spellings of the same identifier are read.
    #[test]
    fn an_oid_is_read_as_either_sign() {
        assert_eq!(
            invoke(strings_of(&["4294967295"]), int32s_of(&[-1]), 1).expect("valid"),
            invoke(int32s_of(&[-1]), int32s_of(&[-1]), 1).expect("valid"),
        );
    }

    /// A string that is not a number is PostgreSQL's `22P02`, with the value in the message
    /// the way PostgreSQL prints it — not Arrow's cast failure under a generic code.
    #[test]
    fn a_string_that_is_not_a_number_is_invalid_input_syntax() {
        let err = invoke(strings_of(&["abc"]), int32s_of(&[0]), 1).expect_err("not an oid");
        assert!(
            err.to_string()
                .contains("invalid input syntax for type oid: \"abc\""),
            "got: {err}"
        );
        assert_eq!(
            crate::error::code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::InvalidTextRepresentation)
        );

        // The type modifier is an `integer`, and is named as one.
        let err = invoke(int32s_of(&[23]), strings_of(&["x"]), 1).expect_err("not an integer");
        assert!(
            err.to_string()
                .contains("invalid input syntax for type integer: \"x\""),
            "got: {err}"
        );
        // An `integer` is signed and narrow, so an OID-sized value is out of its range here
        // exactly as it is in PostgreSQL.
        assert!(invoke(int32s_of(&[23]), strings_of(&["4294967295"]), 1).is_err());
    }

    /// A NULL row stays NULL rather than raising, in either argument.
    #[test]
    fn a_null_row_is_carried_through() {
        let oids = ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(vec![
            Some("23"),
            None,
        ])));
        assert_eq!(
            invoke(oids, int32s_of(&[-1, -1]), 2).expect("valid"),
            ["integer", "NULL"]
        );
        // A NULL type modifier is "no modifier", which is the `\gdesc` spelling.
        assert_eq!(
            invoke(
                strings_of(&["1043"]),
                ColumnarValue::Array(Arc::new(arrow::array::NullArray::new(1))),
                1
            )
            .expect("valid"),
            ["character varying"]
        );
    }

    /// The wire carries only the name, so the name has to resolve after registration — and
    /// what resolves has to be *this* one, since upstream's is registered under it too.
    #[test]
    fn the_widened_function_is_the_one_that_resolves_by_name() {
        let mut ctx = SessionContext::new();
        register_format_type(&mut ctx).expect("registration failed");
        let udf = ctx.udf(FORMAT_TYPE_UDF_NAME).expect("resolved");
        assert!(
            resolved(&udf, &[DataType::Utf8, DataType::Int32]).is_ok(),
            "the registered `format_type` is the widened one"
        );
        // Registering twice is what a context reached by two paths does.
        register_format_type(&mut ctx).expect("second registration failed");
        assert!(ctx.udf(FORMAT_TYPE_UDF_NAME).is_ok());
    }

    /// End to end through the planner, which is where the original failure was: the call
    /// has to *resolve* and then answer.
    #[tokio::test]
    async fn a_quoted_oid_plans_and_answers() {
        let mut ctx = SessionContext::new();
        register_format_type(&mut ctx).expect("registration failed");
        let batches = ctx
            .sql("SELECT format_type('23', 0), format_type('1043', '14'), format_type(25, NULL)")
            .await
            .expect("planned")
            .collect()
            .await
            .expect("executed");
        let batch = &batches[0];
        let column = |i: usize| batch.column(i).as_string::<i32>().value(0).to_string();
        assert_eq!(column(0), "integer");
        assert_eq!(column(1), "character varying(10)");
        assert_eq!(column(2), "text");
    }
}
