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
//! ## What is where
//!
//! `input.rs` owns the check both casts are, and `accessor.rs` owns the four operators. They
//! share that check and nothing else, and they change for different reasons: the first when
//! PostgreSQL accepts or refuses a document differently, the second when an operator
//! navigates one differently. Everything either of them exposes is re-exported here, so a
//! caller still writes `vairedb_common::json_pg::validate`.

mod accessor;
mod input;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;

pub use accessor::{
    JSON_GET_TEXT_UDF_NAME, JSON_GET_UDF_NAME, JSON_PATH_TEXT_UDF_NAME, JSON_PATH_UDF_NAME,
};
pub use input::{JSON_IN_UDF_NAME, JSONB_IN_UDF_NAME, JsonInputError, JsonType, validate};

/// Register the `json` input conversion and the four accessors on `registry`.
///
/// Call this on every context that plans **or** executes a read: the read path rewrites
/// `::json` and each operator into a call, and only the name crosses the wire.
pub fn register_json_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    input::register_casts(registry)?;
    accessor::register_accessors(registry)
}

/// Invoking a UDF the way a physical expression does, for the tests of both submodules.
///
/// Every rule here is a rule about a *column*: a NULL row, an empty batch and a row that
/// raises are what the accessors and the cast are written against, and none of them is
/// reachable by calling the private function underneath.
#[cfg(test)]
mod invoking {
    use std::sync::Arc;

    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::Result;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    /// Invoke a UDF over a whole column, and read the strings it answered.
    pub(super) fn invoke(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        rows: usize,
    ) -> Result<Vec<Option<String>>> {
        let out = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(Field::new("v", DataType::Utf8, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })?
            .to_array(rows)?;
        Ok(out
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect())
    }

    pub(super) fn text_column(values: Vec<Option<&str>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

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
