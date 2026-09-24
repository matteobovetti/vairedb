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
//!
//! ## How the module is laid out
//!
//! Three responsibilities, one file each. This one is the registration seam and the one
//! thing all three functions need — the `text` rendering of an argument. [`format_string`]
//! holds the `format()` grammar, and [`quote`] the two quoting rules, which `format()`'s
//! `%L` and `%I` are and which `quote_literal`/`quote_nullable` are built on: the grammar
//! depends on the rules and neither depends on the seam.

mod format_string;
mod quote;

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

use format_string::Format;
use quote::{NullAnswer, Quote};

pub use quote::quote_literal_text;

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
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Quote::new(NullAnswer::Null)))))
}

/// The shared `quote_nullable` instance.
pub fn quote_nullable_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(Quote::new(NullAnswer::Keyword)))))
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
                return Ok(None);
            }
            Ok(Some(formatter.value(row).try_to_string()?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

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
