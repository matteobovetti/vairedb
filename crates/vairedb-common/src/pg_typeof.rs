//! PostgreSQL's `pg_typeof()`, and the `DataType` → PostgreSQL type-name table it needs.
//!
//! `pg_typeof(expr)` is how a client asks the server what an expression's type *is* rather
//! than guessing from the row description — which is why ORMs, migration tools and anyone
//! debugging an implicit cast reach for it. Its answer is a `regtype`, and a `regtype`
//! renders as the type's SQL name: `integer`, not `int4`.
//!
//! That spelling is the whole point of the module. VaireDB already has a `DataType` → pgwire
//! `Type` map for the row description, and pgwire's `Type::name()` is the *catalog* name
//! (`int4`, `varchar`, `float8`). A client comparing `pg_typeof(x) = 'integer'` — the form
//! PostgreSQL's own documentation and test suites use — would get no rows from it. So the
//! table below is written out by hand, and reads the way `\d` and `format_type` do.
//!
//! ## Why it is a UDF and not a constant the coordinator folds in
//!
//! The value depends only on the argument's type, so the coordinator *could* answer it
//! while planning. It does not, because the argument's type is exactly what a rewrite at
//! AST level does not know yet, and because a function registered by name is the seam the
//! rest of the read path already uses (see [`crate::pg_udf`] for why a name has to resolve
//! on every node). The evaluation is a constant per batch either way.

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, IntervalUnit};
use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

/// The name PostgreSQL uses, so a client's SQL needs no rewriting.
pub const PG_TYPEOF_UDF_NAME: &str = "pg_typeof";

/// Register `pg_typeof` on `registry`.
pub fn register_pg_typeof(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(pg_typeof_udf())?;
    Ok(())
}

/// The shared `pg_typeof` instance.
pub fn pg_typeof_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    UDF.get_or_init(|| Arc::new(ScalarUDF::from(PgTypeof::default())))
        .clone()
}

/// PostgreSQL's SQL name for the type an Arrow `DataType` is reported as.
///
/// Used by `pg_typeof` and by the error messages that have to name a type the way
/// PostgreSQL's would — `function count(integer, text) does not exist` reads as
/// PostgreSQL's only if the types do.
pub fn pg_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".to_string(),
        // No PostgreSQL type is one byte wide, and nothing narrower than `smallint` can
        // hold one, so a DuckDB `TINYINT` column reports as the type it is widened into.
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "smallint".to_string(),
        DataType::Int32 | DataType::UInt16 => "integer".to_string(),
        DataType::Int64 | DataType::UInt32 => "bigint".to_string(),
        // `bigint` is signed, so the top half of a `UInt64` would not fit in it.
        DataType::UInt64 => "numeric".to_string(),
        DataType::Float16 | DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double precision".to_string(),
        // The scale is part of the type, but `pg_typeof` reports the *name*: PostgreSQL
        // answers `numeric` for `1.5::numeric(10,2)` too.
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => "numeric".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "bytea".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time without time zone".to_string(),
        DataType::Timestamp(_, None) => "timestamp without time zone".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone".to_string(),
        // PostgreSQL has one interval type; Arrow has three layouts of it and a separate
        // `Duration`, and all four reach a client as `interval`.
        DataType::Interval(
            IntervalUnit::YearMonth | IntervalUnit::DayTime | IntervalUnit::MonthDayNano,
        )
        | DataType::Duration(_) => "interval".to_string(),
        // PostgreSQL spells an array type as its element type followed by `[]`, however
        // many dimensions it has — `integer[]`, never `integer[][]`.
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("{}[]", pg_type_name(field.data_type()))
        }
        // An anonymous row type. PostgreSQL's own name for one is `record`, which is what
        // `pg_typeof(row(1, 'a'))` answers.
        DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_, _) => "record".to_string(),
        // A dictionary or run-end array is an encoding, not a type: the client never sees
        // the encoding, so neither does `pg_typeof`.
        DataType::Dictionary(_, value) => pg_type_name(value),
        DataType::RunEndEncoded(_, value) => pg_type_name(value.data_type()),
        // `SELECT pg_typeof(NULL)` answers `unknown` in PostgreSQL 16.15, measured. The
        // *column* is still described as `text` — PostgreSQL resolves an unresolved literal to
        // `text` on the way out, and so does VaireDB — but `unknown` is the type the
        // expression has, and `unknown` is what a client comparing types sees. The same name
        // is the one PostgreSQL uses for such an argument in a `42883` message, which is the
        // other thing this function's answers are read for.
        DataType::Null => "unknown".to_string(),
    }
}

/// `pg_typeof(expr)`: the SQL name of the argument's type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgTypeof {
    signature: Signature,
}

impl Default for PgTypeof {
    fn default() -> Self {
        Self {
            // One argument of any type, and no coercion: coercing would change the very
            // thing the function reports.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PgTypeof {
    fn name(&self) -> &str {
        PG_TYPEOF_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // PostgreSQL's is `regtype`; VaireDB has no OID-valued type to report, and the
        // text a `regtype` renders as is what a client compares against anyway.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // The *declared* type of the argument, not the type of the values that arrived:
        // a scalar NULL folded out of a wider expression still has to report that
        // expression's type.
        let name = args
            .arg_fields
            .first()
            .map(|field| pg_type_name(field.data_type()))
            .unwrap_or_else(|| "text".to_string());

        // The answer is the same for every row, so it stays a scalar — a NULL argument
        // included, since a type is a property of the expression and not of its value.
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion::execution::context::SessionContext;

    fn invoke(data_type: DataType) -> String {
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])))],
            arg_fields: vec![Arc::new(Field::new("x", data_type, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("t", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        match PgTypeof::default().invoke_with_args(args).expect("invoked") {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            other => panic!("expected a Utf8 scalar, got {other:?}"),
        }
    }

    /// The reason the table is hand-written: every one of these has a pgwire catalog name
    /// (`int4`, `float8`, `varchar`, `timestamp`) that a client comparing against
    /// PostgreSQL's documented answer would not match.
    #[test]
    fn reports_the_sql_name_and_not_the_catalog_name() {
        assert_eq!(invoke(DataType::Int32), "integer");
        assert_eq!(invoke(DataType::Int64), "bigint");
        assert_eq!(invoke(DataType::Float64), "double precision");
        assert_eq!(invoke(DataType::Utf8), "text");
        assert_eq!(
            invoke(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp without time zone"
        );
    }

    /// A time zone is part of the type name in PostgreSQL, and the two types behave
    /// differently, so collapsing them would misreport the one a client cares about.
    #[test]
    fn a_time_zone_changes_the_reported_type() {
        assert_eq!(
            invoke(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "timestamp with time zone"
        );
    }

    /// `numeric(10, 2)` is a `numeric`: `pg_typeof` reports type names, not type
    /// modifiers, which is what `format_type` is for.
    #[test]
    fn a_precision_and_scale_do_not_reach_the_name() {
        assert_eq!(invoke(DataType::Decimal128(10, 2)), "numeric");
        assert_eq!(invoke(DataType::Decimal128(38, 16)), "numeric");
    }

    /// PostgreSQL spells an array as `element[]` at any dimension.
    #[test]
    fn an_array_is_its_element_type_followed_by_brackets() {
        let inner = Arc::new(Field::new("item", DataType::Int32, true));
        assert_eq!(invoke(DataType::List(inner.clone())), "integer[]");
        let nested = Arc::new(Field::new("item", DataType::List(inner), true));
        assert_eq!(invoke(DataType::List(nested)), "integer[][]");
    }

    /// A dictionary is Arrow's storage choice for a `text` column, and a client that asked
    /// what type it has wants `text`.
    #[test]
    fn an_encoding_reports_the_type_it_encodes() {
        assert_eq!(
            invoke(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )),
            "text"
        );
    }

    /// The name has to resolve on every node that plans or executes, because the plan
    /// carries it and nothing else.
    #[tokio::test]
    async fn resolves_by_name_and_answers_over_a_column() {
        let mut ctx = SessionContext::new();
        assert!(
            ctx.udf(PG_TYPEOF_UDF_NAME).is_err(),
            "a bare context should not have it, or this test proves nothing"
        );
        register_pg_typeof(&mut ctx).expect("registration failed");

        let batches = ctx
            .sql("SELECT pg_typeof(CAST(x AS INT)) AS t FROM (VALUES (1), (2)) v(x)")
            .await
            .expect("planned")
            .collect()
            .await
            .expect("executed");
        let rendered = arrow::util::pretty::pretty_format_batches(&batches)
            .expect("rendered")
            .to_string();
        assert!(rendered.contains("integer"), "got:\n{rendered}");
    }

    /// An untyped NULL is `unknown`, as measured against PostgreSQL 16.15 — the one type name
    /// that is not also the name of a column type a client can declare.
    #[test]
    fn an_untyped_null_is_unknown() {
        assert_eq!(invoke(DataType::Null), "unknown");
        assert_eq!(pg_type_name(&DataType::Null), "unknown");
    }
}
