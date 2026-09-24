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
use datafusion::common::{Result, exec_err};
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

/// The shared `pg_typeof` instance, built once and handed to every registry that asks.
fn pg_typeof_udf() -> Arc<ScalarUDF> {
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
        // PostgreSQL spells an array type as its element type followed by `[]`.
        //
        // The nesting is where the two disagree. PostgreSQL has exactly one array type per
        // element type and carries the dimension count beside the value, so
        // `pg_typeof(ARRAY[ARRAY[1,2]])` answers `integer[]` there. Arrow makes a list of
        // lists a *distinct* type, and the recursion below names it as one — `integer[][]`,
        // a spelling PostgreSQL never prints. Flattening it to one `[]` is the name a client
        // comparing against PostgreSQL expects, and it is deliberately not done here: it is
        // a change to an answer clients already read, not a refactoring, so it belongs to
        // whoever owns the array-type contract rather than to this table.
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
        //
        // The signature admits exactly one argument, so the planner refuses any other arity
        // before this runs and the `else` is unreachable. It refuses rather than naming a
        // plausible type anyway, because the one thing this function must never do is answer
        // a type name that is not the argument's — a wrong answer here is one a client
        // cannot detect, while a missing one it can.
        let [field] = args.arg_fields.as_slice() else {
            return exec_err!(
                "{PG_TYPEOF_UDF_NAME} takes one argument, got {}",
                args.arg_fields.len()
            );
        };

        // The answer is the same for every row, so it stays a scalar — a NULL argument
        // included, since a type is a property of the expression and not of its value.
        let name = pg_type_name(field.data_type());
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, FieldRef, Fields, TimeUnit, UnionFields, UnionMode};
    use datafusion::execution::context::SessionContext;

    fn field(data_type: DataType) -> FieldRef {
        Arc::new(Field::new("item", data_type, true))
    }

    /// Invoke the function the way a physical expression does: the values are `Int32`
    /// whatever `declared_as` says, because the declared type is the only thing the answer
    /// may come from.
    fn invoke(declared_as: Vec<FieldRef>) -> Result<ColumnarValue> {
        PgTypeof::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2])))],
            arg_fields: declared_as,
            number_rows: 2,
            return_field: Arc::new(Field::new("t", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
    }

    /// Every arm of the table, because the table *is* the contract: a client reads these
    /// names and compares them against the ones PostgreSQL's documentation prints, so an arm
    /// nobody checked is a wrong answer waiting for the type that reaches it.
    ///
    /// The whole reason it is hand-written rather than taken from pgwire's `Type::name()`:
    /// every name below has a *catalog* spelling (`int4`, `float8`, `varchar`, `timestamp`)
    /// that a client comparing against PostgreSQL's documented answer would not match.
    #[test]
    fn names_every_arrow_type_the_way_postgres_names_it() {
        let int_list = field(DataType::Int32);
        let cases: Vec<(DataType, &str)> = vec![
            (DataType::Boolean, "boolean"),
            // Nothing in PostgreSQL is one byte wide, so a `TINYINT` reports as the type it
            // is widened into, and an unsigned width reports as the signed one that holds it.
            (DataType::Int8, "smallint"),
            (DataType::Int16, "smallint"),
            (DataType::UInt8, "smallint"),
            (DataType::Int32, "integer"),
            (DataType::UInt16, "integer"),
            (DataType::Int64, "bigint"),
            (DataType::UInt32, "bigint"),
            // `bigint` is signed, so the top half of a `UInt64` would not fit in it.
            (DataType::UInt64, "numeric"),
            (DataType::Float16, "real"),
            (DataType::Float32, "real"),
            (DataType::Float64, "double precision"),
            // `numeric(10, 2)` is a `numeric`: this reports type names, and the modifiers are
            // what `format_type` is for.
            (DataType::Decimal32(9, 2), "numeric"),
            (DataType::Decimal64(18, 4), "numeric"),
            (DataType::Decimal128(10, 2), "numeric"),
            (DataType::Decimal128(38, 16), "numeric"),
            (DataType::Decimal256(50, 2), "numeric"),
            (DataType::Utf8, "text"),
            (DataType::LargeUtf8, "text"),
            (DataType::Utf8View, "text"),
            (DataType::Binary, "bytea"),
            (DataType::LargeBinary, "bytea"),
            (DataType::BinaryView, "bytea"),
            (DataType::FixedSizeBinary(16), "bytea"),
            (DataType::Date32, "date"),
            (DataType::Date64, "date"),
            (DataType::Time32(TimeUnit::Second), "time without time zone"),
            (
                DataType::Time64(TimeUnit::Microsecond),
                "time without time zone",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "timestamp without time zone",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "timestamp without time zone",
            ),
            // A time zone is part of the type name in PostgreSQL, and the two types behave
            // differently, so collapsing them would misreport the one a client cares about.
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "timestamp with time zone",
            ),
            // One PostgreSQL type, four Arrow spellings of it.
            (DataType::Interval(IntervalUnit::YearMonth), "interval"),
            (DataType::Interval(IntervalUnit::DayTime), "interval"),
            (DataType::Interval(IntervalUnit::MonthDayNano), "interval"),
            (DataType::Duration(TimeUnit::Microsecond), "interval"),
            // An array is its element type followed by `[]`, in every list layout.
            (DataType::List(int_list.clone()), "integer[]"),
            (DataType::LargeList(int_list.clone()), "integer[]"),
            (DataType::ListView(int_list.clone()), "integer[]"),
            (DataType::LargeListView(int_list.clone()), "integer[]"),
            (DataType::FixedSizeList(int_list.clone(), 2), "integer[]"),
            // The divergence the table's comment states: PostgreSQL answers `integer[]` for a
            // nested array and this answers `integer[][]`, which is pinned here so the day it
            // is corrected is a deliberate change to a client-visible answer.
            (
                DataType::List(field(DataType::List(int_list.clone()))),
                "integer[][]",
            ),
            // An anonymous row type, whichever way Arrow spells it.
            (
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
                "record",
            ),
            (
                DataType::Map(
                    field(DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ]))),
                    false,
                ),
                "record",
            ),
            (
                DataType::Union(
                    UnionFields::try_new([0], [Field::new("a", DataType::Int32, true)])
                        .expect("a one-variant union"),
                    UnionMode::Sparse,
                ),
                "record",
            ),
            // An encoding is not a type: a dictionary is Arrow's storage choice for a `text`
            // column, and a client that asked what type it has wants `text`.
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "text",
            ),
            (
                DataType::RunEndEncoded(field(DataType::Int32), field(DataType::Utf8)),
                "text",
            ),
            // An untyped NULL is `unknown`, as measured against PostgreSQL 16.15 — the one
            // name here that is not also a type a client can declare a column as.
            (DataType::Null, "unknown"),
        ];

        for (data_type, expected) in cases {
            assert_eq!(pg_type_name(&data_type), expected, "for {data_type}");
        }
    }

    /// The shell around the table: the answer comes from the argument's *declared* type and
    /// not from the values that arrived — a scalar NULL folded out of a wider expression
    /// still reports that expression's type — and it is one scalar for the whole batch,
    /// since a type is a property of the expression rather than of a row.
    #[test]
    fn the_answer_is_the_declared_type_as_one_scalar_for_the_batch() {
        for declared in [DataType::Utf8, DataType::Null, DataType::Float64] {
            let expected = pg_type_name(&declared);
            match invoke(vec![field(declared.clone())]).expect("invoked") {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => {
                    assert_eq!(name, expected, "for a column declared {declared}");
                }
                other => panic!("expected one Utf8 scalar for {declared}, got {other:?}"),
            }
        }
    }

    /// The arity the signature already refuses, refused here too: naming a plausible type
    /// for an argument that is not there would be a wrong answer no client could detect.
    #[test]
    fn a_call_without_an_argument_is_refused_rather_than_named() {
        let err = invoke(vec![]).expect_err("no argument is not a pg_typeof call");
        assert!(err.to_string().contains("takes one argument"), "got: {err}");
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
}
