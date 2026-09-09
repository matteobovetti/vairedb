//! Decide which `WHERE` predicates a shard can evaluate, and render them as DuckDB SQL.
//!
//! A pushed-down predicate is the difference between a shard streaming its whole table
//! over Arrow Flight and streaming the rows the query actually asked for. But a predicate
//! only helps if the shard evaluates it *the same way the coordinator would*, and the
//! shard runs a different engine than the one that planned the query — so this module is
//! deliberately narrow about what it pushes, and everything it pushes is reported to
//! DataFusion as [`Inexact`](TableProviderFilterPushDown::Inexact).
//!
//! ## Why `Inexact`, always
//!
//! `Exact` tells DataFusion to delete its own `FilterExec`: the shard's answer becomes the
//! answer. That is a promise this layer cannot make, because the predicate is re-parsed
//! and re-evaluated by DuckDB against the types *it* stored, which are not always the
//! types the catalog advertises. `Inexact` keeps the coordinator's filter as the
//! authority and leaves the pushed copy doing what it is actually good for — not shipping
//! rows nobody wants. The cost is evaluating the predicate twice, the second time over the
//! rows that survived the first.
//!
//! `Inexact` covers one direction of disagreement: if DuckDB returns *more* rows than the
//! predicate allows, the coordinator drops the extras. It does not cover the other
//! direction — a shard that returns too *few* rows has silently changed the answer — so
//! the allow-list below admits only shapes whose meaning is the same in both engines.
//!
//! ## What is pushed
//!
//! Comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, `IN` over a
//! literal list, `BETWEEN`, and `LIKE`/`ILIKE` — over column references and scalar
//! literals. No function calls, no casts, no subqueries, no nested-type literals: each of
//! those either means something subtly different in DuckDB, does not exist there under the
//! same name, or would need a translation this module does not have. Anything not on the
//! list stays at the coordinator, which is where it already runs today.
//!
//! Not every scalar kind qualifies as a literal, either: one whose SQL spelling the two
//! engines read differently is left behind even in an otherwise pushable shape. See
//! [`is_pushable_literal`], where `Date64` is the instructive case.
//!
//! Three narrowings inside the list:
//!
//! Nothing at all is pushed onto a column the coordinator advertises as text but the shard
//! does not store as text — see [`OpaqueTextColumns`], which is the one case where pushing
//! turns a working query into a failing one rather than into a slower one.
//!
//! An *ordering* comparison (`<`, `<=`, `>`, `>=`, and `BETWEEN`) is not pushed when either
//! side is a text column, even a genuine one. Both engines compare text by byte value
//! today, so they agree — but DuckDB has a `default_collation` setting, and VaireDB neither
//! pins it nor checks it, so the agreement is a coincidence of configuration rather than
//! something this module can rely on. Equality is unaffected, and equality is the shape
//! predicates overwhelmingly take, so the restriction costs little.
//!
//! A `LIKE` is pushed only when its pattern is a literal with no backslash in it, because
//! the two engines disagree about what a backslash in a pattern is — see
//! [`is_pushable_pattern`].
//!
//! ## Why the rendering is not `Expr::to_string()`
//!
//! Because that is a diagnostic rendering, not SQL: a string literal displays as
//! `Utf8("o'brien")`, which is neither the value nor parseable. DataFusion's own
//! [`Unparser`] with its [`DuckDBDialect`] is asked instead, exactly as the write path
//! asks Arrow to render a bind parameter — the engine that owns the expression tree is the
//! one that knows how to write it back out, including how to quote an identifier and
//! escape a literal.
//!
//! Column references are stripped of their table qualifier first. The predicate was
//! planned against the logical table (`orders`), and the statement it lands in scans the
//! shard's physical table (`orders_shard0`), so a qualified `"orders"."id"` would name a
//! relation that is not in the `FROM` clause.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::DuckDBDialect;

use crate::column_types::{is_declared_text, parse_data_type};

/// The columns the coordinator advertises as text but a shard stores as something else.
///
/// [`parse_data_type`](crate::column_types::parse_data_type) answers `Utf8` for every
/// declared type it does not recognize, and for a few it does: `UUID`, `JSON`, `ENUM`,
/// `STRUCT`. Reading such a column as text is faithful — that is why the fallback exists —
/// but a *predicate* on it is not, because DuckDB evaluates the pushed copy against the type
/// it really stored. Measured against DuckDB 1.5:
///
/// | Column | Pushed predicate | DuckDB |
/// |--------|------------------|--------|
/// | `UUID` | `u = 'notauuid'` | `Conversion Error: Could not convert string 'notauuid' to INT128` |
/// | `STRUCT(a INTEGER)` | `s = 'text'` | `Conversion Error: ... can't be cast to the destination type STRUCT` |
/// | `JSON` | `j = 'x'` | `Conversion Error: Malformed JSON at byte 0 of input` |
/// | `ENUM('a','b')` | `e = 'notamember'` | no rows |
/// | `CHAR(3)` | `c = 'zz'` | no rows |
///
/// The first three are worse than a wrong answer: they turn a query that would have
/// returned nothing into a query that *fails*, and no amount of re-filtering at the
/// coordinator repairs an error. So no predicate touching one of these columns is pushed at
/// all — not equality, not `IS NULL`, nothing. Such columns are rare enough that the lost
/// bandwidth does not matter, and a rule with no exceptions is a rule that cannot be got
/// wrong when the next shape is added to the allow-list.
#[derive(Debug, Clone, Default)]
pub enum OpaqueTextColumns {
    /// These columns, by name, are the opaque ones; every other text column is real text.
    Named(HashSet<String>),
    /// Which columns are opaque did not survive the trip (an encoding from a build before it
    /// was carried), so every text column is treated as opaque. Conservative by
    /// construction, which is why it is also the default: a provider built without the
    /// declared types must not push more than one built with them.
    #[default]
    Unknown,
}

impl OpaqueTextColumns {
    /// The opaque columns among `columns`, given as `(name, declared type)` pairs — the form
    /// the catalog stores them in.
    ///
    /// A column qualifies only if it is *advertised* as text and is not *declared* as text:
    /// the gap between the two is precisely what makes it opaque. A column that is text on
    /// neither count — an `INTEGER` — has nothing to be confused about and is left out, so
    /// the set stays as small as the wire form deserves.
    pub fn from_declared_types<'a>(columns: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self::Named(
            columns
                .into_iter()
                .filter(|(_, declared)| {
                    matches!(
                        parse_data_type(declared),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ) && !is_declared_text(declared)
                })
                .map(|(name, _)| name.to_string())
                .collect(),
        )
    }

    /// The names to carry across the distributed query boundary, or `None` when there is
    /// nothing to carry because the set was never known.
    pub fn names(&self) -> Option<Vec<String>> {
        match self {
            Self::Named(names) => Some(names.iter().cloned().collect()),
            Self::Unknown => None,
        }
    }

    /// Rebuild from what [`names`](Self::names) encoded. An absent list is [`Unknown`], not
    /// an empty set: "nobody told me" must not read as "nothing is opaque".
    pub fn from_names(names: Option<Vec<String>>) -> Self {
        match names {
            Some(names) => Self::Named(names.into_iter().collect()),
            None => Self::Unknown,
        }
    }

    /// Whether `name` is a column no predicate may be pushed onto. Only a column the
    /// *schema* calls text can be one, so a `Unknown` set does not sterilize the numeric
    /// and temporal columns too.
    fn covers(&self, schema: &Schema, name: &str) -> bool {
        if !is_text_field(schema, name) {
            return false;
        }
        match self {
            Self::Named(names) => names.contains(name),
            Self::Unknown => true,
        }
    }
}

/// The schema a predicate is checked against, plus which of its text columns are only
/// nominally text. Bundled because every step of the walk needs both.
struct ShardColumns<'a> {
    schema: &'a Schema,
    opaque: &'a OpaqueTextColumns,
}

/// How each of `filters` can be handled by a shard scan, in the order given.
pub(super) fn pushdown_support(
    schema: &Schema,
    opaque: &OpaqueTextColumns,
    filters: &[&Expr],
) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|filter| match duckdb_predicate(schema, opaque, filter) {
            // Never `Exact`: see the module docs.
            Some(_) => TableProviderFilterPushDown::Inexact,
            None => TableProviderFilterPushDown::Unsupported,
        })
        .collect()
}

/// The SQL fragments for the `filters` a shard scan received, dropping any that turn out
/// not to be renderable.
///
/// DataFusion only passes filters [`pushdown_support`] accepted, so a `None` here means
/// the two disagreed — a bug, but one whose safe reading is "do not push it": the
/// coordinator's own filter is still in the plan.
pub(super) fn duckdb_predicates(
    schema: &Schema,
    opaque: &OpaqueTextColumns,
    filters: &[Expr],
) -> Vec<String> {
    filters
        .iter()
        .filter_map(|filter| duckdb_predicate(schema, opaque, filter))
        .collect()
}

/// `filter` as a DuckDB `WHERE` fragment, or `None` if it must stay at the coordinator.
///
/// The fragment is parenthesized because the caller joins fragments with `AND`, and a
/// top-level `OR` would otherwise re-associate into a different predicate. The grouping is
/// added to the *expression*, not around the string, so a fragment the unparser already
/// grouped does not collect a second pair of parentheses on its way into an `EXPLAIN`.
fn duckdb_predicate(schema: &Schema, opaque: &OpaqueTextColumns, filter: &Expr) -> Option<String> {
    if !is_pushable(&ShardColumns { schema, opaque }, filter) {
        return None;
    }
    let unqualified = strip_qualifiers(filter)?;
    let sql = Unparser::new(&DuckDBDialect::new())
        .expr_to_sql(&unqualified)
        .ok()?;
    let grouped = match sql {
        already @ ast::Expr::Nested(_) => already,
        bare => ast::Expr::Nested(Box::new(bare)),
    };
    Some(grouped.to_string())
}

/// Rewrite every column reference to its bare name, so the fragment refers to the columns
/// of whatever table the shard scan puts in its `FROM` clause.
fn strip_qualifiers(filter: &Expr) -> Option<Expr> {
    filter
        .clone()
        .transform(|expr| {
            Ok(match expr {
                Expr::Column(column) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(column.name)))
                }
                other => Transformed::no(other),
            })
        })
        .map(|transformed| transformed.data)
        .ok()
}

/// Whether `expr` is one of the shapes both engines agree on. See the module docs for what
/// the list admits and why it stops where it does.
fn is_pushable(cx: &ShardColumns<'_>, expr: &Expr) -> bool {
    match expr {
        // A column has to exist in the schema, and has to be one the shard stores as the
        // type the schema claims. See [`OpaqueTextColumns`].
        Expr::Column(column) => {
            cx.schema.field_with_name(&column.name).is_ok()
                && !cx.opaque.covers(cx.schema, &column.name)
        }
        Expr::Literal(value, _) => is_pushable_literal(value),

        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if !is_pushable_operator(*op) {
                return false;
            }
            if is_ordering_comparison(*op) && (is_text(cx, left) || is_text(cx, right)) {
                return false;
            }
            is_pushable(cx, left) && is_pushable(cx, right)
        }

        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::Negative(inner) => is_pushable(cx, inner),

        // Two ordering comparisons wearing one keyword, so it inherits their restriction.
        Expr::Between(between) => {
            !is_text(cx, &between.expr)
                && is_pushable(cx, &between.expr)
                && is_pushable(cx, &between.low)
                && is_pushable(cx, &between.high)
        }

        // Equality against each element, so it is pushable wherever equality is.
        Expr::InList(in_list) => {
            is_pushable(cx, &in_list.expr) && in_list.list.iter().all(|item| is_pushable(cx, item))
        }

        // `%` and `_` mean the same thing in both engines, and both are case-sensitive
        // (`ILIKE` in both is the case-insensitive one) — but only for a pattern with no
        // escaping in it. See [`is_pushable_pattern`].
        Expr::Like(like) => {
            like.escape_char.is_none()
                && is_pushable_pattern(&like.pattern)
                && is_pushable(cx, &like.expr)
        }

        _ => false,
    }
}

/// Operators whose meaning is identical in both engines for the types VaireDB stores.
fn is_pushable_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
    )
}

/// The comparisons that depend on a type's *order* rather than only on equality.
fn is_ordering_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

/// Whether a `LIKE` pattern means the same thing to both engines.
///
/// It has to be a literal string with no backslash in it. DataFusion follows PostgreSQL,
/// where `\` is the *default* escape character with no `ESCAPE` clause present, so
/// `'a\_b'` matches a literal underscore. DuckDB has no default escape, so the same
/// pattern asks for a literal backslash followed by any character — and matches nothing.
/// That is the direction of disagreement [`Inexact`](TableProviderFilterPushDown::Inexact)
/// cannot repair: the shard would return *fewer* rows than the query wants, and the
/// coordinator's own filter never sees the rows that were dropped. `\` is exactly what
/// every ORM emits when it escapes a `%` or `_` in user input, so this is not a corner.
///
/// A non-literal pattern — a column, or an expression — is not pushed either, because
/// there is nothing to inspect for a backslash.
fn is_pushable_pattern(pattern: &Expr) -> bool {
    match pattern {
        Expr::Literal(ScalarValue::Utf8(Some(text)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(text)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(text)), _) => !text.contains('\\'),
        _ => false,
    }
}

/// A literal with an unambiguous SQL form in both engines.
///
/// Nested values are excluded because a list or struct literal has no shared spelling, and
/// binary because DuckDB's blob literal is not the one the unparser writes. `Interval` is
/// excluded for the same reason the write path renders intervals by hand: the two engines
/// name the units differently.
///
/// `Date64` is excluded on the strength of what the unparser actually produces for it: a
/// `CAST('… 01:20:00' AS DATETIME)`, a *timestamp*, not a date. Arrow's `Date64` is a
/// whole number of days expressed in milliseconds, so a well-formed one renders at midnight
/// and would compare correctly — but nothing enforces well-formedness, and a value carrying
/// a time of day would be truncated to its day by the coordinator and compared exactly by
/// the shard, which drops rows. No PostgreSQL client produces one either: `date` arrives as
/// `Date32`, which renders as a real `CAST(… AS DATE)` and is pushed. The checked list is
/// [`renders_the_literal_kinds_it_admits`](tests::renders_the_literal_kinds_it_admits).
fn is_pushable_literal(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Null
            | ScalarValue::Boolean(_)
            | ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128(..)
            | ScalarValue::Utf8(_)
            | ScalarValue::LargeUtf8(_)
            | ScalarValue::Utf8View(_)
            | ScalarValue::Date32(_)
    )
}

/// Whether `expr` is a text column — the case the ordering restriction exists for. A text
/// *literal* does not count: it is the column's type that decides how the comparison is
/// evaluated.
fn is_text(cx: &ShardColumns<'_>, expr: &Expr) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };
    is_text_field(cx.schema, &column.name)
}

/// Whether the column `name` of `schema` is advertised as text, whatever the shard stores.
fn is_text_field(schema: &Schema, name: &str) -> bool {
    schema.field_with_name(name).is_ok_and(|field| {
        matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit};

    /// A table with one column of each interesting kind: a real text column (`name`), one
    /// the catalog only calls text (`tag`, declared `ENUM`), and three the two engines agree
    /// on outright.
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(18, 3), true),
            Field::new("name", DataType::Utf8, true),
            Field::new("tag", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
        ])
    }

    /// The declared types behind [`schema`], as the catalog stores them.
    fn opaque() -> OpaqueTextColumns {
        OpaqueTextColumns::from_declared_types([
            ("id", "INTEGER"),
            ("amount", "DECIMAL(18,3)"),
            ("name", "VARCHAR(64)"),
            ("tag", "ENUM('a', 'b')"),
            ("flag", "BOOLEAN"),
            ("day", "DATE"),
        ])
    }

    /// The fragment a filter is pushed as, or `None` when it stays at the coordinator.
    fn pushed(expr: Expr) -> Option<String> {
        duckdb_predicate(&schema(), &opaque(), &expr)
    }

    // The rendering the old `Expr::to_string()` got wrong, and the reason this module
    // exists: a string literal has to arrive as a string literal.
    #[test]
    fn renders_a_string_literal_as_sql_not_as_debug_output() {
        assert_eq!(
            pushed(col("name").eq(lit("o'brien"))).unwrap(),
            "(\"name\" = 'o''brien')",
            "the quote must be escaped and the Utf8(..) wrapper must be gone"
        );
    }

    // Membership in the allow-list is only half of the claim; the other half is that the
    // literal *renders* as the value it stands for, in a spelling DuckDB reads the same way.
    // Asserted on the SQL rather than on `is_pushable_literal` because that is where the
    // risk lives: `Date64` passed a membership test happily and still rendered as a
    // timestamp.
    #[test]
    fn renders_the_literal_kinds_it_admits() {
        let numeric = [
            (lit(42i64), "(\"id\" = 42)"),
            (lit(1.5f32), "(\"id\" = 1.5)"),
            (lit(ScalarValue::Null), "(\"id\" = NULL)"),
            (
                lit(ScalarValue::Decimal128(Some(1500), 4, 2)),
                "(\"id\" = 15.00)",
            ),
            (
                lit(ScalarValue::Decimal128(Some(-12345), 8, 3)),
                "(\"id\" = -12.345)",
            ),
            (
                lit(ScalarValue::Decimal128(Some(42), 4, 0)),
                "(\"id\" = 42)",
            ),
        ];
        for (value, expected) in numeric {
            assert_eq!(
                pushed(col("id").eq(value.clone())).as_deref(),
                Some(expected),
                "the scale and sign have to survive: {value}"
            );
        }

        // A date has to arrive as a date, cast and all.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date32(Some(19000))))).as_deref(),
            Some("(\"day\" = CAST('2022-01-08' AS DATE))")
        );

        // ...whereas a `Date64` arrives as a *timestamp*, which is why it is not admitted.
        // 1641000000000 ms is 2022-01-01 01:20:00, and the unparser prints the time.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date64(Some(1_641_000_000_000))))),
            None,
            "a Date64 renders with a time component, so it must stay at the coordinator"
        );
    }

    // A predicate is planned against the logical table and executed against the shard's,
    // so a surviving qualifier names a relation the statement never mentions.
    #[test]
    fn drops_the_table_qualifier_a_shard_scan_has_no_name_for() {
        let qualified = Expr::Column(Column::new(Some("orders"), "id")).gt(lit(7i32));
        let sql = pushed(qualified).unwrap();
        assert!(
            !sql.contains("orders"),
            "the logical table name must not reach the shard: {sql}"
        );
        assert_eq!(sql, "(\"id\" > 7)");
    }

    // A top-level OR is joined to the other fragments with AND, so it has to keep its own
    // grouping.
    #[test]
    fn parenthesizes_a_fragment_so_joining_cannot_reassociate_it() {
        let sql = pushed(col("id").eq(lit(1i32)).or(col("id").eq(lit(2i32)))).unwrap();
        assert!(
            sql.starts_with('(') && sql.ends_with(')'),
            "unexpected fragment: {sql}"
        );
    }

    #[test]
    fn pushes_the_ordinary_predicate_shapes() {
        let cases = [
            col("id").eq(lit(1i32)),
            col("id").not_eq(lit(1i32)),
            col("amount").gt(lit(10i64)),
            col("name").is_null(),
            col("name").is_not_null(),
            col("flag").is_true(),
            col("id").in_list(vec![lit(1i32), lit(2i32)], false),
            col("id").between(lit(1i32), lit(9i32)),
            col("name").like(lit("a%")),
            col("name").ilike(lit("a%")),
            col("id").eq(lit(1i32)).and(col("name").eq(lit("x"))),
            !col("flag"),
        ];
        for case in cases {
            assert!(
                pushed(case.clone()).is_some(),
                "should have been pushed: {case}"
            );
        }
    }

    // Both engines compare text by byte value, so an ordering comparison on a real text
    // column would agree — but only because nothing has changed DuckDB's collation, which
    // this layer does not check. It stays at the coordinator.
    #[test]
    fn keeps_an_ordering_comparison_on_a_text_column_at_the_coordinator() {
        for expr in [
            col("name").gt(lit("b")),
            col("name").lt_eq(lit("b")),
            col("name").between(lit("a"), lit("z")),
        ] {
            assert_eq!(
                pushed(expr.clone()),
                None,
                "an ordering comparison on text must not be pushed: {expr}"
            );
        }
        // Equality is unaffected — it is the same answer in both engines, and it is the
        // shape that matters most.
        assert!(pushed(col("name").eq(lit("b"))).is_some());
        assert!(
            pushed(col("name").in_list(vec![lit("a"), lit("b")], false)).is_some(),
            "IN is a list of equalities"
        );
        // An ordering comparison on a column with a type both engines agree on is fine.
        assert!(pushed(col("amount").lt(lit(3i64))).is_some());
    }

    // Split #5 of the operator gap analysis, and the one disagreement `Inexact` cannot
    // repair: DuckDB would match nothing where PostgreSQL matches the escaped character, so
    // the shard returns fewer rows and the coordinator's filter never sees the missing ones.
    #[test]
    fn keeps_an_escaped_like_pattern_at_the_coordinator() {
        assert_eq!(
            pushed(col("name").like(lit("a\\_b"))),
            None,
            "a backslash in the pattern means different things to the two engines"
        );
        assert_eq!(pushed(col("name").ilike(lit("%50\\%%"))), None);
        // A pattern that is not a literal cannot be inspected for one.
        assert_eq!(pushed(col("name").like(col("name"))), None);
        // The ordinary pattern is unaffected.
        assert!(pushed(col("name").like(lit("a%_b"))).is_some());
    }

    // The column the catalog calls text and the shard does not. Pushing anything onto it
    // risks a conversion *error* in DuckDB, which is the one outcome re-filtering at the
    // coordinator cannot undo — so nothing is pushed onto it at all.
    #[test]
    fn pushes_nothing_onto_a_column_that_is_text_in_name_only() {
        for expr in [
            col("tag").eq(lit("a")),
            col("tag").not_eq(lit("a")),
            col("tag").in_list(vec![lit("a"), lit("b")], false),
            col("tag").like(lit("a%")),
            col("tag").is_null(),
            col("tag").is_not_null(),
            // Including where it is only one branch of a larger predicate: the fragment is
            // pushed whole or not at all.
            col("id").eq(lit(1i32)).and(col("tag").eq(lit("a"))),
            col("id").eq(lit(1i32)).or(col("tag").eq(lit("a"))),
        ] {
            assert_eq!(
                pushed(expr.clone()),
                None,
                "a predicate on an ENUM column must stay at the coordinator: {expr}"
            );
        }

        // The real text column next to it is unaffected — this narrowing is about the
        // declared type, not about `Utf8`.
        assert!(pushed(col("name").eq(lit("a"))).is_some());
        // And a predicate that never mentions the opaque column still pushes.
        assert!(pushed(col("id").eq(lit(1i32))).is_some());
    }

    // What a rolling upgrade gets: an encoding that carried no list at all. "Not known"
    // has to read as "assume the worst", or the hazard comes back for one release.
    #[test]
    fn an_unknown_opaque_set_protects_every_text_column() {
        let schema = schema();
        let unknown = OpaqueTextColumns::Unknown;

        assert_eq!(
            duckdb_predicate(&schema, &unknown, &col("name").eq(lit("a"))),
            None,
            "with no list to consult, even a real text column must be left alone"
        );
        // Non-text columns are untouched by the fallback: it is scoped to what the schema
        // itself calls text.
        assert!(
            duckdb_predicate(&schema, &unknown, &col("id").eq(lit(1i32))).is_some(),
            "an integer column cannot be text in name only"
        );

        // And it survives the round trip a distributed plan puts it through.
        assert_eq!(OpaqueTextColumns::from_names(unknown.names()).names(), None);
        let named = opaque();
        let mut round_tripped = OpaqueTextColumns::from_names(named.names())
            .names()
            .expect("a known set stays known");
        round_tripped.sort();
        assert_eq!(round_tripped, vec!["tag".to_string()]);
    }

    // A wide float renders as a 300-digit decimal literal rather than in exponent form.
    // DuckDB widens a literal that overflows DECIMAL to DOUBLE, so it compares as the same
    // value — checked against DuckDB 1.5, not assumed.
    #[test]
    fn renders_a_float_literal_duckdb_can_read() {
        let sql = pushed(col("amount").gt(lit(1e300f64))).expect("a float is pushable");
        assert!(
            sql.starts_with("(\"amount\" > 1000000000000000052504"),
            "unexpected rendering: {sql}"
        );
    }

    // Anything this module has no translation for stays where it already runs. The check
    // is the same one `supports_filters_pushdown` reports, so DataFusion keeps its filter.
    #[test]
    fn refuses_what_it_cannot_translate() {
        let unknown_column = Expr::Column(Column::new_unqualified("nope")).eq(lit(1i32));
        assert_eq!(pushed(unknown_column), None);

        // A function call: DuckDB may not have it, or may not have it under this name.
        let function = datafusion::functions::expr_fn::upper(col("name")).eq(lit("X"));
        assert_eq!(pushed(function), None);

        // A cast, which is where the two engines' type coercion rules would start to
        // matter.
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("id")),
            DataType::Int64,
        ))
        .eq(lit(1i64));
        assert_eq!(pushed(cast), None);

        // An arithmetic operator is not on the list: integer division differs.
        let arithmetic = (col("id") + lit(1i32)).eq(lit(2i32));
        assert_eq!(pushed(arithmetic), None);
    }

    // What `scan` and `supports_filters_pushdown` must agree on: every filter reported
    // pushable renders, and nothing is reported `Exact`.
    #[test]
    fn reports_every_renderable_filter_as_inexact_and_the_rest_as_unsupported() {
        let schema = schema();
        let pushable = col("id").eq(lit(1i32));
        let not_pushable = datafusion::functions::expr_fn::upper(col("name")).eq(lit("X"));
        let filters = [&pushable, &not_pushable];

        assert_eq!(
            pushdown_support(&schema, &opaque(), &filters),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
            ]
        );

        // `scan` is handed only the accepted ones, and renders exactly those.
        let rendered = duckdb_predicates(&schema, &opaque(), &[pushable]);
        assert_eq!(rendered, vec!["(\"id\" = 1)".to_string()]);
    }
}
