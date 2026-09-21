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
//! [`is_pushable_literal`], where `Date64` is the instructive case — a whole-day one is
//! renumbered to the `Date32` that means the same day ([`date64_as_date32`]) rather than
//! trusted to the unparser, which writes it as a *timestamp*.
//!
//! Two narrowings inside the list, both about a column or a pattern rather than about a
//! shape:
//!
//! Only the **null-ness** of a column the coordinator advertises as text but the shard does
//! not store as text is pushed — see [`OpaqueTextColumns`]. A comparison against such a
//! column is the one case where pushing turns a working query into a failing one rather
//! than into a slower one; `IS [NOT] NULL` is not a comparison and converts nothing, so it
//! is exempt.
//!
//! A `LIKE` is pushed only when its pattern is a literal that does not end with a dangling
//! escape, and the fragment spells out `ESCAPE '\'` so DuckDB applies PostgreSQL's default
//! rather than its own absence of one — see [`is_pushable_pattern`].
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
//! ## Why an ordering comparison on text *is* pushed
//!
//! It used to be the third narrowing, and it was a narrowing about a setting rather than
//! about an expression: both engines compare text by byte value, but DuckDB's
//! `default_collation` can say otherwise and nothing pinned it, so the agreement was a
//! coincidence of configuration. It is no longer a coincidence. Byte order is now the
//! comparison VaireDB enforces at every layer that can name a collation: a shard pins and
//! verifies `default_collation` when it opens its database
//! ([`DuckDbEngine::open`](../../../vairedb_core/engine/struct.DuckDbEngine.html#method.open)),
//! an expression `COLLATE` naming anything else is refused on both paths
//! ([`pg_operators::reject_unsupported_collation`](crate::pgwire_handler::pg_operators::reject_unsupported_collation),
//! [`write_sql_cl::reject_duckdb_divergent`](crate::write_sql_cl::reject_duckdb_divergent)),
//! and a **column** declared with one is refused at DDL
//! ([`reject_unsupported_column_collation`](crate::pgwire_handler::table_meta_ops::reject_unsupported_column_collation)) —
//! which matters here because a DDL broadcast is the client's own AST rendered back to SQL,
//! so a column collation would have reached a shard and made its comparisons disagree with
//! the coordinator's for that column alone. With all four in place, `s < 'x'` means the same
//! thing on both sides of the wire, which is the only thing this module ever asks of a shape
//! before pushing it. An opaque text column is still excluded, from this shape as from
//! every other.
//!
//! Column references are stripped of their table qualifier first. The predicate was
//! planned against the logical table (`orders`), and the statement it lands in scans the
//! shard's physical table (`orders_shard0`), so a qualified `"orders"."id"` would name a
//! relation that is not in the `FROM` clause.

use std::collections::HashSet;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue, plan_err};
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
/// coordinator repairs an error. So no predicate that puts a *value* beside one of these
/// columns is pushed — not equality, not `IN`, not `LIKE`, not an ordering comparison.
///
/// The one exemption is `IS [NOT] NULL`, and it is exempt for a reason that holds for every
/// declared type rather than for the four measured above: it names no value, so there is
/// nothing for the shard to convert, and a column's null-ness is the one thing a faithful
/// read cannot disagree about — a value that arrives is not null and a NULL that arrives is.
/// That matters because the set is open-ended by design:
/// [`parse_data_type`](crate::column_types::parse_data_type) answers `Utf8` for every type
/// it does not recognize, which is what keeps VaireDB forward-compatible with a type DuckDB
/// adds whose values are faithful as text, and an unrecognized type has no measured
/// rendering to compare a literal against. Closing the value shapes too would mean pushing
/// `CAST(<col> AS VARCHAR) = '…'` and asserting that DuckDB's own text rendering of the
/// stored type is byte-identical to the text its Arrow export produces — checkable for
/// `UUID`, `JSON` and `ENUM`, not checkable for a type nobody has named yet, and
/// [`names`](Self::names) carries column names across the query boundary rather than
/// declared types.
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

    /// Whether `name` is a column no predicate may compare a value against. Only a column
    /// the *schema* calls text can be one, so a `Unknown` set does not sterilize the numeric
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
    let rendered = prepare_for_rendering(filter)?;
    let sql = Unparser::new(&DuckDBDialect::new())
        .expr_to_sql(&rendered)
        .ok()?;
    let grouped = match sql {
        already @ ast::Expr::Nested(_) => already,
        bare => ast::Expr::Nested(Box::new(bare)),
    };
    Some(grouped.to_string())
}

/// The three respellings a fragment needs before the unparser sees it, each of them a
/// difference between what the coordinator's expression tree says and what the shard has to
/// read to mean the same thing.
///
/// * **Column references lose their table qualifier.** The predicate was planned against
///   the logical table (`orders`), and the statement it lands in scans the shard's physical
///   table (`orders_shard0`), so a qualified `"orders"."id"` would name a relation that is
///   not in the `FROM` clause.
/// * **A whole-day `Date64` becomes the `Date32` for the same day**, because the unparser
///   writes a `Date64` as a timestamp. See [`date64_as_date32`].
/// * **A `LIKE`/`ILIKE` gains an explicit `ESCAPE '\'`.** PostgreSQL — and so DataFusion,
///   and so the coordinator's own copy of the filter — escapes with a backslash when no
///   `ESCAPE` clause is present; DuckDB has no default escape at all. The clause is
///   therefore not decoration but the statement of *which* engine's default applies, and it
///   is added whether or not the pattern happens to contain a backslash, so one rule covers
///   every pattern [`is_pushable_pattern`] admits.
fn prepare_for_rendering(filter: &Expr) -> Option<Expr> {
    filter
        .clone()
        .transform(|expr| {
            Ok(match expr {
                Expr::Column(column) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(column.name)))
                }
                Expr::Literal(ScalarValue::Date64(Some(millis)), metadata) => {
                    match date64_as_date32(millis) {
                        Some(day) => Transformed::yes(Expr::Literal(day, metadata)),
                        // Unreachable for a filter `is_pushable` admitted, and a plain
                        // refusal if it ever is: the unparser's timestamp spelling is what
                        // `is_pushable_literal` set out to avoid.
                        None => {
                            return plan_err!(
                                "a Date64 with a time of day does not render as a date"
                            );
                        }
                    }
                }
                Expr::Like(mut like) => {
                    like.escape_char = Some('\\');
                    Transformed::yes(Expr::Like(like))
                }
                other => Transformed::no(other),
            })
        })
        .map(|transformed| transformed.data)
        .ok()
}

/// The number of milliseconds in a whole day, which is the unit an Arrow `Date64` is a
/// multiple of.
const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// `millis` as the `Date32` naming the same day, or `None` if it is not a whole number of
/// days or is outside `Date32`'s range.
///
/// Both [`is_pushable_literal`] and [`prepare_for_rendering`] ask, so that the acceptance
/// test and the rendering cannot drift: a `Date64` is admitted exactly when it has a
/// `Date32` to be rendered as.
fn date64_as_date32(millis: i64) -> Option<ScalarValue> {
    let days = (millis % MILLIS_PER_DAY == 0).then_some(millis / MILLIS_PER_DAY)?;
    Some(ScalarValue::Date32(Some(i32::try_from(days).ok()?)))
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
            is_pushable(cx, left) && is_pushable(cx, right)
        }

        // Null-ness is the one question an opaque text column may be asked, because it puts
        // no value beside the column for the shard to convert — see [`OpaqueTextColumns`].
        // The exemption is for the column *itself*, so the operand has to be exactly a
        // column; anything else under `IS NULL` is judged on its own terms.
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => match inner.as_ref() {
            Expr::Column(column) => cx.schema.field_with_name(&column.name).is_ok(),
            other => is_pushable(cx, other),
        },

        Expr::Not(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::Negative(inner) => is_pushable(cx, inner),

        // Two ordering comparisons wearing one keyword, so it is pushable wherever they are.
        Expr::Between(between) => {
            is_pushable(cx, &between.expr)
                && is_pushable(cx, &between.low)
                && is_pushable(cx, &between.high)
        }

        // Equality against each element, so it is pushable wherever equality is.
        Expr::InList(in_list) => {
            is_pushable(cx, &in_list.expr) && in_list.list.iter().all(|item| is_pushable(cx, item))
        }

        // `%` and `_` mean the same thing in both engines, and both are case-sensitive
        // (`ILIKE` in both is the case-insensitive one). The escape character does not agree
        // by default and is spelled out on the pushed copy instead, which only works for a
        // pattern the fragment can carry literally. See [`is_pushable_pattern`].
        //
        // A pattern that already carries its own `ESCAPE` stays behind: that character would
        // have to be re-escaped for the DuckDB fragment, and a client asking for `ESCAPE` is
        // rare enough not to be worth a second escaping rule.
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

/// Whether a `LIKE` pattern means the same thing to both engines once the fragment states
/// its escape character.
///
/// It has to be a **literal** string, because the agreement is a property of the pattern's
/// text: a column or an expression is not pushed, since there is nothing to inspect. Given
/// the `ESCAPE '\'` [`prepare_for_rendering`] writes, every escape sequence PostgreSQL
/// defines then means in DuckDB what it means at the coordinator — `\%` and `\_` a literal
/// wildcard, `\\` a literal backslash, `\x` the character `x`, checked against DuckDB 1.5
/// and `arrow-string`'s own matcher.
///
/// One text is refused: a pattern ending in an **unpaired** backslash. Arrow reads the
/// trailing `\` as a literal backslash; DuckDB refuses the pattern outright with `Like
/// pattern must not end with escape character`. An error is the one outcome
/// [`Inexact`](TableProviderFilterPushDown::Inexact) cannot repair — it is not too many rows
/// or too few, it is no answer at all — so that pattern stays at the coordinator, where it
/// already works.
fn is_pushable_pattern(pattern: &Expr) -> bool {
    match pattern {
        Expr::Literal(ScalarValue::Utf8(Some(text)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(text)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(text)), _) => !ends_with_dangling_escape(text),
        _ => false,
    }
}

/// Whether `pattern` ends with a backslash that has nothing to escape, which is what an odd
/// run of trailing backslashes means: the last one is unpaired.
fn ends_with_dangling_escape(pattern: &str) -> bool {
    pattern.chars().rev().take_while(|c| *c == '\\').count() % 2 == 1
}

/// A literal with an unambiguous SQL form in both engines.
///
/// Nested values are excluded because a list or struct literal has no shared spelling, and
/// binary because DuckDB's blob literal is not the one the unparser writes. `Interval` is
/// excluded for the same reason the write path renders intervals by hand: the two engines
/// name the units differently.
///
/// `Date64` is the kind that does not render as itself and is admitted anyway. The unparser
/// writes it as `CAST('… 01:20:00' AS DATETIME)` — a *timestamp*, which DuckDB then compares
/// against a `DATE` column by promoting the column, so a row at midnight is on the wrong side
/// of a `<`. Arrow's `Date64` is documented as a whole number of days expressed in
/// milliseconds, so the fix is not to refuse the kind but to say the same day in the kind
/// that renders as a date: [`date64_as_date32`] renumbers it, and a `Date64` is admitted
/// exactly when that renumbering exists. One carrying a time of day does not qualify —
/// truncating it to its day is not a smaller answer but a different one, which
/// [`Inexact`](TableProviderFilterPushDown::Inexact) cannot repair for `<`.
///
/// The checked list is
/// [`renders_the_literal_kinds_it_admits`](tests::renders_the_literal_kinds_it_admits).
fn is_pushable_literal(value: &ScalarValue) -> bool {
    if let ScalarValue::Date64(millis) = value {
        // `None` is the `NULL` keyword, which needs no day to render.
        return millis.is_none_or(|millis| date64_as_date32(millis).is_some());
    }
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

        // A `Date64` naming the same day has to arrive as the same date, not as the timestamp
        // the unparser would have written for it: 19000 days is 1_641_600_000_000 ms.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date64(Some(19_000 * MILLIS_PER_DAY)))))
                .as_deref(),
            Some("(\"day\" = CAST('2022-01-08' AS DATE))"),
            "a whole-day Date64 must be renumbered to its Date32, not rendered as a DATETIME"
        );
        // Including a negative one, where the renumbering divides below zero.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date64(Some(-1 * MILLIS_PER_DAY))))).as_deref(),
            Some("(\"day\" = CAST('1969-12-31' AS DATE))")
        );
        // A NULL needs no day to render.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date64(None)))).as_deref(),
            Some("(\"day\" = NULL)")
        );

        // ...whereas one carrying a time of day has no date to be renumbered to, and
        // truncating it would move the answer rather than widen it. 1641000000000 ms is
        // 2022-01-01 01:20:00.
        assert_eq!(
            pushed(col("day").eq(lit(ScalarValue::Date64(Some(1_641_000_000_000))))),
            None,
            "a Date64 with a time component must stay at the coordinator"
        );
        // And one too far out to be a Date32 at all.
        assert_eq!(
            pushed(col("day").lt(lit(ScalarValue::Date64(Some(
                i64::from(i32::MAX).saturating_add(1) * MILLIS_PER_DAY
            ))))),
            None,
            "a day count that does not fit a Date32 has no renumbering"
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

    // Both engines compare text by byte value, and that is now enforced rather than
    // observed: the shard pins and verifies `default_collation` when it opens its database,
    // and a `COLLATE` naming anything else is refused on both paths and at DDL. So the
    // ordering comparisons push, and `BETWEEN` — two of them wearing one keyword — with
    // them.
    #[test]
    fn pushes_an_ordering_comparison_on_a_text_column() {
        for expr in [
            col("name").gt(lit("b")),
            col("name").lt_eq(lit("b")),
            col("name").between(lit("a"), lit("z")),
            // And mixed with the shapes around it, since a fragment is pushed whole.
            col("name").gt(lit("b")).and(col("id").eq(lit(1i32))),
        ] {
            assert!(
                pushed(expr.clone()).is_some(),
                "an ordering comparison on text is byte order on both sides: {expr}"
            );
        }
        // Equality and `IN` are unaffected, as they always were.
        assert!(pushed(col("name").eq(lit("b"))).is_some());
        assert!(
            pushed(col("name").in_list(vec![lit("a"), lit("b")], false)).is_some(),
            "IN is a list of equalities"
        );
        // An ordering comparison on a column with a type both engines agree on is fine.
        assert!(pushed(col("amount").lt(lit(3i64))).is_some());

        // The column the catalog only calls text is still excluded, from this shape as from
        // every other: the hazard there is a conversion error, not a collation.
        for expr in [
            col("tag").gt(lit("b")),
            col("tag").between(lit("a"), lit("z")),
        ] {
            assert_eq!(
                pushed(expr.clone()),
                None,
                "an opaque text column is excluded from the ordering shapes too: {expr}"
            );
        }
    }

    // Split #5 of the operator gap analysis. PostgreSQL escapes with a backslash when no
    // `ESCAPE` clause is present and DuckDB escapes with nothing, so the fragment says which
    // one applies rather than leaving the pattern to be read two ways — `\_` matched a
    // literal underscore at the coordinator and nothing at the shard, and a shard returning
    // fewer rows is the disagreement `Inexact` cannot repair.
    #[test]
    fn spells_out_the_escape_character_a_like_pattern_relies_on() {
        assert_eq!(
            pushed(col("name").like(lit("a\\_b"))).as_deref(),
            Some("(\"name\" LIKE 'a\\_b' ESCAPE '\\')"),
            "the escape character has to be named, not inherited from the engine"
        );
        assert_eq!(
            pushed(col("name").ilike(lit("%50\\%%"))).as_deref(),
            Some("(\"name\" ILIKE '%50\\%%' ESCAPE '\\')"),
            "ILIKE carries the clause too"
        );
        // The clause is unconditional, so one rule covers every pattern: no branch to get
        // wrong for the pattern that happens to have no backslash in it.
        assert_eq!(
            pushed(col("name").like(lit("a%_b"))).as_deref(),
            Some("(\"name\" LIKE 'a%_b' ESCAPE '\\')")
        );

        // A pattern ending in an unpaired backslash is the one text left behind: DuckDB
        // refuses it outright, and an error is neither too many rows nor too few.
        assert_eq!(
            pushed(col("name").like(lit("a\\"))),
            None,
            "DuckDB: Like pattern must not end with escape character"
        );
        assert_eq!(pushed(col("name").like(lit("a\\\\\\"))), None);
        // A *paired* one at the end is a literal backslash, and pushes.
        assert!(
            pushed(col("name").like(lit("a\\\\"))).is_some(),
            "an escaped escape is a value, not a dangling escape"
        );

        // A pattern that is not a literal cannot be inspected at all.
        assert_eq!(pushed(col("name").like(col("name"))), None);
        // Nor is a pattern that brought its own escape character, which would need a second
        // escaping rule to re-render.
        let own_escape = Expr::Like(datafusion::logical_expr::Like::new(
            false,
            Box::new(col("name")),
            Box::new(lit("a!_b")),
            Some('!'),
            false,
        ));
        assert_eq!(pushed(own_escape), None);
    }

    // The column the catalog calls text and the shard does not. Putting a *value* beside it
    // risks a conversion *error* in DuckDB, which is the one outcome re-filtering at the
    // coordinator cannot undo — so no comparison is pushed onto it, only its null-ness, which
    // names no value to convert.
    #[test]
    fn pushes_only_the_null_ness_of_a_column_that_is_text_in_name_only() {
        for expr in [
            col("tag").eq(lit("a")),
            col("tag").not_eq(lit("a")),
            col("tag").in_list(vec![lit("a"), lit("b")], false),
            col("tag").like(lit("a%")),
            col("tag").gt(lit("a")),
            col("tag").between(lit("a"), lit("z")),
            // `IS TRUE` is a comparison against a boolean, so the shard would still have to
            // convert the column.
            col("tag").is_true(),
            // Including where it is only one branch of a larger predicate: the fragment is
            // pushed whole or not at all.
            col("id").eq(lit(1i32)).and(col("tag").eq(lit("a"))),
            col("id").eq(lit(1i32)).or(col("tag").eq(lit("a"))),
        ] {
            assert_eq!(
                pushed(expr.clone()),
                None,
                "a comparison against an ENUM column must stay at the coordinator: {expr}"
            );
        }

        // Null-ness is exempt, and exempt for a reason that holds for any declared type: a
        // value that arrives is not null and a NULL that arrives is, whatever the type.
        assert_eq!(
            pushed(col("tag").is_null()).as_deref(),
            Some("(\"tag\" IS NULL)")
        );
        assert_eq!(
            pushed(col("tag").is_not_null()).as_deref(),
            Some("(\"tag\" IS NOT NULL)")
        );
        // Including as one branch of a predicate whose other branches are pushable anyway.
        assert!(pushed(col("tag").is_null().or(col("id").eq(lit(1i32)))).is_some());

        // The exemption is for the column itself: `IS NULL` over an *expression* mentioning
        // it is refused, since evaluating the expression is the conversion the exemption
        // claimed not to need.
        assert_eq!(pushed(col("tag").eq(lit("a")).is_null()), None);
        // And a column that is not in the schema is still refused, exemption or not.
        assert_eq!(
            pushed(Expr::Column(Column::new_unqualified("nope")).is_null()),
            None
        );

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
        // And the null-ness exemption survives it, because it never depended on knowing the
        // declared type in the first place.
        assert!(
            duckdb_predicate(&schema, &unknown, &col("name").is_null()).is_some(),
            "null-ness is answerable without knowing what the shard stored"
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
