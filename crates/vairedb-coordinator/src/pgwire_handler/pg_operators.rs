//! Read-path handling for the PostgreSQL expression forms DataFusion's planner does
//! not implement, in the two shapes a gap can honestly take: a **rewrite** to an
//! equivalent DataFusion expression, or a **refusal** naming what was refused.
//!
//! Which of the two applies is not a judgement call. A form whose PostgreSQL meaning
//! has an exact DataFusion spelling is rewritten, because the client's statement is
//! answerable and only the surface differs. A form whose meaning VaireDB does not
//! implement is refused, even where the planner would happily accept it — a
//! `COLLATE` that is parsed and thrown away returns plausible rows in byte order,
//! and that is worse than an error, because nothing tells the client the collation
//! never applied.
//!
//! One of the two runs earlier than the rest: a `COLLATE` clause is refused at parse
//! time ([`reject_unsupported_collation`]), because the compatibility parser the read
//! path is fed by deletes the clause before the AST reaches the rewrites here.
//!
//! All of this is read-path only: it prepares an AST for DataFusion's planner, and
//! the write path renders its AST back to SQL for DuckDB instead
//! ([`crate::write_sql_cl`]). The two paths therefore still disagree on some of
//! these operators; the read half is what this module closes.

use std::ops::ControlFlow;

use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::error::CoordinatorError;
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement, UnaryOperator, Value, ValueWithSpan,
    visit_expressions, visit_expressions_mut,
};

/// Rewrite the PostgreSQL operators DataFusion lacks, and refuse the expressions it
/// would accept while quietly ignoring part of what they say.
///
/// Applied to a whole statement, so it reaches a `WHERE`, a projection, an
/// `ORDER BY` key and a subquery alike. The visit is post-order, so an inner
/// expression is rewritten before the one containing it and a rewrite's own
/// operands are never re-examined.
pub(super) fn rewrite_pg_expressions(stmt: &mut Statement) -> PgWireResult<()> {
    // Not an expression, so outside the visit below: a `WINDOW` clause is a property of
    // the select, and it can lose a clause the same way an `OVER (...)` can.
    reject_chained_named_windows(stmt)?;
    // Nor is a set operation an expression: `INTERSECT ALL` and `EXCEPT ALL` are the two
    // whose answer is wrong rather than absent.
    reject_multiplicity_set_operations(stmt)?;
    match visit_expressions_mut(stmt, |expr| match rewrite_expr(expr) {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// The per-node half of [`rewrite_pg_expressions`].
fn rewrite_expr(expr: &mut Expr) -> PgWireResult<()> {
    // Split in two: a refusal only needs to read the node, whereas a rewrite has to
    // own its operands. So decide from a borrow first, and take the node apart only
    // in the arms that are going to replace it.
    match expr {
        Expr::Cast {
            data_type:
                data_type @ (DataType::Character(Some(_))
                | DataType::Char(Some(_))
                | DataType::CharacterVarying(Some(_))
                | DataType::CharVarying(Some(_))
                | DataType::Varchar(Some(_))),
            ..
        } => {
            return Err(unsupported(
                format!("CAST to {data_type}"),
                "the length is not enforced on the read path, so the cast would neither \
                 truncate nor pad and the value would come back unchanged; cast to TEXT, or \
                 truncate explicitly with substr()",
            ));
        }
        Expr::AllOp {
            compare_op, right, ..
        } if matches!(right.as_ref(), Expr::Subquery(_)) => {
            return Err(quantified_subquery_unsupported(compare_op, "ALL"));
        }
        Expr::AnyOp {
            compare_op,
            right,
            is_some,
            ..
        } if matches!(right.as_ref(), Expr::Subquery(_)) => {
            let quantifier = if *is_some { "SOME" } else { "ANY" };
            return Err(quantified_subquery_unsupported(compare_op, quantifier));
        }
        Expr::Function(func) => {
            reject_discarded_window_clauses(func)?;
            reject_percentile_fraction_out_of_range(func)?;
            if let Some(datafusion_name) = postgres_aggregate_alias(&func.name) {
                func.name = ObjectName::from(vec![Ident::new(datafusion_name)]);
            }
            return Ok(());
        }
        Expr::Like {
            pattern,
            escape_char: escape_char @ Some(_),
            ..
        }
        | Expr::ILike {
            pattern,
            escape_char: escape_char @ Some(_),
            ..
        } => {
            return rewrite_like_escape(pattern, escape_char);
        }
        Expr::BinaryOp {
            op: BinaryOperator::PGExp | BinaryOperator::PGStartsWith | BinaryOperator::PGOverlap,
            ..
        }
        | Expr::UnaryOp {
            op: UnaryOperator::BitwiseNot,
            ..
        }
        | Expr::SimilarTo { .. } => {}
        _ => return Ok(()),
    }

    match std::mem::replace(expr, Expr::value(Value::Null)) {
        // `^` is exponentiation in PostgreSQL and in DuckDB; DataFusion's planner reads
        // it as bitwise XOR and then rejects the node. `power()` is what both dialects
        // mean, so the parsed meaning is preserved rather than reinterpreted.
        //
        // `^@` and `&&` have no DataFusion operator at all. `&&` is array overlap here:
        // its range and geometric meanings need types VaireDB does not store.
        Expr::BinaryOp { left, op, right } => {
            let name = match op {
                BinaryOperator::PGExp => "power",
                BinaryOperator::PGStartsWith => "starts_with",
                _ => "array_has_any",
            };
            *expr = call(name, vec![*left, *right]);
            Ok(())
        }
        // DataFusion has no unary bitwise NOT, but it has XOR, and `~x` is `x # -1` for
        // every two's-complement integer width. The one visible difference from
        // PostgreSQL is the result type: XOR against a `bigint` literal widens `~int4`
        // from `integer` to `bigint`. The value is identical.
        Expr::UnaryOp { expr: inner, .. } => {
            *expr = Expr::BinaryOp {
                left: inner,
                op: BinaryOperator::PGBitwiseXor,
                right: Box::new(Expr::value(Value::Number("-1".to_string(), false))),
            };
            Ok(())
        }
        Expr::SimilarTo {
            negated,
            expr: inner,
            pattern,
            escape_char,
        } => {
            let matched = similar_to_call(*inner, &pattern, escape_char.as_ref())?;
            *expr = if negated {
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(matched),
                }
            } else {
                matched
            };
            Ok(())
        }
        other => {
            *expr = other;
            Ok(())
        }
    }
}

/// Refuse the three window-function clauses DataFusion parses and then drops.
///
/// Each of the three is a clause that changes which rows the function sees, so losing
/// it does not fail — it answers a different question and says nothing about it. All
/// three were measured against DataFusion 54.1 on a sharded table, and each refusal is
/// scoped to exactly the form that loses the clause, because the neighbouring forms are
/// correct and refusing them would cost a working query:
///
/// * `FILTER` is dropped **only when combined with `OVER`**: the serialized window
///   expression has no field to carry it, so `sum(x) FILTER (WHERE x > 10) OVER (...)`
///   sums every row. On a plain aggregate `FILTER` is applied correctly and stays
///   accepted.
/// * A window spec that *names* another window loses the named window's own
///   `PARTITION BY` and `ORDER BY` — `OVER (w ORDER BY x)`, and even `OVER (w)`,
///   aggregate over the whole result instead of over `w`'s partitions. Referring to a
///   named window without parentheses, `OVER w`, resolves correctly and stays accepted.
/// * `IGNORE NULLS` is discarded, so `last_value(x) IGNORE NULLS` still returns the
///   NULL it was told to skip. `RESPECT NULLS` asks for the default and so loses
///   nothing by being dropped.
fn reject_discarded_window_clauses(func: &Function) -> PgWireResult<()> {
    use crate::sqlparser::ast::{NullTreatment, WindowType};

    if func.filter.is_some() && func.over.is_some() {
        return Err(unsupported(
            format!("FILTER on the window function {}", func.name),
            "the window expression carries no filter across the wire, so every row in \
             the window would be aggregated as though the FILTER were absent; move the \
             condition into a CASE expression inside the aggregate, or aggregate a \
             subquery that applies it in its WHERE",
        ));
    }

    if let Some(WindowType::WindowSpec(spec)) = &func.over
        && let Some(name) = &spec.window_name
    {
        return Err(unsupported(
            format!("a window specification referring to the named window {name}"),
            "the named window's own PARTITION BY and ORDER BY are dropped, so the \
             function would aggregate over the whole result instead of over that \
             window; write the clauses out in the OVER (...) itself, or refer to the \
             window without parentheses as OVER <name>",
        ));
    }

    if matches!(func.null_treatment, Some(NullTreatment::IgnoreNulls)) {
        return Err(unsupported(
            format!("IGNORE NULLS on {}", func.name),
            "the clause is discarded, so a NULL it asks to skip would still be \
             returned; filter the nulls out in a subquery instead",
        ));
    }

    Ok(())
}

/// Refuse a `WINDOW` clause that defines one window in terms of another.
///
/// The same lost clause as the `OVER (w …)` refusal above, declared in the other place
/// PostgreSQL lets it be declared. `WINDOW w1 AS (PARTITION BY cat ORDER BY id),
/// w2 AS (w1)` then used as `OVER w2` measured `100, 100, 100, 100` on DataFusion 54.1 —
/// the whole-table sum — against PostgreSQL's `10, 30, 30, 70`; adding a clause of its
/// own, `w2 AS (w1 ORDER BY id)`, measured `10, 30, 60, 100`, which is `w1`'s
/// `PARTITION BY` dropped. Referring to `w1` directly as `OVER w1` is correct and stays
/// accepted, and so does any definition that names no other window.
///
/// The one inheritance spelling left alone is `w2 AS w1` without parentheses, which is
/// BigQuery's rather than PostgreSQL's and which DataFusion already rejects by name
/// (`The window w1 is not defined!`).
fn reject_chained_named_windows(stmt: &Statement) -> PgWireResult<()> {
    use crate::sqlparser::ast::{Query, SetExpr, Visit, Visitor};

    struct Chained;

    impl Visitor for Chained {
        type Break = PgWireError;

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<PgWireError> {
            match check_set_expr(&query.body) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }

    /// A nested `SetExpr::Query` is deliberately not followed — the visitor reaches that
    /// `Query` on its own, and checking it here would only duplicate the work.
    fn check_set_expr(body: &SetExpr) -> PgWireResult<()> {
        match body {
            SetExpr::Select(select) => check_named_windows(&select.named_window),
            SetExpr::SetOperation { left, right, .. } => {
                check_set_expr(left)?;
                check_set_expr(right)
            }
            _ => Ok(()),
        }
    }

    fn check_named_windows(
        windows: &[crate::sqlparser::ast::NamedWindowDefinition],
    ) -> PgWireResult<()> {
        use crate::sqlparser::ast::NamedWindowExpr;

        for crate::sqlparser::ast::NamedWindowDefinition(defined, expr) in windows {
            if let NamedWindowExpr::WindowSpec(spec) = expr
                && let Some(inherited) = &spec.window_name
            {
                return Err(unsupported(
                    format!("the window {defined} defined in terms of the window {inherited}"),
                    "the inherited PARTITION BY and ORDER BY are dropped, so a function \
                     using it would aggregate over the whole result instead of over that \
                     window; write the clauses out in each WINDOW definition",
                ));
            }
        }
        Ok(())
    }

    match stmt.visit(&mut Chained) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Refuse `INTERSECT ALL` and `EXCEPT ALL`, whose answers ignore how many times a row
/// appears.
///
/// The `ALL` in a set operation is not a synonym for "no `DISTINCT`": it makes the
/// operation count duplicates, so `INTERSECT ALL` keeps a row as many times as it appears
/// on *both* sides and `EXCEPT ALL` removes one left row per matching right row. Measured
/// on a 5-node cluster with the left side holding `1, 1, 1, 2` and the right `1, 1, 3`:
/// `INTERSECT ALL` answered `1, 1, 1` where PostgreSQL answers `1, 1`, and `EXCEPT ALL`
/// answered `2` where PostgreSQL answers `1, 2`. Both are the semi/anti join the
/// `DISTINCT` forms are built from, applied without the multiplicity bookkeeping.
///
/// So both are refused rather than answered: a row count is exactly what an analytical
/// client would go on to aggregate, and a plausible wrong one is worse than an error.
/// `INTERSECT` and `EXCEPT` are correct and stay accepted, and they are what the message
/// points at. `UNION ALL` is untouched — it concatenates, which is all its `ALL` asks for.
fn reject_multiplicity_set_operations(stmt: &Statement) -> PgWireResult<()> {
    use crate::sqlparser::ast::{Query, SetExpr, SetOperator, SetQuantifier, Visit, Visitor};

    struct Multiplicity;

    impl Visitor for Multiplicity {
        type Break = PgWireError;

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<PgWireError> {
            match check_set_expr(&query.body) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }

    /// As in [`reject_chained_named_windows`], a nested `SetExpr::Query` is left to the
    /// visitor, which reaches that `Query` itself.
    fn check_set_expr(body: &SetExpr) -> PgWireResult<()> {
        let SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } = body
        else {
            return Ok(());
        };
        // `MINUS` is `EXCEPT` under another name, so it counts duplicates the same way.
        if matches!(
            op,
            SetOperator::Intersect | SetOperator::Except | SetOperator::Minus
        ) && matches!(
            set_quantifier,
            SetQuantifier::All | SetQuantifier::AllByName
        ) {
            let distinct = match op {
                SetOperator::Intersect => "INTERSECT",
                _ => "EXCEPT",
            };
            return Err(unsupported(
                format!("{op} {set_quantifier}"),
                &format!(
                    "duplicate rows are not counted, so it answers neither the number of \
                     rows PostgreSQL does nor a subset of them; use {distinct}, which \
                     compares the rows as sets and is correct"
                ),
            ));
        }
        check_set_expr(left)?;
        check_set_expr(right)
    }

    match stmt.visit(&mut Multiplicity) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Refuse a quantified comparison over a subquery in one of the spellings that cannot
/// cross a stage boundary — every one except the two [`normalize_any_all_subqueries`]
/// has already turned into `IN`/`NOT IN`.
///
/// `x > ALL (SELECT …)`, `x = ALL (…)` and `x > ANY (…)` all plan to a *mark* join, whose
/// output column is named `mark` on both sides of the join it feeds. Serializing that plan
/// for an executor fails — `Schema contains duplicate unqualified field name mark` — so
/// the query reached the client as `XX000` carrying a raw gRPC `Status { … }`. Refusing
/// says the same thing without the internals, and names the rewrite that works.
///
/// The rewrite is *named*, not applied: `x > (SELECT max(c) …)` differs from
/// `x > ALL (SELECT c …)` on an empty subquery and on one containing a NULL, so
/// substituting it would answer a different question quietly. Which of the two the client
/// wants is the client's to decide.
///
/// [`normalize_any_all_subqueries`]: super::compat_rewrite::normalize_any_all_subqueries
fn quantified_subquery_unsupported(
    compare_op: &BinaryOperator,
    quantifier: &'static str,
) -> PgWireError {
    let aggregate = if quantifier == "ALL" {
        "max()/min()"
    } else {
        "min()/max()"
    };
    unsupported(
        format!("{compare_op} {quantifier} (subquery)"),
        &format!(
            "the plan it needs cannot be shipped to an executor; compare against an \
             aggregate over the same subquery instead — {aggregate} for an ordering \
             operator — or use EXISTS, and note that the aggregate form answers \
             differently for an empty subquery and for one containing NULLs. \
             `= ANY (subquery)` and `<> ALL (subquery)` are supported"
        ),
    )
}

/// The DataFusion aggregate a PostgreSQL name means, for the three names that differ
/// only in spelling. `None` for every other function, which is left alone.
///
/// `variance` and `every` are exact synonyms of `var_samp` and `bool_and`, so those
/// two are pure renames. `any_value` is not a rename: PostgreSQL defines it as *an
/// arbitrary value among the non-null inputs*, and `min` satisfies that contract —
/// it skips nulls, so it never answers `NULL` where a value exists, and it is
/// deterministic, which "arbitrary" permits and a user comparing two runs
/// appreciates. DataFusion's own `first_value` would be the closer-looking choice
/// and the wrong one: with no `ORDER BY` it can return the `NULL` PostgreSQL
/// promises to skip.
///
/// Renaming in the AST rather than registering three alias functions keeps the set
/// of functions the coordinator plans with identical to the set every executor
/// holds — an alias registered on one side only would fail the stage on the other.
/// The cost is that the result column is labelled with the DataFusion name; column
/// labelling is a separate, already-recorded gap.
fn postgres_aggregate_alias(name: &ObjectName) -> Option<&'static str> {
    if name.0.len() != 1 {
        return None;
    }
    match name
        .0
        .first()?
        .as_ident()?
        .value
        .to_ascii_lowercase()
        .as_str()
    {
        "variance" => Some("var_samp"),
        "every" => Some("bool_and"),
        "any_value" => Some("min"),
        _ => None,
    }
}

/// Refuse a `COLLATE` asking for anything other than the byte order VaireDB applies.
///
/// A collation is checked here, at parse time on the verbatim AST, and not alongside
/// the rewrites above — not by preference but because by then there is nothing left to
/// check. The read path's statements come from `datafusion-pg-catalog`'s compatibility
/// parser, whose `StripCollate` rule deletes every `COLLATE` clause on its way past;
/// the clause is only visible to a parse of the client's own text.
///
/// Stripping is the right answer for the collations that name byte order, and
/// [`is_byte_order_collation`] is what says which those are. For every other, the
/// statement asks for an ordering VaireDB does not implement and gets byte order
/// anyway: `'B' COLLATE "en_US" < 'a'` answers `true` where PostgreSQL answers
/// `false`, and nothing in the result says the collation never applied.
///
/// Takes a [`CoordinatorError`] rather than a `PgWireError` because [`parse_sql`] is
/// the one caller and parsing is not yet on a connection.
///
/// [`parse_sql`]: crate::pgwire_handler::parser::parse_sql
pub(super) fn reject_unsupported_collation(stmt: &Statement) -> crate::error::Result<()> {
    match visit_expressions(stmt, |expr| {
        if let Expr::Collate { collation, .. } = expr
            && !is_byte_order_collation(collation)
        {
            return ControlFlow::Break(CoordinatorError::Unsupported(format!(
                "COLLATE {collation} is not supported: VaireDB compares and orders text by \
                 byte value, so a collation that is not byte order would be ignored rather \
                 than applied; omit the COLLATE, or use \"C\""
            )));
        }
        ControlFlow::Continue(())
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Whether `collation` names the collation VaireDB in fact applies: byte order.
///
/// `C` and `POSIX` are PostgreSQL's names for it and `ucs_basic` is the SQL
/// standard's, so all three describe the comparison already in effect and can be
/// dropped without changing an answer. They are also the collations client
/// introspection queries reach for when they want a stable, locale-independent sort,
/// which is the case worth not breaking. `C` and `POSIX` are compared exactly, the
/// way PostgreSQL stores them: an unquoted `c` folds to lower case and is a different,
/// nonexistent collation.
///
/// `default` (`pg_catalog.default`) is accepted for a different reason: it does not
/// name an ordering at all, it names *the database's own* — and VaireDB's own is byte
/// order, so asking for the default gets exactly what it asked for. It has to be
/// accepted rather than merely tolerated: `psql`'s `\d` sends
/// `relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default`, so refusing it
/// would refuse the most ordinary introspection command there is. Unlike the other
/// three it is matched case-insensitively, since it is a keyword to the parser rather
/// than a stored collation name.
pub(crate) fn is_byte_order_collation(collation: &ObjectName) -> bool {
    let Some(name) = collation.0.last().and_then(|part| part.as_ident()) else {
        return false;
    };
    name.value == "C"
        || name.value == "POSIX"
        || name.value.eq_ignore_ascii_case("ucs_basic")
        || name.value.eq_ignore_ascii_case("default")
}

/// Refuse a percentile whose fraction is a literal outside 0..1, here rather than there.
///
/// The UDAFs check the fraction themselves ([`vairedb_common::udaf`]) and have to — it
/// need not be a literal the coordinator can see. But that check runs where the
/// aggregate runs, on an executor, so what reaches the client is a *job* failure that
/// wraps the reason in a stage number and a `DataFusionError(Execution(...))` debug
/// dump, under an error class about transport rather than about the argument. A literal
/// fraction is decidable before any plan is shipped, which is what lets the client have
/// PostgreSQL's own message and an `invalid_parameter_value` SQLSTATE.
fn reject_percentile_fraction_out_of_range(func: &Function) -> PgWireResult<()> {
    let Some(name) = func.name.0.last().and_then(|part| part.as_ident()) else {
        return Ok(());
    };
    if !name.value.eq_ignore_ascii_case("percentile_cont")
        && !name.value.eq_ignore_ascii_case("percentile_disc")
    {
        return Ok(());
    }

    let FunctionArguments::List(list) = &func.args else {
        return Ok(());
    };
    // Anything that is not a plain number — a parameter, a column, or the `ARRAY[…]`
    // multi-fraction form — is left to the UDAF, which sees the evaluated value.
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))) = list.args.first() else {
        return Ok(());
    };
    let Some(fraction) = numeric_literal(argument) else {
        return Ok(());
    };

    if !(0.0..=1.0).contains(&fraction) {
        return Err(make_vdb_error(
            VdbErrorCode::InvalidParameterValue,
            // Worded and formatted as the UDAF words it, so the two places this can be
            // caught cannot be told apart by the message.
            format!("percentile value {fraction} is not between 0 and 1"),
        ));
    }
    Ok(())
}

/// The number a literal argument spells, however the parser shaped it — a bare `0.5`,
/// a negated `-0.5`, or either of those in parentheses.
fn numeric_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => numeric_literal(expr).map(|n| -n),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => numeric_literal(expr),
        Expr::Nested(inner) => numeric_literal(inner),
        _ => None,
    }
}

/// Re-spell a `LIKE`/`ILIKE` pattern so its escape character is the backslash.
///
/// PostgreSQL lets `ESCAPE` name any single character, and DataFusion accepts exactly
/// one of them — anything else fails at execution with *"LIKE does not support
/// escape_char other than the backslash"*, which refuses an entirely ordinary
/// predicate (`name LIKE 'a!_%' ESCAPE '!'`) that the write path already runs. So the
/// pattern is rewritten rather than the statement refused: the escape character becomes
/// `\`, each `<escape>X` pair becomes `\X`, and each backslash that was an ordinary
/// character under the old escape becomes `\\` so that it stays one under the new.
///
/// Only a literal pattern can be re-spelled, for the same reason a `SIMILAR TO` can
/// only be translated as a literal — this runs before anything is evaluated. A
/// parameter or column pattern is refused instead of being handed to DataFusion to
/// fail on later, so the client is told why.
///
/// The write path needs none of this: DuckDB honours whatever `ESCAPE` names, and
/// `write_sql_cl::dialect` only has to supply the `\` default PostgreSQL has and
/// DuckDB does not.
fn rewrite_like_escape(
    pattern: &mut Expr,
    escape_char: &mut Option<ValueWithSpan>,
) -> PgWireResult<()> {
    let escape = match escape_char.as_ref().map(|v| &v.value) {
        // `ESCAPE ''` turns escaping off, leaving `_` and `%` as the only metacharacters.
        Some(Value::SingleQuotedString(s)) if s.is_empty() => None,
        Some(Value::SingleQuotedString(s)) if s.chars().count() == 1 => s.chars().next(),
        // Only reachable with an `ESCAPE` present, and what is left is what PostgreSQL
        // rejects too.
        _ => {
            return Err(make_vdb_error(
                VdbErrorCode::InvalidParameterValue,
                "invalid escape string: it must be empty or one character",
            ));
        }
    };

    // Already the escape DataFusion reads, so the pattern already means what it says.
    if escape == Some('\\') {
        return Ok(());
    }

    let Expr::Value(value) = &*pattern else {
        return Err(non_literal_like_pattern());
    };
    let Value::SingleQuotedString(literal) = &value.value else {
        return Err(non_literal_like_pattern());
    };

    let respelled = like_pattern_with_backslash_escape(literal, escape)?;
    *pattern = Expr::value(Value::SingleQuotedString(respelled));
    *escape_char = Some(Value::SingleQuotedString("\\".to_string()).with_empty_span());
    Ok(())
}

/// A `LIKE ... ESCAPE` whose pattern is not a string literal, so there is nothing to
/// re-spell before the query runs.
fn non_literal_like_pattern() -> PgWireError {
    unsupported(
        "LIKE with a non-literal pattern and a non-backslash ESCAPE",
        "the pattern's escape character is rewritten to a backslash before the query \
         runs, so the pattern has to be a string literal; use ESCAPE '\\' to match a \
         computed pattern",
    )
}

/// The same `LIKE` pattern with `\` in place of `escape` as its escape character.
fn like_pattern_with_backslash_escape(pattern: &str, escape: Option<char>) -> PgWireResult<String> {
    let mut out = String::with_capacity(pattern.len() + 2);
    let mut chars = pattern.chars();

    while let Some(c) = chars.next() {
        if Some(c) == escape {
            // The escape makes whatever follows literal — including another escape, and
            // including a character that was not a metacharacter to begin with, which
            // PostgreSQL allows. A backslash does the same job, so the pair carries over.
            let Some(next) = chars.next() else {
                return Err(make_vdb_error(
                    VdbErrorCode::InvalidParameterValue,
                    "LIKE pattern must not end with escape character",
                ));
            };
            out.push('\\');
            out.push(next);
        } else if c == '\\' {
            // An ordinary character under the old escape, which has to stay one under the
            // new: left alone it would escape the character after it instead.
            out.push_str("\\\\");
        } else {
            out.push(c);
        }
    }

    Ok(out)
}

/// Build `regexp_like(target, <pattern as a POSIX regex>)` for a `SIMILAR TO`.
///
/// The pattern has to be translated rather than passed through: DataFusion and DuckDB
/// both hand a `SIMILAR TO` pattern to a regex engine unchanged, which is wrong in
/// both directions — `%` under-matches, because it is a literal percent to a regex,
/// and `.` over-matches, because it is a literal dot to `SIMILAR TO`.
///
/// Only a literal pattern can be translated here, since the translation happens
/// before anything is evaluated. A parameter or column pattern is refused rather
/// than passed through to be matched by the wrong rules.
fn similar_to_call(
    target: Expr,
    pattern: &Expr,
    escape_char: Option<&ValueWithSpan>,
) -> PgWireResult<Expr> {
    let regex = similar_to_regex_from_ast(pattern, escape_char).map_err(|e| e.into_pg_error())?;

    Ok(call(
        "regexp_like",
        vec![target, Expr::value(Value::SingleQuotedString(regex))],
    ))
}

/// Why a `SIMILAR TO` cannot be translated, as [`similar_to_regex_from_ast`] reports it.
///
/// An enum rather than a formatted message because the two paths raise it in different
/// error types at different moments — the read path answers a client mid-plan, the write
/// path refuses at parse time — and each needs the SQLSTATE, not the prose.
pub(crate) enum SimilarToReject {
    /// The pattern is a parameter, a column or an expression, so there is nothing to
    /// translate before the query runs.
    NonLiteralPattern,
    /// `ESCAPE` was given something other than an empty or one-character string.
    InvalidEscape,
    /// The pattern is a literal, and not a well-formed `SIMILAR TO` pattern.
    InvalidPattern(String),
}

impl SimilarToReject {
    /// The message a client sees, whichever path refused.
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NonLiteralPattern => "SIMILAR TO with a non-literal pattern is not supported: \
                 the pattern is translated to a regular expression before the query runs, so it \
                 has to be a string literal; use regexp_like() to match a computed pattern"
                .to_string(),
            Self::InvalidEscape => {
                "invalid escape string: it must be empty or one character".to_string()
            }
            Self::InvalidPattern(e) => format!("invalid SIMILAR TO pattern: {e}"),
        }
    }

    fn code(&self) -> VdbErrorCode {
        match self {
            Self::NonLiteralPattern => VdbErrorCode::FeatureNotSupported,
            Self::InvalidEscape | Self::InvalidPattern(_) => VdbErrorCode::InvalidParameterValue,
        }
    }

    fn into_pg_error(self) -> PgWireError {
        make_vdb_error(self.code(), self.message())
    }
}

/// The POSIX regex a `SIMILAR TO` means, taken from the pattern and `ESCAPE` as parsed.
///
/// Shared with the write path ([`crate::write_sql_cl::translate_similar_to`]). The two
/// paths build different calls around the result — DataFusion has `regexp_like`, DuckDB
/// has `regexp_matches` — but the pattern language being translated is PostgreSQL's in
/// both cases, so translating it in two places would be two places for it to drift from
/// PostgreSQL. The regex is anchored (`^(?:…)$`), which is what makes it mean the same
/// thing to a partial-match function as to a full-match one.
pub(crate) fn similar_to_regex_from_ast(
    pattern: &Expr,
    escape_char: Option<&ValueWithSpan>,
) -> Result<String, SimilarToReject> {
    let Expr::Value(value) = pattern else {
        return Err(SimilarToReject::NonLiteralPattern);
    };
    let Value::SingleQuotedString(pattern) = &value.value else {
        return Err(SimilarToReject::NonLiteralPattern);
    };

    let escape = match escape_char.map(|v| &v.value) {
        None => Some('\\'),
        // PostgreSQL's own default, and `ESCAPE ''` explicitly turns it off.
        Some(Value::SingleQuotedString(s)) if s.is_empty() => None,
        Some(Value::SingleQuotedString(s)) if s.chars().count() == 1 => s.chars().next(),
        Some(_) => return Err(SimilarToReject::InvalidEscape),
    };

    similar_to_regex(pattern, escape).map_err(SimilarToReject::InvalidPattern)
}

/// Translate a SQL `SIMILAR TO` pattern into the POSIX regular expression that means
/// the same thing, following PostgreSQL's own `similar_escape_internal` step for step
/// — including where that is naive.
///
/// The naivety is deliberate: a bracket expression is closed by the first `]` after
/// the `[`, which mis-reads `[[:alpha:]]`, and an escaped `"` becomes a group
/// delimiter, which is a `SUBSTRING` feature PostgreSQL applies to every pattern.
/// Both are quirks a client can observe on a real PostgreSQL server, and PostgreSQL
/// is the contract, so reproducing them is closer to right than improving on them.
///
/// The result is wrapped in `^(?:…)$` because `SIMILAR TO` matches the whole string
/// while a regex match does not.
fn similar_to_regex(pattern: &str, escape: Option<char>) -> Result<String, String> {
    let mut out = String::with_capacity(pattern.len() * 2 + 6);
    out.push_str("^(?:");

    let mut after_escape = false;
    let mut in_char_class = false;
    let mut quotes = 0usize;

    for c in pattern.chars() {
        if after_escape {
            if c == '"' && !in_char_class {
                // A `SUBSTRING` capture marker, per PostgreSQL, in any pattern.
                out.push(if quotes.is_multiple_of(2) { '(' } else { ')' });
                quotes += 1;
            } else {
                out.push('\\');
                out.push(c);
            }
            after_escape = false;
        } else if Some(c) == escape {
            after_escape = true;
        } else if in_char_class {
            // Inside a bracket expression the two languages agree, so it is copied
            // through; only a backslash needs doubling, since it is literal to
            // `SIMILAR TO` and an escape to the regex engine.
            if c == '\\' {
                out.push('\\');
            }
            out.push(c);
            if c == ']' {
                in_char_class = false;
            }
        } else {
            match c {
                '[' => {
                    out.push('[');
                    in_char_class = true;
                }
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                // Non-capturing, so the group cannot be confused with a `SUBSTRING`
                // capture.
                '(' => out.push_str("(?:"),
                '\\' | '.' | '^' | '$' => {
                    out.push('\\');
                    out.push(c);
                }
                // `|`, `*`, `+`, `?`, `{`, `}` and `)` mean the same in both languages.
                _ => out.push(c),
            }
        }
    }

    if after_escape {
        return Err("the pattern ends with its escape character".to_string());
    }

    out.push_str(")$");
    Ok(out)
}

/// A `0A000` naming the expression refused and what to write instead.
fn unsupported(what: impl std::fmt::Display, why: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!("{what} is not supported: {why}"),
    )
}

/// Build a plain `name(args…)` call — no `DISTINCT`, no `FILTER`, no window.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|a| FunctionArg::Unnamed(FunctionArgExpr::Expr(a)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    /// Parse one statement, apply the rewrites, and render the result back to SQL.
    fn rewritten(sql: &str) -> Result<String, String> {
        let mut stmt: Statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0);
        match rewrite_pg_expressions(&mut stmt) {
            Ok(()) => Ok(stmt.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }

    // The three operators PostgreSQL and DuckDB both have and DataFusion's planner
    // rejects. Each becomes the DataFusion function with the same meaning, so the
    // client's own spelling is what changes and not the answer.
    #[test]
    fn rewrites_the_operators_datafusion_lacks_to_equivalent_calls() {
        assert_eq!(
            rewritten("SELECT 2 ^ 10").unwrap(),
            "SELECT power(2, 10)",
            "`^` is exponentiation in PostgreSQL, not XOR"
        );
        assert_eq!(
            rewritten("SELECT s ^@ 'a' FROM t").unwrap(),
            "SELECT starts_with(s, 'a') FROM t"
        );
        assert_eq!(
            rewritten("SELECT a && b FROM t").unwrap(),
            "SELECT array_has_any(a, b) FROM t"
        );
        assert_eq!(
            rewritten("SELECT ~5").unwrap(),
            "SELECT 5 # -1",
            "bitwise NOT is XOR against all-ones"
        );
    }

    // Post-order visiting is what makes a nested rewrite work: the inner `^` is
    // rewritten before the outer one, and neither rewrite's own operands are
    // re-examined.
    #[test]
    fn rewrites_nested_occurrences() {
        assert_eq!(
            rewritten("SELECT 2 ^ 3 ^ 2").unwrap(),
            "SELECT power(power(2, 3), 2)"
        );
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE (a ^ 2) > 3 AND ~b = 0").unwrap(),
            "SELECT 1 FROM t WHERE (power(a, 2)) > 3 AND b # -1 = 0"
        );
    }

    // `%` and `_` are the whole point: a regex engine reads them as a literal percent
    // and a literal underscore, which is why passing the pattern through unchanged
    // under-matches. And `.` has to be escaped, or it over-matches.
    #[test]
    fn translates_similar_to_wildcards_and_escapes_regex_metacharacters() {
        assert_eq!(similar_to_regex("a%", Some('\\')).unwrap(), "^(?:a.*)$");
        assert_eq!(similar_to_regex("a_c", Some('\\')).unwrap(), "^(?:a.c)$");
        assert_eq!(similar_to_regex("a.c", Some('\\')).unwrap(), r"^(?:a\.c)$");
        assert_eq!(
            similar_to_regex("a^$c", Some('\\')).unwrap(),
            r"^(?:a\^\$c)$"
        );
        // Regex metacharacters `SIMILAR TO` shares keep their meaning.
        assert_eq!(
            similar_to_regex("(a|b)+c{2,3}", Some('\\')).unwrap(),
            "^(?:(?:a|b)+c{2,3})$"
        );
    }

    // The escape character makes the next character literal, including a wildcard,
    // which is the only way to match a real `%`.
    #[test]
    fn honors_the_escape_character() {
        assert_eq!(
            similar_to_regex(r"a\%b", Some('\\')).unwrap(),
            r"^(?:a\%b)$"
        );
        assert_eq!(similar_to_regex("a#%b", Some('#')).unwrap(), r"^(?:a\%b)$");
        // With no escape character, a backslash is an ordinary literal.
        assert_eq!(similar_to_regex(r"a\b", None).unwrap(), r"^(?:a\\b)$");
        assert!(similar_to_regex(r"ab\", Some('\\')).is_err());
    }

    // Inside a bracket expression the two languages agree, so `%` is a literal
    // percent there and must not become `.*`.
    #[test]
    fn leaves_bracket_expressions_alone() {
        assert_eq!(
            similar_to_regex("[a%_]x", Some('\\')).unwrap(),
            "^(?:[a%_]x)$"
        );
    }

    // A whole-string match, not a search: `'abc' SIMILAR TO 'a'` is false.
    #[test]
    fn anchors_the_translated_pattern() {
        let regex = similar_to_regex("a", Some('\\')).unwrap();
        assert!(regex.starts_with("^(?:") && regex.ends_with(")$"));
    }

    #[test]
    fn rewrites_similar_to_into_an_anchored_regexp_like() {
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s SIMILAR TO 'a%'").unwrap(),
            "SELECT 1 FROM t WHERE regexp_like(s, '^(?:a.*)$')"
        );
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s NOT SIMILAR TO 'a%'").unwrap(),
            "SELECT 1 FROM t WHERE NOT regexp_like(s, '^(?:a.*)$')"
        );
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s SIMILAR TO 'a#%' ESCAPE '#'").unwrap(),
            r"SELECT 1 FROM t WHERE regexp_like(s, '^(?:a\%)$')"
        );
    }

    // Translation happens before evaluation, so a pattern that is only known at run
    // time cannot be translated — and passing it through would match by the wrong
    // rules, silently.
    #[test]
    fn refuses_a_similar_to_pattern_it_cannot_translate() {
        let err = rewritten("SELECT 1 FROM t WHERE s SIMILAR TO p").unwrap_err();
        assert!(err.contains("SIMILAR TO"), "{err}");
        assert!(err.contains("regexp_like"), "{err}");
    }

    // The fraction is the one thing about a percentile that is knowable without running
    // it, and knowing it here is the difference between an argument error and a failed
    // distributed job.
    #[test]
    fn refuses_a_percentile_fraction_outside_the_range() {
        for sql in [
            "SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_disc(-0.5) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_cont((2)) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT PERCENTILE_DISC(1.0001) WITHIN GROUP (ORDER BY n) FROM t",
        ] {
            let err = rewritten(sql).unwrap_err();
            assert!(err.contains("is not between 0 and 1"), "`{sql}`: {err}");
        }
    }

    #[test]
    fn leaves_a_percentile_it_cannot_fault_alone() {
        for sql in [
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_disc(0) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_cont(1) WITHIN GROUP (ORDER BY n) FROM t",
            // Not a literal, so the fraction is the UDAF's to check at run time.
            "SELECT percentile_cont(f) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_cont($1) WITHIN GROUP (ORDER BY n) FROM t",
            // The multi-fraction form: an array, not a number.
            "SELECT percentile_cont(ARRAY[0.5, 1.5]) WITHIN GROUP (ORDER BY n) FROM t",
        ] {
            assert_eq!(rewritten(sql).unwrap(), sql, "`{sql}` must be untouched");
        }
    }

    // A client-chosen `ESCAPE` is one DataFusion refuses outright, so the pattern is
    // re-spelled with the one it does read. The set of strings matched is what has to
    // survive: `a!_%` matches a literal underscore, and so does `a\_%`.
    #[test]
    fn respells_a_like_escape_as_a_backslash() {
        assert_eq!(
            like_pattern_with_backslash_escape("a!_%", Some('!')).unwrap(),
            r"a\_%"
        );
        // The escape escaping itself, and escaping something that needed no escaping.
        assert_eq!(
            like_pattern_with_backslash_escape("a!!b!c", Some('!')).unwrap(),
            r"a\!b\c"
        );
        // A backslash was an ordinary character under `!`, and must stay one under `\`.
        assert_eq!(
            like_pattern_with_backslash_escape(r"a\b!%", Some('!')).unwrap(),
            r"a\\b\%"
        );
        // `ESCAPE ''` turns escaping off, so every backslash is a literal too.
        assert_eq!(
            like_pattern_with_backslash_escape(r"a\_%", None).unwrap(),
            r"a\\_%"
        );
        // Nothing follows the escape to be made literal.
        assert!(like_pattern_with_backslash_escape("ab!", Some('!')).is_err());
    }

    #[test]
    fn rewrites_a_like_escape_and_leaves_a_backslash_one_alone() {
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s LIKE 'a!_%' ESCAPE '!'").unwrap(),
            r"SELECT 1 FROM t WHERE s LIKE 'a\_%' ESCAPE '\'"
        );
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s ILIKE 'a#%b' ESCAPE '#'").unwrap(),
            r"SELECT 1 FROM t WHERE s ILIKE 'a\%b' ESCAPE '\'"
        );
        assert_eq!(
            rewritten("SELECT 1 FROM t WHERE s NOT LIKE 'a!_%' ESCAPE '!'").unwrap(),
            r"SELECT 1 FROM t WHERE s NOT LIKE 'a\_%' ESCAPE '\'"
        );
        // Already the escape DataFusion reads: unchanged, including the pattern, which
        // must not be re-spelled twice.
        assert_eq!(
            rewritten(r"SELECT 1 FROM t WHERE s LIKE 'a\_%' ESCAPE '\'").unwrap(),
            r"SELECT 1 FROM t WHERE s LIKE 'a\_%' ESCAPE '\'"
        );
        // No `ESCAPE` at all is DataFusion's own default, so there is nothing to do.
        assert_eq!(
            rewritten(r"SELECT 1 FROM t WHERE s LIKE 'a\_%'").unwrap(),
            r"SELECT 1 FROM t WHERE s LIKE 'a\_%'"
        );
    }

    // The re-spelling happens before evaluation, so a pattern known only at run time
    // cannot be re-spelled — and left alone it would fail inside the executor with
    // DataFusion's own message instead of ours.
    #[test]
    fn refuses_a_like_escape_it_cannot_respell() {
        let err = rewritten("SELECT 1 FROM t WHERE s LIKE p ESCAPE '!'").unwrap_err();
        assert!(err.contains("non-literal pattern"), "{err}");

        let err = rewritten("SELECT 1 FROM t WHERE s LIKE 'a!_' ESCAPE '!!'").unwrap_err();
        assert!(err.contains("one character"), "{err}");
    }

    /// Parse one statement and run the parse-time collation check over it, the way
    /// [`crate::pgwire_handler::parser::parse_sql`] does — on the client's own text,
    /// before the compatibility parser has a chance to strip the clause.
    fn collation_checked(sql: &str) -> Result<(), String> {
        let stmt: Statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0);
        reject_unsupported_collation(&stmt).map_err(|e| e.to_string())
    }

    // Byte order is what VaireDB does, so asking for it by name changes nothing and
    // the clause can go. Client introspection queries rely on this — `pg_catalog`
    // probes routinely order by `relname COLLATE "C"`.
    #[test]
    fn accepts_the_collations_that_are_byte_order() {
        collation_checked(r#"SELECT s COLLATE "C" FROM t"#).unwrap();
        collation_checked(r#"SELECT 1 FROM t ORDER BY s COLLATE "POSIX""#).unwrap();
        collation_checked("SELECT s COLLATE ucs_basic FROM t").unwrap();
        // The database's own collation, which is byte order. This is the shape `psql`'s
        // `\d` sends, so it is the one an accidental refusal would be noticed by first.
        collation_checked(
            "SELECT 1 FROM pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' \
             COLLATE pg_catalog.default",
        )
        .unwrap();
        collation_checked("SELECT s COLLATE DEFAULT FROM t").unwrap();
    }

    // Any other collation would be parsed and thrown away, and the rows would come
    // back in byte order looking plausible. Naming the refusal is the whole point.
    #[test]
    fn refuses_a_collation_it_would_otherwise_ignore() {
        let err = collation_checked(r#"SELECT 1 FROM t ORDER BY s COLLATE "en_US""#).unwrap_err();
        assert!(err.contains("COLLATE"), "{err}");
        assert!(err.contains("byte value"), "{err}");
        // Wherever an expression can appear: a projection, a predicate, a subquery.
        assert!(collation_checked(r#"SELECT s COLLATE "de_DE" FROM t"#).is_err());
        assert!(collation_checked(r#"SELECT 1 FROM t WHERE s COLLATE "de_DE" < 'a'"#).is_err());
        assert!(
            collation_checked(r#"SELECT (SELECT max(s COLLATE "de_DE") FROM u) FROM t"#).is_err()
        );
        // An unquoted `c` is not PostgreSQL's `C`: it folds to lower case and names
        // no collation at all.
        assert!(collation_checked("SELECT s COLLATE c FROM t").is_err());
    }

    // The premise of checking a collation at parse time rather than alongside the
    // rewrites: by the time the read path holds an AST, the clause has been deleted
    // from it. If this ever fails because upstream stopped stripping, the refusal
    // above still stands — but `COLLATE "C"` would start reaching DataFusion, which
    // rejects the node, and the accepting half would need a rewrite of its own.
    #[test]
    fn the_compatibility_parser_strips_collate_before_the_read_path_sees_it() {
        let stmt = crate::pgwire_handler::parser::parse_sql(r#"SELECT s COLLATE "C" FROM t"#)
            .unwrap()
            .remove(0);
        assert_eq!(stmt.to_string(), "SELECT s FROM t");
    }

    // A discarded cast length is the same class of defect as a discarded collation:
    // the statement asks for truncation, gets none, and is told nothing.
    #[test]
    fn refuses_a_cast_length_it_would_discard() {
        for sql in [
            "SELECT CAST(s AS VARCHAR(5)) FROM t",
            "SELECT s::VARCHAR(5) FROM t",
            "SELECT CAST(s AS CHAR(5)) FROM t",
            "SELECT CAST(s AS CHARACTER VARYING(5)) FROM t",
        ] {
            let err = rewritten(sql).unwrap_err();
            assert!(err.contains("is not supported"), "{sql}: {err}");
            assert!(err.contains("substr()"), "{sql}: {err}");
        }
    }

    // An unbounded character cast is honest — there is no length to enforce — so it
    // stays accepted, as does every cast whose parameters DataFusion does apply.
    #[test]
    fn leaves_casts_without_a_discarded_length_untouched() {
        assert_eq!(
            rewritten("SELECT CAST(s AS VARCHAR) FROM t").unwrap(),
            "SELECT CAST(s AS VARCHAR) FROM t"
        );
        assert_eq!(
            rewritten("SELECT CAST(n AS NUMERIC(10, 2)) FROM t").unwrap(),
            "SELECT CAST(n AS NUMERIC(10,2)) FROM t"
        );
    }

    // Three PostgreSQL aggregate spellings DataFusion does not answer to. The first
    // two are exact synonyms; `min` is chosen for `any_value` because it honours
    // PostgreSQL's promise that the value returned is one of the non-null ones.
    #[test]
    fn renames_the_postgres_aggregate_spellings() {
        assert_eq!(
            rewritten("SELECT variance(x) FROM t").unwrap(),
            "SELECT var_samp(x) FROM t"
        );
        assert_eq!(
            rewritten("SELECT EVERY(b) FROM t").unwrap(),
            "SELECT bool_and(b) FROM t"
        );
        assert_eq!(
            rewritten("SELECT any_value(x) FROM t GROUP BY y").unwrap(),
            "SELECT min(x) FROM t GROUP BY y"
        );
        // A window use is the same node, so the OVER clause survives the rename.
        assert_eq!(
            rewritten("SELECT variance(x) OVER (PARTITION BY y) FROM t").unwrap(),
            "SELECT var_samp(x) OVER (PARTITION BY y) FROM t"
        );
        // A qualified name is somebody else's function, not one of these.
        assert_eq!(
            rewritten("SELECT myschema.every(b) FROM t").unwrap(),
            "SELECT myschema.every(b) FROM t"
        );
    }

    // The rewrites reach into a subquery too, since the visit walks the statement
    // rather than a projection list.
    #[test]
    fn reaches_expressions_inside_a_subquery() {
        assert_eq!(
            rewritten("SELECT (SELECT 2 ^ 3) AS x").unwrap(),
            "SELECT (SELECT power(2, 3)) AS x"
        );
    }

    // Everything else is left byte-identical: this runs on every SELECT, so a
    // statement with none of these forms must come through untouched.
    #[test]
    fn leaves_an_ordinary_statement_unchanged() {
        let sql = "SELECT a + b, c # d, e ~ '^x' FROM t WHERE f LIKE 'a%' ORDER BY a";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    // --- the window clauses DataFusion parses and drops ---

    // `FILTER` survives on a plain aggregate and is lost on a windowed one, so the
    // refusal has to distinguish the two rather than refuse the keyword.
    #[test]
    fn refuses_filter_only_when_the_aggregate_is_windowed() {
        let err = rewritten("SELECT sum(x) FILTER (WHERE x > 10) OVER (PARTITION BY g) FROM t")
            .expect_err("the filter would be dropped and every row summed");
        assert!(err.contains("FILTER"), "{err}");

        let sql = "SELECT sum(x) FILTER (WHERE x > 10) FROM t GROUP BY g";
        assert_eq!(
            rewritten(sql).unwrap(),
            sql,
            "an unwindowed FILTER is applied correctly, so refusing it would cost a \
             working query"
        );
    }

    // Naming a window inside the parentheses loses that window's own clauses; naming it
    // without them resolves correctly. `OVER (w)` with nothing added is the same defect,
    // so the refusal keys on the name, not on what accompanies it.
    #[test]
    fn refuses_a_window_spec_that_names_another_window() {
        for sql in [
            "SELECT sum(x) OVER (w ORDER BY x) FROM t WINDOW w AS (PARTITION BY g)",
            "SELECT sum(x) OVER (w) FROM t WINDOW w AS (PARTITION BY g ORDER BY x)",
        ] {
            let err = rewritten(sql).expect_err("the named window's clauses would be dropped");
            assert!(err.contains("named window"), "{err}");
        }

        let sql = "SELECT sum(x) OVER w FROM t WINDOW w AS (PARTITION BY g ORDER BY x)";
        assert_eq!(rewritten(sql).unwrap(), sql, "`OVER w` resolves correctly");
    }

    // The same lost clause declared in the `WINDOW` list rather than in the `OVER`. Both
    // spellings measured wrong on 54.1: `w2 AS (w1)` sums the whole table, and
    // `w2 AS (w1 ORDER BY id)` keeps the order and drops the partition.
    #[test]
    fn refuses_a_window_defined_in_terms_of_another_window() {
        for sql in [
            "SELECT sum(x) OVER w2 FROM t WINDOW w1 AS (PARTITION BY g), w2 AS (w1 ORDER BY x)",
            "SELECT sum(x) OVER w2 FROM t WINDOW w1 AS (PARTITION BY g ORDER BY x), w2 AS (w1)",
        ] {
            let err = rewritten(sql).expect_err("the inherited clauses would be dropped");
            assert!(err.contains("defined in terms of the window w1"), "{err}");
        }

        // Two independent definitions inherit nothing, so there is nothing to lose.
        let sql = "SELECT sum(x) OVER w1, sum(x) OVER w2 FROM t \
                   WINDOW w1 AS (PARTITION BY g), w2 AS (ORDER BY x)";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    // The chained definition is refused wherever it is written, including inside a
    // derived table, where the enclosing query's own `WINDOW` list is a different one.
    #[test]
    fn refuses_a_chained_window_inside_a_subquery() {
        let err = rewritten(
            "SELECT s FROM (SELECT sum(x) AS s FROM t \
             WINDOW w1 AS (PARTITION BY g), w2 AS (w1)) d",
        )
        .expect_err("a subquery's WINDOW clause loses the same clauses");
        assert!(err.contains("defined in terms of the window w1"), "{err}");
    }

    // `IGNORE NULLS` is discarded; `RESPECT NULLS` asks for the default, so dropping it
    // loses nothing and it stays accepted.
    #[test]
    fn refuses_ignore_nulls_but_not_respect_nulls() {
        let err = rewritten("SELECT last_value(x) IGNORE NULLS OVER (ORDER BY x) FROM t")
            .expect_err("a NULL it asks to skip would still be returned");
        assert!(err.contains("IGNORE NULLS"), "{err}");

        let sql = "SELECT last_value(x) RESPECT NULLS OVER (ORDER BY x) FROM t";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    // The ordinary window, with its clauses written out, is what all three refusals
    // point the client at — so it has to keep working.
    #[test]
    fn leaves_a_window_with_its_own_clauses_alone() {
        let sql = "SELECT row_number() OVER (PARTITION BY g ORDER BY x) FROM t";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    // `INTERSECT ALL` and `EXCEPT ALL` counted duplicates wrongly rather than not at all,
    // so the refusal is about an answer, and it names the form that is right.
    #[test]
    fn refuses_the_set_operations_that_ignore_duplicates() {
        for (sql, form, workaround) in [
            (
                "SELECT k FROM l INTERSECT ALL SELECT k FROM r",
                "INTERSECT ALL",
                "use INTERSECT",
            ),
            (
                "SELECT k FROM l EXCEPT ALL SELECT k FROM r",
                "EXCEPT ALL",
                "use EXCEPT",
            ),
            (
                "SELECT k FROM l MINUS ALL SELECT k FROM r",
                "MINUS ALL",
                "use EXCEPT",
            ),
        ] {
            let err = rewritten(sql).expect_err("the row count would be wrong");
            assert!(err.contains(form), "the message names the form: {err}");
            assert!(
                err.contains(workaround),
                "the message names the correct form: {err}"
            );
        }
    }

    // The set operations that are correct: the two `DISTINCT` forms the refusals point at,
    // and `UNION ALL`, whose `ALL` only asks for concatenation.
    #[test]
    fn leaves_the_set_operations_that_are_correct_alone() {
        for sql in [
            "SELECT k FROM l INTERSECT SELECT k FROM r",
            "SELECT k FROM l EXCEPT SELECT k FROM r",
            "SELECT k FROM l UNION ALL SELECT k FROM r",
            "SELECT k FROM l UNION SELECT k FROM r",
        ] {
            assert_eq!(rewritten(sql).unwrap(), sql);
        }
    }

    // A set operation nested in a subquery or a CTE is reached too: the visitor sees
    // every `Query`, not only the outermost.
    #[test]
    fn refuses_a_multiplicity_set_operation_inside_a_subquery() {
        let err = rewritten("SELECT count(*) FROM (SELECT k FROM l EXCEPT ALL SELECT k FROM r) d")
            .expect_err("a nested EXCEPT ALL is as wrong as a top-level one");
        assert!(err.contains("EXCEPT ALL"), "{err}");

        let err =
            rewritten("WITH c AS (SELECT k FROM l INTERSECT ALL SELECT k FROM r) SELECT * FROM c")
                .expect_err("a CTE's body is a query too");
        assert!(err.contains("INTERSECT ALL"), "{err}");
    }

    // The quantified comparisons that plan to a mark join reached the client as `XX000`
    // with a raw gRPC `Status { … }` in it. Refused by name instead, with the aggregate
    // rewrite spelled out — and with the two spellings that do work named, since they are
    // one character away from the refused ones.
    #[test]
    fn refuses_the_quantified_subquery_forms_that_cannot_be_shipped() {
        for (sql, form) in [
            (
                "SELECT id FROM l WHERE id > ALL (SELECT id FROM r)",
                "> ALL",
            ),
            (
                "SELECT id FROM l WHERE id = ALL (SELECT id FROM r)",
                "= ALL",
            ),
            (
                "SELECT id FROM l WHERE id > ANY (SELECT id FROM r)",
                "> ANY",
            ),
            (
                "SELECT id FROM l WHERE id < SOME (SELECT id FROM r)",
                "< SOME",
            ),
        ] {
            let err = rewritten(sql).expect_err("this plan cannot cross a stage boundary");
            assert!(
                err.contains(&format!("{form} (subquery)")),
                "the message names the form: {err}"
            );
            assert!(
                err.contains("<> ALL (subquery)"),
                "the message names what does work: {err}"
            );
        }
    }

    // An `ANY`/`ALL` over an array is a different expression that works, and the two
    // subquery spellings `parse_sql` normalizes to `IN`/`NOT IN` never reach this refusal
    // — so what arrives here as an `IN` has to pass.
    #[test]
    fn leaves_the_quantified_forms_that_work_alone() {
        // The array forms come back with sqlparser's own spacing, `ANY(…)`, which is a
        // rendering of the same expression and not a rewrite.
        for (sql, rendered) in [
            (
                "SELECT id FROM l WHERE id = ANY (ARRAY[1, 2])",
                "SELECT id FROM l WHERE id = ANY(ARRAY[1, 2])",
            ),
            (
                "SELECT id FROM l WHERE id <> ALL (ARRAY[1, 2])",
                "SELECT id FROM l WHERE id <> ALL(ARRAY[1, 2])",
            ),
            (
                "SELECT id FROM l WHERE id IN (SELECT id FROM r)",
                "SELECT id FROM l WHERE id IN (SELECT id FROM r)",
            ),
            (
                "SELECT id FROM l WHERE id NOT IN (SELECT id FROM r)",
                "SELECT id FROM l WHERE id NOT IN (SELECT id FROM r)",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered);
        }
    }
}
