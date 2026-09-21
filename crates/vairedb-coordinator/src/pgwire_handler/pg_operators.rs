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

use vairedb_common::bytea_in::BYTEA_IN_UDF_NAME;
use vairedb_common::json_agg::{
    JSON_AGG_NAME, JSON_ARRAY_DOCS_UDF_NAME, JSON_ARRAY_UDF_NAME, JSONB_AGG_NAME,
};
use vairedb_common::json_pg::{
    JSON_GET_TEXT_UDF_NAME, JSON_GET_UDF_NAME, JSON_IN_UDF_NAME, JSON_PATH_TEXT_UDF_NAME,
    JSON_PATH_UDF_NAME, JSONB_IN_UDF_NAME,
};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;
use vairedb_common::uuid_in::UUID_IN_UDF_NAME;

use crate::error::CoordinatorError;
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::pgwire_handler::pg_named_windows;
use crate::pgwire_handler::pg_set_op_multiplicity;
use crate::pgwire_handler::pg_subscripts;
use crate::sqlparser::ast::helpers::attached_token::AttachedToken;
use crate::sqlparser::ast::{
    BinaryOperator, CaseWhen, CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, Statement, UnaryOperator, Value,
    ValueWithSpan, visit_expressions, visit_expressions_mut,
};

/// Rewrite the PostgreSQL operators DataFusion lacks, and refuse the expressions it
/// would accept while quietly ignoring part of what they say.
///
/// Applied to a whole statement, so it reaches a `WHERE`, a projection, an
/// `ORDER BY` key and a subquery alike. The visit is post-order, so an inner
/// expression is rewritten before the one containing it and a rewrite's own
/// operands are never re-examined.
pub(super) fn rewrite_pg_expressions(stmt: &mut Statement) -> PgWireResult<()> {
    // Not an expression, so outside the visit below: a `WINDOW` clause is a property of the
    // select, and the inheritance it can declare — `OVER (w ...)` and `w2 AS (w1 ...)` — has
    // to be written out before the planner reads a specification whose name it ignores. See
    // [`pg_named_windows`].
    pg_named_windows::expand_named_windows(stmt)?;
    // Nor is a set operation an expression: `INTERSECT ALL` and `EXCEPT ALL` count duplicate
    // rows, which is the one thing the join DataFusion plans them as does not do, so they are
    // marked here for the plan-level rewrite that repairs them. See
    // [`pg_set_op_multiplicity`].
    pg_set_op_multiplicity::mark_multiplicity_set_operations(stmt)?;
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
        // A cast to `bytea`, which Arrow performs by copying the characters: the four bytes
        // of `'\xDEADBEEF'` came back as the ten of its text. PostgreSQL's own conversion
        // runs as a UDF instead ([`vairedb_common::bytea_in`]), which is what lets the same
        // rule serve the executors that run the projection.
        //
        // Rewritten from the AST rather than from the logical plan because the AST is where
        // the client's `::bytea` is still distinguishable: by planning time it is an
        // ordinary `CAST(… AS Binary)`, and VaireDB's own `Utf8` → `Binary` casts — a
        // `COPY`, a schema rebuild — must keep Arrow's meaning. Only the two spellings a
        // client writes, so a `TRY_CAST` is left as it was.
        //
        // `::json`, `::jsonb` and `::uuid` are here for a related but not identical reason:
        // Arrow has no cast at all, because all three are *stored* as text (see
        // [`vairedb_common::json_pg`] and [`vairedb_common::uuid_in`]) and the cast is a
        // validation rather than a change of representation. DataFusion's `convert_data_type`
        // has no arm for any of the three type names, so without this the cast does not plan.
        // Rewriting them here — and not only for their own sake — is what unblocks the `json`
        // operator family and `json_agg` below, which is why one planner gap held so much.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type: DataType::Bytea | DataType::JSON | DataType::JSONB | DataType::Uuid,
            ..
        } => {}
        // `@?` asks whether a jsonpath matches. A jsonpath is its own language with its own
        // parser, which VaireDB does not have, and there is no rewrite that approximates it
        // — so it is refused by name rather than left to fail as an unsupported operator with
        // nothing said about why.
        Expr::BinaryOp {
            op: op @ BinaryOperator::AtQuestion,
            ..
        } => {
            return Err(unsupported(
                format!("the {op} operator"),
                "it takes a jsonpath expression, which VaireDB does not implement; extract the \
                 value with ->, ->>, #> or #>> and test that instead",
            ));
        }
        // An out-of-range array subscript answers NULL in PostgreSQL and the last element
        // in DataFusion. The clamp is inside the brackets and the subscript stays a
        // subscript, which is what lets the write path apply the same one — see
        // [`super::pg_subscripts`].
        Expr::CompoundFieldAccess { access_chain, .. } => {
            pg_subscripts::clamp_to_pg_semantics(access_chain);
            return Ok(());
        }
        Expr::Function(func) => {
            // Before the refusal, because it is what turns `FILTER … OVER` from a refusal
            // into an answer for the aggregates it is exact for.
            rewrite_window_filter(func);
            reject_discarded_window_clauses(func)?;
            reject_ordered_set_without_within_group(func)?;
            reject_percentile_fraction_out_of_range(func)?;
            if let Some(datafusion_name) = postgres_aggregate_alias(&func.name) {
                func.name = ObjectName::from(vec![Ident::new(datafusion_name)]);
            }
            rename_hypothetical_set_aggregate(func);
            // `json_agg` is rewritten rather than renamed, so it falls through to the
            // arm below; every other function is finished with here.
            if !is_json_aggregate(&func.name) {
                return Ok(());
            }
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
            op:
                BinaryOperator::PGExp
                | BinaryOperator::PGStartsWith
                | BinaryOperator::PGOverlap
                // The `json` accessors. DataFusion's planner does map all four to an
                // `Operator`, but nothing implements them, so the failure would come from
                // physical planning with the client's spelling already gone.
                | BinaryOperator::Arrow
                | BinaryOperator::LongArrow
                | BinaryOperator::HashArrow
                | BinaryOperator::HashLongArrow,
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
        // `x::bytea` becomes `vaire_bytea_in(x)`, and the three text-backed types become
        // their own input conversions. The argument keeps whatever it was, so a literal is
        // folded by the simplifier and a column is converted per row.
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let name = match data_type {
                DataType::JSON => JSON_IN_UDF_NAME,
                DataType::JSONB => JSONB_IN_UDF_NAME,
                DataType::Uuid => UUID_IN_UDF_NAME,
                // The borrow-only match above admits no other cast to this arm.
                _ => BYTEA_IN_UDF_NAME,
            };
            *expr = call(name, vec![*inner]);
            Ok(())
        }
        // `json_agg(x)` becomes `vaire_json_array(array_agg(x))`, which is how the
        // in-aggregate `ORDER BY`, `DISTINCT` and `FILTER` PostgreSQL allows keep working
        // across a partial aggregate on each shard — see [`vairedb_common::json_agg`] for
        // why the aggregation is borrowed and only the rendering is ours.
        Expr::Function(func) => {
            *expr = json_aggregate_call(func);
            Ok(())
        }
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
                // The two `->` forms take a key or an index, the two `#>` forms a path; the
                // doubled forms return the extracted value as text where the single ones
                // return it as json. See [`vairedb_common::json_pg`].
                BinaryOperator::Arrow => JSON_GET_UDF_NAME,
                BinaryOperator::LongArrow => JSON_GET_TEXT_UDF_NAME,
                BinaryOperator::HashArrow => JSON_PATH_UDF_NAME,
                BinaryOperator::HashLongArrow => JSON_PATH_TEXT_UDF_NAME,
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

/// Refuse the window-function clauses DataFusion parses and then drops, for the forms
/// [`rewrite_window_filter`] and [`pg_named_windows`] do not turn into an answer first.
///
/// Each of these is a clause that changes which rows the function sees, so losing it does
/// not fail — it answers a different question and says nothing about it. All were measured
/// against DataFusion 54.1 on a sharded table, and each refusal is scoped to exactly the
/// form that loses the clause, because the neighbouring forms are correct and refusing them
/// would cost a working query:
///
/// * `FILTER` is dropped **only when combined with `OVER`**: the serialized window
///   expression has no field to carry it, so `sum(x) FILTER (WHERE x > 10) OVER (...)`
///   sums every row. On a plain aggregate `FILTER` is applied correctly and stays
///   accepted. [`rewrite_window_filter`] has already folded the predicate into the
///   argument for every aggregate where that is exact, so what reaches here is the rest:
///   the aggregates that count nulls, and `DISTINCT`.
/// * `IGNORE NULLS` is discarded, so `last_value(x) IGNORE NULLS` still returns the
///   NULL it was told to skip. This one is refused rather than rewritten because
///   PostgreSQL does not implement it either — §9.22 is explicit that the standard's
///   `RESPECT NULLS` / `IGNORE NULLS` option on `lead`, `lag`, `first_value`,
///   `last_value` and `nth_value` "is not implemented in PostgreSQL: the behavior is
///   always the same as the standard's default, namely RESPECT NULLS". So there is no
///   PostgreSQL meaning to match, and a refusal is the answer PostgreSQL gives too, only
///   as a parse error rather than as an unsupported feature. `RESPECT NULLS` asks for
///   the default and so loses nothing by being dropped.
/// * A window specification that still *names* another window. [`pg_named_windows`] writes
///   every such reference out before this runs and refuses the ones PostgreSQL refuses, so
///   this is a backstop rather than a rule: if some position that pass does not reach ever
///   appears, the outcome to have is a refusal and not a frame silently widened to the
///   whole result.
fn reject_discarded_window_clauses(func: &Function) -> PgWireResult<()> {
    use crate::sqlparser::ast::{NullTreatment, WindowType};

    if func.filter.is_some() && func.over.is_some() {
        return Err(unsupported(
            format!("FILTER on the window function {}", func.name),
            "the window expression carries no filter across the wire, so every row in \
             the window would be aggregated as though the FILTER were absent, and this \
             aggregate is one whose answer a null argument changes, so the condition \
             cannot be folded into the argument for you; aggregate a subquery that \
             applies the condition in its WHERE instead",
        ));
    }

    if matches!(func.null_treatment, Some(NullTreatment::IgnoreNulls)) {
        return Err(unsupported(
            format!("IGNORE NULLS on {}", func.name),
            "the clause is discarded, so a NULL it asks to skip would still be returned; \
             PostgreSQL does not implement this option either and always behaves as \
             RESPECT NULLS, so there is no PostgreSQL result to match — filter the nulls \
             out in a subquery if you need them skipped",
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

    Ok(())
}

/// Fold a window function's `FILTER (WHERE p)` into its argument, so
/// `agg(x) FILTER (WHERE p) OVER (…)` becomes `agg(CASE WHEN p THEN x END) OVER (…)`.
///
/// `FILTER` beside `OVER` is the one combination DataFusion drops: `Expr::WindowFunction`
/// has no filter field, so the predicate is parsed, discarded, and every row in the window
/// aggregated. Ballista would need a proto field and both codecs to carry one — but for the
/// aggregates that *ignore* nulls, nothing has to be carried at all. Turning the excluded
/// rows into NULL arguments removes them from the aggregate by the aggregate's own rule,
/// which is exactly the definition of `FILTER`, and it is the workaround the refusal used to
/// name. Doing it here means the client does not have to.
///
/// The rewrite is only exact where a NULL argument is *no* input:
///
/// * `count(x)` counts non-null arguments, so `count(*)` becomes `count(CASE WHEN p THEN 1
///   END)` — the value only has to be non-null.
/// * `sum`, `avg`, `min`, `max`, the `stddev`/`variance` family, `bool_and`/`bool_or` and
///   `bit_and`/`bit_or` all skip nulls, and all answer NULL over no rows, which is what
///   `FILTER` excluding every row answers too.
///
/// It is *not* exact for `array_agg`, `string_agg` or `json_agg`, which keep a null as an
/// element and would gain one entry per excluded row. Those keep the refusal above, which
/// now says why. `DISTINCT` is left alone for the same reason it is left alone elsewhere:
/// `count(DISTINCT x) OVER (…)` is not a form DataFusion evaluates, so folding into it would
/// only move where the failure comes from.
fn rewrite_window_filter(func: &mut Function) {
    if func.over.is_none() {
        return;
    }
    let Some(predicate) = func.filter.clone() else {
        return;
    };
    let Some(name) = bare_function_name(&func.name) else {
        return;
    };
    if !aggregate_ignores_null_arguments(&name) {
        return;
    }
    let FunctionArguments::List(list) = &mut func.args else {
        return;
    };
    // A single unnamed argument is the whole of the surface this applies to: the
    // multi-argument aggregates are the ones excluded above, and `DISTINCT` or a `WITHIN
    // GROUP`-style clause list means a form whose answer this would not preserve.
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return;
    }
    let [FunctionArg::Unnamed(arg)] = list.args.as_mut_slice() else {
        return;
    };
    let counted = match arg {
        FunctionArgExpr::Expr(expr) => expr.clone(),
        // `count(*)` counts rows rather than values, so any non-null stands in for one.
        FunctionArgExpr::Wildcard => Expr::value(Value::Number("1".to_string(), false)),
        // `count(t.*)` and Snowflake's `* EXCLUDE (…)`: neither is a value to guard.
        _ => return,
    };
    *arg = FunctionArgExpr::Expr(Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: *predicate,
            result: counted,
        }],
        // No `ELSE`, so an excluded row is NULL and the aggregate skips it — which is the
        // whole of the rewrite.
        else_result: None,
    });
    func.filter = None;
}

/// Whether a NULL argument is no input at all to this aggregate, which is what makes
/// [`rewrite_window_filter`] exact. PostgreSQL's own spellings, since the rewrite runs
/// before [`postgres_aggregate_alias`].
fn aggregate_ignores_null_arguments(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "bool_or"
            | "every"
            | "bit_and"
            | "bit_or"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
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

/// The bare, unqualified name of a function call, lower-cased.
fn bare_function_name(name: &ObjectName) -> Option<String> {
    if name.0.len() != 1 {
        return None;
    }
    Some(name.0.first()?.as_ident()?.value.to_ascii_lowercase())
}

/// Whether `name` is `json_agg` or `jsonb_agg`.
fn is_json_aggregate(name: &ObjectName) -> bool {
    matches!(
        bare_function_name(name).as_deref(),
        Some(JSON_AGG_NAME | JSONB_AGG_NAME)
    )
}

/// Rewrite `json_agg(…)` into `vaire_json_array(array_agg(…))`.
///
/// Only the function's *name* changes; `DISTINCT`, an in-aggregate `ORDER BY`, `FILTER` and
/// an `OVER` all ride along on the `array_agg` untouched, which is the point of composing
/// rather than implementing an aggregate — see [`vairedb_common::json_agg`].
///
/// The rendering is picked here because this is the last place the difference is visible:
/// `json_agg` of a json *document* embeds it and `json_agg` of text quotes it, and both are
/// `Utf8` by planning time. [`produces_json_document`] is what decides.
fn json_aggregate_call(mut func: Function) -> Expr {
    let documents = json_aggregate_argument(&func).is_some_and(produces_json_document);
    func.name = ObjectName::from(vec![Ident::new("array_agg")]);
    let rendering = match documents {
        true => JSON_ARRAY_DOCS_UDF_NAME,
        false => JSON_ARRAY_UDF_NAME,
    };
    call(rendering, vec![Expr::Function(func)])
}

/// The single aggregated expression, if the call has exactly one positional argument.
fn json_aggregate_argument(func: &Function) -> Option<&Expr> {
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    match list.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(only))] => Some(only),
        _ => None,
    }
}

/// Whether `expr` produces a json document rather than a value to be quoted as one.
///
/// Read off the expression, because the value cannot say: both are `Utf8`. By the time this
/// runs the expression has already been rewritten — the visit is post-order — so the json
/// forms are the calls the arms above emitted, not the casts and operators a client wrote.
///
/// A bare column *declared* `JSON` or `JSONB` is not recognized, and is quoted: at this
/// point it is an identifier with no type attached. `json_agg(payload::jsonb)` is the
/// spelling that embeds; the residue is recorded in `docs/specs/gap-analysis.md`.
fn produces_json_document(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => produces_json_document(inner),
        Expr::Function(func) => matches!(
            bare_function_name(&func.name).as_deref(),
            // The `_text` accessors are deliberately absent: `->>` and `#>>` return text,
            // which PostgreSQL quotes here just like any other string.
            Some(JSON_IN_UDF_NAME | JSONB_IN_UDF_NAME | JSON_GET_UDF_NAME | JSON_PATH_UDF_NAME)
        ),
        _ => false,
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

/// Refuse an ordered-set aggregate called without the clause that makes it one, the way
/// PostgreSQL refuses it.
///
/// `mode()` and `rank(5)` are not functions on their own: the `WITHIN GROUP (ORDER BY …)` is
/// where their input comes from, so without it there is nothing to aggregate. PostgreSQL says
/// `WITHIN GROUP is required for ordered-set aggregate mode` with `ERRCODE_WRONG_OBJECT_TYPE`,
/// and this returns the same sentence and the same `42809`. Left to the planner it would be a
/// signature-resolution error instead — `'mode' does not support zero arguments … Candidate
/// functions: mode(Any)` — which advertises an internal arity the client cannot use, since
/// PostgreSQL's `mode` takes no direct argument at all.
///
/// The four hypothetical-set names are only checked when the call *has* arguments. `rank()`
/// with neither clause is a window function missing its `OVER`, which is a different statement
/// and a different PostgreSQL message; `rank(5)` can only have meant the aggregate.
fn reject_ordered_set_without_within_group(func: &Function) -> PgWireResult<()> {
    if !func.within_group.is_empty() || func.over.is_some() {
        return Ok(());
    }
    let Some(name) = bare_function_name(&func.name) else {
        return Ok(());
    };
    let lowered = name.to_lowercase();
    let has_arguments = match &func.args {
        FunctionArguments::List(list) => !list.args.is_empty(),
        _ => false,
    };
    let is_ordered_set = matches!(
        lowered.as_str(),
        "mode" | "percentile_cont" | "percentile_disc"
    ) || (has_arguments
        && vairedb_common::within_group::hypothetical_set_udaf(&lowered).is_some());
    if !is_ordered_set {
        return Ok(());
    }
    Err(make_vdb_error(
        VdbErrorCode::WrongObjectType,
        format!("WITHIN GROUP is required for ordered-set aggregate {lowered}"),
    ))
}

/// Rename `rank(h) WITHIN GROUP (ORDER BY x)` — and its three siblings — to the UDAF that
/// answers it, leaving `rank() OVER (…)` alone.
///
/// The two are different functions that PostgreSQL spells with one name, and DataFusion's
/// planner resolves `OVER` by looking in the *aggregate* registry first: an aggregate called
/// `rank` would take the window function away from every query that uses it. So the aggregate
/// is registered under a name of VaireDB's own and this is where the client's spelling reaches
/// it — see [`vairedb_common::within_group`], which records the measurement.
///
/// The conditions are the whole of the distinction. A `WITHIN GROUP` clause is what makes the
/// call the aggregate; an `OVER` clause is what makes it the window function, and a call
/// carrying both is a statement PostgreSQL rejects, so it is left as it is for the planner to
/// fault rather than quietly turned into one of the two.
fn rename_hypothetical_set_aggregate(func: &mut Function) {
    if func.within_group.is_empty() || func.over.is_some() {
        return;
    }
    let Some(name) = bare_function_name(&func.name) else {
        return;
    };
    if let Some(udaf) = vairedb_common::within_group::hypothetical_set_udaf(&name) {
        func.name = ObjectName::from(vec![Ident::new(udaf)]);
    }
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

    // The hypothetical-set aggregates reach their UDAF by being renamed, because the name
    // the client writes belongs to a window function in DataFusion's planner. `WITHIN GROUP`
    // and no `OVER` is the whole of the condition — see `rename_hypothetical_set_aggregate`.
    #[test]
    fn renames_a_hypothetical_set_aggregate_to_its_udaf() {
        for (name, udaf) in [
            ("rank", "vaire_hypothetical_rank"),
            ("dense_rank", "vaire_hypothetical_dense_rank"),
            ("percent_rank", "vaire_hypothetical_percent_rank"),
            ("cume_dist", "vaire_hypothetical_cume_dist"),
        ] {
            let sql = format!("SELECT {name}(5) WITHIN GROUP (ORDER BY n) FROM t");
            assert_eq!(
                rewritten(&sql).unwrap(),
                format!("SELECT {udaf}(5) WITHIN GROUP (ORDER BY n) FROM t")
            );
        }
        // The client's own casing is not the name it is matched against.
        assert_eq!(
            rewritten("SELECT DENSE_RANK(5) WITHIN GROUP (ORDER BY n) FROM t").unwrap(),
            "SELECT vaire_hypothetical_dense_rank(5) WITHIN GROUP (ORDER BY n) FROM t"
        );
    }

    // The regression the rename exists to prevent: a window function of the same name must
    // still be the window function, whatever else it carries.
    #[test]
    fn leaves_the_window_functions_of_those_names_alone() {
        for sql in [
            "SELECT rank() OVER (ORDER BY n) FROM t",
            "SELECT dense_rank() OVER (PARTITION BY g ORDER BY n) FROM t",
            "SELECT percent_rank() OVER (ORDER BY n) FROM t",
            "SELECT cume_dist() OVER (ORDER BY n) FROM t",
            // Neither clause, so neither aggregate: this is a window function that has lost
            // its OVER, and the planner's own message about that is the honest one.
            "SELECT rank() FROM t",
        ] {
            assert_eq!(rewritten(sql).unwrap(), sql, "`{sql}` must be untouched");
        }
    }

    // `mode()` keeps its name: no window function has it, so nothing has to be protected
    // from the aggregate.
    #[test]
    fn leaves_mode_under_its_own_name() {
        let sql = "SELECT mode() WITHIN GROUP (ORDER BY n) FROM t";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    // An ordered-set aggregate without the clause that gives it its input is PostgreSQL's own
    // error, not a signature-resolution one — and the planner's version advertises an internal
    // arity (`mode(Any)`) that PostgreSQL's `mode` does not have.
    #[test]
    fn refuses_an_ordered_set_aggregate_without_within_group() {
        for sql in [
            "SELECT mode() FROM t",
            "SELECT percentile_cont(0.5) FROM t",
            "SELECT percentile_disc(0.5) FROM t",
            "SELECT rank(5) FROM t",
            "SELECT cume_dist(5) FROM t",
        ] {
            let err = rewritten(sql).expect_err("the clause is what makes it an aggregate");
            assert!(
                err.contains("WITHIN GROUP is required for ordered-set aggregate"),
                "{err}"
            );
        }

        // The neighbours. With the clause, and as window functions, and `rank()` with neither
        // clause — which is a window function missing its OVER, a different statement.
        for sql in [
            "SELECT mode() WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT rank(5) WITHIN GROUP (ORDER BY n) FROM t",
            "SELECT rank() OVER (ORDER BY n) FROM t",
            "SELECT cume_dist() OVER (ORDER BY n) FROM t",
            "SELECT rank() FROM t",
            "SELECT sum(n) FROM t",
        ] {
            rewritten(sql).unwrap_or_else(|e| panic!("`{sql}` must not be refused: {e}"));
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

    // `FILTER` beside `OVER` is dropped, so the predicate moves into the argument where the
    // aggregate skipping nulls makes that exact. On a plain aggregate the filter is applied
    // correctly, so it must come through untouched.
    #[test]
    fn folds_a_windowed_filter_into_the_argument() {
        assert_eq!(
            rewritten("SELECT sum(x) FILTER (WHERE x > 10) OVER (PARTITION BY g) FROM t").unwrap(),
            "SELECT sum(CASE WHEN x > 10 THEN x END) OVER (PARTITION BY g) FROM t"
        );
        // `count(*)` counts rows, so any non-null argument stands in for one.
        assert_eq!(
            rewritten("SELECT count(*) FILTER (WHERE x > 10) OVER (ORDER BY x) FROM t").unwrap(),
            "SELECT count(CASE WHEN x > 10 THEN 1 END) OVER (ORDER BY x) FROM t"
        );

        let sql = "SELECT sum(x) FILTER (WHERE x > 10) FROM t GROUP BY g";
        assert_eq!(
            rewritten(sql).unwrap(),
            sql,
            "an unwindowed FILTER is applied correctly, so rewriting it would be work \
             for nothing"
        );
    }

    // The aggregates a NULL argument is an *input* to keep the refusal: folding the
    // predicate in would add one null element per excluded row.
    #[test]
    fn refuses_a_windowed_filter_on_an_aggregate_that_counts_nulls() {
        for sql in [
            "SELECT array_agg(x) FILTER (WHERE x > 10) OVER (ORDER BY x) FROM t",
            "SELECT string_agg(x, ',') FILTER (WHERE x > 10) OVER (ORDER BY x) FROM t",
            // `DISTINCT` is not a windowed form DataFusion evaluates, so folding into it
            // would only move where the failure comes from.
            "SELECT count(DISTINCT x) FILTER (WHERE x > 10) OVER (ORDER BY x) FROM t",
        ] {
            let err = rewritten(sql).expect_err("the filter would be dropped");
            assert!(err.contains("FILTER"), "{err}");
        }
    }

    // The named-window expansion is wired into this pass, which is what the rest of its
    // rules are tested against in [`super::super::pg_named_windows`].
    #[test]
    fn expands_a_window_specification_that_names_a_window() {
        assert_eq!(
            rewritten("SELECT sum(x) OVER (w ORDER BY x) FROM t WINDOW w AS (PARTITION BY g)")
                .unwrap(),
            "SELECT sum(x) OVER (PARTITION BY g ORDER BY x) FROM t WINDOW w AS (PARTITION BY g)"
        );
        assert_eq!(
            rewritten(
                "SELECT s FROM (SELECT sum(x) AS s FROM t \
                 WINDOW w1 AS (PARTITION BY g), w2 AS (w1 ORDER BY x)) d"
            )
            .unwrap(),
            "SELECT s FROM (SELECT sum(x) AS s FROM t \
             WINDOW w1 AS (PARTITION BY g), w2 AS (PARTITION BY g ORDER BY x)) d"
        );
        // And PostgreSQL's own refusal still arrives through this pass.
        let err = rewritten(
            "SELECT sum(x) OVER (w ORDER BY x) FROM t WINDOW w AS (PARTITION BY g ORDER BY g)",
        )
        .expect_err("PostgreSQL refuses overriding an inherited ORDER BY");
        assert!(err.contains("cannot override ORDER BY"), "{err}");

        // `OVER w` names no window inside the parentheses, so there is nothing to expand.
        let sql = "SELECT sum(x) OVER w FROM t WINDOW w AS (PARTITION BY g ORDER BY x)";
        assert_eq!(rewritten(sql).unwrap(), sql);
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

    // `INTERSECT ALL` and `EXCEPT ALL` count duplicates wrongly rather than not at all, so
    // they are marked here for the plan-level rewrite that repairs them rather than refused.
    // What this asserts is only that the marker is applied, and everywhere a set operation can
    // be written: whether the marked plan answers PostgreSQL's row counts is
    // `pg_set_op_multiplicity`'s question.
    #[test]
    fn marks_the_set_operations_that_count_duplicates() {
        for sql in [
            "SELECT k FROM l INTERSECT ALL SELECT k FROM r",
            "SELECT k FROM l EXCEPT ALL SELECT k FROM r",
            "SELECT k FROM l MINUS ALL SELECT k FROM r",
            // A set operation nested in a subquery or a CTE is reached too: the visitor sees
            // every `Query`, not only the outermost.
            "SELECT count(*) FROM (SELECT k FROM l EXCEPT ALL SELECT k FROM r) d",
            "WITH c AS (SELECT k FROM l INTERSECT ALL SELECT k FROM r) SELECT * FROM c",
        ] {
            let marked = rewritten(sql).expect("marked, not refused");
            assert!(
                marked.contains("__vaire_set_op_all"),
                "`{sql}` should be marked, got: {marked}"
            );
        }
    }

    // The set operations that need no marker: the two `DISTINCT` forms, which compare the rows
    // as sets, and `UNION ALL`, whose `ALL` only asks for concatenation.
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

    // The quantified comparisons over a subquery are no longer this module's business: they
    // are lowered on the plan, where the subquery's arity and the position it sits in can be
    // read, by `pg_set_op_multiplicity`'s neighbour `pg_quantified_subqueries`. So they pass
    // through here as written.
    #[test]
    fn leaves_the_quantified_subquery_forms_to_the_plan_pass() {
        for (sql, rendered) in [
            (
                "SELECT id FROM l WHERE id > ALL (SELECT id FROM r)",
                "SELECT id FROM l WHERE id > ALL(SELECT id FROM r)",
            ),
            (
                "SELECT id FROM l WHERE id < SOME (SELECT id FROM r)",
                "SELECT id FROM l WHERE id < SOME(SELECT id FROM r)",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered);
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

    // The three text-backed types, each becoming its own input conversion. `bytea` is here
    // beside them because they all leave through the same arm and the arm has to tell them
    // apart — a mix-up would validate a UUID as json and say so in the wrong SQLSTATE.
    #[test]
    fn rewrites_the_casts_arrow_has_no_conversion_for() {
        for (sql, rendered) in [
            ("SELECT x::json FROM t", "SELECT vaire_json_in(x) FROM t"),
            ("SELECT x::jsonb FROM t", "SELECT vaire_jsonb_in(x) FROM t"),
            ("SELECT x::uuid FROM t", "SELECT vaire_uuid_in(x) FROM t"),
            ("SELECT x::bytea FROM t", "SELECT vaire_bytea_in(x) FROM t"),
            (
                "SELECT CAST(x AS JSON) FROM t",
                "SELECT vaire_json_in(x) FROM t",
            ),
            (
                "SELECT CAST(x AS UUID) FROM t",
                "SELECT vaire_uuid_in(x) FROM t",
            ),
            // Reached wherever an expression is, and nested inside another rewrite.
            (
                "SELECT 1 FROM t WHERE x::uuid = y::uuid",
                "SELECT 1 FROM t WHERE vaire_uuid_in(x) = vaire_uuid_in(y)",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered, "`{sql}`");
        }
    }

    // A `TRY_CAST` is left alone, exactly as it is for `bytea`: it asks for NULL instead of an
    // error, which is not what these conversions do, so respelling it would change what the
    // client asked for rather than only how it is spelled.
    #[test]
    fn leaves_a_try_cast_to_a_text_backed_type_alone() {
        for sql in [
            "SELECT TRY_CAST(x AS JSON) FROM t",
            "SELECT TRY_CAST(x AS UUID) FROM t",
        ] {
            assert_eq!(rewritten(sql).unwrap(), sql);
        }
    }

    // The operator family the cast was blocking. The doubled forms are separate functions
    // rather than a wrapper, because `->>` returns the extracted value as text where `->`
    // returns it as json and the two differ for a JSON string.
    #[test]
    fn rewrites_the_json_accessors_to_calls() {
        for (sql, rendered) in [
            (
                "SELECT d -> 'k' FROM t",
                "SELECT vaire_json_get(d, 'k') FROM t",
            ),
            (
                "SELECT d ->> 'k' FROM t",
                "SELECT vaire_json_get_text(d, 'k') FROM t",
            ),
            ("SELECT d -> 0 FROM t", "SELECT vaire_json_get(d, 0) FROM t"),
            (
                "SELECT d #> '{a,b}' FROM t",
                "SELECT vaire_json_path(d, '{a,b}') FROM t",
            ),
            (
                "SELECT d #>> ARRAY['a', 'b'] FROM t",
                "SELECT vaire_json_path_text(d, ARRAY['a', 'b']) FROM t",
            ),
            // Chained, which is the usual way to reach into a nested document. Post-order
            // visiting is what makes the inner accessor the outer one's argument.
            (
                "SELECT d -> 'a' ->> 'b' FROM t",
                "SELECT vaire_json_get_text(vaire_json_get(d, 'a'), 'b') FROM t",
            ),
            // In a predicate, over a cast — both rewrites meeting on one expression.
            (
                "SELECT 1 FROM t WHERE (x::jsonb) ->> 'k' = 'v'",
                "SELECT 1 FROM t WHERE vaire_json_get_text((vaire_jsonb_in(x)), 'k') = 'v'",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered, "`{sql}`");
        }
    }

    // `@?` is the one member of the family that is refused rather than rewritten: it takes a
    // jsonpath, which is a language VaireDB has no parser for. Refusing by name is the honest
    // shape — the alternative is an unsupported-operator failure that says nothing about why.
    #[test]
    fn refuses_the_jsonpath_operator() {
        let err = rewritten("SELECT 1 FROM t WHERE d @? '$.a'").unwrap_err();
        assert!(err.contains("@?"), "the operator must be named: {err}");
        assert!(err.contains("jsonpath"), "the reason must be named: {err}");
        assert!(err.contains("->>"), "the alternative must be named: {err}");
    }

    // `json_agg` composes over `array_agg` rather than being its own aggregate, so every
    // modifier has to survive the rename — that is the whole reason for composing.
    #[test]
    fn composes_the_json_aggregates_over_array_agg() {
        for (sql, rendered) in [
            (
                "SELECT json_agg(v) FROM t",
                "SELECT vaire_json_array(array_agg(v)) FROM t",
            ),
            // Both spellings render identically here; see `vairedb_common::json_agg`.
            (
                "SELECT jsonb_agg(v) FROM t",
                "SELECT vaire_json_array(array_agg(v)) FROM t",
            ),
            (
                "SELECT json_agg(v ORDER BY v DESC) FROM t",
                "SELECT vaire_json_array(array_agg(v ORDER BY v DESC)) FROM t",
            ),
            (
                "SELECT json_agg(DISTINCT v) FROM t",
                "SELECT vaire_json_array(array_agg(DISTINCT v)) FROM t",
            ),
            (
                "SELECT json_agg(v) FILTER (WHERE v > 1) FROM t",
                "SELECT vaire_json_array(array_agg(v) FILTER (WHERE v > 1)) FROM t",
            ),
            // Case is not significant, and a group key beside it is untouched.
            (
                "SELECT k, JSON_AGG(v) FROM t GROUP BY k",
                "SELECT k, vaire_json_array(array_agg(v)) FROM t GROUP BY k",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered, "`{sql}`");
        }
    }

    // The one thing the composition cannot read off the value: whether the aggregated
    // expression is a json *document* to splice or a value to quote. Both are text by
    // planning time, so the decision is taken here from the expression — and the expression
    // has already been rewritten, which is why the check is for the accessor calls.
    #[test]
    fn picks_the_document_rendering_for_a_json_argument() {
        for (sql, rendered) in [
            (
                "SELECT json_agg(v::json) FROM t",
                "SELECT vaire_json_array_docs(array_agg(vaire_json_in(v))) FROM t",
            ),
            (
                "SELECT json_agg(v::jsonb) FROM t",
                "SELECT vaire_json_array_docs(array_agg(vaire_jsonb_in(v))) FROM t",
            ),
            (
                "SELECT json_agg(d -> 'k') FROM t",
                "SELECT vaire_json_array_docs(array_agg(vaire_json_get(d, 'k'))) FROM t",
            ),
            (
                "SELECT json_agg(d #> '{a}') FROM t",
                "SELECT vaire_json_array_docs(array_agg(vaire_json_path(d, '{a}'))) FROM t",
            ),
            (
                "SELECT json_agg((v::json)) FROM t",
                "SELECT vaire_json_array_docs(array_agg((vaire_json_in(v)))) FROM t",
            ),
            // `->>` and `#>>` return text, which PostgreSQL quotes like any other string.
            (
                "SELECT json_agg(d ->> 'k') FROM t",
                "SELECT vaire_json_array(array_agg(vaire_json_get_text(d, 'k'))) FROM t",
            ),
            (
                "SELECT json_agg(d #>> '{a}') FROM t",
                "SELECT vaire_json_array(array_agg(vaire_json_path_text(d, '{a}'))) FROM t",
            ),
            // A bare column declared JSONB is quoted: at this point it is an identifier with
            // no type attached. The documented residue — see `vairedb_common::json_agg`.
            (
                "SELECT json_agg(payload) FROM t",
                "SELECT vaire_json_array(array_agg(payload)) FROM t",
            ),
            // A cast to something else is a value, not a document. It comes back with
            // sqlparser's own upper-cased type name, which is a rendering and not a rewrite.
            (
                "SELECT json_agg(v::text) FROM t",
                "SELECT vaire_json_array(array_agg(v::TEXT)) FROM t",
            ),
        ] {
            assert_eq!(rewritten(sql).unwrap(), rendered, "`{sql}`");
        }
    }

    // The refusals and rewrites above must not reach past what they name. `array_agg` itself
    // is untouched, a function whose name merely contains `json_agg` is not one, and the
    // json operators DataFusion does implement over arrays keep their own meaning.
    #[test]
    fn leaves_the_neighbouring_forms_alone() {
        for sql in [
            "SELECT array_agg(v) FROM t",
            "SELECT my_json_agg(v) FROM t",
            "SELECT other.json_agg(v) FROM t",
            "SELECT string_agg(v, ',') FROM t",
            // Upper-cased because that is how sqlparser renders a type name back.
            "SELECT x::TEXT FROM t",
            "SELECT x::INT FROM t",
        ] {
            assert_eq!(rewritten(sql).unwrap(), sql, "`{sql}`");
        }
    }
}
