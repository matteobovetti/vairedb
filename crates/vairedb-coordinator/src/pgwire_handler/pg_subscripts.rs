//! PostgreSQL's array subscripts, whose out-of-range answer is not the one DataFusion
//! and DuckDB give.
//!
//! PostgreSQL has one rule for a subscript that falls outside the array: **the element is
//! NULL and the slice is empty**. Both engines VaireDB runs on have a different rule
//! borrowed from Python — a negative index counts back from the end — and they agree with
//! each other, so the wrong answer is the same on the read path and on the write path, and
//! there is nothing in it a client can detect. Measured on PostgreSQL 17, DataFusion 54.1
//! and DuckDB 1.5.5 over `ARRAY[1,2,3]`:
//!
//! | expression | PostgreSQL | both engines | why it matters |
//! |---|---|---|---|
//! | `a[-1]` | NULL | `3` | reads the last element instead of nothing |
//! | `a[0]` | NULL | NULL | agrees already |
//! | `a[-1:2]` | `{1,2}` | `{}` | drops rows |
//! | `a[-3:-1]` | `{}` | `{1,2,3}` | invents rows |
//!
//! Rows 3 and 4 were found while measuring rows 1 and 2 and are the worse pair: an index
//! that reads the wrong element is one value, whereas a slice that comes back empty or
//! whole changes how many values there are.
//!
//! ## Why the bounds and not the subscript
//!
//! The rewrite replaces the index and the bounds *inside* the brackets and keeps the
//! subscript syntax itself. That is what lets one function serve both paths: the read path
//! hands the AST to DataFusion's planner and the write path renders it back to SQL text
//! for a shard's DuckDB, and `a[greatest(…)]` is a subscript both of them accept. Rewriting
//! to `array_element`/`array_slice` calls instead would have needed two spellings, one per
//! engine, for the same rule.
//!
//! The clamps are chosen so that each engine's own out-of-range handling then produces
//! PostgreSQL's answer, rather than the rule being reimplemented on top of it:
//!
//! * an index `i` becomes `nullif(greatest(i, 0), 0)` — every index at or below zero
//!   becomes NULL, and a NULL subscript is NULL in both engines, which is what PostgreSQL
//!   answers for `a[0]`, `a[-1]` and `a[NULL]` alike. An index past the end already
//!   answers NULL in both engines and is left to do so.
//! * a lower bound `l` becomes `CASE WHEN l < 1 THEN 1 ELSE l END` — PostgreSQL clamps a
//!   low lower bound to the start of the array, so `a[-1:2]` is `a[1:2]`.
//! * an upper bound `u` becomes `CASE WHEN u < 0 THEN 0 ELSE u END` — a negative upper
//!   bound is past the *start* of the array, so the slice is empty; `0` is the upper bound
//!   both engines already read as empty. A `u` of zero or more needs no change: both
//!   engines clamp an upper bound past the end, as PostgreSQL does.
//!
//! `CASE` and not `greatest` for the two bounds, even though `greatest` reads better: PG's
//! `greatest` ignores NULLs, so `greatest(NULL, 1)` is `1` and `a[NULL:2]` would stop being
//! NULL. It does cost evaluating the bound expression twice, which is visible only for a
//! volatile bound — `a[random()::int : 2]` — and is the lesser of the two errors.
//!
//! ## What is deliberately not rewritten
//!
//! * **A missing bound.** `a[2:]` and `a[:2]` are correct in DuckDB and are refused
//!   outright by DataFusion (*"Slice subscript requires an upper bound"*), so there is no
//!   wrong answer to fix and supplying a bound would be adding a feature under cover of a
//!   correctness fix.
//! * **A string index.** `s['name']` is a struct field, not an array element, and clamping
//!   it to a number would break it.
//! * **A stride.** `a[1:6:2]` is DuckDB's, not PostgreSQL's — a superset, left alone under
//!   the same rule as every other one.

use crate::sqlparser::ast::{
    AccessExpr, BinaryOperator, CaseWhen, Expr, Subscript, Value,
    helpers::attached_token::AttachedToken,
};

/// Clamp every array index and slice bound in `access_chain` to PostgreSQL's meaning.
///
/// Shared by both paths and applied to one `Expr::CompoundFieldAccess`'s chain: the read
/// path calls it from [`super::pg_operators::rewrite_pg_expressions`] and the write path
/// from `write_sql_cl::dialect::transform_pg_semantics`. Each statement reaches exactly one
/// of the two; clamping an already-clamped bound would in any case give the same bound,
/// so the shared entry needs no guard against being reached twice.
pub(crate) fn clamp_to_pg_semantics(access_chain: &mut [AccessExpr]) {
    for access in access_chain {
        let AccessExpr::Subscript(subscript) = access else {
            continue;
        };
        match subscript {
            Subscript::Index { index } if !is_field_name(index) => {
                let taken = std::mem::replace(index, null());
                *index = clamp_index(taken);
            }
            // A stride is DuckDB's own extension; leave the whole subscript alone.
            Subscript::Slice {
                stride: Some(_), ..
            } => {}
            Subscript::Slice {
                lower_bound,
                upper_bound,
                ..
            } => {
                if let Some(lower) = lower_bound {
                    let taken = std::mem::replace(lower, null());
                    *lower = clamp_lower_bound(taken);
                }
                if let Some(upper) = upper_bound {
                    let taken = std::mem::replace(upper, null());
                    *upper = clamp_upper_bound(taken);
                }
            }
            Subscript::Index { .. } => {}
        }
    }
}

/// Whether this subscript names a field rather than indexing an array.
///
/// A single-quoted string is the only form that does: `payload['user']` is a struct or map
/// field in both engines and in PostgreSQL's `jsonb`, and a numeric clamp would turn it
/// into a subscript of something that has no elements.
fn is_field_name(index: &Expr) -> bool {
    matches!(
        index,
        Expr::Value(v) if matches!(
            v.value,
            Value::SingleQuotedString(_) | Value::DoubleQuotedString(_) | Value::EscapedStringLiteral(_)
        )
    )
}

/// `nullif(greatest(index, 0), 0)` — every index at or below zero collapses to NULL.
fn clamp_index(index: Expr) -> Expr {
    call(
        "nullif",
        vec![call("greatest", vec![index, int(0)]), int(0)],
    )
}

/// `CASE WHEN lower < 1 THEN 1 ELSE lower END` — a lower bound before the array starts is
/// the array's start.
fn clamp_lower_bound(lower: Expr) -> Expr {
    case(less_than(lower.clone(), 1), int(1), lower)
}

/// `CASE WHEN upper < 0 THEN 0 ELSE upper END` — an upper bound before the array starts
/// selects nothing, and `0` is the bound both engines already read that way.
fn clamp_upper_bound(upper: Expr) -> Expr {
    case(less_than(upper.clone(), 0), int(0), upper)
}

/// `expr < n`.
fn less_than(expr: Expr, n: i64) -> Expr {
    Expr::BinaryOp {
        left: Box::new(expr),
        op: BinaryOperator::Lt,
        right: Box::new(int(n)),
    }
}

/// `CASE WHEN condition THEN result ELSE otherwise END`.
fn case(condition: Expr, result: Expr, otherwise: Expr) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen { condition, result }],
        else_result: Some(Box::new(otherwise)),
    }
}

/// An unsigned integer literal.
fn int(n: i64) -> Expr {
    Expr::value(Value::Number(n.to_string(), false))
}

/// A `NULL` literal, used only as the placeholder a bound is swapped out for while it is
/// being wrapped.
fn null() -> Expr {
    Expr::value(Value::Null)
}

/// A function call `name(args…)`.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    use crate::sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

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
    use crate::sqlparser::ast::{Statement, visit_expressions_mut};
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;
    use std::ops::ControlFlow;

    /// Apply the clamp everywhere in `sql` and render the statement back, which is exactly
    /// what the write path does with it.
    fn clamped(sql: &str) -> String {
        let mut stmt: Statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0);
        let _ = visit_expressions_mut(&mut stmt, |expr| {
            if let Expr::CompoundFieldAccess { access_chain, .. } = expr {
                clamp_to_pg_semantics(access_chain);
            }
            ControlFlow::<()>::Continue(())
        });
        stmt.to_string()
    }

    #[test]
    fn clamps_an_index_to_null_at_or_below_zero() {
        assert_eq!(
            clamped("SELECT a[1] FROM t"),
            "SELECT a[nullif(greatest(1, 0), 0)] FROM t"
        );
        assert_eq!(
            clamped("SELECT a[-1] FROM t"),
            "SELECT a[nullif(greatest(-1, 0), 0)] FROM t"
        );
    }

    // An expression index is the case a rewrite exists for: a literal could have been
    // folded here, a column could not.
    #[test]
    fn clamps_an_index_that_is_an_expression() {
        assert_eq!(
            clamped("SELECT a[i] FROM t"),
            "SELECT a[nullif(greatest(i, 0), 0)] FROM t"
        );
        assert_eq!(
            clamped("SELECT a[$1] FROM t"),
            "SELECT a[nullif(greatest($1, 0), 0)] FROM t"
        );
    }

    #[test]
    fn clamps_both_slice_bounds() {
        assert_eq!(
            clamped("SELECT a[-1:2] FROM t"),
            "SELECT a[CASE WHEN -1 < 1 THEN 1 ELSE -1 END:CASE WHEN 2 < 0 THEN 0 ELSE 2 END] FROM t"
        );
        assert_eq!(
            clamped("SELECT a[i:j] FROM t"),
            "SELECT a[CASE WHEN i < 1 THEN 1 ELSE i END:CASE WHEN j < 0 THEN 0 ELSE j END] FROM t"
        );
    }

    // A missing bound is DuckDB-correct and DataFusion-refused, so it stays missing — and
    // the bound beside it is still clamped.
    #[test]
    fn leaves_a_missing_bound_missing() {
        assert_eq!(
            clamped("SELECT a[2:] FROM t"),
            "SELECT a[CASE WHEN 2 < 1 THEN 1 ELSE 2 END:] FROM t"
        );
        assert_eq!(
            clamped("SELECT a[:2] FROM t"),
            "SELECT a[:CASE WHEN 2 < 0 THEN 0 ELSE 2 END] FROM t"
        );
        assert_eq!(clamped("SELECT a[:] FROM t"), "SELECT a[:] FROM t");
    }

    // The two forms that are not PostgreSQL array subscripts at all.
    #[test]
    fn leaves_a_field_name_and_a_stride_alone() {
        for sql in [
            "SELECT s['name'] FROM t",
            "SELECT a[1:6:2] FROM t",
            "SELECT a[i:j:2] FROM t",
        ] {
            assert_eq!(clamped(sql), sql, "`{sql}` must not be rewritten");
        }
    }

    // Every subscript in a chain, and a nested array's inner subscript too.
    #[test]
    fn clamps_every_subscript_in_a_chain() {
        assert_eq!(
            clamped("SELECT a[1][2] FROM t"),
            "SELECT a[nullif(greatest(1, 0), 0)][nullif(greatest(2, 0), 0)] FROM t"
        );
        assert_eq!(
            clamped("SELECT a[b[1]] FROM t"),
            "SELECT a[nullif(greatest(b[nullif(greatest(1, 0), 0)], 0), 0)] FROM t"
        );
    }

    // Reachable wherever an expression is, not just in a projection.
    #[test]
    fn clamps_a_subscript_in_a_predicate_and_an_update() {
        assert_eq!(
            clamped("SELECT id FROM t WHERE a[-1] IS NULL"),
            "SELECT id FROM t WHERE a[nullif(greatest(-1, 0), 0)] IS NULL"
        );
        assert_eq!(
            clamped("UPDATE t SET v = a[-1]"),
            "UPDATE t SET v = a[nullif(greatest(-1, 0), 0)]"
        );
    }

    // A statement with no subscript in it comes back byte-identical.
    #[test]
    fn leaves_a_statement_without_subscripts_unchanged() {
        for sql in ["SELECT a FROM t", "SELECT a + 1 FROM t WHERE b = 2"] {
            assert_eq!(clamped(sql), sql);
        }
    }
}
