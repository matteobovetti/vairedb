//! The three datetime forms whose answer depends on a clock the executors must not read:
//! `statement_timestamp()`, `transaction_timestamp()` and the one-argument `age(x)`.
//!
//! PostgreSQL has six ways to ask what time it is, and they differ in *when* the time is
//! taken, not in what they return:
//!
//! | function | fixed at | VaireDB |
//! |---|---|---|
//! | `now()`, `current_timestamp` | the start of the transaction | DataFusion's `now()` |
//! | `transaction_timestamp()` | the start of the transaction | rewritten to `now()` here |
//! | `statement_timestamp()` | the start of the statement | rewritten to `now()` here |
//! | `clock_timestamp()` | every call | a `Volatile` UDF |
//! | `timeofday()` | every call | a `Volatile` UDF |
//!
//! Only the last two can be UDFs. The others must be resolved **once, on the coordinator**,
//! and the reason is that VaireDB is distributed: a `Stable` UDF is evaluated per record
//! batch on whichever executor holds the shard, and three shards reading three system clocks
//! answer three different times for one query. A client that writes
//! `WHERE created_at < statement_timestamp()` would then get a predicate applied
//! inconsistently across shards, and nothing in the result would say so. DataFusion's `now()`
//! is already handled correctly — the planner folds it to a literal before the plan is
//! serialized — so rewriting into it inherits the property rather than reimplementing it.
//!
//! VaireDB's transactions and statements begin close enough together that
//! `transaction_timestamp()` and `statement_timestamp()` both becoming `now()` is exact for
//! the single-statement case and, for a multi-statement transaction, makes
//! `statement_timestamp()` per-statement and `transaction_timestamp()` per-statement too —
//! the second of which is a divergence, and a deliberate one: the alternative is holding a
//! transaction-start timestamp the read path has no other use for, to make a function answer
//! an *earlier* time than the truthful one. § 2.3 of the gap analysis records it.
//!
//! ## `age(x)`
//!
//! The one-argument form is defined as `age(current_date, x)`, which makes it clock-dependent
//! for the same reason — and worse, dependent on a *date*, so two shards either side of
//! midnight would disagree by a whole day. This rewrite writes the second argument in, and
//! `current_date` is then folded by DataFusion's planner into one literal for the whole
//! statement. The two-argument form is a plain UDF and this pass does not touch it; see
//! [`vairedb_common::pg_datetime`].
//!
//! ## Why the AST and not the plan
//!
//! Because these are *calls*, and the point is to change which function the planner resolves
//! before it resolves one. `statement_timestamp` is not a name DataFusion has, so a plan-level
//! pass would run after the planner had already failed on it, and `age(x)` with one argument
//! would have failed to match the two-argument signature for the same reason. Rewriting the
//! AST means the planner only ever sees names it can resolve.

use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, Statement, visit_expressions_mut,
};

/// DataFusion's transaction-scoped clock, which the planner folds to a literal.
const NOW: &str = "now";
/// DataFusion's statement-scoped date, folded the same way.
const CURRENT_DATE: &str = "current_date";

/// Resolve the clock-dependent datetime forms in `stmt` into ones the planner can fold.
///
/// Rewrites `statement_timestamp()` and `transaction_timestamp()` into `now()`, and the
/// one-argument `age(x)` into `age(current_date, x)`. Every other call is left alone.
pub(super) fn resolve_clock_functions(stmt: &mut Statement) {
    let _: ControlFlow<()> = visit_expressions_mut(stmt, |expr| {
        if let Expr::Function(function) = expr {
            rewrite(function);
        }
        ControlFlow::Continue(())
    });
}

fn rewrite(function: &mut Function) {
    let Some(name) = single_part_name(&function.name) else {
        // A qualified call: `pg_catalog.now()` is the same function, but anything else
        // schema-qualified is a name this pass has no business deciding about.
        return;
    };
    // A `FILTER`, an `OVER` or a `WITHIN GROUP` on one of these is not a form PostgreSQL has
    // either, so leave it to fail as the call the client wrote rather than as the rewritten
    // one — the message then names something they can find in their own SQL.
    if function.filter.is_some() || function.over.is_some() || !function.within_group.is_empty() {
        return;
    }

    match name.as_str() {
        "statement_timestamp" | "transaction_timestamp"
            if arguments(function).is_some_and(|a| a.is_empty()) =>
        {
            function.name = ObjectName::from(vec![Ident::new(NOW)]);
        }
        "age" if arguments(function).is_some_and(|a| a.len() == 1) => {
            let Some(args) = arguments_mut(function) else {
                return;
            };
            // Prepended, not appended: `age(later, earlier)` subtracts the second from the
            // first, so today goes in front and `age(birthday)` stays a positive age.
            args.insert(
                0,
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    name: ObjectName::from(vec![Ident::new(CURRENT_DATE)]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                }))),
            );
        }
        _ => {}
    }
}

/// The function's name when it is written unqualified, folded to lower case the way an
/// unquoted identifier is. `"Age"` quoted is a different function and is not matched.
fn single_part_name(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };
    let ident = part.as_ident()?;
    match ident.quote_style {
        None => Some(ident.value.to_ascii_lowercase()),
        Some(_) => None,
    }
}

/// The call's positional argument list, or `None` when it has a form this pass does not
/// rewrite — named arguments, `*`, or no parenthesis at all.
fn arguments(function: &Function) -> Option<&Vec<FunctionArg>> {
    match &function.args {
        FunctionArguments::List(list) if list.duplicate_treatment.is_none() => Some(&list.args),
        _ => None,
    }
}

fn arguments_mut(function: &mut Function) -> Option<&mut Vec<FunctionArg>> {
    match &mut function.args {
        FunctionArguments::List(list) if list.duplicate_treatment.is_none() => Some(&mut list.args),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    fn rewritten(sql: &str) -> String {
        let mut stmt = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .expect("parsed")
            .remove(0);
        resolve_clock_functions(&mut stmt);
        stmt.to_string()
    }

    /// Both transaction-scoped spellings become the one function DataFusion folds.
    #[test]
    fn the_two_statement_clocks_become_now() {
        assert_eq!(
            rewritten("SELECT statement_timestamp()"),
            "SELECT now()".to_string()
        );
        assert_eq!(
            rewritten("SELECT transaction_timestamp()"),
            "SELECT now()".to_string()
        );
    }

    /// Case-insensitively, because an unquoted identifier is folded.
    #[test]
    fn the_clocks_are_matched_however_they_are_cased() {
        assert_eq!(
            rewritten("SELECT STATEMENT_TIMESTAMP()"),
            "SELECT now()".to_string()
        );
    }

    /// `age(x)` gains the argument PostgreSQL's definition supplies, in front — so an age is
    /// positive, which is the whole reason anyone writes the one-argument form.
    #[test]
    fn the_one_argument_age_gains_todays_date_in_front() {
        assert_eq!(
            rewritten("SELECT age(birthday) FROM people"),
            "SELECT age(current_date(), birthday) FROM people".to_string()
        );
    }

    /// The two-argument form is a UDF and is left exactly as written; rewriting it again
    /// would silently change which instant the answer is measured from.
    #[test]
    fn the_two_argument_age_is_left_alone() {
        assert_eq!(
            rewritten("SELECT age(a, b) FROM t"),
            "SELECT age(a, b) FROM t".to_string()
        );
    }

    /// Nested calls are reached, which is where a plan-level pass would have been enough and
    /// an expression-level one that only looked at the select list would not.
    #[test]
    fn a_call_nested_anywhere_in_the_statement_is_rewritten() {
        assert_eq!(
            rewritten(
                "SELECT x FROM t WHERE created_at < statement_timestamp() \
                 AND age(birthday) > INTERVAL '18 years'"
            ),
            "SELECT x FROM t WHERE created_at < now() \
             AND age(current_date(), birthday) > INTERVAL '18 years'"
                .to_string()
        );
    }

    /// Quoting makes it a different identifier, and PostgreSQL has no `"Age"`. Left alone so
    /// the planner refuses the name the client wrote.
    #[test]
    fn a_quoted_name_is_a_different_function() {
        assert_eq!(
            rewritten("SELECT \"age\"(birthday) FROM t"),
            "SELECT \"age\"(birthday) FROM t".to_string()
        );
    }

    /// An unrelated `age`-like call and a zero-argument `age()` are both untouched: this pass
    /// only supplies the argument PostgreSQL's own definition supplies, and invents nothing.
    #[test]
    fn nothing_else_is_touched() {
        assert_eq!(rewritten("SELECT age()"), "SELECT age()".to_string());
        assert_eq!(
            rewritten("SELECT average(x) FROM t"),
            "SELECT average(x) FROM t".to_string()
        );
        assert_eq!(
            rewritten("SELECT now(), clock_timestamp(), timeofday()"),
            "SELECT now(), clock_timestamp(), timeofday()".to_string()
        );
    }

    /// A window or a `FILTER` on one of these is not a PostgreSQL form either, and the
    /// refusal should name what the client wrote rather than the rewrite's version of it.
    #[test]
    fn a_windowed_call_is_left_for_the_planner_to_refuse() {
        let sql = "SELECT statement_timestamp() OVER () FROM t";
        assert_eq!(rewritten(sql), sql.to_string());
    }
}
