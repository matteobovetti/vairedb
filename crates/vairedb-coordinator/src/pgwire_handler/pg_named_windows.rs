//! Expand PostgreSQL's named-window inheritance, so `OVER (w …)` and `WINDOW w2 AS (w1 …)`
//! mean what they say instead of losing the window they name.
//!
//! ```text
//! t(id, g, n) = (1,1,10), (2,1,NULL), (3,1,30), (4,2,40), (5,2,50), (6,2,NULL)
//!
//! SELECT sum(n) OVER (w) FROM t WINDOW w AS (PARTITION BY g ORDER BY id)
//!
//! PostgreSQL  10, 10, 40, 40, 90, 90
//! DataFusion  130, 130, 130, 130, 130, 130     -- the whole table, in every row
//! ```
//!
//! sqlparser parses `WindowSpec::window_name` and datafusion-sql never reads it, so a window
//! specification that *names* another window keeps only the clauses written beside the name
//! and silently drops that window's own `PARTITION BY` and `ORDER BY`. The frame widens to
//! the whole result and the answer is a plausible number in every row. Adding a frame to the
//! reference was measured worse than wrong — five consecutive runs on unchanged data returned
//! five different answers — which is why both spellings were refused before this module
//! existed.
//!
//! Refusing was never the right answer, only the safe one: the inheritance is pure syntax.
//! `OVER (w ORDER BY id)` is an abbreviation for a specification the client could have
//! written out, and writing it out is something the AST can do before the planner ever sees
//! the name. So this pass expands it, and what is left to refuse is only what PostgreSQL
//! itself refuses.
//!
//! ## PostgreSQL's merge rules, which are the whole of this module
//!
//! A specification that names a window `w` inherits from it and may add to it, in exactly
//! these ways (SQL:2003 `<window clause>`, and `transformWindowDefinitions` in PostgreSQL):
//!
//! * `w` may **not** have a frame clause. A frame is defined relative to the current row
//!   within `w`'s own ordering; a copy that adds an `ORDER BY` would reinterpret it. PG:
//!   `cannot copy window "w" because it has a frame clause`.
//! * the copy may **not** give its own `PARTITION BY`. Partitioning is the identity of the
//!   window, not a detail of it. PG: `cannot override PARTITION BY clause of window "w"`.
//! * the copy may **not** give its own `ORDER BY` if `w` has one. PG:
//!   `cannot override ORDER BY clause of window "w"`.
//! * what results: `w`'s `PARTITION BY`; `w`'s `ORDER BY` if it has one and the copy's
//!   otherwise; and the copy's frame, which by the first rule is the only frame in play.
//!
//! All three refusals are `42P20`, PostgreSQL's `windowing_error`, with PostgreSQL's own
//! wording — so a client that already handles them against PostgreSQL handles them here.
//!
//! ## Two places, one rule
//!
//! Inheritance can be written in an `OVER (…)` or in the `WINDOW` list, and the second can
//! chain: `WINDOW w1 AS (PARTITION BY g), w2 AS (w1 ORDER BY id)`. So the list is resolved
//! first, in declaration order — PostgreSQL requires the referenced window to be declared
//! earlier, and resolving in order means each definition is already expanded by the time
//! anything refers to it — and the `OVER` clauses are then resolved against the expanded
//! list. Both end up as a specification naming nothing, which is the shape DataFusion reads
//! correctly.
//!
//! ## Scope, and why the scopes are a stack
//!
//! A window name belongs to the one query level that declares it: PostgreSQL does not make
//! an outer query's `WINDOW w` visible inside a subquery, and a subquery may declare its own
//! `w` meaning something else. So the visitor keeps a stack, pushing each query's own
//! definitions on the way in and popping them on the way out, and resolves a name against the
//! **innermost** frame only. A name that frame does not hold is refused as `42704`,
//! PostgreSQL's `undefined_object`, rather than left for a planner that would read it as no
//! window at all.
//!
//! ## What is left refused
//!
//! `WINDOW w2 AS w1` without parentheses, which is BigQuery's syntax rather than
//! PostgreSQL's — PostgreSQL requires the parentheses, and DataFusion already rejects the
//! bare form by name. And [`super::pg_operators::reject_discarded_window_clauses`] keeps its
//! own check on a surviving `window_name` as a backstop: this pass is what makes that check
//! unreachable, and if some position it does not reach ever appears, a refusal is the outcome
//! to have rather than a silently widened frame.

use std::ops::ControlFlow;

use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{
    Expr, Ident, NamedWindowDefinition, NamedWindowExpr, Query, SetExpr, Statement, VisitMut,
    VisitorMut, WindowSpec, WindowType,
};

/// Expand every named-window reference in `stmt` into the specification it abbreviates.
///
/// Rewrites the `WINDOW` list and the `OVER (…)` clauses of every query level in place, and
/// returns `42P20` or `42704` for the references PostgreSQL itself refuses. See the module
/// doc for the rules.
pub(super) fn expand_named_windows(stmt: &mut Statement) -> PgWireResult<()> {
    let mut expander = Expander { scopes: Vec::new() };
    match stmt.visit(&mut expander) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// One query level's window definitions, innermost last.
struct Expander {
    scopes: Vec<Vec<(Ident, WindowSpec)>>,
}

impl VisitorMut for Expander {
    type Break = PgWireError;

    /// On the way in, so the level's own definitions are resolved and in scope before any of
    /// its expressions are visited.
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<PgWireError> {
        // Only a plain select body carries a `WINDOW` clause. A set operation's branches are
        // queries of their own and the visitor reaches each on its own terms, which is also
        // what keeps their window names from leaking into each other.
        let scope = match &mut *query.body {
            SetExpr::Select(select) => match resolve_definitions(&mut select.named_window) {
                Ok(scope) => scope,
                Err(e) => return ControlFlow::Break(e),
            },
            _ => Vec::new(),
        };
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<PgWireError> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    /// On the way in rather than out: the specification this substitutes comes from the same
    /// query level, so visiting it afterwards costs nothing and reaching an inner function
    /// call first would gain nothing.
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<PgWireError> {
        let Expr::Function(func) = expr else {
            return ControlFlow::Continue(());
        };
        let Some(WindowType::WindowSpec(spec)) = &mut func.over else {
            return ControlFlow::Continue(());
        };
        let Some(referenced) = spec.window_name.clone() else {
            return ControlFlow::Continue(());
        };
        // Outside any query there is no `OVER` to expand, so nothing to resolve against.
        let Some(scope) = self.scopes.last() else {
            return ControlFlow::Continue(());
        };
        match lookup(scope, &referenced) {
            None => ControlFlow::Break(undefined_window(&referenced)),
            Some(base) => match merge(&base, spec, &referenced, Reference::OverClause) {
                Ok(merged) => {
                    *spec = merged;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            },
        }
    }
}

/// Expand each definition in a `WINDOW` list against the ones declared before it, and return
/// the resolved list as the scope its query's `OVER` clauses are read against.
///
/// In declaration order, and only against earlier definitions: that is PostgreSQL's own rule
/// — a window may not refer to one declared after it — and it is what makes one pass enough,
/// since every definition a later one can name has already been expanded.
fn resolve_definitions(
    windows: &mut [NamedWindowDefinition],
) -> PgWireResult<Vec<(Ident, WindowSpec)>> {
    let mut resolved: Vec<(Ident, WindowSpec)> = Vec::with_capacity(windows.len());
    for NamedWindowDefinition(defined, expr) in windows.iter_mut() {
        let NamedWindowExpr::WindowSpec(spec) = expr else {
            // `WINDOW w2 AS w1`, BigQuery's parenthesis-less spelling. Not PostgreSQL's, and
            // DataFusion rejects it by name, so it is left exactly as written.
            continue;
        };
        if let Some(referenced) = spec.window_name.clone() {
            let Some(base) = lookup(&resolved, &referenced) else {
                return Err(undefined_window(&referenced));
            };
            *spec = merge(
                &base,
                spec,
                &referenced,
                Reference::WindowList(defined.clone()),
            )?;
        }
        resolved.push((defined.clone(), spec.clone()));
    }
    Ok(resolved)
}

/// The definition `name` refers to, compared the way PostgreSQL compares identifiers: an
/// unquoted one is folded to lower case and a quoted one is taken as written, so unquoted
/// `W` and quoted `"w"` are the same window and quoted `"W"` is not.
fn lookup(scope: &[(Ident, WindowSpec)], name: &Ident) -> Option<WindowSpec> {
    let wanted = folded(name);
    scope
        .iter()
        .find(|(defined, _)| folded(defined) == wanted)
        .map(|(_, spec)| spec.clone())
}

fn folded(name: &Ident) -> String {
    match name.quote_style {
        Some(_) => name.value.clone(),
        None => name.value.to_lowercase(),
    }
}

/// Which of the two places the inheritance was written, which is all the difference between
/// the two refusal messages.
enum Reference {
    /// `OVER (w …)`, where omitting the parentheses is a complete workaround — so the hint
    /// PostgreSQL gives is worth giving.
    OverClause,
    /// `WINDOW w2 AS (w1 …)`, where the workaround is to write `w1`'s clauses out.
    WindowList(Ident),
}

/// The specification `own` abbreviates, given the `base` it names.
///
/// The three refusals are PostgreSQL's, worded as PostgreSQL words them. Each is a case where
/// the abbreviation would have to mean something the client cannot have meant: see the module
/// doc for why a frame cannot be copied and why the identity of a window cannot be overridden.
fn merge(
    base: &WindowSpec,
    own: &WindowSpec,
    referenced: &Ident,
    site: Reference,
) -> PgWireResult<WindowSpec> {
    let name = &referenced.value;
    if base.window_frame.is_some() {
        let workaround = match &site {
            Reference::OverClause => {
                String::from("Omit the parentheses in this OVER clause, as OVER <name>")
            }
            Reference::WindowList(defined) => {
                format!("Write the clauses out in the definition of {defined}")
            }
        };
        return Err(windowing_error(format!(
            "cannot copy window \"{name}\" because it has a frame clause. {workaround}"
        )));
    }
    if !own.partition_by.is_empty() {
        return Err(windowing_error(format!(
            "cannot override PARTITION BY clause of window \"{name}\""
        )));
    }
    if !own.order_by.is_empty() && !base.order_by.is_empty() {
        return Err(windowing_error(format!(
            "cannot override ORDER BY clause of window \"{name}\""
        )));
    }
    Ok(WindowSpec {
        // Nothing is inherited any more, which is the point: what is left is a specification
        // the planner reads whole.
        window_name: None,
        partition_by: base.partition_by.clone(),
        // The referenced window's ordering wins where it has one, and the rule above is what
        // makes that unambiguous: the two cannot both be present.
        order_by: match base.order_by.is_empty() {
            false => base.order_by.clone(),
            true => own.order_by.clone(),
        },
        // The copy's own, and by the first rule the only one there can be.
        window_frame: own.window_frame.clone(),
    })
}

/// `42P20`, PostgreSQL's `windowing_error`.
fn windowing_error(message: String) -> PgWireError {
    make_vdb_error(VdbErrorCode::WindowingError, message)
}

/// `42704`, PostgreSQL's `undefined_object` — and PostgreSQL's own wording for it.
///
/// Refused here rather than left to the planner because the planner does not read the name at
/// all: an unresolvable reference would be taken as a specification with no clauses in it, and
/// answered over the whole result.
fn undefined_window(name: &Ident) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::UndefinedObject,
        format!(
            "window \"{}\" does not exist: a window name is visible only in the query level \
             whose WINDOW clause declares it",
            name.value
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    /// Parse one statement, expand it, and render the result back to SQL.
    fn expanded(sql: &str) -> Result<String, String> {
        let mut stmt: Statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0);
        match expand_named_windows(&mut stmt) {
            Ok(()) => Ok(stmt.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn refusal(sql: &str) -> String {
        expanded(sql).expect_err(&format!("`{sql}` must be refused"))
    }

    /// The gap: `OVER (w)` kept none of `w`, and answered over the whole table.
    #[test]
    fn an_over_clause_naming_a_window_becomes_that_window() {
        assert_eq!(
            expanded("SELECT sum(n) OVER (w) FROM t WINDOW w AS (PARTITION BY g ORDER BY id)")
                .unwrap(),
            "SELECT sum(n) OVER (PARTITION BY g ORDER BY id) FROM t \
             WINDOW w AS (PARTITION BY g ORDER BY id)"
        );
    }

    /// Adding an `ORDER BY` to a window that has none is the abbreviation PostgreSQL allows,
    /// and the partition has to survive it — losing the partition was the measured defect.
    #[test]
    fn an_over_clause_may_add_the_ordering_the_window_lacks() {
        assert_eq!(
            expanded("SELECT sum(n) OVER (w ORDER BY id) FROM t WINDOW w AS (PARTITION BY g)")
                .unwrap(),
            "SELECT sum(n) OVER (PARTITION BY g ORDER BY id) FROM t WINDOW w AS (PARTITION BY g)"
        );
    }

    /// A frame of its own is legal beside an inherited partition and ordering — and is the
    /// form that measured *nondeterministic* before the expansion, which is why it is here.
    #[test]
    fn an_over_clause_may_add_a_frame() {
        assert_eq!(
            expanded(
                "SELECT sum(n) OVER (w ROWS 1 PRECEDING) FROM t \
                 WINDOW w AS (PARTITION BY g ORDER BY id)"
            )
            .unwrap(),
            "SELECT sum(n) OVER (PARTITION BY g ORDER BY id ROWS 1 PRECEDING) FROM t \
             WINDOW w AS (PARTITION BY g ORDER BY id)"
        );
    }

    /// The `WINDOW` list is the other place the inheritance can be written, and it chains.
    #[test]
    fn a_window_list_definition_inherits_too() {
        assert_eq!(
            expanded(
                "SELECT sum(n) OVER w2 FROM t \
                 WINDOW w1 AS (PARTITION BY g ORDER BY id), w2 AS (w1)"
            )
            .unwrap(),
            "SELECT sum(n) OVER w2 FROM t \
             WINDOW w1 AS (PARTITION BY g ORDER BY id), w2 AS (PARTITION BY g ORDER BY id)"
        );
        assert_eq!(
            expanded(
                "SELECT sum(n) OVER w2 FROM t \
                 WINDOW w1 AS (PARTITION BY g), w2 AS (w1 ORDER BY id)"
            )
            .unwrap(),
            "SELECT sum(n) OVER w2 FROM t \
             WINDOW w1 AS (PARTITION BY g), w2 AS (PARTITION BY g ORDER BY id)"
        );
    }

    /// A chain of three, which only works because the list is resolved in declaration order.
    #[test]
    fn a_chain_of_definitions_resolves_in_order() {
        assert_eq!(
            expanded(
                "SELECT sum(n) OVER (w3) FROM t \
                 WINDOW w1 AS (PARTITION BY g), w2 AS (w1), w3 AS (w2 ORDER BY id)"
            )
            .unwrap(),
            "SELECT sum(n) OVER (PARTITION BY g ORDER BY id) FROM t \
             WINDOW w1 AS (PARTITION BY g), w2 AS (PARTITION BY g), \
             w3 AS (PARTITION BY g ORDER BY id)"
        );
    }

    /// PostgreSQL's three refusals, with PostgreSQL's wording and `42P20`.
    #[test]
    fn the_overrides_postgresql_refuses_are_refused() {
        let message = refusal(
            "SELECT sum(n) OVER (w ORDER BY n) FROM t WINDOW w AS (PARTITION BY g ORDER BY id)",
        );
        assert!(
            message.contains("cannot override ORDER BY clause of window \"w\""),
            "{message}"
        );

        let message =
            refusal("SELECT sum(n) OVER (w PARTITION BY id) FROM t WINDOW w AS (PARTITION BY g)");
        assert!(
            message.contains("cannot override PARTITION BY clause of window \"w\""),
            "{message}"
        );

        let message = refusal(
            "SELECT sum(n) OVER (w) FROM t \
             WINDOW w AS (PARTITION BY g ORDER BY id ROWS 1 PRECEDING)",
        );
        assert!(
            message.contains("cannot copy window \"w\" because it has a frame clause"),
            "{message}"
        );
        // And the workaround, which for an `OVER` clause is to drop the parentheses.
        assert!(message.contains("OVER <name>"), "{message}");
    }

    /// The same three, written in the `WINDOW` list instead — where the workaround differs.
    #[test]
    fn the_overrides_are_refused_in_the_window_list_too() {
        let message = refusal(
            "SELECT sum(n) OVER w2 FROM t \
             WINDOW w1 AS (PARTITION BY g ORDER BY id), w2 AS (w1 ORDER BY n)",
        );
        assert!(
            message.contains("cannot override ORDER BY clause of window \"w1\""),
            "{message}"
        );

        let message = refusal(
            "SELECT sum(n) OVER w2 FROM t \
             WINDOW w1 AS (PARTITION BY g ROWS 1 PRECEDING), w2 AS (w1)",
        );
        assert!(message.contains("cannot copy window \"w1\""), "{message}");
        assert!(
            message.contains("definition of w2"),
            "the hint names the definition, not an OVER clause: {message}"
        );
    }

    /// A name no `WINDOW` clause declares is refused, because the planner would read it as no
    /// window at all and aggregate over everything.
    #[test]
    fn an_undefined_window_name_is_refused() {
        let message = refusal("SELECT sum(n) OVER (w) FROM t");
        assert!(message.contains("does not exist"), "{message}");
        let message = refusal("SELECT sum(n) OVER (nope) FROM t WINDOW w AS (PARTITION BY g)");
        assert!(message.contains("\"nope\""), "{message}");
    }

    /// A window name declared after the definition that names it is not visible to it —
    /// PostgreSQL's declaration order, which is also what keeps the resolution a single pass.
    #[test]
    fn a_definition_cannot_name_a_later_one() {
        let message =
            refusal("SELECT sum(n) OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (PARTITION BY g)");
        assert!(message.contains("\"w2\""), "{message}");
    }

    /// A window name belongs to the level that declares it: an inner query cannot see the
    /// outer one's, and its own definition of the same name is the one that applies.
    #[test]
    fn a_window_name_does_not_cross_a_query_level() {
        let message =
            refusal("SELECT (SELECT sum(n) OVER (w) FROM u) FROM t WINDOW w AS (PARTITION BY g)");
        assert!(
            message.contains("does not exist"),
            "an outer window is not visible inside a subquery: {message}"
        );
        // The inner definition shadows, so each level expands to its own.
        assert_eq!(
            expanded(
                "SELECT (SELECT sum(n) OVER (w) FROM u WINDOW w AS (PARTITION BY h)) \
                 FROM t WINDOW w AS (PARTITION BY g)"
            )
            .unwrap(),
            "SELECT (SELECT sum(n) OVER (PARTITION BY h) FROM u WINDOW w AS (PARTITION BY h)) \
             FROM t WINDOW w AS (PARTITION BY g)"
        );
    }

    /// Identifier folding is PostgreSQL's: unquoted names are case-insensitive.
    #[test]
    fn an_unquoted_window_name_is_folded() {
        assert_eq!(
            expanded("SELECT sum(n) OVER (W) FROM t WINDOW w AS (PARTITION BY g)").unwrap(),
            "SELECT sum(n) OVER (PARTITION BY g) FROM t WINDOW w AS (PARTITION BY g)"
        );
    }

    /// Everything that names no window is untouched, so the expansion cannot cost a query
    /// that already worked.
    #[test]
    fn a_window_that_names_nothing_is_untouched() {
        for sql in [
            "SELECT sum(n) OVER () FROM t",
            "SELECT sum(n) OVER (PARTITION BY g ORDER BY id) FROM t",
            "SELECT sum(n) OVER w FROM t WINDOW w AS (PARTITION BY g)",
            "SELECT sum(n) OVER w1, sum(n) OVER w2 FROM t \
             WINDOW w1 AS (PARTITION BY g), w2 AS (PARTITION BY id)",
            "SELECT sum(n) FROM t",
        ] {
            assert_eq!(expanded(sql).unwrap(), sql, "`{sql}` must be untouched");
        }
    }
}
