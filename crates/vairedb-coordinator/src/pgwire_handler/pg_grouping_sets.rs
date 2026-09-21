//! Remove the two spellings of "group by nothing" — `GROUPING SETS (())` on its own and
//! `GROUP BY ()` — so a statement asking for a grand total gets one.
//!
//! ```text
//! w(n int4, g text), 7 rows
//!
//! SELECT count(*) FROM w GROUP BY GROUPING SETS (())
//! PostgreSQL  7                   -- one grand-total row
//! VaireDB     (no rows)           -- before this rewrite
//!
//! SELECT count(*) FROM w GROUP BY ()
//! PostgreSQL  7
//! VaireDB     0A000 Empty tuple not supported yet
//! ```
//!
//! The empty grouping set is the standard's way of writing "one group, containing every
//! row" — the same aggregation a statement with no `GROUP BY` at all asks for. It is not an
//! exotic spelling: a `GROUPING SETS` list is what a reporting tool generates, and the empty
//! set is the subtotal line at the bottom, so a tool that emits `GROUPING SETS ((region),
//! ())` and a tool that emits the two separately produce the same report in PostgreSQL and
//! did not here.
//!
//! The first form is the worse of the two, and it is the reason this is a rewrite rather than
//! a refusal: **no rows** is a shape a client cannot distinguish from a table that happened
//! to be empty. `count(*)` returning nothing where it must return a number is a number the
//! client then reads as absent, and a subtotal line silently missing from a report is not an
//! error anyone sees.
//!
//! ## The rewrite
//!
//! Delete the empty set from the `GROUP BY` list, because deleting it is what it means:
//!
//! | written | becomes | rows |
//! |---|---|---|
//! | `GROUP BY GROUPING SETS (())` | `GROUP BY` nothing | the grand total |
//! | `GROUP BY ()` | `GROUP BY` nothing | the grand total |
//! | `GROUP BY g, GROUPING SETS (())` | `GROUP BY g` | one row per `g` |
//! | `GROUP BY g, ()` | `GROUP BY g` | one row per `g` |
//!
//! Each of the four was measured against PostgreSQL 16.15, including the two combined forms:
//! grouping by `g` and by nothing is grouping by `g`, since the empty set adds no column to
//! group *by* and no row to group — which is what makes the deletion exact rather than
//! approximately right.
//!
//! An aggregate with no grouping columns is also the shape the distributed read path handles
//! best: one partial aggregate per shard and one final merge, with no grouping-set expansion
//! to serialize. So the rewrite removes a construct rather than adding one.
//!
//! ## What is deliberately left alone
//!
//! * **A non-empty grouping set beside an empty one** — `GROUPING SETS ((), (g))`. That
//!   already answers correctly (the grand total row plus one row per `g`), and the empty set
//!   there is not removable: it contributes a row of its own.
//! * **`CUBE (())` and `ROLLUP (())`**, which are syntax errors in PostgreSQL. There is no
//!   PostgreSQL answer to match, so inventing one would be a superset — see § 5 of the gap
//!   analysis.
//!
//! ## What is refused instead
//!
//! `GROUPING SETS ((), ())` — two or more empty sets and nothing else — which PostgreSQL
//! answers with *two* identical grand-total rows, one per set. Deleting them would answer one
//! row, and leaving them answers **none**, because the same distributed shortfall that loses
//! the row above loses both of these. One row where two were asked for is the same class of
//! wrong answer this module exists to remove, and a plain aggregate cannot express a
//! duplicated grand total, so this refuses with `0A000`: PostgreSQL has the form and VaireDB
//! does not.
//!
//! The refusal is narrow, and the measurements are what make it so. A repeated empty set
//! *beside* a grouping column answers correctly already — `GROUPING SETS ((), (), (g))` gives
//! five rows and `GROUP BY g, GROUPING SETS ((), ())` gives six, both matching PostgreSQL —
//! so only the case with no grouping column left is refused. `GROUP BY (), ()` is different
//! again and *is* rewritten: PostgreSQL answers that with one row, because a repeated empty
//! *expression* adds nothing to the group-by list where a repeated empty *set* adds a
//! grouping.
//!
//! ## Where this runs
//!
//! On the AST in [`super::parser::prepare_select_for_planning`], because `GROUP BY ()` does
//! not survive the planner at all — *"Empty tuple not supported yet"* is raised while the
//! statement is being planned, so there is no plan to rewrite.

use std::ops::ControlFlow;

use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{
    Expr, GroupByExpr, Query, Select, SetExpr, Statement, VisitMut, VisitorMut,
};

/// Delete every empty grouping set from the `GROUP BY` list of every query block in `stmt`.
///
/// A statement with no empty set — which is every statement a client normally sends — is
/// left byte-identical. Fails only on the one shape that cannot be rewritten: a `GROUP BY`
/// whose every remaining item is a repeated empty grouping set.
pub(super) fn remove_empty_grouping_sets(stmt: &mut Statement) -> PgWireResult<()> {
    struct Remover {
        refusal: Option<PgWireError>,
    }

    impl VisitorMut for Remover {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            match remove_in_set_expr(&mut query.body) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => {
                    self.refusal = Some(e);
                    ControlFlow::Break(())
                }
            }
        }
    }

    let mut remover = Remover { refusal: None };
    let _ = stmt.visit(&mut remover);
    match remover.refusal {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Reach the selects directly under `body`.
///
/// A set operation's branches are `SetExpr`s rather than `Query`s, so the visitor above never
/// sees them on their own.
fn remove_in_set_expr(body: &mut SetExpr) -> PgWireResult<()> {
    match body {
        SetExpr::Select(select) => remove_in_select(select),
        SetExpr::SetOperation { left, right, .. } => {
            remove_in_set_expr(left)?;
            remove_in_set_expr(right)
        }
        _ => Ok(()),
    }
}

fn remove_in_select(select: &mut Select) -> PgWireResult<()> {
    let GroupByExpr::Expressions(exprs, _) = &mut select.group_by else {
        // `GROUP BY ALL` has no expression list to prune.
        return Ok(());
    };
    exprs.retain(|expr| !groups_by_nothing(expr));
    // Every item that is left is a repeated empty set, so there is no grouping column to fall
    // back on and no plain aggregate that answers it. See the module doc.
    if !exprs.is_empty() && exprs.iter().all(repeats_the_empty_set) {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "GROUPING SETS with a repeated empty grouping set and no grouping column is not \
             supported. PostgreSQL answers one grand-total row per empty set; write the empty \
             set once, or add the grouping column the other sets use."
                .to_string(),
        ));
    }
    Ok(())
}

/// Whether this item is a `GROUPING SETS` list of two or more empty sets.
///
/// Only reached after the single-empty case has been removed above, so a `GroupingSets` of
/// exactly one empty set is never seen here.
fn repeats_the_empty_set(expr: &Expr) -> bool {
    match expr {
        Expr::GroupingSets(sets) => !sets.is_empty() && sets.iter().all(|set| set.is_empty()),
        _ => false,
    }
}

/// Whether this `GROUP BY` item asks for no grouping at all, and so can be deleted.
///
/// Exactly two shapes, and the arity of the second is the whole of the care taken here: a
/// `GROUPING SETS` list of *one* empty set is a grand total that the absence of the item
/// also produces, while a list of two is two grand totals that the absence does not. See the
/// module doc.
fn groups_by_nothing(expr: &Expr) -> bool {
    match expr {
        // `GROUP BY ()`, and also each `()` of `GROUP BY (), ()` — a repeated empty
        // expression is still the empty expression list.
        Expr::Tuple(items) => items.is_empty(),
        Expr::GroupingSets(sets) => matches!(sets.as_slice(), [set] if set.is_empty()),
        _ => false,
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
        remove_empty_grouping_sets(&mut stmt).unwrap_or_else(|e| panic!("{sql} was refused: {e}"));
        stmt.to_string()
    }

    fn refusal(sql: &str) -> String {
        let mut stmt = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .expect("parsed")
            .remove(0);
        match remove_empty_grouping_sets(&mut stmt) {
            Err(PgWireError::UserError(info)) => info.message.clone(),
            other => panic!("{sql} was not refused: {other:?}"),
        }
    }

    /// The two spellings of "group by nothing", each becoming the statement that already
    /// answers PostgreSQL's grand total.
    #[test]
    fn an_empty_grouping_set_alone_becomes_no_grouping_at_all() {
        assert_eq!(
            rewritten("SELECT count(*) FROM w GROUP BY GROUPING SETS (())"),
            "SELECT count(*) FROM w"
        );
        assert_eq!(
            rewritten("SELECT count(*) FROM w GROUP BY ()"),
            "SELECT count(*) FROM w"
        );
    }

    /// Beside a real grouping column, the empty set adds no column and no row — so deleting
    /// it leaves exactly the grouping PostgreSQL 16.15 answers with.
    #[test]
    fn an_empty_set_beside_a_column_leaves_the_column() {
        assert_eq!(
            rewritten("SELECT g, count(*) FROM w GROUP BY g, GROUPING SETS (())"),
            "SELECT g, count(*) FROM w GROUP BY g"
        );
        assert_eq!(
            rewritten("SELECT g, count(*) FROM w GROUP BY g, ()"),
            "SELECT g, count(*) FROM w GROUP BY g"
        );
    }

    /// A repeated empty *expression* is still the empty list, which PostgreSQL answers with
    /// one row.
    #[test]
    fn a_repeated_empty_tuple_is_still_one_group() {
        assert_eq!(
            rewritten("SELECT count(*) FROM w GROUP BY (), ()"),
            "SELECT count(*) FROM w"
        );
    }

    /// Two empty *sets* are two grand totals in PostgreSQL. Deleting them would answer one row
    /// and leaving them answers none, so this refuses — and says which of the two codes it is,
    /// because `0A000` tells a client to wait for a release and `42883` tells them to edit.
    #[test]
    fn a_repeated_empty_grouping_set_is_refused() {
        let message = refusal("SELECT count(*) FROM w GROUP BY GROUPING SETS ((), ())");
        assert!(
            message.contains("repeated empty grouping set"),
            "got: {message}"
        );
        assert!(
            message.contains("one grand-total row per empty set"),
            "got: {message}"
        );
        // Refused wherever it is written, including the empty set spelled twice over two
        // items — the second of which this pass removes, leaving the first with nothing beside
        // it.
        refusal("SELECT count(*) FROM w GROUP BY GROUPING SETS ((), ()), GROUPING SETS (())");
        refusal("SELECT count(*) FROM w GROUP BY GROUPING SETS ((), ()), GROUPING SETS ((), ())");
        refusal("SELECT c FROM (SELECT count(*) AS c FROM w GROUP BY GROUPING SETS ((), ())) s");
    }

    /// Beside a grouping column, the repeated empty set answers correctly already — five rows
    /// for the first and six for the second, both matching PostgreSQL 16.15 — so the refusal
    /// above stops exactly where the wrong answer stops.
    #[test]
    fn a_repeated_empty_set_beside_a_column_is_allowed() {
        for sql in [
            "SELECT g, count(*) FROM w GROUP BY GROUPING SETS ((), (), (g))",
            "SELECT g, count(*) FROM w GROUP BY g, GROUPING SETS ((), ())",
        ] {
            assert_eq!(rewritten(sql), sql);
        }
    }

    /// An empty set beside a non-empty one contributes a row of its own and already answers
    /// correctly, so it stays.
    #[test]
    fn an_empty_set_beside_a_non_empty_one_stays() {
        let sql = "SELECT g, count(*) FROM w GROUP BY GROUPING SETS ((), (g))";
        assert_eq!(rewritten(sql), sql);
    }

    /// Ordinary grouping, in every shape a client writes it — untouched, and the guard that
    /// says this pass only ever removes an empty set.
    #[test]
    fn ordinary_grouping_is_untouched() {
        for sql in [
            "SELECT count(*) FROM w",
            "SELECT g, count(*) FROM w GROUP BY g",
            "SELECT g, n, count(*) FROM w GROUP BY g, n",
            "SELECT g, count(*) FROM w GROUP BY GROUPING SETS ((g), (n))",
            "SELECT g, count(*) FROM w GROUP BY CUBE (g, n)",
            "SELECT g, count(*) FROM w GROUP BY ROLLUP (g, n)",
            "SELECT g, count(*) FROM w GROUP BY (g, n)",
        ] {
            assert_eq!(rewritten(sql), sql);
        }
    }

    /// Every query block, so a subquery, a CTE and a set-operation branch are each pruned —
    /// a missing subtotal is as invisible inside a subquery as at the top level.
    #[test]
    fn every_query_block_is_pruned() {
        assert_eq!(
            rewritten(
                "WITH t AS (SELECT count(*) AS c FROM w GROUP BY GROUPING SETS (())) \
                 SELECT c FROM t UNION ALL SELECT count(*) FROM w GROUP BY ()"
            ),
            "WITH t AS (SELECT count(*) AS c FROM w) \
             SELECT c FROM t UNION ALL SELECT count(*) FROM w"
        );
    }
}
