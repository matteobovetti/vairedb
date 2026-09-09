//! Label a result column of a function call the way PostgreSQL labels it.
//!
//! PostgreSQL names an unaliased result column after the function that produced it:
//! `SELECT sum(n)` returns a column called `sum`, and `SELECT row_number() OVER (...)`
//! one called `row_number`. DataFusion instead names it after the whole expression as
//! it renders internally, which is a different string and sometimes a very long one —
//! `row_number() ORDER BY [t.x ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND
//! CURRENT ROW` is 99 bytes, past PostgreSQL's 63-byte `NAMEDATALEN`, and it leaks the
//! planner's internal spelling of a frame the client never wrote.
//!
//! A label is not cosmetic to a client. A driver that reads columns by name, an ORM
//! that maps them onto fields, and `psql`'s own header all take the label as the
//! contract, so the read path adds the alias PostgreSQL would have given.
//!
//! ## Why only some columns
//!
//! PostgreSQL is happy to return two columns with the same name: `SELECT sum(a), sum(b)`
//! is two columns both called `sum`. DataFusion refuses that outright —
//! *"Projections require unique expression names"* — so aliasing both would turn a
//! query that works today into a planning error, which is a worse outcome than a
//! verbose label.
//!
//! So a label is applied only when it is **unique** within its own select list,
//! counting the aliases the client wrote as well. Where it is not unique the item is
//! left exactly as it was: still verbose, still working. That residue is the part of
//! the gap DataFusion owns.

use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, VisitMut, VisitorMut,
};

/// Give every unaliased function-call result column the label PostgreSQL would.
///
/// Applied to a whole statement, so a subquery's and a set operation's own select lists
/// are labelled too — a `UNION` takes its column names from its first branch, and a
/// derived table's names are what the enclosing query refers to.
///
/// Read-path only, and skipped for catalog introspection: those statements come from
/// `datafusion-pg-catalog`'s own rewrites, tuned to the column names particular drivers
/// look for, and this has no business renaming them.
pub(super) fn label_function_columns(stmt: &mut Statement) {
    struct Labeler;

    impl VisitorMut for Labeler {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            label_set_expr(&mut query.body);
            ControlFlow::Continue(())
        }
    }

    let _ = stmt.visit(&mut Labeler);
}

/// Label the select lists directly under `body`.
///
/// A nested `SetExpr::Query` is deliberately *not* followed: the visitor reaches that
/// `Query` on its own and would then label it twice.
fn label_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => label_select(select),
        SetExpr::SetOperation { left, right, .. } => {
            label_set_expr(left);
            label_set_expr(right);
        }
        _ => {}
    }
}

/// Alias the items of one select list whose PostgreSQL label is unambiguous there.
fn label_select(select: &mut Select) {
    // Every name the projection already resolves to, whether the client wrote it or
    // DataFusion would derive it. A label is only safe to add if it appears once in
    // here, so an item is counted before any aliasing happens.
    let mut counts = std::collections::HashMap::<String, usize>::new();
    for item in &select.projection {
        if let Some(label) = existing_label(item) {
            *counts.entry(label).or_default() += 1;
        }
    }

    select.projection = std::mem::take(&mut select.projection)
        .into_iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => match function_label(&expr) {
                Some(label) if counts.get(&label) == Some(&1) => SelectItem::ExprWithAlias {
                    expr,
                    alias: Ident::new(label),
                },
                _ => SelectItem::UnnamedExpr(expr),
            },
            other => other,
        })
        .collect();
}

/// The name this select item already occupies in the result, where that name is one an
/// added label could collide with.
///
/// Only the three forms whose label is a bare identifier are reported: an explicit
/// alias, a plain column, and a function call (which is what would be relabelled).
/// Every other expression gets a derived label containing the expression itself —
/// `sum(a) + Int64(1)` — which no function name can equal, so leaving it uncounted
/// cannot hide a collision. A wildcard is uncounted for the opposite reason: its
/// columns are not known here, so no label is provably unique and nothing about the
/// conservative direction is lost.
fn existing_label(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
            Expr::CompoundIdentifier(parts) => Some(parts.last()?.value.to_ascii_lowercase()),
            _ => function_label(expr),
        },
        _ => None,
    }
}

/// The label PostgreSQL gives an unaliased call to this function, or `None` if the
/// expression is not a function call.
///
/// The bare function name, lowercased, and the *client's* spelling of it: this runs
/// before the aggregate renames in [`super::pg_operators`], so `SELECT variance(x)`
/// is labelled `variance` and not the `var_samp` VaireDB plans it as.
fn function_label(expr: &Expr) -> Option<String> {
    let Expr::Function(func) = expr else {
        return None;
    };
    Some(func.name.0.last()?.as_ident()?.value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    fn labelled(sql: &str) -> String {
        let mut stmt: Statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0);
        label_function_columns(&mut stmt);
        stmt.to_string()
    }

    #[test]
    fn labels_an_aggregate_with_its_own_name() {
        assert_eq!(
            labelled("SELECT sum(n) FROM t"),
            "SELECT sum(n) AS sum FROM t"
        );
        assert_eq!(
            labelled("SELECT count(*) FROM t"),
            "SELECT count(*) AS count FROM t"
        );
    }

    // The long one: the label DataFusion derives here is 99 bytes of frame internals,
    // past PostgreSQL's own 63-byte limit for a name.
    #[test]
    fn labels_a_window_function() {
        assert_eq!(
            labelled("SELECT row_number() OVER (ORDER BY x) FROM t"),
            "SELECT row_number() OVER (ORDER BY x) AS row_number FROM t"
        );
    }

    // The client's spelling, because this runs before the aggregate renames.
    #[test]
    fn labels_with_the_name_the_client_wrote() {
        assert_eq!(
            labelled("SELECT variance(x) FROM t"),
            "SELECT variance(x) AS variance FROM t"
        );
    }

    // DataFusion rejects a projection with two fields of the same name, so a label that
    // would collide is not applied — the query keeps working with a verbose label.
    #[test]
    fn leaves_a_colliding_label_alone() {
        let sql = "SELECT sum(a), sum(b) FROM t";
        assert_eq!(labelled(sql), sql);
    }

    // Including a collision with an alias the client wrote, and with a plain column.
    #[test]
    fn counts_the_names_already_in_the_projection() {
        let with_alias = "SELECT sum(a) AS sum, sum(b) FROM t";
        assert_eq!(labelled(with_alias), with_alias);

        let with_column = "SELECT count, count(*) FROM t";
        assert_eq!(labelled(with_column), with_column);
    }

    // Two different functions do not collide, so both are labelled.
    #[test]
    fn labels_each_of_two_distinct_functions() {
        assert_eq!(
            labelled("SELECT sum(a), count(*) FROM t"),
            "SELECT sum(a) AS sum, count(*) AS count FROM t"
        );
    }

    // An alias the client wrote is the client's, and is never replaced.
    #[test]
    fn never_overwrites_an_alias_the_client_wrote() {
        let sql = "SELECT sum(n) AS total FROM t";
        assert_eq!(labelled(sql), sql);
    }

    #[test]
    fn labels_a_subquery_and_both_sides_of_a_union() {
        assert_eq!(
            labelled("SELECT * FROM (SELECT max(x) FROM t) s"),
            "SELECT * FROM (SELECT max(x) AS max FROM t) s"
        );
        assert_eq!(
            labelled("SELECT min(x) FROM a UNION ALL SELECT max(x) FROM b"),
            "SELECT min(x) AS min FROM a UNION ALL SELECT max(x) AS max FROM b"
        );
    }

    // This runs on every SELECT, so a statement with nothing to label must come through
    // byte-identical — including one whose projection is a wildcard or an arithmetic
    // expression rather than a call.
    #[test]
    fn leaves_a_statement_with_nothing_to_label_unchanged() {
        for sql in [
            "SELECT * FROM t",
            "SELECT a, b FROM t WHERE a > 1 ORDER BY b",
            "SELECT a + 1 FROM t",
        ] {
            assert_eq!(labelled(sql), sql);
        }
    }
}
