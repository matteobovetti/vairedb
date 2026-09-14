//! Label a result column the way PostgreSQL labels it, for the expression forms whose
//! PostgreSQL label is a bare name: a function call, an array subscript, and a cast to
//! `bytea`.
//!
//! PostgreSQL names an unaliased result column after the function that produced it:
//! `SELECT sum(n)` returns a column called `sum`, and `SELECT row_number() OVER (...)`
//! one called `row_number`. A subscript follows the same principle from the other end —
//! the name comes from what is being subscripted, so `SELECT a[1]` and `SELECT a[1:2]`
//! are both a column called `a`. DataFusion instead names either one after the whole
//! expression as it renders internally, which is a different string and sometimes a very
//! long one — `row_number() ORDER BY [t.x ASC NULLS LAST] RANGE BETWEEN UNBOUNDED
//! PRECEDING AND CURRENT ROW` is 99 bytes, past PostgreSQL's 63-byte `NAMEDATALEN`, and
//! it leaks the planner's internal spelling of a frame the client never wrote.
//!
//! A label is not cosmetic to a client. A driver that reads columns by name, an ORM
//! that maps them onto fields, and `psql`'s own header all take the label as the
//! contract, so the read path adds the alias PostgreSQL would have given.
//!
//! The subscript half also keeps a label from *becoming* internal: the read path clamps a
//! subscript's bounds to PostgreSQL's out-of-range rule ([`super::pg_subscripts`]), and
//! `t.a[Int64(1)]` would otherwise have become `t.a[nullif(greatest(Int64(1),Int64(0)),
//! Int64(0))]` — the rewrite's own spelling, in a name, over the 63-byte limit. A cast to
//! `bytea` is labelled here for the same reason and no other: the read path rewrites it
//! into a `vaire_bytea_in(…)` call ([`vairedb_common::bytea_in`]), and a client asking for
//! `s::bytea` should not be told the column is called after VaireDB's own function.
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
    AccessExpr, CastKind, DataType, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement,
    VisitMut, VisitorMut,
};

/// Give every unaliased function-call, array-subscript and `bytea`-cast result column the
/// label PostgreSQL would.
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
    // A subscript's label is its base column's name, so a wildcard beside it collides by
    // construction — `SELECT *, a[1] FROM t` would project `a` twice and DataFusion
    // refuses that. The count above cannot see a wildcard's columns, so this is the one
    // case where the collision has to be inferred from the wildcard itself rather than
    // counted. A function's label is not at risk the same way and stays unaffected.
    let has_wildcard = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    });

    select.projection = std::mem::take(&mut select.projection)
        .into_iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => match pg_label(&expr) {
                Some(label)
                    if counts.get(&label) == Some(&1)
                        && !(has_wildcard && takes_its_label_from_a_column(&expr)) =>
                {
                    SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::new(label),
                    }
                }
                _ => SelectItem::UnnamedExpr(expr),
            },
            other => other,
        })
        .collect();
}

/// The name this select item already occupies in the result, where that name is one an
/// added label could collide with.
///
/// Only the forms whose label is a bare identifier are reported: an explicit
/// alias, a plain column, and the two [`pg_label`] relabels. Every other expression gets
/// a derived label containing the expression itself — `sum(a) + Int64(1)` — which no bare
/// name can equal, so leaving it uncounted cannot hide a collision. A wildcard is
/// uncounted for the opposite reason: its columns are not known here, so no label is
/// provably unique and nothing about the conservative direction is lost.
fn existing_label(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
            Expr::CompoundIdentifier(parts) => Some(parts.last()?.value.to_ascii_lowercase()),
            _ => pg_label(expr),
        },
        _ => None,
    }
}

/// The label PostgreSQL gives this expression unaliased, or `None` if it is not one of the
/// forms whose label is a bare name.
fn pg_label(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Function(_) => function_label(expr),
        // A cast to `bytea`, which the read path rewrites into a `vaire_bytea_in(…)` call
        // ([`vairedb_common::bytea_in`]) and would otherwise be named after. PostgreSQL
        // labels a cast after whatever is being cast and falls back to the type's own name,
        // so `s::bytea` is `s` and `'\x41'::bytea` is `bytea` — measured on PostgreSQL 17.
        //
        // Only `bytea`, because it is the only cast this read path rewrites. Every other
        // cast keeps DataFusion's label, which is already the inner expression's name; the
        // type-name fallback PostgreSQL applies to those is a separate divergence and not
        // one a client can be given a wrong *value* by.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type: DataType::Bytea,
            expr: inner,
            ..
        } => Some(bare_name(inner).unwrap_or_else(|| "bytea".to_string())),
        // Only a chain that *ends* in a subscript: that is the form the clamp rewrites, and
        // the one whose label PostgreSQL takes from the thing being subscripted rather than
        // from the subscript. `a[1].f` ends in a field and is PostgreSQL-labelled `f`, which
        // is not this gap's business.
        Expr::CompoundFieldAccess { root, access_chain }
            if matches!(access_chain.last(), Some(AccessExpr::Subscript(_))) =>
        {
            // Skipping the trailing subscripts leaves whatever is subscripted. A `.field`
            // there is a composite field reference, and PostgreSQL labels a subscript of one
            // after the field: `t.a[1]` and `(s).f[1]` are `a` and `f`. sqlparser puts a
            // table qualifier in the same shape, so both arrive here and both are right.
            let subscripted = access_chain
                .iter()
                .rev()
                .find(|access| !matches!(access, AccessExpr::Subscript(_)));
            match subscripted {
                Some(AccessExpr::Dot(Expr::Identifier(ident))) => {
                    Some(ident.value.to_ascii_lowercase())
                }
                Some(_) => None,
                None => bare_name(root),
            }
        }
        _ => None,
    }
}

/// Whether this expression's label is the name of a column, and so collides with a
/// wildcard in the same projection.
///
/// A wildcard's own columns are not known here, so a label that *is* a column's name has to
/// be given up beside one: `SELECT *, a[1] FROM t` would project `a` twice and DataFusion
/// refuses a projection with two fields of the same name. A label taken from a type or a
/// function name cannot collide that way, so `SELECT *, '\x41'::bytea` and
/// `SELECT *, sum(n) FROM t` are still labelled.
fn takes_its_label_from_a_column(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundFieldAccess { .. } => pg_label(expr).is_some(),
        Expr::Cast { expr: inner, .. } => bare_name(inner).is_some(),
        _ => false,
    }
}

/// The bare name PostgreSQL takes a label from when the expression is not itself named:
/// whatever is being subscripted, or cast.
///
/// `a[1]` and `a[1][2]` are both `a`, `t.a[1]` is `a`, `(ARRAY[1,2,3])[1]` is `array`, and
/// `(s::text)::bytea` is `s` — measured against PostgreSQL 17, whose `FigureColname`
/// recurses through exactly these forms. Anything else has no bare name to take, so it
/// keeps DataFusion's derived label, or the type name where the caller has one.
fn bare_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => Some(parts.last()?.value.to_ascii_lowercase()),
        Expr::Nested(inner) => bare_name(inner),
        Expr::Array(_) => Some("array".to_string()),
        Expr::Function(_) => function_label(expr),
        Expr::CompoundFieldAccess { .. } => pg_label(expr),
        // Recursed into without a type-name fallback of its own: PostgreSQL lets the
        // outermost cast's type name win, and the outermost is the caller.
        Expr::Cast { expr: inner, .. } => bare_name(inner),
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

    // A subscript is labelled after what is subscripted, in each of the shapes that has a
    // bare name — measured against PostgreSQL 17.
    #[test]
    fn labels_a_subscript_after_what_it_subscripts() {
        assert_eq!(labelled("SELECT a[1] FROM t"), "SELECT a[1] AS a FROM t");
        assert_eq!(
            labelled("SELECT a[1:2] FROM t"),
            "SELECT a[1:2] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT a[1][2] FROM t"),
            "SELECT a[1][2] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT t.a[1] FROM t"),
            "SELECT t.a[1] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT (ARRAY[1, 2, 3])[1]"),
            "SELECT (ARRAY[1, 2, 3])[1] AS array"
        );
        assert_eq!(
            labelled("SELECT string_to_array(s, ',')[1] FROM t"),
            "SELECT string_to_array(s, ',')[1] AS string_to_array FROM t"
        );
    }

    // The same uniqueness rule as a function's: PostgreSQL is happy with two columns called
    // `a` and DataFusion is not.
    #[test]
    fn leaves_a_colliding_subscript_label_alone() {
        for sql in [
            "SELECT a, a[1] FROM t",
            "SELECT a[1], a[2] FROM t",
            "SELECT a[1] AS a, a[2] FROM t",
        ] {
            assert_eq!(labelled(sql), sql, "`{sql}`");
        }
    }

    // A wildcard is the collision that cannot be counted: the base column is certainly
    // among its columns, so the label is not added at all.
    #[test]
    fn leaves_a_subscript_beside_a_wildcard_alone() {
        for sql in ["SELECT *, a[1] FROM t", "SELECT t.*, a[1] FROM t"] {
            assert_eq!(labelled(sql), sql, "`{sql}`");
        }
        // A function's label is not at risk the same way, so a wildcard does not stop it.
        assert_eq!(
            labelled("SELECT *, sum(n) FROM t"),
            "SELECT *, sum(n) AS sum FROM t"
        );
    }

    // A `.field` before the subscript is labelled after the field, which is the same rule
    // as the root's — and a chain that *ends* in a field is not a subscript expression at
    // all, so it keeps DataFusion's label.
    #[test]
    fn labels_a_subscript_of_a_field_after_the_field() {
        assert_eq!(
            labelled("SELECT (s).f[1] FROM t"),
            "SELECT (s).f[1] AS f FROM t"
        );
        let ends_in_a_field = "SELECT a[1].f FROM t";
        assert_eq!(labelled(ends_in_a_field), ends_in_a_field);
    }

    // A cast to `bytea` is labelled after what is cast, falling back to the type name —
    // measured against PostgreSQL 17. Both spellings of the cast, since the read path
    // rewrites both.
    #[test]
    fn labels_a_bytea_cast_after_what_is_cast() {
        assert_eq!(
            labelled("SELECT s::bytea FROM t"),
            "SELECT s::BYTEA AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT CAST(t.s AS BYTEA) FROM t"),
            "SELECT CAST(t.s AS BYTEA) AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT (s::TEXT)::BYTEA FROM t"),
            "SELECT (s::TEXT)::BYTEA AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT '\\x41'::bytea"),
            "SELECT '\\x41'::BYTEA AS bytea"
        );
    }

    // Only `bytea`, because it is the only cast the read path rewrites — every other cast
    // already carries the inner expression's name through DataFusion unchanged.
    #[test]
    fn leaves_every_other_cast_alone() {
        for sql in [
            "SELECT s::TEXT FROM t",
            "SELECT '1'::INT",
            "SELECT n::BIGINT FROM t",
        ] {
            assert_eq!(labelled(sql), sql, "`{sql}`");
        }
    }

    // A type name cannot collide with a wildcard's columns, so the literal cast keeps its
    // label where a column cast gives its up.
    #[test]
    fn a_bytea_cast_beside_a_wildcard_keeps_only_a_type_label() {
        assert_eq!(
            labelled("SELECT *, '\\x41'::bytea FROM t"),
            "SELECT *, '\\x41'::BYTEA AS bytea FROM t"
        );
        let from_a_column = "SELECT *, s::BYTEA FROM t";
        assert_eq!(labelled(from_a_column), from_a_column);
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
