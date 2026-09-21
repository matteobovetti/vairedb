//! Resolve an unqualified `USING` or `NATURAL` key written in a `WHERE` clause, which
//! DataFusion refuses as ambiguous and PostgreSQL does not refuse at all.
//!
//! ```text
//! SELECT l.lv, r.rv FROM l JOIN r USING (c) WHERE c > 1
//!
//! PostgreSQL  the rows with c > 1
//! VaireDB     ERROR: 42703: Ambiguous reference to unqualified field c
//! ```
//!
//! `USING (c)` says the join's output has one column named `c`, and PostgreSQL resolves a
//! bare `c` to it from every clause. DataFusion's join schema holds the *two* underlying
//! fields — `l.c` and `r.c` — and its name resolution consults the join's `USING` set from
//! the select list, `GROUP BY`, `HAVING` and `ORDER BY` but **not** from a `WHERE` predicate,
//! where the two fields are simply two candidates for one name.
//!
//! What makes this worth a module rather than a documented limitation is the shape of the
//! failure. It is not a missing feature a client can plan around: it is one clause out of
//! five refusing a name the other four accept, on a join every SQL tutorial teaches, with a
//! code (`42703`, "column does not exist") that names the wrong problem — PostgreSQL's code
//! for a genuinely ambiguous column is `42702`, and this column is not ambiguous in
//! PostgreSQL at all. A client reading the message looks for a typo.
//!
//! ## The rewrite
//!
//! The bare key becomes the expression PostgreSQL's merged column *is*, which depends only
//! on the join type:
//!
//! | join | merged `c` | written as |
//! |---|---|---|
//! | `INNER` | the two are equal | `l.c` |
//! | `LEFT` | the left row always exists | `l.c` |
//! | `RIGHT` | the right row always exists | `r.c` |
//! | `FULL` | either side may be absent | `COALESCE(l.c, r.c)` |
//!
//! Only the `WHERE` clause is rewritten, because it is the only clause that needs it. The
//! others already resolve the merged column, and rewriting them would replace a correct
//! answer with an equal one for no reason — and for `ORDER BY`, with a *different* one, since
//! a bare name there matches an output column first.
//!
//! ## Why it is correct under both of the other two passes
//!
//! This runs after [`super::pg_using_join_qualifiers`], so a block that already qualifies a
//! key has had its join respelled to `ON` and its unqualified keys merged — there is no
//! `USING` constraint left for this pass to find, and nothing to do. A block that does not
//! qualify a key keeps its `USING` join, and [`super::pg_using_join_merge`] writes the merged
//! value into both underlying fields on the plan. The `COALESCE` above is then computed over
//! two copies of the merged value, and `COALESCE(m, m)` is `m` — so the predicate filters on
//! the merged key either way, which is what PostgreSQL does.
//!
//! That is why the rewrite introduces a qualifier here and the plan-level merge is left
//! alone: writing `l.c` into the `WHERE` of a block that has no other qualified reference
//! does not make the join a candidate for respelling (this pass runs after that decision),
//! and it does not need to be one.
//!
//! ## `NATURAL`
//!
//! A `NATURAL` join's key set is the columns the two sides *share*, which is a fact about the
//! catalog rather than about the statement — so it is read from the catalog, for the two
//! sides that have an entry there. A side that is a derived table, a function or a join is
//! left alone, and so is a `NATURAL` join to a view: the shared set would then depend on the
//! view's own expansion, and getting it wrong means filtering on a column the client did not
//! name.
//!
//! Unlike [`super::pg_using_join_qualifiers`], which leaves `NATURAL` alone entirely, reading
//! the catalog is safe *here* because this pass only names a column both sides already have.
//! It changes no join and no output column: if the catalog is stale and `c` is not shared
//! after all, the rewritten `l.c` is refused by the planner exactly as the bare `c` was.
//!
//! ## What is left as it is
//!
//! * A join whose **left side is another join** (`a JOIN b USING (x) JOIN c USING (y)` for the
//!   second join), where which relation below holds the key is a catalog fact this pass does
//!   not need to guess: the bare key still fails, as it did.
//! * Two joins in one block sharing a key **name**, where the bare name has two merged
//!   columns to be — PostgreSQL reads that as ambiguous too, so the refusal is the answer.
//! * A key reached from a **nested** query block, whose bare names resolve against its own
//!   relations first, which is PostgreSQL's rule and not a shortcut.
//! * A side with no single-part qualifier, which is the same set
//!   [`super::pg_using_join_qualifiers`] declines and for the same reason.
//!
//! ## Where this runs
//!
//! On the AST in [`super::parser::prepare_select_for_planning`], and it has to be: the
//! failure is name resolution, so by the time there is a plan the statement has already been
//! refused.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use crate::catalog::MetadataCatalog;
use crate::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    JoinConstraint, JoinOperator, ObjectName, Query, Select, SetExpr, Statement, TableFactor,
    TableWithJoins, VisitMut, VisitorMut,
};

/// Rewrite every unqualified `USING` or `NATURAL` key written in a `WHERE` clause into the
/// side-qualified expression PostgreSQL's merged column is.
///
/// Infallible: a shape it cannot resolve is left exactly as it was, for the planner to refuse
/// as it already does.
pub(super) fn qualify_using_keys_in_where(stmt: &mut Statement, catalog: &Arc<MetadataCatalog>) {
    struct Qualifier<'a> {
        catalog: &'a Arc<MetadataCatalog>,
    }

    impl VisitorMut for Qualifier<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            match &mut *query.body {
                SetExpr::Select(select) => qualify_in_select(select, self.catalog),
                other => qualify_in_set_expr(other, self.catalog),
            }
            ControlFlow::Continue(())
        }
    }

    let _ = stmt.visit(&mut Qualifier { catalog });
}

/// Reach the selects directly under `body`.
///
/// A set operation's branches are `SetExpr`s rather than `Query`s, so the visitor above never
/// sees them on their own — the same walk [`super::pg_using_join_qualifiers`] does.
fn qualify_in_set_expr(body: &mut SetExpr, catalog: &Arc<MetadataCatalog>) {
    match body {
        SetExpr::Select(select) => qualify_in_select(select, catalog),
        SetExpr::SetOperation { left, right, .. } => {
            qualify_in_set_expr(left, catalog);
            qualify_in_set_expr(right, catalog);
        }
        _ => {}
    }
}

/// One merged key of one query block: the name to look for, and what to put in its place.
struct MergedKey {
    key: Ident,
    replacement: Expr,
}

fn qualify_in_select(select: &mut Select, catalog: &Arc<MetadataCatalog>) {
    // Nothing to resolve without a predicate, and the walk below is the only work this pass
    // does — so the common statement pays one `is_none` check.
    if select.selection.is_none() {
        return;
    }
    let merged = merged_keys(&select.from, catalog);
    if merged.is_empty() {
        return;
    }
    let by_name: HashMap<String, &Expr> = merged
        .iter()
        .map(|m| (folded(&m.key), &m.replacement))
        .collect();

    let Some(predicate) = select.selection.as_mut() else {
        return;
    };
    replace_bare_keys(predicate, &by_name);
}

/// Every merged key of this query block, paired with the expression it resolves to.
///
/// A name that two joins in the block both merge is dropped rather than resolved to one of
/// them: PostgreSQL reads that as ambiguous, and so should VaireDB.
fn merged_keys(from: &[TableWithJoins], catalog: &Arc<MetadataCatalog>) -> Vec<MergedKey> {
    let mut merged: Vec<MergedKey> = Vec::new();
    let mut ambiguous: HashSet<String> = HashSet::new();

    for table in from {
        for (join_index, join) in table.joins.iter().enumerate() {
            // Only the first join of a chain: for a later one the left side is the join below
            // it, and which relation down there holds the key is a catalog fact.
            if join_index != 0 {
                continue;
            }
            let (Some(left), Some(right)) =
                (qualifier_of(&table.relation), qualifier_of(&join.relation))
            else {
                continue;
            };
            let Some((kind, constraint)) = merging_join(&join.join_operator) else {
                continue;
            };
            let keys = match constraint {
                Keys::Using(keys) => keys,
                Keys::Natural => match shared_columns(&table.relation, &join.relation, catalog) {
                    Some(keys) => keys,
                    None => continue,
                },
            };
            for key in keys {
                let name = folded(&key);
                if by_name(&merged, &name).is_some() {
                    ambiguous.insert(name);
                    continue;
                }
                let replacement = merged_expr(kind, &left, &right, &key);
                merged.push(MergedKey { key, replacement });
            }
        }
    }

    merged.retain(|m| !ambiguous.contains(&folded(&m.key)));
    merged
}

fn by_name<'a>(merged: &'a [MergedKey], name: &str) -> Option<&'a MergedKey> {
    merged.iter().find(|m| folded(&m.key) == name)
}

/// How the two sides of a join can disagree about a merged key.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    /// The left row always exists, so the merged key is the left one — inner and left joins.
    LeftWins,
    /// The right row always exists, so the merged key is the right one.
    RightWins,
    /// Either side may be absent, so the merged key is the first one present.
    Either,
}

/// Where a join's keys come from.
enum Keys {
    Using(Vec<Ident>),
    Natural,
}

/// The join kind and key source, for the join types that merge a key at all.
///
/// `CROSS JOIN` and a join with an `ON` constraint merge nothing — an `ON` join's two key
/// columns keep their own names, which is why the `ON` spelling of this query has always
/// worked. Semi and anti joins are not PostgreSQL syntax.
fn merging_join(operator: &JoinOperator) -> Option<(JoinKind, Keys)> {
    let (kind, constraint) = match operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (JoinKind::LeftWins, constraint)
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (JoinKind::LeftWins, constraint)
        }
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
            (JoinKind::RightWins, constraint)
        }
        JoinOperator::FullOuter(constraint) => (JoinKind::Either, constraint),
        _ => return None,
    };
    let keys = match constraint {
        JoinConstraint::Using(keys) => Keys::Using(single_part_keys(keys)?),
        JoinConstraint::Natural => Keys::Natural,
        JoinConstraint::On(_) | JoinConstraint::None => return None,
    };
    Some((kind, keys))
}

/// What the merged key is worth, written as an expression over the two sides.
fn merged_expr(kind: JoinKind, left: &Ident, right: &Ident, key: &Ident) -> Expr {
    let left_column = qualified(left, key);
    let right_column = qualified(right, key);
    match kind {
        JoinKind::LeftWins => left_column,
        JoinKind::RightWins => right_column,
        // `COALESCE` and not a `CASE`, because it is also what PostgreSQL's own
        // documentation defines the merged column of a full join as.
        JoinKind::Either => coalesce(left_column, right_column),
    }
}

/// The columns two relations share, from the catalog, as the key set a `NATURAL` join merges.
///
/// `None` for a side the catalog has no table entry for — a derived table, a function, a
/// view, a join, or a name that is not there at all. A view is deliberately in that list: its
/// columns come from its own expansion rather than from a table record, so the shared set
/// would be a guess.
fn shared_columns(
    left: &TableFactor,
    right: &TableFactor,
    catalog: &Arc<MetadataCatalog>,
) -> Option<Vec<Ident>> {
    let left_columns = table_columns(left, catalog)?;
    let right_columns = table_columns(right, catalog)?;
    let right_names: HashSet<String> = right_columns.iter().map(|c| c.to_lowercase()).collect();
    // In the left side's declaration order, which is the order PostgreSQL merges them in —
    // it does not matter for a predicate, and it makes the rewrite deterministic.
    let shared: Vec<Ident> = left_columns
        .iter()
        .filter(|name| right_names.contains(&name.to_lowercase()))
        .map(|name| Ident::new(name.clone()))
        .collect();
    (!shared.is_empty()).then_some(shared)
}

/// The column names the catalog holds for a plain named relation.
fn table_columns(factor: &TableFactor, catalog: &Arc<MetadataCatalog>) -> Option<Vec<String>> {
    let TableFactor::Table { name, .. } = factor else {
        return None;
    };
    let [part] = name.0.as_slice() else {
        return None;
    };
    let table = part.as_ident()?;
    // A best-effort read: a catalog error is the same answer as a missing table here, which
    // is to leave the statement alone rather than to fail a query over a lookup.
    let meta = catalog.get_table(&table.value).ok().flatten()?;
    Some(meta.columns.into_iter().map(|column| column.name).collect())
}

/// Replace every unqualified reference to a merged key inside one predicate.
///
/// Stops at a nested query block, whose bare names resolve against its own relations first.
fn replace_bare_keys(predicate: &mut Expr, by_name: &HashMap<String, &Expr>) {
    struct Replacer<'a> {
        by_name: &'a HashMap<String, &'a Expr>,
        /// How many query blocks below the predicate the walk is.
        depth: usize,
    }

    impl VisitorMut for Replacer<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if self.depth > 0 {
                return ControlFlow::Continue(());
            }
            if let Expr::Identifier(ident) = expr
                && let Some(replacement) = self.by_name.get(&folded(ident))
            {
                *expr = (*replacement).clone();
            }
            ControlFlow::Continue(())
        }
    }

    let mut replacer = Replacer { by_name, depth: 0 };
    let _ = predicate.visit(&mut replacer);
}

/// `<qualifier>.<key>`, preserving how the client spelled the key.
fn qualified(qualifier: &Ident, key: &Ident) -> Expr {
    Expr::CompoundIdentifier(vec![qualifier.clone(), key.clone()])
}

fn coalesce(left: Expr, right: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("coalesce")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![left, right]
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

/// The key list as plain identifiers, or `None` if any key is not one.
fn single_part_keys(keys: &[ObjectName]) -> Option<Vec<Ident>> {
    keys.iter()
        .map(|key| match key.0.as_slice() {
            [part] => part.as_ident().cloned(),
            _ => None,
        })
        .collect()
}

/// The name a client writes to qualify a column of this relation — an alias where one is
/// written, otherwise the relation's own single-part name.
fn qualifier_of(factor: &TableFactor) -> Option<Ident> {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some(alias.name.clone()),
            None => match name.0.as_slice() {
                [part] => part.as_ident().cloned(),
                _ => None,
            },
        },
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|alias| alias.name.clone()),
        _ => None,
    }
}

/// An identifier's name for comparison: case-folded when unquoted, as SQL folds it.
fn folded(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableMeta};
    use crate::pgwire_handler::test_catalog::scratch_catalog;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    /// `l(c, lv)`, `r(c, rv)` and `w(n, g)` — the three tables the `NATURAL` cases need a
    /// column list for. `l` and `r` share `c` and nothing else, which is what makes
    /// `l NATURAL JOIN r` a one-key join.
    fn catalog() -> Arc<MetadataCatalog> {
        let catalog = scratch_catalog("pg_using_join_where_keys");
        for (name, columns) in [("l", ["c", "lv"]), ("r", ["c", "rv"]), ("w", ["n", "g"])] {
            catalog
                .put_table(&TableMeta {
                    table_name: name.to_string(),
                    columns: columns
                        .iter()
                        .map(|column| ColumnDef {
                            name: column.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .expect("stored");
        }
        Arc::new(catalog)
    }

    fn rewritten(sql: &str) -> String {
        let catalog = catalog();
        let mut stmt = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .expect("parsed")
            .remove(0);
        qualify_using_keys_in_where(&mut stmt, &catalog);
        stmt.to_string()
    }

    /// The four join types, each resolving the key to the column PostgreSQL's merged one is.
    #[test]
    fn the_key_resolves_to_the_side_the_join_type_makes_it() {
        assert_eq!(
            rewritten("SELECT lv FROM l JOIN r USING(c) WHERE c > 1"),
            "SELECT lv FROM l JOIN r USING(c) WHERE l.c > 1"
        );
        assert_eq!(
            rewritten("SELECT lv FROM l LEFT JOIN r USING(c) WHERE c > 1"),
            "SELECT lv FROM l LEFT JOIN r USING(c) WHERE l.c > 1"
        );
        assert_eq!(
            rewritten("SELECT lv FROM l RIGHT JOIN r USING(c) WHERE c > 1"),
            "SELECT lv FROM l RIGHT JOIN r USING(c) WHERE r.c > 1"
        );
        assert_eq!(
            rewritten("SELECT lv FROM l FULL JOIN r USING(c) WHERE c > 1"),
            "SELECT lv FROM l FULL JOIN r USING(c) WHERE coalesce(l.c, r.c) > 1"
        );
    }

    /// Aliases are the qualifier when they are written, because the relation's own name is
    /// not addressable then.
    #[test]
    fn an_alias_is_the_qualifier() {
        assert_eq!(
            rewritten("SELECT a.lv FROM l AS a JOIN r AS b USING(c) WHERE c > 1"),
            "SELECT a.lv FROM l AS a JOIN r AS b USING(c) WHERE a.c > 1"
        );
    }

    /// Every occurrence, however deeply the predicate nests it, and whatever else the
    /// predicate says.
    #[test]
    fn every_occurrence_in_the_predicate_is_resolved() {
        assert_eq!(
            rewritten(
                "SELECT lv FROM l JOIN r USING(c) \
                 WHERE (c > 1 AND c < 9) OR c IS NULL OR c + 1 IN (2, 3)"
            ),
            "SELECT lv FROM l JOIN r USING(c) \
             WHERE (l.c > 1 AND l.c < 9) OR l.c IS NULL OR l.c + 1 IN (2, 3)"
        );
    }

    /// Only the `WHERE` clause. The other clauses already resolve the merged column, and
    /// `ORDER BY` resolves a bare name against the *output* columns first — so rewriting it
    /// would change which column is sorted by.
    #[test]
    fn no_other_clause_is_touched() {
        let sql = "SELECT c, count(*) FROM l JOIN r USING(c) \
                   GROUP BY c HAVING count(*) > 1 ORDER BY c";
        assert_eq!(rewritten(sql), sql);
    }

    /// A qualified reference is already unambiguous and means what the client wrote.
    #[test]
    fn a_qualified_reference_is_left_alone() {
        let sql = "SELECT lv FROM l JOIN r USING(c) WHERE r.c > 1";
        assert_eq!(rewritten(sql), sql);
    }

    /// A column that is not a key of the join has one field to resolve to and needs nothing.
    #[test]
    fn a_non_key_column_is_left_alone() {
        let sql = "SELECT lv FROM l JOIN r USING(c) WHERE lv <> 'x'";
        assert_eq!(rewritten(sql), sql);
    }

    /// An `ON` join keeps its two key columns under their own names, so a bare `c` there is
    /// genuinely ambiguous in PostgreSQL too — and the refusal is the right answer.
    #[test]
    fn an_on_join_is_left_for_the_planner_to_refuse() {
        let sql = "SELECT lv FROM l JOIN r ON l.c = r.c WHERE c > 1";
        assert_eq!(rewritten(sql), sql);
    }

    /// A `NATURAL` join's keys come from the catalog, because they are not in the statement.
    #[test]
    fn a_natural_joins_keys_are_read_from_the_catalog() {
        assert_eq!(
            rewritten("SELECT lv FROM l NATURAL JOIN r WHERE c > 1"),
            "SELECT lv FROM l NATURAL JOIN r WHERE l.c > 1"
        );
        assert_eq!(
            rewritten("SELECT lv FROM l NATURAL FULL JOIN r WHERE c > 1"),
            "SELECT lv FROM l NATURAL FULL JOIN r WHERE coalesce(l.c, r.c) > 1"
        );
    }

    /// A side the catalog has no table record for is left alone rather than guessed at: the
    /// shared set is exactly what the client did not write.
    #[test]
    fn a_natural_join_to_something_the_catalog_does_not_hold_is_left_alone() {
        let sql = "SELECT lv FROM l NATURAL JOIN (SELECT 1 AS c) AS d WHERE c > 1";
        assert_eq!(rewritten(sql), sql);
    }

    /// A bare name inside a nested block resolves against that block's own relations first,
    /// which is PostgreSQL's rule.
    #[test]
    fn a_nested_blocks_own_names_are_left_to_it() {
        assert_eq!(
            rewritten(
                "SELECT lv FROM l JOIN r USING(c) \
                 WHERE c > (SELECT max(c) FROM r)"
            ),
            "SELECT lv FROM l JOIN r USING(c) \
             WHERE l.c > (SELECT max(c) FROM r)"
        );
    }

    /// Each query block decides for itself, so a join in a subquery is resolved by that
    /// block's own joins.
    #[test]
    fn each_query_block_is_resolved_on_its_own() {
        assert_eq!(
            rewritten(
                "SELECT x FROM (SELECT lv AS x FROM l JOIN r USING(c) WHERE c > 1) AS s \
                 WHERE x <> 'y'"
            ),
            "SELECT x FROM (SELECT lv AS x FROM l JOIN r USING(c) WHERE l.c > 1) AS s \
             WHERE x <> 'y'"
        );
    }

    /// A later join's left side is the join below it, so which relation holds the key is a
    /// catalog fact this pass does not guess — and the refusal stands.
    #[test]
    fn a_join_of_a_join_is_left_alone() {
        let sql = "SELECT lv FROM l JOIN r USING(c) JOIN w USING(n) WHERE n > 1";
        assert_eq!(rewritten(sql), sql);
        // The first join's key in the same statement is still resolved: declining the second
        // join is not declining the block.
        assert_eq!(
            rewritten("SELECT lv FROM l JOIN r USING(c) JOIN w USING(n) WHERE c > 1"),
            "SELECT lv FROM l JOIN r USING(c) JOIN w USING(n) WHERE l.c > 1"
        );
    }

    /// Two joins merging a key of the same name leave the bare name with two columns to be,
    /// which PostgreSQL reads as ambiguous too.
    #[test]
    fn a_key_name_two_joins_share_stays_ambiguous() {
        let sql = "SELECT l.c FROM l JOIN r USING(c), l AS l2 JOIN r AS r2 USING(c) \
                   WHERE c > 1";
        assert_eq!(rewritten(sql), sql);
    }

    /// A statement with no `WHERE` clause is the common case and is not walked at all.
    #[test]
    fn a_statement_without_a_predicate_is_untouched() {
        let sql = "SELECT c, lv, rv FROM l JOIN r USING(c)";
        assert_eq!(rewritten(sql), sql);
    }
}
