//! Give a `USING` key reached by an **explicit qualifier** the per-side value PostgreSQL
//! gives it, by respelling the join so that the merged column and the two raw ones are
//! three different names again.
//!
//! ```text
//! l = (1,2,3,4)   r = (1,2,3,5)
//!
//! postgres=# SELECT l.id, r.id FROM l FULL JOIN r USING (id);
//!   l.id | r.id
//!   -----+-----
//!    1   | 1
//!    2   | 2
//!    3   | 3
//!    4   |            -- the row only `l` has: `r` has no key to report
//!        | 5          -- the row only `r` has
//! ```
//!
//! `USING (id)` says the join's output has **one** column `id`, worth
//! `COALESCE(l.id, r.id)` — and [`super::pg_using_join_merge`] is what puts that value
//! there. But it does not take `l.id` and `r.id` away: PostgreSQL's join output has *three*
//! addressable names, and only the unqualified one is merged. A `DFSchema` has two fields,
//! so the merge has to write the merged value into whichever of the two every other
//! consumer reads — which is both — and a qualified `l.id` then reads the merged value
//! rather than the left side's own. Silently: `4` where PostgreSQL reports `4` beside a
//! NULL, and `5` where PostgreSQL reports a NULL beside `5`, with nothing in the answer to
//! say the answer is not the answer.
//!
//! ## The repair
//!
//! The third name is not missing from the schema — it is missing from the *statement*. So
//! the statement is respelled, before planning, into the one spelling that has all three:
//!
//! ```text
//! SELECT l.id, r.id   FROM l FULL JOIN r USING (id)
//! SELECT l.id, r.id   FROM l FULL JOIN r ON l.id = r.id
//!
//! SELECT id, l.id AS l_id
//!                     FROM l FULL JOIN r USING (id)
//! SELECT COALESCE(l.id, r.id) AS id, l.id AS l_id
//!                     FROM l FULL JOIN r ON l.id = r.id
//! ```
//!
//! `ON` keeps the two key columns apart — which is why the `ON` spelling of this query has
//! always been correct here — and every unqualified reference to the key in the same query
//! block becomes the `COALESCE` that `USING` promised. The two rewrites are one unit: `ON`
//! alone would leave an unqualified `id` ambiguous, and the `COALESCE` alone would be
//! computed over columns the merge had already overwritten.
//!
//! Nothing else about the join moves. `FULL JOIN … ON l.id = r.id` matches exactly the rows
//! `FULL JOIN … USING (id)` matches; `USING` differs only in what the output columns are
//! *called*, and that is what the two rewrites take over. With the constraint no longer
//! `USING`, [`super::pg_using_join_merge`] leaves the join alone, so the merged value is
//! computed once and in one place.
//!
//! ## Only where a qualifier is actually written
//!
//! A statement with no qualified reference to a key is left exactly as it was, for the
//! plan-level merge to handle. That is not caution for its own sake: `SELECT *` over a
//! `USING` join reports the key **once**, and expanding it correctly over an `ON` join
//! would mean knowing each side's column list — a fact about the catalog rather than about
//! the statement. So the two rewrites split the work by which of them can be right:
//!
//! | Written | Handled by | Answer |
//! |---|---|---|
//! | `SELECT id`, `SELECT *`, `GROUP BY id`, `max(id)` | `pg_using_join_merge`, on the plan | the merged key |
//! | `SELECT l.id`, `WHERE r.id > 2`, `ORDER BY l.id` | here, on the AST | each side's own key |
//! | both, in one query block: `SELECT id, l.id AS l_id` | here | merged and raw, side by side |
//!
//! Only a **full** or **right** join is respelled, the same two the merge touches: an inner
//! join's two keys are equal and a left join's merged key *is* its left key, so a qualified
//! reference is already the value PostgreSQL reports.
//!
//! ## What is refused, and what is left
//!
//! A **wildcard** in a query block that also reaches a key by qualifier is refused
//! (`0A000`), naming the `ON` spelling. `SELECT *, l.id FROM l FULL JOIN r USING (id)` is
//! the one shape where the two rewrites want the same statement in two different spellings
//! — the wildcard needs the key merged into one column, the qualifier needs the two apart —
//! and answering the wildcard while leaving the qualifier wrong is what this module exists
//! to stop.
//!
//! The merged key and the same key per side under **one name** is refused too:
//! `SELECT id, l.id FROM l FULL JOIN r USING (id)` is two result columns both called `id` in
//! PostgreSQL, and a `DFSchema` holds neither two fields of one name nor an unqualified `id`
//! beside a qualified `l.id`. That answer is not representable in any spelling — the `ON` one
//! a client writes by hand is refused by DataFusion in the same words — so it is the residue
//! of the three-names-two-fields limit that no rewrite reaches. Naming the per-side column
//! (`SELECT id, l.id AS l_id`) is representable, and is answered; the refusal says so.
//!
//! A third refusal is reachable only through a comma-separated FROM: two respelled joins in
//! one block merging a key of the same name (`FROM a FULL JOIN b USING (id), c FULL JOIN d
//! USING (id)`), where the bare `id` would have two merged columns to be — which PostgreSQL
//! reads as ambiguous too.
//!
//! Left as it is, and so still merged under a qualifier:
//!
//! * **`NATURAL`**, whose key set is a fact about the catalog and not about the statement:
//!   which columns two relations share is exactly what the client did not write, so there
//!   is no `ON` predicate to build and no way to know whether `l.k` is a key at all.
//!   Refusing every qualified reference to either side would refuse `SELECT l.v`, which is
//!   correct today. Spelling the join `USING (…)` — or `ON` — is answered exactly.
//! * a join whose **left side is another join** (`a JOIN b USING (x) FULL JOIN c USING (y)`),
//!   where which relation below holds the key is again a catalog fact.
//! * a side that is neither a named relation nor an aliased derived table, and a
//!   schema-qualified relation with no alias — whose qualifier is not the catalog key that
//!   [`super::parser::collapse_schema_qualified_relations`] leaves in the FROM clause.
//!
//! An **unqualified** key reference inside a nested block is left alone too, and that is
//! PostgreSQL's own rule rather than a shortcut: a bare name there resolves against that
//! block's relations first. A *correlated* one — a bare `id` in a subquery, meaning the
//! respelled join's merged key — is not reached by the merge, and needs nothing: DataFusion
//! does not resolve an unqualified outer reference from a subquery at all (*"No field named
//! id"*), before this rewrite or after it. A **qualified** one, by contrast, is that join's
//! key wherever it is written, so it does respell the join it belongs to.
//!
//! ## Where this runs
//!
//! On the **AST**, in [`super::parser::prepare_select_for_planning`], because both halves of
//! the repair are things the planner has already decided by the time there is a plan: an
//! unqualified `id` has been resolved to one side's field, and a `USING` constraint has
//! become a schema with two fields named `id`. It has to run **after**
//! [`super::views::expand_views`], so that a join inside a view definition is respelled the
//! same way a client's own join is.

use std::collections::HashMap;
use std::ops::ControlFlow;

use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, Join, JoinConstraint, JoinOperator, ObjectName, OrderBy, Query,
    Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor,
    TableWithJoins, Visit, VisitMut, Visitor, VisitorMut,
};

/// Respell every full or right `USING` join in `stmt` whose key a qualifier reaches, so the
/// qualified name is the side's own column and the unqualified one the merged value.
///
/// Applied to a whole statement: every query block gets its own decision, so a join inside
/// a subquery, a CTE or a `UNION` branch is respelled when that block qualifies a key and
/// left alone when it does not.
///
/// Fallible for the shapes one plan schema cannot hold: a wildcard beside a qualified key,
/// and the merged key beside the same key per side under one name. Everything else either
/// rewrites or is left to [`super::pg_using_join_merge`].
pub(super) fn split_qualified_using_keys(stmt: &mut Statement) -> PgWireResult<()> {
    struct Splitter {
        error: Option<PgWireError>,
    }

    impl VisitorMut for Splitter {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            // Destructured rather than passed whole: the body and the `ORDER BY` are
            // rewritten by different rules and are two fields of the same query.
            let Query { body, order_by, .. } = query;
            let outcome = match &mut **body {
                SetExpr::Select(select) => split_in_select(select, order_by.as_mut()),
                other => split_in_set_expr(other),
            };
            if let Err(e) = outcome {
                self.error = Some(e);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut splitter = Splitter { error: None };
    let _ = stmt.visit(&mut splitter);
    match splitter.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Reach the select lists directly under `body`.
///
/// A set operation's branches are `SetExpr`s rather than `Query`s, so the visitor above
/// never sees them on their own; a nested `SetExpr::Query` it does, and is deliberately not
/// followed here. A branch has no `ORDER BY` of its own — the one a client writes belongs to
/// the whole set operation — so none is passed down.
fn split_in_set_expr(body: &mut SetExpr) -> PgWireResult<()> {
    match body {
        SetExpr::Select(select) => split_in_select(select, None),
        SetExpr::SetOperation { left, right, .. } => {
            split_in_set_expr(left)?;
            split_in_set_expr(right)
        }
        _ => Ok(()),
    }
}

/// One full or right `USING` join of a query block, and the two names its keys are reached
/// by.
struct Candidate {
    /// Where the join is, so its constraint can be replaced once the decision is taken.
    from_index: usize,
    join_index: usize,
    /// The qualifier of the left and right side — an alias where one is written, the
    /// relation's own single-part name otherwise.
    left: Ident,
    right: Ident,
    /// The `USING` key columns, as the client spelled them.
    keys: Vec<Ident>,
}

/// Respell the candidate joins of one select whose keys are reached by a qualifier.
fn split_in_select(select: &mut Select, mut order_by: Option<&mut OrderBy>) -> PgWireResult<()> {
    let candidates = candidate_joins(&select.from);
    if candidates.is_empty() {
        return Ok(());
    }
    let split: Vec<Candidate> = candidates
        .into_iter()
        .filter(|candidate| qualifies_a_key(select, order_by.as_deref(), candidate))
        .collect();
    if split.is_empty() {
        return Ok(());
    }
    reject_a_shared_key_name(&split)?;
    reject_a_wildcard(select, &split)?;
    reject_a_merged_key_beside_its_own_name(select, order_by.as_deref(), &split)?;

    for candidate in &split {
        merge_unqualified_keys(select, order_by.as_deref_mut(), candidate);
        let join = &mut select.from[candidate.from_index].joins[candidate.join_index];
        convert_to_on(join, candidate);
    }
    Ok(())
}

/// Every full or right `USING` join in `from` this rewrite is able to respell.
///
/// Each declined shape is declined because the `ON` predicate it would need names something
/// the statement does not say — see the module doc's list.
fn candidate_joins(from: &[TableWithJoins]) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for (from_index, table) in from.iter().enumerate() {
        for (join_index, join) in table.joins.iter().enumerate() {
            // Only the first join of a chain: for a later one the left side is the join
            // below it, and which of *its* relations holds the key is a catalog fact.
            if join_index != 0 {
                continue;
            }
            let Some(keys) = full_or_right_using_keys(&join.join_operator) else {
                continue;
            };
            let Some(keys) = single_part_keys(keys) else {
                continue;
            };
            let (Some(left), Some(right)) =
                (qualifier_of(&table.relation), qualifier_of(&join.relation))
            else {
                continue;
            };
            candidates.push(Candidate {
                from_index,
                join_index,
                left,
                right,
                keys,
            });
        }
    }
    candidates
}

/// The `USING` keys of a full or right join, or `None` for any other join.
///
/// The same two join types [`super::pg_using_join_merge`] merges, and for the same reason:
/// they are the two where the sides can disagree about the key, so they are the two where a
/// merged column and a raw one are different values.
fn full_or_right_using_keys(operator: &JoinOperator) -> Option<&Vec<ObjectName>> {
    match operator {
        JoinOperator::FullOuter(JoinConstraint::Using(keys))
        | JoinOperator::Right(JoinConstraint::Using(keys))
        | JoinOperator::RightOuter(JoinConstraint::Using(keys)) => Some(keys),
        _ => None,
    }
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

/// The name a client writes to qualify a column of this relation.
///
/// An alias where one is written, otherwise the relation's own name — but only when that
/// name is a single part. A schema-qualified relation is collapsed to one quoted catalog key
/// before planning ([`super::parser::collapse_schema_qualified_relations`]), and the
/// qualifier for `sales.orders` is neither that key nor the two-part name.
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

/// Whether this query block reaches one of `candidate`'s keys through an explicit
/// qualifier — the question that decides whether the join is respelled at all.
///
/// The block and not the whole statement: a nested query is a scope of its own, and the
/// `l` it names may not be this `l`. Descent stops at a subquery that redefines either
/// side's name, so a shadowed `l.id` does not respell a join it has nothing to do with;
/// below any other subquery the search continues, since a correlated `l.id` there is this
/// join's key.
fn qualifies_a_key(select: &Select, order_by: Option<&OrderBy>, candidate: &Candidate) -> bool {
    struct Scan<'a> {
        candidate: &'a Candidate,
        found: bool,
        /// How many query blocks below the one being judged the walk currently is.
        depth: usize,
        /// The depths at which a subquery redefined a side's name; non-empty means the
        /// walk is inside one and its expressions are not this block's.
        shadowed_at: Vec<usize>,
    }

    impl Visitor for Scan<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            self.depth += 1;
            if redefines_a_side(query, self.candidate) {
                self.shadowed_at.push(self.depth);
            }
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            if self.shadowed_at.last() == Some(&self.depth) {
                self.shadowed_at.pop();
            }
            self.depth -= 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if self.shadowed_at.is_empty() && is_qualified_key(expr, self.candidate) {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut scan = Scan {
        candidate,
        found: false,
        depth: 0,
        shadowed_at: Vec::new(),
    };
    let _ = select.visit(&mut scan);
    if !scan.found {
        // The `ORDER BY` of the query this select is the body of: `ORDER BY l.id` reads the
        // left side's own key too, and it is the clause a client writes it in most often.
        if let Some(order_by) = order_by {
            let _ = order_by.visit(&mut scan);
        }
    }
    scan.found
}

/// Whether `expr` is `<side>.<key>` for one of the candidate's two sides and keys.
fn is_qualified_key(expr: &Expr, candidate: &Candidate) -> bool {
    let Expr::CompoundIdentifier(parts) = expr else {
        return false;
    };
    let [qualifier, column] = parts.as_slice() else {
        return false;
    };
    (same_name(qualifier, &candidate.left) || same_name(qualifier, &candidate.right))
        && candidate.keys.iter().any(|key| same_name(column, key))
}

/// Whether a subquery gives either of the candidate's side names to a relation of its own,
/// in which case a qualified reference inside it is not about this join.
fn redefines_a_side(query: &Query, candidate: &Candidate) -> bool {
    if let Some(with) = &query.with
        && with
            .cte_tables
            .iter()
            .any(|cte| names_a_side(&cte.alias.name, candidate))
    {
        return true;
    }
    let SetExpr::Select(select) = &*query.body else {
        return false;
    };
    select.from.iter().any(|table| {
        std::iter::once(&table.relation)
            .chain(table.joins.iter().map(|join| &join.relation))
            .filter_map(qualifier_of)
            .any(|name| names_a_side(&name, candidate))
    })
}

/// Whether `name` is one of the candidate's two side names.
fn names_a_side(name: &Ident, candidate: &Candidate) -> bool {
    same_name(name, &candidate.left) || same_name(name, &candidate.right)
}

/// Whether two identifiers name the same thing, folding the way PostgreSQL folds: an
/// unquoted identifier is lowercased and a quoted one is taken as written.
fn same_name(left: &Ident, right: &Ident) -> bool {
    folded(left) == folded(right)
}

/// One identifier under PostgreSQL's folding rule.
fn folded(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    }
}

/// Refuse a block where two joins being respelled name the same key, since an unqualified
/// reference to it would then have two merged columns to be.
///
/// Reachable only through a comma-separated FROM — `FROM a FULL JOIN b USING (id),
/// c FULL JOIN d USING (id)` — where PostgreSQL reads the bare `id` as ambiguous too.
fn reject_a_shared_key_name(split: &[Candidate]) -> PgWireResult<()> {
    let mut seen: HashMap<String, ()> = HashMap::new();
    for candidate in split {
        for key in &candidate.keys {
            if seen.insert(folded(key), ()).is_some() {
                return Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    format!(
                        "a qualified reference to the USING key {key} is not supported when two \
                         joins in the same query block merge a key of that name, because the \
                         unqualified {key} then has two merged columns to be. Spell the joins \
                         with ON and write COALESCE where the merged value is wanted"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Refuse a block that reaches a key by qualifier *and* projects a wildcard covering the
/// same join.
///
/// The two want the statement spelled two different ways at once: a wildcard over a `USING`
/// join reports the key once and merged, which needs the `USING` the qualifier needs gone.
/// Expanding the wildcard here instead would mean knowing each side's column list, which is
/// a fact about the catalog and not about the statement.
fn reject_a_wildcard(select: &Select, split: &[Candidate]) -> PgWireResult<()> {
    for item in &select.projection {
        let covered = match item {
            // A bare `*` covers every relation in the FROM clause, both sides included.
            SelectItem::Wildcard(_) => true,
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                match name.0.as_slice() {
                    [part] => part.as_ident().is_some_and(|qualifier| {
                        split
                            .iter()
                            .any(|candidate| names_a_side(qualifier, candidate))
                    }),
                    _ => false,
                }
            }
            _ => false,
        };
        if covered {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "a wildcard is not supported beside a qualified reference to a USING key of the \
                 same full or right join: PostgreSQL reports the key once and merged for the \
                 wildcard and each side's own value for the qualifier, and one plan schema \
                 cannot hold both. Spell the join as ON <left>.<key> = <right>.<key> and write \
                 COALESCE(<left>.<key>, <right>.<key>) for the merged column"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Refuse a block that reports the merged key *and* reaches the same key per side under that
/// same name — the one shape whose PostgreSQL answer has two result columns of one name.
///
/// ```text
/// postgres=# SELECT id, l.id FROM l FULL JOIN r USING (id);
///   id | id          -- two columns, both called `id`
/// ```
///
/// A `DFSchema` cannot hold those two: an unqualified `id` beside a qualified `l.id` is
/// rejected as ambiguous, and two fields of the same name are rejected outright. So the
/// answer is not representable in *any* spelling of the query — the `ON` spelling a client
/// writes by hand is refused by DataFusion in the same words — and this is the residue of the
/// three-names-two-fields limit that no rewrite can take away.
///
/// Naming the per-side column something else makes it representable, and that spelling is
/// answered: `SELECT id, l.id AS l_id` is `id` and `l_id`, two names, three values. So the
/// refusal is worth making here rather than leaving to DataFusion, whose message is about a
/// schema the client never wrote and names no way out.
///
/// `ORDER BY l.id` beside an output `id` is the same collision reached from the other end: a
/// sort key is resolved against the schema its input reports, so the qualified column has to
/// join the merged one there. Every other clause is below the projection — `WHERE l.id > 2`
/// beside `SELECT id` is a filter on the join's own schema, and is answered.
fn reject_a_merged_key_beside_its_own_name(
    select: &Select,
    order_by: Option<&OrderBy>,
    split: &[Candidate],
) -> PgWireResult<()> {
    for candidate in split {
        for key in &candidate.keys {
            if !reports_the_merged_key(select, key) {
                continue;
            }
            let Some(side) = a_colliding_qualifier(select, order_by, candidate, key) else {
                continue;
            };
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "the merged USING key {key} and {side}.{key} are not supported in the same \
                     query block: PostgreSQL calls both result columns {key}, and one plan schema \
                     cannot hold two fields of that name. Give the per-side column a name of its \
                     own — SELECT {key}, {side}.{key} AS {side}_{key} — and refer to it by that \
                     name"
                ),
            ));
        }
    }
    Ok(())
}

/// Whether this select list reports the merged key under the key's own name, which is the
/// unqualified field the rewrite is about to put in the projection.
///
/// The two shapes that produce it: the bare key, and the bare key aliased to itself. A key
/// buried in a larger expression aliased to the key's name — `max(id) AS id` — is left to
/// DataFusion, which refuses it for the same reason; that cannot be a wrong answer, only a
/// less well explained refusal, and the shape is not one a client writes.
fn reports_the_merged_key(select: &Select, key: &Ident) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => same_name(ident, key),
        SelectItem::ExprWithAlias {
            expr: Expr::Identifier(ident),
            alias,
        } => same_name(ident, key) && same_name(alias, key),
        _ => false,
    })
}

/// The side of a `<side>.<key>` reference that would have to share a schema with the merged
/// key, or `None` where the two can be reported side by side.
fn a_colliding_qualifier(
    select: &Select,
    order_by: Option<&OrderBy>,
    candidate: &Candidate,
    key: &Ident,
) -> Option<Ident> {
    // In the select list only an *unaliased* `l.id`: that is the item DataFusion names after
    // the column itself, and an alias renames the field, which the merged `id` then sits
    // beside without clashing.
    let projected = select.projection.iter().find_map(|item| match item {
        SelectItem::UnnamedExpr(expr) => qualifier_of_key(expr, candidate, key),
        _ => None,
    });
    // In the `ORDER BY`, any spelling at all: an alias on the sort key would not keep the
    // column out of the schema the sort resolves against.
    projected.or_else(|| {
        let order_by = order_by?;
        struct Scan<'a> {
            candidate: &'a Candidate,
            key: &'a Ident,
            found: Option<Ident>,
        }

        impl Visitor for Scan<'_> {
            type Break = ();

            fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
                if let Some(side) = qualifier_of_key(expr, self.candidate, self.key) {
                    self.found = Some(side);
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
        }

        let mut scan = Scan {
            candidate,
            key,
            found: None,
        };
        let _ = order_by.visit(&mut scan);
        scan.found
    })
}

/// The side name of `expr` when it is `<side>.<key>` for this one key, or `None`.
fn qualifier_of_key(expr: &Expr, candidate: &Candidate, key: &Ident) -> Option<Ident> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    match parts.as_slice() {
        [side, name] if same_name(name, key) && names_a_side(side, candidate) => Some(side.clone()),
        _ => None,
    }
}

/// Replace every unqualified reference to a key of `candidate` in this block with the
/// `COALESCE` the `USING` clause promised, keeping the label a select item had.
///
/// The `ORDER BY` follows PostgreSQL's own resolution rule rather than this one: an
/// unqualified name there is matched against the **output** columns first, and only then
/// evaluated against the input. So `ORDER BY id` beside `SELECT l.id` sorts by the left
/// side's key — the column the select list called `id` — and is left alone, while beside
/// `SELECT l.v` there is no output `id` and the merged key is what PostgreSQL sorts by.
fn merge_unqualified_keys(
    select: &mut Select,
    order_by: Option<&mut OrderBy>,
    candidate: &Candidate,
) {
    // The select list first, so an item that *is* the bare key keeps `id` as its label
    // instead of being named after the `COALESCE` — PostgreSQL calls that column `id`.
    for item in &mut select.projection {
        let key = match &*item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => key_named(ident, &candidate.keys),
            _ => None,
        };
        let Some(key) = key else { continue };
        let SelectItem::UnnamedExpr(Expr::Identifier(alias)) = item else {
            // Unreachable: `key` is `Some` only for that shape.
            continue;
        };
        *item = SelectItem::ExprWithAlias {
            expr: merged(candidate, &key),
            alias: alias.clone(),
        };
    }

    let labels = output_labels(select);
    merge_in(select, candidate, &candidate.keys);
    if let Some(order_by) = order_by {
        // Only the keys the select list does not already report under their own name.
        let keys: Vec<Ident> = candidate
            .keys
            .iter()
            .filter(|key| !labels.contains(&folded(key)))
            .cloned()
            .collect();
        merge_in(order_by, candidate, &keys);
    }
}

/// Rewrite the unqualified references to `keys` in one node of this query block.
///
/// Generic over the node so the select and the enclosing `ORDER BY` share the walk; both
/// stop at a nested query, whose unqualified names resolve against its own relations first.
fn merge_in<N: VisitMut>(node: &mut N, candidate: &Candidate, keys: &[Ident]) {
    struct Merger<'a> {
        candidate: &'a Candidate,
        keys: &'a [Ident],
        /// How many query blocks below this one the walk is.
        depth: usize,
    }

    impl VisitorMut for Merger<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if self.depth == 0
                && let Expr::Identifier(ident) = &*expr
                && let Some(key) = key_named(ident, self.keys)
            {
                *expr = merged(self.candidate, &key);
            }
            ControlFlow::Continue(())
        }
    }

    if keys.is_empty() {
        return;
    }
    let _ = node.visit(&mut Merger {
        candidate,
        keys,
        depth: 0,
    });
}

/// The names this select list reports, for the forms whose name is a bare identifier.
///
/// A wildcard contributes none — and cannot, since its columns are a catalog fact — which is
/// harmless here because a wildcard beside a qualified key is refused before this is asked.
fn output_labels(select: &Select) -> Vec<String> {
    select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(folded(alias)),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(folded(ident)),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts.last().map(folded),
            _ => None,
        })
        .collect()
}

/// The listed spelling of the key `ident` names, or `None` if it names none of them.
fn key_named(ident: &Ident, keys: &[Ident]) -> Option<Ident> {
    keys.iter().find(|key| same_name(ident, key)).cloned()
}

/// `COALESCE(<left>.<key>, <right>.<key>)` — PostgreSQL's definition of the merged column,
/// and the same expression [`super::pg_using_join_merge`] builds on the plan.
fn merged(candidate: &Candidate, key: &Ident) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("coalesce")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: [&candidate.left, &candidate.right]
                .into_iter()
                .map(|side| FunctionArg::Unnamed(FunctionArgExpr::Expr(qualified(side, key))))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `<side>.<key>`.
fn qualified(side: &Ident, key: &Ident) -> Expr {
    Expr::CompoundIdentifier(vec![side.clone(), key.clone()])
}

/// Replace the join's `USING (k, …)` with `ON left.k = right.k AND …`, keeping its join
/// type.
///
/// The predicate is the definition of `USING`, so the rows do not move: what moves is that
/// the two key columns stay two columns, which is the whole of the repair.
fn convert_to_on(join: &mut Join, candidate: &Candidate) {
    let Some(predicate) = candidate
        .keys
        .iter()
        .map(|key| Expr::BinaryOp {
            left: Box::new(qualified(&candidate.left, key)),
            op: BinaryOperator::Eq,
            right: Box::new(qualified(&candidate.right, key)),
        })
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        })
    else {
        return;
    };
    let constraint = JoinConstraint::On(predicate);
    join.join_operator = match &join.join_operator {
        JoinOperator::FullOuter(_) => JoinOperator::FullOuter(constraint),
        JoinOperator::Right(_) => JoinOperator::Right(constraint),
        JoinOperator::RightOuter(_) => JoinOperator::RightOuter(constraint),
        // Unreachable: the candidate came from `full_or_right_using_keys`.
        other => other.clone(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;

    use crate::pgwire_handler::pg_using_join_merge::merge_using_join_keys;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    /// The two tables the merge module's tests use: keys 1–3 in common, `4` only on the
    /// left and `5` only on the right, so a full join has an unmatched row on each side.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register(
            &ctx,
            "l",
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
            "v",
            &["a", "b", "c", "d"],
        );
        register(
            &ctx,
            "r",
            &[1, 2, 3, 5],
            &[10, 20, 30, 50],
            "w",
            &["x", "y", "z", "q"],
        );
        ctx
    }

    fn register(
        ctx: &SessionContext,
        name: &str,
        ids: &[i32],
        ks: &[i32],
        payload_name: &str,
        payload: &[&str],
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("k", DataType::Int32, true),
            Field::new(payload_name, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(ks.to_vec())),
                Arc::new(StringArray::from(payload.to_vec())),
            ],
        )
        .unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    /// Parse one statement the way the read path does.
    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0)
    }

    /// The statement this rewrite hands the planner, back as SQL text.
    fn rewritten(sql: &str) -> Result<String, String> {
        let mut stmt = parse(sql);
        match split_qualified_using_keys(&mut stmt) {
            Ok(()) => Ok(stmt.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// The answer the whole read path gives `sql`: this rewrite, then the planner, then the
    /// plan-level merge — the two halves in the order [`super::super::parser`] runs them.
    ///
    /// Sorted, one string per row and `NULL` spelled out, because a join's row order is not
    /// defined.
    async fn answer(sql: &str) -> Vec<String> {
        let ctx = ctx();
        let mut stmt = parse(sql);
        split_qualified_using_keys(&mut stmt).expect("the statement is not refused");
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
            .await
            .expect("the rewritten statement plans");
        let plan = merge_using_join_keys(plan).unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let cells: Vec<String> = (0..batch.num_columns())
                    .map(|col| {
                        let column = batch.column(col);
                        if column.is_null(row) {
                            "NULL".to_string()
                        } else {
                            datafusion::arrow::util::display::array_value_to_string(column, row)
                                .unwrap()
                        }
                    })
                    .collect();
                rows.push(cells.join("|"));
            }
        }
        rows.sort();
        rows
    }

    // The gap: PostgreSQL answers each side's own key under an explicit qualifier — `4`
    // beside a NULL for the row only `l` has, a NULL beside `5` for the row only `r` has.
    #[tokio::test]
    async fn a_qualified_key_reports_each_side_own_value() {
        assert_eq!(
            answer("SELECT l.id, r.id FROM l FULL JOIN r USING (id)").await,
            ["1|1", "2|2", "3|3", "4|NULL", "NULL|5"]
        );
    }

    // A right join keeps only `r`'s rows, and `5` is the one whose left key PostgreSQL
    // reports as NULL.
    #[tokio::test]
    async fn a_right_join_qualified_key_reports_each_side_own_value() {
        assert_eq!(
            answer("SELECT l.id, r.id FROM l RIGHT JOIN r USING (id)").await,
            ["1|1", "2|2", "3|3", "NULL|5"]
        );
        assert_eq!(
            answer("SELECT l.id, r.id FROM l RIGHT OUTER JOIN r USING (id)").await,
            ["1|1", "2|2", "3|3", "NULL|5"]
        );
    }

    // All three of PostgreSQL's names in one select list, which is the property that says
    // the merged column and the two raw ones are three columns and not two: the unqualified
    // `id` is `COALESCE(l.id, r.id)` on the same row where `r.id` is NULL. The per-side
    // columns are named, because two result columns called `id` are what a plan schema cannot
    // hold — see `the_merged_key_beside_its_own_name_is_refused`.
    #[tokio::test]
    async fn the_merged_and_the_raw_keys_are_answered_side_by_side() {
        assert_eq!(
            answer("SELECT id, l.id AS l_id, r.id AS r_id FROM l FULL JOIN r USING (id)").await,
            ["1|1|1", "2|2|2", "3|3|3", "4|4|NULL", "5|NULL|5"]
        );
    }

    // And the label of that merged column is `id`, the name PostgreSQL gives it — not the
    // `coalesce(...)` the rewrite spells it with.
    #[tokio::test]
    async fn the_merged_column_keeps_the_key_as_its_label() {
        let ctx = ctx();
        let mut stmt = parse("SELECT id, l.id AS l_id FROM l FULL JOIN r USING (id)");
        split_qualified_using_keys(&mut stmt).unwrap();
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
            .await
            .unwrap();
        let names: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, ["id", "l_id"]);
    }

    // REFUSAL. The merged key and the same key per side under *one* name: PostgreSQL answers
    // two columns both called `id`, and a plan schema holds neither two fields of one name nor
    // an unqualified `id` beside a qualified `l.id`. Not representable in any spelling, so it
    // is refused with the spelling that is.
    #[tokio::test]
    async fn the_merged_key_beside_its_own_name_is_refused() {
        for sql in [
            "SELECT id, l.id FROM l FULL JOIN r USING (id)",
            "SELECT r.id, id FROM l FULL JOIN r USING (id)",
            "SELECT id AS id, l.id FROM l FULL JOIN r USING (id)",
            "SELECT id, k, l.k FROM l FULL JOIN r USING (id, k)",
            // From the other end: the sort resolves against the schema its input reports, so
            // the qualified column has to join the merged one there.
            "SELECT id, r.w FROM l FULL JOIN r USING (id) ORDER BY l.id",
        ] {
            let err = rewritten(sql).expect_err("refused");
            assert!(
                err.contains("are not supported in the same query block"),
                "`{sql}` was refused for some other reason: {err}"
            );
        }
        // The way out the message names, and the answer it gives.
        assert_eq!(
            answer("SELECT id, l.id AS l_id FROM l FULL JOIN r USING (id) ORDER BY l_id").await,
            ["1|1", "2|2", "3|3", "4|4", "5|NULL"]
        );
        // A qualifier below the projection is not in the schema the merged key is in, so a
        // predicate on one side beside the merged column is answered.
        assert_eq!(
            answer("SELECT id FROM l FULL JOIN r USING (id) WHERE l.id > 2").await,
            ["3", "4"]
        );
    }

    // A predicate is the other clause a qualifier is written in, and the one where the
    // divergence changed which *rows* came back: `WHERE l.id > 2` is the left side's own
    // key, so the row only `r` has is not one of them.
    #[tokio::test]
    async fn a_predicate_on_a_qualified_key_filters_on_that_side() {
        assert_eq!(
            answer("SELECT l.id FROM l FULL JOIN r USING (id) WHERE l.id > 2").await,
            ["3", "4"]
        );
        assert_eq!(
            answer("SELECT r.id FROM l FULL JOIN r USING (id) WHERE r.id > 2").await,
            ["3", "5"]
        );
    }

    // Every key of a multi-column `USING` is respelled, and each one independently.
    #[tokio::test]
    async fn a_multi_column_using_splits_every_key() {
        assert_eq!(
            answer("SELECT id, k, l.id AS l_id, r.k AS r_k FROM l FULL JOIN r USING (id, k)").await,
            [
                "1|10|1|10",
                "2|20|2|20",
                "3|30|3|30",
                "4|40|4|NULL",
                "5|50|NULL|50"
            ]
        );
    }

    // A statement that never qualifies a key is left byte-identical for
    // [`super::super::pg_using_join_merge`] to handle on the plan — including `SELECT *`,
    // whose shape only the `USING` constraint can keep.
    #[tokio::test]
    async fn a_statement_that_does_not_qualify_a_key_is_untouched() {
        for sql in [
            "SELECT id FROM l FULL JOIN r USING (id)",
            "SELECT * FROM l FULL JOIN r USING (id)",
            "SELECT max(id) FROM l FULL JOIN r USING (id)",
            "SELECT l.v, r.w FROM l FULL JOIN r USING (id)",
            // Not a key of this join, so `l.k` is not a qualified key reference.
            "SELECT l.k, r.k FROM l FULL JOIN r USING (id)",
        ] {
            assert_eq!(
                rewritten(sql).unwrap(),
                parse(sql).to_string(),
                "for `{sql}`"
            );
        }
        // And the merged answers the merge module asserts are unchanged by this being wired
        // in front of it.
        assert_eq!(
            answer("SELECT id FROM l FULL JOIN r USING (id)").await,
            ["1", "2", "3", "4", "5"]
        );
    }

    // The join types where a qualified key is already the value PostgreSQL reports: an
    // inner join's two keys are equal, and a left join's merged key *is* its left key. The
    // merge leaves them alone, so this must too.
    #[test]
    fn an_inner_or_left_join_is_left_alone() {
        for sql in [
            "SELECT l.id, r.id FROM l JOIN r USING (id)",
            "SELECT l.id, r.id FROM l INNER JOIN r USING (id)",
            "SELECT l.id, r.id FROM l LEFT JOIN r USING (id)",
            "SELECT l.id, r.id FROM l FULL JOIN r ON l.id = r.id",
        ] {
            assert_eq!(
                rewritten(sql).unwrap(),
                parse(sql).to_string(),
                "for `{sql}`"
            );
        }
    }

    // `NATURAL` is left alone: which columns two relations share is a fact about the
    // catalog and not about the statement, so there is no `ON` predicate to build — and no
    // way to know whether the column under a qualifier is a key at all. Pinned as the
    // recorded residue it is, with the answer it still gives.
    #[tokio::test]
    async fn a_natural_join_is_left_to_the_merge() {
        let sql = "SELECT l.id, r.id FROM l NATURAL FULL JOIN r";
        assert_eq!(rewritten(sql).unwrap(), parse(sql).to_string());
        // Still the merged value under both qualifiers, which is what the `USING` spelling
        // of the same join now answers correctly.
        assert_eq!(
            answer(sql).await,
            ["1|1", "2|2", "3|3", "4|4", "5|5"],
            "the NATURAL residue"
        );
    }

    // REFUSAL. A wildcard wants the key merged into one column and a qualifier wants the
    // two apart, and one plan schema cannot hold both — so the statement is refused rather
    // than answered with the qualifier wrong.
    #[test]
    fn a_wildcard_beside_a_qualified_key_is_refused() {
        for sql in [
            "SELECT *, l.id FROM l FULL JOIN r USING (id)",
            "SELECT l.*, r.id FROM l FULL JOIN r USING (id)",
            "SELECT r.*, l.id FROM l FULL JOIN r USING (id)",
        ] {
            let err = rewritten(sql).expect_err("refused");
            assert!(
                err.contains("wildcard is not supported beside a qualified reference"),
                "`{sql}` was refused for some other reason: {err}"
            );
        }
        // A wildcard over a *third* relation names no key of the join, so it is answered.
        assert!(
            rewritten("SELECT t.*, l.id FROM t, l FULL JOIN r USING (id)")
                .unwrap()
                .contains("ON l.id = r.id")
        );
    }

    // `ORDER BY` follows PostgreSQL's resolution rule: an output column of that name first,
    // and only then the input. Beside `SELECT l.id` there is one, so the sort is by the
    // left key — the NULL row sorts last either way, which is why the *rows* say which.
    #[tokio::test]
    async fn an_order_by_resolves_the_way_postgresql_resolves_it() {
        // No output column called `id`, so `ORDER BY id` is the merged key: the row only
        // `r` has sorts by `5` and comes last.
        assert_eq!(
            answer("SELECT l.v, r.w FROM l FULL JOIN r USING (id) ORDER BY id").await,
            ["NULL|q", "a|x", "b|y", "c|z", "d|NULL"]
        );
        // A qualified sort key is the side's own column, and needs no output column at all.
        assert_eq!(
            answer("SELECT r.id FROM l FULL JOIN r USING (id) ORDER BY r.id").await,
            ["1", "2", "3", "5", "NULL"]
        );
    }

    // A nested query is a scope of its own: the join in the derived table is respelled
    // because *that* block qualifies its key, and the outer block's `id` is the derived
    // table's column and nothing to do with the join.
    #[tokio::test]
    async fn a_join_inside_a_derived_table_is_respelled_on_its_own() {
        assert_eq!(
            answer(
                "SELECT id, raw FROM (SELECT id, l.id AS raw FROM l FULL JOIN r USING (id)) t \
                 WHERE id > 3"
            )
            .await,
            ["4|4", "5|NULL"]
        );
    }

    // A subquery that gives one of the side names to a relation of its own shadows it, so
    // the `l.id` inside it is not this join's key and does not respell it. Left `USING`, the
    // merge answers the outer `id` — which is what the statement asks for.
    #[test]
    fn a_shadowed_qualifier_does_not_respell_the_join() {
        let sql = "SELECT id FROM l FULL JOIN r USING (id) \
                   WHERE id IN (SELECT l.id FROM r AS l)";
        assert_eq!(rewritten(sql).unwrap(), parse(sql).to_string());
    }

    // A correlated qualifier one level down is *not* shadowed, so it does respell the join
    // the outer block owns — the reference is that join's left key and reads it raw.
    #[test]
    fn a_correlated_qualifier_respells_the_join_it_belongs_to() {
        let rewritten = rewritten(
            "SELECT id FROM l FULL JOIN r USING (id) \
             WHERE EXISTS (SELECT 1 FROM r AS z WHERE z.id = l.id)",
        )
        .unwrap();
        assert!(rewritten.contains("ON l.id = r.id"), "{rewritten}");
        assert!(
            rewritten.contains("coalesce(l.id, r.id) AS id"),
            "{rewritten}"
        );
    }

    // A `UNION` branch is a select list with no `Query` of its own, so it is reached
    // through the set-operation walk rather than by the visitor.
    #[tokio::test]
    async fn a_set_operation_branch_is_respelled() {
        assert_eq!(
            answer(
                "SELECT l.id FROM l FULL JOIN r USING (id) \
                 UNION SELECT 6"
            )
            .await,
            ["1", "2", "3", "4", "6", "NULL"]
        );
    }
}
