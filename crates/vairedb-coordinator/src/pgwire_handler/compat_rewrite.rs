//! The PostgreSQL-compatibility rewrite chain, applied by VaireDB itself for the
//! statements one of its rules would answer wrongly.
//!
//! The read path is normally fed by `datafusion-pg-catalog`'s
//! `PostgresCompatibilityParser`, which parses and then applies its twelve rewrite
//! rules in one call. Those rules are written for the introspection queries drivers
//! emit, and one of them — `RemoveSubqueryFromProjection` — is a lossy fallback: a
//! scalar subquery in a select list that it judges *correlated* is **replaced by
//! `NULL`**, so the statement is answered with a column of NULLs rather than refused.
//! Its own test of correlation is broad: an `$N` placeholder counts, and so does any
//! `t.col` whose `t` is not an *aliased* table in the subquery's own `FROM` — which
//! makes `(SELECT max(x) FROM t WHERE id = $1)` and `(SELECT count(*) FROM orders
//! WHERE orders.id = 1)` "correlated" though neither is.
//!
//! For a driver probe that fallback is the point: a NULL is a serviceable answer to a
//! question about the catalog, and it keeps `psql \d` working where DataFusion cannot
//! plan the subquery. For a client's own query over its own data it is the failure
//! mode this codebase refuses everywhere else — a plausible wrong answer with nothing
//! to tell the client the subquery never ran.
//!
//! So VaireDB takes the statement over. The rules are public
//! (`datafusion_pg_catalog::sql::rules`), so the same chain is applied here in the
//! same order minus that one rule, to a parse of the client's own text. What the
//! client then gets is DataFusion's own answer to the subquery — or, where DataFusion
//! cannot plan it, DataFusion's own error, which is loud.
//!
//! This is read-path only, and it is entered only for the statements
//! [`upstream_would_null_a_subquery`] identifies. Everything else keeps going through
//! the upstream parser untouched, so the compatibility surface is unchanged for the
//! queries the rules were written for.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use datafusion_pg_catalog::sql::rules::{
    AliasDuplicatedProjectionRewrite, CurrentUserVariableToSessionUserFunctionCall,
    FixArrayLiteral, FixVersionColumnName, PrependUnqualifiedPgTableName,
    RemoveSubqueryFromProjection, ResolveUnqualifiedIdentifier, RewriteArrayAnyAllOperation,
    RewritePgCatalogOperator, RewriteRegCastToSubquery, SqlStatementRewriteRule,
    StripCallableQualifier, StripCollate,
};

use crate::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Join, JoinConstraint,
    JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value,
    visit_expressions, visit_expressions_mut,
};

/// The pg-compat rewrite chain as VaireDB applies it: upstream's list, in upstream's
/// order, without `RemoveSubqueryFromProjection`.
///
/// Kept in upstream's order because the order is load-bearing — `RewriteRegCastToSubquery`
/// has to run after the identifier rules so the subquery it emits is already qualified.
/// `StripCollate` is kept: dropping a `COLLATE` is safe for the byte-order collations and
/// the rest are refused at parse time before this runs
/// ([`super::pg_operators::reject_unsupported_collation`]).
///
/// The one rule left out takes its own half of the work with it: upstream also stamps
/// `LIMIT 1` onto an *uncorrelated* projection subquery, which makes a subquery
/// returning several rows return the first one instead. PostgreSQL raises `21000` for
/// that, so losing the stamp on these statements moves the behavior towards the
/// contract, not away from it.
fn read_path_rules() -> &'static [Arc<dyn SqlStatementRewriteRule>] {
    static RULES: OnceLock<Vec<Arc<dyn SqlStatementRewriteRule>>> = OnceLock::new();
    RULES.get_or_init(|| {
        vec![
            Arc::new(AliasDuplicatedProjectionRewrite),
            Arc::new(ResolveUnqualifiedIdentifier),
            Arc::new(RewriteArrayAnyAllOperation),
            Arc::new(PrependUnqualifiedPgTableName),
            Arc::new(StripCallableQualifier),
            Arc::new(FixArrayLiteral),
            Arc::new(CurrentUserVariableToSessionUserFunctionCall),
            Arc::new(StripCollate),
            Arc::new(RewritePgCatalogOperator),
            Arc::new(RewriteRegCastToSubquery::new()),
            Arc::new(FixVersionColumnName),
        ]
    })
}

/// Apply [`read_path_rules`] to `stmt`, producing the AST the read path plans from.
///
/// The blacklist substitution upstream does at the token level is *not* applied, since
/// it happens inside `PostgresCompatibilityParser::parse` and there is no way to reach
/// it separately. That costs nothing here: the substitution table holds whole driver
/// probe queries, and a statement only reaches this function because it carries a
/// scalar subquery of the client's own.
pub(super) fn rewrite_for_read_path(stmt: Statement) -> Statement {
    read_path_rules()
        .iter()
        .fold(stmt, |stmt, rule| rule.rewrite(stmt))
}

/// Rewrite the two `ANY`/`ALL`-over-a-subquery forms upstream mangles into the `IN`
/// spellings that mean the same thing, reporting whether anything changed.
///
/// `RewriteArrayAnyAllOperation` exists to turn `x = ANY (array)` into an
/// `array_contains` call, which is right and is why the rule is in
/// [`read_path_rules`]. But it decides by the *operator* alone and never looks at what
/// the right-hand side is, so a subquery there is passed to `array_contains` as though
/// it were an array — upstream `rules.rs` rewrites `x = ANY (SELECT …)` to
/// `array_contains(<subquery>, x)` and `x <> ALL (SELECT …)` to its negation. Neither
/// is a call any planner can resolve, so everyday PostgreSQL fails on a shape the rule
/// was never aimed at.
///
/// Exactly two spellings are affected, because those are the only two the rule acts on:
/// its `= ALL` and `<> ANY` arms are `TODO`s that fall through, and every ordering
/// operator hits its `_ => {}`. Those reach DataFusion untouched and get DataFusion's
/// own answer or its own loud error, so they are left alone here rather than swept in.
///
/// Both affected forms have an exact equivalent that DataFusion plans natively, so this
/// is a *translation* and not a refusal: `x = ANY (q)` is `x IN (q)`, and
/// `x <> ALL (q)` is `x NOT IN (q)` — including the three-valued cases, since a NULL
/// among the candidates makes both spellings return NULL rather than false. Rewriting
/// before the rule runs also means the rule no longer matches: an `InSubquery` is not
/// an `AnyOp`.
pub(super) fn normalize_any_all_subqueries(stmt: &mut Statement) -> bool {
    let mut changed = false;
    let _ = visit_expressions_mut(stmt, |expr| {
        if let Some(rewritten) = any_all_subquery_as_in(expr) {
            *expr = rewritten;
            changed = true;
        }
        ControlFlow::<()>::Continue(())
    });
    changed
}

/// The `IN` form of `expr`, when `expr` is one of the two mangled shapes.
fn any_all_subquery_as_in(expr: &Expr) -> Option<Expr> {
    let (left, compare_op, right, negated) = match expr {
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => (left, compare_op, right, false),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => (left, compare_op, right, true),
        _ => return None,
    };

    // `= ANY` is membership and `<> ALL` is its complement. The other pairings are the
    // ones upstream leaves alone.
    let wanted = if negated {
        BinaryOperator::NotEq
    } else {
        BinaryOperator::Eq
    };
    if *compare_op != wanted {
        return None;
    }

    let Expr::Subquery(query) = right.as_ref() else {
        return None;
    };

    Some(Expr::InSubquery {
        expr: left.clone(),
        subquery: query.clone(),
        negated,
    })
}

/// Make every `NOT IN (subquery)` in a truth-valued position of `stmt` null-correct,
/// reporting whether anything changed.
///
/// PostgreSQL reads `x NOT IN (q)` as `x <> q1 AND x <> q2 AND …`, so it is
/// **NULL** — never true — as soon as `x` is NULL or any candidate is NULL, and it is
/// `true` for an *empty* `q` whatever `x` is. DataFusion plans the same expression as an
/// anti join, and only its hash join has the `null_aware` flag that reproduces that;
/// under Ballista the anti join is a sort-merge join, which treats a NULL as simply
/// unequal and keeps the row. The result is the failure mode this codebase refuses
/// everywhere else — a plausible wrong answer, with extra rows and nothing to say so.
///
/// Turning the hash join on is not the fix; see
/// [`crate::scheduler::scheduler`]'s `with_postgres_sql_options` for why it cannot be.
/// So the predicate is respelled here, in the AST, into one a *non*-null-aware anti join
/// answers correctly:
///
/// ```sql
///    (SELECT count(*) FROM (q) AS all_ (key)) = 0
/// OR (    (x) IS NOT NULL
///     AND (SELECT count(*) FROM (q) AS null_ (key) WHERE key IS NULL) = 0
///     AND NOT EXISTS (SELECT 1 FROM (q) AS eq_ (key) WHERE eq_.key = (x)))
/// ```
///
/// The three conjuncts are the three ways PostgreSQL declines to say true — a NULL `x`, a
/// NULL among the candidates, a candidate equal to `x` — and the leading disjunct is the
/// one case that overrides all of them: an empty `q` is `true` even for a NULL `x`.
///
/// **Only the last of the three may be a subquery predicate**, and that is the whole
/// reason this spelling is shaped so awkwardly. The obvious single-reference form folds
/// all three rules into one `NOT EXISTS` whose filter is a *disjunction*
/// (`(x) IS NULL OR key IS NULL OR key = (x)`) — which is correct in one process, and
/// **returns no rows at all on a cluster**: a semi or anti join with no equijoin key does
/// not survive Ballista's distributed planner, so the whole predicate silently becomes
/// false. That defect is wider than this rewrite (plain `WHERE EXISTS (SELECT 1 FROM t)`
/// has it too) and is recorded in `docs/specs/gap-analysis-join.md`; here it means the
/// only subquery shape available is an anti join on a **bare equality**, and the two NULL
/// tests have to be asked as uncorrelated `count(*)`s instead. Both are constant for the
/// whole statement, so they are evaluated once, not per row — `q` is named three times
/// but only scanned as one anti join plus two aggregates.
///
/// **`NOT EXISTS` is two-valued, so this is applied only where NULL and false are
/// indistinguishable** — the `WHERE`, `HAVING`, `QUALIFY` and `ON` clauses, reached
/// through `AND`, `OR` and parentheses only. All four keep a row exactly when the
/// predicate is *true*, and in a negation-free combination of `AND`/`OR` replacing a
/// NULL by false never changes whether the whole is true. Under a `NOT`, a `CASE`, or in
/// a select list the substitution *is* observable, so the expression is left alone
/// there: in a select list DataFusion cannot plan either spelling (`Physical plan does
/// not support logical expression InSubquery`), which is loud, and under a `NOT`
/// DataFusion's own simplifier already recovers PostgreSQL's answer.
///
/// Three further shapes are left alone. In each case leaving them means today's behavior
/// rather than a new error, which is the reason the guards are here rather than a
/// refusal:
///
/// * in `HAVING` and `QUALIFY` only, an `x` containing a **function call** — the
///   `HAVING MAX(k) NOT IN (q)` form. Every spelling above puts `x` inside a subquery, and
///   DataFusion cannot plan *any* correlated subquery over an aggregate: `NOT EXISTS (…
///   WHERE r.k = MAX(l.k))` fails with "Aggregate functions are not allowed in the WHERE
///   clause", as does the `count(*)` form. Rewriting would turn a wrong answer into a plan
///   error naming a clause the client did not write. The guard is *any* call rather than a
///   list of aggregate names — a list would rot, and a scalar call in a `HAVING`
///   predicate is rare enough that declining it costs almost nothing. `WHERE` and `ON`
///   need no such guard: an aggregate cannot appear there at all.
/// * a **correlated** `q`. A derived table may not see the outer row without `LATERAL`,
///   so wrapping a correlated `q` would turn a wrong answer into a resolution failure.
///   [`subquery_is_self_contained`] decides this conservatively.
/// * a `q` that is not a single-block `SELECT` of exactly one named column — a set
///   operation, a wildcard, or the row-wise `(a, b) NOT IN (q)` form.
pub(super) fn rewrite_not_in_subqueries(stmt: &mut Statement) -> bool {
    use crate::sqlparser::ast::VisitMut;

    let mut rewriter = NotInRewriter { rewrites: 0 };
    let _ = stmt.visit(&mut rewriter);
    rewriter.rewrites > 0
}

/// Visits every `SELECT` of a statement and rewrites the truth-valued clauses of each.
///
/// `pre_visit_select` rather than `pre_visit_query`, because a branch of a set operation
/// is a `Select` with no `Query` node of its own — `SELECT … UNION SELECT …` would
/// otherwise have only its first branch rewritten. The counter doubles as the source of
/// the derived table's name, so nested rewrites cannot shadow each other.
struct NotInRewriter {
    rewrites: usize,
}

impl crate::sqlparser::ast::VisitorMut for NotInRewriter {
    type Break = ();

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        if let Some(predicate) = &mut select.selection {
            self.rewrite_truth_valued(predicate, Grouped::No);
        }
        // `HAVING` and `QUALIFY` are the two truth-valued clauses whose predicate may be
        // written over an aggregate or a window function, which the rewrite cannot carry.
        for predicate in [&mut select.having, &mut select.qualify]
            .into_iter()
            .flatten()
        {
            self.rewrite_truth_valued(predicate, Grouped::Yes);
        }
        for table in &mut select.from {
            for join in &mut table.joins {
                if let Some(JoinConstraint::On(predicate)) = join_constraint_mut(join) {
                    self.rewrite_truth_valued(predicate, Grouped::No);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// Whether the clause being rewritten is one an aggregate may appear in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Grouped {
    /// `HAVING` or `QUALIFY`.
    Yes,
    /// `WHERE` or a join's `ON`.
    No,
}

impl NotInRewriter {
    /// Rewrite the `NOT IN (subquery)` nodes of `predicate` that sit in a position whose
    /// value is only ever tested for truth — itself, or either side of an `AND`/`OR`.
    ///
    /// Descending through `Expr::Nested` as well means the parentheses a client writes
    /// do not decide whether the fix applies.
    fn rewrite_truth_valued(&mut self, predicate: &mut Expr, grouped: Grouped) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And | BinaryOperator::Or,
                right,
            } => {
                self.rewrite_truth_valued(left, grouped);
                self.rewrite_truth_valued(right, grouped);
            }
            Expr::Nested(inner) => self.rewrite_truth_valued(inner, grouped),
            Expr::InSubquery {
                expr,
                subquery,
                negated: true,
            } => {
                if let Some(rewritten) = null_aware_not_in(expr, subquery, self.rewrites, grouped) {
                    *predicate = rewritten;
                    self.rewrites += 1;
                }
            }
            _ => {}
        }
    }
}

/// The `ON` constraint of `join`, for the join operators that can carry one.
fn join_constraint_mut(join: &mut Join) -> Option<&mut JoinConstraint> {
    match &mut join.join_operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => Some(constraint),
        _ => None,
    }
}

/// The null-aware spelling of `expr NOT IN (subquery)`, or `None` for the shapes
/// [`rewrite_not_in_subqueries`] declines to touch.
///
/// Built by rendering the two operands back to SQL and parsing the result, rather than
/// by assembling the `Select` node by hand: sqlparser's `Select` has some thirty fields
/// and no `Default`, so a hand-built node is a maintenance liability at every version
/// bump, and an operand whose `Display` does not round-trip returns `None` here — which
/// leaves the statement as the client wrote it instead of corrupting it.
fn null_aware_not_in(expr: &Expr, subquery: &Query, nth: usize, grouped: Grouped) -> Option<Expr> {
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    if !projects_one_named_column(subquery) || !subquery_is_self_contained(subquery) {
        return None;
    }
    // A row-wise `(a, b) NOT IN (q)` cannot be compared against a one-column derived
    // table, and the multi-column subquery it needs is not a shape DataFusion plans.
    if matches!(expr, Expr::Tuple(_)) {
        return None;
    }
    // `HAVING MAX(k) NOT IN (q)`: `expr` would move inside a subquery, and no correlated
    // subquery over an aggregate can be planned. See this function's caller's doc.
    if grouped == Grouped::Yes && holds_a_function_call(expr) {
        return None;
    }

    let rel = format!("vaire_notin_{nth}");
    let key = format!("{rel}_key");
    let sql = format!(
        // Parenthesized as a whole: the result is an `OR`, and it replaces a leaf that may
        // sit under an `AND`. The AST keeps the grouping either way, but a rendered
        // statement would not.
        "SELECT ((SELECT count(*) FROM ({subquery}) AS {rel}_all ({key})) = 0 \
         OR (({expr}) IS NOT NULL \
         AND (SELECT count(*) FROM ({subquery}) AS {rel}_null ({key}) WHERE {key} IS NULL) = 0 \
         AND NOT EXISTS (SELECT 1 FROM ({subquery}) AS {rel} ({key}) \
         WHERE {rel}.{key} = ({expr}))))"
    );
    let mut statements = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&sql)
        .ok()?
        .parse_statements()
        .ok()?;
    let Statement::Query(query) = statements.pop()? else {
        return None;
    };
    let SetExpr::Select(select) = *query.body else {
        return None;
    };
    match select.projection.into_iter().next()? {
        SelectItem::UnnamedExpr(expr) => Some(expr),
        _ => None,
    }
}

/// Whether `query` is a single-block `SELECT` of exactly one column that can be given a
/// name — the only shape the one-column derived table can wrap.
fn projects_one_named_column(query: &Query) -> bool {
    let SetExpr::Select(select) = &*query.body else {
        return false;
    };
    matches!(
        select.projection.as_slice(),
        [SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }]
    )
}

/// Whether `expr` calls a function anywhere inside it.
///
/// Used only for a `HAVING` or `QUALIFY` predicate, where a call is almost always an
/// aggregate or a window function and the rewrite cannot carry either; see
/// [`rewrite_not_in_subqueries`]. Deliberately blunter than "is an aggregate": the name
/// list that question needs would have to track two engines' function registries, and
/// declining a scalar call in a `HAVING` leaves it exactly as the client wrote it.
fn holds_a_function_call(expr: &Expr) -> bool {
    use crate::sqlparser::ast::visit_expressions;

    visit_expressions(expr, |node| {
        if matches!(node, Expr::Function(_)) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

/// Whether every qualified column reference in `query` names a relation `query` itself
/// declares — that is, whether `query` can be read without the outer row.
///
/// Conservative in the direction that keeps a statement working: an unrecognized
/// `FROM` item, or a qualifier that is not obviously local, both answer `false` and
/// leave the `NOT IN` alone. An *unqualified* outer reference cannot be told from a
/// local column without resolving names against the catalog, so it is treated as local;
/// where that is wrong the rewrite fails to resolve a column and the client is told,
/// which is the loud direction.
fn subquery_is_self_contained(query: &Query) -> bool {
    use crate::sqlparser::ast::{Visit, Visitor};

    #[derive(Default)]
    struct Declared {
        names: HashSet<String>,
        unrecognized: bool,
    }

    impl Visitor for Declared {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    self.names.insert(fold(&cte.alias.name));
                }
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match factor {
                TableFactor::Table { name, alias, .. } => match alias {
                    Some(alias) => self.names.insert(fold(&alias.name)),
                    None => self
                        .names
                        .insert(fold(&name.0[name.0.len() - 1].to_string())),
                },
                TableFactor::Derived { alias, .. } | TableFactor::NestedJoin { alias, .. } => {
                    match alias {
                        Some(alias) => self.names.insert(fold(&alias.name)),
                        // An unaliased derived table has no name to qualify with, so it
                        // declares nothing — the join it sits in is still recognized.
                        None => true,
                    }
                }
                _ => {
                    self.unrecognized = true;
                    true
                }
            };
            ControlFlow::Continue(())
        }
    }

    fn fold(ident: &impl ToString) -> String {
        ident.to_string().to_ascii_lowercase()
    }

    let mut declared = Declared::default();
    let _ = query.visit(&mut declared);
    if declared.unrecognized {
        return false;
    }

    let mut local = true;
    let _ = visit_expressions(query, |expr| {
        if let Expr::CompoundIdentifier(parts) = expr
            && let Some(qualifier) = parts.first()
            && !declared.names.contains(&fold(&qualifier.value))
        {
            local = false;
        }
        ControlFlow::<()>::Continue(())
    });
    local
}

/// Whether `stmt` — a *compat* AST — carries the `array_contains` call over a subquery
/// that `RewriteArrayAnyAllOperation` leaves behind.
///
/// Used as the cheap guard on the parse path, in the same role as
/// [`projects_a_bare_null`]: it decides whether the client's text is worth parsing a
/// second time, not whether anything is wrong. A client that writes
/// `array_contains((SELECT arr FROM t), 1)` itself matches too and pays one extra parse,
/// because the decision is then taken by [`mentions_any_all_subquery`] against that
/// parse — where its statement has no `ANY`/`ALL` at all and is left alone.
pub(super) fn holds_a_mangled_any_all(stmt: &Statement) -> bool {
    let mut found = false;
    let _ = visit_expressions(stmt, |expr| {
        if let Expr::Function(func) = expr
            && func.name.to_string().eq_ignore_ascii_case("array_contains")
            && let FunctionArguments::List(args) = &func.args
            && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(_)))) =
                args.args.first()
        {
            found = true;
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// Whether `stmt` carries one of the shapes [`normalize_any_all_subqueries`] fixes.
///
/// Separate from the rewrite so the caller can decide whether to take the statement
/// over before cloning it.
pub(super) fn mentions_any_all_subquery(stmt: &Statement) -> bool {
    let mut found = false;
    let _ = visit_expressions(stmt, |expr| {
        if any_all_subquery_as_in(expr).is_some() {
            found = true;
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// Whether the upstream chain would replace a projection subquery of `stmt` with
/// `NULL` — that is, whether the compat AST of this statement is missing an answer
/// rather than merely spelling one differently.
///
/// Decided by running upstream's own rule and counting subqueries, rather than by
/// reimplementing its notion of correlation: the definition then cannot drift from the
/// version in `Cargo.lock`. The rule's other effect (stamping `LIMIT 1`) leaves the
/// count alone, so a drop in the count means a subquery was folded away.
pub(super) fn upstream_would_null_a_subquery(stmt: &Statement) -> bool {
    let folded = RemoveSubqueryFromProjection.rewrite(stmt.clone());
    subquery_count(&folded) < subquery_count(stmt)
}

/// Count the `Expr::Subquery` nodes anywhere in `stmt`.
fn subquery_count(stmt: &Statement) -> usize {
    let mut count = 0;
    let _ = visit_expressions(stmt, |expr| {
        if matches!(expr, Expr::Subquery(_)) {
            count += 1;
        }
        ControlFlow::<()>::Continue(())
    });
    count
}

/// Whether any SELECT in `stmt` projects a bare `NULL` — the shape
/// `RemoveSubqueryFromProjection` leaves behind when it folds a subquery away.
///
/// Used as the cheap guard on the parse path: it decides whether the client's text is
/// worth parsing a second time, not whether anything is wrong. A client that writes
/// `SELECT NULL` matches too and pays one extra parse, because the decision is then
/// taken by [`upstream_would_null_a_subquery`] against that parse.
pub(super) fn projects_a_bare_null(stmt: &Statement) -> bool {
    let mut found = false;
    visit_queries(stmt, &mut |query| {
        if let SetExpr::Select(select) = &*query.body {
            for item in &select.projection {
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) => expr,
                    SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => continue,
                };
                if let Expr::Value(v) = expr
                    && matches!(v.value, Value::Null)
                {
                    found = true;
                }
            }
        }
    });
    found
}

/// Call `f` on every `Query` in `stmt`, outer first.
///
/// sqlparser's visitor is derived over the whole AST, so this reaches a subquery in a
/// projection, a CTE body and a derived table alike — which is where the folded
/// projections can be.
fn visit_queries(stmt: &Statement, f: &mut impl FnMut(&Query)) {
    use crate::sqlparser::ast::{Visit, Visitor};

    struct QueryVisitor<'a, F: FnMut(&Query)>(&'a mut F);

    impl<F: FnMut(&Query)> Visitor for QueryVisitor<'_, F> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            (self.0)(query);
            ControlFlow::Continue(())
        }
    }

    let _ = stmt.visit(&mut QueryVisitor(f));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;
    use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0)
    }

    /// The premise of the whole module: fed through the compatibility parser, a select
    /// list subquery that mentions the outer row comes back as a `NULL` literal. If
    /// this ever fails because upstream stopped folding, the takeover below is no
    /// longer needed.
    #[test]
    fn the_compatibility_parser_folds_a_correlated_projection_subquery_to_null() {
        let sql =
            "SELECT o.id, (SELECT max(l.amount) FROM lines l WHERE l.oid = o.id) FROM orders o";
        let compat = PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("the statement parses")
            .remove(0);

        assert_eq!(
            subquery_count(&compat),
            0,
            "upstream folds the subquery away: {compat}"
        );
        assert!(projects_a_bare_null(&compat));
    }

    #[test]
    fn recognizes_the_statements_upstream_would_fold() {
        // Genuinely correlated — the outer row is referenced.
        assert!(upstream_would_null_a_subquery(&parse(
            "SELECT (SELECT max(l.amount) FROM lines l WHERE l.oid = o.id) FROM orders o"
        )));
        // Not correlated at all, but upstream folds it too: an `$N` placeholder counts
        // as correlation, and so does a qualified name whose table has no alias.
        assert!(upstream_would_null_a_subquery(&parse(
            "SELECT (SELECT max(amount) FROM lines WHERE oid = $1) FROM orders"
        )));
        assert!(upstream_would_null_a_subquery(&parse(
            "SELECT (SELECT count(*) FROM lines WHERE lines.oid = 1) FROM orders"
        )));
    }

    #[test]
    fn leaves_the_statements_upstream_only_stamps() {
        // Upstream adds `LIMIT 1` here rather than folding, so the count is unchanged
        // and VaireDB does not take the statement over.
        assert!(!upstream_would_null_a_subquery(&parse(
            "SELECT (SELECT max(amount) FROM lines) FROM orders"
        )));
        assert!(!upstream_would_null_a_subquery(&parse(
            "SELECT id FROM orders WHERE id IN (SELECT oid FROM lines)"
        )));
        assert!(!upstream_would_null_a_subquery(&parse("SELECT 1")));
    }

    #[test]
    fn keeps_the_subquery_the_upstream_chain_would_delete() {
        let stmt = parse(
            "SELECT o.id, (SELECT max(l.amount) FROM lines l WHERE l.oid = o.id) FROM orders o",
        );

        let rewritten = rewrite_for_read_path(stmt);

        assert_eq!(
            subquery_count(&rewritten),
            1,
            "the client's subquery survives: {rewritten}"
        );
        assert!(!projects_a_bare_null(&rewritten));
    }

    /// The chain is upstream's minus one rule, so the rest of the compatibility
    /// surface has to still be there. `= ANY(ARRAY[...])` is the load-bearing one for
    /// user queries: drivers send it for every "id in list" parameter binding.
    #[test]
    fn still_applies_the_rest_of_the_compatibility_chain() {
        let stmt = parse(
            "SELECT (SELECT max(l.amount) FROM lines l WHERE l.oid = o.id) \
             FROM orders o WHERE o.id = ANY(ARRAY[1, 2])",
        );

        let rewritten = rewrite_for_read_path(stmt).to_string();

        assert!(
            rewritten.contains("array_contains(ARRAY[1, 2], o.id)"),
            "`= ANY(array)` is rewritten to the DataFusion function: {rewritten}"
        );
    }

    /// The premise of [`normalize_any_all_subqueries`]: fed a subquery, upstream's rule
    /// emits an `array_contains` call over it. If this ever fails because upstream
    /// learned to check the right-hand side, the normalization is no longer needed.
    #[test]
    fn the_compatibility_parser_hands_a_subquery_to_array_contains() {
        let compat = PostgresCompatibilityParser::new()
            .parse("SELECT id FROM orders WHERE id = ANY (SELECT oid FROM lines)")
            .expect("the statement parses")
            .remove(0)
            .to_string();

        assert!(
            compat.contains("array_contains((SELECT oid FROM lines), id)"),
            "upstream passes the subquery to array_contains: {compat}"
        );
    }

    #[test]
    fn normalizes_the_two_mangled_any_all_subquery_forms() {
        for (sql, expected) in [
            (
                "SELECT id FROM orders WHERE id = ANY (SELECT oid FROM lines)",
                "id IN (SELECT oid FROM lines)",
            ),
            (
                "SELECT id FROM orders WHERE id <> ALL (SELECT oid FROM lines)",
                "id NOT IN (SELECT oid FROM lines)",
            ),
        ] {
            let mut stmt = parse(sql);
            assert!(mentions_any_all_subquery(&stmt), "not detected: {sql}");
            assert!(normalize_any_all_subqueries(&mut stmt));

            let normalized = stmt.to_string();
            assert!(
                normalized.contains(expected),
                "expected `{expected}` in: {normalized}"
            );

            // And the rewritten AST is out of the rule's reach: an `InSubquery` is not
            // an `AnyOp`, so running the chain over it leaves the membership test alone.
            let rewritten = rewrite_for_read_path(stmt).to_string();
            assert!(
                rewritten.contains(expected) && !rewritten.contains("array_contains"),
                "the chain re-mangled it: {rewritten}"
            );
        }
    }

    /// The guard has to fire on the compat AST for both mangled spellings, or the parse
    /// path returns early and the normalization never runs.
    #[test]
    fn the_guard_sees_both_mangled_spellings() {
        for sql in [
            "SELECT id FROM orders WHERE id = ANY (SELECT oid FROM lines)",
            "SELECT id FROM orders WHERE id <> ALL (SELECT oid FROM lines)",
        ] {
            let compat = PostgresCompatibilityParser::new()
                .parse(sql)
                .expect("the statement parses")
                .remove(0);
            assert!(holds_a_mangled_any_all(&compat), "guard missed: {sql}");
        }

        // And it does not fire on the array forms, which the rule handles correctly:
        // their first argument is an array expression, not a subquery.
        let compat = PostgresCompatibilityParser::new()
            .parse("SELECT id FROM orders WHERE id = ANY(ARRAY[1, 2])")
            .expect("the statement parses")
            .remove(0);
        assert!(!holds_a_mangled_any_all(&compat));
    }

    /// The array forms are what the rule is *for*, and the ones the normalization must
    /// not touch — a driver binding an "id in list" parameter sends `= ANY(ARRAY[…])`.
    #[test]
    fn leaves_the_array_forms_to_the_upstream_rule() {
        let mut stmt = parse("SELECT id FROM orders WHERE id = ANY(ARRAY[1, 2])");
        assert!(!mentions_any_all_subquery(&stmt));
        assert!(!normalize_any_all_subqueries(&mut stmt));

        let rewritten = rewrite_for_read_path(stmt).to_string();
        assert!(
            rewritten.contains("array_contains(ARRAY[1, 2], id)"),
            "the upstream rewrite still happens: {rewritten}"
        );
    }

    /// The operator pairings upstream leaves alone stay alone here too: they reach
    /// DataFusion untouched and get its own answer or its own loud error, and sweeping
    /// them in would change statements that are not part of the defect.
    #[test]
    fn leaves_the_operators_upstream_does_not_act_on() {
        for sql in [
            "SELECT id FROM orders WHERE id > ANY (SELECT oid FROM lines)",
            "SELECT id FROM orders WHERE id <= ALL (SELECT oid FROM lines)",
            "SELECT id FROM orders WHERE id = ALL (SELECT oid FROM lines)",
            "SELECT id FROM orders WHERE id <> ANY (SELECT oid FROM lines)",
        ] {
            let mut stmt = parse(sql);
            assert!(!mentions_any_all_subquery(&stmt), "swept in: {sql}");
            assert!(!normalize_any_all_subqueries(&mut stmt), "rewritten: {sql}");
        }
    }

    /// The visitor is derived over the whole AST, so the shape is fixed wherever it
    /// appears — including inside a projection subquery, which is the statement that
    /// also triggers the takeover above.
    #[test]
    fn normalizes_a_nested_occurrence() {
        let mut stmt = parse(
            "SELECT (SELECT count(*) FROM lines l WHERE l.oid = o.id) FROM orders o \
             WHERE o.id = ANY (SELECT oid FROM shipped)",
        );

        assert!(mentions_any_all_subquery(&stmt));
        assert!(normalize_any_all_subqueries(&mut stmt));

        // One `Expr::Subquery` left: the projection's. The one behind the `= ANY` moved
        // into an `InSubquery`, which is a `Query` and not an `Expr::Subquery`.
        assert_eq!(subquery_count(&stmt), 1);

        let normalized = stmt.to_string();
        assert!(
            normalized.contains("o.id IN (SELECT oid FROM shipped)"),
            "{normalized}"
        );
        assert!(
            normalized.contains("(SELECT count(*) FROM lines l WHERE l.oid = o.id)"),
            "the projection subquery is untouched: {normalized}"
        );
    }

    /// A bare `NULL` a client wrote is not evidence of anything, and the guard says so
    /// only to the extent of asking for a second parse.
    #[test]
    fn the_guard_does_not_decide_on_its_own() {
        let stmt = parse("SELECT NULL AS nothing FROM orders");
        assert!(projects_a_bare_null(&stmt));
        assert!(!upstream_would_null_a_subquery(&stmt));
    }

    /// `sql` after [`rewrite_not_in_subqueries`], asserting it was rewritten at all.
    fn rewritten(sql: &str) -> String {
        let mut stmt = parse(sql);
        assert!(
            rewrite_not_in_subqueries(&mut stmt),
            "`{sql}` holds a `NOT IN (subquery)` in a truth-valued position"
        );
        stmt.to_string()
    }

    /// Assert `sql` comes back exactly as written.
    fn untouched(sql: &str) {
        let mut stmt = parse(sql);
        let before = stmt.to_string();
        assert!(
            !rewrite_not_in_subqueries(&mut stmt),
            "`{sql}` is rewritten"
        );
        assert_eq!(stmt.to_string(), before);
    }

    #[test]
    fn respells_a_not_in_subquery_as_a_null_aware_anti_join() {
        let sql = rewritten("SELECT k FROM l WHERE k NOT IN (SELECT k FROM r)");

        assert_eq!(
            sql,
            "SELECT k FROM l WHERE (\
             (SELECT count(*) FROM (SELECT k FROM r) AS vaire_notin_0_all (vaire_notin_0_key)) = 0 \
             OR ((k) IS NOT NULL \
             AND (SELECT count(*) FROM (SELECT k FROM r) AS vaire_notin_0_null \
             (vaire_notin_0_key) WHERE vaire_notin_0_key IS NULL) = 0 \
             AND NOT EXISTS (SELECT 1 FROM (SELECT k FROM r) AS vaire_notin_0 \
             (vaire_notin_0_key) WHERE vaire_notin_0.vaire_notin_0_key = (k))))"
        );
    }

    /// The positive `IN` needs no help: DataFusion's semi join and PostgreSQL agree that
    /// a NULL is simply not a match.
    #[test]
    fn leaves_the_positive_in_subquery_alone() {
        untouched("SELECT k FROM l WHERE k IN (SELECT k FROM r)");
    }

    /// The list form is evaluated as an expression, not as a join, and is already
    /// three-valued.
    #[test]
    fn leaves_the_list_form_alone() {
        untouched("SELECT k FROM l WHERE k NOT IN (20, NULL)");
    }

    #[test]
    fn reaches_every_truth_valued_clause() {
        for sql in [
            "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r)",
            "SELECT k FROM l GROUP BY k HAVING k NOT IN (SELECT k FROM r)",
            "SELECT k FROM l QUALIFY row_number() OVER () > 1 \
             AND k NOT IN (SELECT k FROM r)",
            "SELECT k FROM l JOIN m ON l.k = m.k AND l.k NOT IN (SELECT k FROM r)",
            "SELECT k FROM l LEFT JOIN m ON l.k NOT IN (SELECT k FROM r)",
        ] {
            assert!(rewritten(sql).contains("NOT EXISTS"), "{sql}");
        }
    }

    /// The one clause-dependent guard: in a `HAVING` or `QUALIFY` the left side may be an
    /// aggregate, and no correlated subquery over an aggregate can be planned — so the
    /// predicate is left as written rather than turned into a plan error. A grouped
    /// *column* on the left is still rewritten (see the test above).
    #[test]
    fn leaves_an_aggregate_left_side_alone_in_a_grouped_clause() {
        untouched("SELECT k FROM l GROUP BY k HAVING max(k) NOT IN (SELECT k FROM r)");
        untouched(
            "SELECT k FROM l GROUP BY k \
             HAVING k > 1 AND count(*) NOT IN (SELECT k FROM r)",
        );
        // The same call in a `WHERE` is not an aggregate and is rewritten: an aggregate
        // cannot appear there at all, so the guard would only cost coverage.
        assert!(
            rewritten("SELECT k FROM l WHERE abs(k) NOT IN (SELECT k FROM r)")
                .contains("NOT EXISTS")
        );
    }

    /// Parentheses and boolean connectives are transparent, since `AND`/`OR` cannot tell
    /// a NULL from a false in a clause that keeps a row only when it is true.
    #[test]
    fn reaches_through_and_or_and_parentheses() {
        let sql = rewritten(
            "SELECT k FROM l WHERE (k > 5 AND (k NOT IN (SELECT k FROM r))) \
             OR k NOT IN (SELECT k FROM m)",
        );

        // Both occurrences, each with its own derived table so neither shadows the other.
        assert!(sql.contains("vaire_notin_0 (vaire_notin_0_key)"), "{sql}");
        assert!(sql.contains("vaire_notin_1 (vaire_notin_1_key)"), "{sql}");
    }

    /// Under a `NOT` the two-valued answer is observable — `NOT NULL` is NULL where
    /// `NOT false` is true — so the predicate is left as the client wrote it.
    #[test]
    fn stops_at_a_negation() {
        untouched("SELECT k FROM l WHERE NOT (k NOT IN (SELECT k FROM r))");
        untouched("SELECT k FROM l WHERE (k NOT IN (SELECT k FROM r)) IS NOT TRUE");
        untouched(
            "SELECT k FROM l WHERE CASE WHEN k NOT IN (SELECT k FROM r) THEN 1 ELSE 2 END = 1",
        );
    }

    /// A select list is not a truth-valued position either. DataFusion plans neither
    /// spelling there, and the error it raises for the client's own is the loud one.
    #[test]
    fn stops_at_a_select_list() {
        untouched("SELECT k NOT IN (SELECT k FROM r) FROM l");
    }

    /// A branch of a set operation is a `Select` with no `Query` node of its own, which
    /// is why the visitor hooks `pre_visit_select`.
    #[test]
    fn reaches_both_branches_of_a_set_operation() {
        let sql = rewritten(
            "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r) \
             UNION ALL SELECT k FROM m WHERE k NOT IN (SELECT k FROM r)",
        );

        assert_eq!(sql.matches("NOT EXISTS").count(), 2, "{sql}");
    }

    /// A `NOT IN` inside another subquery is in a truth-valued position of *that*
    /// query, so it is rewritten on its own terms.
    #[test]
    fn reaches_a_nested_query() {
        let sql = rewritten(
            "SELECT k FROM l WHERE k IN \
             (SELECT k FROM m WHERE k NOT IN (SELECT k FROM r))",
        );

        assert_eq!(sql.matches("NOT EXISTS").count(), 1, "{sql}");
    }

    /// A derived table cannot see the outer row without `LATERAL`, so a correlated
    /// subquery keeps today's answer rather than becoming a resolution failure.
    #[test]
    fn leaves_a_correlated_subquery_alone() {
        untouched("SELECT k FROM l WHERE k NOT IN (SELECT k FROM r WHERE r.g = l.g)");
        // A qualifier the subquery declares itself is not a correlation.
        assert!(
            rewritten("SELECT k FROM l WHERE k NOT IN (SELECT r.k FROM r WHERE r.k > 0)")
                .contains("NOT EXISTS")
        );
        // Including one declared by an alias, or by a CTE of the subquery's own.
        assert!(
            rewritten("SELECT k FROM l WHERE k NOT IN (SELECT x.k FROM r AS x)")
                .contains("NOT EXISTS")
        );
        assert!(
            rewritten(
                "SELECT k FROM l WHERE k NOT IN \
                 (WITH c AS (SELECT k FROM r) SELECT c.k FROM c)"
            )
            .contains("NOT EXISTS")
        );
    }

    /// The shapes the one-column derived table cannot wrap. Each is left alone, so each
    /// keeps whatever DataFusion does with it today.
    #[test]
    fn leaves_the_shapes_the_wrapper_cannot_carry_alone() {
        // Row-wise.
        untouched("SELECT k FROM l WHERE (k, v) NOT IN (SELECT k, v FROM r)");
        // A set operation, which has no single `Select` body to name a column of.
        untouched("SELECT k FROM l WHERE k NOT IN (SELECT k FROM r UNION SELECT k FROM m)");
        // A wildcard, which has no column count known here.
        untouched("SELECT k FROM l WHERE k NOT IN (SELECT * FROM r)");
    }

    /// The rewrite leaves nothing behind for a second pass to find, which is what makes
    /// it safe to run at every read-path exit of the parser.
    #[test]
    fn is_idempotent() {
        let mut stmt = parse("SELECT k FROM l WHERE k NOT IN (SELECT k FROM r)");
        assert!(rewrite_not_in_subqueries(&mut stmt));
        let once = stmt.to_string();
        assert!(!rewrite_not_in_subqueries(&mut stmt));
        assert_eq!(stmt.to_string(), once);
    }

    /// `l(k) = 10, 20, 30, NULL` and `r(k) = 20, 99, 50`, split across partitions so the
    /// anti join is repartitioned the way a distributed read's is.
    fn keys_context() -> datafusion::prelude::SessionContext {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionConfig, SessionContext};

        // Ballista's own value, which is what the cluster plans with: the anti join is a
        // sort-merge join and is *not* null-aware. The rewrite has to be correct here.
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().optimizer.prefer_hash_join = false;
        let ctx = SessionContext::new_with_config(config);

        for (name, values) in [
            ("l", &[Some(10), Some(20), Some(30), None][..]),
            ("r", &[Some(20), Some(99), Some(50)][..]),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
            let (head, tail) = values.split_at(values.len() / 2);
            let batches: Vec<Vec<RecordBatch>> = [head, tail]
                .iter()
                .map(|slice| {
                    vec![
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(Int32Array::from(slice.to_vec()))],
                        )
                        .expect("the test batch is well formed"),
                    ]
                })
                .collect();
            let table = MemTable::try_new(schema, batches).expect("the test table is well formed");
            ctx.register_table(name, Arc::new(table))
                .expect("registering the test table");
        }
        ctx
    }

    /// The keys `sql` answers, sorted, with a NULL rendered as `-1` so it is visible.
    async fn keys(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Vec<i32> {
        use datafusion::arrow::array::{Array, Int32Array};

        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must run: {e}"));
        let mut keys = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("an int4 column");
            for row in 0..batch.num_rows() {
                keys.push(if column.is_null(row) {
                    -1
                } else {
                    column.value(row)
                });
            }
        }
        keys.sort_unstable();
        keys
    }

    /// The contract, executed: the rewritten predicate answers what PostgreSQL answers
    /// in all three of its NULL cases, under the operator the cluster actually uses.
    ///
    /// The unrewritten answer is asserted beside each one, so the test is also the record
    /// of what the rewrite is for — and fails if DataFusion ever fixes it upstream.
    #[tokio::test]
    async fn the_rewritten_predicate_is_null_aware() {
        let ctx = keys_context();

        for (sql, postgres, without_the_rewrite) in [
            // The left NULL drops: `NULL NOT IN (20, 99, 50)` is NULL, not true.
            (
                "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r)",
                vec![10, 30],
                vec![-1, 10, 30],
            ),
            // A NULL among the candidates makes every non-matching row NULL too, so a
            // subquery holding one answers nothing at all.
            (
                "SELECT k FROM l WHERE k NOT IN (SELECT k FROM l)",
                Vec::new(),
                vec![-1],
            ),
            // An empty subquery has no NULL to poison the comparison and no row to
            // match, so every row is true — including the one whose key is NULL.
            (
                "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r WHERE k > 1000)",
                vec![-1, 10, 20, 30],
                vec![-1, 10, 20, 30],
            ),
        ] {
            assert_eq!(keys(&ctx, sql).await, without_the_rewrite, "{sql}");
            assert_eq!(keys(&ctx, &rewritten(sql)).await, postgres, "{sql}");
        }
    }

    /// The rewrite has to survive the contexts a predicate appears in, not just the bare
    /// one — a wrong count or a lost conjunct would be as silent as the defect.
    #[tokio::test]
    async fn the_rewritten_predicate_composes() {
        let ctx = keys_context();

        // In a conjunction, PostgreSQL answers `30`.
        assert_eq!(
            keys(
                &ctx,
                &rewritten("SELECT k FROM l WHERE k > 15 AND k NOT IN (SELECT k FROM r)")
            )
            .await,
            vec![30]
        );
        // Under an aggregate, PostgreSQL answers `2`.
        let counted = rewritten("SELECT count(*) FROM l WHERE k NOT IN (SELECT k FROM r)");
        let batches = ctx
            .sql(&counted)
            .await
            .expect("the count must plan")
            .collect()
            .await
            .expect("the count must run");
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .expect("count is int8")
            .value(0);
        assert_eq!(count, 2);
        // With a left side that is not a bare column: `k + 10 NOT IN (20, 99, 50)`
        // leaves `20, 30`, and the NULL row still drops.
        assert_eq!(
            keys(
                &ctx,
                &rewritten("SELECT k FROM l WHERE k + 10 NOT IN (SELECT k FROM r)")
            )
            .await,
            vec![20, 30]
        );
    }
}
