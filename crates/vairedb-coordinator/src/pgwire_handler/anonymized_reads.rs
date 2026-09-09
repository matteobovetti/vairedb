//! Refuse the reads of a pseudonymized column whose answer cannot mean what it says.
//!
//! A column declared in `WITH (anonymized_columns = …)` stores the HMAC-SHA256 hex
//! digest of its plaintext and reads back as one ([`crate::anonymization`]). That is the
//! contract, and most of SQL survives it: HMAC is deterministic, so **equality** is
//! preserved, and with it `GROUP BY`, `DISTINCT`, `count`, `count(DISTINCT)` and a join
//! on the column. Those stay accepted and answer correctly.
//!
//! What HMAC destroys is *order* and *structure*. The digest of `'alice@x.com'` sorts
//! nowhere near the digest of `'bob@x.com'`, and contains none of the characters either
//! address does. So:
//!
//! * `ORDER BY email`, `min(email)`, `max(email)` return the lexicographic extreme of
//!   the **digests** — an ordering unrelated to the plaintext's, reported without a hint
//!   that it is arbitrary.
//! * `email LIKE '%@x.com'` matches nothing, ever, because a 64-character hex string
//!   contains no `@`.
//! * `email = 'alice@x.com'` matches nothing, because the stored value is the digest.
//!
//! Every one of those is a *successful* query returning plausible rows, which is the
//! shape of wrong answer a client cannot detect. This module converts them into a
//! `0A000` naming what was refused and what to write instead, which is the same trade
//! the write path already makes for a statement it cannot anonymize honestly.
//!
//! ## Why equality is refused only against a literal that cannot be a digest
//!
//! The two cases differ in whether a correct query exists at all. There is no
//! client-side rewrite that recovers plaintext order from digests, so an ordering read is
//! refused outright. Equality, in contrast, is answerable: the client hashes its own
//! literal and looks the digest up, which is the documented contract and what the
//! anonymization e2e tests pin. So equality is refused only when the literal *cannot* be
//! a digest — anything that is not 64 hex characters — where the answer is certain to be
//! empty and the refusal can say precisely what to send instead.
//!
//! A digest-shaped literal is accepted, as is a comparison against a column or a bind
//! parameter, whose value this pass cannot see. The parameter case is a known hole: a
//! `WHERE email = $1` bound to plaintext still answers empty. Closing it needs the check
//! at `Bind` rather than at parse, where the plan is already cached.
//!
//! ## Scope
//!
//! Read path only, and only for user data — a catalog query reaches no anonymized
//! column. Column names are matched by name, unqualified: a query joining a table that
//! pseudonymizes `email` to one that does not is refused for either `email`. That
//! over-refuses in a case no schema in this codebase's tests has, and over-refusing is
//! the safe direction when the alternative is a plausible wrong answer.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::MetadataCatalog;
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{
    BinaryOperator, Expr, Function, NamedWindowDefinition, NamedWindowExpr, OrderByExpr,
    OrderByKind, Query, SetExpr, Statement, Value, Visit, Visitor, WindowType, visit_expressions,
};

/// The length of an HMAC-SHA256 digest in hex.
const DIGEST_HEX_LEN: usize = 64;

/// Refuse every read in `stmt` whose result would be derived from a pseudonymized
/// column's digest rather than from its plaintext.
///
/// A no-op — and, for a statement reading no pseudonymized table, a single catalog
/// lookup per relation — when nothing in the statement is anonymized, which is every
/// query against every table that does not declare the option.
pub(super) fn reject_meaningless_reads(
    stmt: &Statement,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<()> {
    reject_reads_of(stmt, &pseudonymized_columns_read(stmt, catalog))
}

/// [`reject_meaningless_reads`] once the catalog has been consulted, so the rules can be
/// tested against a column set rather than against a live catalog.
fn reject_reads_of(stmt: &Statement, columns: &BTreeSet<String>) -> PgWireResult<()> {
    if columns.is_empty() {
        return Ok(());
    }
    match stmt.visit(&mut Guard { columns }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// The pseudonymized column names of every table `stmt` reads, lower-cased.
///
/// A relation missing from the catalog is skipped rather than reported: it is either a
/// CTE or a genuinely unknown table, and in the second case the planner's own
/// "table not found" is the better message.
fn pseudonymized_columns_read(
    stmt: &Statement,
    catalog: &Arc<MetadataCatalog>,
) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    for relation in crate::write_sql_cl::relations_read(stmt) {
        let Ok(Some(meta)) = catalog.get_table(&relation) else {
            continue;
        };
        // Already lower-cased by `parse_anonymized_columns`.
        columns.extend(meta.anonymized_columns.keys().cloned());
    }
    columns
}

/// Walks the statement looking for the ordering and matching constructs.
struct Guard<'a> {
    columns: &'a BTreeSet<String>,
}

impl Visitor for Guard<'_> {
    type Break = PgWireError;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<PgWireError> {
        flow(self.check_query(query))
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<PgWireError> {
        flow(self.check_expr(expr))
    }
}

/// A check's verdict as the visitor wants it. The checks themselves are written with
/// `?` on `Result`, which reads better than nesting `ControlFlow` matches.
fn flow(checked: PgWireResult<()>) -> ControlFlow<PgWireError> {
    match checked {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    }
}

impl Guard<'_> {
    /// The ordering clauses that are properties of a query rather than expressions: its
    /// own `ORDER BY`, and the `ORDER BY` of a window it defines by name.
    fn check_query(&self, query: &Query) -> PgWireResult<()> {
        if let Some(order_by) = &query.order_by
            && let OrderByKind::Expressions(exprs) = &order_by.kind
        {
            self.check_order_by(exprs)?;
        }
        // A nested query is reached by the visitor on its own; only this level's
        // `WINDOW` clause is read here.
        if let SetExpr::Select(select) = query.body.as_ref() {
            for NamedWindowDefinition(_, definition) in &select.named_window {
                if let NamedWindowExpr::WindowSpec(spec) = definition {
                    self.check_order_by(&spec.order_by)?;
                }
            }
        }
        Ok(())
    }

    fn check_expr(&self, expr: &Expr) -> PgWireResult<()> {
        match expr {
            Expr::Function(func) => self.check_function(func)?,
            Expr::BinaryOp { left, op, right } => self.check_binary_op(left, op, right)?,
            Expr::Between {
                expr, low, high, ..
            } => {
                // A range test is an ordering read of every operand it compares.
                for operand in [expr.as_ref(), low.as_ref(), high.as_ref()] {
                    if let Some(column) = self.mentioned_in(operand) {
                        return Err(ordering_refused(&column, "BETWEEN"));
                    }
                }
            }
            Expr::Like { expr, .. }
            | Expr::ILike { expr, .. }
            | Expr::SimilarTo { expr, .. }
            | Expr::RLike { expr, .. } => {
                if let Some(column) = self.mentioned_in(expr) {
                    return Err(pattern_refused(&column));
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Some(column) = self.mentioned_in(expr)
                    && let Some(literal) = list.iter().find_map(non_digest_string)
                {
                    return Err(plaintext_refused(&column, &literal));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The pseudonymized column `expr` reads, if it reads one.
    ///
    /// Any mention counts, at any depth: `lower(email)` and `email || '!'` are as
    /// unordered as `email` itself, since the digest is what the expression is computed
    /// from.
    fn mentioned_in(&self, expr: &Expr) -> Option<String> {
        let mut found = None;
        let _ = visit_expressions(expr, |node| {
            let name = match node {
                Expr::Identifier(ident) => Some(&ident.value),
                Expr::CompoundIdentifier(parts) => parts.last().map(|ident| &ident.value),
                _ => None,
            };
            match name {
                Some(name) if self.columns.contains(&name.to_ascii_lowercase()) => {
                    found = Some(name.clone());
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }
        });
        found
    }

    fn check_order_by(&self, exprs: &[OrderByExpr]) -> PgWireResult<()> {
        for order in exprs {
            if let Some(column) = self.mentioned_in(&order.expr) {
                return Err(ordering_refused(&column, "ORDER BY"));
            }
        }
        Ok(())
    }

    /// `min`/`max` read an ordering, and so does an aggregate's or window's own
    /// `ORDER BY`. Every other function is left alone: `count(email)` is exact, and a
    /// projection of the digest is the column's documented value.
    fn check_function(&self, func: &Function) -> PgWireResult<()> {
        use crate::sqlparser::ast::{FunctionArgumentClause, FunctionArguments};

        if let Some(name) = func.name.0.last().and_then(|part| part.as_ident()) {
            let name = name.value.to_ascii_lowercase();
            if name == "min" || name == "max" {
                for arg in argument_expressions(&func.args) {
                    if let Some(column) = self.mentioned_in(arg) {
                        return Err(ordering_refused(&column, &name));
                    }
                }
            }
        }

        if let FunctionArguments::List(list) = &func.args {
            for clause in &list.clauses {
                if let FunctionArgumentClause::OrderBy(exprs) = clause {
                    self.check_order_by(exprs)?;
                }
            }
        }

        if let Some(WindowType::WindowSpec(spec)) = &func.over {
            self.check_order_by(&spec.order_by)?;
        }
        Ok(())
    }

    /// The comparison operators, split by what they ask of the column.
    fn check_binary_op(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> PgWireResult<()> {
        let column = match (self.mentioned_in(left), self.mentioned_in(right)) {
            (Some(column), _) | (None, Some(column)) => column,
            (None, None) => return Ok(()),
        };
        match op {
            BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => Err(ordering_refused(&column, &op.to_string())),
            BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch
            | BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch => Err(pattern_refused(&column)),
            // Equality survives hashing, so it is refused only for a literal that
            // cannot be a digest and therefore cannot match a row.
            BinaryOperator::Eq | BinaryOperator::NotEq => {
                match non_digest_string(left).or_else(|| non_digest_string(right)) {
                    Some(literal) => Err(plaintext_refused(&column, &literal)),
                    None => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }
}

/// The expressions a function was called with, ignoring wildcards and named arguments.
fn argument_expressions(args: &crate::sqlparser::ast::FunctionArguments) -> Vec<&Expr> {
    use crate::sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

    let FunctionArguments::List(list) = args else {
        return Vec::new();
    };
    list.args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            }
            | FunctionArg::ExprNamed {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => Some(expr),
            _ => None,
        })
        .collect()
}

/// `expr` as a string literal that is not digest-shaped, i.e. one no row can hold.
///
/// A literal of 64 hex characters is taken to be a digest the client hashed itself, and
/// left alone. Either case is accepted: a client that upper-cases its hex gets an empty
/// answer, but it is asking for a digest and this pass has no business second-guessing
/// which one.
fn non_digest_string(expr: &Expr) -> Option<String> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let (Value::SingleQuotedString(text) | Value::DoubleQuotedString(text)) = &value.value else {
        return None;
    };
    let digest_shaped =
        text.len() == DIGEST_HEX_LEN && text.bytes().all(|byte| byte.is_ascii_hexdigit());
    (!digest_shaped).then(|| text.clone())
}

/// `0A000`: the construct orders by a digest, so its answer is an arbitrary permutation
/// of the plaintext order rather than that order.
fn ordering_refused(column: &str, construct: &str) -> PgWireError {
    unsupported(
        format!("{construct} on the pseudonymized column \"{column}\""),
        "the column stores an HMAC digest, so it would order by the digests — an ordering \
         unrelated to the plaintext's, returned with nothing to say so. Equality, GROUP BY, \
         DISTINCT and count are preserved by the hash and stay available; an ordering read \
         needs a column that is not pseudonymized",
    )
}

/// `0A000`: the pattern is matched against hex, so it can only ever answer empty.
fn pattern_refused(column: &str) -> PgWireError {
    unsupported(
        format!("pattern matching on the pseudonymized column \"{column}\""),
        "the column stores a 64-character HMAC digest, which contains none of the characters \
         the plaintext did, so the match would answer empty for every row. Look the whole \
         value up by its digest instead",
    )
}

/// `0A000`: the literal is plaintext where the column holds digests.
fn plaintext_refused(column: &str, literal: &str) -> PgWireError {
    unsupported(
        format!("comparing the pseudonymized column \"{column}\" against '{literal}'"),
        "the column stores the HMAC-SHA256 digest of its plaintext, not the plaintext, so this \
         would match no row. Compare against the digest — the same one an INSERT of that value \
         would store — which is 64 hex characters",
    )
}

/// A `0A000` naming the read refused and what remains available.
fn unsupported(what: impl std::fmt::Display, why: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!("{what} is not supported: {why}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A digest of the right shape, standing in for one the client hashed itself.
    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// The verdict on `sql`, with `email` the one pseudonymized column.
    fn verdict(sql: &str) -> PgWireResult<()> {
        let stmt = crate::pgwire_handler::parser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        reject_reads_of(&stmt, &BTreeSet::from(["email".to_string()]))
    }

    /// The refusal message for a statement that must be refused.
    fn refusal(sql: &str) -> String {
        verdict(sql)
            .expect_err(&format!("expected a refusal for: {sql}"))
            .to_string()
    }

    fn accepted(sql: &str) {
        verdict(sql).unwrap_or_else(|e| panic!("expected acceptance for {sql}, got: {e}"));
    }

    // The ordering reads. Each one would answer in digest order, which is a permutation
    // of the plaintext order with nothing in the result to say so.
    #[test]
    fn ordering_by_a_pseudonymized_column_is_refused() {
        for sql in [
            "SELECT id FROM t ORDER BY email",
            "SELECT id FROM t ORDER BY email DESC",
            "SELECT id FROM t ORDER BY lower(email)",
            "SELECT id FROM t ORDER BY id, t.email",
        ] {
            assert!(refusal(sql).contains("ORDER BY"), "{sql}");
        }
    }

    #[test]
    fn the_extremes_of_a_pseudonymized_column_are_refused() {
        assert!(refusal("SELECT min(email) FROM t").contains("min"));
        assert!(refusal("SELECT max(email) FROM t").contains("max"));
    }

    #[test]
    fn a_range_comparison_on_a_pseudonymized_column_is_refused() {
        for sql in [
            "SELECT id FROM t WHERE email > 'a'",
            "SELECT id FROM t WHERE 'a' < email",
            "SELECT id FROM t WHERE email <= 'z'",
            "SELECT id FROM t WHERE email BETWEEN 'a' AND 'z'",
        ] {
            let message = refusal(sql);
            assert!(message.contains("pseudonymized column \"email\""), "{sql}");
        }
    }

    // A window's ordering is the same read as the query's, in the two other places it
    // can be written.
    #[test]
    fn a_window_ordered_by_a_pseudonymized_column_is_refused() {
        assert!(
            refusal("SELECT row_number() OVER (ORDER BY email) FROM t").contains("ORDER BY"),
            "inline window"
        );
        assert!(
            refusal("SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY email)")
                .contains("ORDER BY"),
            "named window"
        );
    }

    #[test]
    fn an_aggregates_own_ordering_is_refused() {
        assert!(
            refusal("SELECT array_agg(id ORDER BY email) FROM t").contains("ORDER BY"),
            "ORDER BY inside an aggregate"
        );
    }

    // A digest contains none of the characters the plaintext did, so a pattern match
    // answers empty for every row rather than for the rows that do not match.
    #[test]
    fn pattern_matching_a_pseudonymized_column_is_refused() {
        for sql in [
            "SELECT id FROM t WHERE email LIKE '%@x.com'",
            "SELECT id FROM t WHERE email ILIKE '%@X.COM'",
            "SELECT id FROM t WHERE email NOT LIKE '%@x.com'",
            "SELECT id FROM t WHERE email SIMILAR TO '%@x.com'",
            "SELECT id FROM t WHERE email ~ '@x[.]com$'",
        ] {
            assert!(refusal(sql).contains("pattern matching"), "{sql}");
        }
    }

    // The equality half: a literal that cannot be a digest cannot match a row, and the
    // refusal says to send the digest instead.
    #[test]
    fn equality_against_plaintext_is_refused_and_says_what_to_send() {
        let message = refusal("SELECT id FROM t WHERE email = 'alice@x.com'");
        assert!(message.contains("alice@x.com"), "{message}");
        assert!(message.contains("64 hex characters"), "{message}");

        assert!(refusal("SELECT id FROM t WHERE email <> 'alice@x.com'").contains("no row"));
        assert!(
            refusal("SELECT id FROM t WHERE email IN ('alice@x.com', 'bob@x.com')")
                .contains("alice@x.com")
        );
    }

    // The documented lookup: hash client-side, then select by the digest. This is the
    // contract the anonymization e2e tests pin, and it has to stay accepted.
    #[test]
    fn a_lookup_by_digest_is_accepted() {
        accepted(&format!("SELECT id FROM t WHERE email = '{DIGEST}'"));
        accepted(&format!("SELECT id FROM t WHERE email IN ('{DIGEST}')"));
    }

    // Everything HMAC preserves. Equality is deterministic, so cardinality and grouping
    // are exact, and the projection of a digest is the column's documented value.
    #[test]
    fn the_reads_hashing_preserves_are_accepted() {
        for sql in [
            "SELECT email FROM t",
            "SELECT count(email) FROM t",
            "SELECT count(DISTINCT email) FROM t",
            "SELECT email, count(*) FROM t GROUP BY email",
            "SELECT DISTINCT email FROM t",
            "SELECT id FROM t WHERE email IS NOT NULL",
            "SELECT a.id FROM t a JOIN t b ON a.email = b.email",
        ] {
            accepted(sql);
        }
    }

    // A column that is not pseudonymized is not this pass's business, whatever is done
    // to it, including in the same statement as one that is.
    #[test]
    fn a_plain_column_is_left_alone() {
        accepted("SELECT id FROM t ORDER BY name");
        accepted("SELECT min(name) FROM t WHERE name LIKE 'A%' ORDER BY id");
        accepted("SELECT email FROM t WHERE name > 'a' ORDER BY id");
    }

    // A table with no pseudonymized column short-circuits before any of the rules run,
    // which is the path every ordinary query takes.
    #[test]
    fn a_statement_with_no_pseudonymized_column_in_scope_is_untouched() {
        let stmt = crate::pgwire_handler::parser::parse_sql("SELECT id FROM t ORDER BY email")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        reject_reads_of(&stmt, &BTreeSet::new()).unwrap();
    }

    // A subquery is reached too: the visitor walks the whole statement, so a refusal
    // cannot be evaded by nesting the ordering one level down.
    #[test]
    fn a_subquery_is_checked_as_well() {
        assert!(
            refusal("SELECT id FROM (SELECT id, email FROM t ORDER BY email) s")
                .contains("ORDER BY")
        );
        assert!(
            refusal("SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE email = 'a@x.com')")
                .contains("a@x.com")
        );
    }

    #[test]
    fn a_digest_shaped_literal_is_recognized_by_shape_alone() {
        let quoted = |text: &str| {
            Expr::Value(crate::sqlparser::ast::Value::SingleQuotedString(text.to_string()).into())
        };
        assert_eq!(non_digest_string(&quoted(DIGEST)), None);
        // One character short, and one character that is not hex.
        assert!(non_digest_string(&quoted(&DIGEST[1..])).is_some());
        assert!(non_digest_string(&quoted(&format!("{}z", &DIGEST[1..]))).is_some());
    }
}
