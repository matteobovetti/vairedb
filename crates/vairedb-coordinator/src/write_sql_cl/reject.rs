//! The write-path expressions VaireDB refuses rather than ship to a shard that would
//! answer them differently from PostgreSQL.
//!
//! A write is not planned. It is rendered back to SQL text and executed verbatim by the
//! shards' DuckDB, so every place DuckDB's reading of an expression differs from
//! PostgreSQL's is a place a client can be told a write succeeded when it did something
//! else. Most of those differences are translated — [`super::dialect`] rewrites the
//! regex operators, the `LIKE` escape and `SIMILAR TO` into forms DuckDB reads the way
//! PostgreSQL does, the shards' `integer_division` setting makes `/` truncate, and a
//! guard around the divisor makes a zero divisor raise. What is left here is the residue:
//! expressions with no DuckDB spelling that means the same thing, plus the one shape a
//! translation exists for but cannot be applied to without growing the statement
//! exponentially.
//!
//! Refusal is the answer rather than a translation because the alternative is not
//! "unimplemented" but *wrong*: the statement runs, reports the rows it changed, and the
//! rows are not the ones the client asked for. A `0A000` naming the divergence is
//! recoverable; a silent one is not.
//!
//! Read-path parity is the other half of the rule. Each refusal here has a counterpart
//! on the read path ([`crate::pgwire_handler::pg_operators`]), so the same expression is
//! answered the same way whichever path a statement takes — an expression accepted on a
//! `SELECT` and refused on an `UPDATE` would be its own kind of split.

use std::ops::ControlFlow;

use vairedb_common::bytea_in;

use crate::error::CoordinatorError;
use crate::pgwire_handler::pg_operators::{is_byte_order_collation, similar_to_regex_from_ast};
use crate::sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Expr, Statement, Value, visit_expressions,
};

/// Refuse the expressions of `stmt` that DuckDB would answer differently from
/// PostgreSQL and that cannot be translated.
///
/// Called on the **verbatim** AST at parse time, before any DuckDB rewrite: the point is
/// to judge what the client wrote, and [`super::transform_to_duckdb`] has by then already
/// changed some of it. Takes a [`CoordinatorError`] rather than a `PgWireError` for the
/// same reason [`crate::pgwire_handler::pg_operators::reject_unsupported_collation`] does
/// — parsing is not yet on a connection.
pub fn reject_duckdb_divergent(stmt: &Statement) -> crate::error::Result<()> {
    match visit_expressions(stmt, |expr| match check_expr(expr) {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// The per-node half of [`reject_duckdb_divergent`].
fn check_expr(expr: &Expr) -> crate::error::Result<()> {
    match expr {
        // A collation VaireDB does not implement, refused on the same terms the read
        // path refuses it ([`is_byte_order_collation`]) so that the answer does not
        // depend on which path the statement took. Not translatable in either
        // direction: DuckDB *has* its own collations and would apply one, so a write
        // that reached a shard would be ordered by rules the read path — which compares
        // by byte value — could never reproduce.
        Expr::Collate { collation, .. } if !is_byte_order_collation(collation) => {
            Err(CoordinatorError::Unsupported(format!(
                "COLLATE {collation} is not supported on the write path: VaireDB compares and \
                 orders text by byte value, and a shard applying its own collation instead would \
                 not agree with the read path; omit the COLLATE, or use \"C\""
            )))
        }
        // Measured on DuckDB 1.5.5: `CAST('abcdef' AS VARCHAR(3))` returns all six
        // characters, exactly as the read path does. PostgreSQL truncates to three. The
        // read path refuses this rather than answering it, and refusing it here too is
        // what keeps the two paths from disagreeing about the same cast.
        Expr::Cast {
            data_type:
                data_type @ (DataType::Character(Some(_))
                | DataType::Char(Some(_))
                | DataType::CharacterVarying(Some(_))
                | DataType::CharVarying(Some(_))
                | DataType::Varchar(Some(_))),
            ..
        } => Err(CoordinatorError::Unsupported(format!(
            "CAST to {data_type} is not supported: the length is not enforced, so the cast would \
             neither truncate nor pad and the value would be stored unchanged; cast to TEXT, or \
             truncate explicitly with substr()"
        ))),
        // A division whose divisor is itself a division or a modulo. Every division on the
        // write path is wrapped in a guard that raises on a zero divisor
        // ([`super::dialect`]), and the guard has to name the divisor twice — DuckDB has no
        // way to raise from an expression except `error()` inside a `CASE`, and no way to
        // bind an intermediate value. Duplicating an ordinary divisor costs one extra
        // evaluation; duplicating a divisor that carries a guard of its own doubles that
        // guard, and `a / (b / (c / d))` doubles once per level, so a short statement can
        // be made to render a very long one. Refused rather than left unguarded, because
        // the unguarded form is the silent NULL this whole rewrite exists to remove.
        Expr::BinaryOp {
            op: BinaryOperator::Divide | BinaryOperator::Modulo,
            right,
            ..
        } if contains_division(right) => Err(CoordinatorError::Unsupported(
            "a division or modulo whose divisor is itself a division or modulo is not \
             supported on the write path: the divisor is evaluated twice to check it \
             against zero, so nesting one inside another would grow the statement \
             exponentially; compute the inner division first, or parenthesize the \
             expression so the divisions are not nested"
                .to_string(),
        )),
        // A cast to `bytea`, whose PostgreSQL meaning is a whole input conversion and not a
        // reinterpretation of the bytes: `'\xDEADBEEF'::bytea` is four bytes in PostgreSQL,
        // and DuckDB's `VARCHAR` → `BLOB` cast reads its own escape syntax and stores seven.
        // A literal is translated by [`super::dialect`], which needs it decodable to do so;
        // anything else is refused here, because the conversion cannot be expressed in
        // DuckDB SQL and a shard would silently store different bytes.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type: DataType::Bytea,
            expr: inner,
            ..
        } => reject_untranslatable_bytea_cast(inner),
        // A `SIMILAR TO` is translated by [`super::dialect`], which needs the pattern as
        // a literal to translate it. Refused here so that the translation itself can be
        // infallible, and with the read path's own message so a client that moved the
        // same predicate from a SELECT to an UPDATE reads the same explanation.
        Expr::SimilarTo {
            pattern,
            escape_char,
            ..
        } => similar_to_regex_from_ast(pattern, escape_char.as_ref())
            .map(|_| ())
            .map_err(|e| CoordinatorError::Unsupported(e.message())),
        _ => Ok(()),
    }
}

/// Refuse a cast to `bytea` that [`super::dialect`] cannot translate, and a literal whose
/// text is not a `bytea` at all.
///
/// Three shapes are accepted, and each for its own reason:
///
/// * a **single-quoted string literal** that decodes, which the translation replaces with
///   `unhex('…')`. A literal that does *not* decode is refused with PostgreSQL's own
///   message and SQLSTATE rather than with `0A000`: the statement is not unsupported, the
///   value is wrong, and `'\xzz'::bytea` fails identically on PostgreSQL.
/// * `NULL`, because a NULL `bytea` is a NULL in DuckDB too — there is nothing to convert.
/// * a **placeholder**, because a driver sending `bytea` sends it as a typed parameter and
///   the shard binds it as a `BLOB`, where `::BYTEA` is the identity. Refusing `$1::bytea`
///   would break the one shape that is already right. A parameter bound as *text* and cast
///   to `bytea` is the residue this leaves, and it cannot be told apart from here: the
///   inferred type is not known at parse time.
///
/// Everything else — a column, a function call, an expression — is refused. The conversion
/// has no DuckDB spelling, so the alternative is a shard storing PostgreSQL-invisible bytes.
fn reject_untranslatable_bytea_cast(inner: &Expr) -> crate::error::Result<()> {
    match inner {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => {
                bytea_in::decode(text)
                    .map(|_| ())
                    .map_err(|e| CoordinatorError::InvalidValue {
                        message: e.to_string(),
                        code: e.error_code(),
                    })
            }
            Value::Null | Value::Placeholder(_) => Ok(()),
            _ => Err(untranslatable_bytea_cast()),
        },
        _ => Err(untranslatable_bytea_cast()),
    }
}

/// The refusal for a cast to `bytea` VaireDB cannot perform on the write path.
fn untranslatable_bytea_cast() -> CoordinatorError {
    CoordinatorError::Unsupported(
        "CAST to BYTEA is only supported on the write path for a string literal: PostgreSQL \
         reads the text through its own bytea input conversion, where '\\xDEADBEEF' is four \
         bytes, and a shard would instead reinterpret the characters and store different \
         bytes; write the value as a literal, or send it as a bytea parameter"
            .to_string(),
    )
}

/// Whether `expr` is, or contains, a division or a modulo — the operators
/// [`super::dialect`] wraps in a zero-divisor guard.
fn contains_division(expr: &Expr) -> bool {
    matches!(
        visit_expressions(expr, |e| match e {
            Expr::BinaryOp {
                op: BinaryOperator::Divide | BinaryOperator::Modulo,
                ..
            } => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }),
        ControlFlow::Break(())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;
    use vairedb_common::proto::vairedb::v1::VdbErrorCode;

    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0)
    }

    fn rejected(sql: &str) -> String {
        match reject_duckdb_divergent(&parse(sql)) {
            Ok(()) => panic!("`{sql}` should be refused"),
            Err(e) => e.to_string(),
        }
    }

    fn accepted(sql: &str) {
        if let Err(e) = reject_duckdb_divergent(&parse(sql)) {
            panic!("`{sql}` should be accepted: {e}");
        }
    }

    /// A decodable string literal is the shape [`super::dialect`] translates, in both
    /// spellings of the cast — plus the two that need no translation at all.
    #[test]
    fn a_bytea_cast_of_a_literal_is_left_to_the_translation() {
        accepted("INSERT INTO t (b) VALUES ('\\xDEADBEEF'::bytea)");
        accepted("INSERT INTO t (b) VALUES (CAST('a\\101b' AS BYTEA))");
        accepted("UPDATE t SET b = ''::bytea");
        accepted("INSERT INTO t (b) VALUES (NULL::bytea)");
        // A driver's `bytea` parameter binds as a BLOB on the shard, where the cast is the
        // identity — refusing this would break the one shape that is already right.
        accepted("INSERT INTO t (b) VALUES ($1::bytea)");
    }

    /// A literal that is not a `bytea` is refused with PostgreSQL's own message, and with
    /// PostgreSQL's own SQLSTATE rather than the `0A000` of an unsupported form — the
    /// statement is fine and the value is not.
    #[test]
    fn a_bytea_literal_that_does_not_decode_is_refused_as_postgresql_does() {
        for (sql, message, code) in [
            (
                "INSERT INTO t (b) VALUES ('\\xzz'::bytea)",
                "invalid hexadecimal digit: \"z\"",
                VdbErrorCode::InvalidParameterValue,
            ),
            (
                "INSERT INTO t (b) VALUES ('\\xdeadbee'::bytea)",
                "invalid hexadecimal data: odd number of digits",
                VdbErrorCode::InvalidParameterValue,
            ),
            (
                "UPDATE t SET b = 'a\\12'::bytea",
                "invalid input syntax for type bytea",
                VdbErrorCode::InvalidTextRepresentation,
            ),
        ] {
            let err = reject_duckdb_divergent(&parse(sql)).expect_err("should be refused");
            assert_eq!(err.to_string(), message, "`{sql}`");
            assert_eq!(err.vdb_error_code(), code, "`{sql}`");
        }
    }

    /// Everything else has no DuckDB spelling: the conversion is PostgreSQL's own, and a
    /// shard would reinterpret the characters instead of converting them.
    #[test]
    fn a_bytea_cast_of_anything_but_a_literal_is_refused() {
        for sql in [
            "INSERT INTO t (b) SELECT s::bytea FROM u",
            "UPDATE t SET b = s::bytea",
            "UPDATE t SET b = concat(s, 'x')::bytea",
            "DELETE FROM t WHERE b = s::bytea",
        ] {
            let msg = rejected(sql);
            assert!(msg.contains("CAST to BYTEA"), "{msg}");
            assert!(
                msg.contains("four bytes"),
                "the message says what PostgreSQL does instead: {msg}"
            );
        }
    }

    /// A `BYTEA` *column* is not a cast: declaring one is how a client stores bytes and is
    /// translated to `BLOB` by [`super::dialect::transform_to_duckdb`], not refused.
    #[test]
    fn a_declared_bytea_column_is_not_a_refused_cast() {
        accepted("CREATE TABLE t (b BYTEA)");
        accepted("ALTER TABLE t ADD COLUMN b BYTEA");
    }

    /// A collation naming a real locale, in each of the four statement kinds the write
    /// path renders — the check is on expressions, so it has to reach a WHERE clause, an
    /// assignment and a value row alike.
    #[test]
    fn a_locale_collation_is_refused_in_every_write_statement() {
        for sql in [
            "UPDATE t SET x = 1 WHERE s COLLATE \"en_US\" < 'a'",
            "DELETE FROM t WHERE s COLLATE \"de_DE\" > 'z'",
            "INSERT INTO t (s) VALUES ('a' COLLATE \"en_US\")",
            "MERGE INTO t USING u ON t.id = u.id \
             WHEN MATCHED AND t.s COLLATE \"en_US\" < 'a' THEN DELETE",
        ] {
            let msg = rejected(sql);
            assert!(msg.contains("COLLATE"), "{msg}");
            assert!(
                msg.contains("byte value"),
                "the message says what VaireDB does instead: {msg}"
            );
        }
    }

    /// The byte-order collations name the comparison VaireDB in fact performs, so they
    /// ask for what they are going to get. `pg_catalog.default` is the one that has to
    /// be accepted rather than merely tolerated — it is what a driver sends.
    #[test]
    fn byte_order_collations_are_accepted() {
        accepted("UPDATE t SET x = 1 WHERE s COLLATE \"C\" < 'a'");
        accepted("UPDATE t SET x = 1 WHERE s COLLATE \"POSIX\" < 'a'");
        accepted("UPDATE t SET x = 1 WHERE s COLLATE ucs_basic < 'a'");
        accepted("DELETE FROM t WHERE s COLLATE pg_catalog.default < 'a'");
    }

    #[test]
    fn a_cast_length_that_would_not_be_enforced_is_refused() {
        for sql in [
            "INSERT INTO t (s) VALUES (CAST('abcdef' AS VARCHAR(3)))",
            "UPDATE t SET s = CAST(s AS CHAR(2))",
            "UPDATE t SET s = s::VARCHAR(4)",
            "DELETE FROM t WHERE s = CAST('ab' AS CHARACTER VARYING(1))",
        ] {
            let msg = rejected(sql);
            assert!(msg.contains("CAST to"), "{msg}");
            assert!(msg.contains("not enforced"), "{msg}");
        }

        // An unqualified cast has no length to discard, so it is not this problem.
        accepted("UPDATE t SET s = CAST(s AS VARCHAR)");
        accepted("UPDATE t SET s = s::TEXT");
        accepted("UPDATE t SET n = n::INTEGER");
    }

    /// A `SIMILAR TO` whose pattern is a literal is translated rather than refused, and
    /// one whose pattern is not cannot be — the pattern is turned into a regex before
    /// anything runs.
    #[test]
    fn similar_to_is_accepted_only_where_it_can_be_translated() {
        accepted("UPDATE t SET x = 1 WHERE s SIMILAR TO 'a%'");
        accepted("DELETE FROM t WHERE s NOT SIMILAR TO 'a_c'");
        accepted("UPDATE t SET x = 1 WHERE s SIMILAR TO 'a!%' ESCAPE '!'");

        let msg = rejected("UPDATE t SET x = 1 WHERE s SIMILAR TO other_col");
        assert!(msg.contains("non-literal pattern"), "{msg}");
        let msg = rejected("UPDATE t SET x = 1 WHERE s SIMILAR TO $1");
        assert!(msg.contains("non-literal pattern"), "{msg}");
        // An ESCAPE that is not a single character is the client's mistake, not a gap.
        let msg = rejected("UPDATE t SET x = 1 WHERE s SIMILAR TO 'a%' ESCAPE 'ab'");
        assert!(msg.contains("escape string"), "{msg}");
    }

    /// The forms [`super::dialect`] translates must *not* be refused here — the two
    /// halves of the fix have to agree about which is responsible for what, or ordinary
    /// PostgreSQL stops working on the write path.
    #[test]
    fn the_translated_forms_are_left_to_the_translation() {
        accepted("UPDATE t SET x = 1 WHERE s ~ '^a'");
        accepted("UPDATE t SET x = 1 WHERE s !~ '^a'");
        accepted("UPDATE t SET x = 1 WHERE s ~* '^A'");
        accepted("UPDATE t SET x = 1 WHERE s !~* '^A'");
        accepted("UPDATE t SET x = 1 WHERE s LIKE 'a\\_b'");
        accepted("UPDATE t SET x = 1 WHERE s ILIKE 'a\\%b'");
        accepted("UPDATE t SET x = 7 / 2");
    }

    /// A divisor that is itself a division carries a guard of its own, and the outer guard
    /// names the divisor twice — so `a / (b / c)` would render two copies of the inner
    /// guard and `a / (b / (c / d))` twice that again. One shape, refused; every other
    /// division is translated.
    #[test]
    fn a_division_nested_in_a_divisor_is_refused() {
        for sql in [
            "UPDATE t SET x = a / (b / c)",
            "UPDATE t SET x = a % (b / c)",
            "UPDATE t SET x = a / (b % c)",
            // The division does not have to be the whole divisor to be duplicated with it.
            "UPDATE t SET x = a / (1 + b / c)",
            "DELETE FROM t WHERE a / (b / c) > 1",
            "INSERT INTO t (x) VALUES (a / (b / c))",
        ] {
            let msg = rejected(sql);
            assert!(
                msg.contains("divisor is itself a division"),
                "`{sql}`: {msg}"
            );
            assert!(
                msg.contains("exponentially"),
                "the message says why, not just that: `{sql}`: {msg}"
            );
        }
    }

    /// Only the *divisor* is duplicated by the guard, so a division in the dividend costs
    /// one guard per division and is not refused. Refusing it would take away ordinary
    /// arithmetic for a cost the guard does not have.
    #[test]
    fn a_division_nested_in_a_dividend_is_accepted() {
        accepted("UPDATE t SET x = (a / b) / c");
        accepted("UPDATE t SET x = (a / b) % c");
        accepted("UPDATE t SET x = a / b / c");
        accepted("UPDATE t SET x = a / b + c / d");
        accepted("UPDATE t SET x = a / (b + c)");
        accepted("UPDATE t SET x = a / $1");
    }

    /// Nothing ordinary is caught. A statement whose expressions DuckDB reads the way
    /// PostgreSQL does has to pass untouched, or the refusal is worse than the gap.
    #[test]
    fn ordinary_writes_are_untouched() {
        accepted("INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b')");
        accepted("UPDATE t SET n = n + 1 WHERE id = $1");
        accepted("DELETE FROM t WHERE id IN (1, 2, 3)");
        accepted("UPDATE t SET s = upper(s) WHERE s LIKE 'a%'");
        accepted("CREATE TABLE t (id INTEGER, s VARCHAR(10))");
    }

    /// A column *declaration* of `VARCHAR(10)` is not a cast and is not refused: the
    /// length is part of the schema the client asked for, and DDL is a different
    /// question from an expression that discards one.
    #[test]
    fn a_declared_column_length_is_not_a_refused_cast() {
        accepted("CREATE TABLE t (s VARCHAR(3))");
        accepted("ALTER TABLE t ADD COLUMN s CHAR(2)");
    }
}
