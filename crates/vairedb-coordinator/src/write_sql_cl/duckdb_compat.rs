//! Where the shards' DuckDB reads PostgreSQL differently, and what VaireDB does
//! about it: translate the difference away, or refuse the statement.
//!
//! A write is never planned — it is rendered back to SQL text and run verbatim by
//! the shards' DuckDB. So every divergence is a place a client could be told a write
//! succeeded when it did something else. Where no translation exists the statement is
//! refused, because the alternative is not "unimplemented" but *wrong*: a `0A000`
//! naming the divergence is recoverable, a silent one is not.
//!
//! Both halves live here because they are one decision per divergence and must agree
//! on which owns what: a form the translation handles must not be refused, and a form
//! the refusal admits must be translatable. They run at different times:
//!
//! - [`reject_duckdb_divergent`] — **parse time, verbatim AST**. Judges what the
//!   client wrote, before any rewrite.
//! - [`transform_to_duckdb`] — **render time**, on the AST about to become shard SQL.
//!   May assume the refusal already ran, which is what lets it be infallible.
//!
//! Each refusal has a read-path counterpart in
//! [`crate::pgwire_handler::pg_operators`], so an expression is answered the same way
//! on a `SELECT` as on an `UPDATE`.

use std::ops::ControlFlow;

use vairedb_common::bytea_in;

use crate::error::CoordinatorError;
use crate::pgwire_handler::parser::translate_format_arg;
use crate::pgwire_handler::pg_operators::{is_byte_order_collation, similar_to_regex_from_ast};
use crate::pgwire_handler::pg_subscripts::clamp_to_pg_semantics;
use crate::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ArrayElemTypeDef, BinaryOperator, CaseWhen,
    CastKind, ColumnDef, ColumnOption, CreateTableOptions, DataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, MergeClauseKind, ObjectName,
    ObjectNamePart, OrderByOptions, Statement, UnaryOperator, Value, ValueWithSpan, VisitMut,
    helpers::attached_token::AttachedToken, visit_expressions, visit_expressions_mut,
};

// ---------------------------------------------------------------------------
// Refusal — parse time, on the verbatim AST.
// ---------------------------------------------------------------------------

/// Refuse the expressions of `stmt` that DuckDB would answer differently from
/// PostgreSQL and that [`transform_to_duckdb`] cannot translate.
///
/// Called on the **verbatim** AST at parse time, so it judges what the client wrote
/// rather than what the translation made of it. Yields a [`CoordinatorError`] and not
/// a `PgWireError` for the same reason
/// [`crate::pgwire_handler::pg_operators::reject_unsupported_collation`] does:
/// parsing is not yet on a connection.
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
        // A collation VaireDB does not implement. Not translatable: DuckDB *has* its
        // own collations and would apply one, ordering the shard by rules the read
        // path — which compares by byte value — could never reproduce. Refused on the
        // read path's own terms ([`is_byte_order_collation`]).
        Expr::Collate { collation, .. } if !is_byte_order_collation(collation) => {
            Err(CoordinatorError::Unsupported(format!(
                "COLLATE {collation} is not supported on the write path: VaireDB compares and \
                 orders text by byte value, and a shard applying its own collation instead would \
                 not agree with the read path; omit the COLLATE, or use \"C\""
            )))
        }
        // Measured on DuckDB 1.5.5: `CAST('abcdef' AS VARCHAR(3))` returns all six
        // characters, as does the read path; PostgreSQL truncates to three. Refused on
        // both paths so neither silently keeps the untruncated value.
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
        // A division whose divisor is itself a division or modulo. [`guarded_division`]
        // has to name the divisor twice — DuckDB can only raise via `error()` inside a
        // `CASE`, and cannot bind an intermediate — so a guarded divisor doubles its own
        // guard, and `a / (b / (c / d))` doubles once per level. Refused rather than left
        // unguarded, since unguarded is the silent NULL the rewrite exists to remove.
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
        // In PostgreSQL a cast to `bytea` is an input conversion, not a reinterpretation:
        // `'\xDEADBEEF'::bytea` is four bytes, where DuckDB's `VARCHAR` → `BLOB` reads its
        // own escape syntax and stores seven. A decodable literal is translated below;
        // anything else has no DuckDB spelling, so it is refused rather than left to store
        // different bytes.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type: DataType::Bytea,
            expr: inner,
            ..
        } => reject_untranslatable_bytea_cast(inner),
        // The `SIMILAR TO` translation below needs the pattern as a literal. Refusing a
        // non-literal here is what lets that translation be infallible; the message is the
        // read path's own, so moving the predicate to a SELECT reads the same explanation.
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

/// Refuse a cast to `bytea` the translation cannot perform, and a literal whose text
/// is not a `bytea` at all.
///
/// Three shapes are accepted:
///
/// * a **single-quoted literal** that decodes, which the translation replaces with
///   `unhex('…')`. One that does *not* decode is refused with PostgreSQL's own message and
///   SQLSTATE, not `0A000` — the value is wrong, not the statement unsupported.
/// * `NULL`, which is a NULL in DuckDB too.
/// * a **placeholder**, because a driver sends `bytea` as a typed parameter that the shard
///   binds as `BLOB`, where `::BYTEA` is the identity; refusing `$1::bytea` would break the
///   one shape already correct. A parameter bound as *text* and cast to `bytea` is the
///   residue, and it is indistinguishable here — the inferred type is unknown at parse time.
///
/// Everything else — a column, a call, an expression — is refused: the conversion has no
/// DuckDB spelling, so the alternative is a shard storing different bytes.
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
/// [`guarded_division`] wraps in a zero-divisor guard.
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

// ---------------------------------------------------------------------------
// Translation — render time, on the AST about to become SQL text.
// ---------------------------------------------------------------------------

/// Rewrite `stmt` in place from PostgreSQL dialect to DuckDB-compatible form:
/// PostgreSQL-only types are mapped to DuckDB equivalents, coordinator-only table
/// options are stripped, a few function-name differences are bridged, and the
/// expressions DuckDB reads differently are respelled to mean what PostgreSQL means.
pub fn transform_to_duckdb(stmt: &mut Statement) {
    match stmt {
        Statement::CreateTable(create) => {
            for col in &mut create.columns {
                transform_data_type(&mut col.data_type);
                strip_column_collation(col);
            }
            create.table_options = CreateTableOptions::None;
        }
        // DuckDB has one index type (ART) and none of PostgreSQL's index decorations.
        // Each describes *how* the index is built, not which rows it admits, so
        // dropping them costs performance and nothing else. The exception is anything
        // narrowing a UNIQUE index's scope, which would weaken a constraint the client
        // asked for: that is refused upstream in `indexes::plan_create_index`.
        Statement::CreateIndex(create) => {
            create.using = None;
            create.concurrently = false;
            create.include = Vec::new();
            create.nulls_distinct = None;
            create.with = Vec::new();
            create.index_options = Vec::new();
            create.alter_options = Vec::new();
            create.predicate = None;
            for column in &mut create.columns {
                column.operator_class = None;
                column.column.options = OrderByOptions::default();
                column.column.with_fill = None;
            }
        }
        Statement::AlterTable(alter) => {
            for op in &mut alter.operations {
                match op {
                    AlterTableOperation::AddColumn { column_def, .. } => {
                        transform_data_type(&mut column_def.data_type);
                        strip_column_collation(column_def);
                    }
                    AlterTableOperation::AlterColumn {
                        op: AlterColumnOperation::SetDataType { data_type, .. },
                        ..
                    } => {
                        transform_data_type(data_type);
                    }
                    _ => {}
                }
            }
        }
        // Every expression of a write, at any depth: an INSERT's value rows, an
        // UPDATE's assignments, and either one's WHERE clause. The statement-kind gate
        // is the point — the read path hands its AST to DataFusion, which speaks
        // PostgreSQL, so rewriting a SELECT here would break it.
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            transform_expressions(stmt);
        }
        // A MERGE's expressions are rewritten like any other write's, plus two keyword
        // differences. DuckDB requires the `INTO` PostgreSQL makes optional, so omitting
        // it would be a syntax error from a node. `WHEN NOT MATCHED` already means `BY
        // TARGET` in both dialects; spelling it out drops the reliance on the shard
        // engine defaulting the same way. Optimizer hints go — they named a plan for a
        // different engine.
        Statement::Merge(merge) => {
            merge.into = true;
            merge.optimizer_hints.clear();
            for clause in &mut merge.clauses {
                if clause.clause_kind == MergeClauseKind::NotMatched {
                    clause.clause_kind = MergeClauseKind::NotMatchedByTarget;
                }
            }
            transform_expressions(merge);
        }
        _ => {}
    }
}

/// Apply the expression-level rewrites to every expression `node` contains, at any
/// depth. Generic over the node so a whole statement and a bare `Merge` — which
/// [`transform_to_duckdb`] has already borrowed mutably to fix its keywords — are
/// walked by the same code.
fn transform_expressions<V: VisitMut>(node: &mut V) {
    let _ = visit_expressions_mut(node, |expr| {
        if let Expr::Function(func) = expr {
            transform_function(func);
        }
        transform_pg_semantics(expr);
        ControlFlow::<()>::Continue(())
    });
}

/// Drop a column's `COLLATE` clause, for the same reason the expression-level one is
/// dropped: it names byte order, the comparison the shard already performs, and DuckDB
/// has no collation by any of PostgreSQL's names for it.
///
/// Anything other than byte order is refused when the DDL is planned
/// ([`crate::pgwire_handler::table_meta_ops`]), so the clause here is always absent or a
/// no-op — no ordering a client would have got is discarded. The guard stays so the two
/// rules cannot drift apart.
fn strip_column_collation(col: &mut ColumnDef) {
    col.options.retain(|option| match &option.option {
        ColumnOption::Collation(collation) => !is_byte_order_collation(collation),
        _ => true,
    });
}

/// Rewrite the expressions DuckDB reads differently from PostgreSQL into DuckDB
/// spellings that mean what PostgreSQL means.
///
/// These are not spelling differences — each is a *silently different answer*. A shard
/// runs the write verbatim, so without this the predicate that ran was not the predicate
/// the client wrote, and the row count came back as though it were. Each rewrite was
/// checked against DuckDB 1.5.5.
///
/// What cannot be rewritten is refused instead ([`reject_duckdb_divergent`]), including
/// the one input this function cannot supply itself: a literal `SIMILAR TO` pattern. That
/// refusal runs first, at parse time, which is what makes this function infallible —
/// anything it does not recognize is left exactly as it was.
fn transform_pg_semantics(expr: &mut Expr) {
    match expr {
        // PostgreSQL's `~` is a *partial* match; DuckDB's is `regexp_full_match`, so
        // even `'abcd' ~ '^ab'` was false and a write predicate matched nothing at all.
        // `regexp_matches` is DuckDB's partial match, which is what PostgreSQL means,
        // and its third argument carries the case-insensitive variants.
        Expr::BinaryOp {
            op:
                BinaryOperator::PGRegexMatch
                | BinaryOperator::PGRegexNotMatch
                | BinaryOperator::PGRegexIMatch
                | BinaryOperator::PGRegexNotIMatch,
            ..
        } => {
            let Expr::BinaryOp { left, op, right } = std::mem::replace(expr, null()) else {
                unreachable!("matched a BinaryOp");
            };
            let negated = matches!(
                op,
                BinaryOperator::PGRegexNotMatch | BinaryOperator::PGRegexNotIMatch
            );
            let insensitive = matches!(
                op,
                BinaryOperator::PGRegexIMatch | BinaryOperator::PGRegexNotIMatch
            );

            let mut args = vec![*left, *right];
            if insensitive {
                args.push(string("i"));
            }
            *expr = negate_if(negated, call("regexp_matches", args));
        }
        // PostgreSQL's `LIKE` escapes with `\` unless told otherwise; DuckDB has no
        // default escape, so `s LIKE 'a\_b'` matched on a wildcard where PostgreSQL
        // matched a literal underscore — the shape every ORM emits when escaping `_` or
        // `%` in user input. Naming the escape explicitly also covers a pattern arriving
        // as a parameter, where the backslash is in the value and invisible here.
        Expr::Like { escape_char, .. } | Expr::ILike { escape_char, .. }
            if escape_char.is_none() =>
        {
            *escape_char = Some(ValueWithSpan {
                value: Value::SingleQuotedString("\\".to_string()),
                span: crate::sqlparser::tokenizer::Span::empty(),
            });
        }
        // A collation naming byte order asks for exactly what DuckDB does with no
        // collation named, so the clause is dropped — as the read path drops it.
        //
        // Dropping rather than passing through is what makes all four spellings work.
        // Measured on DuckDB 1.5.5: `COLLATE "C"` and `COLLATE POSIX` happen to resolve,
        // but `ucs_basic` and `pg_catalog.default` are a `Catalog Error` raised by a
        // *shard*, after the coordinator accepted the statement — and
        // `pg_catalog.default` is the spelling drivers send. Any other collation is
        // refused before this ([`reject_duckdb_divergent`]), so no ordering is discarded.
        Expr::Collate { collation, .. } if is_byte_order_collation(collation) => {
            let Expr::Collate { expr: inner, .. } = std::mem::replace(expr, null()) else {
                unreachable!("matched a Collate");
            };
            *expr = *inner;
        }
        // An out-of-range array subscript. PostgreSQL answers NULL for an element and an
        // empty array for a slice; DuckDB counts a negative index back from the end, so
        // `a[-1]` read the last element and `a[-3:-1]` the whole array. The clamp is the
        // read path's own ([`crate::pgwire_handler::pg_subscripts`]) — a subscript stays a
        // subscript afterwards, which is what lets one rule serve both paths.
        Expr::CompoundFieldAccess { access_chain, .. } => {
            clamp_to_pg_semantics(access_chain);
        }
        // A cast to `bytea`. PostgreSQL runs the text through its own input conversion, so
        // `'\xDEADBEEF'::bytea` is the four bytes `DE AD BE EF`; DuckDB's `VARCHAR` →
        // `BLOB` reads its *own* escape syntax, where `\xDE` is one byte and `ADBEEF` six
        // characters, and stored seven. So the coordinator decodes the literal itself
        // ([`vairedb_common::bytea_in`], the read-path UDF's decoder) and emits hex.
        //
        // `unhex` and not `X'…'`: measured on DuckDB 1.5.5, `X'DEADBEEF'` is the *VARCHAR*
        // `xDEADBEEF` — nine characters, not four bytes. `unhex('…')` is a `BLOB`.
        //
        // Only a literal reaches here ([`reject_duckdb_divergent`] refused the rest), so a
        // decode failure is impossible and the arm falls through rather than raising.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type: DataType::Bytea,
            expr: inner,
            ..
        } => {
            let Expr::Value(value) = inner.as_ref() else {
                return;
            };
            let Value::SingleQuotedString(text) = &value.value else {
                return;
            };
            let Ok(bytes) = bytea_in::decode(text) else {
                return;
            };
            *expr = call("unhex", vec![string(&hex_of(&bytes))]);
        }
        // A zero divisor. PostgreSQL raises `22012` and writes nothing; DuckDB — under the
        // `integer_division` setting the shards run with, the one that makes `7/2` answer
        // `3` — answers **NULL** for `7/0`, `7.0/0` and `7 % 0` alike, so the statement
        // stored a NULL and reported success. The setting cannot fix it: one flag decides
        // both behaviours.
        //
        // The guard is a `CASE` around the operator because DuckDB has no setting that
        // turns a zero divisor into an error (checked against `duckdb_settings()` on 1.5.5)
        // and `error()` is its only way to raise from an expression. Three properties were
        // measured, not assumed:
        //
        // * `error()` unifies to the `ELSE` branch's type, so `7/2` is still `INTEGER` and
        //   `7.5/2` `DOUBLE` — the guard does not change the type a client binds.
        // * It evaluates per row rather than folding at bind time: no zero divisor in the
        //   table raises nothing, and an empty table raises nothing even for a literal `0`.
        // * A repeated `$n` binds once, so duplicating a placeholder divisor is safe —
        //   which matters because `renumber_placeholders` maps each original index to one
        //   new one and the shard binds positionally.
        //
        // The divisor is evaluated twice, which is why [`reject_duckdb_divergent`] refuses
        // a divisor that is itself a division: it would duplicate its own guard, and a
        // right-nested chain would grow the statement exponentially.
        Expr::BinaryOp {
            op: BinaryOperator::Divide | BinaryOperator::Modulo,
            ..
        } => {
            let Expr::BinaryOp { left, op, right } = std::mem::replace(expr, null()) else {
                unreachable!("matched a BinaryOp");
            };
            *expr = guarded_division(*left, op, *right);
        }
        // `SIMILAR TO` is its own wildcard language, and DuckDB hands the pattern straight
        // to a regex engine — wrong both ways at once, under-matching `%` and over-matching
        // `.`. The translation is the read path's own, and the regex it returns is anchored,
        // which is what lets partial-match `regexp_matches` give the whole-string answer.
        Expr::SimilarTo {
            negated,
            pattern,
            escape_char,
            ..
        } => {
            let Ok(regex) = similar_to_regex_from_ast(pattern, escape_char.as_ref()) else {
                return;
            };
            let negated = *negated;
            let Expr::SimilarTo { expr: target, .. } = std::mem::replace(expr, null()) else {
                unreachable!("matched a SimilarTo");
            };
            *expr = negate_if(
                negated,
                call("regexp_matches", vec![*target, string(&regex)]),
            );
        }
        _ => {}
    }
}

/// `CASE WHEN divisor = 0 THEN error('division by zero') ELSE dividend op divisor END`.
///
/// The message is PostgreSQL's own wording, and the core node classifies it back into
/// `22012` on the way out, so a client reads the same error and the same SQLSTATE from a
/// write as from a `SELECT`.
fn guarded_division(dividend: Expr, op: BinaryOperator, divisor: Expr) -> Expr {
    let zero = Expr::value(Value::Number("0".to_string(), false));
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::BinaryOp {
                left: Box::new(divisor.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(zero),
            },
            result: call("error", vec![string("division by zero")]),
        }],
        else_result: Some(Box::new(Expr::BinaryOp {
            left: Box::new(dividend),
            op,
            right: Box::new(divisor),
        })),
    }
}

/// `NOT inner`, or `inner` — the negation of a call, where the operator was negated.
fn negate_if(negated: bool, inner: Expr) -> Expr {
    if negated {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(inner),
        }
    } else {
        inner
    }
}

/// A `NULL` literal, used only as the placeholder [`std::mem::replace`] leaves behind
/// while a node is taken apart.
fn null() -> Expr {
    Expr::value(Value::Null)
}

/// A single-quoted string literal.
fn string(s: &str) -> Expr {
    Expr::value(Value::SingleQuotedString(s.to_string()))
}

/// `bytes` as uppercase hexadecimal, the argument `unhex` takes.
fn hex_of(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        // Infallible into a `String`, and the alternative — `map` plus `collect` — allocates
        // one `String` per byte.
        let _ = write!(out, "{b:02X}");
        out
    })
}

/// Build a plain `name(args…)` call — no `DISTINCT`, no `FILTER`, no window.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
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

fn transform_data_type(dt: &mut DataType) {
    match dt {
        DataType::Bytea => {
            *dt = DataType::Blob(None);
        }
        DataType::JSONB => {
            *dt = DataType::JSON;
        }
        // `T[n]` becomes `T[]`. PostgreSQL accepts the declared length and then ignores
        // it — "the current implementation does not enforce the declared number of
        // elements" — whereas DuckDB takes it literally and stores a fixed-size array,
        // which is a different Arrow type (`FixedSizeList`) that arrow-pg cannot encode
        // and that the schema rebuild rejects. Keeping the length would mean every read
        // of the column fails; dropping it costs a constraint PostgreSQL never applied.
        DataType::Array(ArrayElemTypeDef::SquareBracket(element, Some(_))) => {
            *dt = DataType::Array(ArrayElemTypeDef::SquareBracket(element.clone(), None));
        }
        _ => {}
    }
    // Element types are rewritten too, so `BYTEA[]` reaches the shards as `BLOB[]`.
    if let DataType::Array(
        ArrayElemTypeDef::SquareBracket(element, _)
        | ArrayElemTypeDef::AngleBracket(element)
        | ArrayElemTypeDef::Parenthesis(element),
    ) = dt
    {
        transform_data_type(element);
    }
}

fn transform_function(func: &mut Function) {
    let func_name = func.name.to_string().to_uppercase();

    if func_name == "TO_CHAR" {
        func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("STRFTIME"))]);
        if let FunctionArguments::List(ref mut arg_list) = func.args {
            let args = &mut arg_list.args;
            if args.len() == 2 {
                // PG `TO_CHAR(value, format)` -> DuckDB `STRFTIME(format, value)`.
                args.swap(0, 1);
                // The PG format template now sits at position 0.
                translate_format_arg(&mut args[0]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{parse_one, statement_to_sql};
    use super::*;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;
    use vairedb_common::proto::vairedb::v1::VdbErrorCode;

    /// Parse without going through [`crate::pgwire_handler::parser::parse_sql`].
    ///
    /// The refusal is *part of* `parse_sql`, called on the verbatim AST the moment a
    /// statement is parsed. So a refusal test routed through `parse_sql` would never
    /// reach the function under test — the parse would fail first with the very error
    /// being asserted, and the test would assert nothing. A `PostgreSqlDialect` parse
    /// yields the same verbatim AST `parse_sql` hands the refusal for a write or DDL.
    ///
    /// The *translation* tests use `parse_one`: that half runs at render time, after
    /// `parse_sql`, so its input really is `parse_sql`'s output.
    fn parse_verbatim(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap_or_else(|e| panic!("`{sql}` should tokenize: {e}"))
            .parse_statements()
            .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
            .remove(0)
    }

    fn rejected(sql: &str) -> String {
        match reject_duckdb_divergent(&parse_verbatim(sql)) {
            Ok(()) => panic!("`{sql}` should be refused"),
            Err(e) => e.to_string(),
        }
    }

    fn accepted(sql: &str) {
        if let Err(e) = reject_duckdb_divergent(&parse_verbatim(sql)) {
            panic!("`{sql}` should be accepted: {e}");
        }
    }

    /// The SQL text a shard would receive for `sql`.
    fn rendered(sql: &str) -> String {
        let mut stmt = parse_one(sql);
        transform_to_duckdb(&mut stmt);
        statement_to_sql(&stmt)
    }

    // --- casts to bytea: a literal is translated, anything else refused ---

    /// A decodable string literal is the shape the translation handles, in both
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

    /// And the translation in fact emits the bytes PostgreSQL's own input conversion
    /// reads, as `unhex` rather than `X'…'` — which DuckDB 1.5.5 reads as a VARCHAR.
    #[test]
    fn a_bytea_literal_is_rendered_as_the_bytes_postgresql_reads() {
        assert!(
            rendered("INSERT INTO t (b) VALUES ('\\xDEADBEEF'::bytea)")
                .contains("unhex('DEADBEEF')"),
            "got: {}",
            rendered("INSERT INTO t (b) VALUES ('\\xDEADBEEF'::bytea)")
        );
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
            let err = reject_duckdb_divergent(&parse_verbatim(sql)).expect_err("should be refused");
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
    /// translated to `BLOB`, not refused.
    #[test]
    fn a_declared_bytea_column_is_translated_not_refused() {
        for sql in [
            "CREATE TABLE t (b BYTEA)",
            "ALTER TABLE t ADD COLUMN b BYTEA",
        ] {
            accepted(sql);
            assert!(rendered(sql).contains("BLOB"), "got: {}", rendered(sql));
        }
        // Including as an element type, so `BYTEA[]` reaches the shards as `BLOB[]`.
        assert!(rendered("CREATE TABLE t (b BYTEA[])").contains("BLOB[]"));
    }

    // --- collations ---

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
    /// ask for what they are going to get, and the clause is dropped rather than passed
    /// on — `ucs_basic` and `pg_catalog.default` are a `Catalog Error` on DuckDB 1.5.5,
    /// and `pg_catalog.default` is the spelling a driver sends.
    #[test]
    fn a_byte_order_collation_is_accepted_and_dropped_before_the_shard_sees_it() {
        for collation in ["\"C\"", "\"POSIX\"", "ucs_basic", "pg_catalog.default"] {
            let sql = format!("UPDATE t SET x = 1 WHERE s COLLATE {collation} < 'a'");
            accepted(&sql);
            assert!(
                !rendered(&sql).to_uppercase().contains("COLLATE"),
                "`{sql}` still carries a collation: {}",
                rendered(&sql)
            );
        }
    }

    /// A *column* collation naming byte order is dropped on the same terms, in both the
    /// DDL statements that carry one. DuckDB knows none of PostgreSQL's names for byte
    /// order — even the two that happen to resolve are an accident — so passing the clause
    /// through is a `Catalog Error` raised by a shard after the coordinator said yes.
    #[test]
    fn a_byte_order_column_collation_is_dropped_before_the_shard_sees_it() {
        for collation in [
            "\"C\"",
            "POSIX",
            "ucs_basic",
            "\"default\"",
            "pg_catalog.default",
        ] {
            for sql in [
                format!("CREATE TABLE t (id INT, s VARCHAR COLLATE {collation})"),
                format!("ALTER TABLE t ADD COLUMN s VARCHAR COLLATE {collation}"),
            ] {
                let rendered = rendered(&sql);
                assert!(
                    !rendered.to_uppercase().contains("COLLATE"),
                    "{sql} still carries a collation: {rendered}"
                );
                assert!(rendered.contains("s VARCHAR"), "got: {rendered}");
            }
        }
    }

    /// Any other column collation is refused when the DDL is planned, so this render is
    /// never reached with one. The guard stays so that a future planner change shows up as
    /// a loud shard error rather than as an ordering silently replaced by byte order.
    #[test]
    fn any_other_column_collation_is_left_for_the_planner_to_refuse() {
        assert!(rendered("CREATE TABLE t (s VARCHAR COLLATE nocase)").contains("COLLATE nocase"));
    }

    // --- casts that would discard a length ---

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

    /// A column *declaration* of `VARCHAR(10)` is not a cast and is not refused: the
    /// length is part of the schema the client asked for, and DDL is a different
    /// question from an expression that discards one.
    #[test]
    fn a_declared_column_length_is_not_a_refused_cast() {
        accepted("CREATE TABLE t (s VARCHAR(3))");
        accepted("ALTER TABLE t ADD COLUMN s CHAR(2)");
    }

    // --- SIMILAR TO ---

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

    /// And the accepted ones become DuckDB's partial-match `regexp_matches` over the
    /// anchored regex PostgreSQL's wildcard language means, negation included.
    #[test]
    fn similar_to_is_rendered_as_an_anchored_regexp_match() {
        let sql = rendered("UPDATE t SET x = 1 WHERE s SIMILAR TO 'a%'");
        assert!(sql.contains("regexp_matches(s, '^(?:a.*)$')"), "got: {sql}");
        let sql = rendered("DELETE FROM t WHERE s NOT SIMILAR TO 'a_c'");
        assert!(
            sql.contains("NOT regexp_matches(s, '^(?:a.c)$')"),
            "got: {sql}"
        );
    }

    // --- the forms the translation owns ---

    /// These must *not* be refused — the two halves have to agree about which is
    /// responsible for what, or ordinary PostgreSQL stops working on the write path.
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

    /// PostgreSQL's `~` is a partial match and DuckDB's is a full one, so the operator
    /// becomes `regexp_matches`; the case-insensitive spellings carry the `'i'` flag and
    /// the negated ones a `NOT`.
    #[test]
    fn the_regex_operators_are_rendered_as_duckdbs_partial_match() {
        for (sql, expected) in [
            (
                "UPDATE t SET x = 1 WHERE s ~ '^a'",
                "regexp_matches(s, '^a')",
            ),
            (
                "UPDATE t SET x = 1 WHERE s !~ '^a'",
                "NOT regexp_matches(s, '^a')",
            ),
            (
                "UPDATE t SET x = 1 WHERE s ~* '^A'",
                "regexp_matches(s, '^A', 'i')",
            ),
            (
                "UPDATE t SET x = 1 WHERE s !~* '^A'",
                "NOT regexp_matches(s, '^A', 'i')",
            ),
        ] {
            assert!(
                rendered(sql).contains(expected),
                "`{sql}`: {}",
                rendered(sql)
            );
        }
    }

    /// DuckDB has no default `LIKE` escape, so the `\` PostgreSQL assumes is named
    /// explicitly — the shape every ORM emits when it escapes `_` or `%` in user input.
    #[test]
    fn like_is_given_postgresqls_default_escape() {
        assert!(
            rendered("UPDATE t SET x = 1 WHERE s LIKE 'a\\_b'").contains("ESCAPE '\\'"),
            "got: {}",
            rendered("UPDATE t SET x = 1 WHERE s LIKE 'a\\_b'")
        );
        // A client-supplied escape is left as it was.
        let sql = rendered("UPDATE t SET x = 1 WHERE s LIKE 'a!_b' ESCAPE '!'");
        assert!(sql.contains("ESCAPE '!'"), "got: {sql}");
    }

    // --- the zero-divisor guard ---

    /// DuckDB answers NULL for `7/0` under the `integer_division` setting the shards run,
    /// so the divisor is checked against zero and `error()` raises PostgreSQL's own
    /// message, which the core node classifies back into `22012`.
    #[test]
    fn a_division_is_wrapped_in_a_zero_divisor_guard() {
        for sql in ["UPDATE t SET x = a / b", "UPDATE t SET x = a % b"] {
            let rendered = rendered(sql);
            assert!(rendered.contains("CASE WHEN b = 0"), "`{sql}`: {rendered}");
            assert!(
                rendered.contains("error('division by zero')"),
                "`{sql}`: {rendered}"
            );
        }
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

    // --- everything else ---

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

    /// A `SELECT` is the read path's, and DataFusion speaks PostgreSQL — so the
    /// expression rewrites must not touch one. This is the statement-kind gate, and it is
    /// the reason `transform_to_duckdb` matches on the statement rather than just walking
    /// expressions.
    #[test]
    fn a_select_is_left_for_datafusion() {
        let rendered = rendered("SELECT a / b FROM t WHERE s ~ '^a' AND u LIKE 'a\\_b'");
        assert!(!rendered.contains("CASE WHEN"), "got: {rendered}");
        assert!(!rendered.contains("regexp_matches"), "got: {rendered}");
        assert!(!rendered.contains("ESCAPE"), "got: {rendered}");
    }

    /// PostgreSQL's `TO_CHAR(value, format)` is DuckDB's `STRFTIME(format, value)`, with
    /// the format template translated as well.
    #[test]
    fn to_char_becomes_strftime_with_its_arguments_swapped() {
        let sql = rendered("UPDATE t SET s = TO_CHAR(ts, 'YYYY-MM-DD')");
        assert!(sql.contains("STRFTIME('%Y-%m-%d', ts)"), "got: {sql}");
    }

    /// Coordinator-only table options name storage the shards do not have, and would be a
    /// syntax error on a node.
    #[test]
    fn coordinator_only_table_options_are_stripped() {
        let sql = rendered("CREATE TABLE t (id INT) WITH (fillfactor = 70)");
        assert!(!sql.contains("fillfactor"), "got: {sql}");
    }

    /// `JSONB` is DuckDB's `JSON`, and a declared array length is dropped: PostgreSQL
    /// ignores it, while DuckDB stores a `FixedSizeList` that arrow-pg cannot encode.
    #[test]
    fn postgresql_only_types_are_mapped_to_duckdb_equivalents() {
        assert!(rendered("CREATE TABLE t (j JSONB)").contains("JSON"));
        let sql = rendered("CREATE TABLE t (a INT[4])");
        assert!(
            sql.contains("INT[]") && !sql.contains("INT[4]"),
            "got: {sql}"
        );
    }
}
