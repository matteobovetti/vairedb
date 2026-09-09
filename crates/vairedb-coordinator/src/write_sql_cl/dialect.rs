//! Rewrites a PostgreSQL-dialect AST in place so it executes on the storage
//! nodes' DuckDB engine: PostgreSQL-only types are mapped to DuckDB equivalents,
//! coordinator-only table options are stripped, and a few function-name
//! differences are bridged.

use std::ops::ControlFlow;

use crate::pgwire_handler::parser::translate_format_arg;
use crate::pgwire_handler::pg_operators::{is_byte_order_collation, similar_to_regex_from_ast};
use crate::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ArrayElemTypeDef, BinaryOperator, CaseWhen,
    CreateTableOptions, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, MergeClauseKind, ObjectName, ObjectNamePart,
    OrderByOptions, Statement, UnaryOperator, Value, ValueWithSpan,
    helpers::attached_token::AttachedToken, visit_expressions_mut,
};

/// Rewrite `stmt` in place from PostgreSQL dialect to DuckDB-compatible form.
pub fn transform_to_duckdb(stmt: &mut Statement) {
    match stmt {
        Statement::CreateTable(create) => {
            for col in &mut create.columns {
                transform_data_type(&mut col.data_type);
            }
            create.table_options = CreateTableOptions::None;
        }
        // DuckDB has one index type (ART) and none of PostgreSQL's index
        // decorations. Every one of them describes *how* the index is built, not
        // which rows it admits, so dropping them changes performance and nothing
        // else — the exception is anything that narrows a UNIQUE index's scope,
        // which would weaken a constraint the client asked for and is refused
        // upstream in `indexes::plan_create_index` instead of being stripped here.
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
        // Every expression of a write statement, at any depth: an INSERT's value
        // rows, an UPDATE's assignments, and the WHERE clause of either or of a
        // DELETE. The statement-kind gate is the point — the read path hands its
        // AST to DataFusion, which speaks PostgreSQL, so rewriting a SELECT here
        // would break it.
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            let _ = visit_expressions_mut(stmt, |expr| {
                if let Expr::Function(func) = expr {
                    transform_function(func);
                }
                transform_pg_semantics(expr);
                ControlFlow::<()>::Continue(())
            });
        }
        // A MERGE's expressions are rewritten like any other write's, plus two
        // spelling differences. DuckDB's grammar requires the `INTO` keyword that
        // PostgreSQL's makes optional, so a client that omitted it would otherwise
        // get a syntax error from a node. And `WHEN NOT MATCHED` already means `BY
        // TARGET` in both dialects — writing it out removes the reliance on the
        // shards' engine defaulting the same way. Any optimizer hint goes: it named
        // a plan for an engine that is not the one running the statement.
        Statement::Merge(merge) => {
            merge.into = true;
            merge.optimizer_hints.clear();
            for clause in &mut merge.clauses {
                if clause.clause_kind == MergeClauseKind::NotMatched {
                    clause.clause_kind = MergeClauseKind::NotMatchedByTarget;
                }
            }
            let _ = visit_expressions_mut(merge, |expr| {
                if let Expr::Function(func) = expr {
                    transform_function(func);
                }
                transform_pg_semantics(expr);
                ControlFlow::<()>::Continue(())
            });
        }
        _ => {}
    }
}

/// Rewrite the expressions DuckDB reads differently from PostgreSQL into DuckDB
/// spellings that mean what PostgreSQL means.
///
/// These are not spelling differences — each one is a *silently different answer*. A
/// write is executed verbatim by a shard, so before this the predicate a client wrote and
/// the predicate that ran were not the same predicate, and the row count came back as
/// though they were. Each rewrite below was checked against DuckDB 1.5.5.
///
/// What cannot be rewritten is refused instead ([`super::reject`]) — including the one
/// input this function needs and cannot supply itself, a literal `SIMILAR TO` pattern.
/// Because that refusal runs first, at parse time, an untranslatable expression never
/// reaches here and this function is infallible: anything it does not recognize is left
/// exactly as it was.
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
        // default escape at all, so `s LIKE 'a\_b'` matched on a wildcard where
        // PostgreSQL matched a literal underscore — the shape every ORM emits when it
        // escapes `_` or `%` in user input. Naming the escape explicitly makes DuckDB
        // read the pattern PostgreSQL's way, and unlike a refusal it works for a pattern
        // that arrives as a parameter, where the backslash is in the value and cannot be
        // seen here at all.
        Expr::Like { escape_char, .. } | Expr::ILike { escape_char, .. }
            if escape_char.is_none() =>
        {
            *escape_char = Some(ValueWithSpan {
                value: Value::SingleQuotedString("\\".to_string()),
                span: crate::sqlparser::tokenizer::Span::empty(),
            });
        }
        // A collation naming byte order asks for the comparison DuckDB performs when no
        // collation is named at all, so the clause is dropped and the comparison left
        // alone — the same thing the read path does with it once accepted.
        //
        // Dropping it rather than passing it through is what makes all four spellings
        // work. Measured on DuckDB 1.5.5: `COLLATE "C"` and `COLLATE POSIX` happen to
        // resolve, but `ucs_basic` and `pg_catalog.default` are a `Catalog Error` —
        // "Collation with name ... does not exist" — raised by a *shard*, after the
        // coordinator accepted the statement. `pg_catalog.default` is the spelling a
        // driver sends, which is the one that has to work.
        //
        // Any other collation is refused before this ([`super::reject`]), so this arm
        // never silently discards an ordering a client would have got.
        Expr::Collate { collation, .. } if is_byte_order_collation(collation) => {
            let Expr::Collate { expr: inner, .. } = std::mem::replace(expr, null()) else {
                unreachable!("matched a Collate");
            };
            *expr = *inner;
        }
        // A zero divisor. PostgreSQL raises `22012` and writes nothing; DuckDB — with
        // the `integer_division` setting the shards run under, which is what makes `7/2`
        // answer `3` — answers **NULL** for `7/0`, `7.0/0` and `7 % 0` alike. So the
        // statement stored a NULL, reported the rows it changed, and the client was told
        // it succeeded. That is the one split-brain row the write path's expression work
        // introduced, and the setting cannot be asked to fix it: the same flag decides
        // both behaviours.
        //
        // The guard is a `CASE` around the operator, because DuckDB has no setting that
        // turns a zero divisor into an error (checked against `duckdb_settings()` on
        // 1.5.5) and `error()` is its only way to raise one from an expression. Three
        // things about it were measured rather than assumed:
        //
        // * `error()` unifies to the type of the `ELSE` branch, so `7/2` still comes back
        //   `INTEGER` and `7.5/2` `DOUBLE` — the guard does not change the column type a
        //   client binds.
        // * It is evaluated per row, not folded at bind time: the same statement over a
        //   table with no zero divisor raises nothing, and over an empty table raises
        //   nothing even when the divisor is the literal `0`.
        // * A repeated `$n` binds once, so duplicating a placeholder divisor is safe —
        //   which matters because `renumber_placeholders` maps each original index to one
        //   new one and the shard binds positionally.
        //
        // The divisor is evaluated twice, which is why [`super::reject`] refuses the one
        // shape where that is not merely wasteful: a divisor that is itself a division
        // would duplicate its own guard, and a right-nested chain of them would grow the
        // statement exponentially.
        Expr::BinaryOp {
            op: BinaryOperator::Divide | BinaryOperator::Modulo,
            ..
        } => {
            let Expr::BinaryOp { left, op, right } = std::mem::replace(expr, null()) else {
                unreachable!("matched a BinaryOp");
            };
            *expr = guarded_division(*left, op, *right);
        }
        // `SIMILAR TO` is its own wildcard language, and DuckDB hands the pattern
        // straight to a regex engine — wrong in both directions at once, under-matching
        // on `%` and over-matching on `.`. The translation is the read path's own, so
        // both paths answer a `SIMILAR TO` from one implementation of PostgreSQL's rules;
        // the regex it returns is anchored, which is what lets DuckDB's partial-match
        // `regexp_matches` give the whole-string answer `SIMILAR TO` promises.
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
        func.name = ObjectName(vec![ObjectNamePart::Identifier(
            crate::sqlparser::ast::Ident::new("STRFTIME"),
        )]);
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
