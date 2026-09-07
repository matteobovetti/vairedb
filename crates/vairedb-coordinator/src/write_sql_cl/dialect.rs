//! Rewrites a PostgreSQL-dialect AST in place so it executes on the storage
//! nodes' DuckDB engine: PostgreSQL-only types are mapped to DuckDB equivalents,
//! coordinator-only table options are stripped, and a few function-name
//! differences are bridged.

use std::ops::ControlFlow;

use crate::pgwire_handler::parser::translate_format_arg;
use crate::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, CreateTableOptions, DataType, Expr, Function,
    FunctionArguments, MergeClauseKind, ObjectName, ObjectNamePart, OrderByOptions, Statement,
    visit_expressions_mut,
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
            merge.optimizer_hint = None;
            for clause in &mut merge.clauses {
                if clause.clause_kind == MergeClauseKind::NotMatched {
                    clause.clause_kind = MergeClauseKind::NotMatchedByTarget;
                }
            }
            let _ = visit_expressions_mut(merge, |expr| {
                if let Expr::Function(func) = expr {
                    transform_function(func);
                }
                ControlFlow::<()>::Continue(())
            });
        }
        _ => {}
    }
}

fn transform_data_type(dt: &mut DataType) {
    match dt {
        DataType::Bytea => {
            *dt = DataType::Blob(None);
        }
        DataType::JSONB => {
            *dt = DataType::JSON;
        }
        _ => {}
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
