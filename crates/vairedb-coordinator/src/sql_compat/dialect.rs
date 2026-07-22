//! Rewrites a PostgreSQL-dialect AST in place so it executes on the storage
//! nodes' DuckDB engine: PostgreSQL-only types are mapped to DuckDB equivalents,
//! coordinator-only table options are stripped, and a few function-name
//! differences are bridged.

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, CreateTableOptions, DataType, Expr, Function,
    FunctionArguments, ObjectName, ObjectNamePart, SetExpr, Statement,
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
        Statement::AlterTable { operations, .. } => {
            for op in operations {
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
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_) => {
            transform_exprs_in_statement(stmt);
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

fn transform_exprs_in_statement(stmt: &mut Statement) {
    match stmt {
        Statement::Insert(insert) => {
            if let Some(source) = &mut insert.source {
                transform_expr_in_set_expr(source.body.as_mut());
            }
        }
        Statement::Update { assignments, .. } => {
            for assignment in assignments {
                transform_expr(&mut assignment.value);
            }
        }
        _ => {}
    }
}

fn transform_expr_in_set_expr(set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    transform_expr(expr);
                }
            }
        }
        SetExpr::Select(select) => {
            if let Some(selection) = &mut select.selection {
                transform_expr(selection);
            }
        }
        _ => {}
    }
}

fn transform_expr(expr: &mut Expr) {
    if let Expr::Function(func) = expr {
        transform_function(func);
    }
}

fn transform_function(func: &mut Function) {
    let func_name = func.name.to_string().to_uppercase();

    if func_name == "TO_CHAR" {
        func.name = ObjectName(vec![ObjectNamePart::Identifier(
            sqlparser::ast::Ident::new("STRFTIME"),
        )]);
        if let FunctionArguments::List(ref mut arg_list) = func.args {
            let args = &mut arg_list.args;
            if args.len() == 2 {
                args.swap(0, 1);
            }
        }
    }
}
