//! Rewrites a PostgreSQL-dialect AST in place so it executes on the storage
//! nodes' DuckDB engine: PostgreSQL-only types are mapped to DuckDB equivalents,
//! coordinator-only table options are stripped, and a few function-name
//! differences are bridged.

use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, CreateTableOptions, DataType, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, ObjectNamePart, SetExpr,
    Statement, Value, visit_expressions_mut, visit_relations_mut,
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
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
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
        Statement::Update(update) => {
            for assignment in &mut update.assignments {
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

/// Rewrite the format string of every `to_char` call in a SELECT (read path) from
/// PostgreSQL template patterns to strftime `%`-specifiers, keeping the function
/// name and argument order. DataFusion executes read-path projections with its
/// native `to_char`, which formats via chrono/strftime specifiers, so only the
/// format literal needs translating.
pub fn transform_to_char_format_for_read(stmt: &mut Statement) {
    let _ = visit_expressions_mut(stmt, |expr| {
        if let Expr::Function(func) = expr
            && func.name.to_string().eq_ignore_ascii_case("to_char")
            && let FunctionArguments::List(ref mut arg_list) = func.args
            && arg_list.args.len() == 2
        {
            // PG/DataFusion `to_char(value, format)`: the format is at position 1.
            translate_format_arg(&mut arg_list.args[1]);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Collapse every schema-qualified relation in `stmt` to just its last
/// identifier part (preserving that part's quote style) so a `SELECT ... FROM
/// schema.tbl` resolves against the bare table name the provider is registered
/// under. The coordinator namespace is flat: a `schema.` qualifier is only a
/// user-facing prefix, not a real DataFusion schema, so a `Partial{schema,tbl}`
/// reference would otherwise fail to resolve.
///
/// Only multi-part relations are touched; single-part names (the common case)
/// are left byte-identical. Apply on the read path only for non-catalog queries,
/// so `vairedb_catalog.*` / `pg_catalog.*` references keep their qualifier.
pub fn collapse_schema_qualified_relations(stmt: &mut Statement) {
    let _ = visit_relations_mut(stmt, |relation| {
        if relation.0.len() > 1
            && let Some(part) = relation.0.last()
            && part.as_ident().is_some()
        {
            relation.0 = vec![relation.0.pop().unwrap()];
        }
        ControlFlow::<()>::Continue(())
    });
}

/// If `arg` is a single-quoted string literal, translate it in place from a
/// PostgreSQL datetime template to strftime `%`-specifiers.
fn translate_format_arg(arg: &mut FunctionArg) {
    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))) = arg
        && let Value::SingleQuotedString(fmt) = &v.value
    {
        v.value = Value::SingleQuotedString(translate_pg_datetime_format(fmt));
    }
}

/// Translate a PostgreSQL `TO_CHAR` datetime template into strftime `%`-specifiers.
/// chrono (DataFusion's `to_char`) and DuckDB's `strftime` share this specifier
/// syntax, so the same output serves both the read and write paths.
///
/// Recognized patterns are matched longest-first; a leading `FM` fill-mode prefix
/// on a token is dropped (no specifier equivalent); unrecognized characters pass
/// through literally, and a literal `%` is escaped to `%%`.
fn translate_pg_datetime_format(pg: &str) -> String {
    // (PG pattern, strftime specifier), ordered longest-first within each prefix
    // group so greedy matching picks e.g. YYYY over YY and HH24 over HH.
    const PATTERNS: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MONTH", "%B"),
        ("Month", "%B"),
        ("month", "%B"),
        ("MON", "%b"),
        ("Mon", "%b"),
        ("mon", "%b"),
        ("MM", "%m"),
        ("MI", "%M"),
        ("DDD", "%j"),
        ("DD", "%d"),
        ("DAY", "%A"),
        ("Day", "%A"),
        ("day", "%A"),
        ("DY", "%a"),
        ("Dy", "%a"),
        ("dy", "%a"),
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),
        ("SS", "%S"),
        ("AM", "%p"),
        ("PM", "%p"),
        ("am", "%p"),
        ("pm", "%p"),
        ("TZ", "%Z"),
    ];

    let bytes = pg.as_bytes();
    let mut out = String::with_capacity(pg.len() + 8);
    let mut i = 0;
    while i < bytes.len() {
        let rest = &pg[i..];
        // Drop a fill-mode prefix; it has no strftime equivalent.
        if rest.starts_with("FM") {
            i += 2;
            continue;
        }
        if let Some((pat, spec)) = PATTERNS.iter().find(|(pat, _)| rest.starts_with(pat)) {
            out.push_str(spec);
            i += pat.len();
            continue;
        }
        if bytes[i] == b'%' {
            out.push_str("%%");
            i += 1;
            continue;
        }
        let ch = rest.chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}
