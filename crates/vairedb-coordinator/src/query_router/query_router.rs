//! SQL statement inspection used to decide how to route a query: classifying a
//! parsed statement and extracting the target table name from it.

use sqlparser::ast::{FromTable, SetExpr, Statement, TableFactor, TableObject};

/// Coarse category of a SQL statement, used to choose between the read path
/// (scheduler) and the write path (write router), and to detect DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    AlterTable,
    DropTable,
    /// Anything not handled specially (e.g. transaction control, SET).
    Other,
}

/// Classify a parsed statement into its [`QueryType`].
pub fn classify_statement(stmt: &Statement) -> QueryType {
    match stmt {
        Statement::Query(_) => QueryType::Select,
        Statement::Insert(_) => QueryType::Insert,
        Statement::Update { .. } => QueryType::Update,
        Statement::Delete(_) => QueryType::Delete,
        Statement::CreateTable(_) => QueryType::CreateTable,
        Statement::AlterTable { .. } => QueryType::AlterTable,
        Statement::Drop { .. } => QueryType::DropTable,
        _ => QueryType::Other,
    }
}

/// Return the target table name for a write or DDL statement (INSERT, UPDATE,
/// DELETE, CREATE/ALTER/DROP TABLE), or `None` if the statement has no single
/// resolvable target. Use `extract_select_table_name` for SELECTs.
pub fn extract_table_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => Some(name.to_string()),
            _ => None,
        },
        Statement::Update { table, .. } => Some(table.relation.to_string()),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) => t,
                FromTable::WithoutKeyword(t) => t,
            };
            tables.first().map(|f| f.relation.to_string())
        }
        Statement::CreateTable(create) => Some(create.name.to_string()),
        Statement::AlterTable { name, .. } => Some(name.to_string()),
        Statement::Drop { names, .. } => names.first().map(|n| n.to_string()),
        _ => None,
    }
}

/// Return the table name of a simple top-level SELECT's first FROM relation, or
/// `None` for non-SELECT statements, set operations, or non-table sources
/// (subqueries, joins, table functions).
pub fn extract_select_table_name(stmt: &Statement) -> Option<String> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let table_with_joins = select.from.first()?;
    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => Some(name.to_string()),
        _ => None,
    }
}
