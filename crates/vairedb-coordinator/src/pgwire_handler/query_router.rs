//! SQL statement inspection used to decide how to route a query: classifying a
//! parsed statement and extracting the target table name from it.

use crate::sqlparser::ast::{
    FromTable, Ident, ObjectName, SetExpr, Statement, TableFactor, TableObject,
};

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

/// Reduce a (possibly quoted or schema-qualified) relation to its single
/// canonical logical table name — the name used as the catalog key, the physical
/// shard-name input, and the DataFusion registration key.
///
/// Only the LAST identifier part is kept (a `schema.tbl` qualifier is dropped:
/// the coordinator's namespace is flat). Normalization mirrors PostgreSQL/
/// DataFusion identifier folding: an unquoted part is lowercased, a quoted part
/// is taken verbatim so its case survives. Returns `None` if the last part is
/// not a plain identifier (e.g. a function part).
pub fn canonical_table_name(name: &ObjectName) -> Option<String> {
    let ident: &Ident = name.0.last()?.as_ident()?;
    Some(canonicalize_ident(ident))
}

/// Canonical logical name for a single identifier: verbatim when quoted,
/// lowercased when unquoted.
pub fn canonicalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Return the target table name for a write or DDL statement (INSERT, UPDATE,
/// DELETE, CREATE/ALTER/DROP TABLE), or `None` if the statement has no single
/// resolvable target. The name is canonicalized via [`canonical_table_name`].
/// Use `extract_select_table_name` for SELECTs.
pub fn extract_table_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => canonical_table_name(name),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => canonical_table_name(name),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) => t,
                FromTable::WithoutKeyword(t) => t,
            };
            match &tables.first()?.relation {
                TableFactor::Table { name, .. } => canonical_table_name(name),
                _ => None,
            }
        }
        Statement::CreateTable(create) => canonical_table_name(&create.name),
        Statement::AlterTable(alter) => canonical_table_name(&alter.name),
        Statement::Drop { names, .. } => names.first().and_then(canonical_table_name),
        _ => None,
    }
}

/// Return the canonical table name of a simple top-level SELECT's first FROM
/// relation, or `None` for non-SELECT statements, set operations, or non-table
/// sources (subqueries, joins, table functions).
pub fn extract_select_table_name(stmt: &Statement) -> Option<String> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let table_with_joins = select.from.first()?;
    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => canonical_table_name(name),
        _ => None,
    }
}
