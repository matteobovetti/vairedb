//! PostgreSQL-dialect SQL compatibility for the sharded coordinator. Splits into:
//! - `dialect` — rewrite a parsed statement to DuckDB-compatible form.
//! - `shard_routing` — decide which shard(s) a write targets.
//! - `routing_value` — canonicalize a shard-key value for stable hashing.
//! - `statement` — validate/split/renumber write statements for sharding.
//!
//! The parse/render entrypoints and shard-local relation rewrite live here as the
//! shared primitives the submodules build on.

mod dialect;
mod routing_value;
mod shard_routing;
mod statement;

pub use dialect::transform_to_duckdb;
pub use shard_routing::{ShardRouting, extract_shard_key_value, route_target};
pub use statement::{
    extract_insert_row_shard_keys, max_placeholder_index, renumber_placeholders,
    split_insert_by_rows, update_targets_shard_key, validate_insert_shard_key,
};

use std::ops::ControlFlow;

use sqlparser::ast::{ObjectNamePart, Statement, visit_relations_mut};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::Result;

/// Parse a SQL string into statements using the PostgreSQL dialect.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    Ok(statements)
}

/// Suffix every relation name in `stmt` with `_{shard_suffix}` so the statement
/// targets the shard-local DuckDB table (e.g. `orders` → `orders_shard3`).
pub fn rewrite_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    let _ = visit_relations_mut(stmt, |relation| {
        if let Some(ObjectNamePart::Identifier(ident)) = relation.0.last_mut() {
            ident.value = format!("{}_{}", ident.value, shard_suffix);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Render a statement back to its SQL text form.
pub fn statement_to_sql(stmt: &Statement) -> String {
    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn placeholder_survives_shard_rewrite_and_render() {
        let mut stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        rewrite_to_shard_local(&mut stmt, "shard3");
        transform_to_duckdb(&mut stmt);
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("t_shard3"), "got: {sql}");
        assert!(sql.contains("$1") && sql.contains("$2"), "got: {sql}");
    }
}
