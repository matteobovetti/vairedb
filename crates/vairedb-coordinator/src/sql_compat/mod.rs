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

pub use dialect::{
    collapse_schema_qualified_relations, transform_to_char_format_for_read, transform_to_duckdb,
};
pub use shard_routing::{ShardRouting, extract_shard_key_value, route_target};
pub use statement::{
    extract_insert_row_shard_keys, max_placeholder_index, renumber_placeholders,
    split_insert_by_rows, update_targets_shard_key, validate_insert_shard_key,
};

use std::ops::ControlFlow;

use sqlparser::ast::{Ident, ObjectNamePart, Statement, visit_relations_mut};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::Result;
use crate::query_router::canonicalize_ident;

/// Parse a SQL string into statements using the PostgreSQL dialect.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    Ok(statements)
}

/// Rewrite every relation in `stmt` to its bare shard-local table name so the
/// statement targets the physical per-shard DuckDB table (e.g. `orders` →
/// `orders_shard3`).
///
/// The whole (possibly quoted or schema-qualified) relation is collapsed to a
/// single unquoted identifier `{canonical}_{shard_suffix}`, where `canonical` is
/// the [`canonicalize_ident`] form of the relation's last part. This keeps the
/// physical name a plain identifier that byte-matches `util::shard_table_name`
/// on the same canonical logical name — the storage node splices the name into
/// SQL unquoted, so it must not carry quote characters or a schema qualifier.
pub fn rewrite_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    let _ = visit_relations_mut(stmt, |relation| {
        if let Some(ident) = relation.0.last().and_then(|p| p.as_ident()) {
            let shard_local = format!("{}_{}", canonicalize_ident(ident), shard_suffix);
            relation.0 = vec![ObjectNamePart::Identifier(Ident::new(shard_local))];
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
