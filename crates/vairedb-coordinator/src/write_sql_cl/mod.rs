//! Write-path SQL compatibility layer: PostgreSQL wire-protocol SQL → DuckDB,
//! translated directly without DataFusion in the middle. Splits into:
//! - `dialect` — rewrite a parsed statement to DuckDB-compatible form.
//! - `reject` — refuse the expressions DuckDB would answer differently from PostgreSQL
//!   and that `dialect` cannot rewrite.
//! - `shard_routing` — decide which shard(s) a write targets.
//! - `routing_value` — canonicalize a shard-key value for stable hashing.
//! - `statement` — validate/split/renumber write statements for sharding.
//! - `merge` — the shape rules and rewrites that let a `MERGE INTO` be applied
//!   shard by shard.
//! - `rows` — re-emit materialized result rows as a literal `INSERT ... VALUES`,
//!   so a write whose rows come from a query can be routed like any other.
//!
//! The shard-local relation rewrite and the SQL render live here as the shared
//! primitives the submodules build on.
//!
//! Everything here is write-path only. The statements it operates on are parsed by
//! [`crate::pgwire_handler::parser::parse_sql`], which serves both paths and therefore
//! lives at the protocol layer alongside the read-path rewrites.

mod dialect;
mod merge;
mod reject;
mod routing_value;
mod rows;
mod shard_routing;
mod statement;

pub use dialect::transform_to_duckdb;
pub use merge::{
    MergeShape, MergeSource, ensure_merge_relation_aliases, materialize_merge_insert_columns,
    merge_has_not_matched_by_source, merge_key_column, merge_row_shard_keys, merge_shape,
    normalize_merge_column_qualifiers, split_merge_by_rows, validate_merge,
};
pub use reject::reject_duckdb_divergent;
pub use rows::{ROWS_PER_STATEMENT, insert_statements_from_batches, insert_template};
pub use shard_routing::{ShardRouting, extract_shard_key_value, route_target};
pub use statement::{
    extract_insert_row_shard_keys, insert_omits_column_list, insert_source_is_query,
    insert_source_tables, insert_values_row_count, materialize_insert_columns,
    materialize_insert_columns_for_arity, max_placeholder_index, relations_read,
    renumber_placeholders, split_insert_by_rows, update_targets_shard_key,
    validate_insert_shard_key, validate_on_conflict,
};

use std::ops::ControlFlow;

use crate::sqlparser::ast::{Ident, ObjectNamePart, Statement, visit_relations_mut};

use crate::pgwire_handler::query_router::canonical_table_name;
use crate::util::physical_base_name;

/// Rewrite every relation in `stmt` to its bare shard-local table name so the
/// statement targets the physical per-shard DuckDB table (e.g. `orders` →
/// `orders_shard3`, `sales.orders` → `sales_orders_shard3`).
///
/// The whole (possibly quoted or schema-qualified) relation is collapsed to a
/// single unquoted identifier `{physical_base}_{shard_suffix}`, where
/// `physical_base` is [`physical_base_name`] of the relation's canonical catalog
/// key. That keeps the physical name a plain identifier that byte-matches
/// `util::shard_table_name` on the same key — the storage node splices the name
/// into SQL unquoted, so it must carry neither quote characters nor a `.`.
pub fn rewrite_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    let _ = visit_relations_mut(stmt, |relation| {
        if let Some(key) = canonical_table_name(relation) {
            let shard_local = format!("{}_{}", physical_base_name(&key), shard_suffix);
            relation.0 = vec![ObjectNamePart::Identifier(Ident::new(shard_local))];
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Suffix a `CREATE INDEX`'s *own* name with `shard_suffix`, the way
/// [`rewrite_to_shard_local`] suffixes the relations it reads.
///
/// Separate from that rewrite because `CreateIndex.name` is not a relation
/// reference — `visit_relations_mut` never reaches it, while `table_name` beside
/// it is reached — so one pass cannot do both. Left as-is, every shard on a node
/// would try to create an index of the same name and all but the first would
/// collide: N shards, one index.
///
/// Names an unquoted identifier `{physical_base}_{shard_suffix}` for the same
/// reason [`rewrite_to_shard_local`] does: the storage node splices the name into
/// SQL unquoted, and the suffix has to be recomputable from the logical name when
/// `DROP INDEX` comes to remove it. A no-op for any other statement.
pub fn rewrite_index_name_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    if let Statement::CreateIndex(create) = stmt
        && let Some(name) = &create.name
        && let Some(key) = canonical_table_name(name)
    {
        let shard_local = format!("{}_{}", physical_base_name(&key), shard_suffix);
        create.name = Some(crate::sqlparser::ast::ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new(shard_local)),
        ]));
    }
}

/// Render a statement back to its SQL text form.
///
/// **Write path only.** This is the last render in the system and it exists solely
/// because the write path ships SQL *text* over gRPC for a core node to execute on
/// DuckDB (see `write_router::generate_shard_local_sql`). The read path hands the
/// AST to DataFusion directly, so no VaireDB component re-parses this output —
/// which matters because `Display` is lossy (`0x1F` renders as `X'1F'`).
pub fn statement_to_sql(stmt: &Statement) -> String {
    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

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
