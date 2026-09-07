//! Small cross-cutting helpers shared across coordinator modules.

use std::time::{SystemTime, UNIX_EPOCH};

use vairedb_common::proto::vairedb::v1::NodeState;

/// Current wall-clock time as whole seconds since the Unix epoch. Centralizes
/// the `SystemTime::now()` → epoch-duration conversion used wherever the
/// coordinator stamps heartbeats, registration times, and `created_at`.
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// The physical relation name a logical table's shards are built on, before the
/// shard suffix: the catalog key with its schema qualifier folded into the name
/// (`orders` → `orders`, `sales.orders` → `sales_orders`).
///
/// A core node has one flat DuckDB namespace and splices this name into SQL
/// *unquoted*, so a schema cannot be carried as a qualifier or as quoting — it has
/// to become part of a single plain identifier. Folding on `_` is not injective:
/// `sales.orders` and `sales_orders` fold together, which is why `CREATE TABLE`
/// refuses the second of two logical names that would share one physical name (see
/// `pgwire_handler::schemas::physical_name_conflict`).
pub fn physical_base_name(table_name: &str) -> String {
    table_name.replace('.', "_")
}

/// The physical, shard-local table name for a logical table on a given hash
/// bucket (e.g. `orders` bucket `3` → `orders_shard3`, `sales.orders` bucket `3` →
/// `sales_orders_shard3`). This naming is the contract between the coordinator
/// (which rewrites and routes SQL) and the storage nodes (which create the
/// per-shard DuckDB tables), so it must have a single definition.
///
/// `table_name` is a canonical catalog key (see
/// `pgwire_handler::query_router::canonical_table_name`); the write path reaches
/// the same string from the statement's AST via
/// `write_sql_cl::rewrite_to_shard_local`, and the two must agree byte for byte.
pub fn shard_table_name(table_name: &str, hash_bucket: u32) -> String {
    format!("{}_shard{}", physical_base_name(table_name), hash_bucket)
}

/// The logical shard identifier for the `index`-th shard of a table (e.g. index
/// `3` → `shard3`). Distinct from [`shard_table_name`]: this is the
/// table-agnostic `shard_id` stored in `ShardMeta`, whereas `shard_table_name`
/// is the physical per-table DuckDB relation. Shared by shard assignment and the
/// distributed-plan codec so both number shards identically.
pub fn logical_shard_id(index: u32) -> String {
    format!("shard{}", index)
}

/// Canonical uppercase label for a `NodeState` enum discriminant. Used both for
/// the `vairedb_catalog.nodes` virtual table and for error detail messages, so
/// the textual form stays consistent everywhere a node state is shown.
pub fn node_state_str(value: i32) -> &'static str {
    match NodeState::try_from(value) {
        Ok(NodeState::Alive) => "ALIVE",
        Ok(NodeState::Suspect) => "SUSPECT",
        Ok(NodeState::Dead) => "DEAD",
        _ => "UNSPECIFIED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_table_name_formats_bucket() {
        assert_eq!(shard_table_name("orders", 3), "orders_shard3");
        assert_eq!(shard_table_name("t", 0), "t_shard0");
    }

    // A schema-qualified key has to arrive at the node as one plain identifier,
    // because the node splices it into SQL unquoted.
    #[test]
    fn shard_table_name_folds_the_schema_into_the_identifier() {
        assert_eq!(shard_table_name("sales.orders", 2), "sales_orders_shard2");
        assert_eq!(physical_base_name("sales.orders"), "sales_orders");
        assert_eq!(physical_base_name("orders"), "orders");
    }

    #[test]
    fn node_state_str_known_values() {
        assert_eq!(node_state_str(NodeState::Alive as i32), "ALIVE");
        assert_eq!(node_state_str(NodeState::Suspect as i32), "SUSPECT");
        assert_eq!(node_state_str(NodeState::Dead as i32), "DEAD");
    }

    #[test]
    fn node_state_str_unknown_value() {
        assert_eq!(node_state_str(99), "UNSPECIFIED");
    }

    #[test]
    fn now_unix_secs_is_plausible() {
        // Sanity: after 2020-01-01 and before some far-future bound.
        let now = now_unix_secs();
        assert!(now > 1_577_836_800);
    }
}
