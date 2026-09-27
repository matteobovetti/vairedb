//! Small cross-cutting helpers shared across coordinator modules.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vairedb_common::proto::vairedb::v1::NodeState;

use crate::sqlparser::ast::{Ident, ObjectName};

/// Current wall-clock time as a protobuf `Timestamp`, at the full precision the
/// message carries. Centralizes the `SystemTime::now()` → epoch-duration
/// conversion used wherever the coordinator stamps heartbeats, registration
/// times, and `created_at`.
///
/// A clock reading before the Unix epoch is stamped as the epoch rather than
/// panicking. Every caller is recording when something happened, and one of them
/// runs inside the spawned task that serves a node's heartbeat stream: a panic
/// there takes that node's liveness down — the coordinator stops refreshing it
/// and the failure detector buries it — while the cluster is in fact whole and
/// the only trace is a backtrace in one task. The core node's heartbeat sender
/// already reads its clock this way.
pub fn now_timestamp() -> prost_types::Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "system clock is before the Unix epoch, stamping the epoch");
            Duration::ZERO
        });
    prost_types::Timestamp {
        seconds: since_epoch.as_secs() as i64,
        nanos: since_epoch.subsec_nanos() as i32,
    }
}

/// Current wall-clock time as whole seconds since the Unix epoch, from the same
/// reading as [`now_timestamp`] so the two cannot disagree about what "now" is.
pub fn now_unix_secs() -> u64 {
    now_timestamp().seconds as u64
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

/// The single identifier naming an INSERT column-list entry, or `None` if the
/// entry is not one plain identifier.
///
/// sqlparser models an INSERT column list as `Vec<ObjectName>`, so it accepts
/// dotted and function-valued forms (`INSERT INTO t (addr.city) …`) that
/// PostgreSQL reads as writing into a composite field. VaireDB does not
/// implement those, and a caller that took only the last part would silently
/// target the wrong column — so this returns `None` and the caller refuses the
/// statement.
///
/// Returns the [`Ident`] rather than its text so callers keep the quoting needed
/// by `pgwire_handler::query_router::canonicalize_ident`.
pub fn insert_column_ident(column: &ObjectName) -> Option<&Ident> {
    match column.0.as_slice() {
        [part] => part.as_ident(),
        _ => None,
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

    // Every state a node is shown in has a label, and the two different ways of
    // having no state share one. `Unspecified` is a discriminant the enum knows, so
    // it converts successfully and still falls through to the catch-all arm, which
    // an out-of-range integer reaches by failing to convert at all — worth covering
    // both, since the arm is written once and is easy to narrow to only one of them.
    #[test]
    fn node_state_str_labels_every_state_and_neither_kind_of_none() {
        assert_eq!(node_state_str(NodeState::Alive as i32), "ALIVE");
        assert_eq!(node_state_str(NodeState::Suspect as i32), "SUSPECT");
        assert_eq!(node_state_str(NodeState::Dead as i32), "DEAD");
        assert_eq!(node_state_str(NodeState::Unspecified as i32), "UNSPECIFIED");
        assert_eq!(node_state_str(99), "UNSPECIFIED");
    }

    // The two shard names are not interchangeable, which is the whole reason both
    // exist: `logical_shard_id` is the table-agnostic id stored in `ShardMeta`, and
    // using it where a relation name is needed would name a table no node created.
    #[test]
    fn a_logical_shard_id_names_the_shard_and_not_a_table() {
        assert_eq!(logical_shard_id(0), "shard0");
        assert_eq!(logical_shard_id(3), "shard3");
        assert_ne!(logical_shard_id(3), shard_table_name("orders", 3));
    }

    #[test]
    fn insert_column_ident_accepts_a_bare_identifier() {
        let plain = ObjectName::from(vec![Ident::new("email")]);
        assert_eq!(
            insert_column_ident(&plain).map(|i| i.value.as_str()),
            Some("email")
        );
    }

    // A dotted entry is a composite-field target in PostgreSQL. Returning the
    // last part would hash/route the wrong column, so it must not resolve.
    #[test]
    fn insert_column_ident_rejects_a_dotted_entry() {
        let dotted = ObjectName::from(vec![Ident::new("addr"), Ident::new("city")]);
        assert!(insert_column_ident(&dotted).is_none());
    }

    // Bounded on both sides by `now_timestamp`, which is the claim the two functions
    // make about each other: they read the same clock in the same unit. A one-sided
    // "later than 2020" bound is satisfied by any unit larger than seconds, so it would
    // pass on a millisecond count — the reading that looks like the year 51,000 and
    // would make every heartbeat arrive from the future. The epoch still needs the
    // lower bound, since a zero clock is inside the sandwich too.
    #[test]
    fn now_unix_secs_reads_the_same_clock_as_now_timestamp_in_the_same_unit() {
        let before = now_timestamp().seconds as u64;
        let secs = now_unix_secs();
        let after = now_timestamp().seconds as u64;

        assert!(
            (before..=after).contains(&secs),
            "{secs} is not between {before} and {after}"
        );
        assert!(secs > 1_577_836_800, "{secs} is before 2020-01-01");
    }
}
