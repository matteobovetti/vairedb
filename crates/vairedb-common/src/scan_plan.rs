//! Serializable payload describing a single-shard DuckDB scan, exchanged
//! between the coordinator and core nodes via the Ballista physical codec.

use serde::{Deserialize, Serialize};

/// The wire form of a `DuckDbScanExec`: everything an executor needs to rebuild
/// and run a scan of one shard table.
///
/// The coordinator's planner encodes this into the physical plan; the core
/// node's codec decodes it and reconstructs the executable scan. The schema
/// travels as Arrow IPC bytes so both sides agree on column types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDbScanPlanBytes {
    /// Name of the shard table to scan.
    pub shard_table_name: String,
    /// The scan's output schema, encoded as Arrow IPC file-format bytes.
    pub schema_ipc: Vec<u8>,
    /// Column projection (indices into the source schema), if any.
    pub projection: Option<Vec<usize>>,
    /// Pushed-down filter predicates as SQL fragments.
    pub filter_exprs: Vec<String>,
    /// The most rows the shard needs to return, when the query's `LIMIT` could be pushed
    /// down to it. A hint: the coordinator applies the real limit over the union of the
    /// shards' answers, so a shard returning more rows costs bandwidth and not
    /// correctness.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Executor the scan should be routed to, if pinned to a specific node.
    #[serde(default)]
    pub target_executor_id: Option<String>,
    /// Executors holding replicas of the shard, usable as fallbacks.
    #[serde(default)]
    pub replica_executor_ids: Vec<String>,
}

impl DuckDbScanPlanBytes {
    /// Serialize to JSON bytes for embedding in a physical plan.
    ///
    /// # Panics
    ///
    /// Panics if serialization fails, which cannot happen for this type's
    /// fields.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("DuckDbScanPlanBytes serialization should not fail")
    }

    /// Deserialize from JSON bytes produced by [`encode`](Self::encode),
    /// returning a descriptive error string on malformed input.
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(buf)
            .map_err(|e| format!("failed to decode DuckDbScanPlanBytes: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rolling-upgrade direction, and the one `tests/scan_plan_tests.rs` does not cover:
    /// that file pins the `#[serde(default)]` backfill, which is an *older* payload reaching
    /// a newer node. This is the other way round — a coordinator that has learned a field
    /// the core node running the stage has not.
    ///
    /// Both directions have to hold, because the two binaries are upgraded one at a time and
    /// the payload crosses between them while their versions differ. It holds only because
    /// `serde` ignores unknown fields by default: adding `#[serde(deny_unknown_fields)]` to
    /// this struct would turn every scan planned by a newer coordinator into a decode
    /// failure on a node that had not been restarted yet, and nothing else in the tree would
    /// notice until that upgrade.
    #[test]
    fn a_field_the_node_does_not_know_is_ignored_rather_than_refused() {
        let from_a_newer_coordinator = br#"{
            "shard_table_name": "s0",
            "schema_ipc": [1, 2],
            "projection": null,
            "filter_exprs": [],
            "a_field_this_version_has_never_heard_of": {"nested": true}
        }"#;

        let decoded = DuckDbScanPlanBytes::decode(from_a_newer_coordinator)
            .expect("an unknown field must not fail the scan");

        assert_eq!(decoded.shard_table_name, "s0");
        assert_eq!(decoded.schema_ipc, vec![1, 2]);
    }
}
