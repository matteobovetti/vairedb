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
            .map_err(|e| format!("failed to decode DuckDbScanPlanBytes: {}", e))
    }
}
