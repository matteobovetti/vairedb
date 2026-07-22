//! Coordinator configuration loaded from a single YAML file. All fields are
//! required; there are no defaults.

use std::path::Path;

use serde::Deserialize;

/// Fully-specified coordinator configuration deserialized from YAML.
#[derive(Debug, Deserialize)]
pub struct CoordinatorConfig {
    /// `tracing` log filter directive (e.g. `info`), used unless overridden by
    /// the `RUST_LOG` environment variable.
    pub log_level: String,
    /// Directory holding the redb metadata catalog file.
    pub metadata_dir: String,
    /// Listen address for the gRPC `NodeService` that core nodes connect to.
    pub grpc_listen_addr: String,
    /// Listen address for the PostgreSQL wire protocol exposed to clients.
    pub pg_listen_addr: String,
    /// Seconds without a heartbeat before the failure detector marks a node dead.
    pub heartbeat_timeout_secs: u64,
    /// Number of replicas assigned to each new shard.
    pub default_replication_factor: u32,
    /// Initial backoff before retrying a failed replication tail send.
    pub tail_retry_initial_ms: u64,
    /// Maximum backoff for replication tail retries.
    pub tail_retry_max_ms: u64,
    /// Listen address the Ballista scheduler binds for executor connections.
    pub ballista_scheduler_listen_addr: String,
}

impl CoordinatorConfig {
    /// Load and deserialize the coordinator config from the YAML file at `path`.
    ///
    /// Returns `Err` if the file cannot be read or fails to deserialize (e.g. a
    /// missing required field).
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        vairedb_common::config::from_file(path)
    }
}
