use std::path::Path;

use serde::Deserialize;

/// Configuration for a core node, deserialized from a YAML file.
///
/// Every field is required: the YAML must specify all of them, since the node
/// applies no defaults.
#[derive(Debug, Deserialize)]
pub struct CoreConfig {
    /// Tracing log level filter (e.g. `info`, `debug`), used when the
    /// `RUST_LOG` environment variable is unset.
    pub log_level: String,
    /// Stable identifier this node reports to the coordinator and uses as its
    /// Ballista executor id.
    pub node_id: String,
    /// Directory holding the node's DuckDB database file; created if missing.
    pub data_dir: String,
    /// `host:port` the gRPC `WriteService` binds to.
    pub grpc_listen_addr: String,
    /// Address advertised to the coordinator and peers. Falls back to
    /// [`grpc_listen_addr`](Self::grpc_listen_addr) when unset.
    pub advertised_address: Option<String>,
    /// `host:port` of the coordinator's gRPC node service.
    pub coordinator_addr: String,
    /// Seconds between heartbeats sent to the coordinator.
    pub heartbeat_interval_secs: u64,
    /// Bounded capacity of the write queue's channel; writes block once full.
    pub write_queue_capacity: usize,
    /// `host:port` of the Ballista scheduler this node registers with as an
    /// executor.
    pub ballista_scheduler_addr: String,
    /// Number of task slots (concurrent Ballista tasks) this executor advertises.
    pub ballista_concurrent_tasks: usize,
}

impl CoreConfig {
    /// Load and deserialize a [`CoreConfig`] from the YAML file at `path`.
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        vairedb_common::config::from_file(path)
    }

    /// The address to advertise to peers: the explicit
    /// [`advertised_address`](Self::advertised_address) if set, otherwise the
    /// gRPC listen address.
    pub fn effective_advertised_address(&self) -> &str {
        self.advertised_address
            .as_deref()
            .unwrap_or(&self.grpc_listen_addr)
    }

    /// Address the Ballista executor's Flight server binds to: the gRPC listen
    /// host with port `0` so the OS assigns an ephemeral port.
    pub fn ballista_bind_addr(&self) -> String {
        let ip = host_of(&self.grpc_listen_addr).unwrap_or("0.0.0.0");
        format!("{ip}:0")
    }

    /// Host the Ballista executor advertises to the scheduler, derived from the
    /// effective advertised address with any port stripped.
    pub fn ballista_advertise_host(&self) -> String {
        let addr = self.effective_advertised_address();
        host_of(addr).unwrap_or(addr).to_string()
    }
}

/// Return the host portion of a `host:port` address (everything before the last
/// `:`), or `None` if the address carries no port.
fn host_of(addr: &str) -> Option<&str> {
    addr.rsplit_once(':').map(|(host, _)| host)
}
