//! Core-node configuration loaded from a single YAML file.
//!
//! Three of the numbers here have a zero that YAML accepts and the node cannot start or work
//! on, and what each one does is unrecognisable as a configuration problem by the time it
//! happens: two are panics — one of them inside a spawned task, where the only trace is a
//! backtrace and the visible symptom is a node that stopped heartbeating — and the third is
//! an executor that registers successfully and is simply never given work. [`Validate`]
//! turns all three into a refusal at startup that names the field.

use std::path::Path;

use serde::Deserialize;
use vairedb_common::config::{ConfigError, Validate};

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
    ///
    /// The error distinguishes a file that could not be read from one that was read and is
    /// not a configuration — see [`ConfigError`].
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
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

impl Validate for CoreConfig {
    fn validate(&self) -> Result<(), String> {
        if self.heartbeat_interval_secs == 0 {
            // `tokio::time::interval` panics on a zero period, and it is constructed inside
            // the spawned heartbeat session — so the node keeps running while never
            // heartbeating again, and the coordinator buries a node that is perfectly alive.
            return Err(
                "heartbeat_interval_secs is 0; a zero interval panics the heartbeat task, \
                 leaving the node running but silent until the coordinator declares it dead"
                    .to_string(),
            );
        }

        if self.write_queue_capacity == 0 {
            // A bounded channel of zero is a panic in `mpsc::channel`, so the node dies on
            // startup with a message about a channel rather than about its configuration.
            return Err(
                "write_queue_capacity is 0; the write queue is a bounded channel and needs \
                 room for at least one write"
                    .to_string(),
            );
        }

        if self.ballista_concurrent_tasks == 0 {
            // Nothing fails: the executor registers advertising zero task slots, and the
            // scheduler simply never sends it a stage. Reads land on the other nodes, or on
            // none at all, and no error anywhere mentions this node.
            return Err(
                "ballista_concurrent_tasks is 0; the executor would advertise no task slots \
                 and the scheduler would never give this node a share of a read"
                    .to_string(),
            );
        }

        Ok(())
    }
}

/// Return the host portion of a `host:port` address (everything before the last
/// `:`), or `None` if the address carries no port.
fn host_of(addr: &str) -> Option<&str> {
    addr.rsplit_once(':').map(|(host, _)| host)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A valid configuration differing only in the two fields the address helpers read.
    /// Every other value is fixed and irrelevant here — these helpers are pure string
    /// surgery, so a file would add nothing but a temp path.
    fn config(grpc_listen_addr: &str, advertised_address: Option<&str>) -> CoreConfig {
        CoreConfig {
            log_level: "info".to_string(),
            node_id: "core-1".to_string(),
            data_dir: "data/core".to_string(),
            grpc_listen_addr: grpc_listen_addr.to_string(),
            advertised_address: advertised_address.map(str::to_string),
            coordinator_addr: "http://127.0.0.1:50040".to_string(),
            heartbeat_interval_secs: 5,
            write_queue_capacity: 1024,
            ballista_scheduler_addr: "http://127.0.0.1:50050".to_string(),
            ballista_concurrent_tasks: 4,
        }
    }

    /// What peers are told to use, and the fallback that makes `advertised_address` optional
    /// in the first place.
    #[test]
    fn the_advertised_address_falls_back_to_the_listen_address() {
        assert_eq!(
            config("0.0.0.0:50041", Some("10.0.0.5:50041")).effective_advertised_address(),
            "10.0.0.5:50041"
        );
        assert_eq!(
            config("0.0.0.0:50041", None).effective_advertised_address(),
            "0.0.0.0:50041"
        );
    }

    /// The Flight server binds the listen host on port `0` so the OS picks the port. The
    /// host has to be kept: binding `0.0.0.0` where the operator asked for a single
    /// interface would expose the executor on every interface the host has.
    #[test]
    fn the_flight_server_binds_the_listen_host_on_an_ephemeral_port() {
        assert_eq!(
            config("127.0.0.1:50041", None).ballista_bind_addr(),
            "127.0.0.1:0"
        );
        assert_eq!(
            config("0.0.0.0:50041", None).ballista_bind_addr(),
            "0.0.0.0:0"
        );
    }

    /// The scheduler is told a host, not an address, so the port is stripped from whichever
    /// address is in effect.
    #[test]
    fn the_scheduler_is_told_the_advertised_host_without_its_port() {
        assert_eq!(
            config("0.0.0.0:50041", Some("core-1:50041")).ballista_advertise_host(),
            "core-1"
        );
        assert_eq!(
            config("10.0.0.5:50041", None).ballista_advertise_host(),
            "10.0.0.5"
        );
    }

    /// An address with no port keeps both helpers usable rather than losing the host.
    ///
    /// `rsplit_once(':')` finds nothing, and the two fall back differently on purpose: the
    /// bind address still needs a port, so it substitutes a wildcard host it can bind;
    /// the advertised host is already a bare host, so it is passed through as it stands.
    /// Returning an empty host from either would leave the executor unreachable with no
    /// indication of why.
    #[test]
    fn an_address_without_a_port_still_yields_a_usable_host() {
        let config = config("core-1", Some("core-1"));

        assert_eq!(config.ballista_bind_addr(), "0.0.0.0:0");
        assert_eq!(config.ballista_advertise_host(), "core-1");
    }

    /// Each zero the node cannot work with is refused, and the refusal names the field. The
    /// valid configuration is asserted in the same test: a `validate` that rejected
    /// everything would satisfy every rejection below on its own.
    /// A field name the refusal must mention, and the edit that makes it unrunnable.
    type Case = (&'static str, fn(&mut CoreConfig));

    #[test]
    fn a_zero_the_node_cannot_run_on_is_refused_by_field_name() {
        let zeroed: [Case; 3] = [
            ("heartbeat_interval_secs", |c| c.heartbeat_interval_secs = 0),
            ("write_queue_capacity", |c| c.write_queue_capacity = 0),
            ("ballista_concurrent_tasks", |c| {
                c.ballista_concurrent_tasks = 0
            }),
        ];

        assert_eq!(config("0.0.0.0:50041", None).validate(), Ok(()));

        for (field, zero_it) in zeroed {
            let mut broken = config("0.0.0.0:50041", None);
            zero_it(&mut broken);

            let reason = broken
                .validate()
                .expect_err("a zero in this field is not runnable");
            assert!(
                reason.contains(field),
                "the refusal must name the field to fix; got: {reason}"
            );
        }
    }
}
