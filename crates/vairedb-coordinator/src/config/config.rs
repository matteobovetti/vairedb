//! Coordinator configuration loaded from a single YAML file. All fields are
//! required; there are no defaults.
//!
//! Three of the numbers here have a value that YAML accepts and the coordinator cannot run
//! on, and none of them reports itself where it is used: the failure detector cannot tell a
//! zero timeout from a cluster that really is gone, and the replication backoff cannot tell
//! a zero wait from one it was asked for. [`Validate`] refuses them at startup, by name —
//! see the doctrine that prefers a refusal to a plausible wrong answer.

use std::path::Path;

use serde::Deserialize;
use vairedb_common::config::{ConfigError, Validate};

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
    /// Whether a transaction block whose writes span several shard groups may
    /// commit. VaireDB has no cross-shard commit protocol, so such a `COMMIT` is
    /// refused by default — nothing is written. Setting this to `true` applies the
    /// groups one at a time instead: the transaction succeeds, but a failure
    /// part-way through leaves the earlier groups applied.
    pub allow_cross_shard_transactions: bool,
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
    /// The error distinguishes a file that could not be read from one that was read and is
    /// not a configuration — a missing required field, say. See [`ConfigError`].
    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        vairedb_common::config::from_file(path)
    }
}

impl Validate for CoordinatorConfig {
    fn validate(&self) -> Result<(), String> {
        if self.heartbeat_timeout_secs == 0 {
            // Every node has been silent for at least zero seconds, and the detector's
            // comparison is inclusive, so the first scan marks all of them dead.
            // `list_alive_nodes` admits only `Alive`, so `CREATE TABLE` then fails for want
            // of nodes while every core node is in fact heartbeating.
            return Err(
                "heartbeat_timeout_secs is 0, which marks every core node dead on the first \
                 scan and leaves no node able to hold a shard"
                    .to_string(),
            );
        }

        if self.default_replication_factor == 0 {
            // Shard assignment places the primary and then loops `1..replication_factor`
            // for replicas, so 0 quietly behaves as 1. An operator who wrote 0 was asking
            // for something else, and there is no reading of "zero copies" worth guessing.
            return Err(
                "default_replication_factor is 0; a shard needs at least one copy, so use 1 \
                 to keep a single copy per shard"
                    .to_string(),
            );
        }

        if self.tail_retry_initial_ms == 0 {
            // The backoff is `initial * 2^attempt` clamped to the maximum, so a zero initial
            // makes every wait zero: a replica that is down is retried as fast as the loop
            // can go, for as long as it stays down.
            return Err(
                "tail_retry_initial_ms is 0, so replication retries would not wait at all and \
                 an unreachable replica would be retried in a tight loop"
                    .to_string(),
            );
        }

        if self.tail_retry_initial_ms > self.tail_retry_max_ms {
            // Same clamp read the other way: the first wait is already above the ceiling, so
            // every retry waits the maximum and the value the operator wrote is never used.
            return Err(format!(
                "tail_retry_initial_ms ({}) is above tail_retry_max_ms ({}), so every retry \
                 would wait the maximum and the initial backoff would be ignored",
                self.tail_retry_initial_ms, self.tail_retry_max_ms
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A configuration the coordinator can run on, and the one every case below starts from.
    fn config() -> CoordinatorConfig {
        CoordinatorConfig {
            log_level: "info".to_string(),
            metadata_dir: "data/coordinator".to_string(),
            grpc_listen_addr: "0.0.0.0:50040".to_string(),
            pg_listen_addr: "0.0.0.0:5432".to_string(),
            heartbeat_timeout_secs: 15,
            default_replication_factor: 3,
            allow_cross_shard_transactions: false,
            tail_retry_initial_ms: 100,
            tail_retry_max_ms: 5000,
            ballista_scheduler_listen_addr: "0.0.0.0:50050".to_string(),
        }
    }

    /// Every value the coordinator cannot run on is refused, and the refusal says enough to
    /// act on.
    ///
    /// The expected fragment differs per case rather than being the field name in each,
    /// because two of these cases are the same field: `tail_retry_initial_ms` has one rule
    /// about the value itself and one about its relation to the maximum, and asserting only
    /// the field name would let either rule stand in for the other.
    ///
    /// The valid configuration is asserted in the same test. A `validate` that refused
    /// everything would satisfy all four refusals on its own, and it is the failure mode a
    /// new rule is most likely to introduce.
    /// A fragment the refusal must contain, and the edit that provokes it.
    type Case = (&'static str, fn(&mut CoordinatorConfig));

    #[test]
    fn a_value_the_coordinator_cannot_run_on_is_refused_with_a_reason() {
        let cases: [Case; 4] = [
            ("heartbeat_timeout_secs", |c| c.heartbeat_timeout_secs = 0),
            ("default_replication_factor", |c| {
                c.default_replication_factor = 0
            }),
            ("would not wait at all", |c| c.tail_retry_initial_ms = 0),
            ("above tail_retry_max_ms", |c| {
                c.tail_retry_initial_ms = c.tail_retry_max_ms + 1
            }),
        ];

        assert_eq!(config().validate(), Ok(()));

        for (expected, break_it) in cases {
            let mut broken = config();
            break_it(&mut broken);

            let reason = broken
                .validate()
                .expect_err("the coordinator cannot run on this value");
            assert!(
                reason.contains(expected),
                "the refusal must explain itself; wanted {expected:?}, got: {reason}"
            );
        }
    }

    /// The boundary the backoff rule sits on: equal values are a fixed wait, which is a
    /// legitimate thing to ask for and not the contradiction the rule is about.
    #[test]
    fn an_initial_backoff_equal_to_the_maximum_is_a_fixed_wait_and_allowed() {
        let mut config = config();
        config.tail_retry_initial_ms = config.tail_retry_max_ms;

        assert_eq!(config.validate(), Ok(()));
    }
}
