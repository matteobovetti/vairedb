//! Background failure detector that scans node heartbeats and demotes liveness:
//! nodes silent past a suspect threshold become `Suspect`, and past the full
//! timeout become `Dead`. Runs on a periodic loop and writes state changes to
//! the metadata catalog.
//!
//! One third of the timeout is both how often the scan runs and how long a
//! silence has to last to be suspicious, and it is derived **once**. Derived
//! twice it was floored twice over — the interval at a second, the threshold not
//! at all — so a timeout under three seconds gave a threshold of zero and every
//! node, including one that had heartbeated that instant, was demoted to
//! `Suspect` on the first scan. Nothing recovers from that on its own:
//! `list_alive_nodes` admits only `Alive`, so shard placement then finds no node
//! on a cluster where every node is healthy.

use std::sync::Arc;
use std::time::Duration;

use crate::catalog::{MetadataCatalog, NodeState};
use crate::error::Result;
use crate::util::now_unix_secs;

/// Periodically inspects node heartbeats in the catalog and updates node state
/// (`Suspect`/`Dead`) when heartbeats lapse.
pub struct FailureDetector {
    catalog: Arc<MetadataCatalog>,
    /// Silence at or past this many seconds means `Dead`.
    dead_after_secs: u64,
    /// Silence at or past this many seconds means `Suspect`. Never zero, so a
    /// node that has just been heard from is never demoted.
    suspect_after_secs: u64,
    check_interval: Duration,
}

impl FailureDetector {
    /// Create a detector from the heartbeat timeout.
    ///
    /// The scan interval and the suspect threshold are the same third of the
    /// timeout, floored together at one second: the scan has to be frequent
    /// enough to notice a lapse well inside the timeout window, and there is no
    /// point suspecting a node sooner than we look at it. A timeout of one or
    /// two seconds leaves no room between the two thresholds, so such a node
    /// goes straight from `Alive` to `Dead` — which is the honest reading of a
    /// timeout that short, and is not the same thing as suspecting every node.
    pub fn new(catalog: Arc<MetadataCatalog>, heartbeat_timeout_secs: u64) -> Self {
        let third = (heartbeat_timeout_secs / 3).max(1);
        Self {
            catalog,
            dead_after_secs: heartbeat_timeout_secs,
            suspect_after_secs: third,
            check_interval: Duration::from_secs(third),
        }
    }

    /// Consume the detector and run its scan loop on a background Tokio task.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.run_loop().await;
        });
    }

    /// Run forever, sleeping `check_interval` between scans; scan errors are
    /// logged and the loop continues.
    async fn run_loop(&self) {
        loop {
            tokio::time::sleep(self.check_interval).await;

            if let Err(e) = self.check_nodes(now_unix_secs()) {
                tracing::error!(error = %e, "failure detector scan error");
            }
        }
    }

    /// Scan all nodes once against `now`: mark a node `Dead` if its last
    /// heartbeat is at or past the timeout, or `Suspect` if it is at or past the
    /// suspect threshold and still `Alive`. Already-dead nodes are skipped; a
    /// missing heartbeat timestamp counts as never seen, which is effectively
    /// dead.
    ///
    /// `now` is a parameter rather than read here so that a test can sit exactly
    /// on a threshold: both comparisons are inclusive, and a scan that read its
    /// own clock made every boundary case a race with the second hand.
    fn check_nodes(&self, now: u64) -> Result<()> {
        for node in self.catalog.list_all_nodes()? {
            if node.state == NodeState::Dead as i32 {
                continue;
            }

            let last_hb_secs = node
                .last_heartbeat
                .as_ref()
                .map(|ts| ts.seconds as u64)
                .unwrap_or(0);
            let elapsed = now.saturating_sub(last_hb_secs);

            if elapsed >= self.dead_after_secs {
                tracing::warn!(
                    node_id = %node.node_id,
                    elapsed_secs = elapsed,
                    "marking node as dead"
                );
                self.catalog
                    .update_node_state(&node.node_id, NodeState::Dead)?;
            } else if elapsed >= self.suspect_after_secs && node.state == NodeState::Alive as i32 {
                tracing::info!(
                    node_id = %node.node_id,
                    elapsed_secs = elapsed,
                    "marking node as suspect"
                );
                self.catalog
                    .update_node_state(&node.node_id, NodeState::Suspect)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::NodeMeta;
    use crate::catalog::catalog_test_helper::scratch_catalog;

    /// The instant every scan in these tests is run at. Fixed, so a node's
    /// silence is exact and a threshold can be landed on rather than approached.
    const NOW: u64 = 1_700_000_000;

    /// A 30-second timeout, whose third is 10 — comfortably above the floor, so
    /// the two thresholds are distinct and both are reachable.
    const TIMEOUT: u64 = 30;

    fn catalog() -> Arc<MetadataCatalog> {
        Arc::new(scratch_catalog("failure_detector"))
    }

    /// An `Alive` node last heard from `silent_for` seconds before [`NOW`].
    fn node_silent_for(catalog: &MetadataCatalog, node_id: &str, silent_for: u64) {
        put_node(catalog, node_id, Some(NOW - silent_for));
    }

    fn put_node(catalog: &MetadataCatalog, node_id: &str, last_heartbeat_secs: Option<u64>) {
        catalog
            .put_node(&NodeMeta {
                node_id: node_id.to_string(),
                advertised_address: "10.0.0.1:50041".to_string(),
                state: NodeState::Alive as i32,
                last_heartbeat: last_heartbeat_secs.map(|secs| prost_types::Timestamp {
                    seconds: secs as i64,
                    nanos: 0,
                }),
                registered_at: None,
            })
            .unwrap();
    }

    fn state_of(catalog: &MetadataCatalog, node_id: &str) -> i32 {
        catalog.get_node(node_id).unwrap().unwrap().state
    }

    /// What a scan makes of each length of silence, including both boundaries.
    ///
    /// The comparisons are inclusive, so a node silent for exactly the threshold
    /// is already demoted; the second below it is not. Asserted in one catalog
    /// and one scan, because the states are decided per node and a scan that
    /// handled a mixture differently than a single node would be the bug worth
    /// catching.
    #[test]
    fn a_scan_demotes_each_node_by_how_long_it_has_been_silent() {
        let third = TIMEOUT / 3;
        let cases = [
            ("fresh", 0, NodeState::Alive),
            ("nearly_suspect", third - 1, NodeState::Alive),
            ("just_suspect", third, NodeState::Suspect),
            ("nearly_dead", TIMEOUT - 1, NodeState::Suspect),
            ("just_dead", TIMEOUT, NodeState::Dead),
            ("long_dead", TIMEOUT * 10, NodeState::Dead),
        ];

        let catalog = catalog();
        for (node_id, silent_for, _) in cases {
            node_silent_for(&catalog, node_id, silent_for);
        }

        FailureDetector::new(Arc::clone(&catalog), TIMEOUT)
            .check_nodes(NOW)
            .unwrap();

        for (node_id, silent_for, expected) in cases {
            assert_eq!(
                state_of(&catalog, node_id),
                expected as i32,
                "a node silent for {silent_for}s of a {TIMEOUT}s timeout must be {expected:?}"
            );
        }
    }

    /// The regression the single derivation exists for. `timeout / 3` truncates
    /// to zero below three seconds, and a zero threshold is met by a node that
    /// has just been heard from — so every node was suspected on the first scan,
    /// and none could hold a shard afterwards.
    #[test]
    fn a_timeout_too_short_to_have_thirds_still_leaves_a_fresh_node_alive() {
        for timeout in [1, 2, 3] {
            let catalog = catalog();
            node_silent_for(&catalog, "node-1", 0);

            FailureDetector::new(Arc::clone(&catalog), timeout)
                .check_nodes(NOW)
                .unwrap();

            assert_eq!(
                state_of(&catalog, "node-1"),
                NodeState::Alive as i32,
                "a node that heartbeated this instant must survive a {timeout}s timeout"
            );
        }
    }

    /// A `Dead` node is left as it is rather than re-examined. Its heartbeat is
    /// older than ever, so a scan that reconsidered it would rewrite the same
    /// state on every pass; coming back is the registration path's business, not
    /// the detector's.
    #[test]
    fn a_dead_node_is_not_revisited() {
        let catalog = catalog();
        node_silent_for(&catalog, "node-1", TIMEOUT * 10);
        catalog
            .update_node_state("node-1", NodeState::Dead)
            .unwrap();

        FailureDetector::new(Arc::clone(&catalog), TIMEOUT)
            .check_nodes(NOW)
            .unwrap();

        assert_eq!(state_of(&catalog, "node-1"), NodeState::Dead as i32);
    }

    /// No heartbeat at all reads as never seen, not as just seen. Treating a
    /// missing timestamp as `now` would keep a node that never reported itself
    /// eligible for shards forever.
    #[test]
    fn a_node_that_has_never_heartbeated_is_dead() {
        let catalog = catalog();
        put_node(&catalog, "no-hb-node", None);

        FailureDetector::new(Arc::clone(&catalog), TIMEOUT)
            .check_nodes(NOW)
            .unwrap();

        assert_eq!(state_of(&catalog, "no-hb-node"), NodeState::Dead as i32);
    }
}
