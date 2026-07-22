//! Background failure detector that scans node heartbeats and demotes liveness:
//! nodes silent past a suspect threshold become `Suspect`, and past the full
//! timeout become `Dead`. Runs on a periodic loop and writes state changes to
//! the metadata catalog.

use std::sync::Arc;
use std::time::Duration;

use crate::catalog::{MetadataCatalog, NodeState};
use crate::error::Result;
use crate::util::now_unix_secs;

/// Periodically inspects node heartbeats in the catalog and updates node state
/// (`Suspect`/`Dead`) when heartbeats lapse.
pub struct FailureDetector {
    catalog: Arc<MetadataCatalog>,
    heartbeat_timeout_secs: u64,
    check_interval: Duration,
}

impl FailureDetector {
    /// Create a detector. The scan interval is derived as one third of the
    /// timeout (at least one second), so lapses are noticed well within the
    /// timeout window.
    pub fn new(catalog: Arc<MetadataCatalog>, heartbeat_timeout_secs: u64) -> Self {
        let check_interval =
            Duration::from_secs(heartbeat_timeout_secs / 3).max(Duration::from_secs(1));
        Self {
            catalog,
            heartbeat_timeout_secs,
            check_interval,
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

            if let Err(e) = self.check_nodes() {
                tracing::error!(error = %e, "failure detector scan error");
            }
        }
    }

    /// Scan all nodes once: mark a node `Dead` if its last heartbeat is older
    /// than the timeout, or `Suspect` if it is past one third of the timeout and
    /// still `Alive`. Already-dead nodes are skipped; a missing heartbeat
    /// timestamp counts as never seen (effectively dead).
    fn check_nodes(&self) -> Result<()> {
        let nodes = self.catalog.list_all_nodes()?;
        let now = now_unix_secs();

        let suspect_threshold = self.heartbeat_timeout_secs / 3;

        for node in nodes {
            if node.state == NodeState::Dead as i32 {
                continue;
            }

            let last_hb_secs = node
                .last_heartbeat
                .as_ref()
                .map(|ts| ts.seconds as u64)
                .unwrap_or(0);
            let elapsed = now.saturating_sub(last_hb_secs);

            if elapsed >= self.heartbeat_timeout_secs {
                tracing::warn!(
                    node_id = %node.node_id,
                    elapsed_secs = elapsed,
                    "marking node as dead"
                );
                self.catalog
                    .update_node_state(&node.node_id, NodeState::Dead)?;
            } else if elapsed >= suspect_threshold && node.state == NodeState::Alive as i32 {
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
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::catalog::{MetadataCatalog, NodeMeta};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_db_path() -> String {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "/tmp/vairedb_test_fd_unit_{}_{}.redb",
            std::process::id(),
            id
        )
    }

    fn make_catalog() -> Arc<MetadataCatalog> {
        Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap())
    }

    fn insert_node_with_heartbeat(
        catalog: &MetadataCatalog,
        node_id: &str,
        heartbeat_secs_ago: u64,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last_hb = now.saturating_sub(heartbeat_secs_ago);

        let node = NodeMeta {
            node_id: node_id.to_string(),
            advertised_address: "10.0.0.1:50041".to_string(),
            state: NodeState::Alive as i32,
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: last_hb as i64,
                nanos: 0,
            }),
            registered_at: Some(prost_types::Timestamp {
                seconds: now as i64,
                nanos: 0,
            }),
        };
        catalog.put_node(&node).unwrap();
    }

    #[test]
    fn test_failure_detector_marks_node_suspect() {
        let catalog = make_catalog();
        let timeout_secs = 30;
        let suspect_threshold = timeout_secs / 3;

        insert_node_with_heartbeat(&catalog, "node-1", suspect_threshold + 1);

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let node = catalog.get_node("node-1").unwrap().unwrap();
        assert_eq!(node.state, NodeState::Suspect as i32);
    }

    #[test]
    fn test_failure_detector_marks_node_dead() {
        let catalog = make_catalog();
        let timeout_secs = 30;

        insert_node_with_heartbeat(&catalog, "node-1", timeout_secs + 1);

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let node = catalog.get_node("node-1").unwrap().unwrap();
        assert_eq!(node.state, NodeState::Dead as i32);
    }

    #[test]
    fn test_failure_detector_skips_dead_nodes() {
        let catalog = make_catalog();
        let timeout_secs = 30;

        insert_node_with_heartbeat(&catalog, "node-1", timeout_secs + 100);
        catalog
            .update_node_state("node-1", NodeState::Dead)
            .unwrap();

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let node = catalog.get_node("node-1").unwrap().unwrap();
        assert_eq!(node.state, NodeState::Dead as i32);
    }

    #[test]
    fn test_failure_detector_leaves_healthy_node_alive() {
        let catalog = make_catalog();
        let timeout_secs = 30;

        insert_node_with_heartbeat(&catalog, "node-1", 2);

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let node = catalog.get_node("node-1").unwrap().unwrap();
        assert_eq!(node.state, NodeState::Alive as i32);
    }

    #[test]
    fn test_failure_detector_mixed_node_states() {
        let catalog = make_catalog();
        let timeout_secs = 30;
        let suspect_threshold = timeout_secs / 3;

        insert_node_with_heartbeat(&catalog, "healthy", 2);
        insert_node_with_heartbeat(&catalog, "suspect", suspect_threshold + 1);
        insert_node_with_heartbeat(&catalog, "dead", timeout_secs + 1);

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let healthy = catalog.get_node("healthy").unwrap().unwrap();
        assert_eq!(healthy.state, NodeState::Alive as i32);

        let suspect = catalog.get_node("suspect").unwrap().unwrap();
        assert_eq!(suspect.state, NodeState::Suspect as i32);

        let dead = catalog.get_node("dead").unwrap().unwrap();
        assert_eq!(dead.state, NodeState::Dead as i32);
    }

    #[test]
    fn test_failure_detector_node_without_heartbeat_timestamp() {
        let catalog = make_catalog();
        let timeout_secs = 30;

        let node = NodeMeta {
            node_id: "no-hb-node".to_string(),
            advertised_address: "10.0.0.1:50041".to_string(),
            state: NodeState::Alive as i32,
            last_heartbeat: None,
            registered_at: None,
        };
        catalog.put_node(&node).unwrap();

        let detector = FailureDetector::new(Arc::clone(&catalog), timeout_secs);
        detector.check_nodes().unwrap();

        let updated = catalog.get_node("no-hb-node").unwrap().unwrap();
        assert_eq!(updated.state, NodeState::Dead as i32);
    }
}
