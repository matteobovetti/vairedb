//! Replicates writes to a shard's primary and replica core nodes. A write is
//! fanned out to all targets and acknowledged once the primary plus a quorum
//! respond; replicas that lag or fail are queued and re-sent by a background
//! retry loop with exponential backoff, so the missed write is eventually tailed
//! to them (or replayed once a dead node rejoins).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::Channel;

use vairedb_common::proto::vairedb::v1::{
    ExecuteWriteRequest, VdbErrorCode, WriteOperation, WriteParam, WriteStatement,
    write_service_client::WriteServiceClient,
};

use crate::catalog::{MetadataCatalog, NodeState, ShardMeta};
use crate::channel_pool::ChannelPool;

use crate::error::{CoordinatorError, NodeError, Result};
use crate::replication::retry_config::{MAX_PENDING_RETRIES, RetryConfig};

/// A write that failed to reach a replica node and is queued for re-delivery.
/// Carries everything needed to resend it independently and `attempt` drives the
/// backoff schedule.
#[derive(Debug, Clone)]
pub(crate) struct PendingRetry {
    pub(crate) node_address: String,
    pub(crate) node_id: String,
    pub(crate) write_id: String,
    pub(crate) sql: String,
    pub(crate) params: Vec<WriteParam>,
    pub(crate) shard_id: String,
    /// Number of retry attempts already made; used to compute the next backoff.
    pub(crate) attempt: u32,
}

/// Coordinates quorum writes to a shard's nodes and owns the per-node queues of
/// writes awaiting retry, drained by a background loop spawned at construction.
pub struct ReplicationManager {
    catalog: Arc<MetadataCatalog>,
    pool: Arc<ChannelPool>,
    pending_retries: Arc<Mutex<HashMap<String, VecDeque<PendingRetry>>>>,
    retry_config: RetryConfig,
}

impl ReplicationManager {
    /// Construct a manager and spawn its background retry loop. The loop runs for
    /// the lifetime of the process, periodically draining pending retries.
    pub fn new(
        catalog: Arc<MetadataCatalog>,
        pool: Arc<ChannelPool>,
        retry_config: RetryConfig,
    ) -> Self {
        let manager = Self {
            catalog,
            pool,
            pending_retries: Arc::new(Mutex::new(HashMap::new())),
            retry_config,
        };
        manager.spawn_retry_loop();
        manager
    }

    /// Send `sql` to the shard's primary and all replicas in parallel, returning
    /// the max `rows_affected` once at least `quorum_size` nodes acknowledge.
    /// Fails if the primary does not ack (the write is not durable) or fewer than
    /// `quorum_size` nodes ack. Replicas that lag or error are enqueued for
    /// background retry rather than failing the write.
    pub async fn execute_write_with_quorum(
        &self,
        shard: &ShardMeta,
        sql: &str,
        params: &[WriteParam],
        write_id: &str,
        quorum_size: usize,
    ) -> Result<u64> {
        let node_addresses = self.resolve_node_addresses(shard)?;
        let primary_address = node_addresses
            .get(&shard.primary_node_id)
            .ok_or_else(|| CoordinatorError::ShardUnavailable(shard.shard_id.clone()))?
            .clone();

        let mut all_targets: Vec<(String, String)> =
            vec![(shard.primary_node_id.clone(), primary_address.clone())];
        for replica_id in &shard.replica_node_ids {
            if let Some(addr) = node_addresses.get(replica_id) {
                all_targets.push((replica_id.clone(), addr.clone()));
            }
        }

        let shard_id = crate::util::shard_table_name(&shard.table_name, shard.hash_bucket);

        let mut handles = Vec::new();
        for (node_id, addr) in &all_targets {
            let pool = Arc::clone(&self.pool);
            let addr = addr.clone();
            let sql = sql.to_string();
            let params = params.to_vec();
            let write_id = write_id.to_string();
            let shard_id = shard_id.clone();
            let node_id = node_id.clone();

            let handle = tokio::spawn(async move {
                let channel = pool.get(&addr).await.map_err(|e| {
                    tracing::warn!(node_id = %node_id, address = %addr, error = %e, "connection failed");
                    NodeError {
                        message: "connection to storage node failed".to_string(),
                        error_code: VdbErrorCode::NodeUnavailable as i32,
                        shard_id: shard_id.clone(),
                        node_id: node_id.clone(),
                    }
                })?;
                let result =
                    send_write_to_node(channel, &write_id, &sql, &params, &shard_id, &node_id)
                        .await;
                Ok::<_, NodeError>((node_id, addr, result))
            });
            handles.push(handle);
        }

        let mut ack_count = 0usize;
        let mut rows_affected = 0u64;
        let mut primary_acked = false;
        let mut lagging_nodes: Vec<(String, String)> = Vec::new();
        let mut primary_error: Option<NodeError> = None;

        for handle in handles {
            match handle.await {
                Ok(Ok((node_id, _addr, Ok(rows)))) => {
                    ack_count += 1;
                    if rows > rows_affected {
                        rows_affected = rows;
                    }
                    if node_id == shard.primary_node_id {
                        primary_acked = true;
                    }
                }
                Ok(Ok((node_id, addr, Err(e)))) => {
                    if node_id == shard.primary_node_id {
                        primary_error = Some(e);
                    }
                    lagging_nodes.push((node_id, addr));
                }
                Ok(Err(e)) => {
                    if e.node_id == shard.primary_node_id {
                        primary_error = Some(e);
                    } else if let Some(addr) = node_addresses.get(&e.node_id) {
                        lagging_nodes.push((e.node_id, addr.clone()));
                    }
                }
                Err(join_err) => {
                    tracing::error!("replication task panicked: {}", join_err);
                }
            }
        }

        if !primary_acked {
            if let Some(node_err) = primary_error {
                return Err(CoordinatorError::NodeExecFailed(Box::new(node_err)));
            }
            return Err(CoordinatorError::ShardUnavailable(shard.shard_id.clone()));
        }

        if ack_count < quorum_size {
            return Err(CoordinatorError::QuorumNotReached {
                needed: quorum_size,
                got: ack_count,
            });
        }

        if !lagging_nodes.is_empty() {
            self.enqueue_retries(lagging_nodes, write_id, sql, params, &shard_id)
                .await;
        }

        Ok(rows_affected)
    }

    /// Build a `node_id -> gRPC address` map for resolving a shard's targets from
    /// the catalog.
    fn resolve_node_addresses(&self, _shard: &ShardMeta) -> Result<HashMap<String, String>> {
        let address_map = self.catalog.get_node_address_map()?;
        Ok(address_map)
    }

    /// Queue a missed write for each lagging node so the background loop can
    /// re-deliver it later.
    async fn enqueue_retries(
        &self,
        lagging_nodes: Vec<(String, String)>,
        write_id: &str,
        sql: &str,
        params: &[WriteParam],
        shard_id: &str,
    ) {
        let mut retries = self.pending_retries.lock().await;
        for (node_id, address) in lagging_nodes {
            push_pending_retry(
                &mut retries,
                PendingRetry {
                    node_address: address,
                    node_id,
                    write_id: write_id.to_string(),
                    sql: sql.to_string(),
                    params: params.to_vec(),
                    shard_id: shard_id.to_string(),
                    attempt: 0,
                },
            );
        }
    }

    /// Spawn the background task that periodically pops one queued retry per node
    /// and re-sends it after a per-attempt backoff. Nodes still marked Dead are
    /// skipped (their queue is left intact) so writes replay only once a node is
    /// reachable again; a re-send that fails is pushed back onto the queue.
    fn spawn_retry_loop(&self) {
        let pending_retries = Arc::clone(&self.pending_retries);
        let catalog = Arc::clone(&self.catalog);
        let pool = Arc::clone(&self.pool);
        let retry_config = self.retry_config;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(retry_config.initial_retry_ms)).await;

                let mut retries = pending_retries.lock().await;
                let node_ids: Vec<String> = retries.keys().cloned().collect();

                let mut to_retry: Vec<PendingRetry> = Vec::new();

                for node_id in &node_ids {
                    // A node that is still Dead can't accept the write yet. Leave its
                    // queue intact so the missed write is replayed once it rejoins
                    // (heartbeat flips it back to Alive); skip it this round.
                    let node_state = catalog.get_node(node_id);
                    if let Ok(Some(node)) = node_state
                        && node.state == NodeState::Dead as i32
                    {
                        continue;
                    }

                    if let Some(queue) = retries.get_mut(node_id)
                        && let Some(entry) = queue.pop_front()
                    {
                        to_retry.push(entry);
                    }
                }

                drop(retries);

                for mut entry in to_retry {
                    entry.attempt += 1;
                    let backoff = compute_backoff(entry.attempt, &retry_config);
                    let pending = Arc::clone(&pending_retries);
                    let pool = Arc::clone(&pool);

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        let channel = match pool.get(&entry.node_address).await {
                            Ok(ch) => ch,
                            Err(_) => {
                                let mut retries = pending.lock().await;
                                push_pending_retry(&mut retries, entry);
                                return;
                            }
                        };
                        match send_write_to_node(
                            channel,
                            &entry.write_id,
                            &entry.sql,
                            &entry.params,
                            &entry.shard_id,
                            &entry.node_id,
                        )
                        .await
                        {
                            Ok(_) => {
                                tracing::debug!(
                                    "tail replication succeeded for node {}",
                                    entry.node_id
                                );
                            }
                            Err(_) => {
                                let mut retries = pending.lock().await;
                                push_pending_retry(&mut retries, entry);
                            }
                        }
                    });
                }
            }
        });
    }
}

/// Re-enqueue `entry` onto its node's pending-retry queue, dropping it if the
/// queue is already at [`MAX_PENDING_RETRIES`]. The cap bounds memory when a
/// node stays unreachable; a dropped tail write is reconciled when the node
/// rejoins. Single definition shared by the initial enqueue and both retry-loop
/// failure paths.
fn push_pending_retry(retries: &mut HashMap<String, VecDeque<PendingRetry>>, entry: PendingRetry) {
    let queue = retries.entry(entry.node_id.clone()).or_default();
    if queue.len() < MAX_PENDING_RETRIES {
        queue.push_back(entry);
    }
}

/// Exponential backoff (ms) for a given attempt: `initial_retry_ms * 2^attempt`,
/// clamped to `max_retry_ms`. The exponent is capped at 10 to avoid overflow.
fn compute_backoff(attempt: u32, config: &RetryConfig) -> u64 {
    let backoff = config.initial_retry_ms * 2u64.pow(attempt.min(10));
    backoff.min(config.max_retry_ms)
}

/// Send a single write statement to one node over `channel` and return the rows
/// affected. Maps tonic transport status to a `VdbErrorCode`, and surfaces a
/// node-reported failure (or a missing result) as a `NodeError`.
async fn send_write_to_node(
    channel: Channel,
    write_id: &str,
    sql: &str,
    params: &[WriteParam],
    shard_id: &str,
    node_id: &str,
) -> std::result::Result<u64, NodeError> {
    let mut client = WriteServiceClient::new(channel);

    let request = tonic::Request::new(ExecuteWriteRequest {
        write_id: write_id.to_string(),
        statements: vec![WriteStatement {
            sql: sql.to_string(),
            shard_id: shard_id.to_string(),
            operation: WriteOperation::Insert.into(),
            params: params.to_vec(),
        }],
    });

    let response = client.execute_write(request).await.map_err(|e| {
        let error_code = match e.code() {
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                VdbErrorCode::NodeUnavailable as i32
            }
            tonic::Code::NotFound => VdbErrorCode::ShardNotFound as i32,
            tonic::Code::ResourceExhausted => VdbErrorCode::WriteQueueFull as i32,
            _ => VdbErrorCode::InternalError as i32,
        };
        NodeError {
            message: vairedb_common::error::sanitize_message(e.message()),
            error_code,
            shard_id: shard_id.to_string(),
            node_id: node_id.to_string(),
        }
    })?;
    let resp = response.into_inner();

    if let Some(result) = resp.results.first() {
        if result.success {
            Ok(result.rows_affected as u64)
        } else {
            let error = result.error.as_ref();
            let msg = error
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "unknown error".to_string());
            let code = error.map(|e| e.code).unwrap_or(0);
            Err(NodeError {
                message: msg,
                error_code: code,
                shard_id: shard_id.to_string(),
                node_id: node_id.to_string(),
            })
        }
    } else {
        Err(NodeError {
            message: "no results returned".to_string(),
            error_code: 0,
            shard_id: shard_id.to_string(),
            node_id: node_id.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::retry_config::{DEFAULT_INITIAL_RETRY_MS, DEFAULT_MAX_RETRY_MS};

    use super::*;

    fn default_config() -> RetryConfig {
        RetryConfig::default()
    }

    #[test]
    fn test_compute_backoff_first_attempt() {
        let backoff = compute_backoff(1, &default_config());
        assert_eq!(backoff, 200);
    }

    #[test]
    fn test_compute_backoff_second_attempt() {
        let backoff = compute_backoff(2, &default_config());
        assert_eq!(backoff, 400);
    }

    #[test]
    fn test_compute_backoff_third_attempt() {
        let backoff = compute_backoff(3, &default_config());
        assert_eq!(backoff, 800);
    }

    #[test]
    fn test_compute_backoff_capped_at_max() {
        let backoff = compute_backoff(20, &default_config());
        assert_eq!(backoff, DEFAULT_MAX_RETRY_MS);
    }

    #[test]
    fn test_compute_backoff_at_boundary() {
        let config = default_config();
        let backoff = compute_backoff(10, &config);
        let expected = config.initial_retry_ms * 2u64.pow(10);
        assert_eq!(backoff, expected.min(config.max_retry_ms));
    }

    #[test]
    fn test_compute_backoff_zero_attempt() {
        let backoff = compute_backoff(0, &default_config());
        assert_eq!(backoff, DEFAULT_INITIAL_RETRY_MS);
    }

    #[test]
    fn test_compute_backoff_increases_monotonically() {
        let config = default_config();
        let mut prev = 0u64;
        for attempt in 0..20 {
            let backoff = compute_backoff(attempt, &config);
            assert!(backoff >= prev);
            prev = backoff;
        }
    }

    #[test]
    fn test_compute_backoff_never_exceeds_max() {
        let config = default_config();
        for attempt in 0..100 {
            let backoff = compute_backoff(attempt, &config);
            assert!(backoff <= config.max_retry_ms);
        }
    }

    #[test]
    fn test_compute_backoff_custom_config() {
        let config = RetryConfig {
            initial_retry_ms: 50,
            max_retry_ms: 1000,
        };
        assert_eq!(compute_backoff(0, &config), 50);
        assert_eq!(compute_backoff(1, &config), 100);
        assert_eq!(compute_backoff(2, &config), 200);
        assert_eq!(compute_backoff(20, &config), 1000);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.initial_retry_ms, DEFAULT_INITIAL_RETRY_MS);
        assert_eq!(config.max_retry_ms, DEFAULT_MAX_RETRY_MS);
    }
}
