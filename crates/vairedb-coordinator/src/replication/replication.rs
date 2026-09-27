//! Replicates writes to a shard's primary and replica core nodes. A write is
//! fanned out to all targets and acknowledged once the primary plus a quorum
//! respond; replicas that lag or fail are queued and re-sent by a background
//! retry loop with exponential backoff, so the missed write is eventually tailed
//! to them (or replayed once a dead node rejoins).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use vairedb_common::proto::vairedb::v1::{
    ExecuteWriteRequest, VdbErrorCode, WriteOperation, WriteParam, WriteStatement,
    write_service_client::WriteServiceClient,
};

use crate::catalog::{MetadataCatalog, NodeState, ShardMeta};
use crate::channel_pool::ChannelPool;

use crate::error::{CoordinatorError, NodeError, Result};

/// Per-node cap on queued retries; bounds memory when a node stays unreachable.
/// Writes beyond the cap are dropped and reconciled when the node rejoins.
const MAX_PENDING_RETRIES: usize = 4096;
/// Default base delay (ms) for the first retry; doubles with each attempt.
pub const DEFAULT_INITIAL_RETRY_MS: u64 = 100;
/// Default ceiling (ms) that exponential backoff is clamped to.
pub const DEFAULT_MAX_RETRY_MS: u64 = 5000;

/// Backoff parameters for retrying writes to lagging replica nodes.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Base delay in milliseconds; the backoff doubles per attempt from here.
    pub initial_retry_ms: u64,
    /// Upper bound in milliseconds that the computed backoff is capped to.
    pub max_retry_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_retry_ms: DEFAULT_INITIAL_RETRY_MS,
            max_retry_ms: DEFAULT_MAX_RETRY_MS,
        }
    }
}

/// One statement of a write, already rewritten to shard-local SQL.
#[derive(Debug, Clone)]
pub struct BatchStatement {
    pub sql: String,
    pub params: Vec<WriteParam>,
    /// Physical shard table the statement targets, e.g. `orders_shard0`.
    pub shard_id: String,
}

/// Everything a node must be told to apply a write, and the only thing a resend
/// needs: the initial fan-out and the retry queue ship the identical batch, which
/// is what makes a tailed write the same write rather than a reconstruction of it.
///
/// Shared as an `Arc` because every target of a fan-out is sent the same batch —
/// the SQL and bind parameters are not copied per node, nor again per queued retry.
#[derive(Debug)]
struct WriteBatch {
    write_id: String,
    /// Names the batch as a whole in node errors; the per-statement shard ids ride
    /// along on the statements themselves.
    shard_id: String,
    statements: Vec<BatchStatement>,
    /// Apply the statements as one transaction: all of them take effect or none.
    atomic: bool,
}

/// A write that failed to reach a node and is queued for re-delivery. `attempt`
/// drives the backoff schedule.
struct PendingRetry {
    node_address: String,
    node_id: String,
    batch: Arc<WriteBatch>,
    attempt: u32,
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
        let statements = vec![BatchStatement {
            sql: sql.to_string(),
            params: params.to_vec(),
            shard_id: crate::util::shard_table_name(&shard.table_name, shard.hash_bucket),
        }];

        let rows = self
            .fan_out(shard, statements, write_id, quorum_size, false)
            .await?;

        Ok(rows.first().copied().unwrap_or(0))
    }

    /// Send `statements` to the shard's nodes as a single transaction: on each
    /// node either all of them take effect or none does. Returns one row count
    /// per statement once the primary plus a quorum acknowledge.
    ///
    /// The statements may target several shards as long as those shards live on
    /// the same nodes — `shard` names that node set. Atomicity is per node, so a
    /// node that acks has applied the whole batch while a lagging node applies it
    /// (still as one transaction) from the retry queue.
    pub async fn execute_transaction_with_quorum(
        &self,
        shard: &ShardMeta,
        statements: Vec<BatchStatement>,
        write_id: &str,
        quorum_size: usize,
    ) -> Result<Vec<u64>> {
        self.fan_out(shard, statements, write_id, quorum_size, true)
            .await
    }

    /// Fan `statements` out to the shard's primary and replicas in parallel and
    /// collect the per-statement row counts (the max reported by any acking node).
    async fn fan_out(
        &self,
        shard: &ShardMeta,
        statements: Vec<BatchStatement>,
        write_id: &str,
        quorum_size: usize,
        atomic: bool,
    ) -> Result<Vec<u64>> {
        let addresses = self.catalog.node_address_map()?;
        let targets = replication_targets(shard, &addresses)?;

        let batch = Arc::new(WriteBatch {
            write_id: write_id.to_string(),
            shard_id: crate::util::shard_table_name(&shard.table_name, shard.hash_bucket),
            statements,
            atomic,
        });

        let handles: Vec<_> = targets
            .into_iter()
            .map(|(node_id, address)| {
                let pool = Arc::clone(&self.pool);
                let batch = Arc::clone(&batch);
                tokio::spawn(async move {
                    let outcome = send_write(&pool, &address, &batch, &node_id).await;
                    (node_id, address, outcome)
                })
            })
            .collect();

        let mut ack_count = 0usize;
        let mut rows_affected = vec![0u64; batch.statements.len()];
        let mut primary_acked = false;
        let mut lagging_nodes: Vec<(String, String)> = Vec::new();
        let mut primary_error: Option<NodeError> = None;

        for handle in handles {
            let (node_id, address, outcome) = match handle.await {
                Ok(outcome) => outcome,
                Err(join_err) => {
                    tracing::error!("replication task panicked: {}", join_err);
                    continue;
                }
            };

            match outcome {
                Ok(rows) => {
                    ack_count += 1;
                    merge_rows_affected(&mut rows_affected, &rows);
                    primary_acked |= node_id == shard.primary_node_id;
                }
                Err(node_error) => {
                    // The primary is queued alongside the replicas, which costs
                    // nothing: a write the primary refused returns below, before
                    // any queue is touched.
                    if node_id == shard.primary_node_id {
                        primary_error = Some(node_error);
                    }
                    lagging_nodes.push((node_id, address));
                }
            }
        }

        if !primary_acked {
            return Err(match primary_error {
                Some(node_error) => CoordinatorError::NodeExecFailed(Box::new(node_error)),
                None => CoordinatorError::ShardUnavailable(shard.shard_id.clone()),
            });
        }

        if ack_count < quorum_size {
            return Err(CoordinatorError::QuorumNotReached {
                needed: quorum_size,
                got: ack_count,
            });
        }

        if !lagging_nodes.is_empty() {
            self.enqueue_retries(lagging_nodes, &batch).await;
        }

        Ok(rows_affected)
    }

    /// Queue a missed write for each lagging node so the background loop can
    /// re-deliver it later.
    async fn enqueue_retries(&self, lagging_nodes: Vec<(String, String)>, batch: &Arc<WriteBatch>) {
        let mut retries = self.pending_retries.lock().await;
        for (node_id, node_address) in lagging_nodes {
            push_pending_retry(
                &mut retries,
                PendingRetry {
                    node_address,
                    node_id,
                    batch: Arc::clone(batch),
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
                    if let Ok(Some(node)) = catalog.get_node(node_id)
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
                        match send_write(&pool, &entry.node_address, &entry.batch, &entry.node_id)
                            .await
                        {
                            Ok(_) => tracing::debug!(
                                "tail replication succeeded for node {}",
                                entry.node_id
                            ),
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

/// The nodes a write to `shard` must reach: its primary first, then whichever of
/// its replicas the catalog still has an address for. A replica the catalog has
/// forgotten is simply not a target — it holds no current data to keep current —
/// but a primary with no address means the shard cannot be written at all.
fn replication_targets(
    shard: &ShardMeta,
    addresses: &HashMap<String, String>,
) -> Result<Vec<(String, String)>> {
    let primary_address = addresses
        .get(&shard.primary_node_id)
        .ok_or_else(|| CoordinatorError::ShardUnavailable(shard.shard_id.clone()))?;

    let mut targets = vec![(shard.primary_node_id.clone(), primary_address.clone())];
    targets.extend(
        shard
            .replica_node_ids
            .iter()
            .filter_map(|id| addresses.get(id).map(|addr| (id.clone(), addr.clone()))),
    );
    Ok(targets)
}

/// Re-enqueue `entry` onto its node's pending-retry queue, dropping it if the
/// queue is already at [`MAX_PENDING_RETRIES`]. The cap bounds memory when a
/// node stays unreachable; a dropped tail write is reconciled when the node
/// rejoins. Single definition shared by the initial enqueue and the retry-loop
/// failure path.
fn push_pending_retry(retries: &mut HashMap<String, VecDeque<PendingRetry>>, entry: PendingRetry) {
    let queue = retries.entry(entry.node_id.clone()).or_default();
    if queue.len() < MAX_PENDING_RETRIES {
        queue.push_back(entry);
    }
}

/// Exponential backoff (ms) for a given attempt: `initial_retry_ms * 2^attempt`,
/// clamped to `max_retry_ms`. The exponent is capped so that a node unreachable
/// for a long time keeps waiting the ceiling rather than overflowing.
fn compute_backoff(attempt: u32, config: &RetryConfig) -> u64 {
    config
        .initial_retry_ms
        .saturating_mul(2u64.pow(attempt.min(10)))
        .min(config.max_retry_ms)
}

/// The gRPC request that carries `batch` to a node.
///
/// Every statement is tagged `Insert` regardless of what it does: the node
/// dispatches on the SQL text it is handed, so the operation kind decides nothing
/// and only the `atomic` flag changes how the batch is applied.
fn write_request(batch: &WriteBatch) -> tonic::Request<ExecuteWriteRequest> {
    tonic::Request::new(ExecuteWriteRequest {
        write_id: batch.write_id.clone(),
        statements: batch
            .statements
            .iter()
            .map(|stmt| WriteStatement {
                sql: stmt.sql.clone(),
                shard_id: stmt.shard_id.clone(),
                operation: WriteOperation::Insert.into(),
                params: stmt.params.clone(),
            })
            .collect(),
        atomic: batch.atomic,
    })
}

/// Apply `batch` on one node and report the rows each of its statements affected.
///
/// Unreachable, refused by the transport, and refused by the node itself all come
/// back as one `NodeError`, because the caller does the same thing with all three:
/// none of them leaves the node holding the write, so all three mean "queue it".
async fn send_write(
    pool: &ChannelPool,
    node_address: &str,
    batch: &WriteBatch,
    node_id: &str,
) -> std::result::Result<Vec<u64>, NodeError> {
    let node_error = |message: String, error_code: i32| NodeError {
        message,
        error_code,
        shard_id: batch.shard_id.clone(),
        node_id: node_id.to_string(),
    };

    let channel = pool.get(node_address).await.map_err(|e| {
        tracing::warn!(node_id = %node_id, address = %node_address, error = %e, "connection failed");
        node_error(
            "connection to storage node failed".to_string(),
            VdbErrorCode::NodeUnavailable as i32,
        )
    })?;

    let response = WriteServiceClient::new(channel)
        .execute_write(write_request(batch))
        .await
        .map_err(|e| {
            let error_code = match e.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    VdbErrorCode::NodeUnavailable as i32
                }
                tonic::Code::NotFound => VdbErrorCode::ShardNotFound as i32,
                tonic::Code::ResourceExhausted => VdbErrorCode::WriteQueueFull as i32,
                _ => VdbErrorCode::InternalError as i32,
            };
            node_error(
                vairedb_common::error::sanitize_message(e.message()),
                error_code,
            )
        })?;
    let results = response.into_inner().results;

    if results.is_empty() {
        return Err(node_error("no results returned".to_string(), 0));
    }

    // Any failed statement fails the whole call: for an atomic batch nothing was
    // applied, and for a single statement there is nothing else to report.
    if let Some(failed) = results.iter().find(|result| !result.success) {
        let error = failed.error.as_ref();
        return Err(node_error(
            error.map_or_else(|| "unknown error".to_string(), |e| e.message.clone()),
            error.map_or(0, |e| e.code),
        ));
    }

    Ok(results
        .iter()
        .map(|result| result.rows_affected as u64)
        .collect())
}

/// Fold one node's per-statement row counts into the running maxima. A node that
/// reports fewer counts than the batch has statements (only possible from a
/// malformed response) contributes what it did report.
fn merge_rows_affected(rows_affected: &mut [u64], node_rows: &[u64]) {
    for (total, rows) in rows_affected.iter_mut().zip(node_rows) {
        *total = (*total).max(*rows);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The schedule a lagging replica is retried on. Attempt 0 is never waited —
    /// the loop increments before computing — so the first resend waits the base
    /// delay, and from there each attempt doubles until the ceiling flattens it.
    #[test]
    fn backoff_doubles_per_attempt_until_it_reaches_the_ceiling() {
        let schedule: Vec<u64> = (0..8)
            .map(|attempt| compute_backoff(attempt, &RetryConfig::default()))
            .collect();

        assert_eq!(schedule, vec![100, 200, 400, 800, 1600, 3200, 5000, 5000]);
    }

    /// A node unreachable for hours keeps waiting the ceiling: the exponent is
    /// capped rather than allowed to run away, so there is no attempt count that
    /// turns into an overflow or an unbounded sleep.
    #[test]
    fn backoff_stays_at_the_ceiling_however_many_attempts_were_made() {
        let config = RetryConfig {
            initial_retry_ms: 50,
            max_retry_ms: 1000,
        };

        assert_eq!(compute_backoff(0, &config), 50);
        for attempt in [11, 64, u32::MAX] {
            assert_eq!(compute_backoff(attempt, &config), 1000, "attempt {attempt}");
        }
    }

    #[test]
    fn merge_rows_affected_keeps_the_max_per_statement() {
        let mut totals = vec![0, 5, 2];
        merge_rows_affected(&mut totals, &[1, 3, 7]);
        assert_eq!(totals, vec![1, 5, 7]);
    }

    #[test]
    fn merge_rows_affected_tolerates_a_short_node_response() {
        let mut totals = vec![0, 0, 0];
        merge_rows_affected(&mut totals, &[4]);
        assert_eq!(totals, vec![4, 0, 0]);
    }
}
