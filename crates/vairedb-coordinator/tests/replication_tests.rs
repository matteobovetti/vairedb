//! What a caller of `ReplicationManager` can observe against real nodes: which
//! acknowledgements make a write succeed, what a failure says, and what the retry
//! loop does with the nodes that missed it.
//!
//! Every test runs its own mock nodes over a real gRPC channel, because the
//! properties under test are about what actually reached the wire.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Server};

use vairedb_common::proto::vairedb::v1::{
    ErrorDetail, ExecuteWriteRequest, ExecuteWriteResponse, WriteResult,
    write_service_server::{WriteService, WriteServiceServer},
};
use vairedb_coordinator::catalog::{MetadataCatalog, NodeMeta, NodeState, ShardMeta};
use vairedb_coordinator::channel_pool::ChannelPool;
use vairedb_coordinator::error::Result;
use vairedb_coordinator::replication::{BatchStatement, ReplicationManager, RetryConfig};

mod common;
use common::temp_catalog;

// ---------------------------------------------------------------------------
// Mock nodes
// ---------------------------------------------------------------------------

/// A node that refuses its first `fail_until_call` requests and then answers
/// normally, counting every request it received.
///
/// `fail_until_call: 0` is a node that simply works, which is why there is no
/// separate always-succeeds mock: "did it work" and "how many times was it asked"
/// are the same question once a retry loop is involved.
struct FlakyWriteService {
    calls: Arc<Mutex<u32>>,
    fail_until_call: u32,
    rows_affected: i64,
}

impl FlakyWriteService {
    /// A node that always answers, reporting `rows_affected` for the statement.
    fn healthy(rows_affected: i64) -> Self {
        Self {
            calls: Arc::new(Mutex::new(0)),
            fail_until_call: 0,
            rows_affected,
        }
    }

    /// A node that refuses `failures` requests before it starts answering.
    fn refusing(failures: u32) -> Self {
        Self {
            calls: Arc::new(Mutex::new(0)),
            fail_until_call: failures,
            rows_affected: 1,
        }
    }

    /// Handle on the request counter, held by the test after the service moves
    /// into its server.
    fn calls(&self) -> Arc<Mutex<u32>> {
        Arc::clone(&self.calls)
    }
}

#[tonic::async_trait]
impl WriteService for FlakyWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        let mut calls = self.calls.lock().await;
        *calls += 1;
        if *calls <= self.fail_until_call {
            return Err(Status::unavailable("not ready yet"));
        }
        Ok(Response::new(ExecuteWriteResponse {
            results: vec![WriteResult {
                success: true,
                rows_affected: self.rows_affected,
                error: None,
            }],
        }))
    }
}

/// A node that is down: the transport itself refuses.
struct FailingWriteService;

#[tonic::async_trait]
impl WriteService for FailingWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        Err(Status::unavailable("node down"))
    }
}

/// A node that answers, but reports the write as failed — the engine refused it.
struct ErrorResultWriteService {
    message: String,
}

#[tonic::async_trait]
impl WriteService for ErrorResultWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        Ok(Response::new(ExecuteWriteResponse {
            results: vec![WriteResult {
                success: false,
                rows_affected: 0,
                error: Some(ErrorDetail {
                    code: 0,
                    message: self.message.clone(),
                }),
            }],
        }))
    }
}

/// A node whose response carries no result at all, which no correct node sends.
struct EmptyResultWriteService;

#[tonic::async_trait]
impl WriteService for EmptyResultWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        Ok(Response::new(ExecuteWriteResponse { results: vec![] }))
    }
}

/// Records every request it receives and answers with one successful result per
/// statement, as a core node applying a batch does. Lets a test inspect what the
/// coordinator actually put on the wire.
struct RecordingBatchWriteService {
    requests: Arc<Mutex<Vec<ExecuteWriteRequest>>>,
}

#[tonic::async_trait]
impl WriteService for RecordingBatchWriteService {
    async fn execute_write(
        &self,
        request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        let request = request.into_inner();
        // Distinct row counts per statement so a test can tell them apart.
        let results = (0..request.statements.len())
            .map(|idx| WriteResult {
                success: true,
                rows_affected: idx as i64 + 1,
                error: None,
            })
            .collect();
        self.requests.lock().await.push(request);
        Ok(Response::new(ExecuteWriteResponse { results }))
    }
}

/// A node that rolled an atomic batch back: it reports the statements it managed
/// to run as successful and the one that broke as failed. Nothing was applied.
struct RolledBackBatchWriteService;

#[tonic::async_trait]
impl WriteService for RolledBackBatchWriteService {
    async fn execute_write(
        &self,
        request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        let count = request.get_ref().statements.len();
        let results = (0..count)
            .map(|idx| {
                let failed = idx + 1 == count;
                WriteResult {
                    success: !failed,
                    rows_affected: if failed { 0 } else { 1 },
                    error: failed.then(|| ErrorDetail {
                        code: 0,
                        message: "constraint violation, transaction rolled back".to_string(),
                    }),
                }
            })
            .collect();
        Ok(Response::new(ExecuteWriteResponse { results }))
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn start_mock_server<S: WriteService>(svc: S) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(WriteServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// A catalog holding `nodes`, all Alive, at the addresses their mock servers are
/// listening on.
fn catalog_with(nodes: &[(&str, SocketAddr)]) -> Arc<MetadataCatalog> {
    let catalog = temp_catalog();
    for (node_id, address) in nodes {
        catalog
            .put_node(&NodeMeta {
                node_id: node_id.to_string(),
                advertised_address: address.to_string(),
                state: NodeState::Alive as i32,
                last_heartbeat: None,
                registered_at: None,
            })
            .unwrap();
    }
    Arc::new(catalog)
}

/// A manager whose retry loop ticks fast enough that a test can watch a resend
/// happen instead of waiting out the production backoff.
fn manager(catalog: &Arc<MetadataCatalog>) -> ReplicationManager {
    ReplicationManager::new(
        Arc::clone(catalog),
        Arc::new(ChannelPool::new()),
        RetryConfig {
            initial_retry_ms: 50,
            max_retry_ms: 200,
        },
    )
}

/// Bucket 0 of `orders`, held by `primary` with `replicas`.
fn shard(primary: &str, replicas: &[&str]) -> ShardMeta {
    ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: primary.to_string(),
        replica_node_ids: replicas.iter().map(|id| id.to_string()).collect(),
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    }
}

/// The single-statement write used wherever only the outcome is under test.
async fn insert(manager: &ReplicationManager, shard: &ShardMeta, quorum: usize) -> Result<u64> {
    manager
        .execute_write_with_quorum(shard, "INSERT INTO orders VALUES (1)", &[], "w1", quorum)
        .await
}

fn statement(sql: &str, shard_id: &str) -> BatchStatement {
    BatchStatement {
        sql: sql.to_string(),
        params: vec![],
        shard_id: shard_id.to_string(),
    }
}

/// Wait for a node's request count to reach `target`, failing if it never does.
///
/// The retry loop is driven by wall-clock sleeps, so a test that slept a guessed
/// interval and then asserted would be asserting on the scheduler's luck.
async fn await_calls(calls: &Arc<Mutex<u32>>, target: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while *calls.lock().await < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "node was asked {} times, expected {target}",
            *calls.lock().await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Quorum
// ---------------------------------------------------------------------------

/// The row count reported is the largest any acking node reported, not the
/// primary's: a replica that applied more rows means the primary under-reported,
/// and the client is told the number of rows the write actually touched.
#[tokio::test]
async fn a_write_every_node_acks_reports_the_highest_row_count() {
    let primary = start_mock_server(FlakyWriteService::healthy(3)).await;
    let replica = start_mock_server(FlakyWriteService::healthy(5)).await;
    let catalog = catalog_with(&[("node-1", primary), ("node-2", replica)]);

    let rows = insert(&manager(&catalog), &shard("node-1", &["node-2"]), 2)
        .await
        .expect("both nodes acked");

    assert_eq!(rows, 5);
}

/// A replica that is down does not fail the write. Durability is the primary's
/// job and the quorum's; the replica is tailed the write later.
#[tokio::test]
async fn a_failed_replica_does_not_fail_a_write_whose_quorum_is_still_met() {
    let healthy = start_mock_server(FlakyWriteService::healthy(1)).await;
    let down = start_mock_server(FailingWriteService).await;
    let catalog = catalog_with(&[("node-1", healthy), ("node-2", healthy), ("node-3", down)]);

    let rows = insert(
        &manager(&catalog),
        &shard("node-1", &["node-2", "node-3"]),
        2,
    )
    .await
    .expect("the primary and one replica are quorum enough");

    assert_eq!(rows, 1);
}

/// A write the primary refused is not durable anywhere, so it fails — and it
/// fails with the node's own reason, since "which node, on which shard, why" is
/// the whole of what the client can act on.
#[tokio::test]
async fn a_write_the_primary_refuses_fails_with_that_nodes_reason() {
    let down = start_mock_server(FailingWriteService).await;
    let healthy = start_mock_server(FlakyWriteService::healthy(1)).await;
    let catalog = catalog_with(&[("node-1", down), ("node-2", healthy)]);

    let error = insert(&manager(&catalog), &shard("node-1", &["node-2"]), 2)
        .await
        .expect_err("a write the primary did not take cannot be reported as done")
        .to_string();

    assert!(error.contains("node-1"), "{error}");
    assert!(error.contains("orders_shard0"), "{error}");
    assert!(error.contains("node down"), "{error}");
}

/// The primary acked, so the write is durable — and it still fails, because the
/// configured quorum is the durability the client was promised.
#[tokio::test]
async fn a_write_short_of_quorum_fails_even_though_the_primary_acked() {
    let healthy = start_mock_server(FlakyWriteService::healthy(1)).await;
    let down = start_mock_server(FailingWriteService).await;
    let catalog = catalog_with(&[("node-1", healthy), ("node-2", down), ("node-3", down)]);

    let error = insert(
        &manager(&catalog),
        &shard("node-1", &["node-2", "node-3"]),
        3,
    )
    .await
    .expect_err("one ack out of three cannot satisfy a quorum of three")
    .to_string();

    assert!(error.contains("quorum not reached"), "{error}");
}

/// A primary the catalog has no address for makes the shard unwritable: there is
/// no node that could take the write, and no replica may stand in for it.
#[tokio::test]
async fn a_shard_whose_primary_the_catalog_forgot_cannot_be_written() {
    let healthy = start_mock_server(FlakyWriteService::healthy(1)).await;
    let catalog = catalog_with(&[("node-2", healthy)]);

    let error = insert(&manager(&catalog), &shard("node-1", &["node-2"]), 1)
        .await
        .expect_err("a shard with no reachable primary is unavailable")
        .to_string();

    assert!(error.contains("shard0"), "{error}");
}

/// A replica the catalog has no address for is simply not a target — it holds no
/// current data to keep current, so it must not count against the write the way a
/// known-but-unreachable node does.
#[tokio::test]
async fn a_replica_the_catalog_forgot_is_not_a_target() {
    let healthy = start_mock_server(FlakyWriteService::healthy(1)).await;
    let catalog = catalog_with(&[("node-1", healthy)]);

    let rows = insert(&manager(&catalog), &shard("node-1", &["node-2"]), 1)
        .await
        .expect("the primary alone meets a quorum of one");

    assert_eq!(rows, 1);
}

/// A node that answers but reports the write as failed, and a node that answers
/// with no result at all, both fail the write. The first must carry the node's
/// reason to the client; the second has none to carry, and a malformed response
/// must not be read as a successful write of zero rows.
#[tokio::test]
async fn a_node_that_does_not_apply_the_write_fails_it() {
    let refused = start_mock_server(ErrorResultWriteService {
        message: "disk full".to_string(),
    })
    .await;
    let catalog = catalog_with(&[("node-1", refused)]);

    let error = insert(&manager(&catalog), &shard("node-1", &[]), 1)
        .await
        .expect_err("a write the engine refused is not a write")
        .to_string();
    assert!(error.contains("disk full"), "{error}");

    let silent = start_mock_server(EmptyResultWriteService).await;
    let catalog = catalog_with(&[("node-1", silent)]);

    let error = insert(&manager(&catalog), &shard("node-1", &[]), 1)
        .await
        .expect_err("a response with no result says nothing was applied")
        .to_string();
    assert!(error.contains("no results returned"), "{error}");
}

/// Multi-shard DML is not atomic. `pgwire_handler::handle_dml` writes the target
/// shards in sequence with no cross-shard rollback, which this drives directly:
/// shard0 commits, shard1's primary is down, and shard0's write stays applied.
/// That partially-applied statement is the documented behaviour, pinned here so
/// it cannot change silently.
#[tokio::test]
async fn a_multi_shard_write_leaves_the_earlier_shard_committed_when_a_later_one_fails() {
    let node0 = FlakyWriteService::healthy(1);
    let shard0_calls = node0.calls();
    let shard0_addr = start_mock_server(node0).await;
    let down = start_mock_server(FailingWriteService).await;
    let catalog = catalog_with(&[("node-0", shard0_addr), ("node-1", down)]);
    let manager = manager(&catalog);

    let shard0 = shard("node-0", &[]);
    let shard1 = ShardMeta {
        shard_id: "shard1".to_string(),
        hash_bucket: 1,
        ..shard("node-1", &[])
    };

    manager
        .execute_write_with_quorum(&shard0, "DELETE FROM orders_shard0", &[], "ms-0", 1)
        .await
        .expect("the first shard commits");
    manager
        .execute_write_with_quorum(&shard1, "DELETE FROM orders_shard1", &[], "ms-1", 1)
        .await
        .expect_err("the second shard's primary is down");

    assert_eq!(
        *shard0_calls.lock().await,
        1,
        "shard0 committed its write before the statement failed, and nothing rolls it back"
    );
}

// ---------------------------------------------------------------------------
// Retry loop
// ---------------------------------------------------------------------------

/// A replica that missed the write is sent it again once it recovers, without the
/// client being told anything: that is the whole point of acking on quorum.
#[tokio::test]
async fn a_replica_that_missed_a_write_is_sent_it_again() {
    let primary = start_mock_server(FlakyWriteService::healthy(1)).await;
    let lagging = FlakyWriteService::refusing(1);
    let lagging_calls = lagging.calls();
    let lagging_addr = start_mock_server(lagging).await;
    let catalog = catalog_with(&[("node-1", primary), ("node-2", lagging_addr)]);

    insert(&manager(&catalog), &shard("node-1", &["node-2"]), 1)
        .await
        .expect("the primary meets a quorum of one");

    // One refused attempt during the write, then the resend from the retry loop.
    await_calls(&lagging_calls, 2).await;
}

/// A node still marked Dead is skipped rather than retried, and its queue is left
/// intact: the write waits for the node to rejoin instead of being spent on a
/// machine that cannot take it, and instead of being dropped.
#[tokio::test]
async fn a_dead_node_keeps_its_queued_write_until_it_rejoins() {
    let primary = start_mock_server(FlakyWriteService::healthy(1)).await;
    // Never stops refusing, so every resend re-queues and the count only ever
    // reflects how many times the loop was willing to try.
    let lagging = FlakyWriteService::refusing(u32::MAX);
    let lagging_calls = lagging.calls();
    let lagging_addr = start_mock_server(lagging).await;
    let catalog = catalog_with(&[("node-1", primary), ("node-2", lagging_addr)]);

    insert(&manager(&catalog), &shard("node-1", &["node-2"]), 1)
        .await
        .expect("the primary meets a quorum of one");
    catalog
        .update_node_state("node-2", NodeState::Dead)
        .unwrap();

    // Several retry ticks' worth of time, during which a Dead node is skipped.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        *lagging_calls.lock().await,
        1,
        "only the original attempt: a Dead node must not be retried"
    );

    catalog
        .update_node_state("node-2", NodeState::Alive)
        .unwrap();

    // The queue survived being skipped, so rejoining is what replays the write.
    await_calls(&lagging_calls, 2).await;
}

// ---------------------------------------------------------------------------
// Transaction batches
//
// This is what COMMIT ships for a buffered transaction block: every statement of
// the block that shares a node set travels in ONE request marked atomic, so the
// node applies all of them or none.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_transaction_batch_reaches_every_node_as_one_atomic_request() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let addr = start_mock_server(RecordingBatchWriteService {
        requests: Arc::clone(&requests),
    })
    .await;
    let catalog = catalog_with(&[("node-1", addr), ("node-2", addr)]);

    // Two tables on the same node set, exactly what a block writing both buffers.
    let statements = vec![
        statement("INSERT INTO orders_shard0 VALUES (1)", "orders_shard0"),
        statement(
            "INSERT INTO customers_shard0 VALUES (2)",
            "customers_shard0",
        ),
    ];

    let rows = manager(&catalog)
        .execute_transaction_with_quorum(&shard("node-1", &["node-2"]), statements, "txn-1", 2)
        .await
        .expect("both nodes ack the batch");

    // One row count per statement, in the order the client issued them.
    assert_eq!(rows, vec![1, 2]);

    let requests = requests.lock().await;
    assert_eq!(
        requests.len(),
        2,
        "the primary and the replica each receive the batch"
    );
    for request in requests.iter() {
        assert!(
            request.atomic,
            "a transaction batch must be marked atomic, or the node would apply its statements one by one"
        );
        assert_eq!(request.write_id, "txn-1");
        let sql: Vec<&str> = request
            .statements
            .iter()
            .map(|stmt| stmt.sql.as_str())
            .collect();
        assert_eq!(
            sql,
            vec![
                "INSERT INTO orders_shard0 VALUES (1)",
                "INSERT INTO customers_shard0 VALUES (2)"
            ],
            "the whole block travels in one request, in client order"
        );
        // The batch is named by one shard, but each statement keeps its own
        // target table — otherwise the second write would land on `orders`.
        assert_eq!(request.statements[1].shard_id, "customers_shard0");
    }
}

/// The node reported the first statement as applied and the second as failed.
/// Because the batch was atomic, nothing was applied, so the caller must see an
/// error rather than a partial success it might report as a commit.
#[tokio::test]
async fn a_transaction_batch_the_primary_rolled_back_fails_the_commit() {
    let addr = start_mock_server(RolledBackBatchWriteService).await;
    let catalog = catalog_with(&[("node-1", addr)]);

    let statements = vec![
        statement("INSERT INTO orders_shard0 VALUES (1)", "orders_shard0"),
        statement("INSERT INTO orders_shard0 VALUES (1)", "orders_shard0"),
    ];

    let error = manager(&catalog)
        .execute_transaction_with_quorum(&shard("node-1", &[]), statements, "txn-2", 1)
        .await
        .expect_err("a rolled-back batch cannot be reported as committed")
        .to_string();

    assert!(
        error.contains("constraint violation"),
        "the node's reason should reach the client, got: {error}"
    );
}

/// A lagging replica does not fail the commit: the primary plus quorum decide,
/// and the replica is queued to receive the same batch — still as one
/// transaction — from the retry loop.
#[tokio::test]
async fn a_transaction_batch_commits_on_the_primary_when_a_replica_lags() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let primary = start_mock_server(RecordingBatchWriteService {
        requests: Arc::clone(&requests),
    })
    .await;
    let down = start_mock_server(FailingWriteService).await;
    let catalog = catalog_with(&[("node-1", primary), ("node-2", down)]);

    let statements = vec![statement(
        "INSERT INTO orders_shard0 VALUES (1)",
        "orders_shard0",
    )];

    let rows = manager(&catalog)
        .execute_transaction_with_quorum(&shard("node-1", &["node-2"]), statements, "txn-3", 1)
        .await
        .expect("quorum of one is met by the primary alone");

    assert_eq!(rows, vec![1]);
    assert_eq!(
        requests.lock().await.len(),
        1,
        "the primary applied the batch"
    );
}
