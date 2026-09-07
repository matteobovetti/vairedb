use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Server};

use vairedb_common::proto::vairedb::v1::{
    ErrorDetail, ExecuteWriteRequest, ExecuteWriteResponse, WriteResult,
    write_service_server::{WriteService, WriteServiceServer},
};
use vairedb_coordinator::catalog::{MetadataCatalog, NodeMeta, NodeState, ShardMeta};
use vairedb_coordinator::channel_pool::ChannelPool;
use vairedb_coordinator::replication::{BatchStatement, ReplicationManager, RetryConfig};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_replication_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn make_catalog() -> MetadataCatalog {
    MetadataCatalog::open(&temp_db_path()).unwrap()
}

// ---------------------------------------------------------------------------
// Mock WriteService implementations
// ---------------------------------------------------------------------------

struct SuccessWriteService {
    rows_affected: i64,
}

#[tonic::async_trait]
impl WriteService for SuccessWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        Ok(Response::new(ExecuteWriteResponse {
            results: vec![WriteResult {
                success: true,
                rows_affected: self.rows_affected,
                error: None,
            }],
        }))
    }
}

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

/// Success service that records how many writes it received, so a test can prove
/// a per-shard write actually committed before a later shard failed.
struct CountingSuccessWriteService {
    call_count: Arc<Mutex<u32>>,
    rows_affected: i64,
}

#[tonic::async_trait]
impl WriteService for CountingSuccessWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        *self.call_count.lock().await += 1;
        Ok(Response::new(ExecuteWriteResponse {
            results: vec![WriteResult {
                success: true,
                rows_affected: self.rows_affected,
                error: None,
            }],
        }))
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

struct ToggleWriteService {
    call_count: Arc<Mutex<u32>>,
    fail_until_call: u32,
    rows_affected: i64,
}

#[tonic::async_trait]
impl WriteService for ToggleWriteService {
    async fn execute_write(
        &self,
        _request: Request<ExecuteWriteRequest>,
    ) -> std::result::Result<Response<ExecuteWriteResponse>, Status> {
        let mut count = self.call_count.lock().await;
        *count += 1;
        if *count <= self.fail_until_call {
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

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

fn setup_catalog_with_nodes(catalog: &MetadataCatalog, nodes: &[(&str, &str)]) {
    for (node_id, addr) in nodes {
        catalog
            .put_node(&NodeMeta {
                node_id: node_id.to_string(),
                advertised_address: addr.to_string(),
                state: NodeState::Alive as i32,
                last_heartbeat: None,
                registered_at: None,
            })
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// ReplicationManager — quorum success (all nodes ack)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_all_nodes_ack() {
    let addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;
    let addr_str = addr.to_string();

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &addr_str), ("node-2", &addr_str)]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-1", 2)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// ReplicationManager — quorum met with primary only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_primary_only_quorum_one() {
    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 3 }).await;
    let failing_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &success_addr.to_string()),
            ("node-2", &failing_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-2", 1)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);
}

// ---------------------------------------------------------------------------
// ReplicationManager — primary failure returns ShardUnavailable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_primary_fails() {
    let failing_addr = start_mock_server(FailingWriteService).await;
    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &failing_addr.to_string()),
            ("node-2", &success_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-3", 2)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("node execution failed") || err_str.contains("shard0"),
        "expected NodeExecFailed or ShardUnavailable error, got: {}",
        err_str
    );
}

// ---------------------------------------------------------------------------
// ReplicationManager — quorum not reached
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_not_reached() {
    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;
    let failing_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &success_addr.to_string()),
            ("node-2", &failing_addr.to_string()),
            ("node-3", &failing_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string(), "node-3".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-4", 3)
        .await;

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("quorum") || err_str.contains("Quorum"),
        "expected QuorumNotReached error, got: {}",
        err_str
    );
}

// ---------------------------------------------------------------------------
// ReplicationManager — rows_affected takes max across nodes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_rows_affected_takes_max() {
    let addr_5 = start_mock_server(SuccessWriteService { rows_affected: 5 }).await;
    let addr_3 = start_mock_server(SuccessWriteService { rows_affected: 3 }).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &addr_5.to_string()),
            ("node-2", &addr_3.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-5", 1)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 5);
}

// ---------------------------------------------------------------------------
// ReplicationManager — shard with no primary in address map
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_primary_not_in_catalog() {
    let addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-2", &addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-6", 1)
        .await;

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("shard0"),
        "expected ShardUnavailable error, got: {}",
        err_str
    );
}

// ---------------------------------------------------------------------------
// ReplicationManager — single node, no replicas
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_single_node_no_replicas() {
    let addr = start_mock_server(SuccessWriteService { rows_affected: 2 }).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-7", 1)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
}

// ---------------------------------------------------------------------------
// ReplicationManager — lagging nodes get retried
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lagging_node_gets_retried() {
    let call_count = Arc::new(Mutex::new(0u32));

    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;

    let toggle_svc = ToggleWriteService {
        call_count: Arc::clone(&call_count),
        fail_until_call: 1,
        rows_affected: 1,
    };
    let toggle_addr = start_mock_server(toggle_svc).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &success_addr.to_string()),
            ("node-2", &toggle_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(Arc::clone(&catalog), pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-8", 1)
        .await;

    assert!(result.is_ok());

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let count = *call_count.lock().await;
    assert!(
        count >= 2,
        "expected at least 2 calls (1 initial + 1 retry), got {}",
        count
    );
}

// ---------------------------------------------------------------------------
// ReplicationManager — dead node queues are cleared
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_dead_node_retries_are_cleared() {
    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;
    let failing_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &success_addr.to_string()),
            ("node-2", &failing_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(Arc::clone(&catalog), pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-9", 1)
        .await;
    assert!(result.is_ok());

    // Mark node-2 as dead — retry loop should drop its queue
    catalog
        .update_node_state("node-2", NodeState::Dead)
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

// ---------------------------------------------------------------------------
// ReplicationManager — error result from write service
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_error_result_from_service() {
    let error_addr = start_mock_server(ErrorResultWriteService {
        message: "disk full".to_string(),
    })
    .await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &error_addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-10", 1)
        .await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ReplicationManager — empty result from write service
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_empty_result_from_service() {
    let empty_addr = start_mock_server(EmptyResultWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &empty_addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-11", 1)
        .await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ReplicationManager — multiple replicas, quorum of 2 with 3 nodes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_partial_replica_failure_quorum_met() {
    let success_addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;
    let failing_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &success_addr.to_string()),
            ("node-2", &success_addr.to_string()),
            ("node-3", &failing_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string(), "node-3".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-12", 2)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// ReplicationManager — replica not in address map is skipped
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_quorum_write_replica_not_in_catalog_skipped() {
    let addr = start_mock_server(SuccessWriteService { rows_affected: 1 }).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let result = manager
        .execute_write_with_quorum(&shard, "INSERT INTO orders VALUES (1)", &[], "write-13", 1)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Multi-shard DML is non-atomic. The coordinator (pgwire_handler::handle_dml /
// handle_insert_with_split) writes shards sequentially via repeated
// execute_write_with_quorum calls with NO cross-shard rollback. This test drives
// that loop directly: shard0 commits, then shard1's primary fails. It documents
// that the first shard's write is already durable when the statement errors —
// i.e. a partially-applied multi-shard statement.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multi_shard_write_partial_failure_leaves_first_shard_committed() {
    let shard0_calls = Arc::new(Mutex::new(0u32));
    let shard0_addr = start_mock_server(CountingSuccessWriteService {
        call_count: Arc::clone(&shard0_calls),
        rows_affected: 1,
    })
    .await;
    let failing_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-0", &shard0_addr.to_string()),
            ("node-1", &failing_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard0 = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-0".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };
    let shard1 = ShardMeta {
        shard_id: "shard1".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 1,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    // Sequential per-shard writes, exactly as handle_dml loops over target shards.
    let r0 = manager
        .execute_write_with_quorum(&shard0, "DELETE FROM orders_shard0", &[], "write-ms-0", 1)
        .await;
    assert!(r0.is_ok(), "first shard write should commit");

    let r1 = manager
        .execute_write_with_quorum(&shard1, "DELETE FROM orders_shard1", &[], "write-ms-1", 1)
        .await;
    assert!(r1.is_err(), "second shard write should fail (primary down)");

    // The statement errored, yet shard0's write was already applied and is NOT
    // rolled back — the documented non-atomic behavior for v0.1.
    assert_eq!(
        *shard0_calls.lock().await,
        1,
        "shard0 received and committed its write before the statement failed"
    );
}

// ---------------------------------------------------------------------------
// ReplicationManager — transaction batches (execute_transaction_with_quorum).
// This is what COMMIT ships for a buffered transaction block: every statement
// of the block that shares a node set travels in ONE request marked atomic, so
// the node applies all of them or none. These tests pin the two properties the
// coordinator's transaction semantics rest on — the request really is one atomic
// batch, and a node that rolls it back fails the whole commit.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_batch_reaches_every_node_as_one_atomic_request() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let addr = start_mock_server(RecordingBatchWriteService {
        requests: Arc::clone(&requests),
    })
    .await;
    let addr_str = addr.to_string();

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &addr_str), ("node-2", &addr_str)]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    // Two tables on the same node set, exactly what a block writing both buffers.
    let statements = vec![
        BatchStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1)".to_string(),
            params: vec![],
            shard_id: "orders_shard0".to_string(),
        },
        BatchStatement {
            sql: "INSERT INTO customers_shard0 VALUES (2)".to_string(),
            params: vec![],
            shard_id: "customers_shard0".to_string(),
        },
    ];

    let rows = manager
        .execute_transaction_with_quorum(&shard, statements, "txn-1", 2)
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

#[tokio::test]
async fn test_transaction_batch_rolled_back_by_the_primary_fails_the_commit() {
    let addr = start_mock_server(RolledBackBatchWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(&catalog, &[("node-1", &addr.to_string())]);

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec![],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let statements = vec![
        BatchStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1)".to_string(),
            params: vec![],
            shard_id: "orders_shard0".to_string(),
        },
        BatchStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1)".to_string(),
            params: vec![],
            shard_id: "orders_shard0".to_string(),
        },
    ];

    let result = manager
        .execute_transaction_with_quorum(&shard, statements, "txn-2", 1)
        .await;

    // The node reported the first statement as applied and the second as failed.
    // Because the batch was atomic, nothing was applied, so the caller must see
    // an error rather than a partial success it might report as a commit.
    let err_str = result
        .expect_err("a rolled-back batch cannot be reported as committed")
        .to_string();
    assert!(
        err_str.contains("constraint violation"),
        "the node's reason should reach the client, got: {err_str}"
    );
}

// A lagging replica does not fail the commit: the primary plus quorum decide,
// and the replica is queued to receive the same batch — still as one
// transaction — from the retry loop.
#[tokio::test]
async fn test_transaction_batch_commits_on_primary_when_a_replica_lags() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let primary_addr = start_mock_server(RecordingBatchWriteService {
        requests: Arc::clone(&requests),
    })
    .await;
    let replica_addr = start_mock_server(FailingWriteService).await;

    let catalog = Arc::new(make_catalog());
    setup_catalog_with_nodes(
        &catalog,
        &[
            ("node-1", &primary_addr.to_string()),
            ("node-2", &replica_addr.to_string()),
        ],
    );

    let pool = Arc::new(ChannelPool::new());
    let config = RetryConfig {
        initial_retry_ms: 50,
        max_retry_ms: 200,
    };
    let manager = ReplicationManager::new(catalog, pool, config);

    let shard = ShardMeta {
        shard_id: "shard0".to_string(),
        table_name: "orders".to_string(),
        primary_node_id: "node-1".to_string(),
        replica_node_ids: vec!["node-2".to_string()],
        hash_bucket: 0,
        range_lower: String::new(),
        range_upper: String::new(),
    };

    let statements = vec![BatchStatement {
        sql: "INSERT INTO orders_shard0 VALUES (1)".to_string(),
        params: vec![],
        shard_id: "orders_shard0".to_string(),
    }];

    let rows = manager
        .execute_transaction_with_quorum(&shard, statements, "txn-3", 1)
        .await
        .expect("quorum of one is met by the primary alone");
    assert_eq!(rows, vec![1]);
    assert_eq!(
        requests.lock().await.len(),
        1,
        "the primary applied the batch"
    );
}
