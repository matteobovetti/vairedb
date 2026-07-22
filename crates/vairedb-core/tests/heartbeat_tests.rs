use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use vairedb_common::proto::vairedb::v1::{
    HeartbeatAction, HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
    ReportFailureRequest, ReportFailureResponse,
    node_service_server::{NodeService, NodeServiceServer},
};

use vairedb_core::heartbeat::HeartbeatClient;

#[derive(Default)]
struct MockState {
    registered_nodes: Vec<(String, String)>,
    heartbeat_count: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum DrainBehavior {
    Never,
    AfterFirstHeartbeat,
}

struct MockNodeService {
    state: Arc<Mutex<MockState>>,
    reject_registration: bool,
    drain_behavior: DrainBehavior,
}

impl MockNodeService {
    fn new(state: Arc<Mutex<MockState>>) -> Self {
        Self {
            state,
            reject_registration: false,
            drain_behavior: DrainBehavior::Never,
        }
    }

    fn rejecting(state: Arc<Mutex<MockState>>) -> Self {
        Self {
            state,
            reject_registration: true,
            drain_behavior: DrainBehavior::Never,
        }
    }

    fn draining(state: Arc<Mutex<MockState>>) -> Self {
        Self {
            state,
            reject_registration: false,
            drain_behavior: DrainBehavior::AfterFirstHeartbeat,
        }
    }
}

#[tonic::async_trait]
impl NodeService for MockNodeService {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.lock().await;
        state
            .registered_nodes
            .push((req.node_id.clone(), req.advertised_address.clone()));

        if self.reject_registration {
            return Ok(Response::new(RegisterResponse {
                accepted: false,
                message: "cluster full".to_string(),
            }));
        }

        Ok(Response::new(RegisterResponse {
            accepted: true,
            message: "registered".to_string(),
        }))
    }

    type HeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let state = Arc::clone(&self.state);
        let drain_behavior = self.drain_behavior;
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Ok(Some(_hb)) = stream.message().await {
                let mut s = state.lock().await;
                s.heartbeat_count += 1;
                let count = s.heartbeat_count;
                drop(s);

                let action = if drain_behavior == DrainBehavior::AfterFirstHeartbeat && count >= 1 {
                    HeartbeatAction::Drain
                } else {
                    HeartbeatAction::None
                };

                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let resp = HeartbeatResponse {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: now.as_secs() as i64,
                        nanos: now.subsec_nanos() as i32,
                    }),
                    action: action.into(),
                };

                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn report_failure(
        &self,
        _request: Request<ReportFailureRequest>,
    ) -> Result<Response<ReportFailureResponse>, Status> {
        Ok(Response::new(ReportFailureResponse { acknowledged: true }))
    }
}

async fn start_mock_server(service: MockNodeService) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

/// Starts a mock server that can be shut down via the returned sender.
/// Uses SO_REUSEADDR + SO_REUSEPORT so the port can be rebound immediately.
async fn start_stoppable_mock_server(service: MockNodeService) -> (SocketAddr, watch::Sender<()>) {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket.set_reuse_address(true).unwrap();
    #[cfg(unix)]
    socket.set_reuse_port(true).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    socket.listen(128).unwrap();
    let std_listener: std::net::TcpListener = socket.into();
    let addr = std_listener.local_addr().unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    let (shutdown_tx, mut shutdown_rx) = watch::channel(());

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown_rx.changed().await;
                },
            )
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, shutdown_tx)
}

#[tokio::test]
async fn register_succeeds_with_mock_server() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::new(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-1".to_string(),
        "127.0.0.1:50041".to_string(),
        format!("http://{}", addr),
        5,
    );

    client
        .register(vec![
            "orders_shard0".to_string(),
            "orders_shard1".to_string(),
        ])
        .await
        .unwrap();

    let s = state.lock().await;
    assert_eq!(s.registered_nodes.len(), 1);
    assert_eq!(s.registered_nodes[0].0, "node-1");
    assert_eq!(s.registered_nodes[0].1, "127.0.0.1:50041");
}

#[tokio::test]
async fn register_returns_error_when_rejected() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::rejecting(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-2".to_string(),
        "127.0.0.1:50052".to_string(),
        format!("http://{}", addr),
        5,
    );

    let result = client.register(vec![]).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("rejected"));
}

#[tokio::test]
async fn register_returns_error_when_server_unreachable() {
    let client = HeartbeatClient::new(
        "node-3".to_string(),
        "127.0.0.1:50053".to_string(),
        "http://127.0.0.1:1".to_string(),
        5,
    );

    let result = client.register(vec![]).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("failed to connect"));
}

#[tokio::test]
async fn heartbeat_loop_sends_heartbeats() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::new(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-4".to_string(),
        "127.0.0.1:50054".to_string(),
        format!("http://{}", addr),
        1,
    );

    client.start_heartbeat_loop().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    let s = state.lock().await;
    assert!(
        s.heartbeat_count >= 2,
        "expected at least 2 heartbeats, got {}",
        s.heartbeat_count
    );
}

#[tokio::test]
async fn heartbeat_loop_returns_error_when_server_unreachable() {
    let client = HeartbeatClient::new(
        "node-5".to_string(),
        "127.0.0.1:50055".to_string(),
        "http://127.0.0.1:1".to_string(),
        1,
    );

    let result = client.start_heartbeat_loop().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn register_with_empty_shards_succeeds() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::new(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-6".to_string(),
        "127.0.0.1:50056".to_string(),
        format!("http://{}", addr),
        5,
    );

    client.register(vec![]).await.unwrap();

    let s = state.lock().await;
    assert_eq!(s.registered_nodes.len(), 1);
}

#[tokio::test]
async fn drain_signal_propagates_from_coordinator() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::draining(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-drain".to_string(),
        "127.0.0.1:50060".to_string(),
        format!("http://{}", addr),
        1,
    );

    let mut drain_rx = client.drain_receiver();

    client.start_heartbeat_loop().await.unwrap();

    // The server sends DRAIN on the first heartbeat response, so the signal
    // should arrive within a couple of heartbeat intervals.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        drain_rx.wait_for(|drained| *drained),
    )
    .await;

    assert!(result.is_ok(), "drain signal should have been received");
}

#[tokio::test]
async fn spawn_with_reconnect_returns_drain_receiver() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let addr = start_mock_server(MockNodeService::draining(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-reconnect".to_string(),
        "127.0.0.1:50061".to_string(),
        format!("http://{}", addr),
        1,
    );

    let mut drain_rx = client.spawn_with_reconnect();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        drain_rx.wait_for(|drained| *drained),
    )
    .await;

    assert!(
        result.is_ok(),
        "drain signal should propagate via spawn_with_reconnect"
    );
}

#[tokio::test]
async fn drain_receiver_initially_false() {
    let client = HeartbeatClient::new(
        "node-init".to_string(),
        "127.0.0.1:50062".to_string(),
        "http://127.0.0.1:1".to_string(),
        5,
    );

    let drain_rx = client.drain_receiver();
    assert!(!*drain_rx.borrow());
}

#[tokio::test]
async fn reconnect_loop_retries_after_initial_failure() {
    // Start with server unreachable, then start it — reconnect should succeed.
    let state = Arc::new(Mutex::new(MockState::default()));

    // Pick a port that we'll start the server on after a delay.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // free the port

    let client = HeartbeatClient::new(
        "node-retry".to_string(),
        "127.0.0.1:50070".to_string(),
        format!("http://{}", addr),
        1,
    );

    let drain_rx = client.spawn_with_reconnect();

    // Let the client fail a couple of times (~2s of backoff).
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // Now start the server on the same port.
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(MockNodeService::new(state_clone)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Wait long enough for the client to reconnect and send heartbeats.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let s = state.lock().await;
    assert!(
        s.heartbeat_count >= 1,
        "expected at least 1 heartbeat after reconnect, got {}",
        s.heartbeat_count
    );
    assert!(!*drain_rx.borrow());
}

#[tokio::test]
async fn reconnect_loop_stops_when_drain_received_during_backoff() {
    let state = Arc::new(Mutex::new(MockState::default()));
    let (addr, shutdown_tx) =
        start_stoppable_mock_server(MockNodeService::draining(Arc::clone(&state))).await;

    let client = HeartbeatClient::new(
        "node-drain-backoff".to_string(),
        "127.0.0.1:50071".to_string(),
        format!("http://{}", addr),
        1,
    );

    let mut drain_rx = client.spawn_with_reconnect();

    // The draining server sends DRAIN on first heartbeat. The reconnect loop
    // should detect drain and stop.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        drain_rx.wait_for(|drained| *drained),
    )
    .await;

    assert!(result.is_ok(), "drain should terminate the reconnect loop");
    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn reconnect_loop_keeps_retrying_on_persistent_failure() {
    // With server always unreachable, the reconnect loop should keep retrying
    // (exponential backoff) without panicking. Verify the task stays alive and
    // the drain receiver remains false.
    let client = HeartbeatClient::new(
        "node-persistent-fail".to_string(),
        "127.0.0.1:50072".to_string(),
        "http://127.0.0.1:1".to_string(),
        1,
    );

    let drain_rx = client.spawn_with_reconnect();

    // Let backoff run for a few iterations (1s + 2s + 4s = 7s of retries).
    tokio::time::sleep(Duration::from_secs(7)).await;

    // The loop should still be alive (drain never set).
    assert!(!*drain_rx.borrow());
}

#[tokio::test]
async fn heartbeat_loop_handles_server_dropping_stream() {
    // Server accepts the heartbeat stream but immediately closes its response sender.
    struct DropStreamService;

    #[tonic::async_trait]
    impl NodeService for DropStreamService {
        async fn register(
            &self,
            _request: Request<RegisterRequest>,
        ) -> Result<Response<RegisterResponse>, Status> {
            Ok(Response::new(RegisterResponse {
                accepted: true,
                message: "ok".to_string(),
            }))
        }

        type HeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

        async fn heartbeat(
            &self,
            _request: Request<Streaming<HeartbeatRequest>>,
        ) -> Result<Response<Self::HeartbeatStream>, Status> {
            let (tx, rx) = mpsc::channel(1);
            // Drop the sender immediately — simulates server closing the response stream.
            drop(tx);
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn report_failure(
            &self,
            _request: Request<ReportFailureRequest>,
        ) -> Result<Response<ReportFailureResponse>, Status> {
            Ok(Response::new(ReportFailureResponse { acknowledged: true }))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(DropStreamService))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = HeartbeatClient::new(
        "node-drop".to_string(),
        "127.0.0.1:50073".to_string(),
        format!("http://{}", addr),
        1,
    );

    // start_heartbeat_loop should succeed (stream is opened), but the spawned tasks
    // should handle the closed stream gracefully without panicking.
    let result = client.start_heartbeat_loop().await;
    assert!(result.is_ok());

    // Give the spawned tasks time to notice the closed stream.
    tokio::time::sleep(Duration::from_millis(500)).await;
    // If we reach here without a panic, the test passes.
}

#[tokio::test]
async fn ack_timeout_triggers_reconnect() {
    // A coordinator that accepts the heartbeat stream and consumes inbound
    // heartbeats but NEVER sends an ack back. It holds the response sender open
    // (so the stream is not "closed"), forcing the client to detect liveness via
    // the ack timeout rather than a stream end. Each reopened stream increments
    // `stream_opens`, so a rising count proves ack-timeout drives reconnection.
    #[derive(Default)]
    struct SilentState {
        stream_opens: u64,
    }

    struct SilentService {
        state: Arc<Mutex<SilentState>>,
    }

    #[tonic::async_trait]
    impl NodeService for SilentService {
        async fn register(
            &self,
            _request: Request<RegisterRequest>,
        ) -> Result<Response<RegisterResponse>, Status> {
            Ok(Response::new(RegisterResponse {
                accepted: true,
                message: "ok".to_string(),
            }))
        }

        type HeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

        async fn heartbeat(
            &self,
            request: Request<Streaming<HeartbeatRequest>>,
        ) -> Result<Response<Self::HeartbeatStream>, Status> {
            {
                let mut s = self.state.lock().await;
                s.stream_opens += 1;
            }
            let mut stream = request.into_inner();
            // Channel capacity 1, but we never send — keeping `tx` alive holds the
            // response stream open without ever acking.
            let (tx, rx) = mpsc::channel::<Result<HeartbeatResponse, Status>>(1);
            tokio::spawn(async move {
                let _tx = tx; // hold the sender so the stream stays open (no ack)
                while let Ok(Some(_hb)) = stream.message().await {
                    // Drain inbound heartbeats silently.
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn report_failure(
            &self,
            _request: Request<ReportFailureRequest>,
        ) -> Result<Response<ReportFailureResponse>, Status> {
            Ok(Response::new(ReportFailureResponse { acknowledged: true }))
        }
    }

    let state = Arc::new(Mutex::new(SilentState::default()));
    let state_clone = Arc::clone(&state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(SilentService { state: state_clone }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // interval = 1s, so ack_timeout = 3 intervals = 3s.
    let client = HeartbeatClient::new(
        "node-silent".to_string(),
        "127.0.0.1:50074".to_string(),
        format!("http://{}", addr),
        1,
    );

    let _drain_rx = client.spawn_with_reconnect();

    // In ~9s the client should: open the stream, wait ~3s for an ack that never
    // comes, time out, reconnect, and repeat — yielding multiple stream opens.
    tokio::time::sleep(Duration::from_secs(9)).await;

    let opens = state.lock().await.stream_opens;
    assert!(
        opens >= 2,
        "expected the ack timeout to trigger at least one reconnect (>= 2 stream opens), got {opens}"
    );
}
