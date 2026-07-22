use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_stream::StreamExt;
use tonic::Request;

use vairedb_common::proto::vairedb::v1::{
    FailureType, HeartbeatRequest, NodeStatus, RegisterRequest, ReportFailureRequest, ShardInfo,
    node_service_server::NodeService,
};

use vairedb_coordinator::catalog::{MetadataCatalog, NodeState};
use vairedb_coordinator::node_service::NodeServiceImpl;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_node_svc_{}_{}.redb",
        std::process::id(),
        id
    )
}

fn make_catalog() -> Arc<MetadataCatalog> {
    Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap())
}

fn make_service() -> NodeServiceImpl {
    NodeServiceImpl::new(make_catalog())
}

#[tokio::test]
async fn test_register_node_success() {
    let svc = make_service();

    let req = Request::new(RegisterRequest {
        node_id: "test-node-1".to_string(),
        advertised_address: "10.0.0.1:50041".to_string(),
        shards: vec![ShardInfo {
            shard_id: "orders_shard0".to_string(),
            is_primary: true,
        }],
    });

    let resp = svc.register(req).await.unwrap();
    let inner = resp.into_inner();
    assert!(inner.accepted);
    assert_eq!(inner.message, "registered");
}

#[tokio::test]
async fn test_register_node_persists_to_catalog() {
    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    let req = Request::new(RegisterRequest {
        node_id: "persist-node".to_string(),
        advertised_address: "192.168.1.1:9000".to_string(),
        shards: vec![],
    });

    svc.register(req).await.unwrap();

    let node = catalog.get_node("persist-node").unwrap().unwrap();
    assert_eq!(node.advertised_address, "192.168.1.1:9000");
    assert_eq!(node.state, NodeState::Alive as i32);
}

#[tokio::test]
async fn test_register_multiple_nodes() {
    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    for i in 0..3 {
        let req = Request::new(RegisterRequest {
            node_id: format!("node-{}", i),
            advertised_address: format!("10.0.0.{}:50041", i),
            shards: vec![],
        });
        svc.register(req).await.unwrap();
    }

    let nodes = catalog.list_alive_nodes().unwrap();
    assert_eq!(nodes.len(), 3);
}

#[tokio::test]
async fn test_report_failure_marks_node_suspect() {
    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    let reg = Request::new(RegisterRequest {
        node_id: "fail-node".to_string(),
        advertised_address: "10.0.0.1:50041".to_string(),
        shards: vec![],
    });
    svc.register(reg).await.unwrap();

    let req = Request::new(ReportFailureRequest {
        node_id: "fail-node".to_string(),
        failure_type: FailureType::Duckdb.into(),
        detail: "segfault".to_string(),
        affected_shard_ids: vec!["orders_shard0".to_string()],
    });

    let resp = svc.report_failure(req).await.unwrap();
    assert!(resp.into_inner().acknowledged);

    let node = catalog.get_node("fail-node").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Suspect as i32);
}

#[tokio::test]
async fn test_report_failure_unknown_node() {
    let svc = make_service();

    let req = Request::new(ReportFailureRequest {
        node_id: "unknown-node".to_string(),
        failure_type: FailureType::BallistaExecutor.into(),
        detail: "connection lost".to_string(),
        affected_shard_ids: vec![],
    });

    let resp = svc.report_failure(req).await.unwrap();
    assert!(resp.into_inner().acknowledged);
}

// ---------------------------------------------------------------------------
// Heartbeat stream tests (integration via gRPC server+client)
// ---------------------------------------------------------------------------

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use vairedb_common::proto::vairedb::v1::node_service_client::NodeServiceClient;
use vairedb_common::proto::vairedb::v1::node_service_server::NodeServiceServer;

async fn start_test_server(catalog: Arc<MetadataCatalog>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = NodeServiceImpl::new(catalog);

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    format!("http://{}", addr)
}

#[tokio::test]
async fn test_heartbeat_updates_catalog() {
    let catalog = make_catalog();

    let reg_req = Request::new(RegisterRequest {
        node_id: "hb-node".to_string(),
        advertised_address: "10.0.0.5:50041".to_string(),
        shards: vec![],
    });
    let svc_direct = NodeServiceImpl::new(Arc::clone(&catalog));
    svc_direct.register(reg_req).await.unwrap();

    let addr = start_test_server(Arc::clone(&catalog)).await;
    let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
    let mut client = NodeServiceClient::new(channel);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let outbound = tokio_stream::once(HeartbeatRequest {
        node_id: "hb-node".to_string(),
        timestamp: Some(prost_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }),
        status: NodeStatus::Healthy.into(),
    });

    let resp = client.heartbeat(outbound).await.unwrap();
    let mut resp_stream = resp.into_inner();

    let msg = resp_stream.next().await.unwrap().unwrap();
    assert!(msg.timestamp.is_some());

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let node = catalog.get_node("hb-node").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Alive as i32);
    assert!(node.last_heartbeat.is_some());
}

#[tokio::test]
async fn test_heartbeat_stream_multiple_messages() {
    let catalog = make_catalog();

    let svc_direct = NodeServiceImpl::new(Arc::clone(&catalog));
    let reg_req = Request::new(RegisterRequest {
        node_id: "multi-hb".to_string(),
        advertised_address: "10.0.0.7:50041".to_string(),
        shards: vec![],
    });
    svc_direct.register(reg_req).await.unwrap();

    let addr = start_test_server(Arc::clone(&catalog)).await;
    let channel = Channel::from_shared(addr).unwrap().connect().await.unwrap();
    let mut client = NodeServiceClient::new(channel);

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

    let resp = client.heartbeat(outbound).await.unwrap();
    let mut resp_stream = resp.into_inner();

    for _ in 0..3 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        tx.send(HeartbeatRequest {
            node_id: "multi-hb".to_string(),
            timestamp: Some(prost_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            status: NodeStatus::Healthy.into(),
        })
        .await
        .unwrap();

        let msg = resp_stream.next().await.unwrap().unwrap();
        assert!(msg.timestamp.is_some());
    }

    drop(tx);

    let final_msg = resp_stream.next().await;
    assert!(final_msg.is_none());
}
