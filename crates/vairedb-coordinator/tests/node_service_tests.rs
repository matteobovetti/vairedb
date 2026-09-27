//! The gRPC surface core nodes call: registration, the heartbeat stream, and
//! failure reports.
//!
//! What these assert is membership, and membership gates storage:
//! `list_alive_nodes` admits only `Alive`, so a node this service fails to
//! record, or records in the wrong state, cannot hold a shard — `CREATE TABLE`
//! then fails for want of nodes on a cluster that is in fact whole. The
//! heartbeat tests go over a real tonic server because the response the client
//! gets back is half of the contract: the stream carries an *action*, and a node
//! that is never told to register can never repair its own absence.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::{Channel, Server};

use vairedb_common::proto::vairedb::v1::node_service_client::NodeServiceClient;
use vairedb_common::proto::vairedb::v1::node_service_server::{NodeService, NodeServiceServer};
use vairedb_common::proto::vairedb::v1::{
    FailureType, HeartbeatAction, HeartbeatRequest, NodeStatus, RegisterRequest,
    ReportFailureRequest, ShardInfo,
};

use vairedb_coordinator::catalog::{MetadataCatalog, NodeState};
use vairedb_coordinator::node_service::NodeServiceImpl;

mod common;
use common::temp_catalog;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn make_catalog() -> Arc<MetadataCatalog> {
    Arc::new(temp_catalog())
}

/// Register `node_id` against `catalog` by calling the service directly.
///
/// Registration is a plain unary call, so the tests that only need a node to
/// exist do not pay for a server and a client to get one.
async fn register(catalog: &Arc<MetadataCatalog>, node_id: &str, address: &str) {
    NodeServiceImpl::new(Arc::clone(catalog))
        .register(Request::new(RegisterRequest {
            node_id: node_id.to_string(),
            advertised_address: address.to_string(),
            shards: vec![],
        }))
        .await
        .expect("registration must succeed");
}

/// A healthy heartbeat from `node_id`, stamped now.
///
/// Stamped through the coordinator's own clock helper rather than a second
/// reading of `SystemTime`, so the test cannot disagree with the service about
/// what a timestamp looks like.
fn heartbeat_from(node_id: &str) -> HeartbeatRequest {
    HeartbeatRequest {
        node_id: node_id.to_string(),
        timestamp: Some(vairedb_coordinator::util::now_timestamp()),
        status: NodeStatus::Healthy.into(),
    }
}

/// The epoch seconds of `node_id`'s last heartbeat, which must be recorded.
fn heartbeat_secs(catalog: &MetadataCatalog, node_id: &str) -> i64 {
    catalog
        .get_node(node_id)
        .unwrap()
        .expect("the node is registered")
        .last_heartbeat
        .expect("a registered node has been heard from")
        .seconds
}

/// Serve `catalog` over a real tonic server on an ephemeral port and return a
/// client connected to it.
async fn connected_client(catalog: &Arc<MetadataCatalog>) -> NodeServiceClient<Channel> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = NodeServiceImpl::new(Arc::clone(catalog));

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("the test server accepts a connection");
    NodeServiceClient::new(channel)
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// A registration is acknowledged *and* recorded — both halves, because either
/// one alone is a node that cannot be given work: an unacknowledged node retries
/// forever, and an unrecorded one is invisible to shard placement.
///
/// `last_heartbeat` must be stamped too. Registering is being heard from, and a
/// node left with no heartbeat would be read as never seen and buried by the
/// failure detector on its first scan.
#[tokio::test]
async fn registering_a_node_is_acknowledged_and_recorded_as_alive_and_heard_from() {
    let catalog = make_catalog();
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    let response = svc
        .register(Request::new(RegisterRequest {
            node_id: "test-node-1".to_string(),
            advertised_address: "10.0.0.1:50041".to_string(),
            shards: vec![ShardInfo {
                shard_id: "orders_shard0".to_string(),
                is_primary: true,
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.accepted);
    assert_eq!(response.message, "registered");

    let node = catalog.get_node("test-node-1").unwrap().unwrap();
    assert_eq!(node.advertised_address, "10.0.0.1:50041");
    assert_eq!(node.state, NodeState::Alive as i32);
    assert!(
        node.last_heartbeat.is_some(),
        "registering counts as being heard from"
    );
}

/// Every registered node is listed alive, which is the list shard placement
/// reads. One node overwriting another would leave a cluster that reports itself
/// smaller than it is.
#[tokio::test]
async fn every_registered_node_is_listed_alive() {
    let catalog = make_catalog();

    for i in 0..3 {
        register(&catalog, &format!("node-{i}"), &format!("10.0.0.{i}:50041")).await;
    }

    let mut ids: Vec<String> = catalog
        .list_alive_nodes()
        .unwrap()
        .into_iter()
        .map(|node| node.node_id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["node-0", "node-1", "node-2"]);
}

// ---------------------------------------------------------------------------
// Failure reports
// ---------------------------------------------------------------------------

/// A node reporting its own failure is demoted to `Suspect`, which takes it out
/// of `list_alive_nodes` and so out of shard placement.
#[tokio::test]
async fn a_reported_failure_marks_the_node_suspect() {
    let catalog = make_catalog();
    register(&catalog, "fail-node", "10.0.0.1:50041").await;
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    let response = svc
        .report_failure(Request::new(ReportFailureRequest {
            node_id: "fail-node".to_string(),
            failure_type: FailureType::Duckdb.into(),
            detail: "segfault".to_string(),
            affected_shard_ids: vec!["orders_shard0".to_string()],
        }))
        .await
        .unwrap();
    assert!(response.into_inner().acknowledged);

    let node = catalog.get_node("fail-node").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Suspect as i32);
}

/// A report about a node the catalog does not know is still acknowledged. There
/// is nothing to demote, and refusing would leave the reporting node retrying a
/// report that can never land.
#[tokio::test]
async fn a_failure_reported_for_an_unknown_node_is_still_acknowledged() {
    let catalog = make_catalog();
    let svc = NodeServiceImpl::new(Arc::clone(&catalog));

    let response = svc
        .report_failure(Request::new(ReportFailureRequest {
            node_id: "unknown-node".to_string(),
            failure_type: FailureType::BallistaExecutor.into(),
            detail: "connection lost".to_string(),
            affected_shard_ids: vec![],
        }))
        .await
        .unwrap();

    assert!(response.into_inner().acknowledged);
}

// ---------------------------------------------------------------------------
// The heartbeat stream
// ---------------------------------------------------------------------------

/// A heartbeat refreshes the node's last-seen time and is answered. Without the
/// refresh the failure detector buries a node that is still reporting.
#[tokio::test]
async fn a_heartbeat_refreshes_the_node_and_is_answered() {
    let catalog = make_catalog();
    register(&catalog, "hb-node", "10.0.0.5:50041").await;
    let registered_at = heartbeat_secs(&catalog, "hb-node");

    let mut client = connected_client(&catalog).await;
    let mut responses = client
        .heartbeat(tokio_stream::once(heartbeat_from("hb-node")))
        .await
        .unwrap()
        .into_inner();

    assert!(responses.next().await.unwrap().unwrap().timestamp.is_some());

    // No wait is needed before reading the catalog: the service writes the refresh
    // before it sends the response, so receiving the ack orders the two.
    let node = catalog.get_node("hb-node").unwrap().unwrap();
    assert_eq!(node.state, NodeState::Alive as i32);
    assert!(
        heartbeat_secs(&catalog, "hb-node") >= registered_at,
        "a heartbeat must never move the last-seen time backwards"
    );
}

/// Every message in a stream is acked, and the stream ends when the client's end
/// closes. A response channel that outlived its client would leak the spawned
/// task serving it for as long as the coordinator runs.
#[tokio::test]
async fn a_heartbeat_stream_acks_every_message_and_ends_with_its_client() {
    let catalog = make_catalog();
    register(&catalog, "multi-hb", "10.0.0.7:50041").await;

    let mut client = connected_client(&catalog).await;
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let mut responses = client
        .heartbeat(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    for _ in 0..3 {
        tx.send(heartbeat_from("multi-hb")).await.unwrap();
        assert!(responses.next().await.unwrap().unwrap().timestamp.is_some());
    }

    drop(tx);
    assert!(
        responses.next().await.is_none(),
        "the response stream must end when the client's does"
    );
}

/// A heartbeat naming a node this catalog has no record of is not a lost update
/// to shrug at: the node is running and healthy, but no shard can be placed on a
/// node the catalog does not list, so `CREATE TABLE` fails for want of nodes. A
/// coordinator started over a fresh store sees exactly this from every core that
/// never stopped, and answering REGISTER is the only way out that does not need
/// the cores restarted.
#[tokio::test]
async fn a_heartbeat_from_an_unknown_node_asks_it_to_register() {
    let catalog = make_catalog();
    let mut client = connected_client(&catalog).await;

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let mut responses = client
        .heartbeat(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    tx.send(heartbeat_from("stranger-node")).await.unwrap();
    let msg = responses.next().await.unwrap().unwrap();
    assert_eq!(
        HeartbeatAction::try_from(msg.action).unwrap(),
        HeartbeatAction::Register,
        "an unknown node must be told to register"
    );

    // Once it has registered, the same stream must go back to plain acks — a node
    // that keeps being told to register would reconnect forever.
    register(&catalog, "stranger-node", "10.0.0.9:50041").await;

    tx.send(heartbeat_from("stranger-node")).await.unwrap();
    let msg = responses.next().await.unwrap().unwrap();
    assert_eq!(
        HeartbeatAction::try_from(msg.action).unwrap(),
        HeartbeatAction::None,
        "a registered node must be acked, not asked to register again"
    );
}
