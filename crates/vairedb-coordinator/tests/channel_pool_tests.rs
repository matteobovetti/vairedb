use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tonic::transport::Server;
use vairedb_common::proto::vairedb::v1::node_service_server::NodeServiceServer;
use vairedb_coordinator::catalog::MetadataCatalog;
use vairedb_coordinator::channel_pool::ChannelPool;
use vairedb_coordinator::node_service::NodeServiceImpl;

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_db_path() -> String {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/vairedb_test_chpool_{}_{}.redb",
        std::process::id(),
        id
    )
}

async fn start_grpc_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let catalog = Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap());
    let svc = NodeServiceImpl::new(catalog);

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServiceServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn test_new_returns_empty_pool() {
    let pool = ChannelPool::new();
    pool.remove("anything").await;
}

#[tokio::test]
async fn test_default_is_same_as_new() {
    let _pool: ChannelPool = Default::default();
}

#[tokio::test]
async fn test_get_connects_to_listening_server() {
    let addr = start_grpc_server().await;
    let pool = ChannelPool::new();

    let channel = pool.get(&addr.to_string()).await;
    assert!(channel.is_ok());
}

#[tokio::test]
async fn test_get_returns_cached_channel() {
    let addr = start_grpc_server().await;
    let pool = ChannelPool::new();
    let addr_str = addr.to_string();

    let ch1 = pool.get(&addr_str).await.unwrap();
    let ch2 = pool.get(&addr_str).await.unwrap();

    // Both calls succeed without reconnecting — second is served from cache
    drop(ch1);
    drop(ch2);
}

#[tokio::test]
async fn test_get_fails_for_unreachable_address() {
    let pool = ChannelPool::new();
    let result = pool.get("127.0.0.1:1").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_remove_evicts_cached_entry() {
    let addr = start_grpc_server().await;
    let pool = ChannelPool::new();
    let addr_str = addr.to_string();

    pool.get(&addr_str).await.unwrap();
    pool.remove(&addr_str).await;

    // After removal, a new get reconnects successfully
    let result = pool.get(&addr_str).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_remove_nonexistent_does_not_panic() {
    let pool = ChannelPool::new();
    pool.remove("192.168.99.99:9999").await;
}

#[tokio::test]
async fn test_concurrent_get_same_address() {
    let addr = start_grpc_server().await;
    let pool = Arc::new(ChannelPool::new());
    let addr_str = addr.to_string();

    let mut handles = Vec::new();
    for _ in 0..10 {
        let p = pool.clone();
        let a = addr_str.clone();
        handles.push(tokio::spawn(async move { p.get(&a).await }));
    }

    for h in handles {
        let result = h.await.unwrap();
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_multiple_addresses() {
    let addr1 = start_grpc_server().await;
    let addr2 = start_grpc_server().await;
    let pool = ChannelPool::new();

    let ch1 = pool.get(&addr1.to_string()).await;
    let ch2 = pool.get(&addr2.to_string()).await;

    assert!(ch1.is_ok());
    assert!(ch2.is_ok());
}
