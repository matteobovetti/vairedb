//! What the channel pool promises a caller, observed through real connections.
//!
//! Taking the server away is the instrument. A cache claim is only testable if a
//! cache hit and a fresh dial can be told apart, and they look identical while the
//! node is up — so these tests stop the server and then ask again: a `get` that
//! still succeeds can only have been answered from the cache, and one that fails
//! can only have dialed. The previous suite called `get` twice, asserted both were
//! `Ok`, and commented that the second came from the cache; a pool with no cache at
//! all passes that, as does one that ignores the address it was given.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use vairedb_common::proto::vairedb::v1::node_service_server::NodeServiceServer;
use vairedb_coordinator::channel_pool::ChannelPool;
use vairedb_coordinator::node_service::NodeServiceImpl;

mod common;
use common::temp_catalog;

/// A running gRPC server on an ephemeral port, and the handle that stops it.
///
/// The socket is bound before `start` returns, which is why there is no sleep here:
/// the previous fixture bound a listener to claim a port, dropped it, let the server
/// re-bind the same port and slept 50 ms hoping that had finished. That sleep was
/// either wasted time or a flake, and the drop-and-rebind let an unrelated process
/// take the port in between.
struct TestServer {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .expect("an ephemeral port is available");
        let addr = incoming.local_addr().expect("the socket is bound");

        let (shutdown, stopped) = oneshot::channel();
        let service = NodeServiceImpl::new(Arc::new(temp_catalog()));
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    stopped.await.ok();
                })
                .await
                .expect("the server serves until it is asked to stop");
        });

        Self {
            addr,
            shutdown,
            task,
        }
    }

    /// Stop the server and wait for the port to be released, so a later dial to this
    /// address is refused rather than racing the shutdown.
    async fn stop(self) -> SocketAddr {
        let addr = self.addr;
        drop(self.shutdown);
        self.task.await.expect("the server task ends cleanly");
        addr
    }
}

/// An address whose SYN goes unanswered, which is what an unreachable core node looks
/// like to the coordinator: the dial stays pending for the whole connect timeout.
///
/// `192.0.2.0/24` is TEST-NET-1, reserved by RFC 5737 and routed nowhere. A socket on
/// localhost cannot stand in for this: accepting the connection *completes* the dial,
/// because `Endpoint::connect` resolves once TCP is established and leaves the HTTP/2
/// handshake to the first request. A local listener that never accepts does not work
/// either — the kernel completes the handshake from the backlog on its own.
const UNANSWERING: &str = "192.0.2.1:1";

/// The cache is consulted: the second `get` is answered after the server it would
/// have dialed is gone.
#[tokio::test]
async fn a_channel_is_dialed_once_and_then_served_from_the_cache() {
    let server = TestServer::start().await;
    let pool = ChannelPool::new();
    let address = server.addr.to_string();

    pool.get(&address).await.expect("the server is listening");
    server.stop().await;

    pool.get(&address)
        .await
        .expect("the cached channel is returned without dialing the stopped server");
}

/// The cache is keyed by address. A pool that returned whatever it had cached would
/// answer the second call with the first node's channel and wrongly succeed.
#[tokio::test]
async fn an_address_nothing_is_listening_on_is_an_error_even_with_another_cached() {
    let server = TestServer::start().await;
    let vacated = TestServer::start().await.stop().await;
    let pool = ChannelPool::new();

    pool.get(&server.addr.to_string())
        .await
        .expect("the server is listening");

    pool.get(&vacated.to_string())
        .await
        .expect_err("nothing is listening on the vacated port");
}

/// A dial to an unreachable node does not stall the pool for any other address.
///
/// The pool used to hold its write guard across the dial, so this is the failure it
/// prevents: one unreachable node made every *other* node's channel unobtainable for
/// as long as its dial ran — writes to a healthy cluster held up by the one node
/// already excluded from it. Under that version the second `get` never reaches the map
/// and the timeout below expires.
///
/// The pending dial is asserted to still be pending, because everything this test
/// claims rests on that: a fixture that stopped hanging would leave the test passing
/// while checking nothing, which is how the same assertion phrased against a local
/// socket went unnoticed.
#[tokio::test(flavor = "multi_thread")]
async fn a_dial_to_an_unreachable_node_does_not_block_another_address() {
    let server = TestServer::start().await;
    let pool = Arc::new(ChannelPool::new());

    let stuck = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.get(UNANSWERING).await }
    });
    // Long enough for the task to be polled into the dial, short against the pool's
    // own 5 s connect timeout.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !stuck.is_finished(),
        "the dial to {UNANSWERING} resolved, so this test no longer exercises anything"
    );

    let reachable =
        tokio::time::timeout(Duration::from_secs(2), pool.get(&server.addr.to_string()))
            .await
            .expect("the pool is not blocked by the pending dial");
    reachable.expect("the server is listening");

    stuck.abort();
}

/// Concurrent first callers for one address all get a usable channel.
///
/// This pins that the race is neither a deadlock nor an error — not that exactly one
/// dial happens, which the pool no longer promises: dialing without the lock means
/// simultaneous first callers may each dial, and all but the first channel inserted
/// is dropped.
#[tokio::test]
async fn concurrent_first_gets_for_one_address_all_succeed() {
    let server = TestServer::start().await;
    let pool = Arc::new(ChannelPool::new());
    let address = server.addr.to_string();

    let racers: Vec<_> = (0..10)
        .map(|_| {
            let pool = Arc::clone(&pool);
            let address = address.clone();
            tokio::spawn(async move { pool.get(&address).await })
        })
        .collect();

    for racer in racers {
        racer
            .await
            .expect("the task did not panic")
            .expect("the server is listening");
    }
}
