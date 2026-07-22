use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::Channel;

use vairedb_common::proto::vairedb::v1::{
    HeartbeatAction, HeartbeatRequest, HeartbeatResponse, NodeStatus, RegisterRequest, ShardInfo,
    node_service_client::NodeServiceClient,
};

use crate::error::CoreError;

/// Number of heartbeat intervals to wait for a coordinator ack before treating
/// the stream as dead. The coordinator acks every heartbeat, so a healthy stream
/// produces a response roughly once per interval; missing several in a row means
/// the connection is broken (including half-open TCP that the socket won't notice).
const ACK_TIMEOUT_INTERVALS: u32 = 3;

/// Why a heartbeat session ended, so the reconnect loop can react appropriately.
enum SessionOutcome {
    /// Coordinator asked this node to drain — stop permanently.
    Drained,
    /// The response stream closed or errored — reconnect.
    StreamClosed,
    /// No ack arrived within the timeout — treat as dead and reconnect.
    AckTimeout,
}

/// A single established heartbeat stream: owns the outbound sender, the inbound
/// response stream, and the drain channel. `run` drives send/receive/ack-timeout
/// to completion and reports why it ended.
struct HeartbeatSession {
    node_id: String,
    interval: Duration,
    ack_timeout: Duration,
    tx: mpsc::Sender<HeartbeatRequest>,
    inbound: Streaming<HeartbeatResponse>,
    drain_tx: watch::Sender<bool>,
    drain_rx: watch::Receiver<bool>,
}

impl HeartbeatSession {
    async fn run(self) -> SessionOutcome {
        let HeartbeatSession {
            node_id,
            interval,
            ack_timeout,
            tx,
            mut inbound,
            drain_tx,
            mut drain_rx,
        } = self;

        let mut send_tick = tokio::time::interval(interval);
        send_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let ack_deadline = tokio::time::sleep(ack_timeout);
        tokio::pin!(ack_deadline);

        loop {
            tokio::select! {
                _ = send_tick.tick() => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_else(|e| {
                            tracing::warn!(error = %e, "system clock is before Unix epoch, using zero timestamp");
                            Duration::ZERO
                        });
                    let hb = HeartbeatRequest {
                        node_id: node_id.clone(),
                        timestamp: Some(prost_types::Timestamp {
                            seconds: now.as_secs() as i64,
                            nanos: now.subsec_nanos() as i32,
                        }),
                        status: NodeStatus::Healthy.into(),
                    };
                    if tx.send(hb).await.is_err() {
                        tracing::warn!("heartbeat channel closed, stopping heartbeat session");
                        return SessionOutcome::StreamClosed;
                    }
                }
                msg = inbound.message() => {
                    match msg {
                        Ok(Some(resp)) => {
                            // Any response is an ack: the stream is alive, so push the
                            // deadline forward.
                            ack_deadline.as_mut().reset(Instant::now() + ack_timeout);
                            let action = HeartbeatAction::try_from(resp.action)
                                .unwrap_or(HeartbeatAction::None);
                            if action == HeartbeatAction::Drain {
                                tracing::warn!(
                                    "received DRAIN signal from coordinator, initiating graceful shutdown"
                                );
                                let _ = drain_tx.send(true);
                                return SessionOutcome::Drained;
                            }
                            tracing::trace!(action = ?resp.action, "heartbeat ack received");
                        }
                        Ok(None) => {
                            tracing::warn!("heartbeat response stream ended");
                            return SessionOutcome::StreamClosed;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "heartbeat response stream error");
                            return SessionOutcome::StreamClosed;
                        }
                    }
                }
                _ = &mut ack_deadline => {
                    tracing::warn!(
                        timeout_secs = ack_timeout.as_secs(),
                        "no heartbeat ack within timeout, treating stream as dead"
                    );
                    return SessionOutcome::AckTimeout;
                }
                _ = drain_rx.changed() => {
                    if *drain_rx.borrow() {
                        tracing::info!("drain signal observed, stopping heartbeat session");
                        return SessionOutcome::Drained;
                    }
                }
            }
        }
    }
}

/// Client that registers this node with the coordinator and maintains the
/// heartbeat stream.
///
/// It exposes a drain channel (a [`watch`] of `bool`) that flips to `true` when
/// the coordinator requests a graceful shutdown, letting the rest of the node
/// observe the signal and wind down.
pub struct HeartbeatClient {
    node_id: String,
    advertised_address: String,
    coordinator_addr: String,
    interval: Duration,
    drain_tx: watch::Sender<bool>,
    drain_rx: watch::Receiver<bool>,
}

impl HeartbeatClient {
    /// Construct a client for `node_id` targeting `coordinator_addr`, sending a
    /// heartbeat every `interval_secs`. `advertised_address` is the address
    /// peers should use to reach this node.
    pub fn new(
        node_id: String,
        advertised_address: String,
        coordinator_addr: String,
        interval_secs: u64,
    ) -> Self {
        let (drain_tx, drain_rx) = watch::channel(false);
        Self {
            node_id,
            advertised_address,
            coordinator_addr,
            interval: Duration::from_secs(interval_secs),
            drain_tx,
            drain_rx,
        }
    }

    /// A receiver that observes the drain signal; resolves to `true` once the
    /// coordinator has requested a graceful shutdown.
    pub fn drain_receiver(&self) -> watch::Receiver<bool> {
        self.drain_rx.clone()
    }

    /// Register this node and its `shards` with the coordinator.
    ///
    /// Returns [`CoreError::Heartbeat`] if the call fails or the coordinator
    /// rejects the registration.
    pub async fn register(&self, shards: Vec<String>) -> Result<(), CoreError> {
        let mut client = self.connect().await?;

        let shard_infos: Vec<ShardInfo> = shards
            .into_iter()
            .map(|shard_id| ShardInfo {
                shard_id,
                is_primary: true,
            })
            .collect();

        let request = RegisterRequest {
            node_id: self.node_id.clone(),
            advertised_address: self.advertised_address.clone(),
            shards: shard_infos,
        };

        let response = client
            .register(request)
            .await
            .map_err(|e| CoreError::heartbeat("registration failed", e))?;

        let resp = response.into_inner();
        if !resp.accepted {
            return Err(CoreError::Heartbeat(format!(
                "registration rejected: {}",
                resp.message
            )));
        }

        tracing::info!("registered with coordinator successfully");
        Ok(())
    }

    /// Connect to the coordinator and open the bidirectional heartbeat stream,
    /// returning a ready-to-run `HeartbeatSession`. Fails if the connection or the
    /// stream open fails; a successful return means the stream is established.
    async fn establish_session(&self) -> Result<HeartbeatSession, CoreError> {
        let mut client = self.connect().await?;

        let (tx, rx) = mpsc::channel(16);
        let outbound = ReceiverStream::new(rx);
        let response = client
            .heartbeat(outbound)
            .await
            .map_err(|e| CoreError::heartbeat("heartbeat stream failed", e))?;

        Ok(HeartbeatSession {
            node_id: self.node_id.clone(),
            interval: self.interval,
            ack_timeout: self.interval * ACK_TIMEOUT_INTERVALS,
            tx,
            inbound: response.into_inner(),
            drain_tx: self.drain_tx.clone(),
            drain_rx: self.drain_rx.clone(),
        })
    }

    /// Establish a heartbeat stream and run it in a background task. Returns `Ok`
    /// once the stream is established; the session then sends heartbeats, consumes
    /// acks, and propagates a drain signal until the stream ends.
    pub async fn start_heartbeat_loop(&self) -> Result<(), CoreError> {
        let session = self.establish_session().await?;
        tokio::spawn(session.run());
        Ok(())
    }

    /// Spawn the reconnecting heartbeat loop on a background task and return a
    /// drain receiver.
    ///
    /// The loop establishes a session, runs it to completion, and reconnects
    /// with exponential backoff on failure, stopping permanently once a drain
    /// is requested.
    pub fn spawn_with_reconnect(self) -> watch::Receiver<bool> {
        let drain_rx = self.drain_rx.clone();
        tokio::spawn(async move {
            self.reconnect_loop().await;
        });
        drain_rx
    }

    /// Reconnect loop: (re)establish a session, run it until it ends, and back
    /// off before retrying. Returns when a drain is requested.
    async fn reconnect_loop(&self) {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            match self.establish_session().await {
                Ok(session) => {
                    tracing::info!("heartbeat session established");
                    backoff = Duration::from_secs(1);
                    // Run the session to completion in the foreground. It returns only
                    // when the stream dies (closed, errored, or no ack within timeout)
                    // or a drain is requested — a real signal, not a periodic probe.
                    match session.run().await {
                        SessionOutcome::Drained => {
                            tracing::info!("drain signal received, stopping reconnect loop");
                            return;
                        }
                        SessionOutcome::StreamClosed | SessionOutcome::AckTimeout => {
                            tracing::warn!("heartbeat stream ended, reconnecting...");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        backoff_secs = backoff.as_secs(),
                        "heartbeat connection failed, retrying"
                    );
                }
            }

            if *self.drain_rx.borrow() {
                return;
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    /// Open a gRPC connection to the coordinator's node service.
    async fn connect(&self) -> Result<NodeServiceClient<Channel>, CoreError> {
        NodeServiceClient::connect(self.coordinator_addr.clone())
            .await
            .map_err(|e| {
                CoreError::heartbeat(
                    format!(
                        "failed to connect to coordinator at {}",
                        self.coordinator_addr
                    ),
                    e,
                )
            })
    }
}
