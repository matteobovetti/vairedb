//! gRPC `NodeService` implementation that core nodes call to register, stream
//! heartbeats, and report failures. Registration and heartbeats update node
//! liveness in the metadata catalog, which the failure detector reads.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use vairedb_common::proto::vairedb::v1::{
    HeartbeatAction, HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
    ReportFailureRequest, ReportFailureResponse, node_service_server::NodeService,
};

use crate::catalog::{MetadataCatalog, NodeMeta, NodeState};
use crate::util::now_unix_secs;

/// gRPC service handling core-node lifecycle: registration, heartbeat streaming,
/// and failure reports. All liveness state is persisted to the metadata catalog.
pub struct NodeServiceImpl {
    catalog: Arc<MetadataCatalog>,
}

impl NodeServiceImpl {
    /// Create the service backed by the shared metadata catalog.
    pub fn new(catalog: Arc<MetadataCatalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    /// Register a core node, persisting it to the catalog as `Alive` with fresh
    /// heartbeat/registration timestamps. Returns `unavailable` if catalog
    /// storage fails and `internal` for other errors.
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> std::result::Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(
            "node registration: id={}, address={}",
            req.node_id,
            req.advertised_address
        );

        let now = now_unix_secs();

        let node_meta = NodeMeta {
            node_id: req.node_id.clone(),
            advertised_address: req.advertised_address.clone(),
            state: NodeState::Alive as i32,
            last_heartbeat: Some(prost_types::Timestamp {
                seconds: now as i64,
                nanos: 0,
            }),
            registered_at: Some(prost_types::Timestamp {
                seconds: now as i64,
                nanos: 0,
            }),
        };

        self.catalog.put_node(&node_meta).map_err(|e| {
            use crate::error::CoordinatorError;
            tracing::error!(node_id = %req.node_id, error = %e, "failed to register node");
            match e {
                CoordinatorError::CatalogStorage(_) | CoordinatorError::CatalogCommit(_) => {
                    Status::unavailable("failed to register node: storage unavailable")
                }
                _ => Status::internal("failed to register node: internal error"),
            }
        })?;

        tracing::info!("node {} registered successfully", req.node_id);

        Ok(Response::new(RegisterResponse {
            accepted: true,
            message: "registered".to_string(),
        }))
    }

    type HeartbeatStream = ReceiverStream<std::result::Result<HeartbeatResponse, Status>>;

    /// Handle a bidirectional heartbeat stream: each inbound heartbeat refreshes
    /// the node's last-seen time in the catalog and is answered with a timestamp.
    /// Heartbeat-update failures are logged and tolerated; the stream ends when
    /// the client disconnects or the response channel closes.
    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> std::result::Result<Response<Self::HeartbeatStream>, Status> {
        let catalog = Arc::clone(&self.catalog);
        let mut stream = request.into_inner();

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Ok(Some(hb)) = stream.message().await {
                tracing::trace!("heartbeat from node {}", hb.node_id);

                if let Err(e) = catalog.update_node_heartbeat(&hb.node_id) {
                    tracing::warn!("failed to update heartbeat for {}: {}", hb.node_id, e);
                }

                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

                let response = HeartbeatResponse {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: now.as_secs() as i64,
                        nanos: now.subsec_nanos() as i32,
                    }),
                    action: HeartbeatAction::None.into(),
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Record a node-reported failure by marking the node `Suspect` in the
    /// catalog. Always acknowledges; catalog update errors are logged but do not
    /// fail the response.
    async fn report_failure(
        &self,
        request: Request<ReportFailureRequest>,
    ) -> std::result::Result<Response<ReportFailureResponse>, Status> {
        let req = request.into_inner();
        tracing::warn!(
            "failure reported by node {}: type={:?}, detail={}",
            req.node_id,
            req.failure_type,
            req.detail
        );

        if let Err(e) = self
            .catalog
            .update_node_state(&req.node_id, NodeState::Suspect)
        {
            tracing::error!("failed to update node state: {}", e);
        }

        Ok(Response::new(ReportFailureResponse { acknowledged: true }))
    }
}
