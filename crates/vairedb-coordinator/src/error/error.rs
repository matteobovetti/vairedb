//! Coordinator error type ([`CoordinatorError`]) and its translation to the
//! `VdbErrorCode` carried back to clients over the PostgreSQL wire protocol.

use std::fmt;

use thiserror::Error;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

/// Failure reported by a core node while executing a write or query, preserving
/// the originating node, shard, and the node's own error code so it can be
/// surfaced unchanged to the client.
#[derive(Debug)]
pub struct NodeError {
    /// Human-readable error message from the node.
    pub message: String,
    /// The node's `VdbErrorCode` discriminant, mapped back when reporting.
    pub error_code: i32,
    /// Shard on which the operation failed.
    pub shard_id: String,
    /// Node that reported the failure.
    pub node_id: String,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "node execution failed on node '{}' (shard '{}'): {}",
            self.node_id, self.shard_id, self.message
        )
    }
}

/// All error conditions the coordinator can produce, spanning catalog access,
/// SQL parsing, shard routing, quorum/availability, and node communication.
#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("catalog error: {0}")]
    Catalog(#[from] redb::Error),

    #[error("catalog transaction error: {0}")]
    CatalogTransaction(#[from] redb::TransactionError),

    #[error("catalog table error: {0}")]
    CatalogTable(#[from] redb::TableError),

    #[error("catalog storage error: {0}")]
    CatalogStorage(#[from] redb::StorageError),

    #[error("catalog commit error: {0}")]
    CatalogCommit(#[from] redb::CommitError),

    #[error("sql parse error: {0}")]
    SqlParse(#[from] crate::sqlparser::parser::ParserError),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("shard not assigned: {0}")]
    ShardNotAssigned(String),

    #[error("null shard key: {0}")]
    NullShardKey(String),

    #[error("quorum not reached: needed {needed}, got {got}")]
    QuorumNotReached { needed: usize, got: usize },

    #[error("shard unavailable: primary node unreachable for shard {0}")]
    ShardUnavailable(String),

    #[error("grpc error: {0}")]
    Grpc(Box<tonic::Status>),

    #[error("grpc transport error: {0}")]
    GrpcTransport(Box<tonic::transport::Error>),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("no alive nodes available for shard assignment")]
    NoAliveNodes,

    #[error("anonymization error: {0}")]
    Anonymization(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("node execution failed: {0}")]
    NodeExecFailed(Box<NodeError>),
}

/// Convenience alias for results that fail with [`CoordinatorError`].
pub type Result<T> = std::result::Result<T, CoordinatorError>;

impl CoordinatorError {
    /// Map this error to the `VdbErrorCode` reported to the client. gRPC and
    /// node-execution failures are further narrowed by their inner status/code.
    pub(crate) fn vdb_error_code(&self) -> VdbErrorCode {
        match self {
            CoordinatorError::TableNotFound(_) => VdbErrorCode::TableNotFound,
            CoordinatorError::NodeNotFound(_) => VdbErrorCode::NodeNotFound,
            CoordinatorError::ShardNotAssigned(_) => VdbErrorCode::ShardNotAssigned,
            CoordinatorError::NullShardKey(_) => VdbErrorCode::FeatureNotSupported,
            CoordinatorError::QuorumNotReached { .. } => VdbErrorCode::QuorumNotReached,
            CoordinatorError::ShardUnavailable(_) => VdbErrorCode::ShardUnavailable,
            CoordinatorError::NoAliveNodes => VdbErrorCode::NoAliveNodes,
            CoordinatorError::Anonymization(_) => VdbErrorCode::FeatureNotSupported,
            CoordinatorError::SqlParse(_) => VdbErrorCode::SqlSyntaxError,
            CoordinatorError::Serialization(_) => VdbErrorCode::SerializationError,
            CoordinatorError::Internal(_) => VdbErrorCode::InternalError,
            CoordinatorError::Grpc(status) => match status.code() {
                tonic::Code::NotFound => VdbErrorCode::ShardNotFound,
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    VdbErrorCode::NodeUnavailable
                }
                tonic::Code::ResourceExhausted => VdbErrorCode::WriteQueueFull,
                _ => VdbErrorCode::InternalError,
            },
            CoordinatorError::GrpcTransport(_) => VdbErrorCode::NodeCommunicationError,
            CoordinatorError::NodeExecFailed(node_err) => {
                VdbErrorCode::try_from(node_err.error_code).unwrap_or(VdbErrorCode::EngineError)
            }
            CoordinatorError::Catalog(_) => VdbErrorCode::CatalogAccessError,
            CoordinatorError::CatalogTransaction(_) => VdbErrorCode::CatalogTransactionError,
            CoordinatorError::CatalogTable(_) => VdbErrorCode::CatalogAccessError,
            CoordinatorError::CatalogStorage(_) => VdbErrorCode::CatalogStorageError,
            CoordinatorError::CatalogCommit(_) => VdbErrorCode::CatalogCommitError,
        }
    }
}
