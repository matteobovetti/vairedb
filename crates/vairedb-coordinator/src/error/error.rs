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

    /// Passed through for the same reason as [`Self::NodeExecFailed`]: `ParserError`'s
    /// `Display` already opens with "sql parser error". The client's wording is built
    /// separately by `classify_error` from the inner error.
    #[error("{0}")]
    SqlParse(#[from] crate::sqlparser::parser::ParserError),

    /// A statement form VaireDB refuses by name instead of answering as if it had
    /// applied it. Raised at parse time, which is where a clause an upstream
    /// compatibility rewrite would otherwise discard is still visible — see
    /// [`crate::pgwire_handler::parser::parse_sql`]. The message is already
    /// client-facing, so it is passed through unchanged.
    #[error("{0}")]
    Unsupported(String),

    /// A literal that is not a valid input for the type it is cast to, refused at parse
    /// time with the message *and* the SQLSTATE PostgreSQL raises for that same input —
    /// which is why the code travels with the message instead of being derived from the
    /// variant: PostgreSQL reports `'a\12'::bytea` as `22P02` and `'\xzz'::bytea` as
    /// `22023`, and a client that moved the cast from a `SELECT` to an `INSERT` should read
    /// the same pair either way. See [`vairedb_common::bytea_in`].
    #[error("{message}")]
    InvalidValue { message: String, code: VdbErrorCode },

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("shard not assigned: {0}")]
    ShardNotAssigned(String),

    #[error("null shard key: {0}")]
    NullShardKey(String),

    /// The statement pins the shard key to something the coordinator cannot
    /// reduce to a value, so the owning shard is not computable. Rejected rather
    /// than guessed: a guessed shard stores the row where no lookup finds it.
    #[error("unroutable shard key: {0}")]
    UnroutableShardKey(String),

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

    /// Passed through like [`Self::Unsupported`]: [`NodeError`]'s own `Display`
    /// already opens with "node execution failed", and prefixing it again said it
    /// twice. The client never reads this text anyway — `classify_error` rebuilds the
    /// message from the fields, so `Display` here serves logs and error chains.
    #[error("{0}")]
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
            CoordinatorError::UnroutableShardKey(_) => VdbErrorCode::FeatureNotSupported,
            CoordinatorError::QuorumNotReached { .. } => VdbErrorCode::QuorumNotReached,
            CoordinatorError::ShardUnavailable(_) => VdbErrorCode::ShardUnavailable,
            CoordinatorError::NoAliveNodes => VdbErrorCode::NoAliveNodes,
            CoordinatorError::Anonymization(_) => VdbErrorCode::FeatureNotSupported,
            CoordinatorError::SqlParse(_) => VdbErrorCode::SqlSyntaxError,
            CoordinatorError::Unsupported(_) => VdbErrorCode::FeatureNotSupported,
            CoordinatorError::InvalidValue { code, .. } => *code,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn node_error(code: i32) -> CoordinatorError {
        CoordinatorError::NodeExecFailed(Box::new(NodeError {
            message: "disk full".to_string(),
            error_code: code,
            shard_id: "orders_shard0".to_string(),
            node_id: "node-1".to_string(),
        }))
    }

    /// Every variant reports the code the client reads as SQLSTATE, and no two
    /// unrelated failures collapse onto one.
    ///
    /// These are tested here rather than from `tests/`, which is where they used to
    /// live: `vdb_error_code` is `pub(crate)`, so an integration test cannot call it —
    /// which is why the module's one real function had no coverage at all and only its
    /// `Display` strings were pinned.
    #[test]
    fn each_variant_reports_its_own_code() {
        let cases = [
            (
                CoordinatorError::TableNotFound("orders".into()),
                VdbErrorCode::TableNotFound,
            ),
            (
                CoordinatorError::NodeNotFound("node-1".into()),
                VdbErrorCode::NodeNotFound,
            ),
            (
                CoordinatorError::ShardNotAssigned("shard0".into()),
                VdbErrorCode::ShardNotAssigned,
            ),
            (
                CoordinatorError::QuorumNotReached { needed: 2, got: 1 },
                VdbErrorCode::QuorumNotReached,
            ),
            (
                CoordinatorError::ShardUnavailable("shard3".into()),
                VdbErrorCode::ShardUnavailable,
            ),
            (CoordinatorError::NoAliveNodes, VdbErrorCode::NoAliveNodes),
            (
                CoordinatorError::Serialization("bad bytes".into()),
                VdbErrorCode::SerializationError,
            ),
            (
                CoordinatorError::Internal("broke".into()),
                VdbErrorCode::InternalError,
            ),
            (
                CoordinatorError::Catalog(redb::Error::Corrupted("bad".into())),
                VdbErrorCode::CatalogAccessError,
            ),
            (
                CoordinatorError::CatalogStorage(redb::StorageError::Corrupted("bad".into())),
                VdbErrorCode::CatalogStorageError,
            ),
            // The redb family is five variants routed to four codes, which is exactly
            // the shape a mis-wired arm hides in.
            (
                CoordinatorError::CatalogTable(redb::TableError::TableDoesNotExist("t".into())),
                VdbErrorCode::CatalogAccessError,
            ),
            (
                CoordinatorError::CatalogTransaction(redb::TransactionError::Storage(
                    redb::StorageError::Corrupted("bad".into()),
                )),
                VdbErrorCode::CatalogTransactionError,
            ),
            (
                CoordinatorError::CatalogCommit(redb::CommitError::Storage(
                    redb::StorageError::Corrupted("bad".into()),
                )),
                VdbErrorCode::CatalogCommitError,
            ),
            (
                CoordinatorError::SqlParse(crate::sqlparser::parser::ParserError::TokenizerError(
                    "invalid character".into(),
                )),
                VdbErrorCode::SqlSyntaxError,
            ),
        ];

        for (err, expected) in cases {
            assert_eq!(err.vdb_error_code(), expected, "for {err}");
        }
    }

    /// The four refusal-shaped variants deliberately share `FeatureNotSupported`: each
    /// is a form VaireDB declines rather than answers wrongly, and PostgreSQL has one
    /// SQLSTATE for that. Pinned so the sharing stays a decision instead of drift.
    #[test]
    fn a_refused_statement_reports_feature_not_supported() {
        for err in [
            CoordinatorError::NullShardKey("id is null".into()),
            CoordinatorError::UnroutableShardKey("id = random()".into()),
            CoordinatorError::Anonymization("no secret".into()),
            CoordinatorError::Unsupported("MERGE is not supported".into()),
        ] {
            assert_eq!(err.vdb_error_code(), VdbErrorCode::FeatureNotSupported);
        }
    }

    /// A gRPC failure is narrowed by the node's own status code, because the three
    /// cases a client can act on differ: a missing shard is a routing problem, an
    /// unreachable node is worth retrying elsewhere, and a full write queue is worth
    /// retrying later. Anything else is the coordinator's own bug.
    #[test]
    fn a_grpc_status_is_narrowed_by_its_code() {
        let cases = [
            (tonic::Code::NotFound, VdbErrorCode::ShardNotFound),
            (tonic::Code::Unavailable, VdbErrorCode::NodeUnavailable),
            (tonic::Code::DeadlineExceeded, VdbErrorCode::NodeUnavailable),
            (tonic::Code::ResourceExhausted, VdbErrorCode::WriteQueueFull),
            (tonic::Code::PermissionDenied, VdbErrorCode::InternalError),
        ];

        for (code, expected) in cases {
            let err = CoordinatorError::Grpc(Box::new(tonic::Status::new(code, "from the node")));
            assert_eq!(err.vdb_error_code(), expected, "for {code:?}");
        }
    }

    /// A transport failure is the one gRPC case *not* narrowed: there is no status to
    /// read, because the request never reached a node.
    #[test]
    fn a_transport_failure_is_a_communication_error() {
        // `from_shared` fails with the transport error type without any I/O; the
        // previous test built a runtime and dialed `http://[::1]:0` to obtain one.
        let transport = tonic::transport::Endpoint::from_shared("not a uri".to_string())
            .expect_err("that is not a URI");

        assert_eq!(
            CoordinatorError::GrpcTransport(Box::new(transport)).vdb_error_code(),
            VdbErrorCode::NodeCommunicationError
        );
    }

    /// A node's code is forwarded unchanged, and an unrecognized one becomes
    /// `EngineError` rather than a code the client would read as something specific —
    /// a newer node reporting a code this coordinator does not know is still an engine
    /// failure.
    #[test]
    fn a_node_code_is_forwarded_and_an_unknown_one_becomes_an_engine_error() {
        assert_eq!(
            node_error(VdbErrorCode::WriteConflict as i32).vdb_error_code(),
            VdbErrorCode::WriteConflict
        );
        assert_eq!(node_error(-7).vdb_error_code(), VdbErrorCode::EngineError);
    }

    /// `InvalidValue` carries its code instead of deriving one from the variant, so the
    /// same variant reports whichever SQLSTATE PostgreSQL raises for that input.
    #[test]
    fn an_invalid_value_reports_the_code_it_was_built_with() {
        for code in [
            VdbErrorCode::InvalidTextRepresentation,
            VdbErrorCode::InvalidParameterValue,
        ] {
            let err = CoordinatorError::InvalidValue {
                message: "invalid input syntax for type bytea".to_string(),
                code,
            };
            assert_eq!(err.vdb_error_code(), code);
        }
    }

    /// Variants prefix their source, except those whose message already opens with one:
    /// `Unsupported` and `InvalidValue` are client-facing text, while `NodeError` and
    /// `ParserError` name themselves — prefixing those two said the sentence twice
    /// ("sql parse error: sql parser error: …").
    #[test]
    fn a_message_is_prefixed_unless_it_is_already_a_whole_sentence() {
        assert_eq!(
            CoordinatorError::TableNotFound("orders".into()).to_string(),
            "table not found: orders"
        );
        assert_eq!(
            CoordinatorError::QuorumNotReached { needed: 2, got: 1 }.to_string(),
            "quorum not reached: needed 2, got 1"
        );
        assert_eq!(
            CoordinatorError::SqlParse(crate::sqlparser::parser::ParserError::ParserError(
                "unexpected token".into()
            ))
            .to_string(),
            "sql parser error: unexpected token"
        );
        assert_eq!(
            CoordinatorError::Unsupported("MERGE is not supported".into()).to_string(),
            "MERGE is not supported"
        );
        assert_eq!(
            node_error(VdbErrorCode::EngineError as i32).to_string(),
            "node execution failed on node 'node-1' (shard 'orders_shard0'): disk full"
        );
    }

    /// The pgwire handler holds a `CoordinatorError` across an await while replying to
    /// the client, so the whole enum has to stay `Send + Sync`: a variant carrying a
    /// bare `Box<dyn Error>` would compile here and break every caller.
    #[test]
    fn the_error_crosses_an_await() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CoordinatorError>();
    }
}
