//! Turns internal coordinator/engine errors into rich PostgreSQL-style error
//! responses. Classifies an error into a `VdbErrorCode` and SQLSTATE, attaches
//! a client-facing `DETAIL` and `HINT` where useful, and sanitizes messages so
//! internal details (node IDs, storage internals, wire offsets) never leak to
//! clients.

use std::fmt::Display;
use std::sync::Arc;

use pgwire::error::{ErrorInfo, PgWireError};
use vairedb_common::error::{VaireDbError, sanitize_message, sqlstate_for_code};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::MetadataCatalog;
use crate::error::CoordinatorError;
use crate::util::node_state_str;

/// Optional context threaded into error enrichment so `DETAIL`/`HINT` can name
/// the offending table and report the relevant replication factor.
#[derive(Default, Clone)]
pub struct ErrorContext {
    /// Relation the failing operation targeted, surfaced in the error's `table` field.
    pub table_name: Option<String>,
    /// Replication factor of the target table, used to enrich quorum hints.
    pub replication_factor: Option<u32>,
}

impl ErrorContext {
    /// Build a context naming the table the operation was acting on.
    pub fn for_table(name: &str) -> Self {
        Self {
            table_name: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// Attach the target table's replication factor for richer quorum hints.
    pub fn with_replication(mut self, factor: u32) -> Self {
        self.replication_factor = Some(factor);
        self
    }
}

/// Construct a pgwire `UserError` from a `VdbErrorCode` and message, formatting
/// the message with the `[VDB-NNNN]` code prefix and the matching SQLSTATE.
pub fn make_vdb_error(code: VdbErrorCode, message: impl Into<String>) -> PgWireError {
    let err = VaireDbError::new(code, message);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate_for_code(code).to_string(),
        err.formatted_message(),
    )))
}

/// Enrich a typed `CoordinatorError` into a full pgwire error, classifying it
/// and attaching table name, a catalog-derived `DETAIL`, and a `HINT`. The
/// catalog is queried best-effort to build detail (e.g. listing alive nodes).
pub fn enrich_coordinator_error(
    err: &CoordinatorError,
    ctx: &ErrorContext,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireError {
    let (code, message) = classify_error(err);
    let sqlstate = sqlstate_for_code(code).to_string();

    let vdb_error = VaireDbError::new(code, &message);
    let formatted = vdb_error.formatted_message();

    let mut info = ErrorInfo::new("ERROR".to_string(), sqlstate, formatted);
    info.table = ctx.table_name.clone();
    info.detail = try_build_detail(err, ctx, catalog);
    info.hint = build_hint(err, ctx);

    PgWireError::UserError(Box::new(info))
}

/// Enrich an untyped (string-based) error, typically from the engine, by
/// inferring a `VdbErrorCode` from its message substrings and sanitizing it.
pub fn enrich_generic_error(e: &dyn Display, ctx: &ErrorContext) -> PgWireError {
    let msg = e.to_string();
    let code = classify_generic_error_code(&msg);
    let sqlstate = sqlstate_for_code(code).to_string();
    let sanitized = sanitize_message(&msg);

    let vdb_error = VaireDbError::new(code, &sanitized);
    let formatted = vdb_error.formatted_message();

    let mut info = ErrorInfo::new("ERROR".to_string(), sqlstate, formatted);
    info.table = ctx.table_name.clone();
    PgWireError::UserError(Box::new(info))
}

/// Infer a `VdbErrorCode` from an untyped error message via case-insensitive
/// substring matching against known engine/driver phrasings, falling back to
/// `InternalError`. The first matching rule wins, so order is significant.
pub(crate) fn classify_generic_error_code(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if (lower.contains("table") && lower.contains("not found")) || lower.contains("no table named")
    {
        VdbErrorCode::TableNotFound
    } else if lower.contains("shard") && lower.contains("not found") {
        VdbErrorCode::ShardNotFound
    } else if lower.contains("no field named")
        || (lower.contains("column") && lower.contains("not found"))
        || lower.contains("ambiguous")
    {
        VdbErrorCode::ColumnNotFound
    } else if lower.contains("type mismatch") || lower.contains("cannot cast") {
        VdbErrorCode::TypeMismatch
    } else if lower.contains("syntax error") || lower.contains("unexpected token") {
        VdbErrorCode::SqlSyntaxError
    } else if lower.contains("not yet implemented")
        || lower.contains("unsupported")
        || lower.contains("no function matches")
        || lower.contains("invalid function")
    {
        VdbErrorCode::FeatureNotSupported
    } else if lower.contains("resources exhausted") || lower.contains("memory limit") {
        VdbErrorCode::WriteQueueFull
    } else if lower.contains("divide by zero")
        || lower.contains("division by zero")
        || lower.contains("overflow")
    {
        VdbErrorCode::EngineError
    } else if lower.contains("unique constraint")
        || lower.contains("duplicate key")
        || lower.contains("primary key constraint")
    {
        VdbErrorCode::WriteConflict
    } else if lower.contains("not null constraint")
        || lower.contains("null value")
        || lower.contains("check constraint")
    {
        VdbErrorCode::EngineError
    } else if lower.contains("connection")
        && (lower.contains("refused") || lower.contains("unreachable"))
    {
        VdbErrorCode::NodeUnavailable
    } else {
        VdbErrorCode::InternalError
    }
}

/// Map a `CoordinatorError` to its `VdbErrorCode` and a sanitized, client-safe
/// message. Logs the underlying detail for operator diagnosis while returning a
/// generic message so internal specifics (node IDs, storage internals) never leak.
pub(crate) fn classify_error(err: &CoordinatorError) -> (VdbErrorCode, String) {
    let code = err.vdb_error_code();
    let message = match err {
        CoordinatorError::TableNotFound(name) => {
            format!("table '{}' does not exist", name)
        }
        CoordinatorError::NodeNotFound(id) => {
            format!("node '{}' not found in cluster", id)
        }
        CoordinatorError::ShardNotAssigned(msg) => msg.clone(),
        CoordinatorError::NullShardKey(msg) => msg.clone(),
        CoordinatorError::QuorumNotReached { needed, got } => {
            format!(
                "write quorum not reached: {}/{} nodes acknowledged",
                got, needed
            )
        }
        CoordinatorError::ShardUnavailable(pid) => {
            format!("shard '{}' unavailable: primary node unreachable", pid)
        }
        CoordinatorError::Grpc(status) => {
            format!(
                "node execution failed: {}",
                sanitize_message(status.message())
            )
        }
        CoordinatorError::GrpcTransport(e) => {
            tracing::error!(error = %e, "gRPC transport failure");
            "failed to communicate with storage node".to_string()
        }
        CoordinatorError::SqlParse(e) => format!("SQL syntax error: {}", e),
        CoordinatorError::NodeExecFailed(node_err) => {
            tracing::error!(
                node_id = %node_err.node_id,
                shard_id = %node_err.shard_id,
                error = %node_err.message,
                "node execution failed"
            );
            format!(
                "node execution failed: {}",
                sanitize_message(&node_err.message)
            )
        }
        CoordinatorError::CatalogTransaction(e) => {
            tracing::error!(error = %e, "catalog transaction failed");
            "catalog transaction failed".to_string()
        }
        CoordinatorError::CatalogStorage(e) => {
            tracing::error!(error = %e, "catalog storage error");
            "catalog storage error".to_string()
        }
        CoordinatorError::CatalogCommit(e) => {
            tracing::error!(error = %e, "catalog commit failed");
            "catalog commit failed".to_string()
        }
        CoordinatorError::Catalog(e) => {
            tracing::error!(error = %e, "catalog error");
            "catalog error".to_string()
        }
        CoordinatorError::CatalogTable(e) => {
            tracing::error!(error = %e, "catalog table access failed");
            "catalog table access failed".to_string()
        }
        CoordinatorError::NoAliveNodes => {
            "no alive nodes available for shard assignment".to_string()
        }
        CoordinatorError::Anonymization(msg) => msg.clone(),
        CoordinatorError::Serialization(s) => {
            tracing::error!(detail = %s, "internal serialization error");
            "internal serialization error".to_string()
        }
        CoordinatorError::Internal(s) => {
            tracing::error!(detail = %s, "internal error");
            "internal error".to_string()
        }
    };
    (code, sanitize_message(&message))
}

/// Build the optional `DETAIL` line for an error, querying the catalog for
/// supporting facts (alive node counts, the unavailable shard's primary node,
/// etc.). Returns `None` when no useful detail applies or a catalog lookup fails.
pub(crate) fn try_build_detail(
    err: &CoordinatorError,
    ctx: &ErrorContext,
    catalog: &Arc<MetadataCatalog>,
) -> Option<String> {
    match err {
        CoordinatorError::QuorumNotReached { needed, .. } => {
            let nodes = catalog.list_alive_nodes().ok()?;
            let alive_count = nodes.len();
            Some(format!(
                "Alive nodes in cluster: {}. Required quorum: {}.",
                alive_count, needed
            ))
        }
        CoordinatorError::ShardUnavailable(pid) => {
            let table_name = ctx.table_name.as_ref()?;
            let shards = catalog.get_shards_for_table(table_name).ok()?;
            let p = shards.iter().find(|p| p.shard_id == *pid)?;
            let node = catalog.get_node(&p.primary_node_id).ok()??;
            tracing::debug!(
                shard_id = %pid,
                node_id = %node.node_id,
                address = %node.advertised_address,
                state = node_state_str(node.state),
                "shard unavailable detail"
            );
            Some(format!(
                "Primary node '{}' (state: {}).",
                node.node_id,
                node_state_str(node.state),
            ))
        }
        CoordinatorError::NodeNotFound(_) => {
            let nodes = catalog.list_alive_nodes().ok()?;
            if nodes.is_empty() {
                return Some("No alive nodes in cluster.".to_string());
            }
            let ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
            Some(format!("Known alive nodes: {}.", ids.join(", ")))
        }
        CoordinatorError::NodeExecFailed(node_err) => Some(format!(
            "Node '{}' failed on shard '{}'.",
            node_err.node_id, node_err.shard_id
        )),
        CoordinatorError::Grpc(status) => {
            let sanitized = sanitize_message(status.message());
            if sanitized.is_empty() {
                None
            } else {
                Some(sanitized)
            }
        }
        CoordinatorError::GrpcTransport(_) => None,
        _ => None,
    }
}

/// Build the optional `HINT` line suggesting a remediation or diagnostic query
/// for the error, returning `None` when no actionable hint applies.
pub(crate) fn build_hint(err: &CoordinatorError, ctx: &ErrorContext) -> Option<String> {
    match err {
        CoordinatorError::TableNotFound(_) => {
            Some("Run SELECT * FROM vairedb_catalog.tables to see available tables.".to_string())
        }
        CoordinatorError::NodeNotFound(_) => {
            Some("Check vairedb_catalog.nodes for registered nodes.".to_string())
        }
        CoordinatorError::ShardNotAssigned(_) => Some(
            "Verify the table was created successfully and shards were assigned.".to_string(),
        ),
        CoordinatorError::QuorumNotReached { .. } => {
            let rf_info = ctx
                .replication_factor
                .map(|rf| format!("Replication factor is {}. ", rf))
                .unwrap_or_default();
            Some(format!(
                "{}Check node health: SELECT * FROM vairedb_catalog.nodes WHERE state != 'ALIVE'.",
                rf_info
            ))
        }
        CoordinatorError::ShardUnavailable(_) => Some(
            "The primary node may be down. Writes will resume when the shard is reassigned."
                .to_string(),
        ),
        CoordinatorError::GrpcTransport(_) => {
            Some("Check that core nodes are running and reachable.".to_string())
        }
        CoordinatorError::NodeExecFailed(node_err) => {
            match VdbErrorCode::try_from(node_err.error_code) {
                Ok(VdbErrorCode::WriteConflict) => Some("Retry the transaction.".to_string()),
                Ok(VdbErrorCode::NodeShuttingDown) => {
                    Some("The node is shutting down. Retry after cluster rebalancing.".to_string())
                }
                _ => None,
            }
        }
        CoordinatorError::NoAliveNodes => Some(
            "Register at least one storage node before creating tables. Check vairedb_catalog.nodes."
                .to_string(),
        ),
        CoordinatorError::Catalog(_)
        | CoordinatorError::CatalogTransaction(_)
        | CoordinatorError::CatalogTable(_)
        | CoordinatorError::CatalogStorage(_)
        | CoordinatorError::CatalogCommit(_) => Some(
            "This is an internal metadata storage issue. Check disk space and coordinator logs."
                .to_string(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_db_path() -> String {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "/tmp/vairedb_test_error_enrichment_unit_{}_{}.redb",
            std::process::id(),
            id
        )
    }

    fn make_catalog() -> Arc<MetadataCatalog> {
        Arc::new(MetadataCatalog::open(&temp_db_path()).unwrap())
    }

    // --- classify_error tests ---

    #[test]
    fn test_classify_table_not_found() {
        let err = CoordinatorError::TableNotFound("orders".to_string());
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::TableNotFound);
        assert!(msg.contains("orders"));
    }

    #[test]
    fn test_classify_node_not_found() {
        let err = CoordinatorError::NodeNotFound("node-1".to_string());
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::NodeNotFound);
    }

    #[test]
    fn test_classify_shard_not_assigned() {
        let err = CoordinatorError::ShardNotAssigned("shard0".to_string());
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::ShardNotAssigned);
    }

    #[test]
    fn test_classify_quorum_not_reached() {
        let err = CoordinatorError::QuorumNotReached { needed: 2, got: 1 };
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::QuorumNotReached);
        assert!(msg.contains("1/2"));
    }

    #[test]
    fn test_classify_shard_unavailable() {
        let err = CoordinatorError::ShardUnavailable("shard3".to_string());
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::ShardUnavailable);
    }

    #[test]
    fn test_classify_grpc_not_found() {
        let status = tonic::Status::not_found("gone");
        let err = CoordinatorError::Grpc(Box::new(status));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::ShardNotFound);
    }

    #[test]
    fn test_classify_grpc_unavailable() {
        let status = tonic::Status::unavailable("down");
        let err = CoordinatorError::Grpc(Box::new(status));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::NodeUnavailable);
    }

    #[test]
    fn test_classify_grpc_transport() {
        let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:0");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let transport_err = rt.block_on(async { endpoint.connect().await.unwrap_err() });
        let err = CoordinatorError::GrpcTransport(Box::new(transport_err));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::NodeCommunicationError);
    }

    #[test]
    fn test_classify_node_exec_failed_write_conflict() {
        use crate::error::NodeError;
        let node_err = NodeError {
            message: "write conflict".to_string(),
            error_code: VdbErrorCode::WriteConflict as i32,
            shard_id: "shard0".to_string(),
            node_id: "node-1".to_string(),
        };
        let err = CoordinatorError::NodeExecFailed(Box::new(node_err));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::WriteConflict);
    }

    #[test]
    fn test_classify_node_exec_failed_shard_not_found() {
        use crate::error::NodeError;
        let node_err = NodeError {
            message: "shard missing".to_string(),
            error_code: VdbErrorCode::ShardNotFound as i32,
            shard_id: "shard0".to_string(),
            node_id: "node-1".to_string(),
        };
        let err = CoordinatorError::NodeExecFailed(Box::new(node_err));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::ShardNotFound);
    }

    #[test]
    fn test_classify_node_exec_failed_shutting_down() {
        use crate::error::NodeError;
        let node_err = NodeError {
            message: "shutting down".to_string(),
            error_code: VdbErrorCode::NodeShuttingDown as i32,
            shard_id: "shard0".to_string(),
            node_id: "node-1".to_string(),
        };
        let err = CoordinatorError::NodeExecFailed(Box::new(node_err));
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::NodeShuttingDown);
    }

    #[test]
    fn test_classify_internal() {
        let err = CoordinatorError::Internal("oops".to_string());
        let (code, _) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::InternalError);
    }

    #[test]
    fn test_classify_catalog_storage() {
        let err = CoordinatorError::CatalogStorage(redb::StorageError::Corrupted(
            "disk full".to_string(),
        ));
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::CatalogStorageError);
        assert!(msg.contains("catalog storage error"));
        assert!(!msg.contains("disk full"));
    }

    #[test]
    fn test_classify_catalog_does_not_leak_redb_details() {
        let err = CoordinatorError::CatalogStorage(redb::StorageError::Corrupted(
            "metadata invalid".to_string(),
        ));
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::CatalogStorageError);
        assert!(!msg.contains("metadata invalid"));
        assert!(msg.contains("catalog storage error"));
    }

    #[test]
    fn test_classify_no_alive_nodes() {
        let err = CoordinatorError::NoAliveNodes;
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::NoAliveNodes);
        assert!(msg.contains("no alive nodes"));
    }

    #[test]
    fn test_classify_grpc_transport_does_not_leak_details() {
        let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:0");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let transport_err = rt.block_on(async { endpoint.connect().await.unwrap_err() });
        let err = CoordinatorError::GrpcTransport(Box::new(transport_err));
        let (_, msg) = classify_error(&err);
        assert_eq!(msg, "failed to communicate with storage node");
        assert!(!msg.contains("::1"));
    }

    #[test]
    fn test_classify_node_exec_failed_does_not_leak_node_id() {
        use crate::error::NodeError;
        let node_err = NodeError {
            message: "table 'orders_shard0' not found".to_string(),
            error_code: VdbErrorCode::ShardNotFound as i32,
            shard_id: "orders_shard0".to_string(),
            node_id: "core-node-secret-1".to_string(),
        };
        let err = CoordinatorError::NodeExecFailed(Box::new(node_err));
        let (_, msg) = classify_error(&err);
        assert!(!msg.contains("core-node-secret-1"));
        assert!(msg.contains("node execution failed"));
    }

    #[test]
    fn test_classify_serialization_does_not_leak_prost_details() {
        let err = CoordinatorError::Serialization(
            "failed to decode: invalid wire type 6 at offset 42".to_string(),
        );
        let (code, msg) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::SerializationError);
        assert!(!msg.contains("wire type"));
        assert!(!msg.contains("offset 42"));
        assert!(msg.contains("internal serialization error"));
    }

    // --- try_build_detail tests ---

    #[test]
    fn test_try_build_detail_empty_catalog() {
        let catalog = make_catalog();
        let err = CoordinatorError::QuorumNotReached { needed: 2, got: 1 };
        let ctx = ErrorContext::default();
        let detail = try_build_detail(&err, &ctx, &catalog);
        assert!(detail.is_some());
        assert!(detail.unwrap().contains("Alive nodes in cluster: 0"));
    }

    #[test]
    fn test_try_build_detail_node_not_found_empty() {
        let catalog = make_catalog();
        let err = CoordinatorError::NodeNotFound("node-x".to_string());
        let ctx = ErrorContext::default();
        let detail = try_build_detail(&err, &ctx, &catalog);
        assert!(detail.is_some());
        assert!(detail.unwrap().contains("No alive nodes"));
    }

    #[test]
    fn test_try_build_detail_table_not_found_returns_none() {
        let catalog = make_catalog();
        let err = CoordinatorError::TableNotFound("t".to_string());
        let ctx = ErrorContext::default();
        let detail = try_build_detail(&err, &ctx, &catalog);
        assert!(detail.is_none());
    }

    #[test]
    fn test_try_build_detail_grpc_transport_returns_none() {
        let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:0");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let transport_err = rt.block_on(async { endpoint.connect().await.unwrap_err() });
        let err = CoordinatorError::GrpcTransport(Box::new(transport_err));
        let catalog = make_catalog();
        let ctx = ErrorContext::default();
        let detail = try_build_detail(&err, &ctx, &catalog);
        assert!(detail.is_none());
    }

    // --- build_hint tests ---

    #[test]
    fn test_build_hint_table_not_found() {
        let err = CoordinatorError::TableNotFound("t".to_string());
        let ctx = ErrorContext::default();
        let hint = build_hint(&err, &ctx);
        assert!(hint.is_some());
        assert!(hint.unwrap().contains("vairedb_catalog.tables"));
    }

    #[test]
    fn test_build_hint_quorum_with_rf() {
        let err = CoordinatorError::QuorumNotReached { needed: 2, got: 1 };
        let ctx = ErrorContext::default().with_replication(3);
        let hint = build_hint(&err, &ctx);
        assert!(hint.is_some());
        assert!(hint.unwrap().contains("Replication factor is 3"));
    }

    #[test]
    fn test_build_hint_catalog_errors() {
        let err =
            CoordinatorError::CatalogStorage(redb::StorageError::Corrupted("bad".to_string()));
        let ctx = ErrorContext::default();
        let hint = build_hint(&err, &ctx);
        assert!(hint.is_some());
        assert!(hint.unwrap().contains("metadata storage issue"));
    }

    #[test]
    fn test_build_hint_no_alive_nodes() {
        let err = CoordinatorError::NoAliveNodes;
        let ctx = ErrorContext::default();
        let hint = build_hint(&err, &ctx);
        assert!(hint.is_some());
        assert!(hint.unwrap().contains("Register at least one storage node"));
    }

    // --- classify_generic_error_code tests ---

    /// Each case maps a representative engine/driver error string to the
    /// `VdbErrorCode` its substring rules should produce. One row per branch of
    /// `classify_generic_error_code`; add a row when a branch is added.
    #[test]
    fn test_classify_generic_error_code_mapping() {
        let cases: &[(&str, VdbErrorCode)] = &[
            ("table 'orders' not found", VdbErrorCode::TableNotFound),
            (
                "No table named 'users' in schema",
                VdbErrorCode::TableNotFound,
            ),
            (
                "No field named 'age' in schema",
                VdbErrorCode::ColumnNotFound,
            ),
            (
                "column reference 'id' is ambiguous",
                VdbErrorCode::ColumnNotFound,
            ),
            (
                "type mismatch: expected Int32, got Utf8",
                VdbErrorCode::TypeMismatch,
            ),
            ("Cannot cast string to integer", VdbErrorCode::TypeMismatch),
            (
                "This feature is not yet implemented",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "Unsupported SQL type: GEOMETRY",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "no function matches the given name and argument types",
                VdbErrorCode::FeatureNotSupported,
            ),
            ("invalid function 'foo'", VdbErrorCode::FeatureNotSupported),
            (
                "syntax error at or near 'FROM'",
                VdbErrorCode::SqlSyntaxError,
            ),
            (
                "unexpected token in expression",
                VdbErrorCode::SqlSyntaxError,
            ),
            ("divide by zero", VdbErrorCode::EngineError),
            ("integer overflow in computation", VdbErrorCode::EngineError),
            (
                "NOT NULL constraint failed: column 'name' cannot be null",
                VdbErrorCode::EngineError,
            ),
            (
                "CHECK constraint failed: age > 0",
                VdbErrorCode::EngineError,
            ),
            (
                "resources exhausted: memory limit reached",
                VdbErrorCode::WriteQueueFull,
            ),
            (
                "shard 'orders_shard0' not found",
                VdbErrorCode::ShardNotFound,
            ),
            ("connection refused to host", VdbErrorCode::NodeUnavailable),
            (
                "connection unreachable for node",
                VdbErrorCode::NodeUnavailable,
            ),
            (
                "unique constraint violated: duplicate key value",
                VdbErrorCode::WriteConflict,
            ),
            (
                "duplicate key value violates unique constraint",
                VdbErrorCode::WriteConflict,
            ),
            (
                "PRIMARY KEY constraint failed for table",
                VdbErrorCode::WriteConflict,
            ),
            ("something went wrong", VdbErrorCode::InternalError),
        ];

        for (msg, expected) in cases {
            assert_eq!(
                classify_generic_error_code(msg),
                *expected,
                "classifying {msg:?}"
            );
        }
    }

    // --- ErrorContext builder tests ---

    #[test]
    fn test_error_context_builder() {
        let ctx = ErrorContext::for_table("orders").with_replication(3);
        assert_eq!(ctx.table_name.as_deref(), Some("orders"));
        assert_eq!(ctx.replication_factor, Some(3));
    }

    // --- enrich tests ---

    #[test]
    fn test_enrich_coordinator_error_produces_user_error() {
        let catalog = make_catalog();
        let err = CoordinatorError::TableNotFound("orders".to_string());
        let ctx = ErrorContext::for_table("orders");
        let pgwire_err = enrich_coordinator_error(&err, &ctx, &catalog);
        match pgwire_err {
            pgwire::error::PgWireError::UserError(_) => {}
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    #[test]
    fn test_enrich_coordinator_error_contains_vdb_code() {
        let catalog = make_catalog();
        let err = CoordinatorError::TableNotFound("orders".to_string());
        let ctx = ErrorContext::for_table("orders");
        let pgwire_err = enrich_coordinator_error(&err, &ctx, &catalog);
        match pgwire_err {
            pgwire::error::PgWireError::UserError(info) => {
                assert!(info.message.contains("[VDB-1000]"));
            }
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    #[test]
    fn test_enrich_generic_error_produces_user_error() {
        let ctx = ErrorContext::for_table("orders");
        let pgwire_err = enrich_generic_error(&"something failed", &ctx);
        match pgwire_err {
            pgwire::error::PgWireError::UserError(_) => {}
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    #[test]
    fn test_enrich_generic_error_contains_vdb_code() {
        let ctx = ErrorContext::for_table("orders");
        let pgwire_err = enrich_generic_error(&"something failed", &ctx);
        match pgwire_err {
            pgwire::error::PgWireError::UserError(info) => {
                assert!(info.message.contains("[VDB-5001]"));
            }
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    // --- make_vdb_error tests ---

    #[test]
    fn test_make_vdb_error_formats_code_in_message() {
        let err = make_vdb_error(VdbErrorCode::TableNotFound, "table 'orders' does not exist");
        match err {
            pgwire::error::PgWireError::UserError(info) => {
                assert!(info.message.contains("[VDB-1000]"));
                assert!(info.message.contains("orders"));
                assert_eq!(info.code, "42P01");
            }
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    #[test]
    fn test_make_vdb_error_table_already_exists() {
        let err = make_vdb_error(
            VdbErrorCode::TableAlreadyExists,
            "table 'orders' already exists",
        );
        match err {
            pgwire::error::PgWireError::UserError(info) => {
                assert!(info.message.contains("[VDB-1005]"));
                assert_eq!(info.code, "42P07");
            }
            other => panic!("expected UserError, got: {:?}", other),
        }
    }

    #[test]
    fn test_make_vdb_error_column_already_exists() {
        let err = make_vdb_error(
            VdbErrorCode::ColumnAlreadyExists,
            "column \"age\" already exists",
        );
        match err {
            pgwire::error::PgWireError::UserError(info) => {
                assert!(info.message.contains("[VDB-1006]"));
                assert_eq!(info.code, "42701");
            }
            other => panic!("expected UserError, got: {:?}", other),
        }
    }
}
