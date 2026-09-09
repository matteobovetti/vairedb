//! Turns internal coordinator/engine errors into rich PostgreSQL-style error
//! responses. Classifies an error into a `VdbErrorCode` and SQLSTATE, attaches
//! a client-facing `DETAIL` and `HINT` where useful, and sanitizes messages so
//! internal details (node IDs, storage internals, wire offsets) never leak to
//! clients.

use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
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

/// Enrich a `DataFusionError` whose type is still in hand, classifying it from its
/// **variant** rather than from the text its `Display` happens to produce.
///
/// Prefer this to [`enrich_generic_error`] everywhere the concrete error survives.
/// The substring classifier cannot be made to work here, and not merely because
/// message text drifts: `DataFusionError::Context` and `Diagnostic` have an empty
/// `error_prefix`, so the variant that matters is the *inner* one, and a scan of the
/// flattened string sees whichever phrase happens to appear first. Matching the
/// variant also means a DataFusion upgrade that rewords an error cannot silently
/// reclassify it — the compiler reports a new variant instead.
pub fn enrich_datafusion_error(e: &DataFusionError, ctx: &ErrorContext) -> PgWireError {
    let code = reclassify_transported_data_error(e, classify_datafusion_error_code(e));
    let sqlstate = sqlstate_for_code(code).to_string();
    let sanitized = sanitize_message(&e.to_string());

    let vdb_error = VaireDbError::new(code, &sanitized);
    let mut info = ErrorInfo::new("ERROR".to_string(), sqlstate, vdb_error.formatted_message());
    info.table = ctx.table_name.clone();
    PgWireError::UserError(Box::new(info))
}

/// Classify a [`DataFusionError`] by variant, refining within a variant only where
/// PostgreSQL draws a distinction DataFusion carries in the message.
///
/// The wrapper variants recurse, which is the point: `Context`, `Diagnostic` and
/// `Shared` exist to annotate an error without replacing it, so the classification
/// belongs to what they wrap. `Collection` takes its first error, matching
/// DataFusion's own `error_prefix`.
///
/// The catch-all arm is deliberate rather than lazy. `ParquetError`, `ObjectStore`
/// and `Ffi` are behind cargo features, so naming them here would make this module's
/// compilation depend on DataFusion's feature resolution; all three are engine
/// failures and `EngineError` is what they would map to anyway.
pub(crate) fn classify_datafusion_error_code(e: &DataFusionError) -> VdbErrorCode {
    match e {
        // Annotation wrappers — classify what is inside.
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            classify_datafusion_error_code(inner)
        }
        DataFusionError::Shared(inner) => classify_datafusion_error_code(inner),
        DataFusionError::Collection(errs) => {
            errs.first().map_or(VdbErrorCode::InternalError, |first| {
                classify_datafusion_error_code(first)
            })
        }

        // An honest refusal, and the class a client can act on.
        DataFusionError::NotImplemented(_) | DataFusionError::Substrait(_) => {
            VdbErrorCode::FeatureNotSupported
        }

        // A parse failure is the client's syntax, whoever noticed it.
        DataFusionError::SQL(_, _) => VdbErrorCode::SqlSyntaxError,

        // The name resolution errors, already typed by DataFusion — all four are a
        // column the query named and the schema does not have.
        DataFusionError::SchemaError(_, _) => VdbErrorCode::ColumnNotFound,

        // Planning covers several PostgreSQL classes; the message is the only thing
        // that separates them, but scoped to this variant rather than scanned globally.
        DataFusionError::Plan(msg) => classify_plan_message(msg),

        // Execution reached a row it could not process. Divide-by-zero and overflow are
        // data errors PostgreSQL names; the rest is genuinely the engine.
        DataFusionError::Execution(msg) => classify_execution_message(msg),
        DataFusionError::ArrowError(arrow, _) => classify_arrow_error(arrow),

        DataFusionError::ResourcesExhausted(_) => VdbErrorCode::WriteQueueFull,

        // Text that already crossed a process boundary: the type is gone, so this is
        // the one place the substring classifier is the best available answer.
        DataFusionError::External(inner) => match inner.downcast_ref::<DataFusionError>() {
            Some(df) => classify_datafusion_error_code(df),
            None => classify_generic_error_code(&inner.to_string()),
        },

        DataFusionError::IoError(_) | DataFusionError::ExecutionJoin(_) => {
            VdbErrorCode::EngineError
        }
        DataFusionError::Internal(_) | DataFusionError::Configuration(_) => {
            VdbErrorCode::InternalError
        }

        _ => VdbErrorCode::EngineError,
    }
}

/// Split a `DataFusionError::Plan` message into the PostgreSQL classes it covers.
///
/// Everything here is a defect in the statement rather than in the server, so the
/// fallback is `42601` and not `XX000`: a planning error means the query was read and
/// rejected, which is exactly what the syntax-error class tells a client.
fn classify_plan_message(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if lower.contains("aggregate function calls cannot be nested")
        || lower.contains("aggregate functions cannot be nested")
    {
        VdbErrorCode::GroupingError
    } else if lower.contains("window function") || lower.contains("over clause") {
        VdbErrorCode::WindowingError
    } else if lower.contains("no function matches")
        || lower.contains("invalid function")
        || lower.contains("not supported")
    {
        VdbErrorCode::FeatureNotSupported
    } else if lower.contains("no field named") || lower.contains("ambiguous") {
        VdbErrorCode::ColumnNotFound
    } else if (lower.contains("table") && lower.contains("not found"))
        || lower.contains("no table named")
    {
        VdbErrorCode::TableNotFound
    } else if lower.contains("cannot cast") || lower.contains("type mismatch") {
        VdbErrorCode::TypeMismatch
    } else {
        VdbErrorCode::SqlSyntaxError
    }
}

/// Split a `DataFusionError::Execution` message into the data errors PostgreSQL names.
///
/// `22012` and `22003` matter more than they look: both used to arrive as `XX000`,
/// which tells a client the server broke and invites a retry that cannot succeed. The
/// statement and the server were both fine and one row's data was not.
fn classify_execution_message(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if is_divide_by_zero(&lower) {
        VdbErrorCode::DivisionByZero
    } else if lower.contains("overflow") {
        VdbErrorCode::NumericValueOutOfRange
    } else if lower.contains("cannot cast string")
        || lower.contains("invalid input syntax")
        || lower.contains("error parsing")
    {
        VdbErrorCode::InvalidTextRepresentation
    } else {
        VdbErrorCode::EngineError
    }
}

/// Rescue a data error that lost its type crossing the Ballista scheduler boundary.
///
/// The variant-based classifier is the right default: it cannot rot when DataFusion
/// rewords a message. But it only works while there is a variant to read, and an error
/// raised inside an executor does not keep one. The scheduler formats the whole failure
/// into a string, and Ballista returns it under whichever wrapper the call site
/// happened to use — `Execution` from `DistributedQueryExec`, but wrapped again by
/// `collect()` on the way out. Depending on which wrapper is on top is depending on an
/// implementation detail of a dependency; the *rendered message* is the one part of a
/// transported error that is stable.
///
/// So this runs only when classification already gave up — `EngineError` or
/// `InternalError`, the two "we do not know" answers — and only for divide-by-zero,
/// where the cost of the wrong answer is concrete: `XX000` tells a driver the server
/// broke and the statement is worth retrying, and `1 / 0` will fail identically every
/// time. Widening this to more codes would erode the reason the classifier is
/// variant-based, so it stays a named exception rather than a general fallback.
fn reclassify_transported_data_error(e: &DataFusionError, code: VdbErrorCode) -> VdbErrorCode {
    if !matches!(
        code,
        VdbErrorCode::EngineError | VdbErrorCode::InternalError
    ) {
        return code;
    }
    if is_divide_by_zero(&e.to_string().to_lowercase()) {
        return VdbErrorCode::DivisionByZero;
    }
    code
}

/// Recognize divide-by-zero in an already-lowercased message, in prose or in Rust's
/// `Debug` spelling.
///
/// The `Debug` spelling is not a nicety. An error raised inside a Ballista executor is
/// serialized by the scheduler before the coordinator ever sees it, and what comes back
/// is the `Debug` rendering nested a few layers deep:
///
/// ```text
/// Job abc failed: … DataFusionError(Execution("ArrowError(DivideByZero)"))
/// ```
///
/// `ArrowError::DivideByZero`'s typed arm in [`classify_arrow_error`] cannot fire on
/// that, because there is no longer an `ArrowError` to match — only text. Since scans
/// and projections run distributed, this is the spelling most arithmetic errors on a
/// real table actually arrive in, so missing it meant `1 / 0` reported `XX000` and
/// invited a retry that could not succeed. Matching without the spaces covers both.
fn is_divide_by_zero(lower: &str) -> bool {
    lower.contains("divide by zero")
        || lower.contains("division by zero")
        || lower.contains("dividebyzero")
}

/// Classify an [`ArrowError`] reached through `DataFusionError::ArrowError`.
///
/// Arrow types the two cases PostgreSQL cares about most, so unlike the string
/// variants above these need no message inspection at all: a failed cast or parse of
/// a text value is `22P02`, and its dedicated divide-by-zero variant is `22012`.
fn classify_arrow_error(e: &ArrowError) -> VdbErrorCode {
    match e {
        ArrowError::DivideByZero => VdbErrorCode::DivisionByZero,
        ArrowError::ArithmeticOverflow(_) => VdbErrorCode::NumericValueOutOfRange,
        ArrowError::CastError(_) | ArrowError::ParseError(_) => {
            VdbErrorCode::InvalidTextRepresentation
        }
        ArrowError::NotYetImplemented(_) => VdbErrorCode::FeatureNotSupported,
        ArrowError::SchemaError(_) => VdbErrorCode::ColumnNotFound,
        _ => VdbErrorCode::EngineError,
    }
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
    // `"this feature is not implemented"` and `"not supported"` are the phrases
    // DataFusion has emitted since 53 (`DataFusionError::error_prefix`); the two that
    // preceded them are kept because DuckDB and Ballista still use them.
    } else if lower.contains("this feature is not implemented")
        || lower.contains("not yet implemented")
        || lower.contains("not supported")
        || lower.contains("unsupported")
        || lower.contains("no function matches")
        || lower.contains("invalid function")
    {
        VdbErrorCode::FeatureNotSupported
    } else if lower.contains("resources exhausted") || lower.contains("memory limit") {
        VdbErrorCode::WriteQueueFull
    } else if is_divide_by_zero(&lower) {
        VdbErrorCode::DivisionByZero
    } else if lower.contains("overflow") {
        VdbErrorCode::NumericValueOutOfRange
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
        CoordinatorError::UnroutableShardKey(msg) => msg.clone(),
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
        // Already written for the client, and already naming what to write instead.
        CoordinatorError::Unsupported(msg) => msg.clone(),
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
            // Both used to be `EngineError`, i.e. `XX000`. They are data errors the
            // client caused, and reporting them as internal tells a driver to retry
            // something that cannot succeed.
            ("divide by zero", VdbErrorCode::DivisionByZero),
            (
                "integer overflow in computation",
                VdbErrorCode::NumericValueOutOfRange,
            ),
            // The phrasings DataFusion has used since 53, which the two rules above
            // this line did not match — so these reached clients as `XX000` too.
            (
                "This feature is not implemented: window EXCLUDE",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "Sort expression is not supported",
                VdbErrorCode::FeatureNotSupported,
            ),
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

    // --- enrich_datafusion_error / classify_datafusion_error_code ---

    /// One case per variant this classifies deliberately, so an upstream reshuffle of
    /// `DataFusionError` shows up as a failure here rather than as a class silently
    /// falling back to `EngineError`.
    #[test]
    fn classifies_each_datafusion_variant_by_its_variant() {
        let cases: Vec<(DataFusionError, VdbErrorCode)> = vec![
            (
                DataFusionError::NotImplemented("window EXCLUDE".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Substrait("nope".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Internal("broke".into()),
                VdbErrorCode::InternalError,
            ),
            (
                DataFusionError::Configuration("bad setting".into()),
                VdbErrorCode::InternalError,
            ),
            (
                DataFusionError::ResourcesExhausted("no memory".into()),
                VdbErrorCode::WriteQueueFull,
            ),
            (
                DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None),
                VdbErrorCode::DivisionByZero,
            ),
            (
                DataFusionError::ArrowError(
                    Box::new(ArrowError::CastError("'x' -> i32".into())),
                    None,
                ),
                VdbErrorCode::InvalidTextRepresentation,
            ),
            (
                DataFusionError::ArrowError(
                    Box::new(ArrowError::ArithmeticOverflow("i64".into())),
                    None,
                ),
                VdbErrorCode::NumericValueOutOfRange,
            ),
            (
                DataFusionError::Execution("Divide by zero error".into()),
                VdbErrorCode::DivisionByZero,
            ),
            // The shape a divide-by-zero actually has after it crosses the Ballista
            // scheduler: the typed `ArrowError` is gone and only its `Debug` spelling
            // survives, with no spaces to match on. Copied from a live 5-node cluster.
            (
                DataFusionError::Execution(
                    "Job MUwZj9P failed: Job failed due to stage 1 failed: Task failed due to \
                     runtime execution error: DataFusionError(Execution(\"ArrowError(DivideByZero)\"))"
                        .into(),
                ),
                VdbErrorCode::DivisionByZero,
            ),
            (
                DataFusionError::Execution("Overflow happened".into()),
                VdbErrorCode::NumericValueOutOfRange,
            ),
            (
                DataFusionError::Execution("some engine trouble".into()),
                VdbErrorCode::EngineError,
            ),
            (
                DataFusionError::Plan("No function matches the given name".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Plan("Aggregate function calls cannot be nested".into()),
                VdbErrorCode::GroupingError,
            ),
            (
                DataFusionError::Plan("window function is not allowed in WHERE".into()),
                VdbErrorCode::WindowingError,
            ),
            (
                DataFusionError::Plan("No field named foo".into()),
                VdbErrorCode::ColumnNotFound,
            ),
            (
                DataFusionError::Plan("something else entirely".into()),
                VdbErrorCode::SqlSyntaxError,
            ),
        ];

        for (err, expected) in cases {
            assert_eq!(
                classify_datafusion_error_code(&err),
                expected,
                "misclassified {err:?}"
            );
        }
    }

    /// The reason a substring scan cannot do this job. `Context` has an empty
    /// `error_prefix`, so the flattened text of this error begins with the *annotation*
    /// — and the annotation here names planning while the error underneath is a
    /// division by zero. Only the inner variant is the truth.
    #[test]
    fn a_wrapped_error_is_classified_by_what_it_wraps() {
        let inner = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        let wrapped = DataFusionError::Context(
            "while evaluating projection during planning".into(),
            Box::new(inner),
        );
        assert_eq!(
            classify_datafusion_error_code(&wrapped),
            VdbErrorCode::DivisionByZero
        );
    }

    #[test]
    fn a_shared_and_a_collection_error_classify_by_their_contents() {
        let shared = DataFusionError::Shared(Arc::new(DataFusionError::NotImplemented("x".into())));
        assert_eq!(
            classify_datafusion_error_code(&shared),
            VdbErrorCode::FeatureNotSupported
        );

        let collection = DataFusionError::Collection(vec![
            DataFusionError::Execution("Divide by zero".into()),
            DataFusionError::Internal("noise".into()),
        ]);
        assert_eq!(
            classify_datafusion_error_code(&collection),
            VdbErrorCode::DivisionByZero
        );
    }

    /// A `DataFusionError` boxed inside `External` is what a Ballista stage failure
    /// looks like once it has crossed gRPC, so unwrapping it keeps the typed answer
    /// rather than falling back to the substring scan.
    #[test]
    fn an_external_error_unwraps_a_datafusion_error_inside_it() {
        let external = DataFusionError::External(Box::new(DataFusionError::NotImplemented(
            "something".into(),
        )));
        assert_eq!(
            classify_datafusion_error_code(&external),
            VdbErrorCode::FeatureNotSupported
        );
    }

    /// `SchemaError` is already typed by DataFusion, so no message reading is needed.
    #[test]
    fn a_schema_error_is_a_missing_column() {
        let err = DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::AmbiguousReference {
                field: Box::new(datafusion::common::Column::new_unqualified("id")),
            }),
            Box::new(None),
        );
        assert_eq!(
            classify_datafusion_error_code(&err),
            VdbErrorCode::ColumnNotFound
        );
    }

    /// The end-to-end shape a client sees: the right SQLSTATE, and no `Signature { … }`
    /// debug dump surviving into the message.
    #[test]
    fn enriching_reports_the_sqlstate_and_elides_a_signature_dump() {
        let err = DataFusionError::Plan(
            "No function matches 'lpad': Signature { type_signature: OneOf([Exact([Utf8])]), \
             volatility: Immutable }"
                .into(),
        );
        match enrich_datafusion_error(&err, &ErrorContext::for_table("t")) {
            pgwire::error::PgWireError::UserError(info) => {
                assert_eq!(info.code, "0A000");
                assert!(!info.message.contains("type_signature"), "{}", info.message);
                assert!(info.message.contains("Signature { … }"), "{}", info.message);
            }
            other => panic!("expected UserError, got: {other:?}"),
        }
    }

    /// A division by zero used to reach clients as `XX000`, which says the server broke.
    #[test]
    fn a_division_by_zero_reports_the_data_error_sqlstate() {
        let err = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        match enrich_datafusion_error(&err, &ErrorContext::default()) {
            pgwire::error::PgWireError::UserError(info) => assert_eq!(info.code, "22012"),
            other => panic!("expected UserError, got: {other:?}"),
        }
    }

    /// The same division by zero, but as it actually arrives from a five-node cluster:
    /// raised in an executor, formatted into a string by the scheduler, and handed back
    /// under a wrapper variant that carries no type information. Every one of these
    /// shapes was observed or is a plausible re-wrap of one that was, and all of them
    /// have to answer `22012` — which is the reason
    /// [`reclassify_transported_data_error`] reads the rendered message rather than
    /// trusting the wrapper.
    #[test]
    fn a_transported_division_by_zero_reports_the_data_error_sqlstate() {
        const BALLISTA: &str = "Job 3QdcFzH failed: Job failed due to stage 1 failed: Task \
                                failed due to runtime execution error: \
                                DataFusionError(Execution(\"ArrowError(DivideByZero)\"))";

        for err in [
            DataFusionError::Execution(BALLISTA.into()),
            DataFusionError::Internal(BALLISTA.into()),
            DataFusionError::Context(
                "collect".into(),
                Box::new(DataFusionError::Execution(BALLISTA.into())),
            ),
            DataFusionError::External(Box::new(std::io::Error::other(BALLISTA))),
        ] {
            match enrich_datafusion_error(&err, &ErrorContext::default()) {
                pgwire::error::PgWireError::UserError(info) => {
                    assert_eq!(info.code, "22012", "for {err:?}");
                }
                other => panic!("expected UserError, got: {other:?}"),
            }
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
