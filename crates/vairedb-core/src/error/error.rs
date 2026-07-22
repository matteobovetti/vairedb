use std::fmt::Display;

use thiserror::Error;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

/// The crate-wide error type for core-node operations.
///
/// Variants map onto the protobuf [`VdbErrorCode`] returned to clients via
/// `vdb_error_code`, so callers can classify a failure without parsing its
/// message.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A DuckDB or I/O failure with no more specific classification.
    #[error("engine error: {0}")]
    Engine(String),

    /// A query or write targeted a shard table that does not exist.
    #[error("shard not found: {0}")]
    ShardNotFound(String),

    /// A write violated a unique/primary-key constraint.
    #[error("write conflict: {0}")]
    WriteConflict(String),

    /// A value could not be cast to the column's type.
    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    /// The write queue was closed or its writer task dropped the response.
    #[error("write queue error: {0}")]
    WriteQueue(String),

    /// Coordinator registration or the heartbeat stream failed.
    #[error("heartbeat error: {0}")]
    Heartbeat(String),
}

impl CoreError {
    /// Build an `Engine` error from a context label and an underlying cause,
    /// formatted as `"{context}: {cause}"`.
    pub(crate) fn engine(context: impl Display, cause: impl Display) -> Self {
        CoreError::Engine(format!("{context}: {cause}"))
    }

    /// Build a `Heartbeat` error from a context label and an underlying cause,
    /// formatted as `"{context}: {cause}"`.
    pub(crate) fn heartbeat(context: impl Display, cause: impl Display) -> Self {
        CoreError::Heartbeat(format!("{context}: {cause}"))
    }

    /// Classify a DuckDB error into a `CoreError` variant by inspecting its message.
    /// DuckDB does not expose stable error codes, so this matches on substrings;
    /// unrecognized errors fall back to `Engine`. Shared by every DuckDB execution
    /// path so the same failure is reported consistently.
    pub(crate) fn from_duckdb(e: duckdb::Error) -> Self {
        let msg = e.to_string();
        let lower = msg.to_lowercase();
        if lower.contains("table")
            && (lower.contains("does not exist") || lower.contains("not found"))
        {
            CoreError::ShardNotFound(msg)
        } else if lower.contains("unique constraint")
            || lower.contains("duplicate key")
            || lower.contains("primary key constraint")
        {
            CoreError::WriteConflict(msg)
        } else if lower.contains("conversion error")
            || lower.contains("cannot cast")
            || lower.contains("type mismatch")
        {
            CoreError::TypeMismatch(msg)
        } else {
            CoreError::Engine(format!("write execution failed: {msg}"))
        }
    }

    /// Map this error to the protobuf [`VdbErrorCode`] reported to clients.
    pub(crate) fn vdb_error_code(&self) -> VdbErrorCode {
        match self {
            CoreError::ShardNotFound(_) => VdbErrorCode::ShardNotFound,
            CoreError::WriteConflict(_) => VdbErrorCode::WriteConflict,
            CoreError::TypeMismatch(_) => VdbErrorCode::TypeMismatch,
            CoreError::WriteQueue(_) => VdbErrorCode::WriteQueueFull,
            CoreError::Engine(_) => VdbErrorCode::EngineError,
            CoreError::Heartbeat(_) => VdbErrorCode::InternalError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_error_displays_message() {
        let err = CoreError::Engine("connection lost".to_string());
        assert_eq!(err.to_string(), "engine error: connection lost");
    }

    #[test]
    fn write_queue_error_displays_message() {
        let err = CoreError::WriteQueue("channel closed".to_string());
        assert_eq!(err.to_string(), "write queue error: channel closed");
    }

    #[test]
    fn heartbeat_error_displays_message() {
        let err = CoreError::Heartbeat("timeout".to_string());
        assert_eq!(err.to_string(), "heartbeat error: timeout");
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<CoreError>();
        assert_sync::<CoreError>();
    }

    #[test]
    fn error_implements_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(CoreError::Engine("test".to_string()));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn error_source_is_none() {
        use std::error::Error;
        let variants: Vec<CoreError> = vec![
            CoreError::Engine("e".to_string()),
            CoreError::WriteQueue("w".to_string()),
            CoreError::Heartbeat("h".to_string()),
        ];
        for err in &variants {
            assert!(err.source().is_none());
        }
    }

    #[test]
    fn error_debug_contains_variant_name() {
        let err = CoreError::Engine("some detail".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Engine"));
        assert!(debug.contains("some detail"));
    }

    #[test]
    fn error_variants_are_distinguishable() {
        let err = CoreError::Engine("x".to_string());
        assert!(matches!(err, CoreError::Engine(_)));
        assert!(!matches!(err, CoreError::WriteQueue(_)));
        assert!(!matches!(err, CoreError::Heartbeat(_)));
    }

    #[test]
    fn error_display_includes_inner_message() {
        let msg = "connection refused at 127.0.0.1:5432";
        let err = CoreError::Engine(msg.to_string());
        assert!(err.to_string().contains(msg));
    }
}
