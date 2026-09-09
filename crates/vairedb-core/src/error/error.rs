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

    /// An expression divided by zero.
    ///
    /// Carries no detail on purpose: the message is PostgreSQL's own wording, and it is
    /// what the client reads. What DuckDB said is logged by the caller, and what the
    /// client needs is the class — `22012`, the same one the read path reports for the
    /// same expression. The write path raises this by way of a `CASE … error('division by
    /// zero')` guard the coordinator wraps every division in, since DuckDB itself answers
    /// NULL.
    #[error("division by zero")]
    DivisionByZero,

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
        } else if lower.contains("division by zero") || lower.contains("divide by zero") {
            // Raised by the coordinator's own guard — DuckDB answers NULL for `7/0` — so
            // the phrase is one this tree wrote and not one a version bump can reword.
            // Reported as `22012`, which is what the read path reports for `7/0` too.
            tracing::warn!(error = %msg, "write divided by zero");
            CoreError::DivisionByZero
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
            CoreError::DivisionByZero => VdbErrorCode::DivisionByZero,
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

    /// The guard the coordinator wraps every write-path division in raises through DuckDB's
    /// `error()`, so the failure arrives here as a DuckDB message and has to be classified
    /// back into the class PostgreSQL uses — `22012` — rather than the `XX000` an
    /// unrecognized message would get. The phrase is one this tree wrote, so matching on it
    /// is not the usual bet on an upstream wording.
    #[test]
    fn a_guarded_zero_divisor_is_classified_as_division_by_zero() {
        for msg in [
            "Invalid Input Error: division by zero",
            // DuckDB prefixes and wraps, and PostgreSQL's own spelling of the same class
            // varies by operator ("divide by zero" for modulo), so both are matched.
            "Invalid Input Error: Divide by zero",
        ] {
            let err = CoreError::from_duckdb(duckdb::Error::DuckDBFailure(
                duckdb::ffi::Error::new(1),
                Some(msg.to_string()),
            ));
            assert!(
                matches!(err, CoreError::DivisionByZero),
                "`{msg}` should be a zero divisor, got: {err:?}"
            );
            assert_eq!(err.vdb_error_code(), VdbErrorCode::DivisionByZero);
            // The client reads PostgreSQL's wording, not DuckDB's.
            assert_eq!(err.to_string(), "division by zero");
        }
    }

    /// An unrelated failure must not be swept into the zero-divisor class just because it
    /// mentions division.
    #[test]
    fn an_unrelated_error_is_not_a_division_by_zero() {
        let err = CoreError::from_duckdb(duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("Binder Error: No function matches divide(VARCHAR, VARCHAR)".to_string()),
        ));
        assert!(matches!(err, CoreError::Engine(_)), "got: {err:?}");
    }

    #[test]
    fn error_display_includes_inner_message() {
        let msg = "connection refused at 127.0.0.1:5432";
        let err = CoreError::Engine(msg.to_string());
        assert!(err.to_string().contains(msg));
    }
}
