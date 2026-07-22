use vairedb_common::proto::vairedb::v1::VdbErrorCode;
use vairedb_coordinator::error::{CoordinatorError, NodeError, Result};

use sqlparser::parser::ParserError;

#[test]
fn test_error_display_table_not_found() {
    let err = CoordinatorError::TableNotFound("orders".to_string());
    assert_eq!(err.to_string(), "table not found: orders");
}

#[test]
fn test_error_display_node_not_found() {
    let err = CoordinatorError::NodeNotFound("node-1".to_string());
    assert_eq!(err.to_string(), "node not found: node-1");
}

#[test]
fn test_error_display_shard_not_assigned() {
    let err = CoordinatorError::ShardNotAssigned("shard0".to_string());
    assert_eq!(err.to_string(), "shard not assigned: shard0");
}

#[test]
fn test_error_display_quorum_not_reached() {
    let err = CoordinatorError::QuorumNotReached { needed: 2, got: 1 };
    assert_eq!(err.to_string(), "quorum not reached: needed 2, got 1");
}

#[test]
fn test_error_display_shard_unavailable() {
    let err = CoordinatorError::ShardUnavailable("shard3".to_string());
    assert_eq!(
        err.to_string(),
        "shard unavailable: primary node unreachable for shard shard3"
    );
}

#[test]
fn test_error_display_internal() {
    let err = CoordinatorError::Internal("something broke".to_string());
    assert_eq!(err.to_string(), "internal error: something broke");
}

#[test]
fn test_error_display_serialization() {
    let err = CoordinatorError::Serialization("bad bytes".to_string());
    assert_eq!(err.to_string(), "serialization error: bad bytes");
}

#[test]
fn test_error_is_debug() {
    let err = CoordinatorError::Internal("test".to_string());
    let debug = format!("{:?}", err);
    assert!(debug.contains("Internal"));
}

#[test]
fn test_result_type_alias() {
    let ok: Result<u32> = Ok(42);
    assert!(ok.is_ok());

    let err: Result<u32> = Err(CoordinatorError::Internal("fail".to_string()));
    assert!(err.is_err());
}

#[test]
fn test_grpc_status_conversion() {
    let status = tonic::Status::not_found("gone");
    let err = CoordinatorError::Grpc(Box::new(status));
    assert!(err.to_string().contains("gone"));
}

#[test]
fn test_grpc_status_display_format() {
    let status = tonic::Status::internal("connection reset");
    let err = CoordinatorError::Grpc(Box::new(status));
    let display = err.to_string();
    assert!(display.starts_with("grpc error:"));
    assert!(display.contains("connection reset"));
}

#[test]
fn test_grpc_transport_conversion() {
    let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:0");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let transport_err = rt.block_on(async { endpoint.connect().await.unwrap_err() });

    let err = CoordinatorError::GrpcTransport(Box::new(transport_err));
    let display = err.to_string();
    assert!(display.starts_with("grpc transport error:"));
}

#[test]
fn test_from_sql_parser_error() {
    let parse_err = ParserError::ParserError("unexpected token".to_string());
    let err = CoordinatorError::from(parse_err);
    let display = err.to_string();
    assert!(display.starts_with("sql parse error:"));
    assert!(display.contains("unexpected token"));
}

#[test]
fn test_from_sql_tokenizer_error() {
    let tok_err = ParserError::TokenizerError("invalid character".to_string());
    let err = CoordinatorError::from(tok_err);
    let display = err.to_string();
    assert!(display.starts_with("sql parse error:"));
    assert!(display.contains("invalid character"));
}

#[test]
fn test_from_redb_storage_error() {
    let storage_err = redb::StorageError::Corrupted("bad checksum".to_string());
    let err = CoordinatorError::from(storage_err);
    let display = err.to_string();
    assert!(display.starts_with("catalog storage error:"));
    assert!(display.contains("bad checksum"));
}

#[test]
fn test_from_redb_table_error() {
    let table_err = redb::TableError::TableDoesNotExist("my_table".to_string());
    let err = CoordinatorError::from(table_err);
    let display = err.to_string();
    assert!(display.starts_with("catalog table error:"));
    assert!(display.contains("my_table"));
}

#[test]
fn test_from_redb_transaction_error() {
    let storage_err = redb::StorageError::Corrupted("disk failure".to_string());
    let txn_err = redb::TransactionError::Storage(storage_err);
    let err = CoordinatorError::from(txn_err);
    let display = err.to_string();
    assert!(display.starts_with("catalog transaction error:"));
    assert!(display.contains("disk failure"));
}

#[test]
fn test_from_redb_commit_error() {
    let storage_err = redb::StorageError::Corrupted("write failed".to_string());
    let commit_err = redb::CommitError::Storage(storage_err);
    let err = CoordinatorError::from(commit_err);
    let display = err.to_string();
    assert!(display.starts_with("catalog commit error:"));
    assert!(display.contains("write failed"));
}

#[test]
fn test_from_redb_error() {
    let redb_err = redb::Error::Corrupted("metadata invalid".to_string());
    let err = CoordinatorError::from(redb_err);
    let display = err.to_string();
    assert!(display.starts_with("catalog error:"));
    assert!(display.contains("metadata invalid"));
}

#[test]
fn test_error_display_node_exec_failed() {
    let node_err = NodeError {
        message: "disk full".to_string(),
        error_code: VdbErrorCode::EngineError as i32,
        shard_id: "orders_shard0".to_string(),
        node_id: "node-1".to_string(),
    };
    let err = CoordinatorError::NodeExecFailed(Box::new(node_err));
    assert_eq!(
        err.to_string(),
        "node execution failed: node execution failed on node 'node-1' (shard 'orders_shard0'): disk full"
    );
}

#[test]
fn test_node_error_display() {
    let node_err = NodeError {
        message: "write conflict".to_string(),
        error_code: VdbErrorCode::WriteConflict as i32,
        shard_id: "shard0".to_string(),
        node_id: "node-1".to_string(),
    };
    assert_eq!(
        node_err.to_string(),
        "node execution failed on node 'node-1' (shard 'shard0'): write conflict"
    );
}

#[test]
fn test_error_variants_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CoordinatorError>();
}
