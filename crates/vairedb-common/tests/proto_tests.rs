use vairedb_common::proto::vairedb::v1::*;

// Generated prost structs and their encode/decode are exercised by prost's own
// test suite. The only contract worth pinning here is the on-the-wire enum
// numbering: these values map to PostgreSQL SQLSTATEs and persisted redb data,
// so reordering or renumbering a variant is a breaking change that must fail.

#[test]
fn write_operation_enum_values() {
    assert_eq!(WriteOperation::Unspecified as i32, 0);
    assert_eq!(WriteOperation::Insert as i32, 1);
    assert_eq!(WriteOperation::Update as i32, 2);
    assert_eq!(WriteOperation::Delete as i32, 3);
}

#[test]
fn vdb_error_code_enum_values() {
    assert_eq!(VdbErrorCode::Unspecified as i32, 0);
    assert_eq!(VdbErrorCode::TableNotFound as i32, 1000);
    assert_eq!(VdbErrorCode::ShardNotFound as i32, 2000);
    assert_eq!(VdbErrorCode::WriteConflict as i32, 2001);
    assert_eq!(VdbErrorCode::EngineError as i32, 2002);
    assert_eq!(VdbErrorCode::NodeShuttingDown as i32, 3002);
    assert_eq!(VdbErrorCode::InternalError as i32, 5001);
}

#[test]
fn node_status_enum_values() {
    assert_eq!(NodeStatus::Unspecified as i32, 0);
    assert_eq!(NodeStatus::Healthy as i32, 1);
    assert_eq!(NodeStatus::Degraded as i32, 2);
}

#[test]
fn heartbeat_action_enum_values() {
    assert_eq!(HeartbeatAction::Unspecified as i32, 0);
    assert_eq!(HeartbeatAction::None as i32, 1);
    assert_eq!(HeartbeatAction::Drain as i32, 2);
}

#[test]
fn failure_type_enum_values() {
    assert_eq!(FailureType::Unspecified as i32, 0);
    assert_eq!(FailureType::Duckdb as i32, 1);
    assert_eq!(FailureType::BallistaExecutor as i32, 2);
}
