use vairedb_common::proto::vairedb::v1::*;

// Generated prost structs and their encode/decode are covered by prost itself.
// Only the persisted enum numbering is a real contract: these values are stored
// in the coordinator's redb catalog, so renumbering a variant silently
// corrupts existing metadata and must fail here.

#[test]
fn node_state_enum_values() {
    assert_eq!(NodeState::Unspecified as i32, 0);
    assert_eq!(NodeState::Alive as i32, 1);
    assert_eq!(NodeState::Suspect as i32, 2);
    assert_eq!(NodeState::Dead as i32, 3);
}

#[test]
fn shard_strategy_enum_values() {
    assert_eq!(ShardStrategy::Unspecified as i32, 0);
    assert_eq!(ShardStrategy::Hash as i32, 1);
    assert_eq!(ShardStrategy::Range as i32, 2);
}
