use vairedb_common::scan_plan::DuckDbScanPlanBytes;

// `encode`/`decode` is a thin serde_json wrapper; a to_vec -> from_slice cycle
// is symmetric by construction and only fails on a serde bug, not our code.
// So we don't exhaustively roundtrip field permutations. Instead we pin the
// things that are actually ours: the on-disk JSON shape, the `#[serde(default)]`
// backfill behavior, and the custom decode-error contract.

#[test]
fn encode_decode_roundtrip_preserves_all_fields() {
    let plan = DuckDbScanPlanBytes {
        shard_table_name: "orders_shard0".to_string(),
        schema_ipc: vec![1, 2, 3, 4, 5],
        projection: Some(vec![0, 2, 5]),
        filter_exprs: vec!["id > 10".to_string(), "status = 'active'".to_string()],
        target_executor_id: Some("exec-1".to_string()),
        replica_executor_ids: vec!["exec-2".to_string()],
    };

    let decoded = DuckDbScanPlanBytes::decode(&plan.encode()).unwrap();

    assert_eq!(decoded.shard_table_name, "orders_shard0");
    assert_eq!(decoded.schema_ipc, vec![1, 2, 3, 4, 5]);
    assert_eq!(decoded.projection, Some(vec![0, 2, 5]));
    assert_eq!(decoded.filter_exprs, vec!["id > 10", "status = 'active'"]);
    assert_eq!(decoded.target_executor_id.as_deref(), Some("exec-1"));
    assert_eq!(decoded.replica_executor_ids, vec!["exec-2"]);
}

#[test]
fn encode_produces_expected_json_shape() {
    let plan = DuckDbScanPlanBytes {
        shard_table_name: "test_shard".to_string(),
        schema_ipc: vec![42],
        projection: Some(vec![1, 3]),
        filter_exprs: vec!["col = 'value'".to_string()],
        target_executor_id: None,
        replica_executor_ids: vec![],
    };

    let json: serde_json::Value = serde_json::from_slice(&plan.encode()).unwrap();

    assert_eq!(json["shard_table_name"], "test_shard");
    assert_eq!(json["schema_ipc"], serde_json::json!([42]));
    assert_eq!(json["projection"], serde_json::json!([1, 3]));
    assert_eq!(json["filter_exprs"], serde_json::json!(["col = 'value'"]));
}

#[test]
fn decode_backfills_optional_fields_when_absent() {
    // target_executor_id and replica_executor_ids are #[serde(default)]:
    // older payloads that predate those fields must still decode.
    let legacy = br#"{
        "shard_table_name": "s0",
        "schema_ipc": [1, 2],
        "projection": null,
        "filter_exprs": []
    }"#;

    let decoded = DuckDbScanPlanBytes::decode(legacy).unwrap();

    assert_eq!(decoded.shard_table_name, "s0");
    assert!(decoded.target_executor_id.is_none());
    assert!(decoded.replica_executor_ids.is_empty());
}

#[test]
fn decode_invalid_bytes_returns_error() {
    let result = DuckDbScanPlanBytes::decode(b"not valid json at all {{{");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("failed to decode DuckDbScanPlanBytes")
    );
}

#[test]
fn decode_empty_slice_returns_error() {
    assert!(DuckDbScanPlanBytes::decode(&[]).is_err());
}

#[test]
fn decode_missing_required_fields_returns_error() {
    let partial = br#"{"shard_table_name": "x"}"#;
    assert!(DuckDbScanPlanBytes::decode(partial).is_err());
}
