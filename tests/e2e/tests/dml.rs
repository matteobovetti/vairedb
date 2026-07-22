mod common;
use common::*;

// Write-path DML: INSERT / UPDATE / DELETE affected-row counts plus a read-back
// of the mutated state, and one heavyweight insert that also validates shard
// placement and hash distribution across the cluster.

#[tokio::test]
async fn test_insert_single_row() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_insert",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'hello')"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_insert_multi_row() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_multi",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'one'), (2, 'two'), (3, 'three')"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 3);

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_update() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_update",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'before')"),
    )
    .await
    .unwrap();

    let affected = execute(
        &client,
        &format!("UPDATE {tbl} SET value = 'after' WHERE id = 1"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1);

    // The new value must be readable back.
    let rows = simple_query_rows(&client, &format!("SELECT value FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("after"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_delete() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_delete",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'doomed'), (2, 'safe')"),
    )
    .await
    .unwrap();

    let affected = execute(&client, &format!("DELETE FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // Only the surviving row should remain.
    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("2"));
    assert_eq!(rows[0][1].as_deref(), Some("safe"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_multi_shard_update_no_where_touches_all_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_ms_update",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // One id per bucket so every shard holds exactly one row.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, value) VALUES ({id}, 'before')"),
        )
        .await
        .unwrap();
    }

    // UPDATE with no shard-key predicate fans out to all shards.
    let affected = execute(&client, &format!("UPDATE {tbl} SET value = 'after'"))
        .await
        .unwrap();
    assert_eq!(
        affected, SHARD_COUNT as u64,
        "no-WHERE UPDATE must touch every shard's row"
    );

    let rows = simple_query_rows(&client, &format!("SELECT value FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), SHARD_COUNT);
    assert!(
        rows.iter().all(|r| r[0].as_deref() == Some("after")),
        "every row across all shards must be updated, got {rows:?}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_multi_shard_delete_no_where_clears_all_shards() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_ms_delete",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, value) VALUES ({id}, 'x')"),
        )
        .await
        .unwrap();
    }

    // DELETE with no shard-key predicate fans out to all shards.
    let affected = execute(&client, &format!("DELETE FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(
        affected, SHARD_COUNT as u64,
        "no-WHERE DELETE must remove every shard's row"
    );

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_massive_insert_rows_with_placement_check() {
    let client = ready_client().await;
    const ROW_COUNT: i64 = 10_000;

    let tbl = create_table(
        &client,
        "dml_mass",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // One INSERT statement per row so each is routed individually.
    for i in 1..=ROW_COUNT {
        let affected = execute(
            &client,
            &format!("INSERT INTO {tbl} (id, value) VALUES ({i}, 'row_{i}')"),
        )
        .await
        .unwrap();
        assert_eq!(affected, 1, "INSERT for id={i} did not affect 1 row");
    }

    // Every row must be present, in id order, with the correct value.
    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        ROW_COUNT as usize,
        "expected {ROW_COUNT} rows, got {}",
        rows.len()
    );
    for (idx, row) in rows.iter().enumerate() {
        let expected_id = idx + 1;
        let actual_id: usize = row[0].as_deref().unwrap().parse().unwrap();
        assert_eq!(actual_id, expected_id);
        assert_eq!(
            row[1].as_deref(),
            Some(format!("row_{expected_id}").as_str())
        );
    }

    // Shard layout: exactly SHARD_COUNT shards, each spread across all nodes with
    // no node hosting two copies of the same shard, and primaries all distinct.
    let shards = fetch_shards(&client, &tbl).await;
    assert_eq!(shards.len(), SHARD_COUNT, "expected {SHARD_COUNT} shards");

    let primary_nodes: std::collections::HashSet<&str> = shards
        .iter()
        .map(|(_, primary, _)| primary.as_str())
        .collect();
    assert_eq!(
        primary_nodes.len(),
        SHARD_COUNT,
        "expected each primary shard on a distinct node, got {primary_nodes:?}"
    );

    for (bucket, primary, replicas) in &shards {
        assert_eq!(
            replicas.len(),
            SHARD_COUNT - 1,
            "shard bucket {bucket} should have {} replicas, got {}",
            SHARD_COUNT - 1,
            replicas.len()
        );
        let mut nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        nodes.insert(primary.as_str());
        for replica in replicas {
            assert!(
                nodes.insert(replica.as_str()),
                "shard bucket {bucket} has duplicate node placement on {replica}"
            );
        }
        assert_eq!(
            nodes.len(),
            SHARD_COUNT,
            "shard bucket {bucket} should span all {SHARD_COUNT} nodes, got {nodes:?}"
        );
    }

    // Distribution must be non-degenerate: every bucket holds at least one row,
    // and the per-bucket counts (recomputed with the router's hash) sum to all rows.
    let mut bucket_row_counts = [0usize; SHARD_COUNT];
    for i in 1..=ROW_COUNT {
        bucket_row_counts[bucket_of(i) as usize] += 1;
    }
    for (bucket, count) in bucket_row_counts.iter().enumerate() {
        assert!(
            *count > 0,
            "shard bucket {bucket} has no rows — distribution is degenerate"
        );
    }
    assert_eq!(bucket_row_counts.iter().sum::<usize>(), ROW_COUNT as usize);

    drop_table(&client, &tbl).await;
}
