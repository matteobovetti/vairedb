mod common;
use common::*;

// INSERT / UPDATE / DELETE — rows 2-4 of docs/specs/gap-analysis-command.md
// (✅ supported, each with documented restrictions) plus row 25 MERGE INTO (❌,
// prioritized gap #6 "MERGE INTO / upsert"). See `sql_command_select.rs` for the
// four-file layout and the passing/#[ignore] convention.
//
//     cd tests/e2e && cargo test --test sql_command_dml -- --ignored --test-threads=1
//
// The doc's restrictions on the ✅ rows are what the xfails here encode:
//   * row 2 — INSERT needs an explicit column list naming the shard key with a
//     non-NULL value per row; `INSERT … SELECT` and positional inserts are
//     rejected (`validate_insert_shard_key`).
//   * row 3 — UPDATE may not mutate the shard-key column (row relocation).
//
// The rejection contract for an omitted or NULL shard key already lives in
// `shard_key_hazards.rs` (`test_insert_without_shard_key_rejected`,
// `test_update_shard_key_value_rejected`); this file adds the two restrictions
// that file does not cover (positional INSERT, `INSERT … SELECT`) and the
// target-state xfails for all of them.

// ============================================================================
// INSERT — row 2 (✅ with shard-key restrictions)
// ============================================================================

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

// Restriction 1: no column list. The router cannot tell which VALUES position
// holds the shard key, so it refuses rather than broadcast the row.
#[tokio::test]
async fn test_insert_without_column_list_currently_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_positional",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let err = assert_rejected(&client, &format!("INSERT INTO {tbl} VALUES (1, 'x')")).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "positional INSERT should be rejected as unsupported, got: {}",
        err.message()
    );
    assert!(
        err.message().contains("column list"),
        "error should explain the missing column list, got: {}",
        err.message()
    );
    assert_eq!(row_count(&client, &tbl).await, 0, "nothing may be written");

    drop_table(&client, &tbl).await;
}

// Target state: PostgreSQL matches VALUES to columns positionally, so a
// positional INSERT must land on the shard implied by the shard-key column's
// ordinal position.
#[tokio::test]
#[ignore = "gap (row 2): validate_insert_shard_key rejects an INSERT with no column list, so positional inserts (the PostgreSQL default) never route"]
async fn test_insert_positional_values_route_by_shard_key() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_positional_x",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} VALUES (1, 'x'), (2, 'y')"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 2);

    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "each row must be stored exactly once");
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("x"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));
    assert_eq!(rows[1][1].as_deref(), Some("y"));

    drop_table(&client, &tbl).await;
}

// Restriction 2: `INSERT … SELECT`. The shard key's value is not known until the
// SELECT runs, so the statement cannot be split per shard up front.
#[tokio::test]
async fn test_insert_select_currently_rejected() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "dml_isel_src",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let dst = create_table(
        &client,
        "dml_isel_dst",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {src} (id, value) VALUES (1, 'a'), (2, 'b')"),
    )
    .await
    .unwrap();

    let err = assert_rejected(
        &client,
        &format!("INSERT INTO {dst} (id, value) SELECT id, value FROM {src}"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "INSERT ... SELECT should be rejected as unsupported, got: {}",
        err.message()
    );
    assert_eq!(
        row_count(&client, &dst).await,
        0,
        "a rejected INSERT ... SELECT may not write partial rows"
    );

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
}

// Target state: `INSERT … SELECT` is the standard bulk-copy statement; each
// produced row must be routed to the shard its key hashes to.
#[tokio::test]
#[ignore = "gap (row 2): INSERT ... SELECT is rejected because the shard key is not a literal in the AST; routing it needs the SELECT to be executed and its rows split per shard"]
async fn test_insert_select_copies_rows_sharded() {
    let client = ready_client().await;
    let src = create_table(
        &client,
        "dml_iselx_src",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let dst = create_table(
        &client,
        "dml_iselx_dst",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // One id per shard bucket so the copy has to fan out.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            &client,
            &format!("INSERT INTO {src} (id, value) VALUES ({id}, 'v{id}')"),
        )
        .await
        .unwrap();
    }

    let affected = execute(
        &client,
        &format!("INSERT INTO {dst} (id, value) SELECT id, value FROM {src}"),
    )
    .await
    .unwrap();
    assert_eq!(affected, ids.len() as u64);

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids.clone();
    want.sort_unstable();
    assert_eq!(got, want, "every source row must be copied exactly once");

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
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

// ============================================================================
// UPDATE — row 3 (✅ except mutating the shard key)
// ============================================================================

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

// Target state: changing a row's shard key relocates the row to the shard the
// new key hashes to — deleted from the old shard, inserted on the new one, and
// still readable exactly once. The current rejection is asserted by
// `shard_key_hazards.rs::test_update_shard_key_value_rejected`.
#[tokio::test]
#[ignore = "gap (row 3): mutating the shard-key column is rejected 0A000; relocating the row across shards is unimplemented"]
async fn test_update_shard_key_relocates_row() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_relocate",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    // Pick two ids in different buckets, so the UPDATE must move the row.
    let from_id = id_for_bucket(0, 1);
    let to_id = id_for_bucket(1, 1);
    assert_ne!(bucket_of(from_id), bucket_of(to_id));

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES ({from_id}, 'moves')"),
    )
    .await
    .unwrap();

    let affected = execute(
        &client,
        &format!("UPDATE {tbl} SET id = {to_id} WHERE id = {from_id}"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1);

    // Exactly one row, under the new key, on the new key's shard.
    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the row must not be duplicated or stranded");
    assert_eq!(rows[0][0].as_deref(), Some(to_id.to_string().as_str()));
    assert_eq!(rows[0][1].as_deref(), Some("moves"));

    // And it must be reachable by a shard-key point lookup on the NEW key.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT value FROM {tbl} WHERE id = {to_id}"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "point lookup on the new shard key must find it"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// DELETE — row 4 (✅)
// ============================================================================

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

    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// MERGE INTO / upsert — row 25 (❌, prioritized gap #6)
// ============================================================================

// MERGE INTO is not a routed statement kind, so it must fail rather than silently
// do nothing. Observed today: `0A000` — the coordinator's parser DOES parse MERGE
// (the gap doc's "likely 42601" is wrong), so only routing and execution are
// missing. The assertion stays tolerant of both codes so a parser change cannot
// flip it.
#[tokio::test]
async fn test_merge_into_currently_rejected() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_merge_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_merge_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {target} (id, value) VALUES (1, 'old')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {source} (id, value) VALUES (1, 'new'), (2, 'fresh')"),
    )
    .await
    .unwrap();

    let err = assert_rejected(
        &client,
        &format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ),
    )
    .await;
    let code = err.code().code();
    assert!(
        code == SQLSTATE_FEATURE_NOT_SUPPORTED || code == SQLSTATE_SYNTAX_ERROR,
        "MERGE should be rejected at parse or classification, got {code}: {}",
        err.message()
    );

    // The target is untouched: no upsert happened.
    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {target}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_deref(), Some("old"));

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// Target state: MERGE updates the matched row and inserts the unmatched one,
// each routed to its key's shard.
#[tokio::test]
#[ignore = "gap (row 25): MERGE INTO is unsupported; it needs shard-key-aware routing of the matched/unmatched branches"]
async fn test_merge_into_updates_and_inserts() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_mergex_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_mergex_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {target} (id, value) VALUES (1, 'old')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {source} (id, value) VALUES (1, 'new'), (2, 'fresh')"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id, value FROM {target} ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1].as_deref(), Some("new"), "matched row is updated");
    assert_eq!(
        rows[1][1].as_deref(),
        Some("fresh"),
        "unmatched row is inserted"
    );

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// `INSERT … ON CONFLICT` is PostgreSQL's upsert spelling of the same intent, and
// unlike MERGE it is NOT a gap — see `test_insert_on_conflict_do_update_upserts`
// below. It classifies as an INSERT and reaches DuckDB, which needs an arbiter
// index: without one, the shard reports
//
//   [VDB-2001] node execution failed: The specified columns as conflict target
//   are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT or INDEX
//
// so a table declared without a PRIMARY KEY cannot be upserted into. That is the
// case pinned here: an error, not a silent no-op and not a duplicate row.
#[tokio::test]
async fn test_insert_on_conflict_without_primary_key_is_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_upsert",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'old')"),
    )
    .await
    .unwrap();

    assert_rejected(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, value) VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET value = 'new'"
        ),
    )
    .await;

    // No duplicate row was created either.
    assert_eq!(row_count(&client, &tbl).await, 1);
    let rows = simple_query_rows(&client, &format!("SELECT value FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("old"));

    drop_table(&client, &tbl).await;
}

// Not ignored: upsert on the SHARD KEY already works, provided the arbiter column
// is declared `PRIMARY KEY` in the CREATE TABLE — the declaration reaches the
// per-shard DuckDB tables and gives `ON CONFLICT` its arbiter index. Equal shard
// keys always hash to the same shard, so per-shard uniqueness is globally correct
// here and the ETL-friendly half of prioritized gap #6 is available today.
//
// The remaining limits, each tracked elsewhere: the constraint must exist at
// CREATE time (`ALTER TABLE … ADD CONSTRAINT` and `CREATE UNIQUE INDEX` are both
// rejected — `sql_command_ddl.rs` row 6, `sql_command_unsupported.rs` row 15), and
// an arbiter on a NON-shard-key column would only be enforced per shard, so it
// must not be relied on for global uniqueness.
#[tokio::test]
async fn test_insert_on_conflict_do_update_upserts() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_upsertx",
        &format!("(id INTEGER NOT NULL PRIMARY KEY, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'old')"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, value) VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET value = 'new'"
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        row_count(&client, &tbl).await,
        1,
        "the conflicting row must be updated, not duplicated"
    );
    let rows = simple_query_rows(&client, &format!("SELECT value FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("new"));

    drop_table(&client, &tbl).await;
}
