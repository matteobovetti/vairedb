mod common;
use common::*;

// INSERT / UPDATE / DELETE — rows 2-4 of docs/specs/gap-analysis-command.md
// (✅ supported, each with documented restrictions) plus row 25 MERGE INTO
// (✅ for the shapes that can be applied shard by shard). See
// `sql_command_select.rs` for the four-file layout and the passing/#[ignore]
// convention.
//
//     cd tests/e2e && cargo test --test sql_command_dml -- --ignored --test-threads=1
//
// The doc's restrictions on the ✅ rows are what the xfails here encode:
//   * row 2 — INSERT needs a column list naming the shard key with a non-NULL
//     value per row. A positional `INSERT … VALUES` gets that list resolved from
//     the catalog's declaration order (`materialize_insert_columns`), and an
//     `INSERT … SELECT` has its source query materialized first so its keys become
//     literals — what stays refused there is `RETURNING`, whose rows would have to
//     be gathered back from every shard the copy touched.
//   * row 3 — UPDATE may not mutate the shard-key column (row relocation).
//   * row 25 — MERGE needs its ON clause to equate the target's shard key with a
//     source column, and its source to be either a table sharded identically and
//     co-located or an inline `VALUES` list the coordinator can split per shard.
//
// The rejection contract for an omitted or NULL shard key already lives in
// `shard_key_hazards.rs` (`test_insert_without_shard_key_rejected`,
// `test_update_shard_key_value_rejected`); this file adds the restrictions that
// file does not cover (positional INSERT, `INSERT … SELECT`) and the
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

// A positional row shorter than the table fills the LEADING columns, as
// PostgreSQL does; the rest keep their defaults. The shard key is the first
// column here, so the row is still routable.
#[tokio::test]
async fn test_insert_positional_short_row_fills_leading_columns() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_positional_short",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let affected = execute(&client, &format!("INSERT INTO {tbl} VALUES (7)"))
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("7"));
    assert_eq!(rows[0][1], None, "the omitted column takes its default");

    drop_table(&client, &tbl).await;
}

// More values than the table has columns has no positional mapping at all —
// a client error, reported as one, with nothing written.
#[tokio::test]
async fn test_insert_positional_too_many_values_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_positional_wide",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let err = assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} VALUES (1, 'x', 'extra')"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_SYNTAX_ERROR,
        "got: {}",
        err.message()
    );
    assert!(
        err.message()
            .contains("more expressions than target columns"),
        "error should name the mismatch, got: {}",
        err.message()
    );
    assert_eq!(row_count(&client, &tbl).await, 0, "nothing may be written");

    drop_table(&client, &tbl).await;
}

// PostgreSQL matches VALUES to columns positionally, so a positional INSERT
// lands on the shard implied by the shard-key column's ordinal position.
#[tokio::test]
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

// Restriction 2, as it stands now: `INSERT … SELECT` is routed by materializing the
// source query first (see `test_insert_select_copies_rows_sharded`), but the rows it
// writes land on several shards in whatever order they were shipped, so there is no
// single result set to hand back — `RETURNING` on that form is refused, and refused
// before anything is written.
#[tokio::test]
async fn test_insert_select_with_returning_is_rejected() {
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
        &format!("INSERT INTO {dst} (id, value) SELECT id, value FROM {src} RETURNING id"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "INSERT ... SELECT ... RETURNING should be rejected as unsupported, got: {}",
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

// `INSERT … SELECT` is the standard bulk-copy statement; each produced row is routed
// to the shard its key hashes to.
#[tokio::test]
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
// MERGE INTO — row 25 (✅ for a co-located table source or a VALUES list)
// ============================================================================
//
// A MERGE is the first write that is about two relations, so what these tests pin
// is *which* pairs of relations VaireDB will run one over. The rule is that equal
// shard keys always hash to the same shard, so a MERGE whose ON clause requires
// the target's shard key to equal the source's can be applied shard by shard. Two
// shapes qualify: another table sharded the same way (fanned out to every shard)
// and an inline VALUES list (split per shard by the coordinator). See
// `crates/vairedb-coordinator/src/pgwire_handler/merge.rs`.

// The canonical case, spanning shards: the matched row is updated on its own
// shard, the unmatched one inserted on a *different* shard.
#[tokio::test]
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

    // Two ids on different shards, so a broadcast that ignored the split would
    // show up as a duplicate rather than passing by luck.
    let matched = id_for_bucket(0, 1);
    let unmatched = id_for_bucket(1, 1);

    execute(
        &client,
        &format!("INSERT INTO {target} (id, value) VALUES ({matched}, 'old')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!(
            "INSERT INTO {source} (id, value) VALUES ({matched}, 'new'), ({unmatched}, 'fresh')"
        ),
    )
    .await
    .unwrap();

    let rows_affected = execute(
        &client,
        &format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows_affected, 2, "one row updated, one inserted");

    let rows = simple_query_rows(
        &client,
        &format!("SELECT id, value FROM {target} ORDER BY id"),
    )
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "no row was duplicated across shards: {rows:?}"
    );
    assert_eq!(rows[0][1].as_deref(), Some("new"), "matched row is updated");
    assert_eq!(
        rows[1][1].as_deref(),
        Some("fresh"),
        "unmatched row is inserted"
    );

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// Unaliased relations: the ON clause refers to the target's columns through the
// table's own name, which the shard-local rewrite renames out from under it. The
// coordinator keeps the name alive as an alias — without that, this fails on the
// node with an "unknown table" error.
#[tokio::test]
async fn test_merge_without_relation_aliases() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_mergena_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_mergena_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let id = id_for_bucket(2, 1);
    execute(
        &client,
        &format!("INSERT INTO {target} (id, value) VALUES ({id}, 'old')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {source} (id, value) VALUES ({id}, 'new')"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!(
            "MERGE INTO {target} USING {source} ON {target}.id = {source}.id \
             WHEN MATCHED THEN UPDATE SET value = {source}.value"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT value FROM {target}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("new"));

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// A target-qualified `SET t.value = ...` is how MSSQL, Snowflake and Oracle spell
// it; the coordinator strips the qualifier PostgreSQL and DuckDB both reject.
// `WHEN MATCHED AND <condition>` and `WHEN MATCHED THEN DELETE` are evaluated by
// each shard against its own rows, so both are honored as written.
#[tokio::test]
async fn test_merge_conditional_clauses_and_delete() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_mergec_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_mergec_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let keep = id_for_bucket(0, 1);
    let update = id_for_bucket(1, 1);
    let remove = id_for_bucket(2, 1);
    execute(
        &client,
        &format!(
            "INSERT INTO {target} (id, value) \
             VALUES ({keep}, 'keep'), ({update}, 'old'), ({remove}, 'old')"
        ),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!(
            "INSERT INTO {source} (id, value) \
             VALUES ({update}, 'new'), ({remove}, 'delete')"
        ),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED AND s.value = 'delete' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET t.value = s.value"
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
    let values: Vec<Option<&str>> = rows.iter().map(|r| r[1].as_deref()).collect();
    assert_eq!(rows.len(), 2, "the 'delete' match was removed: {rows:?}");
    assert!(values.contains(&Some("keep")), "got {values:?}");
    assert!(values.contains(&Some("new")), "got {values:?}");

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// `WHEN NOT MATCHED BY SOURCE` is correct under the fan-out because a target row's
// key can only appear in its own shard's source table, so a row no shard matched is
// a row the source genuinely does not contain — including rows on shards the source
// is empty on.
#[tokio::test]
async fn test_merge_not_matched_by_source_deletes_across_shards() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_mergenms_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_mergenms_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let kept = id_for_bucket(0, 1);
    let stale_a = id_for_bucket(1, 1);
    let stale_b = id_for_bucket(2, 1);
    execute(
        &client,
        &format!(
            "INSERT INTO {target} (id, value) \
             VALUES ({kept}, 'x'), ({stale_a}, 'x'), ({stale_b}, 'x')"
        ),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {source} (id, value) VALUES ({kept}, 'x')"),
    )
    .await
    .unwrap();

    execute(
        &client,
        &format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {target}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "only the matched row survives: {rows:?}");
    assert_eq!(rows[0][0].as_deref(), Some(kept.to_string().as_str()));

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
}

// The second supported shape: an inline row list, which the coordinator hashes
// row by row and splits so each shard receives only the rows it owns. This is the
// upsert spelling that needs no staging table — and no unique index, unlike
// `INSERT … ON CONFLICT`.
#[tokio::test]
async fn test_merge_from_a_values_list_upserts() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_mergev",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let existing = id_for_bucket(0, 1);
    let new_a = id_for_bucket(1, 1);
    let new_b = id_for_bucket(2, 1);
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES ({existing}, 'old')"),
    )
    .await
    .unwrap();

    let rows_affected = execute(
        &client,
        &format!(
            "MERGE INTO {tbl} t \
             USING (VALUES ({existing}, 'new'), ({new_a}, 'a'), ({new_b}, 'b')) AS s(id, value) \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows_affected, 3, "one updated, two inserted");

    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "no row landed on more than one shard: {rows:?}"
    );
    let updated = rows
        .iter()
        .find(|r| r[0].as_deref() == Some(existing.to_string().as_str()))
        .expect("the existing row is still there");
    assert_eq!(updated[1].as_deref(), Some("new"));

    drop_table(&client, &tbl).await;
}

// An implicit column list on the INSERT clause resolves to the table's leading
// columns, the same as a positional `INSERT … VALUES` — which is what lets the
// shard-key rule see the column at all.
#[tokio::test]
async fn test_merge_insert_without_a_column_list() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_mergeic",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    let id = id_for_bucket(1, 1);
    execute(
        &client,
        &format!(
            "MERGE INTO {tbl} t USING (VALUES ({id}, 'v')) AS s(id, value) ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.value)"
        ),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_deref(), Some("v"));

    drop_table(&client, &tbl).await;
}

// Every shape VaireDB refuses, and the target left untouched by each: a refusal
// that had partly applied would be worse than no MERGE at all.
#[tokio::test]
async fn test_merge_unsupported_shapes_are_rejected() {
    let client = ready_client().await;
    let target = create_table(
        &client,
        "dml_mergerej_t",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let source = create_table(
        &client,
        "dml_mergerej_s",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    // Sharded by `value` instead, so an ON clause on `id` cannot place its rows.
    let by_value = create_table(
        &client,
        "dml_mergerej_v",
        "(id INTEGER NOT NULL, value VARCHAR NOT NULL) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'value')",
    )
    .await;
    // One shard instead of three: `hash % 1` and `hash % 3` place the same key on
    // unrelated shards.
    let one_shard = create_table(
        &client,
        "dml_mergerej_1",
        "(id INTEGER NOT NULL, value VARCHAR) \
         WITH (shards = 1, replication_factor = 3, shard_by = 'id')",
    )
    .await;

    let id = id_for_bucket(0, 1);
    execute(
        &client,
        &format!("INSERT INTO {target} (id, value) VALUES ({id}, 'old')"),
    )
    .await
    .unwrap();

    for sql in [
        // The coordinator cannot see a subquery's rows, so it cannot tell which
        // shard each one belongs on.
        format!(
            "MERGE INTO {target} t USING (SELECT id, value FROM {source}) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // The ON clause does not pin the shard key: the matching rows could be on
        // any shard.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.value = s.value \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // An OR lets a target row match a source row with a different key, which
        // lives on another shard.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id OR t.value = s.value \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // Unqualified sides: no telling which relation the column belongs to.
        format!(
            "MERGE INTO {target} t USING {source} s ON id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // Assigning the shard key would relocate the row to a shard the router
        // never wrote it to.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET id = s.id + 1"
        ),
        // The INSERT clause omits the shard key, so the new row cannot be placed.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (value) VALUES (s.value)"
        ),
        // It supplies a shard key that is not the matched column, which could hash
        // to a shard other than the one the clause runs on.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id + 1000, s.value)"
        ),
        // The rows come back from every shard the MERGE touched; there is no single
        // result set to return them in.
        format!(
            "MERGE INTO {target} t USING {source} s ON t.id = s.id \
             WHEN MATCHED THEN DELETE RETURNING t.id"
        ),
        // A source sharded by another column: its matching row could be anywhere.
        format!(
            "MERGE INTO {target} t USING {by_value} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // A different shard count breaks the hash correspondence entirely.
        format!(
            "MERGE INTO {target} t USING {one_shard} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value"
        ),
        // A VALUES source is split per shard, so a shard owning none of the rows
        // would never run this clause — and its rows are the ones it is about.
        format!(
            "MERGE INTO {target} t USING (VALUES ({id}, 'x')) AS s(id, value) ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE"
        ),
        // A VALUES source with no column names: the ON clause cannot name the
        // column that decides the shard.
        format!(
            "MERGE INTO {target} t USING (VALUES ({id}, 'x')) AS s ON t.id = s.column1 \
             WHEN MATCHED THEN DELETE"
        ),
        // A LIMIT decides the row set on the shard, after the coordinator has
        // already split the rows.
        format!(
            "MERGE INTO {target} t USING (VALUES ({id}, 'x') LIMIT 1) AS s(id, value) \
             ON t.id = s.id WHEN MATCHED THEN DELETE"
        ),
        // A row whose key the coordinator cannot evaluate would have to go to every
        // shard.
        format!(
            "MERGE INTO {target} t USING (VALUES (random()::INTEGER, 'x')) AS s(id, value) \
             ON t.id = s.id WHEN MATCHED THEN DELETE"
        ),
    ] {
        assert_unsupported(&client, &sql).await;
    }

    // Nothing was applied by any of them.
    let rows = simple_query_rows(&client, &format!("SELECT id, value FROM {target}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the target is untouched: {rows:?}");
    assert_eq!(rows[0][1].as_deref(), Some("old"));

    drop_table(&client, &target).await;
    drop_table(&client, &source).await;
    drop_table(&client, &by_value).await;
    drop_table(&client, &one_shard).await;
}

// MERGE names a table that does not exist: `42P01`, like any other write.
#[tokio::test]
async fn test_merge_into_unknown_table_rejected() {
    let client = ready_client().await;
    let missing = unique_table_name("dml_mergemissing");

    let err = assert_rejected(
        &client,
        &format!(
            "MERGE INTO {missing} t USING (VALUES (1, 'x')) AS s(id, value) ON t.id = s.id \
             WHEN MATCHED THEN DELETE"
        ),
    )
    .await;
    assert_eq!(err.code().code(), "42P01", "got {}", err.message());
}

// Inside a transaction block a MERGE cannot be buffered: it decides row by row
// what to do, so the row count the client would be told now is unknowable, and it
// reads a state the block's own buffered writes are not in.
#[tokio::test]
async fn test_merge_inside_transaction_block_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_mergetxn",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(&client, "BEGIN").await.unwrap();
    assert_unsupported(
        &client,
        &format!(
            "MERGE INTO {tbl} t USING (VALUES (1, 'x')) AS s(id, value) ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
        ),
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();

    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
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
// here and the ETL-friendly half of row 25 is available today.
//
// The remaining limit, tracked elsewhere: the constraint must be *declared*
// rather than added by `ALTER TABLE … ADD CONSTRAINT` (`sql_command_ddl.rs`
// row 6) — though `CREATE UNIQUE INDEX` now supplies the same arbiter after the
// fact, see `test_unique_index_supplies_the_on_conflict_arbiter`. An arbiter on a
// NON-shard-key column is refused outright rather than silently enforced per
// shard — see `test_insert_on_conflict_on_non_shard_key_is_rejected`.
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

// A table created without a `PRIMARY KEY` can be made upsertable afterwards: a
// `CREATE UNIQUE INDEX` on the shard key is one real UNIQUE index per shard, which
// is exactly the arbiter DuckDB looks for. Before indexes were distributed the
// only way to get one was to declare the constraint at CREATE time, so an existing
// table could never gain upsert.
#[tokio::test]
async fn test_unique_index_supplies_the_on_conflict_arbiter() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_upsertidx",
        &format!("(id INTEGER NOT NULL, value VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let idx = unique_table_name("dml_upsertidx_i");

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, value) VALUES (1, 'old')"),
    )
    .await
    .unwrap();

    // Without an arbiter index the upsert fails, as row 2's sibling test pins.
    assert_rejected(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, value) VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET value = 'new'"
        ),
    )
    .await;

    execute(&client, &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (id)"))
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

    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

// An `ON CONFLICT` arbiter that does not include the shard key cannot do what the
// client asked. The unique index that resolves the conflict exists once per shard,
// so two rows with the same arbiter value but different shard keys land on
// different shards, neither sees the other, and both INSERTs succeed — the upsert
// silently becomes a duplicate. The coordinator refuses it instead.
//
// `email` is declared plain here, not `UNIQUE`: since the constraint phase a
// `UNIQUE` declaration off the shard key is refused at `CREATE TABLE` for exactly
// this reason (`sql_command_ddl.rs`), so the table this test needs cannot be
// declared that way. The arbiter check is the second line of defense, and the one
// that catches a client naming a column no constraint covers at all.
#[tokio::test]
async fn test_insert_on_conflict_on_non_shard_key_is_rejected() {
    let client = ready_client().await;
    // Sharded by `id`; `email` is the column a client would reasonably try to
    // upsert on.
    let tbl = create_table(
        &client,
        "dml_upsert_nonkey",
        "(id INTEGER NOT NULL PRIMARY KEY, email VARCHAR, v VARCHAR) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'id')",
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, email, v) VALUES (1, 'a@x', 'old')"),
    )
    .await
    .unwrap();

    let err = execute_expect_err(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, email, v) VALUES (2, 'a@x', 'new') \
             ON CONFLICT (email) DO UPDATE SET v = 'new'"
        ),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "an arbiter without the shard key must be refused (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("shard key"),
        "the error should explain the shard-key requirement, got: {}",
        err.message()
    );

    // The refusal happened before any shard was written: no second row appeared.
    assert_eq!(row_count(&client, &tbl).await, 1);

    // Naming the shard key alongside it is accepted.
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, email, v) VALUES (1, 'a@x', 'new') \
             ON CONFLICT (id) DO UPDATE SET v = 'new'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// `ON CONFLICT … DO UPDATE SET <shard key> = …` relocates the row to a shard the
// router did not write it to, where nothing will look for it. It is refused for
// the same reason a plain `UPDATE … SET <shard key>` is (`shard_key_hazards.rs`),
// which the UPDATE guard alone did not cover because this is an INSERT.
#[tokio::test]
async fn test_insert_on_conflict_do_update_of_shard_key_is_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "dml_upsert_relocate",
        &format!("(id INTEGER NOT NULL PRIMARY KEY, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'old')"),
    )
    .await
    .unwrap();

    let err = execute_expect_err(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, v) VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET id = 99"
        ),
    )
    .await;
    assert_eq!(err.code().code(), SQLSTATE_FEATURE_NOT_SUPPORTED);
    assert!(
        err.message().contains("shard key"),
        "got: {}",
        err.message()
    );

    // The row is untouched and still where it was written.
    let rows = simple_query_rows(&client, &format!("SELECT v FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("old"));

    drop_table(&client, &tbl).await;
}
