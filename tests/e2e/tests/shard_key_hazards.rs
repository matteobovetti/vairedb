mod common;
use common::*;

// Shard-key operations that v0.1 cannot route correctly and therefore rejects at
// the coordinator (SQLSTATE 0A000, FeatureNotSupported): an INSERT must supply a
// non-NULL shard key, and an UPDATE may not modify the shard-key column. These
// tests assert the rejection.

// v0.1: changing a row's shard-key value would relocate it to a different shard,
// which the router does not support (it routes by the OLD key in the WHERE clause
// and updates in place, stranding the row). The coordinator rejects such UPDATEs.
#[tokio::test]
async fn test_update_shard_key_value_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_update",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'orig')"),
    )
    .await
    .unwrap();

    let err = execute_expect_err(&client, &format!("UPDATE {tbl} SET id = 2 WHERE id = 1")).await;
    assert_eq!(
        err.code().code(),
        "0A000",
        "UPDATE of shard key should be rejected as unsupported"
    );
    assert!(
        err.message().contains("shard key"),
        "error should mention the shard key, got: {}",
        err.message()
    );

    // Updating a non-shard-key column must still work.
    execute(
        &client,
        &format!("UPDATE {tbl} SET v = 'changed' WHERE id = 1"),
    )
    .await
    .unwrap();

    drop_table(&client, &tbl).await;
}

// v0.1: an INSERT that omits the shard-key column (or sets it to NULL) cannot be
// routed to a single shard, so the coordinator rejects it rather than broadcasting
// the row to every shard and duplicating it.
#[tokio::test]
async fn test_insert_without_shard_key_rejected() {
    let client = ready_client().await;
    // Shard key `sk` is nullable; `id` is just a payload column.
    let tbl = create_table(
        &client,
        "skh_omit",
        "(id INTEGER NOT NULL, sk INTEGER, v VARCHAR) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'sk')",
    )
    .await;

    // Omitting the shard key is rejected.
    let omit_err = execute_expect_err(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'x')"),
    )
    .await;
    assert_eq!(omit_err.code().code(), "0A000");
    assert!(
        omit_err.message().contains("shard key"),
        "error should mention the shard key, got: {}",
        omit_err.message()
    );

    // Setting the shard key to NULL is rejected.
    let null_err = execute_expect_err(
        &client,
        &format!("INSERT INTO {tbl} (id, sk, v) VALUES (1, NULL, 'x')"),
    )
    .await;
    assert_eq!(null_err.code().code(), "0A000");

    // A non-NULL shard key still works and is not duplicated.
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, sk, v) VALUES (1, 7, 'x')"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1, "a single INSERT should affect exactly one row");

    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

// A shard key bound to a NULL *parameter* (not a literal NULL) must be rejected
// just like the literal case. The validator only saw literal NULLs in the AST,
// so a `$N` bound to NULL slipped through and broadcast/duplicated the row.
#[tokio::test]
async fn test_insert_null_bound_param_shard_key_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_null_param",
        "(id INTEGER NOT NULL, sk INTEGER, v VARCHAR) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'sk')",
    )
    .await;

    let sk: Option<i32> = None;
    let v = "x";
    let err = client
        .execute(
            &format!("INSERT INTO {tbl} (id, sk, v) VALUES ($1, $2, $3)"),
            &[&1i32, &sk, &v],
        )
        .await
        .expect_err("INSERT with NULL-bound shard key must be rejected");
    let db_err = err.as_db_error().expect("should be a db error");
    assert_eq!(db_err.code().code(), "0A000");

    // Nothing should have been written to any shard.
    let rows = simple_query_rows(&client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("0"));

    drop_table(&client, &tbl).await;
}

// A DELETE whose shard-key predicate binds a NULL parameter must be rejected
// rather than silently fanning out to every shard.
#[tokio::test]
async fn test_delete_null_bound_param_shard_key_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_del_null_param",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(&client, &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a')"))
        .await
        .unwrap();

    let id: Option<i32> = None;
    let err = client
        .execute(&format!("DELETE FROM {tbl} WHERE id = $1"), &[&id])
        .await
        .expect_err("DELETE with NULL-bound shard key must be rejected");
    let db_err = err.as_db_error().expect("should be a db error");
    assert_eq!(db_err.code().code(), "0A000");

    drop_table(&client, &tbl).await;
}

// Control: a point DELETE by the shard key (no relocation) removes exactly the
// one matching row. This should pass and confirms single-shard DELETE routing.
#[tokio::test]
async fn test_delete_by_shard_key_point() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_del",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await
    .unwrap();

    let deleted = execute(&client, &format!("DELETE FROM {tbl} WHERE id = 2"))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![1, 3]);

    drop_table(&client, &tbl).await;
}
