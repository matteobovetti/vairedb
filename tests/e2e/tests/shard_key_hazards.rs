mod common;
use chrono::NaiveDate;
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
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a')"),
    )
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

// An INSERT whose shard-key value is a computed expression cannot be placed: the
// coordinator hashes the shard key before the write reaches a shard, and it does
// not evaluate SQL. Routing on the expression's *source text* — `1 + 1` hashed as
// the string "1 + 1" — stores the row on a shard that no lookup of the value `2`
// ever visits, while the INSERT reports success. So it must be refused.
#[tokio::test]
async fn test_insert_with_computed_shard_key_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_computed",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    for sql in [
        format!("INSERT INTO {tbl} (id, v) VALUES (1 + 1, 'x')"),
        format!("INSERT INTO {tbl} (id, v) VALUES (abs(-2), 'x')"),
        // One unroutable row in a multi-row INSERT rejects the whole statement;
        // silently storing only the routable rows would lose data.
        format!("INSERT INTO {tbl} (id, v) VALUES (1, 'a'), (2 * 2, 'b')"),
    ] {
        let err = execute_expect_err(&client, &sql).await;
        assert_eq!(
            err.code().code(),
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "`{sql}` should be refused as unroutable (got {}: {})",
            err.code().code(),
            err.message()
        );
        assert!(
            err.message().contains("shard key"),
            "`{sql}` error should name the shard key, got: {}",
            err.message()
        );
    }

    // Nothing was written: the rejection happens before any shard is touched.
    assert_eq!(row_count(&client, &tbl).await, 0);

    // The literal form of the same value is accepted and lands on one shard.
    let affected = execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (2, 'x')"),
    )
    .await
    .unwrap();
    assert_eq!(affected, 1);
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// The mirror case for UPDATE/DELETE: their predicate travels to the shards, which
// each re-evaluate it against their own rows, so a shard-key expression the
// coordinator cannot reduce only costs a broadcast — the result is still correct.
// Refusing here would reject a statement VaireDB can run.
#[tokio::test]
async fn test_delete_with_computed_shard_key_still_correct() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_computed_pred",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1,'a'),(2,'b'),(3,'c')"),
    )
    .await
    .unwrap();

    // `id = 1 + 1` is not reducible here, so this broadcasts — and must still
    // affect exactly the one matching row, not zero and not three.
    let deleted = execute(&client, &format!("DELETE FROM {tbl} WHERE id = 1 + 1"))
        .await
        .unwrap();
    assert_eq!(
        deleted, 1,
        "the broadcast DELETE must remove exactly one row"
    );
    assert_eq!(row_count(&client, &tbl).await, 2);

    let updated = execute(
        &client,
        &format!("UPDATE {tbl} SET v = 'z' WHERE id = 2 + 1"),
    )
    .await
    .unwrap();
    assert_eq!(updated, 1);

    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows[1][1].as_deref(), Some("z"));

    drop_table(&client, &tbl).await;
}

// A DATE shard key must route the same whether the value arrives as a bind
// parameter, a `DATE '...'` literal, or a bare string: all three denote the same
// stored value, so a row written with one and looked up with another must be on
// the same shard.
#[tokio::test]
async fn test_date_shard_key_routes_the_same_in_every_spelling() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "skh_date_key",
        "(d DATE NOT NULL, v VARCHAR) \
         WITH (shards = 3, replication_factor = 3, shard_by = 'd')",
    )
    .await;

    let day = NaiveDate::from_ymd_opt(2022, 1, 8).unwrap();
    let affected = client
        .execute(
            &format!("INSERT INTO {tbl} (d, v) VALUES ($1, $2)"),
            &[&day, &"param"],
        )
        .await
        .expect("parameterized INSERT on a DATE shard key should succeed");
    assert_eq!(affected, 1);

    // Written by parameter, found by literal: only true if both hash alike.
    let rows = simple_query_rows(
        &client,
        &format!("SELECT v FROM {tbl} WHERE d = DATE '2022-01-08'"),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("param"));

    // And a point DELETE by the literal reaches the parameter-written row.
    let deleted = execute(
        &client,
        &format!("DELETE FROM {tbl} WHERE d = DATE '2022-01-08'"),
    )
    .await
    .unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(row_count(&client, &tbl).await, 0);

    drop_table(&client, &tbl).await;
}
