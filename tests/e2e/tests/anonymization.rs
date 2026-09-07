mod common;
use common::*;

use hmac::{Hmac, Mac};
use sha2::Sha256;

// Create alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

// Data pseudonymization: columns declared in `anonymized_columns` must be stored
// as their HMAC-SHA256 hex digest (never plaintext), while equality lookups on
// the hashed value still work because the hash is deterministic.

fn hmac_sha256_hex(key: &str, value: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).unwrap();
    mac.update(value.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[tokio::test]
async fn test_anonymized_insert_stores_digest_not_plaintext() {
    let client = ready_client().await;

    // Register a secret with an id unique to this test run.
    let secret_id = unique_table_name("anon_secret");
    let secret_key = "my_awesome_and_secret_key";
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', '{secret_key}')"
        ),
    )
    .await
    .unwrap();

    let tbl = create_table(
        &client,
        "anon_tbl",
        &format!(
            "(id INTEGER NOT NULL, email VARCHAR(64)) \
             WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
             anonymized_columns = [ email -> '{secret_id}' ])"
        ),
    )
    .await;

    let email = "antony.mcdonald@gmail.com";
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, email) VALUES (1, '{email}')"),
    )
    .await
    .unwrap();

    let expected = hmac_sha256_hex(secret_key, email);

    // The stored value must be the digest, not the plaintext.
    let rows = simple_query_rows(&client, &format!("SELECT email FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let stored = rows[0][0].as_deref().unwrap();
    assert_eq!(
        stored, expected,
        "stored value must be the HMAC-SHA256 digest"
    );
    assert_ne!(stored, email, "plaintext must never be stored");
    assert_eq!(stored.len(), 64);

    // Deterministic hashing: an equality lookup by the plaintext-derived digest
    // finds the row.
    let by_digest = simple_query_rows(
        &client,
        &format!("SELECT id FROM {tbl} WHERE email = '{expected}'"),
    )
    .await
    .unwrap();
    assert_eq!(by_digest.len(), 1);
    assert_eq!(by_digest[0][0].as_deref(), Some("1"));

    drop_table(&client, &tbl).await;
}

// The pseudonymizer locates anonymized columns by their position in the INSERT's
// column list. A positional `INSERT … VALUES` names no columns, so it only stays
// safe because the coordinator resolves that list from the catalog first — get the
// order wrong and this write ships plaintext to the shards.
#[tokio::test]
async fn test_anonymized_positional_insert_stores_digest_not_plaintext() {
    let client = ready_client().await;

    let secret_id = unique_table_name("anon_pos_secret");
    let secret_key = "another_awesome_and_secret_key";
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', '{secret_key}')"
        ),
    )
    .await
    .unwrap();

    let tbl = create_table(
        &client,
        "anon_positional",
        &format!(
            "(id INTEGER NOT NULL, email VARCHAR(64)) \
             WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
             anonymized_columns = [ email -> '{secret_id}' ])"
        ),
    )
    .await;

    let email = "positional.write@gmail.com";
    execute(&client, &format!("INSERT INTO {tbl} VALUES (1, '{email}')"))
        .await
        .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT email FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the row must be stored exactly once");
    let stored = rows[0][0].as_deref().unwrap();
    assert_eq!(
        stored,
        hmac_sha256_hex(secret_key, email),
        "a positional INSERT must be hashed like a named one"
    );
    assert_ne!(stored, email, "plaintext must never be stored");

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_anonymized_update_stores_digest() {
    let client = ready_client().await;

    let secret_id = unique_table_name("anon_secret_upd");
    let secret_key = "another_key";
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', '{secret_key}')"
        ),
    )
    .await
    .unwrap();

    let tbl = create_table(
        &client,
        "anon_upd",
        &format!(
            "(id INTEGER NOT NULL, name VARCHAR(64)) \
             WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
             anonymized_columns = [ name -> '{secret_id}' ])"
        ),
    )
    .await;

    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, name) VALUES (1, 'Alice')"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("UPDATE {tbl} SET name = 'Bob' WHERE id = 1"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT name FROM {tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(
        rows[0][0].as_deref(),
        Some(hmac_sha256_hex(secret_key, "Bob").as_str())
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_secret_key_not_exposed_via_catalog() {
    let client = ready_client().await;

    let secret_id = unique_table_name("anon_secret_hidden");
    let secret_key = "do_not_leak_me";
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', '{secret_key}')"
        ),
    )
    .await
    .unwrap();

    // The secret id and algo are visible; the key must not be.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT id, algo FROM vairedb_catalog.anonymization_secret WHERE id = '{secret_id}'"
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some(secret_id.as_str()));
    assert_eq!(rows[0][1].as_deref(), Some("HMAC-SHA256"));

    // Selecting the secret_key column must fail — it is not part of the view.
    let err = execute_expect_err(
        &client,
        "SELECT secret_key FROM vairedb_catalog.anonymization_secret",
    )
    .await;
    assert!(
        !err.message().contains(secret_key),
        "error must not echo the secret key"
    );
}

#[tokio::test]
async fn test_anonymized_column_too_short_is_rejected() {
    let client = ready_client().await;

    let secret_id = unique_table_name("anon_secret_short");
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', 'k')"
        ),
    )
    .await
    .unwrap();

    // VARCHAR(32) cannot hold the 64-char digest, so CREATE TABLE must fail.
    let tbl = unique_table_name("anon_short");
    let err = execute_expect_err(
        &client,
        &format!(
            "CREATE TABLE {tbl} (id INTEGER NOT NULL, email VARCHAR(32)) \
             WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
             anonymized_columns = [ email -> '{secret_id}' ])"
        ),
    )
    .await;
    assert!(
        err.message().to_lowercase().contains("anonymized")
            || err.message().to_lowercase().contains("64"),
        "expected a column-length error, got: {}",
        err.message()
    );
}

// A pseudonymized column stores a digest and reads back as one, so copying it into
// another pseudonymizing table would hash it a second time — the row would be there
// and no lookup would find it. Both statements that can do this (`INSERT … SELECT`
// and CTAS) are refused, before the source query runs and with nothing created.
#[tokio::test]
async fn test_copying_between_anonymized_tables_is_refused() {
    let client = ready_client().await;

    let secret_id = unique_table_name("anon_secret_copy");
    execute(
        &client,
        &format!(
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) \
             VALUES ('{secret_id}', 'HMAC-SHA256', 'k')"
        ),
    )
    .await
    .unwrap();

    let anon_opts = format!(
        "WITH (shards = 3, replication_factor = 3, shard_by = 'id', \
         anonymized_columns = [ email -> '{secret_id}' ])"
    );
    let src = create_table(
        &client,
        "anon_copy_src",
        &format!("(id INTEGER NOT NULL, email VARCHAR(64)) {anon_opts}"),
    )
    .await;
    let dst = create_table(
        &client,
        "anon_copy_dst",
        &format!("(id INTEGER NOT NULL, email VARCHAR(64)) {anon_opts}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {src} (id, email) VALUES (1, 'a@example.com')"),
    )
    .await
    .unwrap();

    let err = execute_expect_err(
        &client,
        &format!("INSERT INTO {dst} (id, email) SELECT id, email FROM {src}"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "re-hashing digests should be a feature rejection (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert_eq!(
        row_count(&client, &dst).await,
        0,
        "the refused copy must not have written a row"
    );

    // The same rule on the CTAS spelling, where the destination does not exist yet.
    let ctas_dst = unique_table_name("anon_copy_ctas");
    let err = execute_expect_err(
        &client,
        &format!("CREATE TABLE {ctas_dst} {anon_opts} AS SELECT id, email FROM {src}"),
    )
    .await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "a CTAS that would re-hash digests should be refused (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {ctas_dst}"))
            .await
            .is_err(),
        "the refused CTAS must not have created {ctas_dst}"
    );

    // A copy into a table that does not pseudonymize is allowed: the digests are
    // stored as they are, which is the only faithful thing to do with them.
    let plain = create_table(
        &client,
        "anon_copy_plain",
        &format!("(id INTEGER NOT NULL, email VARCHAR(64)) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("INSERT INTO {plain} (id, email) SELECT id, email FROM {src}"),
    )
    .await
    .expect("copying digests into a plain table must be allowed");
    assert_eq!(row_count(&client, &plain).await, 1);

    drop_table(&client, &plain).await;
    drop_table(&client, &dst).await;
    drop_table(&client, &src).await;
}
