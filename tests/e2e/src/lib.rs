use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use xxhash_rust::xxh3::xxh3_64;

pub const PG_HOST: &str = "127.0.0.1";
pub const PG_PORT: u16 = 5432;
pub const EXPECTED_NODES: usize = 5;
pub const MAX_WAIT: Duration = Duration::from_secs(60);
pub const RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The standard table options used by virtually every test: 3 shards, full
/// replication, hash-sharded on `id`.
pub const CREATE_OPTS: &str = "WITH (shards = 3, replication_factor = 3, shard_by = 'id')";

/// Shard count implied by [`CREATE_OPTS`]. Tests that mirror the router's
/// bucketing rely on this matching the `shards` value above.
pub const SHARD_COUNT: usize = 3;

/// Mirror of the write router's shard hash for a table of `shard_count` shards:
/// `xxh3_64(id_str) % shard_count`. Tests on a table not created with
/// [`CREATE_OPTS`] use this to pick ids that land on a known bucket.
pub fn bucket_of_in(id: i64, shard_count: usize) -> u64 {
    xxh3_64(id.to_string().as_bytes()) % shard_count as u64
}

/// Mirror of the write router's shard hash for a [`CREATE_OPTS`] table.
pub fn bucket_of(id: i64) -> u64 {
    bucket_of_in(id, SHARD_COUNT)
}

/// Smallest id `>= start` that hashes to `bucket` on a table of `shard_count`
/// shards.
pub fn id_for_bucket_in(bucket: u64, start: i64, shard_count: usize) -> i64 {
    let mut id = start;
    loop {
        if bucket_of_in(id, shard_count) == bucket {
            return id;
        }
        id += 1;
    }
}

/// Smallest id `>= start` that hashes to `bucket`.
pub fn id_for_bucket(bucket: u64, start: i64) -> i64 {
    id_for_bucket_in(bucket, start, SHARD_COUNT)
}

/// Collect `k` distinct ids (starting from `start`) that all hash to `bucket`.
pub fn ids_in_bucket(bucket: u64, k: usize, start: i64) -> Vec<i64> {
    let mut ids = Vec::with_capacity(k);
    let mut id = start;
    while ids.len() < k {
        if bucket_of(id) == bucket {
            ids.push(id);
        }
        id += 1;
    }
    ids
}

/// Stopping a core only flips its catalog state after the coordinator's
/// `heartbeat_timeout_secs` (15s) elapses, so node-state transitions need a
/// longer deadline than the general `MAX_WAIT`.
pub const NODE_DOWN_WAIT: Duration = Duration::from_secs(45);

pub async fn connect() -> Client {
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    loop {
        match try_connect().await {
            Ok(client) => return client,
            Err(e) => {
                if tokio::time::Instant::now() > deadline {
                    panic!("failed to connect to VaireDB after {MAX_WAIT:?}: {e}");
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

async fn try_connect() -> Result<Client, tokio_postgres::Error> {
    let conn_str = format!("host={PG_HOST} port={PG_PORT} user=test dbname=test");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub async fn wait_for_cluster_ready(client: &Client) {
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    loop {
        match simple_query_rows(
            client,
            "SELECT node_id FROM vairedb_catalog.nodes WHERE state = 'ALIVE'",
        )
        .await
        {
            Ok(rows) if rows.len() >= EXPECTED_NODES => {
                // Allow gRPC channels to stabilize after nodes report ALIVE
                // tokio::time::sleep(Duration::from_secs(2)).await;
                return;
            }
            _ => {
                if tokio::time::Instant::now() > deadline {
                    panic!("cluster did not become ready within {MAX_WAIT:?}");
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

pub async fn simple_query_rows(
    client: &Client,
    query: &str,
) -> Result<Vec<Vec<Option<String>>>, tokio_postgres::Error> {
    let messages = client.simple_query(query).await?;
    let mut rows = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let ncols = row.columns().len();
            let mut values = Vec::with_capacity(ncols);
            for i in 0..ncols {
                values.push(row.get(i).map(|s| s.to_string()));
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

pub async fn execute(client: &Client, sql: &str) -> Result<u64, tokio_postgres::Error> {
    let messages = client.simple_query(sql).await?;
    for msg in messages {
        if let SimpleQueryMessage::CommandComplete(n) = msg {
            return Ok(n);
        }
    }
    Ok(0)
}

/// Connect and block until the 5-node cluster reports ready. Folds the
/// `connect().await` + `wait_for_cluster_ready(&client).await` pair that opens
/// essentially every test.
pub async fn ready_client() -> Client {
    let client = connect().await;
    wait_for_cluster_ready(&client).await;
    client
}

/// Create a fresh, uniquely-named table and return its name. `ddl_tail` is
/// everything after the table name — the column list plus options, e.g.
/// `"(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"` or a custom `WITH (...)`
/// clause. A defensive `DROP TABLE IF EXISTS` runs first so a name leaked by a
/// previously panicking run cannot collide. Panics on failure (tests want that).
pub async fn create_table(client: &Client, prefix: &str, ddl_tail: &str) -> String {
    let tbl = unique_table_name(prefix);
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(client, &format!("CREATE TABLE {tbl} {ddl_tail}"))
        .await
        .unwrap();
    tbl
}

/// Drop a table created by [`create_table`], ignoring the row count. The
/// trailing-cleanup one-liner for the common single-table test lifecycle.
pub async fn drop_table(client: &Client, tbl: &str) {
    execute(client, &format!("DROP TABLE {tbl}")).await.unwrap();
}

/// Run a statement that is expected to fail and return the structured `DbError`
/// so tests can assert on its SQLSTATE (`err.code().code()`) and message. Panics
/// if the statement succeeds or the error carries no `DbError` (e.g. a transport
/// error rather than a server-side error response).
pub async fn execute_expect_err(client: &Client, sql: &str) -> tokio_postgres::error::DbError {
    let err = client
        .simple_query(sql)
        .await
        .expect_err("expected statement to fail");
    err.as_db_error()
        .cloned()
        .unwrap_or_else(|| panic!("error was not a DbError: {err}"))
}

/// `FeatureNotSupported` — the statement parsed but is not one of the routed
/// kinds, so classification rejected it (`unsupported_statement_error`).
pub const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";

/// `SyntaxError` — the statement does not parse under the coordinator's single
/// parser (`datafusion_pg_catalog`'s PostgreSQL-compatibility parser, which
/// tokenizes with sqlparser's `PostgreSqlDialect`). This is where most
/// DuckDB-only syntax fails.
pub const SQLSTATE_SYNTAX_ERROR: &str = "42601";

/// `WrongObjectType` — the name resolved, but to a different kind of object than
/// the statement needs (a view where a table is required, or the reverse). This is
/// what stops `DROP VIEW t` from dropping the table `t`.
pub const SQLSTATE_WRONG_OBJECT_TYPE: &str = "42809";

/// `TableAlreadyExists` — the relation namespace already holds the name, whether as
/// a table, a view, an index, or the physical per-shard name another relation owns.
pub const SQLSTATE_DUPLICATE_TABLE: &str = "42P07";

/// `SchemaNotFound` — the relation's schema does not exist. One level up from a
/// missing table: the table may well exist, just not there.
pub const SQLSTATE_SCHEMA_NOT_FOUND: &str = "3F000";

/// `SchemaAlreadyExists` — `CREATE SCHEMA` on a name that is taken, including
/// `public` and the metadata schemas, which exist without being recorded.
pub const SQLSTATE_SCHEMA_ALREADY_EXISTS: &str = "42P06";

/// `DependentObjectsExist` — `DROP SCHEMA` on a schema that still holds relations.
pub const SQLSTATE_DEPENDENT_OBJECTS_EXIST: &str = "2BP01";

/// `DivisionByZero` — `x / 0`, `x % 0`. PostgreSQL raises; an engine that folds
/// the expression to NULL instead returns a wrong answer silently, which is why
/// the read-path tests pin the SQLSTATE rather than just "some error".
pub const SQLSTATE_DIVISION_BY_ZERO: &str = "22012";

/// `NumericValueOutOfRange` — a value that does not fit the target type, whether
/// from a cast, an overflowing arithmetic result, or a narrowing coercion. The
/// alternative to raising is wrapping, which a client cannot detect.
pub const SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";

/// `InvalidParameterValue` — a syntactically valid argument the function refuses
/// (`ntile(0)`, a negative `LIMIT`, an unrecognized `SET` value).
pub const SQLSTATE_INVALID_PARAMETER_VALUE: &str = "22023";

/// `UndefinedObject` — the name is not one the server has, and the client's mistake
/// is the name itself rather than a missing feature. What `SET`/`SHOW`/`RESET`
/// report for a configuration parameter VaireDB does not model, so a typo stays
/// distinguishable from a refusal.
pub const SQLSTATE_UNDEFINED_OBJECT: &str = "42704";

/// `CantChangeRuntimeParam` — the parameter is reportable but was fixed when the
/// server started (`server_version`, `is_superuser`). PostgreSQL reports the same
/// for its `internal`-context settings.
pub const SQLSTATE_CANT_CHANGE_RUNTIME_PARAM: &str = "55P02";

/// Assert `sql` fails with exactly `sqlstate`, and return the error. Stronger than
/// [`assert_rejected`]: use it where the SQLSTATE is the contract, so a client can
/// tell a missing schema from a missing table without reading the message.
pub async fn assert_sqlstate(
    client: &Client,
    sql: &str,
    sqlstate: &str,
) -> tokio_postgres::error::DbError {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        sqlstate,
        "`{sql}` should carry SQLSTATE {sqlstate} (got {}: {})",
        err.code().code(),
        err.message()
    );
    err
}

/// Assert `sql` is rejected by classification: SQLSTATE `0A000` with the
/// `[VDB-1004]` FeatureNotSupported marker and a message naming the command.
/// Use this for statements the command gap doc pins to the classification
/// rejection point; use [`assert_rejected`] when the doc only says "likely".
pub async fn assert_unsupported(client: &Client, sql: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "`{sql}` should carry SQLSTATE {SQLSTATE_FEATURE_NOT_SUPPORTED} (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("[VDB-1004]"),
        "`{sql}` message should carry the FeatureNotSupported VDB code: {}",
        err.message()
    );
}

/// Assert `sql` is rejected *somehow* and return the error for further
/// inspection. The weaker sibling of [`assert_unsupported`], for statements that
/// may fail at either rejection point (parse `42601` or classification `0A000`)
/// or inside DuckDB. What it pins is the property that matters to a client: an
/// unimplemented statement fails loudly instead of returning a fake `OK` while
/// doing nothing.
pub async fn assert_rejected(client: &Client, sql: &str) -> tokio_postgres::error::DbError {
    execute_expect_err(client, sql).await
}

/// Result-column PostgreSQL types as reported at Describe, without executing the
/// query. This is the read path's type-metadata surface: a wrong Arrow target in
/// `parse_data_type`, or a gap in `arrow_pg::into_pg_type`, shows up here as a
/// wrong OID even when the values themselves come back fine.
pub async fn describe_result_types(client: &Client, sql: &str) -> Vec<Type> {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("Describe failed for `{sql}`: {e}"));
    stmt.columns().iter().map(|c| c.type_().clone()).collect()
}

/// Bind-parameter PostgreSQL types as reported at Describe. Parameter OIDs are
/// inferred from a DataFusion plan over the advertised schema, so they inherit
/// every `parse_data_type` error.
pub async fn describe_param_types(client: &Client, sql: &str) -> Vec<Type> {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("Describe failed for `{sql}`: {e}"));
    stmt.params().to_vec()
}

/// Result-column *names* as reported at Describe. The column label is part of the
/// wire contract a client keys its result map on, and PostgreSQL's labels are not
/// DataFusion's (`sum(t.b)` vs `sum`, and PG truncates at 63 bytes), so the label
/// needs asserting separately from the type.
pub async fn describe_result_labels(client: &Client, sql: &str) -> Vec<String> {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("Describe failed for `{sql}`: {e}"));
    stmt.columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect()
}

/// `SELECT COUNT(*)` on a table, as an integer.
pub async fn row_count(client: &Client, tbl: &str) -> i64 {
    let rows = simple_query_rows(client, &format!("SELECT COUNT(*) FROM {tbl}"))
        .await
        .unwrap_or_else(|e| panic!("COUNT(*) on {tbl} failed: {e}"));
    rows[0][0].as_deref().unwrap().parse().unwrap()
}

static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Per-process seed mixed into every generated table name. Because the counter
/// resets to 0 on every run, without this seed the first table for a prefix is
/// always `..._00000000`; if a test panics before its trailing DROP, that
/// deterministic name leaks and collides at CREATE on the next run. Seeding with
/// process id + wall-clock nanos makes leaked names unique so reruns stay green.
fn run_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        nanos ^ (std::process::id() as u64)
    })
}

pub fn unique_table_name(prefix: &str) -> String {
    let n = TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{:08x}_{n:04x}", run_seed())
}

/// Parsed row from `vairedb_catalog.shards`:
/// `(hash_bucket, primary_node_id, replica_node_ids)`.
pub type ShardRow = (i32, String, Vec<String>);

/// Read the shard layout for a table from the catalog, ordered by hash bucket.
pub async fn fetch_shards(client: &Client, table: &str) -> Vec<ShardRow> {
    let rows = simple_query_rows(
        client,
        &format!(
            "SELECT hash_bucket, primary_node_id, replica_node_ids \
             FROM vairedb_catalog.shards WHERE table_name = '{table}' ORDER BY hash_bucket"
        ),
    )
    .await
    .unwrap();

    rows.iter()
        .map(|r| {
            let bucket: i32 = r[0].as_deref().unwrap().parse().unwrap();
            let primary = r[1].as_deref().unwrap().to_string();
            let replicas: Vec<String> = r[2]
                .as_deref()
                .unwrap_or("")
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            (bucket, primary, replicas)
        })
        .collect()
}

/// Container name for a core node id such as `"core-3"`.
fn core_container(node_id: &str) -> String {
    let suffix = node_id.strip_prefix("core-").unwrap_or(node_id);
    format!("vairedb-e2e-core-{suffix}")
}

/// Stop a core container (e.g. `node_id = "core-3"`). Panics if `docker stop` fails.
pub fn stop_core(node_id: &str) {
    let container = core_container(node_id);
    let status = Command::new("docker")
        .args(["stop", &container])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap_or_else(|e| panic!("failed to invoke docker stop {container}: {e}"));
    assert!(status.success(), "docker stop {container} failed");
}

/// Start a previously stopped core container. Panics if `docker start` fails.
pub fn start_core(node_id: &str) {
    let container = core_container(node_id);
    let status = Command::new("docker")
        .args(["start", &container])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap_or_else(|e| panic!("failed to invoke docker start {container}: {e}"));
    assert!(status.success(), "docker start {container} failed");
}

/// Number of nodes currently in the `ALIVE` state per the catalog.
pub async fn alive_node_count(client: &Client) -> usize {
    simple_query_rows(
        client,
        "SELECT node_id FROM vairedb_catalog.nodes WHERE state = 'ALIVE'",
    )
    .await
    .map(|rows| rows.len())
    .unwrap_or(0)
}

/// Poll `vairedb_catalog.nodes` until `node_id` reaches `expected_state`, or
/// panic after `deadline_after`. Use `NODE_DOWN_WAIT` for transitions that wait
/// on the heartbeat timeout.
pub async fn wait_for_node_state(
    client: &Client,
    node_id: &str,
    expected_state: &str,
    deadline_after: Duration,
) {
    let deadline = tokio::time::Instant::now() + deadline_after;
    loop {
        let rows = simple_query_rows(
            client,
            &format!("SELECT state FROM vairedb_catalog.nodes WHERE node_id = '{node_id}'"),
        )
        .await
        .unwrap_or_default();
        let current = rows.first().and_then(|r| r[0].as_deref());
        if current == Some(expected_state) {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "node {node_id} did not reach state {expected_state} within {deadline_after:?} (last seen {current:?})"
            );
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

/// Poll until reads are consistently complete. Each round fires `SAMPLE_SIZE`
/// back-to-back full scans; a round "converges" only if EVERY scan returns
/// exactly `expected_ids` (order-independent). Rounds repeat until `deadline`.
///
/// A large all-complete batch (not a short streak) is required because reads are
/// routed to a shard replica by an affinity policy: when one replica is stale,
/// only ~1/3 of reads hit it, so a short run of complete reads happens by routing
/// luck even though the stale replica never converges. Empirically ~30% of reads
/// are stale under such a bug, so a batch of `SAMPLE_SIZE` all-complete reads is
/// ~0.7^SAMPLE_SIZE ≈ 1e-6 by luck — negligible — while a truly converged cluster
/// passes the first batch.
pub async fn poll_until_consistent(
    client: &Client,
    tbl: &str,
    expected_ids: &[i64],
    deadline: Duration,
) -> bool {
    const SAMPLE_SIZE: usize = 40;
    let mut want = expected_ids.to_vec();
    want.sort_unstable();

    let end = tokio::time::Instant::now() + deadline;
    loop {
        let mut all_complete = true;
        for _ in 0..SAMPLE_SIZE {
            let rows = simple_query_rows(client, &format!("SELECT id FROM {tbl} ORDER BY id"))
                .await
                .unwrap_or_default();
            let mut got: Vec<i64> = rows
                .iter()
                .filter_map(|r| r[0].as_deref().and_then(|s| s.parse().ok()))
                .collect();
            got.sort_unstable();
            if got != want {
                all_complete = false;
                break;
            }
        }
        if all_complete {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}
