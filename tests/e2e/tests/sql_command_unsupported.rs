mod common;
use common::*;
use tokio_postgres::Client;

// Rows 8-36 of docs/specs/gap-analysis-command.md — every statement the doc
// marks ❌. See `sql_command_select.rs` for the file layout and the
// passing/#[ignore] convention.
//
//     cd tests/e2e && cargo test --test sql_command_unsupported -- --ignored --test-threads=1
//
// Each statement gets up to two tests:
//
//   * `test_<x>_currently_rejected` (passing) — the statement fails loudly. This
//     is the contract that matters to a client TODAY: these statements used to
//     fall through the dispatch catch-all and return a fake `OK` with NO
//     execution, so a client believed a transaction opened or a view was
//     created when nothing happened. A statement may
//     fail at either rejection point — parse (`42601`, most DuckDB-only syntax)
//     or classification (`0A000` + `[VDB-1004]`) — so `assert_unsupported` is
//     used where the doc pins `0A000`, and `assert_rejected` where either is
//     acceptable.
//
//   * `test_<x>_<target behavior>` + `#[ignore]` — the PostgreSQL-correct
//     behavior, written so it fails by construction until the gap closes.
//
// Sections follow the doc's "Open gaps, ranked" order, then the statements it
// leaves unranked, then the two groups it declares 🚫 not planned — the ones with
// a PostgreSQL rewrite, and the single-node DuckDB concerns. Statements in those
// last two groups get NO xfail: there is no target behavior to write, and their
// test exists to keep them rejected.
//
// Some rows are covered by their sibling files instead, because they belong to
// a statement family that file already owns:
//   * row 25 `MERGE INTO` / upsert -> `sql_command_dml.rs`;
//   * `DROP SEQUENCE`, which lands in the DROP TABLE handler, and the object-kind
//     refusals around it -> `sql_command_ddl.rs` (row 7);
//   * `TRUNCATE`, now routed per shard by the same broadcast machinery ->
//     `sql_command_ddl.rs`;
//   * row 33 transaction control, no longer a gap: BEGIN/COMMIT/ROLLBACK/
//     SAVEPOINT are supported via a buffered block -> `sql_command_transaction.rs`.

/// A small two-column table with one row per shard bucket, for the statements
/// that need something to operate on.
async fn setup_rows(client: &Client, tbl: &str) -> Vec<i64> {
    execute(client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .map(|b| id_for_bucket(b, 1))
        .collect();
    for id in &ids {
        execute(
            client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'v{id}')"),
        )
        .await
        .unwrap();
    }
    ids
}

// ============================================================================
// 1. Session configuration — rows 28-31 (SET / RESET / SHOW / SET VARIABLE)
// ============================================================================
//
// The doc's open gap 1, now closed for rows 28/29/31: drivers send `SET`
// (client_encoding, application_name, extra_float_digits, …) at connect time, so a
// rejection here breaks a client before it runs a single query.
//
// What VaireDB accepts is not "everything": a parameter is accepted only where the
// coordinator's own behaviour already matches the value, and refused by name or by
// value otherwise. Recording a value nothing honours would have `SHOW` promise a
// rendering or a resolution rule the read path never applies, which is a wrong
// answer rather than a missing feature. So these tests come in two halves — the
// values a client can rely on, and the ones that must keep failing.

// The specific compatibility shape the doc calls out: the SETs a driver issues on
// connect must not fail the session.
#[tokio::test]
async fn test_driver_startup_sets_are_accepted() {
    let client = ready_client().await;

    for sql in [
        "SET client_encoding TO 'UTF8'",
        "SET application_name = 'vairedb-e2e'",
        "SET extra_float_digits = 3",
        "SET DateStyle TO 'ISO'",
        "SET DateStyle TO 'ISO, MDY'",
        "SET IntervalStyle = 'postgres'",
        "SET standard_conforming_strings = on",
        "SET TimeZone TO 'UTC'",
        "SET TIME ZONE 'Etc/UTC'",
        "SET client_min_messages TO warning",
        "SET statement_timeout = 0",
    ] {
        execute(&client, sql)
            .await
            .unwrap_or_else(|e| panic!("driver startup statement `{sql}` must be accepted: {e}"));
    }
}

#[tokio::test]
async fn test_set_then_show_round_trips() {
    let client = ready_client().await;

    execute(&client, "SET application_name = 'vairedb-e2e'")
        .await
        .unwrap();

    let rows = simple_query_rows(&client, "SHOW application_name")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "SHOW must return exactly one row");
    assert_eq!(
        rows[0][0].as_deref(),
        Some("vairedb-e2e"),
        "SHOW must report the value SET on this session"
    );
}

#[tokio::test]
async fn test_reset_restores_the_default() {
    let client = ready_client().await;

    let default = simple_query_rows(&client, "SHOW application_name")
        .await
        .unwrap()[0][0]
        .clone();

    execute(&client, "SET application_name = 'vairedb-e2e'")
        .await
        .unwrap();
    execute(&client, "RESET application_name").await.unwrap();

    let rows = simple_query_rows(&client, "SHOW application_name")
        .await
        .unwrap();
    assert_eq!(
        rows[0][0], default,
        "RESET must restore the value the session started with"
    );

    // `RESET ALL` is what a connection pooler issues on checkout, so it has to work
    // as a whole-session reset rather than as one more unrecognized statement.
    execute(&client, "SET application_name = 'vairedb-e2e'")
        .await
        .unwrap();
    execute(&client, "RESET ALL").await.unwrap();
    let rows = simple_query_rows(&client, "SHOW application_name")
        .await
        .unwrap();
    assert_eq!(rows[0][0], default, "RESET ALL must discard session values");
}

// The column label is part of the wire contract: a client keys its result map on
// the name `SHOW` sends, and PostgreSQL sends the parameter's canonical spelling
// however the client spelled it in the statement.
#[tokio::test]
async fn test_show_labels_its_column_with_the_canonical_parameter_name() {
    let client = ready_client().await;

    assert_eq!(
        describe_result_labels(&client, "SHOW datestyle").await,
        vec!["DateStyle".to_string()],
    );
    assert_eq!(
        describe_result_labels(&client, "SHOW ALL").await,
        vec![
            "name".to_string(),
            "setting".to_string(),
            "description".to_string()
        ],
    );
}

#[tokio::test]
async fn test_show_all_lists_every_parameter() {
    let client = ready_client().await;

    let rows = simple_query_rows(&client, "SHOW ALL").await.unwrap();
    assert!(
        rows.len() > 10,
        "SHOW ALL should report the whole registry, got {} rows",
        rows.len()
    );
    for row in &rows {
        assert_eq!(row.len(), 3, "SHOW ALL has name/setting/description");
    }
    assert!(
        rows.iter().any(|r| r[0].as_deref() == Some("DateStyle")),
        "SHOW ALL should include DateStyle"
    );
}

// JDBC calls this from `getTransactionIsolation()` before it runs anything, so an
// error here fails a connection outright. The answer is the weakest level because
// that is the one a multi-shard commit can stand behind: it is applied one node
// set at a time, so a concurrent reader can see it half-applied.
#[tokio::test]
async fn test_show_transaction_isolation_level_answers_the_jdbc_probe() {
    let client = ready_client().await;

    let rows = simple_query_rows(&client, "SHOW TRANSACTION ISOLATION LEVEL")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("read uncommitted"));
}

// A runtime parameter touches no relation, so a block has nothing for it to be
// inconsistent with — PostgreSQL allows it, and refusing it would break any ORM
// that configures the session after opening one.
#[tokio::test]
async fn test_set_is_allowed_inside_a_transaction_block() {
    let client = ready_client().await;

    execute(&client, "BEGIN").await.unwrap();
    execute(&client, "SET application_name = 'in-a-block'")
        .await
        .unwrap();
    let rows = simple_query_rows(&client, "SHOW application_name")
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("in-a-block"));
    execute(&client, "COMMIT").await.unwrap();
}

// The other half of the contract. A value the coordinator does not honour is
// refused, and refused with the SQLSTATE that says which of three things went
// wrong — a driver told `0A000` ("SET is unsupported") would stop retrying with a
// value that would have worked.
#[tokio::test]
async fn test_a_value_the_coordinator_does_not_honour_is_refused() {
    let client = ready_client().await;

    // `22023` — the parameter is real, this value is not one VaireDB can honour.
    for sql in [
        // The result encoder emits UTF-8 only.
        "SET client_encoding TO 'LATIN1'",
        // Timestamps are stored and rendered in UTC throughout.
        "SET TimeZone TO 'Europe/Rome'",
        // Nothing cancels a running statement, so a client that set a timeout would
        // wait forever on the query it expected to be aborted.
        "SET statement_timeout = 5000",
        // Both parsers read a backslash in a '...' literal literally.
        "SET standard_conforming_strings = off",
        // Dates are always rendered ISO.
        "SET DateStyle TO 'German'",
        "SET IntervalStyle = 'iso_8601'",
        // Nothing consults a session default when a block opens, so every new
        // transaction would stay writable while the session claimed otherwise.
        "SET default_transaction_read_only = on",
    ] {
        assert_sqlstate(&client, sql, SQLSTATE_INVALID_PARAMETER_VALUE).await;
    }

    // `42704` — no such parameter. The client's mistake is the name, which is what
    // PostgreSQL reports too, so a typo stays distinguishable from a refusal.
    for sql in [
        "SET work_mem = '64MB'",
        "SET searchpath TO public",
        "SHOW work_mem",
        "RESET work_mem",
    ] {
        assert_sqlstate(&client, sql, SQLSTATE_UNDEFINED_OBJECT).await;
    }

    // `55P02` — reportable, but fixed when the server started, as in PostgreSQL.
    for sql in ["SET server_version = '9.5'", "SET is_superuser = off"] {
        assert_sqlstate(&client, sql, SQLSTATE_CANT_CHANGE_RUNTIME_PARAM).await;
    }
}

// `search_path` is the one parameter refused by name rather than by value: VaireDB
// resolves an unqualified relation to the default schema and nothing else — the
// catalog key *is* the qualified name. Recording a search path would leave every
// unqualified name resolving against `public` while the session reported
// otherwise, which is a wrong answer, not a missing feature.
#[tokio::test]
async fn test_search_path_stays_refused() {
    let client = ready_client().await;

    // Refused even for `public`: accepting it would mean accepting the parameter,
    // and the next statement would set something else.
    for sql in ["SET search_path TO myschema", "SET search_path TO public"] {
        assert_unsupported(&client, sql).await;
    }

    // Reportable, though, so a client can see what it is going to get.
    let rows = simple_query_rows(&client, "SHOW search_path")
        .await
        .unwrap();
    assert_eq!(rows[0][0].as_deref(), Some("public"));

    // And resettable: `RESET` asks for the default, and the default is the
    // behaviour VaireDB has, so the OK is true. A pooler that resets on checkout
    // keeps working while `SET` still cannot lie.
    execute(&client, "RESET search_path").await.unwrap();
}

// The SET forms that name something the coordinator does not implement at all —
// an identity, an isolation level, a character set, a transaction-scoped value.
// Each is refused by name, so a client is never left guessing which part of the
// statement was the problem.
#[tokio::test]
async fn test_unmodelled_set_forms_stay_rejected() {
    let client = ready_client().await;

    for sql in [
        // Scoped to the enclosing transaction, which the coordinator's buffered
        // block cannot roll a parameter back with.
        "SET LOCAL application_name = 'x'",
        "SET ROLE readonly",
        "SET SESSION AUTHORIZATION 'someone'",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET NAMES 'UTF8'",
    ] {
        assert_unsupported(&client, sql).await;
    }

    // DuckDB's `SET VARIABLE` (row 30) has no PostgreSQL equivalent, so it is not
    // a gap to close — it fails at parse.
    assert_rejected(&client, "SET VARIABLE my_var = 42").await;
}

// ============================================================================
// 2. COPY — row 14
// ============================================================================
//
// No longer a gap: all four forms work — CSV to and from a file on the
// *coordinator's* filesystem, and CSV to and from the client over the copy
// sub-protocol, which is what `psql \copy` and every driver's bulk loader use.
// What is still refused is everything that is not CSV, and it is refused by name,
// so a client is never left guessing which part of the statement was the problem.

#[tokio::test]
async fn test_copy_outside_csv_is_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_copy");
    setup_rows(&client, &tbl).await;

    // Piping through a shell on the coordinator.
    assert_unsupported(
        &client,
        &format!("COPY {tbl} TO PROGRAM 'cat > /dev/null' (FORMAT CSV)"),
    )
    .await;

    // Encodings other than CSV, and CSV that is only implied: PostgreSQL's default
    // is its own TEXT encoding, so a statement that does not say CSV is not one.
    assert_unsupported(
        &client,
        &format!("COPY {tbl} TO '/tmp/{tbl}.bin' (FORMAT BINARY)"),
    )
    .await;
    assert_unsupported(&client, &format!("COPY {tbl} TO '/tmp/{tbl}.txt'")).await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_copy_to_file_then_back_round_trips() {
    let client = ready_client().await;
    let src = unique_table_name("un_copyx_src");
    let ids = setup_rows(&client, &src).await;

    // Server-side path, inside the coordinator container.
    let path = format!("/tmp/{src}.csv");
    execute(
        &client,
        &format!("COPY {src} TO '{path}' (FORMAT CSV, HEADER)"),
    )
    .await
    .unwrap();

    let dst = create_table(
        &client,
        "un_copyx_dst",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!("COPY {dst} FROM '{path}' (FORMAT CSV, HEADER)"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids;
    want.sort_unstable();
    assert_eq!(
        got, want,
        "COPY must export every shard's rows and re-import them exactly once"
    );

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
}

// The streaming forms, which are the ones a client can actually reach: the file
// forms need a path on the coordinator's own container, so `psql \copy` and every
// driver's bulk loader use `STDIN`/`STDOUT` instead.
//
// The data is deliberately sent in pieces that do not line up with its rows —
// which is what a real client does, since a `CopyData` message boundary falls
// wherever its buffer ended. The import must not care.
#[tokio::test]
async fn test_copy_from_stdin_streams_rows_to_every_shard() {
    use futures::SinkExt;

    let client = ready_client().await;
    let tbl = unique_table_name("un_copyin");
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    // Enough rows to land on all three shards, with a quoted value carrying the
    // delimiter so the split cannot be a naive one.
    let ids: Vec<i64> = (0..SHARD_COUNT as u64)
        .flat_map(|b| ids_in_bucket(b, 4, 1))
        .collect();
    let mut csv = String::from("id,v\n");
    for id in &ids {
        csv.push_str(&format!("{id},\"v,{id}\"\n"));
    }

    let sink = client
        .copy_in(&format!("COPY {tbl} FROM STDIN (FORMAT CSV, HEADER)"))
        .await
        .expect("the copy must open");
    let mut sink = Box::pin(sink);
    // 7 bytes at a time: every boundary lands mid-row, and some land inside the
    // quoted field.
    for piece in csv.as_bytes().chunks(7) {
        sink.send(bytes::Bytes::copy_from_slice(piece))
            .await
            .expect("a CopyData message must be accepted");
    }
    let written = sink
        .as_mut()
        .finish()
        .await
        .expect("the copy must complete");
    assert_eq!(
        written,
        ids.len() as u64,
        "COPY must report the rows it wrote"
    );

    let rows = simple_query_rows(&client, &format!("SELECT id, v FROM {tbl} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids.clone();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "every row must be routed to its shard and stored exactly once"
    );
    assert!(
        rows.iter()
            .all(|r| r[1].as_deref().unwrap().starts_with("v,")),
        "the quoted delimiter must survive the import: {rows:?}"
    );

    drop_table(&client, &tbl).await;
}

// `TO STDOUT` gathers every shard through the read path and streams the result to
// the client, which is the export half of the same round trip.
#[tokio::test]
async fn test_copy_to_stdout_streams_every_shards_rows() {
    use futures::TryStreamExt;

    let client = ready_client().await;
    let tbl = unique_table_name("un_copyout");
    let ids = setup_rows(&client, &tbl).await;

    let stream = client
        .copy_out(&format!("COPY {tbl} TO STDOUT (FORMAT CSV, HEADER)"))
        .await
        .expect("the copy must open");
    let chunks: Vec<bytes::Bytes> = stream.try_collect().await.expect("the copy must complete");
    let csv = chunks.concat();
    let csv = String::from_utf8(csv).expect("CSV is text");

    let mut lines = csv.lines();
    assert_eq!(
        lines.next(),
        Some("id,v"),
        "HEADER must name the columns: {csv:?}"
    );
    let mut got: Vec<i64> = lines
        .map(|l| l.split(',').next().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids;
    want.sort_unstable();
    assert_eq!(got, want, "the export must gather every shard's rows once");

    drop_table(&client, &tbl).await;
}

// The round trip a client can drive on its own, with no coordinator-side path
// involved: out of one table over the wire and back into another.
#[tokio::test]
async fn test_copy_stdout_to_stdin_round_trips() {
    use futures::{SinkExt, TryStreamExt};

    let client = ready_client().await;
    let src = unique_table_name("un_copyrt_src");
    let ids = setup_rows(&client, &src).await;

    let stream = client
        .copy_out(&format!("COPY {src} TO STDOUT (FORMAT CSV, HEADER)"))
        .await
        .unwrap();
    let exported: Vec<bytes::Bytes> = stream.try_collect().await.unwrap();

    let dst = create_table(
        &client,
        "un_copyrt_dst",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let sink = client
        .copy_in(&format!("COPY {dst} FROM STDIN (FORMAT CSV, HEADER)"))
        .await
        .unwrap();
    let mut sink = Box::pin(sink);
    for chunk in exported {
        sink.send(chunk).await.unwrap();
    }
    let written = sink.as_mut().finish().await.unwrap();
    assert_eq!(written, ids.len() as u64);

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {dst} ORDER BY id"))
        .await
        .unwrap();
    let mut got: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    got.sort_unstable();
    let mut want = ids;
    want.sort_unstable();
    assert_eq!(got, want, "a client-side round trip must lose nothing");

    drop_table(&client, &src).await;
    drop_table(&client, &dst).await;
}

// A streaming import is refused before the client is invited to send anything: a
// bulk loader should not upload a gigabyte for a copy that could never work.
#[tokio::test]
async fn test_a_streaming_import_that_cannot_work_is_refused_up_front() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_copyrej");
    setup_rows(&client, &tbl).await;

    for sql in [
        // Every row is routed by its shard key, so data without it has nowhere to go.
        format!("COPY {tbl} (v) FROM STDIN (FORMAT CSV)"),
        format!("COPY {tbl} (id, nope) FROM STDIN (FORMAT CSV)"),
        format!("COPY {tbl} FROM STDIN"),
    ] {
        assert!(
            client.copy_in::<str, bytes::Bytes>(&sql).await.is_err(),
            "`{sql}` must be refused before any data is sent"
        );
    }

    // The connection is still usable: a refused COPY must not leave it in copy mode.
    let rows = simple_query_rows(&client, &format!("SELECT count(*) FROM {tbl}"))
        .await
        .expect("the connection must survive a refused COPY");
    assert_eq!(rows[0][0].as_deref(), Some("3"));

    drop_table(&client, &tbl).await;
}

// An empty transfer is an empty import, which is what PostgreSQL answers too.
#[tokio::test]
async fn test_an_empty_streaming_import_writes_nothing() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_copyempty");
    execute(&client, &format!("DROP TABLE IF EXISTS {tbl}"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();

    // Nothing is ever sent, so the payload type has to be named outright.
    let sink = client
        .copy_in::<str, bytes::Bytes>(&format!("COPY {tbl} FROM STDIN (FORMAT CSV, HEADER)"))
        .await
        .unwrap();
    let mut sink = Box::pin(sink);
    assert_eq!(
        sink.as_mut().finish().await.unwrap(),
        0,
        "a copy that sent nothing writes nothing"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 3. Views — rows 20 (CREATE VIEW), 8 (ALTER VIEW) and 7 (DROP VIEW)
// ============================================================================
//
// Closed: a view is stored in the coordinator catalog as
// its query text and inlined as a CTE on every read, so it is never stale and
// nothing is broadcast to a shard. What stays refused is what a coordinator-local
// view cannot honestly be — see `test_view_forms_still_refused`.

/// The forms that remain refused, each for a reason a client can act on:
/// `MATERIALIZED` would need rows the coordinator does not hold, the dialect
/// decorations would be stored and ignored, and a view over a metadata schema is
/// answered from a context that never expands views.
#[tokio::test]
async fn test_view_forms_still_refused() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewbase");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_view");

    for sql in [
        format!("CREATE MATERIALIZED VIEW {view} AS SELECT id FROM {tbl}"),
        format!("CREATE TEMPORARY VIEW {view} AS SELECT id FROM {tbl}"),
        format!("CREATE VIEW {view} WITH (security_barrier = true) AS SELECT id FROM {tbl}"),
        format!("CREATE VIEW {view} AS SELECT relname FROM pg_catalog.pg_class"),
    ] {
        assert_unsupported(&client, &sql).await;
    }

    // `ALTER VIEW … RENAME TO` fails earlier, at parse (42601): sqlparser's
    // PostgreSqlDialect only accepts `ALTER VIEW … AS <query>`.
    assert_rejected(&client, &format!("ALTER VIEW {view} RENAME TO {view}_2")).await;

    // Nothing was created, so the view name does not resolve.
    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .is_err(),
        "a rejected CREATE VIEW must not leave a queryable relation"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_view_lifecycle_create_select_drop() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewx_base");
    let ids = setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewx");

    // A view over a sharded table must read through to every shard.
    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id, v FROM {tbl}"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {view} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(rows.len(), ids.len(), "the view must expose every base row");

    // A filtering view narrows the result.
    let filtered = unique_table_name("un_viewx_filtered");
    let one = ids[0];
    execute(
        &client,
        &format!("CREATE VIEW {filtered} AS SELECT id FROM {tbl} WHERE id = {one}"),
    )
    .await
    .unwrap();
    let rows = simple_query_rows(&client, &format!("SELECT id FROM {filtered}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some(one.to_string().as_str()));

    // DROP VIEW removes it without touching the base table.
    execute(&client, &format!("DROP VIEW {filtered}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    drop_table(&client, &tbl).await;
}

/// A view may read another view: the coordinator inlines both, innermost first.
#[tokio::test]
async fn test_a_view_reads_another_view() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewnest_base");
    let ids = setup_rows(&client, &tbl).await;
    let inner = unique_table_name("un_viewnest_inner");
    let outer = unique_table_name("un_viewnest_outer");

    execute(
        &client,
        &format!("CREATE VIEW {inner} AS SELECT id, v FROM {tbl}"),
    )
    .await
    .unwrap();
    execute(
        &client,
        &format!("CREATE VIEW {outer} AS SELECT id FROM {inner} WHERE id >= 0"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("SELECT id FROM {outer} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        ids.len(),
        "the outer view must read through the inner one to every shard"
    );

    execute(&client, &format!("DROP VIEW {outer}"))
        .await
        .unwrap();
    execute(&client, &format!("DROP VIEW {inner}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

/// Both spellings that redefine a view in place: `CREATE OR REPLACE VIEW` and
/// `ALTER VIEW … AS`. Neither may create a view that does not already exist under
/// ALTER, and both must be visible to the very next read.
#[tokio::test]
async fn test_a_view_can_be_redefined_in_place() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewredef_base");
    let ids = setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewredef");
    let one = ids[0];

    // ALTER VIEW on a name nothing holds is refused, not silently a create.
    assert_rejected(
        &client,
        &format!("ALTER VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await;

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .unwrap()
            .len(),
        ids.len()
    );

    // CREATE OR REPLACE narrows it.
    execute(
        &client,
        &format!("CREATE OR REPLACE VIEW {view} AS SELECT id FROM {tbl} WHERE id = {one}"),
    )
    .await
    .unwrap();
    assert_eq!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .unwrap()
            .len(),
        1
    );

    // ALTER VIEW … AS widens it again.
    execute(
        &client,
        &format!("ALTER VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();
    assert_eq!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .unwrap()
            .len(),
        ids.len()
    );

    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

/// A view is read-only and is not a table: every statement that needs real rows or
/// a real relation must say which object kind it found (42809) rather than fail
/// somewhere deeper — and `DROP VIEW` naming a table must not drop the table.
#[tokio::test]
async fn test_a_view_is_not_a_table_by_object_kind() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewkind_base");
    let ids = setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewkind");

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id, v FROM {tbl}"),
    )
    .await
    .unwrap();

    for sql in [
        format!("INSERT INTO {view} (id, v) VALUES (1, 'x')"),
        format!("UPDATE {view} SET v = 'x' WHERE id = {}", ids[0]),
        format!("DELETE FROM {view} WHERE id = {}", ids[0]),
        format!("TRUNCATE TABLE {view}"),
        format!("DROP TABLE {view}"),
        format!("ALTER TABLE {view} ADD COLUMN extra INTEGER"),
        format!("CREATE INDEX {view}_idx ON {view} (id)"),
    ] {
        let err = assert_rejected(&client, &sql).await;
        assert_eq!(
            err.code().code(),
            SQLSTATE_WRONG_OBJECT_TYPE,
            "`{sql}` must report the object kind it found, got: {err}"
        );
        assert!(
            err.message().contains("is a view"),
            "`{sql}` must name the kind in its message, got: {}",
            err.message()
        );
    }

    // The mirror image: DROP VIEW naming a table refuses and leaves it whole.
    let err = assert_rejected(&client, &format!("DROP VIEW {tbl}")).await;
    assert_eq!(err.code().code(), SQLSTATE_WRONG_OBJECT_TYPE);
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    // And the view survived every refusal above.
    assert_eq!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .unwrap()
            .len(),
        ids.len()
    );

    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

/// A view lives only in the coordinator catalog, so `vairedb_catalog.views` is the
/// only place its definition can be read back.
#[tokio::test]
async fn test_the_catalog_reports_a_views_definition() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewcat_base");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewcat");

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT definition FROM vairedb_catalog.views WHERE view_name = '{}'",
            view.to_lowercase()
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "the view must appear in the catalog");
    let definition = rows[0][0].clone().unwrap_or_default();
    assert!(
        definition.contains(&tbl.to_lowercase()) || definition.contains(&tbl),
        "the stored definition must be the query text, got: {definition}"
    );

    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT definition FROM vairedb_catalog.views WHERE view_name = '{}'",
            view.to_lowercase()
        ),
    )
    .await
    .unwrap();
    assert!(rows.is_empty(), "DROP VIEW must forget the definition");

    drop_table(&client, &tbl).await;
}

/// View DDL touches the catalog at once, so a transaction block could not roll it
/// back — it is refused with that reason rather than half-applied.
#[tokio::test]
async fn test_view_ddl_inside_a_transaction_block_is_refused() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_viewtx_base");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_viewtx");

    execute(&client, "BEGIN").await.unwrap();
    assert_unsupported(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await;
    execute(&client, "ROLLBACK").await.unwrap();

    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .is_err(),
        "the refused CREATE VIEW must not have been applied"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
#[ignore = "gap (row 8): ALTER VIEW … RENAME TO does not parse under sqlparser's PostgreSqlDialect (42601) — only `ALTER VIEW … AS <query>` is accepted, which `test_a_view_can_be_redefined_in_place` covers"]
async fn test_alter_view_renames_the_view() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_alterview_base");
    setup_rows(&client, &tbl).await;
    let view = unique_table_name("un_alterview");
    let renamed = format!("{view}_r");

    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();
    execute(&client, &format!("ALTER VIEW {view} RENAME TO {renamed}"))
        .await
        .unwrap();

    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {renamed}"))
            .await
            .is_ok(),
        "the renamed view must resolve"
    );
    assert!(
        simple_query_rows(&client, &format!("SELECT id FROM {view}"))
            .await
            .is_err(),
        "the old view name must stop resolving"
    );

    execute(&client, &format!("DROP VIEW {renamed}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

// ============================================================================
// 4. EXPLAIN and DESCRIBE — rows 27 and 22 (CLOSED)
// ============================================================================
//
// The doc's open gap 2, now closed: both are answered on the read path, because a
// SELECT already builds the DataFusion `LogicalPlan` an `EXPLAIN` reports and
// already resolves the schema a `DESCRIBE` reports.
//
// What matters here is that the answer is *true*: the query inside an `EXPLAIN`
// goes through the same preparation a bare SELECT does, so the plan shown is the
// plan VaireDB would run, per-shard scans included. So these tests come in two
// halves, like the SET ones — what a client is told, and the forms that must keep
// failing because they would be a plausible answer to something VaireDB never does.

#[tokio::test]
async fn test_profiling_pragmas_stay_rejected() {
    let client = ready_client().await;

    // Profiling PRAGMAs share row 27 with EXPLAIN, and stay refused: a PRAGMA is a
    // DuckDB-only statement, and one accepted here would configure a single shard's
    // engine while the client believes it configured the database.
    assert_rejected(&client, "PRAGMA enable_profiling").await;
    assert_rejected(&client, "PRAGMA database_list").await;
}

/// The forms that would answer with something VaireDB cannot know. Each names what
/// it refused, so a client can tell a missing form from a missing feature.
#[tokio::test]
async fn test_untruthful_explain_forms_are_refused() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainr");
    setup_rows(&client, &tbl).await;

    // A write is not planned in the coordinator at all — it is rendered back to SQL
    // and planned by DuckDB on each shard — so a DataFusion plan for one would
    // describe execution that never happens.
    assert_unsupported(
        &client,
        &format!("EXPLAIN INSERT INTO {tbl} (id, v) VALUES (1, 'a')"),
    )
    .await;
    assert_unsupported(&client, &format!("EXPLAIN UPDATE {tbl} SET v = 'a'")).await;
    assert_unsupported(&client, &format!("EXPLAIN DELETE FROM {tbl}")).await;

    // Options that would change the output a client parses. Accepting and ignoring
    // one would return an indent-format plan to a client that asked for JSON.
    assert_unsupported(
        &client,
        &format!("EXPLAIN (FORMAT JSON) SELECT id FROM {tbl}"),
    )
    .await;
    assert_unsupported(
        &client,
        &format!("EXPLAIN (COSTS false) SELECT id FROM {tbl}"),
    )
    .await;
    assert_unsupported(&client, &format!("EXPLAIN (BUFFERS) SELECT id FROM {tbl}")).await;

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_explain_returns_a_plan() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainx");
    setup_rows(&client, &tbl).await;

    let plan = plan_text(
        &client,
        &format!("EXPLAIN SELECT id FROM {tbl} WHERE id > 0"),
    )
    .await;
    assert!(
        plan.contains(&tbl),
        "the plan should name the relation being scanned, got:\n{plan}"
    );

    drop_table(&client, &tbl).await;
}

/// The plan a client is shown must be the one the read path builds, which means it
/// has to name the distributed scan — an `EXPLAIN` that reported a plain table scan
/// would hide the only interesting thing about how VaireDB runs a query.
#[tokio::test]
async fn test_explain_shows_the_distributed_scan() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explaind");
    setup_rows(&client, &tbl).await;

    let plan = plan_text(
        &client,
        &format!("EXPLAIN SELECT id FROM {tbl} WHERE id > 0"),
    )
    .await;
    assert!(
        plan.contains("physical_plan"),
        "the plan should report a physical stage, got:\n{plan}"
    );
    assert!(
        plan.contains("logical_plan"),
        "the plan should report a logical stage, got:\n{plan}"
    );

    drop_table(&client, &tbl).await;
}

/// The column label is part of the wire contract: a client's plan display keys on
/// PostgreSQL's `QUERY PLAN`, not on DataFusion's `plan_type`/`plan` pair.
#[tokio::test]
async fn test_explain_uses_the_postgres_column_shape() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainl");
    setup_rows(&client, &tbl).await;

    assert_eq!(
        describe_result_labels(&client, &format!("EXPLAIN SELECT id FROM {tbl}")).await,
        vec!["QUERY PLAN".to_string()],
        "EXPLAIN must return PostgreSQL's single QUERY PLAN column"
    );
    // Describe and Execute have to agree: the label is derived from the plan at
    // Parse and the rows are reshaped at Execute, and a client that was told one
    // column and sent two would fail to decode the row.
    let rows = simple_query_rows(&client, &format!("EXPLAIN SELECT id FROM {tbl}"))
        .await
        .unwrap();
    assert!(!rows.is_empty(), "EXPLAIN must return plan rows");
    for row in &rows {
        assert_eq!(row.len(), 1, "every plan row has exactly one column");
    }

    drop_table(&client, &tbl).await;
}

/// A view is not a relation any shard holds — it is a definition the coordinator
/// inlines while preparing the query — so explaining one is the test that the
/// *prepared* query is what gets planned.
#[tokio::test]
async fn test_explain_expands_a_view() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainv");
    let view = unique_table_name("un_explainvv");
    setup_rows(&client, &tbl).await;
    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl} WHERE id > 0"),
    )
    .await
    .unwrap();

    let plan = plan_text(&client, &format!("EXPLAIN SELECT id FROM {view}")).await;
    assert!(
        plan.contains(&tbl),
        "the plan should name the table the view reads, not just the view: got\n{plan}"
    );

    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

/// `EXPLAIN ANALYZE` runs the query, which is the whole point of it: it is the one
/// form that reports what execution actually did rather than what it would do.
#[tokio::test]
async fn test_explain_analyze_reports_execution() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_explainax");
    setup_rows(&client, &tbl).await;

    let plan = plan_text(&client, &format!("EXPLAIN ANALYZE SELECT id FROM {tbl}")).await;
    assert!(
        !plan.is_empty(),
        "EXPLAIN ANALYZE must return execution output"
    );
    assert!(
        plan.contains("metrics") || plan.contains("Plan with Metrics"),
        "EXPLAIN ANALYZE should report execution metrics, got:\n{plan}"
    );

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_describe_lists_the_columns() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_describex");
    setup_rows(&client, &tbl).await;

    let rows = simple_query_rows(&client, &format!("DESCRIBE {tbl}"))
        .await
        .unwrap();
    let mut names: Vec<String> = rows
        .iter()
        .filter_map(|r| r[0].as_deref().map(|s| s.to_string()))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["id".to_string(), "v".to_string()],
        "DESCRIBE must list one row per column"
    );

    drop_table(&client, &tbl).await;
}

/// `DESCRIBE <relation>` is answered by describing `SELECT * FROM <relation>`, so a
/// view — which no shard holds — is described by the query it stands for, and a
/// `DESCRIBE <query>` reaches the same path directly.
#[tokio::test]
async fn test_describe_reaches_a_view_and_a_query() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_describev");
    let view = unique_table_name("un_describevv");
    setup_rows(&client, &tbl).await;
    execute(
        &client,
        &format!("CREATE VIEW {view} AS SELECT id FROM {tbl}"),
    )
    .await
    .unwrap();

    let rows = simple_query_rows(&client, &format!("DESCRIBE {view}"))
        .await
        .unwrap();
    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| r[0].as_deref().map(|s| s.to_string()))
        .collect();
    assert_eq!(
        names,
        vec!["id".to_string()],
        "DESCRIBE of a view must report the view's own columns"
    );

    let rows = simple_query_rows(&client, &format!("DESCRIBE SELECT v FROM {tbl}"))
        .await
        .unwrap();
    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| r[0].as_deref().map(|s| s.to_string()))
        .collect();
    assert_eq!(
        names,
        vec!["v".to_string()],
        "DESCRIBE of a query must report the query's columns"
    );

    execute(&client, &format!("DROP VIEW {view}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

/// Every plan row joined into one string, which is how a plan is read: PostgreSQL
/// returns it one line per row, so an assertion about the plan as a whole has to
/// reassemble it.
async fn plan_text(client: &Client, sql: &str) -> String {
    let rows = simple_query_rows(client, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should return a plan: {e}"));
    assert!(!rows.is_empty(), "`{sql}` must return at least one row");
    rows.iter()
        .filter_map(|r| r[r.len() - 1].as_deref())
        .collect::<Vec<_>>()
        .join("\n")
}

// ============================================================================
// 5. MERGE INTO / upsert — row 25 (CLOSED)
// ============================================================================
//
// Closed. MERGE is no longer refused by name: it runs whenever
// the ON clause equates the target's shard key with a source column and the
// source is either a table sharded identically or an inline `VALUES` list.
// Tested in `sql_command_dml.rs` alongside INSERT, since a merge is DML and
// shares the shard-key routing constraints — see `test_merge_into_*` there,
// `test_merge_unsupported_shapes_are_rejected` for what stays refused, and the
// `ON CONFLICT` pair.

// ============================================================================
// 6. Indexes — row 15 (CLOSED)
// ============================================================================
//
// Closed. `CREATE INDEX` / `DROP INDEX` now fan out to one real
// per-shard index each, so the pair below no longer carries `#[ignore]`. What
// stays refused is the part a sharded store cannot honor: a UNIQUE index that
// does not cover the shard key, since equal values would land on different
// shards and no shard could see the duplicate. `DROP INDEX` is also row 7 (see
// `sql_command_ddl.rs`).

// The index shape a shard cannot enforce or a coordinator cannot track. Each of
// these used to be the whole of row 15; keeping them pinned stops the refusal
// from quietly widening back into "accepted, silently wrong".
#[tokio::test]
async fn test_index_shapes_that_stay_refused() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_index");
    setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idx");

    // UNIQUE off the shard key: `v` values that collide live on different
    // shards, so no shard's index would ever see the duplicate.
    assert_unsupported(&client, &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (v)")).await;
    // A UNIQUE index narrowed to a subset of rows, or to NULLs-collide
    // semantics, cannot be reasoned about per shard either.
    assert_unsupported(
        &client,
        &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (id) WHERE id > 0"),
    )
    .await;
    assert_unsupported(
        &client,
        &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (id) NULLS NOT DISTINCT"),
    )
    .await;
    // An unnamed index gets a server-chosen name per shard, which the
    // coordinator could not then map back to one client-visible index.
    assert_unsupported(&client, &format!("CREATE INDEX ON {tbl} (v)")).await;
    // An expression index cannot be checked against the columns an ALTER TABLE
    // is about to move.
    assert_unsupported(&client, &format!("CREATE INDEX {idx} ON {tbl} (lower(v))")).await;

    // Nothing above created anything: the name is still free.
    execute(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)"))
        .await
        .unwrap();
    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();

    drop_table(&client, &tbl).await;
}

// An index name shares the relation namespace with tables, as in PostgreSQL, so
// each statement must refuse the other kind's name rather than acting on it.
#[tokio::test]
async fn test_index_and_table_names_share_one_namespace() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_idxns");
    setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idxns_i");

    execute(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)"))
        .await
        .unwrap();

    // The table's name is taken, so the index cannot claim it.
    let err = assert_rejected(&client, &format!("CREATE INDEX {tbl} ON {tbl} (id)")).await;
    assert_eq!(
        err.code().code(),
        "42P07",
        "an index named after an existing table must report already-exists: {}",
        err.message()
    );
    // Re-creating the same index reports the same way, and `IF NOT EXISTS`
    // makes it a no-op.
    assert_rejected(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)")).await;
    execute(
        &client,
        &format!("CREATE INDEX IF NOT EXISTS {idx} ON {tbl} (v)"),
    )
    .await
    .unwrap();

    // And `DROP INDEX` must not be a way to drop a table.
    let err = assert_rejected(&client, &format!("DROP INDEX {tbl}")).await;
    assert_eq!(
        err.code().code(),
        "42809",
        "DROP INDEX on a table must report the wrong object type: {}",
        err.message()
    );
    assert_eq!(row_count(&client, &tbl).await, SHARD_COUNT as i64);

    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();
    drop_table(&client, &tbl).await;
}

// A table that carries an index cannot have its columns dropped, renamed, retyped
// or made (non-)nullable, and cannot be renamed. The shards' engine refuses to
// alter a table an index depends on — per *table*, not per indexed column —
// so refusing at the coordinator is what makes the error name the index instead of
// blaming the cluster, and keeps the catalog from listing an index over a column
// that moved.
#[tokio::test]
async fn test_altering_an_indexed_column_names_the_index() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_idxalter");
    setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idxalter_i");

    execute(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)"))
        .await
        .unwrap();

    // Adding a column is metadata-only, so it is allowed with the index in place.
    execute(
        &client,
        &format!("ALTER TABLE {tbl} ADD COLUMN extra INTEGER"),
    )
    .await
    .unwrap();

    for sql in [
        // The indexed column itself.
        format!("ALTER TABLE {tbl} DROP COLUMN v"),
        format!("ALTER TABLE {tbl} RENAME COLUMN v TO w"),
        format!("ALTER TABLE {tbl} ALTER COLUMN v TYPE TEXT"),
        // And a column the index does not cover, which the engine blocks just the
        // same.
        format!("ALTER TABLE {tbl} DROP COLUMN extra"),
        format!("ALTER TABLE {tbl} RENAME COLUMN extra TO extra2"),
        format!("ALTER TABLE {tbl} ALTER COLUMN extra TYPE BIGINT"),
        format!("ALTER TABLE {tbl} ALTER COLUMN extra SET NOT NULL"),
        // Renaming the table depends on it too.
        format!("ALTER TABLE {tbl} RENAME TO {tbl}_moved"),
    ] {
        let err = assert_rejected(&client, &sql).await;
        assert!(
            err.message().contains(&idx),
            "`{sql}` should name the index blocking it: {}",
            err.message()
        );
    }

    // Dropping the index unblocks all of it.
    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();
    execute(&client, &format!("ALTER TABLE {tbl} DROP COLUMN extra"))
        .await
        .unwrap();
    execute(&client, &format!("ALTER TABLE {tbl} DROP COLUMN v"))
        .await
        .unwrap();

    drop_table(&client, &tbl).await;
}

#[tokio::test]
async fn test_create_index_then_drop_index() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_indexx");
    let ids = setup_rows(&client, &tbl).await;
    let idx = unique_table_name("un_idxx");

    execute(&client, &format!("CREATE INDEX {idx} ON {tbl} (v)"))
        .await
        .unwrap();

    // Creating an index changes performance, not results.
    assert_eq!(row_count(&client, &tbl).await, ids.len() as i64);

    execute(&client, &format!("DROP INDEX {idx}"))
        .await
        .unwrap();

    drop_table(&client, &tbl).await;
}

// A UNIQUE index on the SHARD KEY is the one uniqueness constraint a sharded
// store can enforce without cross-shard coordination: equal keys always hash to
// the same shard, so per-shard enforcement is globally correct.
#[tokio::test]
async fn test_unique_index_on_shard_key_is_enforced() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_uniqidx",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;
    let idx = unique_table_name("un_uniqidx_i");

    execute(&client, &format!("CREATE UNIQUE INDEX {idx} ON {tbl} (id)"))
        .await
        .unwrap();
    execute(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'first')"),
    )
    .await
    .unwrap();

    assert_rejected(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, 'dup')"),
    )
    .await;
    assert_eq!(row_count(&client, &tbl).await, 1);

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 7. Schemas — row 17
// ============================================================================
//
// `CREATE SCHEMA` / `DROP SCHEMA` are supported: a schema is a namespace in the
// coordinator catalog, and a relation's key carries it, so `schema_a.t` and
// `schema_b.t` are two relations with two shard layouts.
// `identifier_rewrite.rs::test_schema_qualified_name_collision` covers that from
// the naming side; this is the statement side. What stays refused is what a pure
// namespace cannot honor: the clauses that give a schema properties or an owner,
// `CASCADE`, and `ALTER SCHEMA`, which the parser does not accept at all.

#[tokio::test]
async fn test_schema_ddl_refusals() {
    let client = ready_client().await;
    let schema = unique_table_name("un_schema");

    // `ALTER SCHEMA` is not in sqlparser's PostgreSqlDialect at all: it fails at
    // parse (42601, "expected one of VIEW or TYPE or TABLE or INDEX …").
    assert_rejected(
        &client,
        &format!("ALTER SCHEMA {schema} RENAME TO {schema}_2"),
    )
    .await;

    // A schema has no owner (VaireDB has no roles) and no properties, so each of
    // these is refused by name rather than accepted and dropped.
    assert_unsupported(
        &client,
        &format!("CREATE SCHEMA {schema} AUTHORIZATION bob"),
    )
    .await;
    assert_unsupported(&client, "CREATE SCHEMA AUTHORIZATION bob").await;

    // The default namespace and the metadata namespaces exist without being
    // recorded: they can be neither created nor dropped.
    assert_sqlstate(
        &client,
        "CREATE SCHEMA public",
        SQLSTATE_SCHEMA_ALREADY_EXISTS,
    )
    .await;
    assert_unsupported(&client, "DROP SCHEMA public").await;
    assert_unsupported(&client, "DROP SCHEMA pg_catalog").await;
    assert_unsupported(&client, "DROP SCHEMA information_schema").await;
    // `IF NOT EXISTS` on a namespace that exists is success, as in PostgreSQL.
    execute(&client, "CREATE SCHEMA IF NOT EXISTS public")
        .await
        .unwrap();

    // A relation cannot be created in a namespace nobody asked for.
    assert_sqlstate(
        &client,
        &format!("CREATE TABLE {schema}.t (id INTEGER NOT NULL) {CREATE_OPTS}"),
        SQLSTATE_SCHEMA_NOT_FOUND,
    )
    .await;
    assert_sqlstate(
        &client,
        &format!("CREATE VIEW {schema}.v AS SELECT 1 AS one"),
        SQLSTATE_SCHEMA_NOT_FOUND,
    )
    .await;
    assert_sqlstate(
        &client,
        &format!("DROP SCHEMA {schema}"),
        SQLSTATE_SCHEMA_NOT_FOUND,
    )
    .await;
    // `IF EXISTS` turns that into success.
    execute(&client, &format!("DROP SCHEMA IF EXISTS {schema}"))
        .await
        .unwrap();

    execute(&client, &format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    // A second CREATE reports the name is taken; `IF NOT EXISTS` does not.
    assert_sqlstate(
        &client,
        &format!("CREATE SCHEMA {schema}"),
        SQLSTATE_SCHEMA_ALREADY_EXISTS,
    )
    .await;
    execute(&client, &format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();

    // Dropping the relations inside the schema is one shard fan-out per table and
    // cannot be undone part-way, so CASCADE is refused instead of approximated —
    // and a non-empty schema is refused rather than half-dropped.
    execute(
        &client,
        &format!("CREATE TABLE {schema}.t (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await
    .unwrap();
    assert_unsupported(&client, &format!("DROP SCHEMA {schema} CASCADE")).await;
    let err = assert_sqlstate(
        &client,
        &format!("DROP SCHEMA {schema}"),
        SQLSTATE_DEPENDENT_OBJECTS_EXIST,
    )
    .await;
    assert!(
        err.message().contains(&format!("{schema}.t")),
        "the refusal must name what the schema still holds, got: {}",
        err.message()
    );

    drop_table(&client, &format!("{schema}.t")).await;
    execute(&client, &format!("DROP SCHEMA {schema}"))
        .await
        .unwrap();
}

// Schema DDL is written to the coordinator catalog immediately, so it cannot take
// part in a transaction block that might ROLLBACK.
#[tokio::test]
async fn test_schema_ddl_is_refused_inside_a_transaction() {
    let client = ready_client().await;
    let schema = unique_table_name("un_schema_tx");

    execute(&client, "BEGIN").await.unwrap();
    assert_unsupported(&client, &format!("CREATE SCHEMA {schema}")).await;
    execute(&client, "ROLLBACK").await.unwrap();

    // Refused, not created.
    assert_sqlstate(
        &client,
        &format!("DROP SCHEMA {schema}"),
        SQLSTATE_SCHEMA_NOT_FOUND,
    )
    .await;
}

#[tokio::test]
async fn test_schemas_are_independent_namespaces() {
    let client = ready_client().await;
    let a = unique_table_name("un_ns_a");
    let b = unique_table_name("un_ns_b");

    execute(&client, &format!("CREATE SCHEMA {a}"))
        .await
        .unwrap();
    execute(&client, &format!("CREATE SCHEMA {b}"))
        .await
        .unwrap();

    for schema in [&a, &b] {
        execute(
            &client,
            &format!("CREATE TABLE {schema}.t (id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
        )
        .await
        .unwrap();
    }

    execute(
        &client,
        &format!("INSERT INTO {a}.t (id, v) VALUES (1, 'in_a')"),
    )
    .await
    .unwrap();

    // The two tables are independent: the row written to a.t is not in b.t.
    assert_eq!(row_count(&client, &format!("{a}.t")).await, 1);
    assert_eq!(
        row_count(&client, &format!("{b}.t")).await,
        0,
        "a.t and b.t must not share storage"
    );

    execute(&client, &format!("DROP TABLE IF EXISTS {a}.t"))
        .await
        .unwrap();
    execute(&client, &format!("DROP TABLE IF EXISTS {b}.t"))
        .await
        .unwrap();
    execute(&client, &format!("DROP SCHEMA {a}")).await.unwrap();
    execute(&client, &format!("DROP SCHEMA {b}")).await.unwrap();
}

// ============================================================================
// 8. Sequences — row 19: a decided limitation
// ============================================================================
//
// Not a gap waiting for a turn — sequences will not be implemented. A sequence is
// one monotonic counter, and a shared-nothing cluster can only offer it as a
// per-shard counter (same numbers on every shard, and each replica of a shard
// advancing its own copy, because replication is statement shipping) or as a
// coordinator counter every insert queues behind. So there is no target-state test
// here; there are rejections that have to keep explaining themselves, and a test
// that the documented alternative — ids generated by the client — actually works.
//
// This also settles SERIAL in `data_types_round_trips.rs`, which was blocked on it.

/// Assert `sql` is refused `0A000` **and** that the message carries the reason and
/// the way out. The wording is part of the contract for a permanent limitation: a
/// bare "not supported" reads as "not yet", which for this row is false.
async fn assert_refused_as_a_sequence(client: &tokio_postgres::Client, sql: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "`{sql}` should carry SQLSTATE {SQLSTATE_FEATURE_NOT_SUPPORTED} (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("no sequences"),
        "`{sql}` should say sequences do not exist here: {}",
        err.message()
    );
    assert!(
        err.message().contains("UUID"),
        "`{sql}` should name the alternative: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_sequence_ddl_is_refused_with_the_reason() {
    let client = ready_client().await;
    let seq = unique_table_name("un_seq");

    assert_refused_as_a_sequence(&client, &format!("CREATE SEQUENCE {seq}")).await;
    assert_refused_as_a_sequence(&client, &format!("CREATE SEQUENCE {seq} START WITH 100")).await;
    // `ALTER SEQUENCE`, like `ALTER SCHEMA`, is not in sqlparser's
    // PostgreSqlDialect: parse rejection (42601), before any of this can apply.
    assert_rejected(&client, &format!("ALTER SEQUENCE {seq} RESTART WITH 1")).await;
    // `DROP SEQUENCE` reports the object does not exist, which is exactly true and
    // is what PostgreSQL says too — there is never a sequence to drop.
    let err = execute_expect_err(&client, &format!("DROP SEQUENCE {seq}")).await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "DROP SEQUENCE on a nonexistent sequence should be 42P01 (got {}: {})",
        err.code().code(),
        err.message()
    );
}

// Every table-DDL spelling of "the server allocates my ids" is the same request,
// and each one is refused at the coordinator rather than at a shard's DuckDB.
#[tokio::test]
async fn test_server_allocated_id_columns_are_refused() {
    let client = ready_client().await;

    for column in [
        "seq SERIAL",
        "seq BIGSERIAL",
        "seq SMALLSERIAL",
        "seq INTEGER DEFAULT nextval('some_seq')",
        "seq INTEGER GENERATED ALWAYS AS IDENTITY",
        "seq INTEGER GENERATED BY DEFAULT AS IDENTITY",
    ] {
        let tbl = unique_table_name("un_seqcol");
        assert_refused_as_a_sequence(
            &client,
            &format!("CREATE TABLE {tbl} (id INTEGER NOT NULL, {column}) {CREATE_OPTS}"),
        )
        .await;
    }
}

// `nextval()` in a write is refused wherever it appears, not just in the shard-key
// position: shipped into shard-local SQL it would be evaluated once per replica.
#[tokio::test]
async fn test_nextval_in_a_write_is_refused() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_seqx_t",
        &format!("(id INTEGER NOT NULL, v VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    assert_refused_as_a_sequence(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (nextval('s'), 'a')"),
    )
    .await;
    // Not the shard key, and still refused — the divergence is per replica, not
    // per shard, so a non-key column is no safer.
    assert_refused_as_a_sequence(
        &client,
        &format!("INSERT INTO {tbl} (id, v) VALUES (1, nextval('s'))"),
    )
    .await;
    assert_refused_as_a_sequence(
        &client,
        &format!("UPDATE {tbl} SET v = 'x' WHERE id = currval('s')"),
    )
    .await;
    assert_eq!(
        row_count(&client, &tbl).await,
        0,
        "no refused statement may have written a row"
    );

    // The documented alternative: the client supplies the ids. Distinct, spread
    // over the shards, no coordination.
    for (i, id) in [1_i64, 2, 3].iter().enumerate() {
        execute(
            &client,
            &format!("INSERT INTO {tbl} (id, v) VALUES ({id}, 'row{i}')"),
        )
        .await
        .unwrap();
    }
    let rows = simple_query_rows(&client, &format!("SELECT DISTINCT id FROM {tbl}"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "client-generated ids must all be distinct");

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 9. User-defined types — row 21: a decided limitation
// ============================================================================
//
// Like sequences, not a gap waiting for a turn: `CREATE TYPE` (enum, composite or
// range), `CREATE DOMAIN` and `ALTER TYPE` will not be implemented. A type is
// cluster-wide state every replica must already hold before a statement can name
// it, and the catalog models tables — a node that joins or is rebuilt would come
// back without the type and refuse every write using it. Nor would the type reach
// the client: the emulated `pg_catalog` has no OID to hand out for it, and a shard's
// enum column already reads back as text. So there is no target-state test here;
// there are rejections that have to keep explaining themselves, and a test that the
// documented alternative — a plain column validated by the application — works.

/// Assert `sql` is refused `0A000` **and** that the message carries the reason and
/// the way out, the same contract section 8 holds sequences to.
async fn assert_refused_as_a_user_type(client: &Client, sql: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "`{sql}` should carry SQLSTATE {SQLSTATE_FEATURE_NOT_SUPPORTED} (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains("no user-defined types"),
        "`{sql}` should say user types do not exist here: {}",
        err.message()
    );
    assert!(
        err.message().contains("VARCHAR"),
        "`{sql}` should name the alternative: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_user_defined_type_ddl_is_refused_with_the_reason() {
    let client = ready_client().await;
    let ty = unique_table_name("un_ty");

    assert_refused_as_a_user_type(
        &client,
        &format!("CREATE TYPE {ty} AS ENUM ('sad', 'happy')"),
    )
    .await;
    // A composite type is the same answer with a different way out (one column per
    // field), so it must not slip through on the representation.
    assert_refused_as_a_user_type(
        &client,
        &format!("CREATE TYPE {ty} AS (x INTEGER, y INTEGER)"),
    )
    .await;
    // A domain is a named type plus a constraint — the same request.
    assert_refused_as_a_user_type(
        &client,
        &format!("CREATE DOMAIN {ty} AS INTEGER CHECK (VALUE > 0)"),
    )
    .await;
    assert_refused_as_a_user_type(&client, &format!("ALTER TYPE {ty} ADD VALUE 'ok'")).await;
    assert_refused_as_a_user_type(&client, &format!("ALTER TYPE {ty} RENAME TO {ty}_2")).await;
    // `DROP TYPE` reports that the type does not exist, which is exactly true and is
    // what PostgreSQL says too — there is never a type to drop. Same as row 19's
    // `DROP SEQUENCE`.
    let err = execute_expect_err(&client, &format!("DROP TYPE {ty}")).await;
    assert_eq!(
        err.code().code(),
        "42P01",
        "DROP TYPE on a nonexistent type should be 42P01 (got {}: {})",
        err.code().code(),
        err.message()
    );
}

// The documented way out, end to end: the values an enum would have constrained
// live in a plain column, and the cluster stores and returns them unchanged.
#[tokio::test]
async fn test_a_plain_column_is_the_documented_alternative() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_tyalt",
        &format!("(id INTEGER NOT NULL, mood VARCHAR) {CREATE_OPTS}"),
    )
    .await;

    for (i, mood) in ["sad", "ok", "happy"].iter().enumerate() {
        execute(
            &client,
            &format!(
                "INSERT INTO {tbl} (id, mood) VALUES ({}, '{mood}')",
                id_for_bucket((i % SHARD_COUNT) as u64, i as i64 + 1)
            ),
        )
        .await
        .unwrap();
    }

    let rows = simple_query_rows(&client, &format!("SELECT mood FROM {tbl} ORDER BY mood"))
        .await
        .unwrap();
    let moods: Vec<String> = rows.iter().filter_map(|r| r[0].clone()).collect();
    assert_eq!(
        moods,
        vec!["happy".to_string(), "ok".to_string(), "sad".to_string()],
        "the values an enum would have held must round-trip through a VARCHAR"
    );

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 10. Statements with a PostgreSQL rewrite — rows 26, 32, 34, 36 and 9
// ============================================================================
//
// Settled as not planned. Each of these is either DuckDB-only syntax or a
// single-node storage concern, and each has a PostgreSQL form that already works
// here, so accepting the DuckDB spelling would add a dialect VaireDB's clients do
// not otherwise speak:
//
//   * row 26 — PIVOT      → `CASE` inside aggregates with `GROUP BY`
//   * row 34 — UNPIVOT    → `UNION ALL`, or `LATERAL` over a `VALUES` list
//   * row 32 — SUMMARIZE  → the aggregates it wraps, written out
//   * row 36 — VACUUM     → nothing: per-shard storage maintenance is not a
//                           coordinator statement, same as CHECKPOINT
//   * row 9  — ANALYZE    → nothing: there is no planner statistics surface for
//                           it to populate, so it could only report a fake `OK`
//
// So this is a guard section, not a pending gap: it holds no xfail, and what it
// pins is that none of these is ever quietly accepted. PIVOT, UNPIVOT, SUMMARIZE
// and VACUUM fail at parse (`42601`) because `PostgreSqlDialect` has no such
// statement; ANALYZE parses and is refused by classification (`0A000`).

#[tokio::test]
async fn test_statements_with_a_postgres_rewrite_stay_rejected() {
    let client = ready_client().await;
    let tbl = create_table(
        &client,
        "un_pivot",
        &format!("(id INTEGER NOT NULL, cat VARCHAR, amt INTEGER) {CREATE_OPTS}"),
    )
    .await;
    execute(
        &client,
        &format!(
            "INSERT INTO {tbl} (id, cat, amt) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)"
        ),
    )
    .await
    .unwrap();

    // The table exists and every column named below is real, so a refusal here can
    // only be about the statement itself.
    let statements = [
        format!("PIVOT {tbl} ON cat USING SUM(amt)"),
        format!("UNPIVOT {tbl} ON amt INTO NAME measure VALUE val"),
        format!("SUMMARIZE {tbl}"),
        "VACUUM".to_string(),
        format!("VACUUM {tbl}"),
        format!("VACUUM ANALYZE {tbl}"),
        format!("ANALYZE {tbl}"),
    ];

    for sql in &statements {
        assert_rejected(&client, sql).await;
    }

    // The other half of the decision: the PostgreSQL rewrite of that PIVOT is not
    // refused, so nothing is lost by declining the DuckDB spelling.
    let rows = simple_query_rows(
        &client,
        &format!(
            "SELECT SUM(CASE WHEN cat = 'a' THEN amt END) AS a, \
             SUM(CASE WHEN cat = 'b' THEN amt END) AS b FROM {tbl}"
        ),
    )
    .await
    .expect("the CASE rewrite of a PIVOT must be answerable");
    assert_eq!(rows.len(), 1, "the rewrite aggregates to a single row");
    assert_eq!(rows[0][0].as_deref(), Some("40"));
    assert_eq!(rows[0][1].as_deref(), Some("20"));

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 11. Unranked ❌ statements — rejection contract only
// ============================================================================
//
// Rows the doc marks ❌ but does not rank in its open-gaps list. They get no
// xfail: no target behavior has been specified for them yet, so all that is
// pinned is the honest failure. Add an xfail here when one is prioritized.
//
// The trailing code on each line is the rejection point observed against the e2e
// cluster: `0A000` means the statement parses and is refused by classification (so
// only routing/execution is missing), `42601` means sqlparser's PostgreSqlDialect
// does not know the syntax at all (so support starts one layer lower).

#[tokio::test]
async fn test_unranked_statements_are_rejected() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_unranked");
    setup_rows(&client, &tbl).await;

    let statements = [
        // row 11 — CALL (no stored/table procedures)                      0A000
        "CALL my_procedure()".to_string(),
        // row 13 — COMMENT ON (no catalog comment storage)                0A000
        format!("COMMENT ON TABLE {tbl} IS 'a comment'"),
        // row 16 — CREATE MACRO (DuckDB-only)                             42601
        "CREATE MACRO one() AS 1".to_string(),
    ];

    for sql in &statements {
        assert_rejected(&client, sql).await;
    }

    drop_table(&client, &tbl).await;
}

// ============================================================================
// 12. Intentionally out of scope — must STAY rejected
// ============================================================================
//
// The doc's closing list: single-node DuckDB concerns that do not map onto a
// sharded coordinator. These deliberately have no xfail — this test is the guard
// that they are never quietly accepted, since accepting one would mean it ran on
// an arbitrary single node. Section 10 holds the other half of that list, the
// statements declined because PostgreSQL already expresses them another way.

#[tokio::test]
async fn test_out_of_scope_statements_stay_rejected() {
    let client = ready_client().await;

    // Same convention as section 11: the trailing code is the observed rejection
    // point. These are split across both layers, which is why the assertion is the
    // tolerant one — what matters is only that none of them is ever accepted.
    let statements = [
        // row 10 — ATTACH / DETACH (single-node DuckDB attachment)  0A000 / 42601
        "ATTACH 'other.db' AS other",
        "DETACH other",
        // row 24 — INSTALL / LOAD (per-node extensions)             42601 / 0A000
        "INSTALL httpfs",
        "LOAD httpfs",
        // row 12 — CHECKPOINT (per-shard storage concern)                   42601
        "CHECKPOINT",
        // row 23 — EXPORT / IMPORT DATABASE (whole-DB dump/load)            42601
        "EXPORT DATABASE '/tmp/vairedb_export'",
        "IMPORT DATABASE '/tmp/vairedb_export'",
        // row 35 — USE (one database per cluster; nothing to switch to)    0A000
        "USE other_db",
        // row 18 — CREATE SECRET (superseded by VaireDB's own anonymization
        // secret, written via INSERT INTO
        // vairedb_catalog.anonymization_secret)                             0A000
        "CREATE SECRET my_secret (TYPE S3, KEY_ID 'k', SECRET 's')",
    ];

    for sql in statements {
        assert_rejected(&client, sql).await;
    }
}

// ============================================================================
// 13. A rejection names the command it refused
// ============================================================================
//
// `0A000` alone is not actionable: a client that sends several statements, or a
// driver issuing housekeeping SQL of its own, cannot tell *which* command was
// refused from "this statement is not supported by VaireDB". Every command the
// doc pins to the classification rejection point must appear by name in the
// message (`unsupported_statement_label`).

async fn assert_names_the_command(client: &Client, sql: &str, name: &str) {
    let err = execute_expect_err(client, sql).await;
    assert_eq!(
        err.code().code(),
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "`{sql}` should be refused by classification (got {}: {})",
        err.code().code(),
        err.message()
    );
    assert!(
        err.message().contains(name),
        "`{sql}` should be refused by name `{name}`, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_rejection_names_the_refused_command() {
    let client = ready_client().await;
    let tbl = unique_table_name("un_named");
    setup_rows(&client, &tbl).await;

    // `SET`, `SHOW`, `EXPLAIN` and `DESCRIBE` are absent on purpose: they are routed
    // now, so there is no statement-level refusal left to name. What a client can still
    // be refused for is a specific form or a specific parameter, and those name
    // themselves — see `test_unmodelled_set_forms_stay_rejected`,
    // `test_search_path_stays_refused` and `test_untruthful_explain_forms_are_refused`.
    // `COPY` is absent for the same reason: every direction of it is routed now,
    // including the two that drive the copy sub-protocol, so the only refusals left
    // are about a form or an option and they name themselves — see
    // `test_copy_outside_csv_is_rejected` and
    // `test_a_streaming_import_that_cannot_work_is_refused_up_front`.
    let cases: Vec<(String, &str)> = vec![
        (
            "CREATE SEQUENCE un_named_seq".to_string(),
            "CREATE SEQUENCE",
        ),
        (
            "CREATE TYPE un_named_mood AS ENUM ('sad', 'happy')".to_string(),
            "CREATE TYPE",
        ),
        (
            format!("COMMENT ON TABLE {tbl} IS 'a comment'"),
            "COMMENT ON",
        ),
        (format!("ANALYZE {tbl}"), "ANALYZE"),
        ("CALL un_named_proc()".to_string(), "CALL"),
        ("USE un_named_db".to_string(), "USE"),
    ];

    for (sql, name) in &cases {
        assert_names_the_command(&client, sql, name).await;
    }

    drop_table(&client, &tbl).await;
}
