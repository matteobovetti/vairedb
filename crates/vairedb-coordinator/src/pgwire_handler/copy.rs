//! `COPY ... TO/FROM`: the bulk import/export path, to a server-side CSV or
//! Parquet file, or to the client over the copy sub-protocol.
//!
//! Every direction is built from parts that already exist, which is what keeps
//! them consistent with ordinary statements:
//!
//! * `COPY <table|query> TO '<file>' | STDOUT` runs the source on the read path —
//!   so it gathers every shard's rows through the same planner a `SELECT` uses —
//!   and emits the collected batches in the format the statement asked for.
//! * `COPY <table> FROM '<file>' | STDIN` decodes the file into record batches and
//!   hands them to the INSERT lane, so every row is routed by its shard key, split
//!   per shard, and counted exactly like a client `INSERT ... VALUES` would be.
//!
//! The two CSV `FROM` forms differ only in who supplies the bytes: both feed one
//! [`crate::pgwire_handler::copy_stream::CopySink`], which is what makes a file
//! import and a `\copy` import land identically. A named file is on the
//! **coordinator's** filesystem, not the client's — that is PostgreSQL's
//! server-side `COPY` — while `STDIN`/`STDOUT` are the client's own, driven by the
//! copy sub-protocol in [`crate::pgwire_handler::copy_stream`].
//!
//! Two formats are accepted and the statement has to name one of them:
//!
//! * `FORMAT CSV`, in either direction and over either transport. It cannot be
//!   defaulted to, because PostgreSQL's default is its own `TEXT` encoding and
//!   silently writing CSV where a client expects `TEXT` would corrupt whatever
//!   reads the file next.
//! * `FORMAT PARQUET`, for the server-side file forms only — see
//!   [`crate::pgwire_handler::copy_parquet`] for why a Parquet file is not
//!   something the copy sub-protocol can carry, and why the format takes no
//!   options.
//!
//! What the two formats *share* is everything about the table: which columns a
//! file's fields land on ([`target_columns`]), that those columns exist and carry
//! the shard key ([`validate_target_columns`]), and how a failure part-way through
//! is reported ([`partial_copy_error`]). Only the decoding differs.

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::copy::CopyData;
use tokio::io::AsyncReadExt;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::MetadataCatalog;
use crate::error::CoordinatorError;
use crate::pgwire_handler::column_labels;
use crate::pgwire_handler::copy_parquet;
use crate::pgwire_handler::copy_stream::CopySink;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::parser::parse_sql;
use crate::pgwire_handler::query_router::{
    canonical_table_name, canonicalize_ident, canonicalize_ident_str,
};
use crate::pgwire_handler::session::SessionState;
use crate::sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Query, Statement,
};
use crate::write_sql_cl;

/// The CSV dialect a COPY reads or writes.
///
/// Deliberately small: the options kept are the ones that mean the same thing in
/// both directions, so a file written by `COPY ... TO` is read back identically by
/// `COPY ... FROM` with the same options. Anything else is refused rather than
/// accepted-and-ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CsvDialect {
    /// `HEADER`: on export, write the column names first; on import, read them.
    pub(super) header: bool,
    pub(super) delimiter: u8,
    pub(super) quote: u8,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            header: false,
            delimiter: b',',
            quote: b'"',
        }
    }
}

/// The encoding a COPY reads or writes.
///
/// The dialect lives *inside* the CSV arm rather than beside the format, because
/// that is where it has meaning: a Parquet file names and types its own columns, so
/// there is no `HEADER`, `DELIMITER` or `QUOTE` for a client to choose and a
/// statement that names one against `FORMAT PARQUET` is refused rather than left
/// with an option nothing reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CopyFormat {
    Csv(CsvDialect),
    Parquet,
}

/// What a `COPY ... TO` exports.
#[derive(Debug, Clone, PartialEq)]
enum CopyOutSource {
    /// A table, with the columns to export (empty means every column).
    Table { name: String, columns: Vec<String> },
    /// `COPY (SELECT ...) TO`: an arbitrary query.
    Query(Box<Query>),
}

/// Who supplies (or receives) a COPY's bytes.
///
/// For CSV the distinction is *only* about the transport: the CSV either side reads
/// and writes is the same CSV, decoded by the same sink and rendered by the same
/// writer, so a file and a `\copy` cannot disagree about what a row means. Parquet
/// has no client form at all — see [`CopyFormat`] and
/// [`crate::pgwire_handler::copy_parquet`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum CopyEndpoint {
    /// A path on the coordinator's own filesystem — PostgreSQL's server-side form.
    File(String),
    /// The client, over the copy sub-protocol (`STDIN`/`STDOUT`).
    Client,
}

/// A validated COPY: everything decidable from the statement alone is settled
/// here, so the I/O steps have no rules left to apply.
#[derive(Debug, Clone, PartialEq)]
enum CopyPlan {
    Out {
        source: CopyOutSource,
        endpoint: CopyEndpoint,
        format: CopyFormat,
    },
    In {
        table: String,
        /// The columns the file's fields map onto, canonical. Empty means "decide
        /// from the file's own column names, or the table's leading columns".
        columns: Vec<String>,
        endpoint: CopyEndpoint,
        format: CopyFormat,
    },
}

/// Bytes read from a file per `COPY ... FROM '<file>' (FORMAT CSV)` step.
///
/// The sink buffers rows, not bytes, so this only bounds how much of the file is
/// in flight at once; it is a read size, chosen to be a few filesystem blocks.
const FILE_CHUNK_BYTES: usize = 64 * 1024;

/// Rows decoded before a batch of an import is handed to the INSERT lane.
///
/// A multiple of the INSERT lane's own chunk size, so a batch turns into whole
/// statements rather than one full chunk and a remainder. Ten of them is the
/// trade-off between round trips to the shards and how much of the client's data is
/// held in the coordinator at once. Shared by both formats so that the same data
/// imports in the same number of statements whichever file it arrived in.
pub(super) const ROWS_PER_BATCH: usize = 10 * write_sql_cl::ROWS_PER_STATEMENT;

impl VaireDbQueryHandler {
    /// Run a `COPY`, in whichever of the four directions the statement named.
    ///
    /// Three of them answer with PostgreSQL's `COPY <n>` tag straight away. The
    /// fourth, `FROM STDIN`, cannot: the rows have not been sent yet, so it answers
    /// with `CopyInResponse` and the tag is sent by
    /// [`crate::pgwire_handler::copy_stream`] once the client says it is done.
    pub(super) async fn handle_copy(
        &self,
        stmt: &Statement,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        match plan_copy(stmt)? {
            CopyPlan::Out {
                source,
                endpoint,
                format,
            } => self.copy_out(&source, &endpoint, &format).await,
            CopyPlan::In {
                table,
                columns,
                endpoint,
                format,
            } => {
                self.copy_in(&table, &columns, &endpoint, &format, session)
                    .await
            }
        }
    }

    /// Export a table or query, to a file on the coordinator or to the client.
    ///
    /// The source runs on the read path either way, which is what makes the export
    /// complete: gathering the shards is the planner's job, not this function's.
    async fn copy_out(
        &self,
        source: &CopyOutSource,
        endpoint: &CopyEndpoint,
        format: &CopyFormat,
    ) -> PgWireResult<Response> {
        let query = match source {
            CopyOutSource::Table { name, columns } => {
                // Reported before the query is planned so a missing table is a
                // table error rather than whatever the planner makes of it.
                let ctx = ErrorContext::for_table(name);
                if self
                    .catalog
                    .get_table(name)
                    .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
                    .is_none()
                {
                    let err = CoordinatorError::TableNotFound(name.clone());
                    return Err(enrich_coordinator_error(&err, &ctx, &self.catalog));
                }
                select_from_table(name, columns)?
            }
            CopyOutSource::Query(query) => Statement::Query(query.clone()),
        };

        let (schema, batches) = self.collect_query_rows(&query, &[]).await?;
        // A header names the columns, so it is a place a label reaches a client, and it has
        // to name them the way a `SELECT`'s RowDescription does. See
        // [`collapse_anonymous_labels`].
        let (schema, batches) = collapse_anonymous_labels(schema, batches)?;

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        match (endpoint, format) {
            (CopyEndpoint::File(path), CopyFormat::Csv(dialect)) => {
                write_csv_file(path.clone(), batches, dialect.clone()).await?;
                Ok(Response::Execution(Tag::new("COPY").with_rows(rows)))
            }
            (CopyEndpoint::File(path), CopyFormat::Parquet) => {
                copy_parquet::write_file(path.clone(), schema, batches).await?;
                Ok(Response::Execution(Tag::new("COPY").with_rows(rows)))
            }
            (CopyEndpoint::Client, CopyFormat::Csv(dialect)) => {
                let columns = schema.fields().len();
                let chunks = csv_row_messages(&schema, &batches, dialect)?;
                // Format 0 is the textual one, which is what CSV is. pgwire drives
                // the rest of the exchange — the `CopyOutResponse` header, the
                // `CopyDone`, and the `COPY n` tag it derives from the stream.
                Ok(Response::CopyOut(CopyResponse::new(
                    0,
                    columns,
                    futures::stream::iter(chunks.into_iter().map(Ok)),
                )))
            }
            // Refused by [`plan_copy`], which is the only way a plan is built.
            (CopyEndpoint::Client, CopyFormat::Parquet) => Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "COPY ... TO STDOUT (FORMAT PARQUET) reached the export path",
            )),
        }
    }

    /// Import into a table, from a file on the coordinator or from the client.
    ///
    /// Three of the four combinations exist: CSV from a file, CSV from the client,
    /// and Parquet from a file. All three end in the INSERT lane, so an imported row
    /// is routed, split per shard and counted like any other written row — what
    /// differs is only how the bytes became a batch.
    async fn copy_in(
        &self,
        table: &str,
        columns: &[String],
        endpoint: &CopyEndpoint,
        format: &CopyFormat,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        match (endpoint, format) {
            (CopyEndpoint::File(path), CopyFormat::Parquet) => {
                let rows = self
                    .copy_from_parquet_file(table, columns, path, session)
                    .await?;
                Ok(Response::Execution(
                    Tag::new("COPY").with_rows(rows as usize),
                ))
            }
            (endpoint, CopyFormat::Csv(dialect)) => {
                let sink = open_copy_sink(&self.catalog, table, columns, dialect)?;
                match endpoint {
                    CopyEndpoint::File(path) => {
                        let rows = self.copy_from_file(sink, path, session).await?;
                        Ok(Response::Execution(
                            Tag::new("COPY").with_rows(rows as usize),
                        ))
                    }
                    CopyEndpoint::Client => Ok(begin_copy_from_client(sink, session).await),
                }
            }
            // Refused by [`plan_copy`], which is the only way a plan is built.
            (CopyEndpoint::Client, CopyFormat::Parquet) => Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "COPY ... FROM STDIN (FORMAT PARQUET) reached the import path",
            )),
        }
    }

    /// Feed a file on the coordinator's disk to `sink`, returning the rows written.
    ///
    /// Read in chunks rather than whole: a bulk import is as large as the client's
    /// file, and the sink ships a batch at a time, so nothing here needs the file
    /// to fit in memory.
    async fn copy_from_file(
        &self,
        mut sink: CopySink,
        path: &str,
        session: &SessionState,
    ) -> PgWireResult<u64> {
        readable_file(path)?;

        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|e| file_error("read", path, &e))?;
        let mut buffer = vec![0u8; FILE_CHUNK_BYTES];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|e| file_error("read", path, &e))?;
            if read == 0 {
                break;
            }
            sink.push(self, session, &buffer[..read]).await?;
        }
        sink.finish(self, session).await
    }

    /// Import a Parquet file on the coordinator's disk, returning the rows written.
    ///
    /// The file decides the column mapping — it names its own columns — so that is
    /// settled and validated before a batch is read, and a file the table cannot
    /// take is refused with nothing written. From there each batch goes straight to
    /// the INSERT lane, one at a time, so a file larger than memory imports without
    /// ever being held whole.
    async fn copy_from_parquet_file(
        &self,
        table: &str,
        columns: &[String],
        path: &str,
        session: &SessionState,
    ) -> PgWireResult<u64> {
        let ctx = ErrorContext::for_table(table);
        let table_meta = self
            .catalog
            .get_table(table)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(table.to_string());
                enrich_coordinator_error(&err, &ctx, &self.catalog)
            })?;

        readable_file(path)?;
        let mut file = copy_parquet::ParquetFile::open(path).await?;
        let template = copy_parquet::insert_template(table, columns, file.fields(), &table_meta)?;

        // Counted outside the loop so a failure part-way can say how much of the
        // file is already stored — there is no cross-shard rollback to undo it with.
        let mut rows = 0u64;
        match self
            .import_parquet_batches(&mut file, &template, table, session, &mut rows)
            .await
        {
            Ok(()) => Ok(rows),
            Err(e) => Err(partial_copy_error(table, rows, e)),
        }
    }

    /// Ship every remaining batch of `file` through the INSERT lane, adding what each
    /// wrote to `rows`.
    async fn import_parquet_batches(
        &self,
        file: &mut copy_parquet::ParquetFile,
        template: &Statement,
        table: &str,
        session: &SessionState,
        rows: &mut u64,
    ) -> PgWireResult<()> {
        while let Some(batch) = file.next_batch().await? {
            let statements = copy_parquet::insert_statements(template, &batch)?;
            *rows += self
                .write_row_statements(&statements, &[], session, table, "COPY")
                .await?;
        }
        Ok(())
    }
}

/// Check that `path` names something the coordinator can read rows out of.
///
/// Checked before opening so that a directory — which opens fine and reads as an
/// error only later, differently per format — is reported as what it is.
fn readable_file(path: &str) -> PgWireResult<()> {
    let metadata = std::fs::metadata(path).map_err(|e| file_error("read", path, &e))?;
    if !metadata.is_file() {
        return Err(make_vdb_error(
            VdbErrorCode::InternalError,
            format!("COPY could not read \"{path}\" on the coordinator: not a file"),
        ));
    }
    Ok(())
}

/// Report a failed `COPY ... FROM` honestly: as a partial commit once rows are
/// stored, and as itself while nothing is.
///
/// Shared by both import lanes because the guarantee is the same in both. VaireDB
/// has no cross-shard commit protocol, so a batch that fails after earlier batches
/// shipped leaves those rows written — saying so is the whole point, since a client
/// that retries needs to know the table is not as it was.
pub(super) fn partial_copy_error(table: &str, rows: u64, e: PgWireError) -> PgWireError {
    if rows == 0 {
        return e;
    }
    make_vdb_error(
        VdbErrorCode::PartialCommit,
        format!(
            "COPY partially applied: {rows} row(s) were written to \"{table}\" and cannot be undone, then the copy failed. Inspect the table before retrying. Cause: {e}"
        ),
    )
}

/// The sink a `COPY ... FROM (FORMAT CSV)` feeds, with everything decidable before
/// a byte arrives already decided: the table exists, and a column list the statement
/// spelled out names real columns and carries the shard key.
fn open_copy_sink(
    catalog: &Arc<MetadataCatalog>,
    table: &str,
    columns: &[String],
    dialect: &CsvDialect,
) -> PgWireResult<CopySink> {
    let ctx = ErrorContext::for_table(table);
    let table_meta = catalog
        .get_table(table)
        .map_err(|e| enrich_coordinator_error(&e, &ctx, catalog))?
        .ok_or_else(|| {
            let err = CoordinatorError::TableNotFound(table.to_string());
            enrich_coordinator_error(&err, &ctx, catalog)
        })?;

    CopySink::open(table, columns, dialect, &table_meta)
}

/// Decide, at Parse time, whether a `COPY ... FROM STDIN` could ever work.
///
/// The refusal has to land here rather than at Execute, and the reason is the
/// client's own book-keeping rather than politeness. A driver asks for a streaming
/// import by preparing the statement, then binding and executing it, and it queues
/// the abort for that copy — a `CopyFail` and a second `Sync` — the moment the
/// request object is dropped. If the refusal comes from Execute, the first `Sync`
/// has already been answered with `ReadyForQuery`, so the abort's `Sync` draws a
/// second one; the client counts that against its *next* request and every later
/// reply on the connection is attributed to the wrong query. Refusing while the
/// statement is still being prepared means no copy is ever begun, nothing is
/// queued to abort, and the connection is left as it was — which is also what a
/// bulk loader wants, since it learns the copy is hopeless before uploading a
/// gigabyte of rows for it.
///
/// Only `FROM STDIN` is checked. The other three directions do not invite the
/// client to send anything, so nothing about them is riding on when they refuse,
/// and Execute is where they stay.
///
/// `plan_copy` runs either way, which is what makes `FROM STDIN (FORMAT PARQUET)`
/// land here too: a format with no streaming form is refused while the statement is
/// still being prepared, for exactly the reason above.
pub(super) fn precheck_copy_from_stdin(
    stmt: &Statement,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<()> {
    if let CopyPlan::In {
        table,
        columns,
        endpoint: CopyEndpoint::Client,
        format: CopyFormat::Csv(dialect),
    } = plan_copy(stmt)?
    {
        open_copy_sink(catalog, &table, &columns, &dialect)?;
    }
    Ok(())
}

/// Hand `sink` to the connection and tell the client to start sending.
///
/// Nothing is read here: the rows arrive as `CopyData` messages, which pgwire
/// routes to [`crate::pgwire_handler::copy_stream`] for as long as the connection
/// is in copy mode. The sink lives in the session for exactly that reason — the
/// handler is one `Arc` shared by every connection, and this state belongs to one.
async fn begin_copy_from_client(sink: CopySink, session: &SessionState) -> Response {
    let columns = sink.advertised_columns();
    *session.copy_in().await = Some(sink);
    // Format 0 is text, which is what CSV is; the stream is unused for an inbound
    // copy — pgwire only reads the format and column count off it.
    Response::CopyIn(CopyResponse::new(0, columns, futures::stream::empty()))
}

/// The `CopyData` messages a `COPY ... TO STDOUT` sends: one per row, as
/// PostgreSQL sends them.
///
/// One message per row is not just idiomatic, it is what makes the tag right:
/// pgwire counts non-empty `CopyData` messages to build `COPY n`. That is also why
/// a requested header rides along in the *first row's* message instead of getting
/// one of its own — a message of its own would be counted as a row.
///
/// The one case that cannot come out right is a header over an empty result: the
/// header still has to be sent, and any message carrying it is counted, so the tag
/// reads `COPY 1` where PostgreSQL says `COPY 0`. Sending the header is the half
/// worth keeping — a client that reads the output back with `HEADER` needs it, and
/// a count of 1 on a transfer the client can see is empty misleads nobody.
fn csv_row_messages(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    dialect: &CsvDialect,
) -> PgWireResult<Vec<CopyData>> {
    let mut messages = Vec::new();
    let mut header_pending = dialect.header;

    for batch in batches {
        for row in 0..batch.num_rows() {
            let bytes = write_csv_bytes(&batch.slice(row, 1), dialect, header_pending)?;
            header_pending = false;
            messages.push(CopyData::new(Bytes::from(bytes)));
        }
    }

    if header_pending {
        let empty = RecordBatch::new_empty(SchemaRef::clone(schema));
        messages.push(CopyData::new(Bytes::from(write_csv_bytes(
            &empty, dialect, true,
        )?)));
    }

    Ok(messages)
}

/// Give every anonymous result column back the one name PostgreSQL has for it.
///
/// A `COPY (SELECT a + 1, b + 1) TO … WITH (HEADER)` writes a header out of the **batches'**
/// own schema rather than out of the wire schema a `SELECT` describes from, so the
/// numbering [`super::column_labels`] uses to keep those two columns distinct inside the
/// plan would otherwise be visible here — `?column?,?column?2` where PostgreSQL writes
/// `?column?,?column?`. The batches are rebuilt rather than the schema alone, because the
/// CSV writer reads the names off the batch it is handed.
///
/// A statement with nothing to rename comes back untouched, which is every `COPY` of a
/// table and every query whose columns are named. Names are all that changes — not the
/// types [`super::encoding::wire_schema`] also converts, since CSV renders a value rather
/// than declaring it and the batches would then no longer match their own schema.
fn collapse_anonymous_labels(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> PgWireResult<(SchemaRef, Vec<RecordBatch>)> {
    if !schema
        .fields()
        .iter()
        .any(|f| column_labels::wire_label(f.name()).is_some())
    {
        return Ok((schema, batches));
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match column_labels::wire_label(f.name()) {
            Some(name) => f.as_ref().clone().with_name(name),
            None => f.as_ref().clone(),
        })
        .collect();
    let collapsed = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let batches = batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(SchemaRef::clone(&collapsed), batch.columns().to_vec()).map_err(
                |e| {
                    make_vdb_error(
                        VdbErrorCode::InternalError,
                        format!("COPY could not label a result column: {e}"),
                    )
                },
            )
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    Ok((collapsed, batches))
}

/// One batch rendered as CSV, optionally preceded by the column-name header.
fn write_csv_bytes(
    batch: &RecordBatch,
    dialect: &CsvDialect,
    header: bool,
) -> PgWireResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_header(header)
        .with_delimiter(dialect.delimiter)
        .with_quote(dialect.quote)
        .build(&mut out);
    writer.write(batch).map_err(|e| {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!("COPY could not render a row as CSV: {e}"),
        )
    })?;
    drop(writer);
    Ok(out)
}

/// True for the direction that puts rows into a table, `COPY ... FROM`.
///
/// Used by the transaction guard, which has to tell a bulk *write* from a bulk
/// *read* before either has been planned.
pub(super) fn copy_writes_rows(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Copy { to: false, .. })
}

/// Canonical names of every relation a `COPY ... TO` reads — the table it exports,
/// or every relation of the query it exports. Empty for `COPY ... FROM`, which
/// reads a file rather than a table.
pub(super) fn copy_source_tables(stmt: &Statement) -> Vec<String> {
    let Statement::Copy {
        source, to: true, ..
    } = stmt
    else {
        return Vec::new();
    };
    match source {
        CopySource::Table { table_name, .. } => {
            canonical_table_name(table_name).into_iter().collect()
        }
        CopySource::Query(query) => write_sql_cl::relations_read(query.as_ref()),
    }
}

/// Validate a `COPY` and reduce it to the plan the I/O steps run.
fn plan_copy(stmt: &Statement) -> PgWireResult<CopyPlan> {
    let Statement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        values,
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a COPY statement",
        ));
    };

    let endpoint = copy_endpoint(target)?;
    let format = copy_format(options, legacy_options)?;

    // A Parquet file is not a sequence of rows, so there is no streaming form of it
    // to route. Refused here rather than at the transfer, which is what lets the
    // Parse-time pre-check catch it — see [`precheck_copy_from_stdin`].
    if format == CopyFormat::Parquet && endpoint == CopyEndpoint::Client {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY ... FROM STDIN / TO STDOUT (FORMAT PARQUET) is not supported by VaireDB: a Parquet file's footer is written last and has to be read first, so the bytes are not the row-at-a-time stream the copy protocol carries. Name a file on the coordinator with COPY ... TO/FROM '<file>' (FORMAT PARQUET), or stream with (FORMAT CSV)",
        ));
    }

    // `COPY t FROM STDIN ...;` puts the rows after the statement's semicolon, and
    // the parser reads whatever follows it in the same buffer as inline data. On a
    // `COPY` that arrived on its own, that is the statement's own trailing newline —
    // one empty value — which is not data and must not be mistaken for it.
    //
    // Anything more than that *is* data in the query string, which no client sends
    // over the wire protocol: the rows travel as `CopyData` messages, so bytes here
    // would be silently dropped.
    if values
        .iter()
        .any(|v| v.as_deref().is_none_or(|s| !s.trim().is_empty()))
    {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY with inline data in the query text is not supported by VaireDB: send the rows over the copy protocol with COPY ... FROM STDIN (FORMAT CSV), read them from a file on the coordinator with COPY ... FROM '<file>' (FORMAT CSV), or use INSERT",
        ));
    }

    if *to {
        let source = match source {
            CopySource::Table {
                table_name,
                columns,
            } => CopyOutSource::Table {
                name: canonical_table_name(table_name).ok_or_else(|| {
                    make_vdb_error(
                        VdbErrorCode::SqlSyntaxError,
                        "could not determine the table to copy from",
                    )
                })?,
                columns: columns.iter().map(canonicalize_ident).collect(),
            },
            CopySource::Query(query) => CopyOutSource::Query(query.clone()),
        };
        return Ok(CopyPlan::Out {
            source,
            endpoint,
            format,
        });
    }

    // `COPY (SELECT ...) FROM` has no meaning: a query is not a place to put rows.
    let CopySource::Table {
        table_name,
        columns,
    } = source
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "COPY ... FROM must name a table to import into",
        ));
    };

    Ok(CopyPlan::In {
        table: canonical_table_name(table_name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine the table to copy into",
            )
        })?,
        columns: columns.iter().map(canonicalize_ident).collect(),
        endpoint,
        format,
    })
}

/// Where a COPY's bytes come from or go to.
///
/// `PROGRAM` is the one form still refused: it would run a shell command on the
/// coordinator, which VaireDB does not do at all — and unlike `STDIN`/`STDOUT`
/// there is no transport to route it to, only a process to spawn.
fn copy_endpoint(target: &CopyTarget) -> PgWireResult<CopyEndpoint> {
    match target {
        CopyTarget::File { filename } => Ok(CopyEndpoint::File(filename.clone())),
        CopyTarget::Stdin | CopyTarget::Stdout => Ok(CopyEndpoint::Client),
        CopyTarget::Program { .. } => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY ... PROGRAM is not supported by VaireDB: the coordinator does not run shell commands. Stream the data instead with COPY ... FROM STDIN / TO STDOUT (FORMAT CSV), or name a file on the coordinator",
        )),
    }
}

/// The format asked for by a COPY's options, in either the modern
/// `(FORMAT CSV, HEADER)` spelling or the legacy `CSV HEADER` one.
///
/// `FORMAT` is required rather than defaulted: PostgreSQL's default is `TEXT`, a
/// different encoding from either of the two VaireDB writes, and a client that
/// omitted the format is expecting that one. An option VaireDB does not honor is
/// refused by name — accepting and ignoring `NULL 'x'` or `FORCE_QUOTE` would write
/// a file that does not say what the client asked it to say.
///
/// The format is settled last, after every option has been read, so that the
/// refusals do not depend on the order the client wrote them in: `(HEADER, FORMAT
/// PARQUET)` is the same statement as `(FORMAT PARQUET, HEADER)` and both name an
/// option Parquet has nothing to apply it to.
fn copy_format(
    options: &[CopyOption],
    legacy_options: &[CopyLegacyOption],
) -> PgWireResult<CopyFormat> {
    let mut named: Option<&str> = None;
    let mut dialect = CsvDialect::default();
    // Options that only mean something for CSV, in the spelling the client used.
    let mut csv_only: Vec<&str> = Vec::new();

    for option in options {
        match option {
            CopyOption::Format(name) => named = Some(&name.value),
            CopyOption::Header(header) => {
                dialect.header = *header;
                csv_only.push("HEADER");
            }
            CopyOption::Delimiter(c) => {
                dialect.delimiter = single_byte("DELIMITER", *c)?;
                csv_only.push("DELIMITER");
            }
            CopyOption::Quote(c) => {
                dialect.quote = single_byte("QUOTE", *c)?;
                csv_only.push("QUOTE");
            }
            other => return Err(unsupported_option(&other.to_string())),
        }
    }

    for option in legacy_options {
        match option {
            CopyLegacyOption::Csv(csv_options) => {
                named = Some("CSV");
                for csv_option in csv_options {
                    match csv_option {
                        CopyLegacyCsvOption::Header => dialect.header = true,
                        CopyLegacyCsvOption::Quote(c) => {
                            dialect.quote = single_byte("QUOTE", *c)?;
                        }
                        other => return Err(unsupported_option(&other.to_string())),
                    }
                }
            }
            // The legacy spelling of `FORMAT BINARY`, which is neither of the two.
            CopyLegacyOption::Binary => return Err(unsupported_format("BINARY")),
            CopyLegacyOption::Delimiter(c) => {
                dialect.delimiter = single_byte("DELIMITER", *c)?;
                csv_only.push("DELIMITER");
            }
            other => return Err(unsupported_option(&other.to_string())),
        }
    }

    let Some(named) = named else {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY without a FORMAT is not supported by VaireDB: PostgreSQL's default TEXT format is a different encoding, so it is refused rather than written as something else. Add (FORMAT CSV) or (FORMAT PARQUET)",
        ));
    };

    if named.eq_ignore_ascii_case("csv") {
        return Ok(CopyFormat::Csv(dialect));
    }
    if !named.eq_ignore_ascii_case("parquet") {
        return Err(unsupported_format(named));
    }
    if let Some(option) = csv_only.first() {
        return Err(csv_only_option(option));
    }
    Ok(CopyFormat::Parquet)
}

/// `0A000` for a format VaireDB cannot read or write.
fn unsupported_format(name: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "COPY format {name} is not supported by VaireDB: only CSV and PARQUET are. Use (FORMAT CSV) or (FORMAT PARQUET)"
        ),
    )
}

/// `0A000` for an option VaireDB would have to ignore.
fn unsupported_option(rendered: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "COPY option `{rendered}` is not supported by VaireDB, and is refused rather than ignored: a file written or read against options that were dropped would not hold what the statement said. Supported options are FORMAT CSV, FORMAT PARQUET, and HEADER, DELIMITER and QUOTE for CSV"
        ),
    )
}

/// `0A000` for a CSV option named against `FORMAT PARQUET`.
///
/// Refused rather than ignored for the same reason as any other dropped option, and
/// the message says what makes it meaningless rather than only that it is: a client
/// that reached for `HEADER` was asking for column names, which a Parquet file
/// already carries.
fn csv_only_option(option: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "COPY option `{option}` is not supported by VaireDB with FORMAT PARQUET, and is refused rather than ignored: a Parquet file names and types its own columns, so HEADER, DELIMITER and QUOTE have nothing to apply to. Drop the option, or use (FORMAT CSV)"
        ),
    )
}

/// A delimiter or quote character as the single byte the CSV reader/writer takes.
/// A multi-byte character is refused rather than truncated into a byte that would
/// split the file's rows somewhere the client never asked for.
fn single_byte(option: &str, c: char) -> PgWireResult<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("COPY {option} must be a single ASCII character, got '{c}'"),
        ))
    }
}

/// The table columns a CSV file's fields map onto, in file order.
///
/// The statement's own column list wins. Failing that, a file with a header names
/// its columns, and a file without one is positional against the table's leading
/// columns — the same rule PostgreSQL applies to `INSERT INTO t VALUES (...)`.
pub(super) fn target_columns(
    stated: &[String],
    file_fields: &[String],
    table_columns: &[String],
    header: bool,
) -> PgWireResult<Vec<String>> {
    let columns = if !stated.is_empty() {
        stated.to_vec()
    } else if header {
        file_fields
            .iter()
            .map(|f| canonicalize_ident_str(f))
            .collect()
    } else {
        table_columns
            .iter()
            .take(file_fields.len())
            .cloned()
            .collect()
    };

    if columns.len() != file_fields.len() {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!(
                "COPY names {} column(s) but the file has {} field(s)",
                columns.len(),
                file_fields.len()
            ),
        ));
    }
    Ok(columns)
}

/// Check that the columns a `COPY ... FROM` is about to write can actually be
/// written: they exist on the table, and they include the column every row is
/// placed by.
///
/// Both checks are cheap and both are worth making before a byte of data is
/// accepted, so a client that named the columns hears about a mistake in reply to
/// the `COPY` itself rather than after uploading a file. When the columns come from
/// a header instead, the earliest this can run is the first record — which is still
/// before anything is written.
pub(super) fn validate_target_columns(
    table: &str,
    target: &[String],
    table_columns: &[String],
    shard_key: &str,
) -> PgWireResult<()> {
    for column in target {
        if !table_columns.contains(column) {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!("column \"{column}\" does not exist in table \"{table}\""),
            ));
        }
    }
    // Every row is placed by hashing its shard key, so data that does not carry
    // that column cannot be imported — refused before anything is read into the
    // table rather than routed somewhere arbitrary.
    if !target.iter().any(|c| c == shard_key) {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!(
                "COPY ... FROM must include the shard key \"{shard_key}\": every row is routed by it. Add the column to the data, or name the columns in COPY {table} (...) FROM"
            ),
        ));
    }
    Ok(())
}

/// `SELECT <columns> FROM <table>` for a `COPY <table> TO`, built as SQL and
/// re-parsed so it goes through exactly the parse the read path expects. Names are
/// quoted because they are catalog-canonical by now: unquoted, DuckDB and
/// DataFusion would fold a name whose case survived `CREATE TABLE`.
fn select_from_table(table: &str, columns: &[String]) -> PgWireResult<Statement> {
    let projection = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|c| quoted(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let sql = format!("SELECT {projection} FROM {}", quoted(table));
    parse_sql(&sql)
        .map_err(|e| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!("could not build a query for COPY {table} TO: {e}"),
            )
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::InternalError,
                format!("could not build a query for COPY {table} TO"),
            )
        })
}

/// A double-quoted identifier, with any embedded quote doubled.
fn quoted(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Write collected batches to `path` as CSV.
///
/// Runs on a blocking thread: the file is on the coordinator's disk and an export
/// is as large as the result, which is not something to hold an async worker for.
/// The file is truncated, matching PostgreSQL's `COPY ... TO`.
async fn write_csv_file(
    path: String,
    batches: Vec<RecordBatch>,
    dialect: CsvDialect,
) -> PgWireResult<()> {
    let reported_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let file = File::create(&path).map_err(|e| file_error("write", &path, &e))?;
        let mut writer = WriterBuilder::new()
            .with_header(dialect.header)
            .with_delimiter(dialect.delimiter)
            .with_quote(dialect.quote)
            .build(BufWriter::new(file));
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| file_error("write", &path, &e))?;
        }
        // The batches are written through a `BufWriter`, so the last of them only
        // reaches the file when it is flushed — a failure here is a short file,
        // which the client has to hear about.
        writer
            .into_inner()
            .into_inner()
            .map_err(|e| file_error("write", &path, &e))?
            .sync_all()
            .map_err(|e| file_error("write", &path, &e))?;
        Ok(())
    })
    .await
    .map_err(|e| {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!("COPY could not finish writing \"{reported_path}\": {e}"),
        )
    })?
}

/// A file the coordinator could not read or write, named with the cause. The
/// coordinator's filesystem is the one that matters here, so the message says so:
/// a client looking for the file on its own machine will not find it.
pub(super) fn file_error(verb: &str, path: &str, cause: &impl std::fmt::Display) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("COPY could not {verb} \"{path}\" on the coordinator: {cause}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::error::PgWireError;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    fn plan(sql: &str) -> CopyPlan {
        plan_copy(&parse_one(sql)).unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
    }

    /// The SQLSTATE and message a refused COPY reports.
    fn rejection(sql: &str) -> (String, String) {
        match plan_copy(&parse_one(sql)).expect_err("`{sql}` must be refused") {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    #[test]
    fn copy_to_a_file_plans_an_export_of_the_named_table() {
        assert_eq!(
            plan("COPY orders TO '/tmp/orders.csv' (FORMAT CSV, HEADER)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                endpoint: CopyEndpoint::File("/tmp/orders.csv".to_string()),
                format: CopyFormat::Csv(CsvDialect {
                    header: true,
                    ..Default::default()
                }),
            }
        );
    }

    // The names are catalog-canonical from here on: a quoted column keeps its case,
    // an unquoted one folds, and a schema qualifier is dropped like everywhere else.
    #[test]
    fn an_export_canonicalizes_the_table_and_column_names() {
        assert_eq!(
            plan("COPY public.Orders (ID, \"V\") TO '/tmp/o.csv' (FORMAT CSV)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec!["id".to_string(), "V".to_string()],
                },
                endpoint: CopyEndpoint::File("/tmp/o.csv".to_string()),
                format: CopyFormat::Csv(CsvDialect::default()),
            }
        );
    }

    #[test]
    fn copy_from_a_file_plans_an_import_into_the_named_table() {
        assert_eq!(
            plan("COPY orders (id, v) FROM '/tmp/orders.csv' (FORMAT CSV, HEADER)"),
            CopyPlan::In {
                table: "orders".to_string(),
                columns: vec!["id".to_string(), "v".to_string()],
                endpoint: CopyEndpoint::File("/tmp/orders.csv".to_string()),
                format: CopyFormat::Csv(CsvDialect {
                    header: true,
                    ..Default::default()
                }),
            }
        );
    }

    #[test]
    fn a_query_export_keeps_its_query() {
        let plan = plan("COPY (SELECT id FROM orders WHERE id > 1) TO '/tmp/o.csv' (FORMAT CSV)");
        assert!(
            matches!(
                plan,
                CopyPlan::Out {
                    source: CopyOutSource::Query(_),
                    ..
                }
            ),
            "got: {plan:?}"
        );
    }

    // The streaming forms plan like the file ones — same source, same dialect, only
    // the endpoint differs. That is the point of splitting the endpoint out: `\copy`
    // and a server-side file cannot disagree about what the statement meant.
    #[test]
    fn copy_to_stdout_plans_an_export_to_the_client() {
        assert_eq!(
            plan("COPY orders TO STDOUT (FORMAT CSV, HEADER)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                endpoint: CopyEndpoint::Client,
                format: CopyFormat::Csv(CsvDialect {
                    header: true,
                    ..Default::default()
                }),
            }
        );
    }

    // The trailing semicolon is not optional in the test: the parser reads whatever
    // follows it as the statement's inline data, which for a `COPY ... FROM STDIN`
    // sent on its own is one empty value — the statement's own newline, not data.
    #[test]
    fn copy_from_stdin_plans_an_import_from_the_client() {
        assert_eq!(
            plan("COPY orders (id, v) FROM STDIN (FORMAT CSV, HEADER);"),
            CopyPlan::In {
                table: "orders".to_string(),
                columns: vec!["id".to_string(), "v".to_string()],
                endpoint: CopyEndpoint::Client,
                format: CopyFormat::Csv(CsvDialect {
                    header: true,
                    ..Default::default()
                }),
            }
        );
    }

    // Rows written into the query text are not how the protocol carries them, so
    // accepting the statement would drop them. Refused rather than half-honoured.
    #[test]
    fn inline_data_in_the_query_text_is_refused() {
        let (code, msg) = rejection("COPY orders FROM STDIN (FORMAT CSV);\n1,x\n2,y\n\\.\n");
        assert_eq!(code, "0A000");
        assert!(msg.contains("FROM STDIN"), "got: {msg}");
    }

    #[test]
    fn copy_program_is_refused() {
        let (code, msg) = rejection("COPY orders TO PROGRAM 'gzip > /tmp/o.gz' (FORMAT CSV)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("shell"), "got: {msg}");
    }

    // PostgreSQL's default is TEXT, a different encoding. Writing CSV instead would
    // silently produce a file that whatever reads it next cannot parse.
    #[test]
    fn a_copy_that_does_not_ask_for_csv_is_refused() {
        for sql in [
            "COPY orders TO '/tmp/o.csv'",
            "COPY orders FROM '/tmp/o.csv'",
            "COPY orders TO '/tmp/o.csv' (DELIMITER '|')",
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("FORMAT CSV"), "`{sql}` got: {msg}");
        }
    }

    #[test]
    fn a_non_csv_format_is_refused_by_name() {
        for (sql, named) in [
            ("COPY orders TO '/tmp/o.txt' (FORMAT TEXT)", "TEXT"),
            ("COPY orders TO '/tmp/o.bin' (FORMAT BINARY)", "BINARY"),
            ("COPY orders TO '/tmp/o.bin' WITH BINARY", "BINARY"),
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(named), "`{sql}` got: {msg}");
        }
    }

    // Accepting an option and then ignoring it produces a file that does not hold
    // what the statement said it would.
    #[test]
    fn an_option_vairedb_would_have_to_ignore_is_refused() {
        for sql in [
            "COPY orders TO '/tmp/o.csv' (FORMAT CSV, NULL '\\N')",
            "COPY orders TO '/tmp/o.csv' (FORMAT CSV, FORCE_QUOTE (id))",
            "COPY orders FROM '/tmp/o.csv' (FORMAT CSV, ENCODING 'LATIN1')",
            "COPY orders FROM '/tmp/o.csv' (FORMAT CSV, FREEZE)",
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("not supported"), "`{sql}` got: {msg}");
        }
    }

    // The legacy spelling means the same thing, so it plans the same way.
    #[test]
    fn the_legacy_csv_spelling_plans_like_the_modern_one() {
        assert_eq!(
            plan("COPY orders TO '/tmp/o.csv' WITH CSV HEADER"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                endpoint: CopyEndpoint::File("/tmp/o.csv".to_string()),
                format: CopyFormat::Csv(CsvDialect {
                    header: true,
                    ..Default::default()
                }),
            }
        );
    }

    #[test]
    fn a_delimiter_and_quote_are_carried_into_the_dialect() {
        let CopyPlan::Out {
            format: CopyFormat::Csv(dialect),
            ..
        } = plan("COPY orders TO '/tmp/o.csv' (FORMAT CSV, DELIMITER ';', QUOTE '''')")
        else {
            panic!("expected a CSV export");
        };
        assert_eq!(dialect.delimiter, b';');
        assert_eq!(dialect.quote, b'\'');
    }

    // --- FORMAT PARQUET ---

    // A Parquet export plans exactly like a CSV one: the format is a property of the
    // file being written, not of what is being read out of the table.
    #[test]
    fn copy_to_a_parquet_file_plans_an_export() {
        assert_eq!(
            plan("COPY orders TO '/tmp/orders.parquet' (FORMAT PARQUET)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                endpoint: CopyEndpoint::File("/tmp/orders.parquet".to_string()),
                format: CopyFormat::Parquet,
            }
        );
    }

    #[test]
    fn copy_from_a_parquet_file_plans_an_import() {
        assert_eq!(
            plan("COPY orders (id, v) FROM '/tmp/orders.parquet' (FORMAT PARQUET)"),
            CopyPlan::In {
                table: "orders".to_string(),
                columns: vec!["id".to_string(), "v".to_string()],
                endpoint: CopyEndpoint::File("/tmp/orders.parquet".to_string()),
                format: CopyFormat::Parquet,
            }
        );
    }

    // A Parquet file's footer is written last and has to be read first, so the bytes
    // are not the row-at-a-time stream the copy protocol carries. Refused at planning
    // time, before the client is told to start sending or expecting data.
    #[test]
    fn parquet_over_the_copy_protocol_is_refused() {
        for sql in [
            "COPY orders TO STDOUT (FORMAT PARQUET)",
            "COPY orders FROM STDIN (FORMAT PARQUET);",
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("footer"), "`{sql}` got: {msg}");
            assert!(msg.contains("FORMAT CSV"), "`{sql}` got: {msg}");
        }
    }

    // A Parquet file names and types its own columns, so these options have nothing to
    // act on. The order of the options must not matter: the format decides which ones
    // are meaningful, and it can be stated after them.
    #[test]
    fn a_csv_only_option_is_refused_for_parquet_in_either_order() {
        for (sql, named) in [
            (
                "COPY orders TO '/tmp/o.parquet' (FORMAT PARQUET, HEADER)",
                "HEADER",
            ),
            (
                "COPY orders TO '/tmp/o.parquet' (HEADER, FORMAT PARQUET)",
                "HEADER",
            ),
            (
                "COPY orders FROM '/tmp/o.parquet' (FORMAT PARQUET, DELIMITER '|')",
                "DELIMITER",
            ),
            (
                "COPY orders FROM '/tmp/o.parquet' (QUOTE '\"', FORMAT PARQUET)",
                "QUOTE",
            ),
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(named), "`{sql}` got: {msg}");
        }
    }

    // --- target_columns ---

    #[test]
    fn a_stated_column_list_decides_the_mapping() {
        let columns = target_columns(
            &["v".to_string(), "id".to_string()],
            &["one".to_string(), "two".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns, vec!["v".to_string(), "id".to_string()]);
    }

    // A header names the file's columns, so the mapping is by name and the file's
    // order need not match the table's.
    #[test]
    fn a_header_names_the_columns_when_the_statement_does_not() {
        let columns = target_columns(
            &[],
            &["V".to_string(), "id".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns, vec!["v".to_string(), "id".to_string()]);
    }

    // Without a header there is nothing to map by name, so the fields land on the
    // table's leading columns — the rule PostgreSQL uses for a positional INSERT.
    #[test]
    fn a_headerless_file_is_positional_against_the_leading_columns() {
        let columns = target_columns(
            &[],
            &["column_1".to_string()],
            &["id".to_string(), "v".to_string()],
            false,
        )
        .unwrap();
        assert_eq!(columns, vec!["id".to_string()]);
    }

    #[test]
    fn a_column_list_that_does_not_match_the_files_width_is_refused() {
        let err = target_columns(
            &["id".to_string()],
            &["id".to_string(), "v".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .expect_err("a narrower column list than the file must be refused");
        let PgWireError::UserError(info) = err else {
            panic!("expected a user-facing error");
        };
        assert_eq!(info.code, "42601");
        assert!(info.message.contains("1 column(s)"), "{}", info.message);
        assert!(info.message.contains("2 field(s)"), "{}", info.message);
    }

    // --- select_from_table ---

    #[test]
    fn an_export_of_every_column_selects_star() {
        let sql = crate::write_sql_cl::statement_to_sql(&select_from_table("orders", &[]).unwrap());
        assert_eq!(sql, "SELECT * FROM \"orders\"");
    }

    #[test]
    fn an_export_of_named_columns_projects_them_quoted() {
        let columns = ["id".to_string(), "Total".to_string()];
        let sql =
            crate::write_sql_cl::statement_to_sql(&select_from_table("orders", &columns).unwrap());
        assert_eq!(sql, "SELECT \"id\", \"Total\" FROM \"orders\"");
    }

    // --- the handler, against an empty catalog and no reachable nodes ---

    use crate::catalog::{ColumnDef, TableMeta};

    /// Register a table sharded by `id` so a COPY can resolve it.
    fn register(handler: &VaireDbQueryHandler, name: &str, columns: &[&str]) {
        handler
            .catalog
            .put_table(&TableMeta {
                table_name: name.to_string(),
                columns: columns
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.to_string(),
                        data_type: "INTEGER".to_string(),
                        nullable: true,
                        default_expr: String::new(),
                    })
                    .collect(),
                shard_key: "id".to_string(),
                shard_count: 2,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
    }

    /// Write a CSV file for a COPY to read, returning its path.
    fn write_csv(name: &str, contents: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "vairedb_copy_test_{}_{name}.csv",
            std::process::id()
        ));
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// The SQLSTATE and message a COPY reports to the client.
    async fn copy_rejection(handler: &VaireDbQueryHandler, sql: &str) -> (String, String) {
        let session = SessionState::default();
        let err = handler
            .handle_copy(&parse_one(sql), &session)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be rejected"));
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    // Both directions name the table they could not find, rather than failing later
    // as a planner or file error.
    #[tokio::test]
    async fn a_copy_of_an_unknown_table_is_a_table_error() {
        let handler = VaireDbQueryHandler::for_tests(false);
        for sql in [
            "COPY nowhere TO '/tmp/nowhere.csv' (FORMAT CSV)",
            "COPY nowhere FROM '/tmp/nowhere.csv' (FORMAT CSV)",
        ] {
            let (code, msg) = copy_rejection(&handler, sql).await;
            assert_eq!(code, "42P01", "`{sql}`");
            assert!(msg.contains("nowhere"), "`{sql}` got: {msg}");
        }
    }

    // The file is the coordinator's, so the message says whose filesystem was
    // looked at — a client hunting for it locally would not find it.
    #[tokio::test]
    async fn a_missing_import_file_is_reported_against_the_coordinator() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        let missing = std::env::temp_dir().join("vairedb_copy_does_not_exist.csv");
        let (_code, msg) = copy_rejection(
            &handler,
            &format!(
                "COPY orders FROM '{}' (FORMAT CSV, HEADER)",
                missing.display()
            ),
        )
        .await;
        assert!(msg.contains("coordinator"), "got: {msg}");
        assert!(msg.contains("vairedb_copy_does_not_exist"), "got: {msg}");
    }

    // A header naming a column the table does not have is refused by name: the
    // alternative is dropping the field, which loses data without saying so.
    #[tokio::test]
    async fn a_file_column_the_table_does_not_have_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("unknown_column", "id,nope\n1,x\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert_eq!(code, "42703");
        assert!(msg.contains("nope"), "got: {msg}");
    }

    // Every row is placed by hashing its shard key, so a file without that column
    // has nowhere to go. Refused before a single row is written.
    #[tokio::test]
    async fn an_import_without_the_shard_key_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("no_shard_key", "v\nx\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders (v) FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert_eq!(code, "42601");
        assert!(msg.contains("shard key"), "got: {msg}");
        assert!(msg.contains("\"id\""), "got: {msg}");
    }

    // A file wider than the columns it is being mapped onto would silently shift
    // every value one column left.
    #[tokio::test]
    async fn an_import_whose_width_does_not_match_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("too_wide", "1,x,extra\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders (id, v) FROM '{path}' (FORMAT CSV)"),
        )
        .await;
        assert_eq!(code, "42601");
        assert!(msg.contains("2 column(s)"), "got: {msg}");
        assert!(msg.contains("3 field(s)"), "got: {msg}");
    }

    // An empty file is an empty import, not a malformed one — PostgreSQL answers
    // `COPY 0`. It used to be refused for having no columns, which is a file the
    // client can legitimately produce (an export of an empty table without HEADER).
    #[tokio::test]
    async fn an_empty_import_file_writes_no_rows() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("empty", "");

        let session = SessionState::default();
        let response = handler
            .handle_copy(
                &parse_one(&format!("COPY orders FROM '{path}' (FORMAT CSV, HEADER)")),
                &session,
            )
            .await
            .expect("an empty file is an empty import");
        assert_eq!(execution_tag(response), "COPY 0");
    }

    /// The command tag a completed COPY reports.
    fn execution_tag(response: Response) -> String {
        match response {
            Response::Execution(tag) => pgwire::messages::response::CommandComplete::from(tag).tag,
            other => panic!("expected a completed COPY, got {other:?}"),
        }
    }

    // `FROM STDIN` cannot answer with a row count: the rows have not been sent. It
    // hands the sink to the connection and tells the client to start.
    #[tokio::test]
    async fn copy_from_stdin_hands_the_sink_to_the_connection() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        let session = SessionState::default();
        let response = handler
            .handle_copy(
                &parse_one("COPY orders FROM STDIN (FORMAT CSV, HEADER);"),
                &session,
            )
            .await
            .expect("a streaming import opens rather than completing");
        assert!(
            matches!(response, Response::CopyIn(_)),
            "expected a CopyInResponse, got {response:?}"
        );
        assert!(
            session.copy_in().await.is_some(),
            "the sink must be waiting for the client's CopyData"
        );
    }

    // Everything the statement decides is still decided before the client is invited
    // to send anything — a client should not upload data for a copy that cannot work.
    #[tokio::test]
    async fn a_streaming_import_is_refused_before_the_client_sends() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        let session = SessionState::default();
        for (sql, expected) in [
            ("COPY nowhere FROM STDIN (FORMAT CSV);", "42P01"),
            ("COPY orders (id, nope) FROM STDIN (FORMAT CSV);", "42703"),
            ("COPY orders (v) FROM STDIN (FORMAT CSV);", "42601"),
        ] {
            let err = handler
                .handle_copy(&parse_one(sql), &session)
                .await
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be refused"));
            let PgWireError::UserError(info) = err else {
                panic!("`{sql}`: expected a user-facing error");
            };
            assert_eq!(info.code, expected, "`{sql}` got: {}", info.message);
            assert!(
                session.copy_in().await.is_none(),
                "`{sql}` must leave the connection out of copy mode"
            );
        }
    }

    // Those same refusals have to be reachable while the statement is only being
    // prepared, which is the whole point of the pre-check: a driver that hears
    // "no" at Parse never begins a copy, so it never queues the abort whose
    // trailing `Sync` would desynchronize the connection.
    #[tokio::test]
    async fn the_precheck_refuses_a_hopeless_streaming_import() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        for (sql, expected) in [
            ("COPY nowhere FROM STDIN (FORMAT CSV);", "42P01"),
            ("COPY orders (id, nope) FROM STDIN (FORMAT CSV);", "42703"),
            ("COPY orders (v) FROM STDIN (FORMAT CSV);", "42601"),
            // Not CSV, so there is no import VaireDB could perform at all.
            ("COPY orders FROM STDIN;", "0A000"),
            // Parquet cannot arrive as a stream, and hearing so at Parse is what keeps
            // the client from uploading a file it will only be told to abandon.
            ("COPY orders FROM STDIN (FORMAT PARQUET);", "0A000"),
        ] {
            let err = precheck_copy_from_stdin(&parse_one(sql), &handler.catalog)
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be refused at Parse"));
            let PgWireError::UserError(info) = err else {
                panic!("`{sql}`: expected a user-facing error");
            };
            assert_eq!(info.code, expected, "`{sql}` got: {}", info.message);
        }
    }

    // A copy that could work passes, and so does every direction the pre-check is
    // not about: those never invite the client to send, so nothing rides on when
    // they refuse and they stay Execute's business — including a missing table,
    // which the pre-check must not start reporting early for them.
    #[tokio::test]
    async fn the_precheck_passes_what_is_not_a_hopeless_streaming_import() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        for sql in [
            "COPY orders FROM STDIN (FORMAT CSV, HEADER);",
            "COPY orders (id) FROM STDIN (FORMAT CSV);",
            "COPY orders TO STDOUT (FORMAT CSV);",
            "COPY nowhere TO STDOUT (FORMAT CSV);",
            "COPY nowhere FROM '/tmp/nowhere.csv' (FORMAT CSV);",
        ] {
            precheck_copy_from_stdin(&parse_one(sql), &handler.catalog)
                .unwrap_or_else(|e| panic!("`{sql}` must not be refused at Parse: {e}"));
        }
    }

    // --- COPY ... TO STDOUT ---

    fn export_batch(rows: &[(&str, &str)]) -> (SchemaRef, RecordBatch) {
        use std::sync::Arc;

        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn exported(dialect: &CsvDialect, rows: &[(&str, &str)]) -> Vec<String> {
        let (schema, batch) = export_batch(rows);
        csv_row_messages(&schema, &[batch], dialect)
            .unwrap()
            .into_iter()
            .map(|m| String::from_utf8(m.data.to_vec()).unwrap())
            .collect()
    }

    // One message per row, because that is what pgwire counts to build `COPY n`.
    #[test]
    fn an_export_sends_one_message_per_row() {
        assert_eq!(
            exported(&CsvDialect::default(), &[("1", "x"), ("2", "y")]),
            vec!["1,x\n".to_string(), "2,y\n".to_string()]
        );
    }

    // A header cannot have a message of its own: it would be counted as a row. It
    // rides along in the first row's message instead.
    #[test]
    fn a_requested_header_rides_in_the_first_rows_message() {
        assert_eq!(
            exported(
                &CsvDialect {
                    header: true,
                    ..Default::default()
                },
                &[("1", "x"), ("2", "y")]
            ),
            vec!["id,v\n1,x\n".to_string(), "2,y\n".to_string()]
        );
    }

    // The one case that cannot come out right: with no rows to carry it, the header
    // needs a message of its own, and pgwire counts it — so the tag reads `COPY 1`
    // where PostgreSQL says `COPY 0`. Sending the header is the half worth keeping,
    // since a client reading the output back with HEADER needs it.
    #[test]
    fn an_empty_export_still_sends_a_requested_header() {
        assert_eq!(
            exported(
                &CsvDialect {
                    header: true,
                    ..Default::default()
                },
                &[]
            ),
            vec!["id,v\n".to_string()]
        );
    }

    #[test]
    fn an_empty_export_without_a_header_sends_nothing_at_all() {
        assert!(exported(&CsvDialect::default(), &[]).is_empty());
    }

    #[test]
    fn an_exports_dialect_reaches_the_rendered_rows() {
        let dialect = CsvDialect {
            header: true,
            delimiter: b';',
            quote: b'\'',
        };
        assert_eq!(
            exported(&dialect, &[("1", "a;b")]),
            vec!["id;v\n1;'a;b'\n".to_string()]
        );
    }

    // The rows in the file are read, mapped and validated; what stops this import
    // is that the cluster has no shards to ship them to — reported as such, with
    // nothing written.
    #[tokio::test]
    async fn a_valid_import_gets_as_far_as_routing_the_rows() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("routable", "id,v\n1,x\n2,y\n");

        // No shards are assigned in a test handler, so routing is where it stops.
        let (_code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert!(
            !msg.contains("column") && !msg.contains("shard key"),
            "the file must have been read and mapped, got: {msg}"
        );
    }

    // --- the handler, importing Parquet ---

    /// Write a Parquet file naming `columns` and holding one row, returning its path.
    /// Written through the export path, so what the import reads is what an export
    /// produces.
    async fn write_parquet(name: &str, columns: &[&str]) -> String {
        use std::sync::Arc;

        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&schema),
            columns
                .iter()
                .map(|c| Arc::new(StringArray::from(vec![*c])) as _)
                .collect(),
        )
        .unwrap();

        let path = std::env::temp_dir()
            .join(format!(
                "vairedb_copy_test_{}_{name}.parquet",
                std::process::id()
            ))
            .to_str()
            .unwrap()
            .to_string();
        copy_parquet::write_file(path.clone(), schema, vec![batch])
            .await
            .unwrap();
        path
    }

    // The table is resolved before the file is opened, so an import into a table that
    // does not exist says so rather than reporting whatever the file turned out to be.
    #[tokio::test]
    async fn a_parquet_import_of_an_unknown_table_is_a_table_error() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let path = write_parquet("unknown_table", &["id", "v"]).await;

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY nowhere FROM '{path}' (FORMAT PARQUET)"),
        )
        .await;
        assert_eq!(code, "42P01");
        assert!(msg.contains("nowhere"), "got: {msg}");
    }

    // The same message as the CSV lane: the path is the coordinator's, not the
    // client's, and a client hunting for it locally would not find it.
    #[tokio::test]
    async fn a_missing_parquet_file_is_reported_against_the_coordinator() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        let missing = std::env::temp_dir().join("vairedb_copy_does_not_exist.parquet");
        let (_code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{}' (FORMAT PARQUET)", missing.display()),
        )
        .await;
        assert!(msg.contains("coordinator"), "got: {msg}");
        assert!(msg.contains("vairedb_copy_does_not_exist"), "got: {msg}");
    }

    // A Parquet file names its own columns, so a column the table does not have is
    // refused by name — dropping it would lose data without saying so.
    #[tokio::test]
    async fn a_parquet_column_the_table_does_not_have_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_parquet("unknown_column", &["id", "nope"]).await;

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT PARQUET)"),
        )
        .await;
        assert_eq!(code, "42703");
        assert!(msg.contains("nope"), "got: {msg}");
    }

    // Every row is placed by hashing its shard key, so a file without that column has
    // nowhere to go. Refused before a single row is read.
    #[tokio::test]
    async fn a_parquet_import_without_the_shard_key_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_parquet("no_shard_key", &["v"]).await;

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT PARQUET)"),
        )
        .await;
        assert_eq!(code, "42601");
        assert!(msg.contains("shard key"), "got: {msg}");
        assert!(msg.contains("\"id\""), "got: {msg}");
    }

    // As with CSV: the file is opened, its columns mapped and validated, and what
    // stops the import is that the cluster has no shards to ship the rows to.
    #[tokio::test]
    async fn a_valid_parquet_import_gets_as_far_as_routing_the_rows() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_parquet("routable", &["id", "v"]).await;

        // No shards are assigned in a test handler, so routing is where it stops.
        let (_code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT PARQUET)"),
        )
        .await;
        assert!(
            !msg.contains("column") && !msg.contains("shard key"),
            "the file must have been read and mapped, got: {msg}"
        );
    }
}
