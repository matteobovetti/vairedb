//! The streaming half of `COPY`: turning CSV bytes into shard-routed writes as
//! they arrive, and driving PostgreSQL's copy sub-protocol for `FROM STDIN`.
//!
//! [`CopySink`] is where both `COPY ... FROM` forms meet. It takes CSV bytes in
//! arbitrary pieces — a `CopyData` message boundary lands wherever the client's
//! buffer ended, in the middle of a row or a quoted field — and hands rows to the
//! INSERT lane a batch at a time. Nothing here needs the whole input: a bulk load
//! is as large as the client's data, so buffering it would defeat the point.
//!
//! The rows are decoded as **text** and re-emitted as string literals, letting the
//! shard's engine apply each column's declared type exactly as it does for an
//! `INSERT ... VALUES ('...')`. That is deliberate. Typing them here would mean
//! either inferring types from the data — where the type a column gets depends on
//! which rows happened to land in the same batch, so the same file could import
//! differently — or reading the catalog's declared types, which would refuse
//! columns the literal renderer cannot express (`BYTEA` decodes to Arrow `Binary`)
//! and so import *less* than text does. Routing does not care: the shard key is
//! hashed from the value's text form either way.
//!
//! What this does not give is atomicity. A batch that fails after earlier batches
//! shipped leaves those rows written, and the error says so with
//! [`VdbErrorCode::PartialCommit`] rather than reporting a rollback that did not
//! happen. VaireDB has no cross-shard commit protocol, so this is the same
//! guarantee a single multi-row `INSERT` already has — a large `COPY` simply has
//! more places to reach it. Inside an explicit transaction block the rows are
//! buffered until `COMMIT` like any other write, which does make a copy into a
//! single shard atomic.
//!
//! `COPY ... TO STDOUT` is not here: it has no incremental state to keep, so it
//! stays in [`super::copy`] as a straight rendering of the collected result.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::csv::reader::{Decoder, ReaderBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use futures::SinkExt;
use futures::sink::Sink;
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::Tag;
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::TableMeta;
use crate::pgwire_handler::copy::{CsvDialect, target_columns, validate_target_columns};
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::session::SessionState;
use crate::sqlparser::ast::Statement;
use crate::write_sql_cl;

/// Rows decoded before a batch is shipped.
///
/// A multiple of the INSERT lane's own chunk size, so a batch turns into whole
/// statements rather than one full chunk and a remainder. Ten of them is the
/// trade-off between round trips to the shards and how much of the client's data
/// is held in the coordinator at once.
const ROWS_PER_BATCH: usize = 10 * write_sql_cl::ROWS_PER_STATEMENT;

/// How much of the input may go by without completing a first record.
///
/// The first record is the only one buffered whole, because it decides the column
/// list. A bound keeps a client that streams megabytes with no line terminator —
/// data that is not CSV at all — from growing the coordinator's memory until it
/// dies, rather than hearing that its data is malformed.
const MAX_FIRST_RECORD_BYTES: usize = 1024 * 1024;

/// CSV bytes in, INSERT statements out — the whole decoding half of a `COPY ...
/// FROM`, with no idea where the bytes came from or where the rows are going.
///
/// Kept free of the write path on purpose. Everything that can go wrong with a
/// bulk load's *data* — a chunk that ends mid-row, a header split across two
/// messages, a final record with no newline — is decided here, so it can be tested
/// as a function of the bytes rather than against a cluster.
///
/// Bytes are handed over with [`CsvRows::accept`] and statements taken with
/// [`CsvRows::next_statements`] until it yields `None`. That is what bounds memory:
/// the caller writes each batch before asking for the next, so a client sending one
/// enormous message does not turn into an enormous pile of statements.
struct CsvRows {
    table: String,
    dialect: CsvDialect,
    /// Every column of the target table, in declaration order: what a headerless
    /// file is matched against positionally.
    table_columns: Vec<String>,
    /// The column every row is routed by. Data that does not carry it is refused.
    shard_key: String,
    /// The column list the statement spelled out, canonical; empty if it did not.
    stated: Vec<String>,
    /// Bytes of the first record, until it is complete. Empty afterwards.
    head: Vec<u8>,
    /// Set once the column list is settled; from then on the decoder owns the bytes.
    body: Option<Body>,
    /// Accepted but not yet decoded. Never more than one client message plus
    /// whatever the decoder would not take.
    input: Vec<u8>,
    /// Whether the data so far ended on a record terminator. A final record without
    /// one is still a record, and [`CsvRows::end`] supplies it.
    ends_with_newline: bool,
    /// Set by [`CsvRows::end`]: no more bytes are coming, so a partial batch is a
    /// batch.
    ended: bool,
}

/// The decoding state a settled column list makes possible.
struct Body {
    /// `INSERT INTO <table> (<columns>) VALUES ...`, with the rows filled in per
    /// batch — the same template a client `INSERT` is chunked against.
    template: Statement,
    decoder: Decoder,
}

impl CsvRows {
    fn open(
        table: &str,
        columns: &[String],
        dialect: &CsvDialect,
        meta: &TableMeta,
    ) -> PgWireResult<Self> {
        let table_columns: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();
        // A column list the statement named is checked now, so a client that
        // misspelled a column hears about it in reply to the `COPY` itself instead of
        // after uploading its data. Without one there is nothing to check yet.
        if !columns.is_empty() {
            validate_target_columns(table, columns, &table_columns, &meta.shard_key)?;
        }
        Ok(Self {
            table: table.to_string(),
            dialect: dialect.clone(),
            table_columns,
            shard_key: meta.shard_key.clone(),
            stated: columns.to_vec(),
            head: Vec::new(),
            body: None,
            input: Vec::new(),
            // Nothing has been sent, so there is no unterminated record waiting.
            ends_with_newline: true,
            ended: false,
        })
    }

    fn table(&self) -> &str {
        &self.table
    }

    /// The column count to advertise in `CopyInResponse`.
    ///
    /// The client is told how many columns the copy has before it sends anything, so
    /// where the statement did not name them this is the table's own width — the
    /// count a `COPY t FROM STDIN` of every column has.
    fn advertised_columns(&self) -> usize {
        if self.stated.is_empty() {
            self.table_columns.len()
        } else {
            self.stated.len()
        }
    }

    /// Take the next piece of CSV. It may end anywhere: mid-row, mid-field, even
    /// mid-quote.
    fn accept(&mut self, bytes: &[u8]) {
        if let Some(last) = bytes.last() {
            self.ends_with_newline = *last == b'\n';
        }
        self.input.extend_from_slice(bytes);
    }

    /// Declare the data over.
    ///
    /// A client is under no obligation to end its data with a newline, so a final
    /// record without one is terminated here — otherwise the last row of data written
    /// without a trailing newline would be silently dropped.
    fn end(&mut self) {
        if !self.ends_with_newline {
            self.input.push(b'\n');
            self.ends_with_newline = true;
        }
        self.ended = true;
    }

    /// The statements for the next batch of rows, or `None` when the bytes accepted
    /// so far do not complete one.
    ///
    /// Call it until it answers `None`: a single `accept` can complete several
    /// batches, and after [`CsvRows::end`] the last partial batch is one too.
    fn next_statements(&mut self) -> PgWireResult<Option<Vec<Statement>>> {
        if self.body.is_none() {
            // The first record is the only one buffered whole, because it is what
            // decides the column list.
            self.head.append(&mut self.input);
            let Some((fields, consumed)) = first_record(&self.head, &self.dialect) else {
                if self.head.len() > MAX_FIRST_RECORD_BYTES {
                    return Err(make_vdb_error(
                        VdbErrorCode::InvalidTextRepresentation,
                        format!(
                            "COPY read {} bytes without finding the end of the first record: the data does not look like CSV. Check the DELIMITER and QUOTE options, and that the rows are newline-terminated",
                            self.head.len()
                        ),
                    ));
                }
                return Ok(None);
            };
            self.settle(&fields, consumed)?;
        }

        // Taken out so the decoder — which lives in `self.body` — and the byte buffer
        // can be held at the same time.
        let mut input = std::mem::take(&mut self.input);
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| internal("COPY decoded bytes before its columns were settled"))?;

        let mut from = 0;
        while from < input.len() {
            let consumed = body.decoder.decode(&input[from..]).map_err(csv_error)?;
            // Nothing consumed means the decoder is holding a full batch and will
            // take no more until it is flushed.
            if consumed == 0 {
                break;
            }
            from += consumed;
        }
        input.drain(..from);

        // Bytes the decoder would not take mean a full batch; the end of the data
        // means a partial one is all there is.
        let full = !input.is_empty();
        let batch = if full || self.ended {
            body.decoder.flush().map_err(csv_error)?
        } else {
            None
        };
        let template = body.template.clone();
        self.input = input;

        let Some(batch) = batch.filter(|b| b.num_rows() > 0) else {
            if full {
                return Err(internal(
                    "COPY could not make progress: the CSV decoder is full but produced no rows",
                ));
            }
            return Ok(None);
        };

        write_sql_cl::insert_statements_from_batches(
            &template,
            &[batch],
            write_sql_cl::ROWS_PER_STATEMENT,
        )
        .map(Some)
        .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))
    }

    /// Decide the column list from the first record and build the decoder for the
    /// rest, leaving the bytes still to be decoded in `input`.
    ///
    /// A header is consumed here — it named the columns and is not data. Without one
    /// the first record *is* data, so it goes back to the decoder untouched and only
    /// its field count was used.
    fn settle(&mut self, fields: &[String], consumed: usize) -> PgWireResult<()> {
        let target = target_columns(
            &self.stated,
            fields,
            &self.table_columns,
            self.dialect.header,
        )?;
        validate_target_columns(&self.table, &target, &self.table_columns, &self.shard_key)?;

        let refs: Vec<&str> = target.iter().map(String::as_str).collect();
        let template = write_sql_cl::insert_template(&self.table, &refs)
            .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;

        // Every field as text: see the module doc for why the catalog's declared
        // types are deliberately not used here.
        let schema = Arc::new(Schema::new(
            target
                .iter()
                .map(|c| Field::new(c, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        // `with_header(false)`: a header was already taken off above, and the decoder
        // must not mistake the first data row for one.
        let decoder = ReaderBuilder::new(schema)
            .with_header(false)
            .with_delimiter(self.dialect.delimiter)
            .with_quote(self.dialect.quote)
            .with_batch_size(ROWS_PER_BATCH)
            .build_decoder();

        self.input = if self.dialect.header {
            self.head.split_off(consumed)
        } else {
            std::mem::take(&mut self.head)
        };
        self.head = Vec::new();
        self.body = Some(Body { template, decoder });
        Ok(())
    }
}

/// A `COPY ... FROM` in progress: the CSV decoder of [`CsvRows`] joined to the
/// INSERT lane, which is what makes an imported row a routed, counted row.
pub(crate) struct CopySink {
    csv: CsvRows,
    rows: u64,
}

impl CopySink {
    /// Open a sink for `COPY <table> [(columns)] FROM ...`.
    pub(super) fn open(
        table: &str,
        columns: &[String],
        dialect: &CsvDialect,
        meta: &TableMeta,
    ) -> PgWireResult<Self> {
        Ok(Self {
            csv: CsvRows::open(table, columns, dialect, meta)?,
            rows: 0,
        })
    }

    /// The table being imported into, for messages and for the transaction's
    /// bookkeeping.
    pub(super) fn table(&self) -> &str {
        self.csv.table()
    }

    /// Rows shipped so far. Read after a failure to say what is already stored.
    pub(super) fn rows_written(&self) -> u64 {
        self.rows
    }

    /// The column count to advertise in `CopyInResponse`.
    pub(super) fn advertised_columns(&self) -> usize {
        self.csv.advertised_columns()
    }

    /// Take the next piece of CSV, writing whatever batches it completes.
    pub(super) async fn push(
        &mut self,
        handler: &VaireDbQueryHandler,
        session: &SessionState,
        bytes: &[u8],
    ) -> PgWireResult<()> {
        self.csv.accept(bytes);
        match self.write_ready_batches(handler, session).await {
            Ok(()) => Ok(()),
            Err(e) => Err(self.partial(e)),
        }
    }

    /// Finish the copy, returning the rows written.
    pub(super) async fn finish(
        &mut self,
        handler: &VaireDbQueryHandler,
        session: &SessionState,
    ) -> PgWireResult<u64> {
        self.csv.end();
        match self.write_ready_batches(handler, session).await {
            // An empty copy writes nothing and is not an error: PostgreSQL answers
            // `COPY 0` for one, so there is nothing to report and nothing to refuse —
            // not even a missing header, which an empty transfer cannot have.
            Ok(()) => Ok(self.rows),
            Err(e) => Err(self.partial(e)),
        }
    }

    async fn write_ready_batches(
        &mut self,
        handler: &VaireDbQueryHandler,
        session: &SessionState,
    ) -> PgWireResult<()> {
        while let Some(statements) = self.csv.next_statements()? {
            self.rows += handler
                .write_row_statements(&statements, &[], session, self.csv.table(), "COPY")
                .await?;
        }
        Ok(())
    }

    /// Report a failure honestly: as a partial commit once rows are stored, and as
    /// itself while nothing is.
    fn partial(&self, e: PgWireError) -> PgWireError {
        if self.rows == 0 {
            return e;
        }
        make_vdb_error(
            VdbErrorCode::PartialCommit,
            format!(
                "COPY partially applied: {} row(s) were written to \"{}\" and cannot be undone, then the copy failed. Inspect the table before retrying. Cause: {e}",
                self.rows,
                self.csv.table()
            ),
        )
    }
}

/// The first CSV record in `buf`, with the bytes it occupies including its
/// terminator — or `None` if the buffer does not hold a whole one yet.
///
/// Hand-parsed rather than handed to the CSV reader because of what it is *for*:
/// the fields it returns decide the column list, and a reader needs a schema —
/// which is the thing being decided — before it will read anything. It is only the
/// first record; every one after it is the decoder's.
///
/// Quote handling matches the reader's, so a header or first row whose names
/// contain the delimiter or a newline is read the same way here as its data is
/// later: a field opening with the quote character runs until the closing one, and
/// a doubled quote inside is one literal quote.
fn first_record(buf: &[u8], dialect: &CsvDialect) -> Option<(Vec<String>, usize)> {
    let mut fields = Vec::new();
    let mut field: Vec<u8> = Vec::new();
    let mut quoted = false;
    let mut i = 0;

    while i < buf.len() {
        let b = buf[i];
        if quoted {
            if b == dialect.quote {
                // A doubled quote is an escaped one; a single one closes the field.
                if buf.get(i + 1) == Some(&dialect.quote) {
                    field.push(dialect.quote);
                    i += 2;
                    continue;
                }
                quoted = false;
                i += 1;
                continue;
            }
            field.push(b);
            i += 1;
            continue;
        }
        if b == dialect.quote && field.is_empty() {
            quoted = true;
            i += 1;
            continue;
        }
        if b == dialect.delimiter {
            fields.push(String::from_utf8_lossy(&field).into_owned());
            field.clear();
            i += 1;
            continue;
        }
        if b == b'\n' {
            // A CRLF terminator: the CR belongs to the line ending, not the field.
            if field.last() == Some(&b'\r') {
                field.pop();
            }
            fields.push(String::from_utf8_lossy(&field).into_owned());
            return Some((fields, i + 1));
        }
        field.push(b);
        i += 1;
    }

    None
}

/// CSV the decoder could not read.
///
/// Reported as a data error rather than a syntax error: the statement was fine, its
/// data was not. PostgreSQL's own `22P04 bad_copy_file_format` has no VaireDB code,
/// and `22P02` — an invalid text representation — is the nearest thing that says the
/// same about the bytes.
fn csv_error(e: ArrowError) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InvalidTextRepresentation,
        format!("COPY could not read the data as CSV: {e}"),
    )
}

fn internal(message: &str) -> PgWireError {
    make_vdb_error(VdbErrorCode::InternalError, message.to_string())
}

/// `CopyData` arriving outside a copy — a protocol error, since the client was
/// never sent a `CopyInResponse`.
fn no_copy_in_progress() -> PgWireError {
    make_vdb_error(
        VdbErrorCode::SqlSyntaxError,
        "no COPY ... FROM STDIN is in progress on this connection",
    )
}

/// The copy sub-protocol, on the handler the `COPY` statement itself ran on.
///
/// The same object on purpose: importing a row is an INSERT, so the sink calls
/// straight into the write path rather than reaching it through anything new. The
/// in-progress state cannot live here — this handler is one `Arc` shared by every
/// connection — so it lives in the connection's own
/// [`SessionState`](crate::pgwire_handler::session::SessionState), which is also
/// what makes a client that vanishes mid-copy clean up by itself.
#[async_trait]
impl CopyHandler for VaireDbQueryHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = SessionState::for_client(&*client);
        let mut in_progress = session.copy_in().await;
        let sink = in_progress.as_mut().ok_or_else(no_copy_in_progress)?;

        match sink.push(self, &session, copy_data.data.as_ref()).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // The copy is over: pgwire leaves copy mode when this error is
                // reported, so the sink must go with it or the next `COPY` on this
                // connection would inherit its half-decoded state.
                *in_progress = None;
                Err(e)
            }
        }
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = SessionState::for_client(&*client);
        // Taken, not borrowed: the copy is finished either way, so the sink does not
        // survive this call whether it succeeds or fails.
        let mut sink = session
            .copy_in()
            .await
            .take()
            .ok_or_else(no_copy_in_progress)?;

        let rows = sink.finish(self, &session).await?;

        // The command tag is this function's to send: pgwire sends `ReadyForQuery`
        // around this call but never a tag, and without one a client would see the
        // copy accepted and never learn how many rows it wrote.
        let tag = Tag::new("COPY").with_rows(rows as usize);
        client
            .send(PgWireBackendMessage::CommandComplete(tag.into()))
            .await?;

        // Leaving copy mode is this function's to do as well, and only on the
        // extended protocol does anyone notice. pgwire returns the connection to
        // `ReadyForQuery` itself after a copy that a simple `Query` started, but for
        // one started by `Execute` it deliberately does not — it defers to the
        // `Sync` that follows `CopyDone`. That `Sync` never arrives anywhere: its
        // own dispatch loop only looks at `CopyData`/`CopyDone`/`CopyFail` while the
        // state says a copy is in progress and drops everything else, so the state
        // that would be cleared by the message it is waiting for is the reason the
        // message is discarded. The connection then swallows every later `Parse`,
        // `Bind` and `Execute` in silence — a client's *second* `COPY ... FROM
        // STDIN` waits for a `BindComplete` that cannot come. Clearing the state
        // here is what lets the trailing `Sync` be seen.
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = SessionState::for_client(&*client);
        let sink = session.copy_in().await.take();

        // Whatever the client's reason, the rows already shipped are stored — there
        // is no cross-shard rollback to undo them with. Saying so is the whole value
        // of this arm: a client that aborts a copy needs to know the table is not as
        // it was, and which table.
        match sink {
            Some(sink) if sink.rows_written() > 0 => make_vdb_error(
                VdbErrorCode::PartialCommit,
                format!(
                    "COPY aborted by the client after {} row(s) were written to \"{}\", which cannot be undone. Inspect the table before retrying. Client's reason: {}",
                    sink.rows_written(),
                    sink.table(),
                    fail.message
                ),
            ),
            _ => make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!(
                    "COPY aborted by the client with nothing written. Client's reason: {}",
                    fail.message
                ),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;

    fn meta(columns: &[&str], shard_key: &str) -> TableMeta {
        TableMeta {
            table_name: "orders".to_string(),
            columns: columns
                .iter()
                .map(|c| ColumnDef {
                    name: (*c).to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    ..Default::default()
                })
                .collect(),
            shard_key: shard_key.to_string(),
            ..Default::default()
        }
    }

    fn csv(header: bool) -> CsvDialect {
        CsvDialect {
            header,
            delimiter: b',',
            quote: b'"',
        }
    }

    // --- The first record, which is the only one parsed here ---

    #[test]
    fn a_record_is_not_returned_until_its_terminator_arrives() {
        assert_eq!(first_record(b"id,v", &csv(true)), None);
        let (fields, consumed) = first_record(b"id,v\n1,x\n", &csv(true)).expect("terminated");
        assert_eq!(fields, vec!["id".to_string(), "v".to_string()]);
        assert_eq!(consumed, 5, "only the first record is consumed");
    }

    #[test]
    fn a_crlf_terminator_is_not_part_of_the_last_field() {
        let (fields, consumed) = first_record(b"id,v\r\n", &csv(true)).expect("terminated");
        assert_eq!(fields, vec!["id".to_string(), "v".to_string()]);
        assert_eq!(consumed, 6);
    }

    // A newline inside quotes is data, so a record split there is still incomplete —
    // this is the boundary a naive `split(b'\n')` gets wrong.
    #[test]
    fn a_newline_inside_quotes_does_not_end_the_record() {
        assert_eq!(first_record(b"\"a\nb\",v", &csv(true)), None);
        let (fields, _) = first_record(b"\"a\nb\",v\n", &csv(true)).expect("terminated");
        assert_eq!(fields, vec!["a\nb".to_string(), "v".to_string()]);
    }

    #[test]
    fn a_doubled_quote_is_one_literal_quote() {
        let (fields, _) = first_record(b"\"a\"\"b\",v\n", &csv(true)).expect("terminated");
        assert_eq!(fields, vec!["a\"b".to_string(), "v".to_string()]);
    }

    #[test]
    fn the_delimiter_inside_quotes_is_data() {
        let (fields, _) = first_record(b"\"a,b\",v\n", &csv(true)).expect("terminated");
        assert_eq!(fields, vec!["a,b".to_string(), "v".to_string()]);
    }

    #[test]
    fn the_dialect_decides_what_separates_and_what_quotes() {
        let dialect = CsvDialect {
            header: true,
            delimiter: b'|',
            quote: b'\'',
        };
        let (fields, _) = first_record(b"'a|b'|v\n", &dialect).expect("terminated");
        assert_eq!(fields, vec!["a|b".to_string(), "v".to_string()]);
    }

    // --- What the statement alone decides is decided before any data ---

    /// Whether a sink can be opened at all, discarding the sink itself — which holds
    /// a CSV decoder and so is not printable.
    fn opened(stated: &[&str], dialect: &CsvDialect) -> PgWireResult<()> {
        CopySink::open(
            "orders",
            &stated.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
            dialect,
            &meta(&["id", "v"], "id"),
        )
        .map(|_| ())
    }

    #[test]
    fn a_stated_column_the_table_does_not_have_is_refused_at_once() {
        let err = opened(&["id", "nope"], &csv(false))
            .expect_err("a misspelled column must not wait for the client's data");
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    #[test]
    fn a_stated_column_list_without_the_shard_key_is_refused_at_once() {
        let err = opened(&["v"], &csv(false))
            .expect_err("rows that cannot be routed must be refused before they are sent");
        assert!(err.to_string().contains("shard key"), "got: {err}");
    }

    #[test]
    fn columns_the_statement_did_not_name_cannot_be_judged_yet() {
        let sink = CopySink::open("orders", &[], &csv(true), &meta(&["id", "v"], "id"))
            .expect("a COPY without a column list is decided from the data");
        // The header has not arrived, so the width advertised is the table's own.
        assert_eq!(sink.advertised_columns(), 2);
        assert_eq!(sink.rows_written(), 0);
    }

    #[test]
    fn a_stated_column_list_is_the_advertised_width() {
        let sink = CopySink::open(
            "orders",
            &["id".to_string()],
            &csv(false),
            &meta(&["id", "v"], "id"),
        )
        .expect("naming the shard key alone is a valid copy");
        assert_eq!(sink.advertised_columns(), 1);
    }

    // --- Bytes in, INSERT statements out ---

    /// The SQL a sequence of client messages turns into. `chunks` are handed over
    /// exactly as given, so where one ends is the boundary under test.
    fn imported(
        dialect: &CsvDialect,
        stated: &[&str],
        table: &[&str],
        chunks: &[&[u8]],
    ) -> PgWireResult<Vec<String>> {
        let mut rows = CsvRows::open(
            "orders",
            &stated.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
            dialect,
            &meta(table, "id"),
        )?;

        let mut sql = Vec::new();
        for chunk in chunks {
            rows.accept(chunk);
            while let Some(statements) = rows.next_statements()? {
                sql.extend(statements.iter().map(write_sql_cl::statement_to_sql));
            }
        }
        rows.end();
        while let Some(statements) = rows.next_statements()? {
            sql.extend(statements.iter().map(write_sql_cl::statement_to_sql));
        }
        Ok(sql)
    }

    fn one_statement(
        dialect: &CsvDialect,
        stated: &[&str],
        chunks: &[&[u8]],
    ) -> PgWireResult<String> {
        let sql = imported(dialect, stated, &["id", "v"], chunks)?;
        assert_eq!(sql.len(), 1, "expected one statement, got: {sql:?}");
        Ok(sql.into_iter().next().expect("one statement"))
    }

    // The values are rendered as text literals, leaving each column's declared type
    // to the shard's engine — see the module doc.
    #[test]
    fn rows_become_one_insert_of_text_literals() {
        let sql = one_statement(&csv(true), &[], &[b"id,v\n1,x\n2,y\n"]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ('1', 'x'), ('2', 'y')"
        );
    }

    // The point of the whole design: a message boundary is not a row boundary, and
    // where it falls must not change the rows. Every split of the same data — and one
    // byte at a time is every split — has to give the same INSERT.
    #[test]
    fn where_the_chunks_end_does_not_change_the_rows() {
        let data = b"id,v\n1,x\n2,y\n3,z\n";
        let whole = one_statement(&csv(true), &[], &[data]).unwrap();

        for split in 1..data.len() {
            let sql = one_statement(&csv(true), &[], &[&data[..split], &data[split..]]).unwrap();
            assert_eq!(sql, whole, "split at byte {split} changed the rows");
        }

        let byte_at_a_time: Vec<&[u8]> = data.chunks(1).collect();
        assert_eq!(
            one_statement(&csv(true), &[], &byte_at_a_time).unwrap(),
            whole
        );
    }

    // A quoted field can contain the delimiter and the record terminator, so a chunk
    // that ends inside one leaves the decoder mid-value — the case a byte scan for
    // `\n` gets wrong.
    #[test]
    fn a_chunk_may_end_inside_a_quoted_field() {
        let sql = one_statement(
            &csv(true),
            &[],
            &[b"id,v\n1,\"a,b", b"\nc\"\n2,\"d\"\"e\"\n"],
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ('1', 'a,b\nc'), ('2', 'd\"e')"
        );
    }

    // The header is what decides the column list, so a header split across messages
    // has to be reassembled before anything can be decoded at all.
    #[test]
    fn a_header_split_across_chunks_still_names_the_columns() {
        let sql = one_statement(&csv(true), &[], &[b"v", b",i", b"d\n", b"x,1\n"]).unwrap();
        assert_eq!(
            sql, "INSERT INTO \"orders\" (\"v\", \"id\") VALUES ('x', '1')",
            "the file's own column order is kept"
        );
    }

    // Data written without a trailing newline is still data. Dropping its last row is
    // the silent-loss bug this pins.
    #[test]
    fn a_final_record_without_a_newline_is_still_a_row() {
        let sql = one_statement(&csv(true), &[], &[b"id,v\n1,x\n2,y"]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ('1', 'x'), ('2', 'y')"
        );
    }

    #[test]
    fn a_single_unterminated_record_is_a_row() {
        let sql = one_statement(&csv(false), &[], &[b"1,x"]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ('1', 'x')"
        );
    }

    // An empty field is NULL, as it is for the file import this shares its decoder
    // with. PostgreSQL's CSV mode reads a quoted empty field as the empty string
    // instead; arrow-csv does not distinguish the two, so both arrive as NULL.
    #[test]
    fn an_empty_field_is_null() {
        let sql = one_statement(&csv(true), &[], &[b"id,v\n1,\n"]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ('1', NULL)"
        );
    }

    // Nothing sent at all is `COPY 0`, not an error — not even the missing header a
    // non-empty transfer would be refused for.
    #[test]
    fn an_empty_transfer_writes_nothing() {
        assert!(
            imported(&csv(true), &[], &["id", "v"], &[])
                .unwrap()
                .is_empty(),
            "an empty copy has no statements to write"
        );
        assert!(
            imported(&csv(true), &[], &["id", "v"], &[b""])
                .unwrap()
                .is_empty()
        );
    }

    // A stated column list overrides the header, and the columns the file does not
    // carry are left to their defaults.
    #[test]
    fn a_stated_column_list_decides_what_is_written() {
        let sql = imported(&csv(false), &["id"], &["id", "v"], &[b"1\n2\n"]).unwrap();
        assert_eq!(
            sql,
            vec!["INSERT INTO \"orders\" (\"id\") VALUES ('1'), ('2')".to_string()]
        );
    }

    // A batch is shipped as it fills rather than at the end, so a load larger than
    // the batch size never has all of itself in the coordinator at once. The chunk
    // size the INSERT lane uses shows through as several statements per batch.
    #[test]
    fn a_load_larger_than_one_batch_is_written_in_pieces() {
        let mut data = String::from("id,v\n");
        for i in 0..(ROWS_PER_BATCH + write_sql_cl::ROWS_PER_STATEMENT) {
            data.push_str(&format!("{i},x\n"));
        }
        let sql = imported(&csv(true), &[], &["id", "v"], &[data.as_bytes()]).unwrap();

        let expected =
            (ROWS_PER_BATCH + write_sql_cl::ROWS_PER_STATEMENT) / write_sql_cl::ROWS_PER_STATEMENT;
        assert_eq!(sql.len(), expected, "one statement per INSERT chunk");
    }

    // Data that is not CSV at all must be refused rather than buffered until the
    // coordinator runs out of memory.
    #[test]
    fn data_with_no_record_terminator_is_refused_once_it_is_absurd() {
        let blob = vec![b'x'; MAX_FIRST_RECORD_BYTES + 1];
        let err = imported(&csv(true), &[], &["id", "v"], &[&blob])
            .expect_err("a megabyte with no newline is not a CSV header");
        assert!(
            err.to_string().contains("does not look like CSV"),
            "got: {err}"
        );
    }

    // --- Settling the column list from the data ---

    #[test]
    fn a_header_naming_a_column_the_table_lacks_is_refused() {
        let err = imported(&csv(true), &[], &["id", "v"], &[b"id,nope\n1,x\n"])
            .expect_err("a header is checked as strictly as a stated column list");
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    #[test]
    fn a_headerless_record_wider_than_the_table_is_refused() {
        let err = imported(&csv(false), &[], &["id", "v"], &[b"1,x,extra\n"])
            .expect_err("three fields cannot be matched against two columns");
        let message = err.to_string();
        assert!(message.contains("2 column(s)"), "got: {message}");
        assert!(message.contains("3 field(s)"), "got: {message}");
    }

    #[test]
    fn data_without_the_shard_key_is_refused_once_the_header_says_so() {
        let err = imported(&csv(true), &[], &["id", "v"], &[b"v\nx\n"])
            .expect_err("a header that omits the shard key cannot be routed");
        assert!(err.to_string().contains("shard key"), "got: {err}");
    }
}
