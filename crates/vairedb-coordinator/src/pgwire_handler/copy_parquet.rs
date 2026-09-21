//! The Parquet half of `COPY`: a columnar file on the coordinator's disk, read
//! into the INSERT lane a batch at a time and written out of the result the read
//! path collected.
//!
//! Parquet is here for the reason an analytical store wants it: each column's name
//! and type travel *inside* the file, so an export and the import that reads it
//! back cannot disagree about what a row means the way CSV's `HEADER` /
//! `DELIMITER` / `QUOTE` let them. That is also why the format takes no options at
//! all — there is no spelling of a Parquet file for a client to choose, so a CSV
//! option named against it is refused rather than accepted and ignored (see
//! [`super::copy`]).
//!
//! Two properties of the file format shape everything below.
//!
//! * **A file, never a stream.** A Parquet file's footer — the schema, and where
//!   every row group lives — is written last and has to be read first, so the
//!   bytes are not a sequence of rows the copy sub-protocol could carry one
//!   `CopyData` at a time. `FORMAT PARQUET` is therefore accepted only for the
//!   server-side file forms, and `STDIN`/`STDOUT` are refused while the statement
//!   is still being planned rather than part-way through a transfer.
//! * **The file's own types, not text.** The CSV lane deliberately decodes every
//!   field as text and leaves each column's declared type to the shard's engine
//!   (see [`super::copy_stream`]); a Parquet file already *says* what each column
//!   is, so its batches reach the INSERT lane with the Arrow types they were read
//!   at. A type with no SQL literal that stores the same value — `Binary` for a
//!   `BYTEA` column, a list, a struct — is refused naming the column rather than
//!   written as something else.
//!
//! Both directions do their I/O on a blocking thread: the file is on the
//! coordinator's own disk and a bulk load is as large as the client's data, which
//! is not something to hold an async worker for.

use std::fs::File;
use std::io::BufWriter;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::TableMeta;
use crate::pgwire_handler::copy::{
    ROWS_PER_BATCH, file_error, target_columns, validate_target_columns,
};
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::Statement;
use crate::write_sql_cl;

/// A Parquet file on the coordinator's disk, open and handed over a batch at a
/// time.
///
/// The batches are read one at a time rather than collected, for the same reason
/// the CSV lane decodes incrementally: a bulk import is as large as the client's
/// file, so nothing here needs the file to fit in memory.
pub(super) struct ParquetFile {
    /// Named in every error, because the filesystem the file was looked for on is
    /// the coordinator's rather than the client's.
    path: String,
    /// The file's column names, in file order. A Parquet file always names its
    /// columns, so this is what the import maps onto the table's.
    fields: Vec<String>,
    /// `None` once the last batch has been read, which also drops the file handle
    /// rather than holding one open for a file with nothing left in it.
    reader: Option<ParquetRecordBatchReader>,
}

impl ParquetFile {
    /// Open `path` and read its footer, which is what names and types its columns.
    pub(super) async fn open(path: &str) -> PgWireResult<Self> {
        let opening = path.to_string();
        let (fields, reader) = tokio::task::spawn_blocking(move || open_blocking(&opening))
            .await
            .map_err(|e| task_error("read", path, &e))??;
        Ok(Self {
            path: path.to_string(),
            fields,
            reader: Some(reader),
        })
    }

    /// The file's column names, in file order.
    pub(super) fn fields(&self) -> &[String] {
        &self.fields
    }

    /// The next batch of rows, or `None` at the end of the file.
    ///
    /// The reader is moved into the blocking task and back rather than borrowed:
    /// decoding a row group is CPU and disk work, and the reader is not something
    /// a `&mut` can cross a `spawn_blocking` boundary with.
    pub(super) async fn next_batch(&mut self) -> PgWireResult<Option<RecordBatch>> {
        let Some(mut reader) = self.reader.take() else {
            return Ok(None);
        };
        let (next, reader) = tokio::task::spawn_blocking(move || {
            let next = reader.next();
            (next, reader)
        })
        .await
        .map_err(|e| task_error("read", &self.path, &e))?;

        match next {
            Some(Ok(batch)) => {
                self.reader = Some(reader);
                Ok(Some(batch))
            }
            Some(Err(e)) => Err(read_error(&self.path, &e)),
            // The file is done; the reader goes with it.
            None => Ok(None),
        }
    }
}

/// Open the file and build the reader — the blocking half of [`ParquetFile::open`].
fn open_blocking(path: &str) -> PgWireResult<(Vec<String>, ParquetRecordBatchReader)> {
    let file = File::open(path).map_err(|e| file_error("read", path, &e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| read_error(path, &e))?
        .with_batch_size(ROWS_PER_BATCH);
    let fields = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let reader = builder.build().map_err(|e| read_error(path, &e))?;
    Ok((fields, reader))
}

/// The `INSERT` template a Parquet import writes its rows through.
///
/// A Parquet file always names its columns, so the mapping is by name exactly as a
/// CSV file's `HEADER` makes it: [`target_columns`] is asked for the header rule.
/// The statement's own column list still wins, and is still matched positionally
/// against the file's fields.
///
/// Everything decidable before a row is read is decided here — the columns exist
/// on the table and they carry the shard key — so a client hears about a file it
/// cannot import before anything is written.
pub(super) fn insert_template(
    table: &str,
    stated: &[String],
    file_fields: &[String],
    meta: &TableMeta,
) -> PgWireResult<Statement> {
    let table_columns: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();
    // `true`: the file names its columns, which is what a CSV header does.
    let target = target_columns(stated, file_fields, &table_columns, true)?;
    validate_target_columns(table, &target, &table_columns, &meta.shard_key)?;

    let refs: Vec<&str> = target.iter().map(String::as_str).collect();
    write_sql_cl::insert_template(table, &refs)
        .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))
}

/// The `INSERT ... VALUES` statements one batch of the file becomes, chunked the
/// way a client's own multi-row `INSERT` is.
///
/// The batch's columns are in file order, which [`insert_template`] already mapped
/// onto the target columns in the same order, so the positional match the INSERT
/// lane makes is the mapping the statement asked for.
pub(super) fn insert_statements(
    template: &Statement,
    batch: &RecordBatch,
) -> PgWireResult<Vec<Statement>> {
    write_sql_cl::insert_statements_from_batches(
        template,
        std::slice::from_ref(batch),
        write_sql_cl::ROWS_PER_STATEMENT,
    )
    .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))
}

/// Write collected batches to `path` as Parquet, truncating whatever was there —
/// which is what PostgreSQL's `COPY ... TO` does to a file.
///
/// `schema` is used only for a result with no batches to take one from: an export
/// of an empty table still has to produce a file that names its columns, or the
/// import that reads it back would have nothing to map.
///
/// Runs on a blocking thread: the file is on the coordinator's disk and an export
/// is as large as the result.
pub(super) async fn write_file(
    path: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> PgWireResult<()> {
    let reported = path.clone();
    tokio::task::spawn_blocking(move || write_blocking(&path, &schema, &batches))
        .await
        .map_err(|e| task_error("write", &reported, &e))?
}

fn write_blocking(path: &str, schema: &SchemaRef, batches: &[RecordBatch]) -> PgWireResult<()> {
    // The file describes the rows it holds, so its schema is the batches' own where
    // there are any; `schema` is the fallback for an empty result, which has no
    // batch to take one from.
    let schema = match batches.first() {
        Some(batch) => batch.schema(),
        None => SchemaRef::clone(schema),
    };

    let file = File::create(path).map_err(|e| file_error("write", path, &e))?;
    // Snappy is what every other writer in the ecosystem produces by default, so a
    // file VaireDB exports opens in DuckDB, pandas and Spark without the client
    // having to be told anything about it.
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema, Some(properties))
        .map_err(|e| write_error(path, &e))?;
    for batch in batches {
        writer.write(batch).map_err(|e| write_error(path, &e))?;
    }

    // `into_inner` is what writes the footer, and the footer is what makes the bytes
    // a Parquet file at all: without it no reader will open them. The `BufWriter` it
    // hands back still holds the tail of that footer, so a failure to flush or sync
    // is a truncated file — which the client has to hear about rather than discover
    // the next time it reads.
    writer
        .into_inner()
        .map_err(|e| write_error(path, &e))?
        .into_inner()
        .map_err(|e| file_error("write", path, &e))?
        .sync_all()
        .map_err(|e| file_error("write", path, &e))
}

/// Bytes the coordinator could read but not make a Parquet file of.
///
/// A data error rather than a syntax error, exactly as the CSV lane reports its own
/// malformed input: the statement was fine, the file was not. Taken as anything
/// printable because the footer and the row groups fail as different error types —
/// a `ParquetError` while opening, an `ArrowError` while decoding — and a client
/// has no use for the distinction.
fn read_error(path: &str, cause: &impl std::fmt::Display) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InvalidTextRepresentation,
        format!("COPY could not read \"{path}\" on the coordinator as Parquet: {cause}"),
    )
}

/// A result Parquet cannot hold, or a file it could not be written to.
///
/// The type a column has is the likely cause and the message carries it, since
/// Parquet has no encoding for a few of Arrow's types.
fn write_error(path: &str, cause: &impl std::fmt::Display) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("COPY could not write \"{path}\" on the coordinator as Parquet: {cause}"),
    )
}

/// The blocking task doing the file's I/O did not come back.
fn task_error(verb: &str, path: &str, cause: &tokio::task::JoinError) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("COPY could not finish {verb}ing \"{path}\" as Parquet: {cause}"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

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

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: &[i32], vs: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vs.to_vec())),
            ],
        )
        .unwrap()
    }

    /// A path in the temp directory, unique per test and per process.
    fn temp_path(name: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "vairedb_parquet_test_{}_{name}.parquet",
                std::process::id()
            ))
            .to_str()
            .expect("a temp path is UTF-8")
            .to_string()
    }

    /// Every statement a file's batches turn into, as SQL.
    async fn imported(path: &str, stated: &[&str], table: &[&str]) -> PgWireResult<Vec<String>> {
        let mut file = ParquetFile::open(path).await?;
        let stated: Vec<String> = stated.iter().map(|s| (*s).to_string()).collect();
        let template = insert_template("orders", &stated, file.fields(), &meta(table, "id"))?;

        let mut sql = Vec::new();
        while let Some(batch) = file.next_batch().await? {
            sql.extend(
                insert_statements(&template, &batch)?
                    .iter()
                    .map(write_sql_cl::statement_to_sql),
            );
        }
        Ok(sql)
    }

    // The round trip that matters: what an export writes is what an import reads,
    // with each column's name and type carried by the file rather than by options.
    #[tokio::test]
    async fn a_written_file_reads_back_as_the_rows_it_held() {
        let path = temp_path("round_trip");
        write_file(path.clone(), schema(), vec![batch(&[1, 2], &["x", "y"])])
            .await
            .expect("the export must write a file");

        let sql = imported(&path, &[], &["id", "v"]).await.unwrap();
        assert_eq!(
            sql,
            vec!["INSERT INTO \"orders\" (\"id\", \"v\") VALUES (1, 'x'), (2, 'y')".to_string()],
            "the file's own types decide the literals: an integer bare, text quoted"
        );
    }

    // An export of an empty table is an empty file, not a missing one — and it still
    // names its columns, so the import that reads it back has something to map.
    #[tokio::test]
    async fn an_empty_export_still_names_its_columns() {
        let path = temp_path("empty");
        write_file(path.clone(), schema(), Vec::new())
            .await
            .expect("an empty result is an empty file");

        let mut file = ParquetFile::open(&path).await.unwrap();
        assert_eq!(file.fields(), ["id".to_string(), "v".to_string()]);
        assert!(
            file.next_batch().await.unwrap().is_none(),
            "an empty file has no batches"
        );
    }

    // A file wider than the columns it is mapped onto would shift every value one
    // column left, so it is refused rather than half-imported.
    #[tokio::test]
    async fn a_file_the_table_cannot_take_is_refused_before_a_row_is_read() {
        let path = temp_path("unknown_column");
        let wrong = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("nope", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&wrong),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        write_file(path.clone(), wrong, vec![batch]).await.unwrap();

        let err = imported(&path, &[], &["id", "v"])
            .await
            .expect_err("a column the table does not have must be refused");
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    // The file's names are mapped the way a CSV header's are, so the file's own
    // column order need not match the table's.
    #[tokio::test]
    async fn the_files_column_order_is_the_files_own() {
        let path = temp_path("reordered");
        let reordered = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&reordered),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        write_file(path.clone(), reordered, vec![batch])
            .await
            .unwrap();

        let sql = imported(&path, &[], &["id", "v"]).await.unwrap();
        assert_eq!(
            sql,
            vec!["INSERT INTO \"orders\" (\"v\", \"id\") VALUES ('x', 1)".to_string()]
        );
    }

    // Every row is placed by hashing its shard key, so a file that does not carry it
    // has nowhere to go.
    #[tokio::test]
    async fn a_file_without_the_shard_key_is_refused() {
        let path = temp_path("no_shard_key");
        let narrow = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&narrow),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        write_file(path.clone(), narrow, vec![batch]).await.unwrap();

        let err = imported(&path, &[], &["id", "v"])
            .await
            .expect_err("rows that cannot be routed must be refused");
        assert!(err.to_string().contains("shard key"), "got: {err}");
    }

    // A type with no literal that stores the same value is refused naming the column.
    // The CSV lane can import such a column because it reads text; a Parquet file
    // says the column is binary, and hex digits are not the bytes.
    #[tokio::test]
    async fn a_type_with_no_literal_form_is_refused_naming_the_column() {
        let path = temp_path("binary");
        let binary = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            SchemaRef::clone(&binary),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::BinaryArray::from(vec![
                    b"\x00\x01".as_ref(),
                ])),
            ],
        )
        .unwrap();
        write_file(path.clone(), binary, vec![batch]).await.unwrap();

        let err = imported(&path, &[], &["id", "v"])
            .await
            .expect_err("Binary has no literal form");
        assert!(err.to_string().contains("\"v\""), "got: {err}");
    }

    // A file larger than one batch is handed over in pieces, so a load bigger than
    // memory never has all of itself in the coordinator at once.
    #[tokio::test]
    async fn a_file_larger_than_one_batch_is_read_in_pieces() {
        let path = temp_path("many_batches");
        let rows = ROWS_PER_BATCH + 1;
        let ids: Vec<i32> = (0..rows as i32).collect();
        let vs: Vec<&str> = vec!["x"; rows];
        write_file(path.clone(), schema(), vec![batch(&ids, &vs)])
            .await
            .unwrap();

        let mut file = ParquetFile::open(&path).await.unwrap();
        let mut batches = 0;
        let mut read = 0;
        while let Some(batch) = file.next_batch().await.unwrap() {
            batches += 1;
            read += batch.num_rows();
        }
        assert_eq!(read, rows, "every row must be read exactly once");
        assert!(batches > 1, "expected more than one batch, got {batches}");
    }

    // A file that is not Parquet is a data error naming the coordinator's path, not a
    // panic and not a syntax error.
    #[tokio::test]
    async fn bytes_that_are_not_parquet_are_a_data_error() {
        let path = temp_path("not_parquet");
        std::fs::write(&path, b"id,v\n1,x\n").unwrap();

        // Matched rather than `expect_err`: the success arm holds an open reader,
        // which has nothing printable to report.
        let Err(PgWireError::UserError(info)) = ParquetFile::open(&path).await else {
            panic!("CSV must not open as a Parquet file");
        };
        assert_eq!(info.code, "22P02");
        assert!(info.message.contains("coordinator"), "{}", info.message);
        assert!(info.message.contains("not_parquet"), "{}", info.message);
    }
}
