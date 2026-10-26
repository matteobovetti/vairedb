//! A bulk INSERT on the node: rows that arrive as an Arrow batch rather than as the
//! text of an `INSERT ... VALUES`.
//!
//! The batch is not appended to the shard table directly, although DuckDB's appender
//! could do that. An appender casts each column itself and reports a failure in its
//! own words, and one that fails part-way still flushes the rows it had buffered when
//! it is dropped. Either would make a bulk insert a different statement from the
//! literal one it stands in for: a value the literal INSERT refuses with DuckDB's
//! `Conversion Error` would be refused differently — and so classified differently —
//! and a batch could be half stored.
//!
//! So the rows are appended to a staging table typed after the batch, where nothing is
//! cast because the types are the batch's own, and moved into the shard table by one
//! `INSERT ... SELECT`. That statement is the literal INSERT's twin: the same cast from
//! each value to its column's declared type, the same default for a column not listed,
//! the same constraint checks and messages, and all of its rows or none. It runs inside
//! a transaction, which is also what takes the staging table away when it fails.

use std::io::Cursor;

use arrow::datatypes::{DataType, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use duckdb::Connection;

use vairedb_common::proto::vairedb::v1::BulkRows;

use crate::error::CoreError;

/// The table a bulk insert's rows pass through. Temporary, so it belongs to the
/// writer's connection alone: no reader can see it and it does not outlive the process.
const STAGING_TABLE: &str = "vairedb_bulk_rows";

/// Rows to insert into one shard table, decoded from a request.
pub(crate) struct BulkInsert {
    /// Physical shard table, e.g. `orders_shard0`.
    table: String,
    /// Target columns, one per column of `rows`, in order.
    columns: Vec<String>,
    rows: RecordBatch,
}

impl BulkInsert {
    /// Read the rows out of a request's Arrow IPC stream.
    ///
    /// Decoded before the statement is queued, so a payload that is not one fails as a
    /// result of its own without ever reaching the writer thread.
    pub(crate) fn decode(bulk: &BulkRows) -> Result<Self, CoreError> {
        let unreadable = |cause: arrow::error::ArrowError| {
            CoreError::Engine(format!(
                "bulk insert into {} is not a readable Arrow IPC stream: {cause}",
                bulk.table
            ))
        };
        let reader = StreamReader::try_new(Cursor::new(bulk.arrow_ipc.as_ref()), None)
            .map_err(unreadable)?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(unreadable)?;
        let rows = arrow::compute::concat_batches(&schema, &batches).map_err(unreadable)?;

        if rows.num_columns() != bulk.columns.len() {
            return Err(CoreError::Engine(format!(
                "bulk insert into {} names {} column(s) but carries {}",
                bulk.table,
                bulk.columns.len(),
                rows.num_columns()
            )));
        }
        Ok(Self {
            table: bulk.table.clone(),
            columns: bulk.columns.clone(),
            rows,
        })
    }

    /// Insert the rows, returning how many were inserted.
    ///
    /// Must run inside a transaction: the staging table is only taken away by the
    /// statement's success or by the transaction's rollback.
    pub(crate) fn execute(&self, conn: &Connection) -> Result<u64, CoreError> {
        if self.rows.num_rows() == 0 {
            return Ok(0);
        }

        conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {STAGING_TABLE} ({})",
            staging_columns(&self.rows.schema())?
        ))
        .map_err(CoreError::from_duckdb)?;
        {
            // Scoped so the appender is closed — which is when it flushes — before
            // the rows are read back out of the staging table, and before any
            // rollback a failure here leads to.
            let mut appender = conn
                .appender_to_catalog_and_db(STAGING_TABLE, "temp", "main")
                .map_err(CoreError::from_duckdb)?;
            appender
                .append_record_batch(self.rows.clone())
                .map_err(CoreError::from_duckdb)?;
            appender.flush().map_err(CoreError::from_duckdb)?;
        }

        let inserted = conn
            .execute(&self.insert_sql(), [])
            .map_err(CoreError::from_duckdb)?;
        conn.execute_batch(&format!("DROP TABLE temp.main.{STAGING_TABLE}"))
            .map_err(CoreError::from_duckdb)?;
        Ok(inserted as u64)
    }

    /// `INSERT INTO <table> (<columns>) SELECT c0, c1, ... FROM <staging>`.
    fn insert_sql(&self) -> String {
        let targets = self
            .columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let sources = (0..self.columns.len())
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {} ({targets}) SELECT {sources} FROM temp.main.{STAGING_TABLE}",
            quote_ident(&self.table)
        )
    }
}

/// The staging table's column list, `c0 <type>, c1 <type>, ...`.
fn staging_columns(schema: &Schema) -> Result<String, CoreError> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            staging_type(field.data_type())
                .map(|ty| format!("c{i} {ty}"))
                .ok_or_else(|| {
                    CoreError::Engine(format!(
                        "bulk insert column {i} has type {}, which a bulk insert does not carry",
                        field.data_type()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|columns| columns.join(", "))
}

/// The DuckDB type a staging column is declared with: the batch's own, so appending
/// it never casts.
///
/// Only the types the coordinator sends as themselves are here. Every other value
/// arrives as the text its SQL literal would carry, which keeps the cast to the
/// column's declared type the `INSERT`'s, exactly as for the literal.
fn staging_type(data_type: &DataType) -> Option<&'static str> {
    Some(match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Utf8 => "VARCHAR",
        _ => return None,
    })
}

/// A double-quoted identifier, with any embedded quote doubled.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::Field;
    use arrow::ipc::writer::StreamWriter;

    use super::*;

    /// A `BulkRows` carrying `columns` as one Arrow IPC stream, each array landing on
    /// the target column of the same position.
    pub(crate) fn bulk_rows(table: &str, columns: &[(&str, ArrayRef)]) -> BulkRows {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            columns.iter().map(|(_, array)| Arc::clone(array)).collect(),
        )
        .unwrap();

        let mut writer = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        BulkRows {
            table: table.to_string(),
            columns: columns
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect(),
            arrow_ipc: writer.into_inner().unwrap().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};

    use super::test_support::bulk_rows;
    use super::*;
    use crate::write_queue::{QueuedStatement, WriteQueue, WriteQueueHandle};

    /// A writer over a table with one column of each kind a bulk insert has to cast
    /// into, plus a reader of the same in-memory database.
    fn writer() -> (WriteQueueHandle, Connection) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t_shard0 (
                id BIGINT NOT NULL,
                v VARCHAR,
                amount DOUBLE,
                price DECIMAL(10, 2),
                ts TIMESTAMP,
                tstz TIMESTAMPTZ,
                d DATE,
                flag BOOLEAN,
                note VARCHAR DEFAULT 'none'
            )",
        )
        .unwrap();
        let reader = conn.try_clone().unwrap();
        (WriteQueue::start(conn, 8), reader)
    }

    fn text(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    async fn insert(handle: &WriteQueueHandle, bulk: &BulkRows) -> Result<u64, CoreError> {
        handle
            .execute(QueuedStatement::Bulk(BulkInsert::decode(bulk)?))
            .await
    }

    fn rendered(reader: &Connection, sql: &str) -> Vec<String> {
        let mut stmt = reader.prepare(sql).unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn staging_tables(reader: &Connection) -> i64 {
        reader
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'vairedb_bulk_rows'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    // Text is cast by the INSERT exactly as a string literal would be, and the native
    // kinds land as themselves. Columns not listed take their defaults.
    #[tokio::test]
    async fn rows_are_cast_into_their_columns_as_literals_would_be() {
        let (handle, reader) = writer();
        let bulk = bulk_rows(
            "t_shard0",
            &[
                ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
                ("v", text(&[Some("it's"), None])),
                ("amount", text(&[Some("0.1"), Some("NaN")])),
                ("price", text(&[Some("2.675"), Some("-1")])),
                ("ts", text(&[Some("2019-01-01 08:04:00"), None])),
                ("tstz", text(&[Some("2019-01-01 08:04:00+01"), None])),
                ("d", text(&[Some("2024-02-29"), None])),
                (
                    "flag",
                    Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
                ),
            ],
        );

        assert_eq!(insert(&handle, &bulk).await.unwrap(), 2);
        assert_eq!(
            rendered(
                &reader,
                "SELECT concat_ws('|', id, coalesce(v, '∅'), amount, price, coalesce(CAST(ts AS VARCHAR), '∅'), \
                 coalesce(CAST(tstz AS VARCHAR), '∅'), coalesce(CAST(d AS VARCHAR), '∅'), coalesce(CAST(flag AS VARCHAR), '∅'), note) \
                 FROM t_shard0 ORDER BY id"
            ),
            vec![
                "1|it's|0.1|2.68|2019-01-01 08:04:00|2019-01-01 07:04:00+00|2024-02-29|true|none"
                    .to_string(),
                "2|∅|nan|-1.00|∅|∅|∅|∅|none".to_string(),
            ]
        );
        assert_eq!(
            staging_tables(&reader),
            0,
            "the staging table must not outlive the insert"
        );
    }

    // The claim the whole lane rests on: a bulk insert stores what the literal INSERT
    // the coordinator would otherwise have sent stores — integers as bare numbers,
    // booleans as `true`/`false`, everything else as a quoted string — including where
    // that is a rounding, a truncation or an implicit cast nobody would guess.
    #[tokio::test]
    async fn bulk_rows_store_what_the_literal_insert_stores() {
        const COLUMNS: &str = "id BIGINT, small SMALLINT, real4 REAL, dbl DOUBLE, \
            price DECIMAL(10, 2), ts TIMESTAMP, d DATE, flag BOOLEAN, num_text VARCHAR, \
            bool_text VARCHAR, text_bool BOOLEAN";
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE literal_shard0 ({COLUMNS}); CREATE TABLE bulk_shard0 ({COLUMNS})"
        ))
        .unwrap();
        conn.execute_batch(
            "INSERT INTO literal_shard0 VALUES
                (1, 300, '0.1', '1e-7', '2.675', '2019-01-01 08:04:00.123456789', '2024-02-29', true, 42, false, 'T'),
                (2, -7, '3.4028235e38', 'inf', '-0.005', '1999-12-31 23:59:59', '0001-01-01', false, -1, true, 'f'),
                (3, NULL, NULL, '-0', NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .unwrap();
        let reader = conn.try_clone().unwrap();
        let handle = WriteQueue::start(conn, 8);

        let bulk = bulk_rows(
            "bulk_shard0",
            &[
                ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
                (
                    "small",
                    Arc::new(Int64Array::from(vec![Some(300), Some(-7), None])) as ArrayRef,
                ),
                ("real4", text(&[Some("0.1"), Some("3.4028235e38"), None])),
                ("dbl", text(&[Some("1e-7"), Some("inf"), Some("-0")])),
                ("price", text(&[Some("2.675"), Some("-0.005"), None])),
                (
                    "ts",
                    text(&[
                        Some("2019-01-01 08:04:00.123456789"),
                        Some("1999-12-31 23:59:59"),
                        None,
                    ]),
                ),
                ("d", text(&[Some("2024-02-29"), Some("0001-01-01"), None])),
                (
                    "flag",
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
                ),
                (
                    "num_text",
                    Arc::new(Int64Array::from(vec![Some(42), Some(-1), None])) as ArrayRef,
                ),
                (
                    "bool_text",
                    Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])) as ArrayRef,
                ),
                ("text_bool", text(&[Some("T"), Some("f"), None])),
            ],
        );
        assert_eq!(insert(&handle, &bulk).await.unwrap(), 3);

        // Each row as one struct rendered to text, which spells NULL, `-0.0`, `nan` and
        // `inf` as themselves — an equality comparison would let `-0.0 = 0.0` through.
        let stored = |table: &str| {
            rendered(
                &reader,
                &format!("SELECT CAST(t AS VARCHAR) FROM {table} t ORDER BY id"),
            )
        };
        let literal = stored("literal_shard0");
        assert_eq!(literal.len(), 3);
        assert_eq!(stored("bulk_shard0"), literal);
    }

    // A value its column cannot hold is DuckDB's own conversion error — the class the
    // literal INSERT reports — and the rows before it in the batch are not stored.
    #[tokio::test]
    async fn a_value_its_column_cannot_hold_stores_nothing() {
        let (handle, reader) = writer();
        let bulk = bulk_rows(
            "t_shard0",
            &[
                ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
                ("amount", text(&[Some("1.5"), Some("not a number")])),
            ],
        );

        let err = insert(&handle, &bulk).await.unwrap_err();
        assert!(matches!(err, CoreError::TypeMismatch(_)), "got: {err:?}");
        assert_eq!(
            rendered(&reader, "SELECT CAST(count(*) AS VARCHAR) FROM t_shard0"),
            ["0"]
        );
        assert_eq!(
            staging_tables(&reader),
            0,
            "a failed insert must take its staging away"
        );
    }

    // The table's constraints apply, with the literal INSERT's message.
    #[tokio::test]
    async fn the_tables_constraints_apply() {
        let (handle, reader) = writer();
        let bulk = bulk_rows("t_shard0", &[("v", text(&[Some("no id")]))]);

        let err = insert(&handle, &bulk).await.unwrap_err();
        assert!(err.to_string().contains("NOT NULL"), "got: {err}");
        assert_eq!(
            rendered(&reader, "SELECT CAST(count(*) AS VARCHAR) FROM t_shard0"),
            ["0"]
        );
    }

    // Column names are quoted, so a name whose case the catalog kept — or one with a
    // quote in it — lands on its own column.
    #[tokio::test]
    async fn column_names_are_quoted() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE q_shard0 (id BIGINT, \"Mixed \"\"Case\"\"\" VARCHAR)")
            .unwrap();
        let reader = conn.try_clone().unwrap();
        let handle = WriteQueue::start(conn, 8);

        let bulk = bulk_rows(
            "q_shard0",
            &[
                ("id", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
                ("Mixed \"Case\"", text(&[Some("x")])),
            ],
        );
        assert_eq!(insert(&handle, &bulk).await.unwrap(), 1);
        assert_eq!(
            rendered(&reader, "SELECT \"Mixed \"\"Case\"\"\" FROM q_shard0"),
            ["x"]
        );
    }

    #[tokio::test]
    async fn an_empty_batch_inserts_nothing() {
        let (handle, _reader) = writer();
        let bulk = bulk_rows(
            "t_shard0",
            &[(
                "id",
                Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            )],
        );
        assert_eq!(insert(&handle, &bulk).await.unwrap(), 0);
    }

    #[test]
    fn a_payload_that_is_not_arrow_is_refused() {
        let bulk = BulkRows {
            table: "t_shard0".to_string(),
            columns: vec!["id".to_string()],
            arrow_ipc: b"INSERT INTO t_shard0 VALUES (1)".to_vec().into(),
        };
        let err = BulkInsert::decode(&bulk).err().expect("not an IPC stream");
        assert!(err.to_string().contains("Arrow IPC"), "got: {err}");
    }

    #[test]
    fn a_column_list_that_does_not_match_the_payload_is_refused() {
        let mut bulk = bulk_rows(
            "t_shard0",
            &[("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)],
        );
        bulk.columns.push("v".to_string());
        let err = BulkInsert::decode(&bulk)
            .err()
            .expect("two names for one column");
        assert!(
            err.to_string().contains("names 2 column(s) but carries 1"),
            "got: {err}"
        );
    }

    // A kind the coordinator never sends natively is refused rather than appended
    // under a type nobody chose for it.
    #[tokio::test]
    async fn a_type_bulk_rows_do_not_carry_is_refused() {
        let (handle, _reader) = writer();
        let bulk = bulk_rows(
            "t_shard0",
            &[(
                "amount",
                Arc::new(arrow::array::Float64Array::from(vec![1.5])) as ArrayRef,
            )],
        );
        let err = insert(&handle, &bulk).await.unwrap_err();
        assert!(err.to_string().contains("does not carry"), "got: {err}");
    }
}
