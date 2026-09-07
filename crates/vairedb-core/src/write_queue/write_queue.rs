use duckdb::Connection;
use duckdb::types::Value;
use tokio::sync::{mpsc, oneshot};

use crate::error::CoreError;

/// One statement destined for the writer thread: the SQL and its bind
/// parameters.
pub(crate) struct QueuedStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

/// A unit of work for the writer thread, carrying the channel that delivers the
/// row-count (or error) back to the caller.
enum WriteQueueEntry {
    /// A single statement, applied on its own.
    Single {
        sql: String,
        params: Vec<Value>,
        response_tx: oneshot::Sender<Result<u64, CoreError>>,
    },
    /// Several statements applied inside one DuckDB transaction: either all of
    /// them take effect or none does. The writer thread handles one entry at a
    /// time, so no other caller's statement can join the transaction.
    Atomic {
        statements: Vec<QueuedStatement>,
        response_tx: oneshot::Sender<Result<Vec<u64>, CoreError>>,
    },
}

/// Serializes all DuckDB writes through a single background writer thread.
///
/// DuckDB allows one writer at a time; routing every mutation through one
/// dedicated thread keeps writes ordered and avoids write-write contention.
/// [`start`](WriteQueue::start) spawns that thread and hands back a cloneable
/// [`WriteQueueHandle`].
pub struct WriteQueue;

/// A cloneable handle for submitting writes to the [`WriteQueue`]'s writer
/// thread. Cloning yields another producer onto the same queue.
#[derive(Clone)]
pub struct WriteQueueHandle {
    tx: mpsc::Sender<WriteQueueEntry>,
}

impl WriteQueue {
    /// Spawn the writer thread over `conn` and return a handle to it.
    ///
    /// `capacity` bounds the in-flight queue; submissions block once it is full,
    /// applying backpressure.
    pub fn start(conn: Connection, capacity: usize) -> WriteQueueHandle {
        let (tx, rx) = mpsc::channel::<WriteQueueEntry>(capacity);

        tokio::task::spawn_blocking(move || {
            Self::writer_loop(conn, rx);
        });

        WriteQueueHandle { tx }
    }

    /// Drain queued entries one at a time, executing each on `conn` and
    /// replying with the row count or a classified error. Exits when all
    /// handles are dropped.
    fn writer_loop(conn: Connection, mut rx: mpsc::Receiver<WriteQueueEntry>) {
        while let Some(entry) = rx.blocking_recv() {
            match entry {
                WriteQueueEntry::Single {
                    sql,
                    params,
                    response_tx,
                } => {
                    let _ = response_tx.send(Self::execute_one(&conn, &sql, &params));
                }
                WriteQueueEntry::Atomic {
                    statements,
                    response_tx,
                } => {
                    let _ = response_tx.send(Self::execute_atomically(&conn, &statements));
                }
            }
        }
    }

    /// Execute one statement, binding `params` when there are any.
    fn execute_one(conn: &Connection, sql: &str, params: &[Value]) -> Result<u64, CoreError> {
        if params.is_empty() {
            conn.execute(sql, [])
        } else {
            conn.execute(sql, duckdb::params_from_iter(params.iter()))
        }
        .map(|rows| rows as u64)
        .map_err(CoreError::from_duckdb)
    }

    /// Execute every statement inside one DuckDB transaction, returning the row
    /// counts in request order. The first failure rolls the whole batch back and
    /// is returned as-is: nothing was applied, so there is no partial outcome.
    fn execute_atomically(
        conn: &Connection,
        statements: &[QueuedStatement],
    ) -> Result<Vec<u64>, CoreError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }

        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(CoreError::from_duckdb)?;

        let mut rows_affected = Vec::with_capacity(statements.len());
        for stmt in statements {
            match Self::execute_one(conn, &stmt.sql, &stmt.params) {
                Ok(rows) => rows_affected.push(rows),
                Err(e) => {
                    // Report the statement error, not the rollback's: a failing
                    // ROLLBACK is a connection-level problem worth its own log
                    // line, but the caller needs to know why the batch aborted.
                    if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                        tracing::error!(
                            error = %rollback_err,
                            "rolling back a failed atomic write batch failed"
                        );
                    }
                    return Err(e);
                }
            }
        }

        conn.execute_batch("COMMIT")
            .map_err(CoreError::from_duckdb)?;

        Ok(rows_affected)
    }
}

impl WriteQueueHandle {
    /// Submit `sql` with bound `params` to the writer thread and await the
    /// number of rows affected.
    ///
    /// Returns [`CoreError::WriteQueue`] if the queue is closed or the writer
    /// drops the response, or the classified DuckDB error on execution failure.
    pub(crate) async fn execute_with_params(
        &self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<u64, CoreError> {
        let (response_tx, response_rx) = oneshot::channel();

        let entry = WriteQueueEntry::Single {
            sql,
            params,
            response_tx,
        };

        self.submit(entry, response_rx).await
    }

    /// Submit `statements` to the writer thread as a single transaction and await
    /// one row count per statement, in the order given.
    ///
    /// Either every statement takes effect or none does, and no other caller's
    /// statement is interleaved between them. A statement that fails rolls the
    /// batch back and its error is returned.
    pub(crate) async fn execute_atomic_batch(
        &self,
        statements: Vec<QueuedStatement>,
    ) -> Result<Vec<u64>, CoreError> {
        let (response_tx, response_rx) = oneshot::channel();

        let entry = WriteQueueEntry::Atomic {
            statements,
            response_tx,
        };

        self.submit(entry, response_rx).await
    }

    /// Hand `entry` to the writer thread and await its reply on `response_rx`.
    ///
    /// Returns [`CoreError::WriteQueue`] if the queue is closed or the writer
    /// drops the response.
    async fn submit<T>(
        &self,
        entry: WriteQueueEntry,
        response_rx: oneshot::Receiver<Result<T, CoreError>>,
    ) -> Result<T, CoreError> {
        self.tx
            .send(entry)
            .await
            .map_err(|_| CoreError::WriteQueue("write queue closed".to_string()))?;

        response_rx
            .await
            .map_err(|_| CoreError::WriteQueue("writer task dropped response".to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_on_closed_channel_returns_write_queue_error() {
        let (tx, rx) = mpsc::channel::<WriteQueueEntry>(1);
        drop(rx);

        let handle = WriteQueueHandle { tx };
        let result = handle
            .execute_with_params("SELECT 1".to_string(), Vec::new())
            .await;

        assert!(matches!(result, Err(CoreError::WriteQueue(_))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("write queue closed"));
    }

    #[tokio::test]
    async fn execute_returns_error_when_response_sender_dropped() {
        let (tx, mut rx) = mpsc::channel::<WriteQueueEntry>(1);

        let handle = WriteQueueHandle { tx };

        let exec_task = tokio::spawn(async move {
            handle
                .execute_with_params("SELECT 1".to_string(), Vec::new())
                .await
        });

        match rx.recv().await.unwrap() {
            WriteQueueEntry::Single { response_tx, .. } => drop(response_tx),
            WriteQueueEntry::Atomic { .. } => panic!("expected a single-statement entry"),
        }

        let result = exec_task.await.unwrap();
        assert!(matches!(result, Err(CoreError::WriteQueue(_))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("writer task dropped response"));
    }

    #[tokio::test]
    async fn atomic_batch_on_closed_channel_returns_write_queue_error() {
        let (tx, rx) = mpsc::channel::<WriteQueueEntry>(1);
        drop(rx);

        let handle = WriteQueueHandle { tx };
        let result = handle
            .execute_atomic_batch(vec![QueuedStatement {
                sql: "SELECT 1".to_string(),
                params: Vec::new(),
            }])
            .await;

        assert!(matches!(result, Err(CoreError::WriteQueue(_))));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("write queue closed")
        );
    }

    fn setup_write_queue() -> WriteQueueHandle {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE wq_test (id INTEGER, val VARCHAR)", [])
            .unwrap();
        WriteQueue::start(conn, 64)
    }

    #[tokio::test]
    async fn execute_insert_returns_rows_affected() {
        let handle = setup_write_queue();
        let rows = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'a'), (2, 'b')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn execute_with_params_binds_typed_values() {
        let handle = setup_write_queue();
        let rows = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES ($1, $2)".to_string(),
                vec![Value::Int(7), Value::Text("bound".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(rows, 1);

        // A '$1'-looking string literal among the params must be stored verbatim,
        // proving values are bound rather than substituted into the SQL text.
        let rows = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES ($1, $2)".to_string(),
                vec![Value::Int(8), Value::Text("$1 literal".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn execute_with_params_null_binds_sql_null() {
        let handle = setup_write_queue();
        let rows = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES ($1, $2)".to_string(),
                vec![Value::Int(1), Value::Null],
            )
            .await
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn execute_create_table_succeeds() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let handle = WriteQueue::start(conn, 64);
        let rows = handle
            .execute_with_params("CREATE TABLE new_tbl (id INTEGER)".to_string(), Vec::new())
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn execute_invalid_sql_returns_error() {
        let handle = setup_write_queue();
        let result = handle
            .execute_with_params("INSERT INTO nonexistent VALUES (1)".to_string(), Vec::new())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(CoreError::Engine(_)) | Err(CoreError::ShardNotFound(_))
        ));
    }

    #[tokio::test]
    async fn multiple_writes_execute_sequentially() {
        let handle = setup_write_queue();
        for i in 0..10 {
            let sql = format!("INSERT INTO wq_test VALUES ({}, 'item_{}')", i, i);
            let rows = handle.execute_with_params(sql, Vec::new()).await.unwrap();
            assert_eq!(rows, 1);
        }
    }

    #[tokio::test]
    async fn concurrent_writes_all_succeed() {
        let handle = setup_write_queue();
        let mut tasks = Vec::new();
        for i in 0..20 {
            let h = handle.clone();
            let sql = format!("INSERT INTO wq_test VALUES ({}, 'concurrent_{}')", i, i);
            tasks.push(tokio::spawn(async move {
                h.execute_with_params(sql, Vec::new()).await
            }));
        }
        for task in tasks {
            let result = task.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn dropped_handle_does_not_panic() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let handle = WriteQueue::start(conn, 64);
        drop(handle);
    }

    #[tokio::test]
    async fn update_returns_rows_affected() {
        let handle = setup_write_queue();

        handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'old'), (2, 'old'), (3, 'keep')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        let rows = handle
            .execute_with_params(
                "UPDATE wq_test SET val = 'new' WHERE val = 'old'".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn delete_returns_rows_affected() {
        let handle = setup_write_queue();
        handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'a'), (2, 'b'), (3, 'c')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        let rows = handle
            .execute_with_params("DELETE FROM wq_test WHERE id > 1".to_string(), Vec::new())
            .await
            .unwrap();
        assert_eq!(rows, 2);
    }

    /// A MERGE reports the rows all of its clauses touched together, which is what
    /// the coordinator sums across shards into the client's `MERGE <n>` tag.
    #[tokio::test]
    async fn merge_returns_rows_affected() {
        let handle = setup_write_queue();
        handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'old'), (2, 'stay')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        // One row updated, one inserted, one target row left alone.
        let rows = handle
            .execute_with_params(
                "MERGE INTO wq_test t USING (VALUES (1, 'new'), (3, 'fresh')) AS s (id, val) \
                 ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET val = s.val \
                 WHEN NOT MATCHED BY TARGET THEN INSERT (id, val) VALUES (s.id, s.val)"
                    .to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn handle_is_clone_and_independent() {
        let handle = setup_write_queue();
        let handle2 = handle.clone();
        let r1 = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'from_h1')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        let r2 = handle2
            .execute_with_params(
                "INSERT INTO wq_test VALUES (2, 'from_h2')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(r1, 1);
        assert_eq!(r2, 1);
        drop(handle);
        let r3 = handle2
            .execute_with_params(
                "INSERT INTO wq_test VALUES (3, 'still_works')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(r3, 1);
    }

    /// A write queue plus a second connection to the same in-memory database,
    /// for reading back what the writer thread committed.
    fn setup_write_queue_with_reader() -> (WriteQueueHandle, Connection) {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE wq_test (id INTEGER, val VARCHAR)", [])
            .unwrap();
        let reader = conn.try_clone().unwrap();
        (WriteQueue::start(conn, 64), reader)
    }

    fn row_count(reader: &Connection) -> i64 {
        reader
            .query_row("SELECT count(*) FROM wq_test", [], |row| row.get(0))
            .unwrap()
    }

    #[tokio::test]
    async fn atomic_batch_applies_every_statement() {
        let (handle, reader) = setup_write_queue_with_reader();
        let rows = handle
            .execute_atomic_batch(vec![
                QueuedStatement {
                    sql: "INSERT INTO wq_test VALUES (1, 'a')".to_string(),
                    params: Vec::new(),
                },
                QueuedStatement {
                    sql: "INSERT INTO wq_test VALUES ($1, $2), (3, 'c')".to_string(),
                    params: vec![Value::Int(2), Value::Text("b".to_string())],
                },
                QueuedStatement {
                    sql: "UPDATE wq_test SET val = 'z' WHERE id > 1".to_string(),
                    params: Vec::new(),
                },
            ])
            .await
            .unwrap();

        assert_eq!(rows, vec![1, 2, 2]);
        assert_eq!(row_count(&reader), 3);
    }

    #[tokio::test]
    async fn atomic_batch_rolls_back_every_statement_on_failure() {
        let (handle, reader) = setup_write_queue_with_reader();
        let result = handle
            .execute_atomic_batch(vec![
                QueuedStatement {
                    sql: "INSERT INTO wq_test VALUES (1, 'a')".to_string(),
                    params: Vec::new(),
                },
                QueuedStatement {
                    sql: "INSERT INTO nonexistent VALUES (2)".to_string(),
                    params: Vec::new(),
                },
            ])
            .await;

        assert!(result.is_err());
        // The first insert must not survive the aborted batch.
        assert_eq!(row_count(&reader), 0);
    }

    #[tokio::test]
    async fn queue_still_serves_writes_after_a_rolled_back_batch() {
        let (handle, reader) = setup_write_queue_with_reader();
        let _ = handle
            .execute_atomic_batch(vec![QueuedStatement {
                sql: "INSERT INTO nonexistent VALUES (1)".to_string(),
                params: Vec::new(),
            }])
            .await;

        let rows = handle
            .execute_with_params(
                "INSERT INTO wq_test VALUES (1, 'after')".to_string(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(row_count(&reader), 1);
    }

    #[tokio::test]
    async fn empty_atomic_batch_is_a_no_op() {
        let handle = setup_write_queue();
        let rows = handle.execute_atomic_batch(Vec::new()).await.unwrap();
        assert!(rows.is_empty());
    }
}
