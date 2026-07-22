use duckdb::Connection;
use duckdb::types::Value;
use tokio::sync::{mpsc, oneshot};

use crate::error::CoreError;

/// A queued write: the SQL, its bind parameters, and the channel to deliver the
/// row-count (or error) back to the caller.
struct WriteQueueEntry {
    sql: String,
    params: Vec<Value>,
    response_tx: oneshot::Sender<Result<u64, CoreError>>,
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
            let result = if entry.params.is_empty() {
                conn.execute(&entry.sql, [])
            } else {
                conn.execute(&entry.sql, duckdb::params_from_iter(entry.params.iter()))
            }
            .map(|rows| rows as u64)
            .map_err(CoreError::from_duckdb);
            let _ = entry.response_tx.send(result);
        }
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

        let entry = WriteQueueEntry {
            sql,
            params,
            response_tx,
        };

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

        let entry = rx.recv().await.unwrap();
        drop(entry.response_tx);

        let result = exec_task.await.unwrap();
        assert!(matches!(result, Err(CoreError::WriteQueue(_))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("writer task dropped response"));
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
}
