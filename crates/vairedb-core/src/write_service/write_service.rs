use std::sync::{Mutex, MutexGuard};

use duckdb::types::Value;
use tonic::{Request, Response, Status};

use vairedb_common::proto::vairedb::v1::{
    ErrorDetail, ExecuteWriteRequest, ExecuteWriteResponse, WriteResult, WriteStatement,
    write_service_server::WriteService,
};

use crate::error::CoreError;
use crate::write_queue::{QueuedStatement, WriteQueueHandle};

use super::dedup_cache::DedupCache;
use super::param_conversion::write_param_to_duckdb_value;

/// Number of recent `write_id`s retained for idempotent replay detection.
const DEFAULT_DEDUP_CAPACITY: usize = 10_000;

/// gRPC `WriteService` implementation: applies write batches through the
/// [`WriteQueueHandle`] and makes them idempotent via a bounded dedup cache.
///
/// A non-empty `write_id` on a request makes it replay-safe: a repeated id
/// returns the cached per-statement results instead of re-executing the batch.
pub struct WriteServiceImpl {
    write_queue: WriteQueueHandle,
    dedup: Mutex<DedupCache>,
}

impl WriteServiceImpl {
    /// Build the service over `write_queue`, with a dedup cache sized to the
    /// default capacity.
    pub fn new(write_queue: WriteQueueHandle) -> Self {
        Self {
            write_queue,
            dedup: Mutex::new(DedupCache::new(DEFAULT_DEDUP_CAPACITY)),
        }
    }

    /// Lock the dedup cache, recovering the guard if a previous holder panicked.
    /// A poisoned cache is safe to reuse here: a panic mid-update can only leave
    /// stale entries, never corrupt the map, and dedup is a best-effort optimization.
    fn lock_dedup(&self) -> MutexGuard<'_, DedupCache> {
        self.dedup.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("dedup cache mutex poisoned, recovering");
            poisoned.into_inner()
        })
    }

    /// Apply each statement on its own, reporting one result per statement. A
    /// failing statement does not stop the ones after it.
    async fn apply_one_by_one(&self, req: &ExecuteWriteRequest) -> Vec<WriteResult> {
        let mut results = Vec::with_capacity(req.statements.len());

        for stmt in &req.statements {
            let params = duckdb_params(stmt);
            let result = self
                .write_queue
                .execute_with_params(stmt.sql.clone(), params)
                .await;

            match result {
                Ok(rows_affected) => results.push(succeeded(rows_affected)),
                Err(e) => {
                    tracing::warn!(
                        write_id = %req.write_id,
                        shard_id = %stmt.shard_id,
                        error = %e,
                        "write statement failed"
                    );
                    results.push(failed(&e));
                }
            }
        }

        results
    }

    /// Apply every statement inside one transaction on the node. On success there
    /// is one result per statement; on failure a single failed result, because
    /// nothing was applied and no statement has an outcome of its own.
    async fn apply_atomically(&self, req: &ExecuteWriteRequest) -> Vec<WriteResult> {
        let statements: Vec<QueuedStatement> = req
            .statements
            .iter()
            .map(|stmt| QueuedStatement {
                sql: stmt.sql.clone(),
                params: duckdb_params(stmt),
            })
            .collect();

        match self.write_queue.execute_atomic_batch(statements).await {
            Ok(rows_affected) => rows_affected.into_iter().map(succeeded).collect(),
            Err(e) => {
                tracing::warn!(
                    write_id = %req.write_id,
                    statements = req.statements.len(),
                    error = %e,
                    "atomic write batch rolled back"
                );
                vec![failed(&e)]
            }
        }
    }
}

/// Convert a statement's protobuf bind parameters to DuckDB values.
fn duckdb_params(stmt: &WriteStatement) -> Vec<Value> {
    stmt.params
        .iter()
        .map(write_param_to_duckdb_value)
        .collect()
}

fn succeeded(rows_affected: u64) -> WriteResult {
    WriteResult {
        success: true,
        rows_affected: rows_affected as i64,
        error: None,
    }
}

fn failed(error: &CoreError) -> WriteResult {
    WriteResult {
        success: false,
        rows_affected: 0,
        error: Some(ErrorDetail {
            code: error.vdb_error_code().into(),
            message: vairedb_common::error::sanitize_message(&error.to_string()),
        }),
    }
}

#[cfg(test)]
impl WriteServiceImpl {
    fn with_dedup_capacity(write_queue: WriteQueueHandle, dedup_capacity: usize) -> Self {
        Self {
            write_queue,
            dedup: Mutex::new(DedupCache::new(dedup_capacity)),
        }
    }
}

#[tonic::async_trait]
impl WriteService for WriteServiceImpl {
    async fn execute_write(
        &self,
        request: Request<ExecuteWriteRequest>,
    ) -> Result<Response<ExecuteWriteResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(write_id = %req.write_id, statements = req.statements.len(), "executing write");

        if !req.write_id.is_empty() {
            let cache = self.lock_dedup();
            if let Some(cached) = cache.get(&req.write_id) {
                tracing::debug!(write_id = %req.write_id, "returning cached idempotent result");
                return Ok(Response::new(ExecuteWriteResponse {
                    results: cached.clone(),
                }));
            }
        }

        let results = if req.atomic {
            self.apply_atomically(&req).await
        } else {
            self.apply_one_by_one(&req).await
        };

        if !req.write_id.is_empty() {
            let mut cache = self.lock_dedup();
            cache.insert(req.write_id, results.clone());
        }

        Ok(Response::new(ExecuteWriteResponse { results }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;
    use vairedb_common::proto::vairedb::v1::{VdbErrorCode, WriteOperation, WriteStatement};

    use crate::write_queue::WriteQueue;

    fn setup_service() -> WriteServiceImpl {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE orders_shard0 (id INTEGER, amount DOUBLE)", [])
            .unwrap();
        let handle = WriteQueue::start(conn, 64);
        WriteServiceImpl::new(handle)
    }

    fn setup_service_with_dedup(capacity: usize) -> WriteServiceImpl {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE orders_shard0 (id INTEGER, amount DOUBLE)", [])
            .unwrap();
        let handle = WriteQueue::start(conn, 64);
        WriteServiceImpl::with_dedup_capacity(handle, capacity)
    }

    fn make_request(
        write_id: &str,
        statements: Vec<WriteStatement>,
    ) -> Request<ExecuteWriteRequest> {
        Request::new(ExecuteWriteRequest {
            write_id: write_id.to_string(),
            statements,
            atomic: false,
        })
    }

    fn make_atomic_request(
        write_id: &str,
        statements: Vec<WriteStatement>,
    ) -> Request<ExecuteWriteRequest> {
        Request::new(ExecuteWriteRequest {
            write_id: write_id.to_string(),
            statements,
            atomic: true,
        })
    }

    fn insert(sql: &str) -> WriteStatement {
        WriteStatement {
            sql: sql.to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        }
    }

    #[tokio::test]
    async fn single_insert_succeeds() {
        let service = setup_service();
        let stmt = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1, 9.99)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response = service
            .execute_write(make_request("w-1", vec![stmt]))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].rows_affected, 1);
    }

    #[tokio::test]
    async fn batch_insert_returns_per_statement_results() {
        let service = setup_service();
        let stmts = vec![
            WriteStatement {
                sql: "INSERT INTO orders_shard0 VALUES (1, 10.0)".to_string(),
                shard_id: "orders_shard0".to_string(),
                operation: WriteOperation::Insert.into(),
                params: vec![],
            },
            WriteStatement {
                sql: "INSERT INTO orders_shard0 VALUES (2, 20.0), (3, 30.0)".to_string(),
                shard_id: "orders_shard0".to_string(),
                operation: WriteOperation::Insert.into(),
                params: vec![],
            },
        ];
        let response = service
            .execute_write(make_request("w-2", stmts))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert_eq!(results[0].rows_affected, 1);
        assert!(results[1].success);
        assert_eq!(results[1].rows_affected, 2);
    }

    #[tokio::test]
    async fn invalid_sql_returns_error_result() {
        let service = setup_service();
        let stmt = WriteStatement {
            sql: "INSERT INTO nonexistent_table VALUES (1)".to_string(),
            shard_id: "nonexistent_table".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response = service
            .execute_write(make_request("w-3", vec![stmt]))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert_eq!(results.len(), 1);
        assert!(!results[0].success);
        assert!(results[0].error.is_some());
    }

    #[tokio::test]
    async fn duplicate_write_id_returns_cached_result() {
        let service = setup_service();
        let stmt = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1, 9.99)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response1 = service
            .execute_write(make_request("w-dup", vec![stmt.clone()]))
            .await
            .unwrap();
        let results1 = response1.into_inner().results;
        let response2 = service
            .execute_write(make_request("w-dup", vec![stmt]))
            .await
            .unwrap();
        let results2 = response2.into_inner().results;
        assert_eq!(results1[0].rows_affected, results2[0].rows_affected);
    }

    #[tokio::test]
    async fn empty_write_id_bypasses_dedup() {
        let service = setup_service();
        let stmt = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1, 5.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        service
            .execute_write(make_request("", vec![stmt]))
            .await
            .unwrap();
        let stmt2 = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (2, 6.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response = service
            .execute_write(make_request("", vec![stmt2]))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert!(results[0].success);
    }

    #[tokio::test]
    async fn dedup_cache_evicts_oldest_entries() {
        let service = setup_service_with_dedup(2);
        let stmt = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (1, 1.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        service
            .execute_write(make_request("w-evict-1", vec![stmt.clone()]))
            .await
            .unwrap();
        let stmt2 = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (2, 2.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        service
            .execute_write(make_request("w-evict-2", vec![stmt2]))
            .await
            .unwrap();
        let stmt3 = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (3, 3.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        service
            .execute_write(make_request("w-evict-3", vec![stmt3]))
            .await
            .unwrap();
        let stmt_retry = WriteStatement {
            sql: "INSERT INTO orders_shard0 VALUES (4, 4.0)".to_string(),
            shard_id: "orders_shard0".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response = service
            .execute_write(make_request("w-evict-1", vec![stmt_retry]))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert!(results[0].success);
    }

    #[tokio::test]
    async fn error_result_contains_error_code_and_message() {
        let service = setup_service();
        let stmt = WriteStatement {
            sql: "INSERT INTO nonexistent_table VALUES (1)".to_string(),
            shard_id: "nonexistent_table".to_string(),
            operation: WriteOperation::Insert.into(),
            params: vec![],
        };
        let response = service
            .execute_write(make_request("w-err-detail", vec![stmt]))
            .await
            .unwrap();
        let results = response.into_inner().results;
        let error = results[0].error.as_ref().unwrap();
        assert_eq!(error.code, VdbErrorCode::ShardNotFound as i32);
        assert!(!error.message.is_empty());
    }

    /// A service plus a second connection to the same in-memory database, for
    /// reading back what the writer thread committed.
    fn setup_service_with_reader() -> (WriteServiceImpl, Connection) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE orders_shard0 (id INTEGER, amount DOUBLE)", [])
            .unwrap();
        let reader = conn.try_clone().unwrap();
        let handle = WriteQueue::start(conn, 64);
        (WriteServiceImpl::new(handle), reader)
    }

    fn row_count(reader: &Connection) -> i64 {
        reader
            .query_row("SELECT count(*) FROM orders_shard0", [], |row| row.get(0))
            .unwrap()
    }

    #[tokio::test]
    async fn atomic_request_reports_every_statement_on_success() {
        let (service, reader) = setup_service_with_reader();
        let stmts = vec![
            insert("INSERT INTO orders_shard0 VALUES (1, 10.0)"),
            insert("INSERT INTO orders_shard0 VALUES (2, 20.0), (3, 30.0)"),
        ];
        let response = service
            .execute_write(make_atomic_request("w-atomic-ok", stmts))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.success));
        assert_eq!(results[0].rows_affected, 1);
        assert_eq!(results[1].rows_affected, 2);
        assert_eq!(row_count(&reader), 3);
    }

    #[tokio::test]
    async fn atomic_request_applies_nothing_when_a_statement_fails() {
        let (service, reader) = setup_service_with_reader();
        let stmts = vec![
            insert("INSERT INTO orders_shard0 VALUES (1, 10.0)"),
            insert("INSERT INTO nonexistent_table VALUES (2)"),
            insert("INSERT INTO orders_shard0 VALUES (3, 30.0)"),
        ];
        let response = service
            .execute_write(make_atomic_request("w-atomic-fail", stmts))
            .await
            .unwrap();
        let results = response.into_inner().results;

        // One failed result stands for the whole batch: nothing was applied.
        assert_eq!(results.len(), 1);
        assert!(!results[0].success);
        assert!(results[0].error.is_some());
        assert_eq!(row_count(&reader), 0);
    }

    #[tokio::test]
    async fn non_atomic_request_keeps_statements_before_a_failure() {
        let (service, reader) = setup_service_with_reader();
        let stmts = vec![
            insert("INSERT INTO orders_shard0 VALUES (1, 10.0)"),
            insert("INSERT INTO nonexistent_table VALUES (2)"),
        ];
        let response = service
            .execute_write(make_request("w-non-atomic-fail", stmts))
            .await
            .unwrap();
        let results = response.into_inner().results;
        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert!(!results[1].success);
        assert_eq!(row_count(&reader), 1);
    }

    #[tokio::test]
    async fn duplicate_atomic_write_id_is_not_reapplied() {
        let (service, reader) = setup_service_with_reader();
        let stmts = vec![insert("INSERT INTO orders_shard0 VALUES (1, 10.0)")];
        service
            .execute_write(make_atomic_request("w-atomic-dup", stmts.clone()))
            .await
            .unwrap();
        let response = service
            .execute_write(make_atomic_request("w-atomic-dup", stmts))
            .await
            .unwrap();
        assert!(response.into_inner().results[0].success);
        assert_eq!(row_count(&reader), 1);
    }
}
