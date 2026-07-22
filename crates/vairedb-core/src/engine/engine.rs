use std::path::Path;

use duckdb::Connection;
#[cfg(test)]
use duckdb::arrow::record_batch::RecordBatch;

use crate::error::CoreError;

/// Owns the node's DuckDB connection and serves as the factory for the
/// per-operation connection clones used by reads and writes.
///
/// DuckDB connections are cheap to clone and share the underlying database, so
/// callers obtain a fresh handle per query rather than contending on a single
/// connection.
pub struct DuckDbEngine {
    conn: Connection,
}

impl std::fmt::Debug for DuckDbEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbEngine").finish_non_exhaustive()
    }
}

impl DuckDbEngine {
    /// Open (or create) the node's DuckDB database under `data_dir`.
    ///
    /// The directory is created if missing and the database file lives at
    /// `data_dir/core.duckdb`. Returns a [`CoreError::Engine`] if the directory
    /// or database cannot be opened.
    pub fn open(data_dir: &Path) -> Result<Self, CoreError> {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| CoreError::engine("failed to create data dir", e))?;

        let db_path = data_dir.join("core.duckdb");
        let conn = Connection::open(&db_path)
            .map_err(|e| CoreError::engine("failed to open duckdb", e))?;

        Ok(Self { conn })
    }

    /// Clone the underlying connection, yielding an independent handle to the
    /// same database.
    fn clone_connection(&self) -> Result<Connection, CoreError> {
        self.conn
            .try_clone()
            .map_err(|e| CoreError::engine("failed to clone connection", e))
    }

    /// A connection handle for write traffic, owned by the write queue's
    /// single writer thread.
    pub fn write_connection(&self) -> Result<Connection, CoreError> {
        self.clone_connection()
    }

    /// A connection handle for read traffic, used per scan so concurrent reads
    /// don't contend on a shared connection.
    pub(crate) fn read_connection(&self) -> Result<Connection, CoreError> {
        self.clone_connection()
    }

    /// List the names of the shard tables in the `main` schema, i.e. the shards
    /// this node hosts.
    pub fn list_tables(&self) -> Result<Vec<String>, CoreError> {
        let conn = self.read_connection()?;
        let mut stmt = conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .map_err(|e| CoreError::engine("list tables failed", e))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| CoreError::engine("list tables query failed", e))?;

        let mut tables = Vec::new();
        for row in rows {
            tables.push(row.map_err(|e| CoreError::engine("row read failed", e))?);
        }
        Ok(tables)
    }
}

#[cfg(test)]
impl DuckDbEngine {
    fn open_in_memory() -> Result<Self, CoreError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| CoreError::engine("failed to open in-memory duckdb", e))?;
        Ok(Self { conn })
    }

    fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, CoreError> {
        let conn = self.read_connection()?;
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| CoreError::engine("prepare failed", e))?;
        let batches: Vec<RecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| CoreError::engine("query_arrow failed", e))?
            .collect();
        Ok(batches)
    }

    fn execute_write(&self, sql: &str) -> Result<u64, CoreError> {
        let conn = self.write_connection()?;
        let rows = conn
            .execute(sql, [])
            .map_err(|e| CoreError::engine("execute failed", e))?;
        Ok(rows as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn open_creates_database_file() {
        let dir = TempDir::new().unwrap();
        let _engine = DuckDbEngine::open(dir.path()).unwrap();
        assert!(dir.path().join("core.duckdb").exists());
    }

    #[test]
    fn open_creates_data_directory_if_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("nested").join("deep");
        let _engine = DuckDbEngine::open(&nested).unwrap();
        assert!(nested.join("core.duckdb").exists());
    }

    #[test]
    fn open_in_memory_succeeds() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let tables = engine.list_tables().unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn execute_write_creates_table() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let result = engine.execute_write("CREATE TABLE test_tbl (id INTEGER, name VARCHAR)");
        assert!(result.is_ok());
    }

    #[test]
    fn execute_write_returns_rows_affected() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE counts (id INTEGER, val INTEGER)")
            .unwrap();
        let rows = engine
            .execute_write("INSERT INTO counts VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn execute_write_returns_error_on_invalid_sql() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let result = engine.execute_write("NOT VALID SQL AT ALL");
        assert!(result.is_err());
    }

    #[test]
    fn execute_query_returns_record_batches() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE query_test (id INTEGER, name VARCHAR)")
            .unwrap();
        engine
            .execute_write("INSERT INTO query_test VALUES (1, 'alice'), (2, 'bob')")
            .unwrap();
        let batches = engine
            .execute_query("SELECT * FROM query_test ORDER BY id")
            .unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn execute_query_returns_correct_columns() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE cols (a INTEGER, b VARCHAR, c DOUBLE)")
            .unwrap();
        engine
            .execute_write("INSERT INTO cols VALUES (1, 'x', 3.14)")
            .unwrap();
        let batches = engine.execute_query("SELECT a, b, c FROM cols").unwrap();
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(2).name(), "c");
    }

    #[test]
    fn execute_query_returns_error_on_invalid_sql() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let result = engine.execute_query("SELECT * FROM nonexistent_table");
        assert!(result.is_err());
    }

    #[test]
    fn list_tables_returns_empty_on_fresh_db() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let tables = engine.list_tables().unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn list_tables_returns_created_tables() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE orders_shard0 (id INTEGER)")
            .unwrap();
        engine
            .execute_write("CREATE TABLE orders_shard1 (id INTEGER)")
            .unwrap();
        let mut tables = engine.list_tables().unwrap();
        tables.sort();
        assert_eq!(tables, vec!["orders_shard0", "orders_shard1"]);
    }

    #[test]
    fn write_connection_clones_successfully() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        let conn = engine.write_connection().unwrap();
        conn.execute("CREATE TABLE via_clone (x INTEGER)", [])
            .unwrap();
        let tables = engine.list_tables().unwrap();
        assert!(tables.contains(&"via_clone".to_string()));
    }

    #[test]
    fn read_connection_clones_successfully() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE read_test (id INTEGER)")
            .unwrap();
        let conn = engine.read_connection().unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM read_test").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn data_persists_across_connections() {
        let dir = TempDir::new().unwrap();
        {
            let engine = DuckDbEngine::open(dir.path()).unwrap();
            engine
                .execute_write("CREATE TABLE persist (id INTEGER)")
                .unwrap();
            engine
                .execute_write("INSERT INTO persist VALUES (42)")
                .unwrap();
        }
        let engine = DuckDbEngine::open(dir.path()).unwrap();
        let batches = engine.execute_query("SELECT id FROM persist").unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn open_errors_on_invalid_path() {
        let result = DuckDbEngine::open(Path::new("/proc/0/impossible/path"));
        assert!(result.is_err());
    }
}
