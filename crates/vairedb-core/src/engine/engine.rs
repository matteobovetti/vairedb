use std::path::Path;

use duckdb::Connection;
#[cfg(test)]
use duckdb::arrow::record_batch::RecordBatch;

use crate::error::CoreError;

/// Bring the database's arithmetic in line with PostgreSQL's, which is the dialect a
/// VaireDB client speaks.
///
/// One setting, and it decides an answer rather than a spelling. DuckDB's `/` is
/// floating-point division whatever its operands are, so `7 / 2` answers `3.5` on a
/// write while the coordinator's read path — DataFusion, which follows PostgreSQL —
/// answers `3`. One database cannot hold both answers to the same expression, and the
/// contract is PostgreSQL's. `integer_division` makes `/` integer division when both
/// operands are integers and leaves every other combination floating point, which is
/// PostgreSQL's rule exactly.
///
/// Two details of *how* decide whether it works at all, and both were measured against
/// DuckDB 1.5.5 rather than read off its documentation:
///
/// * It is applied at open, not alongside each statement. The setting is consulted when
///   a statement is **bound**, so a `SET` sent in the same batch as the statement it is
///   meant to govern arrives too late and the statement still divides as floats.
/// * `GLOBAL` is required, even though `duckdb_settings()` already reports the setting's
///   scope as `GLOBAL`. A plain `SET` takes effect on this connection only, and
///   [`DuckDbEngine::clone_connection`] — which every read and every write goes through
///   — hands out a connection where it has reverted to `false`. `SET GLOBAL` is what
///   survives the clone.
fn apply_postgres_semantics(conn: &Connection) -> Result<(), CoreError> {
    conn.execute_batch("SET GLOBAL integer_division = true")
        .map_err(|e| CoreError::engine("failed to set integer_division", e))
}

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
        apply_postgres_semantics(&conn)?;

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
        apply_postgres_semantics(&conn)?;
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

    /// The whole point of [`apply_postgres_semantics`], asserted through the same
    /// `read_connection` a real write goes through — which is a *clone* of the
    /// connection the setting was applied to. The `GLOBAL` scope is what makes that
    /// work, and this is the assertion that would fail if DuckDB ever narrowed it.
    #[test]
    fn integer_division_follows_postgres_on_every_connection() {
        let engine = DuckDbEngine::open_in_memory().unwrap();
        engine
            .execute_write("CREATE TABLE divs (a INTEGER, b INTEGER)")
            .unwrap();
        engine
            .execute_write("INSERT INTO divs VALUES (7, 2), (-7, 2)")
            .unwrap();

        // Integer / integer truncates toward zero and stays an integer, as in
        // PostgreSQL. DuckDB's default would answer 3.5 and -3.5 as DOUBLE.
        let batches = engine
            .execute_query("SELECT a / b AS q FROM divs ORDER BY a DESC")
            .unwrap();
        let quotients: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<duckdb::arrow::array::Int32Array>()
                    .expect("integer division yields an integer, not a float")
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(quotients, vec![3, -3]);

        // And a non-integer operand still divides as floating point, which is also
        // PostgreSQL's rule: the setting narrows `/`, it does not replace it.
        let batches = engine.execute_query("SELECT 7.0 / 2 AS q").unwrap();
        let q = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<duckdb::arrow::array::Float64Array>()
            .expect("a decimal operand keeps float division")
            .value(0);
        assert_eq!(q, 3.5);
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
