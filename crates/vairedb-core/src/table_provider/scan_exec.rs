use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;

use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, stream};
use tokio::sync::Mutex;

use tracing::warn;

use crate::engine::DuckDbEngine;

/// Log the underlying `cause` against `shard` and return a sanitized external error.
/// The user-facing message intentionally omits engine internals; details go to the log.
fn shard_query_error(shard: &str, log_msg: &str, cause: impl fmt::Display) -> DataFusionError {
    warn!(shard = %shard, error = %cause, "{}", log_msg);
    DataFusionError::External(format!("query failed on shard '{shard}'").into())
}

/// A DataFusion [`ExecutionPlan`] that scans a single local DuckDB shard table.
///
/// It builds a `SELECT` from the projected schema and pushed-down `filters`,
/// runs it on a connection cloned from the shared [`DuckDbEngine`], and streams
/// the resulting record batches (coerced to the advertised schema). Ballista
/// ships this node to the executor via [`VaireExecutorPhysicalCodec`].
///
/// [`VaireExecutorPhysicalCodec`]: crate::ballista_exec
#[derive(Debug)]
pub(crate) struct DuckDbScanExec {
    shard_table_name: String,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    engine: Arc<Mutex<DuckDbEngine>>,
    properties: Arc<PlanProperties>,
}

impl DuckDbScanExec {
    /// Build a scan of `shard_table_name` that returns `projected_schema`,
    /// applying `projection` and the pushed-down `filters` against `engine`.
    pub(crate) fn new(
        shard_table_name: String,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<String>,
        engine: Arc<Mutex<DuckDbEngine>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            shard_table_name,
            projected_schema,
            projection,
            filters,
            engine,
            properties,
        }
    }

    /// Name of the shard table this plan scans.
    pub(crate) fn shard_table_name(&self) -> &str {
        &self.shard_table_name
    }

    /// The schema this plan advertises and coerces output batches to.
    pub(crate) fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Column projection (indices into the source schema), if any.
    pub(crate) fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    /// The pushed-down filter predicates as SQL fragments.
    pub(crate) fn filter_strings(&self) -> Vec<String> {
        self.filters.clone()
    }

    /// Assemble the `SELECT` statement from the projected columns and filters.
    fn build_query(&self) -> String {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(", ");

        let select_clause = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
        };

        let mut sql = format!("SELECT {} FROM {}", select_clause, self.shard_table_name);

        let where_clauses = &self.filters;

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }
}

impl DisplayAs for DuckDbScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDbScanExec: table={}", self.shard_table_name)
    }
}

impl ExecutionPlan for DuckDbScanExec {
    fn name(&self) -> &str {
        "DuckDbScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let sql = self.build_query();
        let engine = Arc::clone(&self.engine);
        let schema = Arc::clone(&self.projected_schema);
        let shard_name = self.shard_table_name.clone();

        let schema_for_stream = Arc::clone(&schema);

        let fut = async move {
            // Hold the engine lock only long enough to clone a connection; the guard
            // drops at the end of this block so concurrent scans proceed in parallel.
            let conn = {
                let eng = engine.lock().await;
                eng.read_connection()
                    .map_err(|e| shard_query_error(&shard_name, "read connection failed", e))?
            };

            // Run the blocking DuckDB query on the cloned connection. No runtime
            // re-entry: the blocking thread never calls back into the async runtime.
            let shard = shard_name.clone();
            let batches = tokio::task::spawn_blocking(move || {
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| shard_query_error(&shard, "query prepare failed", e))?;

                let arrow_result = stmt
                    .query_arrow([])
                    .map_err(|e| shard_query_error(&shard, "query_arrow failed", e))?;

                arrow_result
                    .map(|b| coerce_batch_to_schema(b, &schema, &shard))
                    .collect::<Result<Vec<RecordBatch>, DataFusionError>>()
            })
            .await
            .map_err(|e| shard_query_error(&shard_name, "spawn_blocking failed", e))??;

            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        };

        let stream = stream::once(fut).flat_map(|result| match result {
            Ok(batches) => stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>()),
            Err(e) => stream::iter(vec![Err(e)]),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            stream,
        )))
    }
}

/// DuckDB returns batches carrying its own inferred Arrow schema, which can diverge
/// from the catalog-derived `projected_schema` advertised by this plan (timestamp
/// unit/timezone, decimal precision/scale, nullability). Cast each column to the
/// target type so downstream operators that trust the advertised schema see matching
/// arrays. Batches that already match, or whose column count differs (e.g. `SELECT *`
/// with an empty projection), are passed through untouched.
fn coerce_batch_to_schema(
    batch: RecordBatch,
    target: &SchemaRef,
    shard: &str,
) -> Result<RecordBatch, DataFusionError> {
    if batch.schema().fields() == target.fields() {
        return Ok(batch);
    }
    if batch.num_columns() != target.fields().len() {
        return Ok(batch);
    }

    let columns = target
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            datafusion::arrow::compute::cast(batch.column(i), field.data_type())
                .map_err(|e| shard_query_error(shard, "schema coercion cast failed", e))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| shard_query_error(shard, "schema coercion rebuild failed", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use tempfile::TempDir;

    use crate::engine::DuckDbEngine;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn shared_engine_with_rows(dir: &TempDir, rows: usize) -> Arc<Mutex<DuckDbEngine>> {
        let engine = DuckDbEngine::open(dir.path()).unwrap();
        let conn = engine.write_connection().unwrap();
        conn.execute("CREATE TABLE scan_t (id INTEGER, name VARCHAR)", [])
            .unwrap();
        for i in 0..rows {
            conn.execute(&format!("INSERT INTO scan_t VALUES ({i}, 'row_{i}')"), [])
                .unwrap();
        }
        Arc::new(Mutex::new(engine))
    }

    async fn collect_rows(plan: &DuckDbScanExec) -> usize {
        let ctx = Arc::new(TaskContext::default());
        let mut stream = plan.execute(0, ctx).unwrap();
        let mut total = 0;
        while let Some(batch) = stream.next().await {
            total += batch.unwrap().num_rows();
        }
        total
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_scans_all_return_rows() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 50);
        let schema = test_schema();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let plan = DuckDbScanExec::new(
                "scan_t".to_string(),
                Arc::clone(&schema),
                None,
                vec![],
                Arc::clone(&engine),
            );
            tasks.push(tokio::spawn(async move { collect_rows(&plan).await }));
        }

        for task in tasks {
            assert_eq!(task.await.unwrap(), 50);
        }
    }

    #[tokio::test]
    async fn scan_with_filter_returns_matching_rows() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 10);
        let plan = DuckDbScanExec::new(
            "scan_t".to_string(),
            test_schema(),
            None,
            vec!["id >= 7".to_string()],
            engine,
        );
        assert_eq!(collect_rows(&plan).await, 3);
    }
}
