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
    limit: Option<usize>,
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
            limit: None,
            engine,
            properties,
        }
    }

    /// Cap the rows this scan returns, when the coordinator was able to push the query's
    /// `LIMIT` down to the shard. See [`build_query`](Self::build_query) for why that is
    /// sound.
    pub(crate) fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
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

    /// The most rows this scan returns, if the coordinator pushed a limit down.
    pub(crate) fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Assemble the `SELECT` statement from the projected columns, filters and limit.
    ///
    /// Each filter fragment is already parenthesized by the coordinator, so joining them
    /// with `AND` cannot re-associate a fragment that is itself an `OR`.
    ///
    /// The `LIMIT` is per shard, not per query. That is the whole reason it can be applied
    /// here at all: the coordinator unions this shard's rows with the other shards' and
    /// applies the query's real limit to the union, so returning up to `limit` rows from
    /// each shard can only ever be a superset of the rows the answer needs. No `ORDER BY`
    /// accompanies it for the same reason — which rows these are does not matter, only that
    /// there are enough of them.
    fn build_query(&self) -> String {
        if self.projected_schema.fields().is_empty() {
            return self.row_count_query();
        }

        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!("SELECT {} FROM {}", columns, self.shard_table_name);

        let where_clauses = &self.filters;

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        sql
    }

    /// The query for a projection of **no columns**, which is what `COUNT(*)` asks a scan
    /// for: how many rows there are, and nothing about them.
    ///
    /// SQL cannot select zero columns, so this counts instead and [`row_count_batch`]
    /// turns the count back into the column-less batch the plan promised. `SELECT *`
    /// would answer the same question, but it reads and ships every value in the shard
    /// to have them all discarded — and it used to fail outright, because the batches it
    /// returned carried columns the advertised schema did not have (`number of columns(3)
    /// must match number of fields(0)`), which is what a cross join under an aggregate
    /// hit: `SELECT COUNT(*) FROM a CROSS JOIN b`.
    ///
    /// The count wraps the row-producing query rather than replacing its `LIMIT`, since
    /// `LIMIT` applies to the rows, not to the count of them.
    fn row_count_query(&self) -> String {
        let mut rows = format!("SELECT 1 FROM {}", self.shard_table_name);
        if !self.filters.is_empty() {
            rows.push_str(" WHERE ");
            rows.push_str(&self.filters.join(" AND "));
        }
        if let Some(limit) = self.limit {
            rows.push_str(&format!(" LIMIT {limit}"));
        }
        format!("SELECT COUNT(*) FROM ({rows}) vaire_rows")
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

                if schema.fields().is_empty() {
                    // The query counted rows instead of returning them — see
                    // `row_count_query`.
                    return row_count_batch(arrow_result, &schema, &shard).map(|b| vec![b]);
                }

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

/// Turn the answer to [`row_count_query`](DuckDbScanExec::row_count_query) into the batch
/// a projection of no columns has to produce: no arrays at all, and a row count.
///
/// A `RecordBatch` normally infers its length from its columns, so one with no columns
/// needs the count set explicitly — otherwise every `COUNT(*)` over a shard answers zero.
fn row_count_batch(
    batches: impl Iterator<Item = RecordBatch>,
    schema: &SchemaRef,
    shard: &str,
) -> Result<RecordBatch, DataFusionError> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::arrow::record_batch::RecordBatchOptions;

    let mut rows = 0usize;
    for batch in batches {
        let Some(column) = batch.columns().first() else {
            return Err(shard_query_error(
                shard,
                "row count returned no column",
                "the count query answered a batch with no columns",
            ));
        };
        // DuckDB answers `COUNT(*)` as `BIGINT`, but cast rather than assume: a wrong
        // guess here would be a silently wrong row count.
        let counts = cast(
            column.as_ref(),
            &datafusion::arrow::datatypes::DataType::Int64,
        )
        .map_err(|e| shard_query_error(shard, "row count was not a number", e))?;
        for count in counts.as_primitive::<Int64Type>().iter().flatten() {
            rows += count.max(0) as usize;
        }
    }

    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| shard_query_error(shard, "row count batch rebuild failed", e))
}

/// DuckDB returns batches carrying its own inferred Arrow schema, which can diverge
/// from the catalog-derived `projected_schema` advertised by this plan (timestamp
/// unit/timezone, decimal precision/scale, nullability). Cast each column to the
/// target type so downstream operators that trust the advertised schema see matching
/// arrays. A batch that already matches is passed through untouched.
///
/// A batch with a different *number* of columns is a scan that asked for something other
/// than what it advertises, which no cast can repair: pairing the columns positionally
/// would relabel unrelated values, and passing it through hands a batch to operators that
/// trust the schema. Both are silent; failing here at least names the shard.
///
/// The cast is **checked**. Arrow's default is a *safe* cast, which replaces any value
/// the target type cannot hold with NULL — so a shard that stored `12345678901234567890`
/// under a column the catalog calls `DECIMAL(18,3)` returned a NULL, and the client had
/// no way to tell that from a NULL it had actually written. A value that cannot be
/// represented in the type this plan promised fails the scan instead, naming the column
/// and both types. That is a worse answer to get and a better one to be given: the row
/// is not silently rewritten.
fn coerce_batch_to_schema(
    batch: RecordBatch,
    target: &SchemaRef,
    shard: &str,
) -> Result<RecordBatch, DataFusionError> {
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    if batch.schema().fields() == target.fields() {
        return Ok(batch);
    }
    if batch.num_columns() != target.fields().len() {
        return Err(width_mismatch_error(
            shard,
            batch.num_columns(),
            target.fields().len(),
        ));
    }

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = target
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let column = batch.column(i);
            if column.data_type() == field.data_type() {
                return Ok(Arc::clone(column));
            }
            cast_with_options(column.as_ref(), field.data_type(), &options)
                .map_err(|e| coercion_error(shard, field, column.data_type(), e))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    RecordBatch::try_new(Arc::clone(target), columns)
        .map_err(|e| shard_query_error(shard, "schema coercion rebuild failed", e))
}

/// The error for a batch that is not as wide as the schema the scan advertises.
///
/// Nothing a client wrote can cause this — the query is built from the same projection the
/// schema is built from — so it is `Internal`, and it keeps its two counts rather than
/// hiding them like [`shard_query_error`]: they are the whole diagnosis and neither is data.
fn width_mismatch_error(shard: &str, returned: usize, advertised: usize) -> DataFusionError {
    DataFusionError::Internal(format!(
        "scan of shard '{shard}' returned a batch of the wrong width: \
         {returned} columns returned, {advertised} advertised"
    ))
}

/// The error for a column the shard cannot return in the type the plan advertises.
///
/// Unlike [`shard_query_error`] this one keeps its explanation, because the cause is not
/// an engine internal: it is the column's own declared type, which the client chose and
/// can change. Only the Arrow-level cast failure stays in the log. The message travels to
/// the coordinator as an opaque `External` error, so it reaches the client under the
/// generic SQLSTATE rather than `22003` — the wording carries what the code cannot.
fn coercion_error(
    shard: &str,
    field: &datafusion::arrow::datatypes::Field,
    returned: &datafusion::arrow::datatypes::DataType,
    cause: impl fmt::Display,
) -> DataFusionError {
    warn!(
        shard = %shard,
        column = %field.name(),
        returned = %returned,
        advertised = %field.data_type(),
        error = %cause,
        "schema coercion cast failed"
    );
    DataFusionError::External(
        format!(
            "column \"{}\" on shard '{}' holds a value its declared type {} cannot \
             represent (the shard returned it as {})",
            field.name(),
            shard,
            field.data_type(),
            returned
        )
        .into(),
    )
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

    /// The statement is what the shard actually runs, so what it does and does not
    /// contain is the whole contract of the two push-downs.
    #[test]
    fn builds_a_query_that_carries_the_filters_and_the_limit() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 1);

        let plan = DuckDbScanExec::new(
            "scan_t".to_string(),
            test_schema(),
            None,
            vec![
                "(\"id\" > 7)".to_string(),
                "(\"name\" IS NOT NULL)".to_string(),
            ],
            Arc::clone(&engine),
        )
        .with_limit(Some(5));

        assert_eq!(
            plan.build_query(),
            "SELECT id, name FROM scan_t WHERE (\"id\" > 7) AND (\"name\" IS NOT NULL) LIMIT 5"
        );

        // No limit means the whole shard: an absent limit must not become `LIMIT 0`.
        let plan = DuckDbScanExec::new("scan_t".to_string(), test_schema(), None, vec![], engine);
        assert_eq!(plan.build_query(), "SELECT id, name FROM scan_t");
    }

    /// The limit is applied by the engine, not merely appended to a string.
    #[tokio::test]
    async fn returns_at_most_the_pushed_limit() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 10);
        let plan = DuckDbScanExec::new("scan_t".to_string(), test_schema(), None, vec![], engine)
            .with_limit(Some(4));

        assert_eq!(collect_rows(&plan).await, 4);
    }

    fn single_column_batch(
        name: &str,
        column: Arc<dyn datafusion::arrow::array::Array>,
    ) -> RecordBatch {
        let field = Field::new(name, column.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).unwrap()
    }

    fn single_column_schema(name: &str, data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, data_type, true)]))
    }

    /// The ordinary reason this function exists: DuckDB's inferred unit is not always the
    /// catalog's, and a value that fits both is simply converted.
    #[test]
    fn coerces_a_column_the_target_type_can_hold() {
        use datafusion::arrow::array::{TimestampMicrosecondArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::TimeUnit;

        let batch = single_column_batch(
            "ts",
            Arc::new(TimestampNanosecondArray::from(vec![1_500_000_000, 0])),
        );
        let target = single_column_schema("ts", DataType::Timestamp(TimeUnit::Microsecond, None));

        let coerced = coerce_batch_to_schema(batch, &target, "shard-1").unwrap();

        let values = coerced
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("the column is microseconds once coerced");
        assert_eq!(values.values(), &[1_500_000, 0]);
    }

    /// A NULL the shard actually stored still comes back as a NULL: the checked cast is
    /// about values that cannot be represented, not about absent ones.
    #[test]
    fn keeps_a_null_a_null() {
        use datafusion::arrow::array::{Array, Int32Array, Int64Array};

        let batch = single_column_batch("n", Arc::new(Int64Array::from(vec![None, Some(7)])));
        let target = single_column_schema("n", DataType::Int32);

        let coerced = coerce_batch_to_schema(batch, &target, "shard-1").unwrap();

        let values = coerced
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 7);
    }

    /// The point of the checked cast: the value is reported, not replaced. A safe cast
    /// returned NULL here, which the client could not distinguish from a stored NULL.
    #[test]
    fn refuses_a_value_the_declared_type_cannot_represent() {
        use datafusion::arrow::array::Decimal128Array;

        let stored = Decimal128Array::from(vec![100_000_000_000_000_000_000_i128])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let batch = single_column_batch("amount", Arc::new(stored));
        let target = single_column_schema("amount", DataType::Decimal128(18, 3));

        let err = coerce_batch_to_schema(batch, &target, "shard-1")
            .expect_err("21 digits do not fit in DECIMAL(18,3)");
        let message = err.to_string();

        assert!(
            message.contains("\"amount\"") && message.contains("shard-1"),
            "the message names the column and the shard: {message}"
        );
        assert!(
            message.contains("Decimal128(18, 3)"),
            "the message names the type the plan advertised: {message}"
        );
    }

    /// A batch of a width the target does not have cannot be coerced into it: pairing the
    /// columns positionally would relabel unrelated values, and passing the batch through
    /// hands operators a batch its schema lies about. Both are silent, so this fails loudly.
    #[test]
    fn refuses_a_batch_of_a_different_width() {
        use datafusion::arrow::array::Int32Array;

        let batch = single_column_batch("id", Arc::new(Int32Array::from(vec![1])));
        let target = test_schema();

        let err = coerce_batch_to_schema(batch, &target, "shard-1")
            .expect_err("one column cannot become two");
        let message = err.to_string();

        assert!(
            message.contains("1 columns returned, 2 advertised"),
            "the message names both widths: {message}"
        );
        assert!(
            message.contains("shard-1"),
            "the message names the shard: {message}"
        );
    }

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    /// `COUNT(*)` projects no columns, and SQL cannot select none: the scan counts instead.
    /// Reading every column with `SELECT *` and discarding it was the alternative, and it
    /// failed — the batches carried columns the advertised schema did not have.
    #[test]
    fn counts_rows_when_the_projection_is_empty() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 1);

        let plan = DuckDbScanExec::new(
            "scan_t".to_string(),
            empty_schema(),
            None,
            vec![],
            Arc::clone(&engine),
        );
        assert_eq!(
            plan.build_query(),
            "SELECT COUNT(*) FROM (SELECT 1 FROM scan_t) vaire_rows"
        );

        // The filters still narrow which rows are counted, and the limit still caps them:
        // it bounds the rows, so it belongs inside the count, not beside it.
        let plan = DuckDbScanExec::new(
            "scan_t".to_string(),
            empty_schema(),
            None,
            vec!["(\"id\" > 7)".to_string()],
            engine,
        )
        .with_limit(Some(5));
        assert_eq!(
            plan.build_query(),
            "SELECT COUNT(*) FROM (SELECT 1 FROM scan_t WHERE (\"id\" > 7) LIMIT 5) vaire_rows"
        );
    }

    /// The count becomes the length of a batch with no arrays in it. A `RecordBatch` infers
    /// its length from its columns, so without the explicit row count every `COUNT(*)` over
    /// a shard would answer zero.
    #[test]
    fn rebuilds_a_column_less_batch_of_the_counted_length() {
        use datafusion::arrow::array::{Int32Array, Int64Array};

        let schema = empty_schema();

        let counted = row_count_batch(
            std::iter::once(single_column_batch(
                "count_star()",
                Arc::new(Int64Array::from(vec![42])),
            )),
            &schema,
            "shard-1",
        )
        .unwrap();
        assert_eq!(counted.num_columns(), 0);
        assert_eq!(counted.num_rows(), 42);

        // Counts arriving over several batches sum, and a narrower integer type still
        // reads as a count — the type DuckDB answers with is cast, not assumed.
        let counted = row_count_batch(
            [
                single_column_batch("c", Arc::new(Int32Array::from(vec![2]))),
                single_column_batch("c", Arc::new(Int64Array::from(vec![3]))),
            ]
            .into_iter(),
            &schema,
            "shard-1",
        )
        .unwrap();
        assert_eq!(counted.num_rows(), 5);
    }

    /// End to end through DuckDB: the shard's row count arrives as the length of the stream,
    /// which is what an aggregate over a cross join reads.
    #[tokio::test]
    async fn an_empty_projection_streams_the_shard_row_count() {
        let dir = TempDir::new().unwrap();
        let engine = shared_engine_with_rows(&dir, 7);

        let plan = DuckDbScanExec::new(
            "scan_t".to_string(),
            empty_schema(),
            None,
            vec![],
            Arc::clone(&engine),
        );
        assert_eq!(collect_rows(&plan).await, 7);

        let filtered = DuckDbScanExec::new(
            "scan_t".to_string(),
            empty_schema(),
            None,
            vec!["(\"id\" >= 5)".to_string()],
            engine,
        );
        assert_eq!(collect_rows(&filtered).await, 2);
    }
}
