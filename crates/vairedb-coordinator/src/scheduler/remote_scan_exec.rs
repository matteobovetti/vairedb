//! `RemoteDuckDbScanExec`: a placeholder physical plan node representing a scan of
//! one DuckDB shard on a core node.
//!
//! It carries the shard's physical table name, projection, pushed-down filters,
//! and the primary/replica executor ids used by the affinity policy to route the
//! task. It is never executed on the coordinator — it must be serialized and
//! shipped to a core node, so calling `execute` here is an error.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Physical plan node describing a single-shard DuckDB scan to be executed
/// remotely on a core node. Holds the shard's physical table name, projection,
/// pushed-down filter expressions, and the primary/replica executor ids used for
/// shard-affinity scheduling.
#[derive(Debug)]
pub struct RemoteDuckDbScanExec {
    shard_table_name: String,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filter_exprs: Vec<String>,
    target_executor_id: Option<String>,
    replica_executor_ids: Vec<String>,
    properties: Arc<PlanProperties>,
}

impl RemoteDuckDbScanExec {
    /// Construct a remote scan node. `target_executor_id` names the primary node
    /// holding the shard (if known) and `replica_executor_ids` the replicas; both
    /// inform the affinity policy. Reports a single unknown-partitioned output.
    pub fn new(
        shard_table_name: String,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter_exprs: Vec<String>,
        target_executor_id: Option<String>,
        replica_executor_ids: Vec<String>,
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
            filter_exprs,
            target_executor_id,
            replica_executor_ids,
            properties,
        }
    }

    /// Physical (shard-suffixed) table name to scan on the core node.
    pub fn shard_table_name(&self) -> &str {
        &self.shard_table_name
    }

    /// Output schema after applying the projection.
    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Column projection indices, if any.
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    /// Pushed-down filter expressions, rendered as strings.
    pub fn filter_exprs(&self) -> &[String] {
        &self.filter_exprs
    }

    /// Executor id of the shard's primary node, if known.
    pub fn target_executor_id(&self) -> Option<&str> {
        self.target_executor_id.as_deref()
    }

    /// Executor ids of nodes holding replicas of the shard.
    pub fn replica_executor_ids(&self) -> &[String] {
        &self.replica_executor_ids
    }
}

impl DisplayAs for RemoteDuckDbScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RemoteDuckDbScanExec: table={}", self.shard_table_name)
    }
}

impl ExecutionPlan for RemoteDuckDbScanExec {
    fn name(&self) -> &str {
        "RemoteDuckDbScanExec"
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
        Err(datafusion::error::DataFusionError::Internal(
            "query execution plan was not correctly distributed to storage nodes".to_string(),
        ))
    }
}
