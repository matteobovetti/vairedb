//! Embedded Ballista scheduler setup and the table provider that turns catalog
//! shard metadata into distributed `RemoteDuckDbScanExec` plans.
//!
//! The coordinator runs a Ballista scheduler in-process to plan distributed
//! reads across core nodes. This module wires up the scheduler with VaireDB's
//! custom plan codecs and shard-affinity task distribution policy, registers the
//! `vairedb_catalog` and emulated `pg_catalog` schemas, and exposes a
//! `TableProvider` whose `scan` expands a table into per-shard remote scans.

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::serde::BallistaCodec;
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::metrics::default_metrics_collector;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use tokio::net::TcpListener;

use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;

use crate::catalog::{MetadataCatalog, ShardMeta, VaireDbCatalogSchema};
use crate::error::{CoordinatorError, Result};

use super::codec::VairePhysicalCodec;
use super::logical_codec::VaireLogicalCodec;
use super::remote_scan_exec::RemoteDuckDbScanExec;

/// Handle to a running embedded Ballista scheduler, holding its bound address
/// and the session contexts used to plan queries.
pub struct BallistaSchedulerHandle {
    /// Address the scheduler's gRPC server is bound to.
    pub addr: SocketAddr,
    /// Client-side context wired for distributed execution against the scheduler.
    pub session_ctx: Arc<SessionContext>,
    /// Local-only context for queries planned and executed in-process (e.g.
    /// pure catalog/`pg_catalog` lookups that need no distribution).
    pub local_ctx: Arc<SessionContext>,
}

/// Start the embedded Ballista scheduler, bind its gRPC server, and build the
/// session contexts with VaireDB's catalog and `pg_catalog` schemas registered.
///
/// Returns a handle once the scheduler is listening. Errors if the scheduler
/// cannot be initialized, bound, or the catalog schemas cannot be registered.
pub async fn start_scheduler(
    catalog: Arc<MetadataCatalog>,
    listen_addr: &str,
) -> Result<BallistaSchedulerHandle> {
    let session_config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(VaireLogicalCodec))
        .with_ballista_physical_extension_codec(Arc::new(VairePhysicalCodec::new()));

    // Use a regular DataFusion session for the scheduler's internal planning.
    // new_ballista_state installs a distributed query planner that wraps plans in
    // DistributedQueryExec — the scheduler must plan locally so that
    // SchedulerTableProvider.scan() produces RemoteDuckDbScanExec instead.
    let session_state = {
        use datafusion::execution::session_state::SessionStateBuilder;
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .build()
    };

    let addr = start_scheduler_on_addr(&session_state, listen_addr).await?;

    let scheduler_url = format!("http://{}", addr);
    let client_config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(VaireLogicalCodec))
        .with_ballista_physical_extension_codec(Arc::new(VairePhysicalCodec::new()))
        .with_information_schema(true);
    let client_state = {
        use datafusion::execution::session_state::SessionStateBuilder;
        let base = SessionStateBuilder::new()
            .with_default_features()
            .with_config(client_config)
            .build();
        base.upgrade_for_ballista(scheduler_url).map_err(|e| {
            CoordinatorError::Internal(format!("failed to create client session state: {}", e))
        })?
    };

    let session_ctx = Arc::new(SessionContext::new_with_state(client_state));
    register_vairedb_catalog_schema(&session_ctx, Arc::clone(&catalog))?;
    setup_pg_catalog_schema(&session_ctx)?;
    refresh_ballista_catalog_tables(&session_ctx, &catalog)?;

    let local_ctx = Arc::new(SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    ));
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog))?;
    setup_pg_catalog_schema(&local_ctx)?;

    tracing::info!(%addr, "Ballista scheduler started");

    Ok(BallistaSchedulerHandle {
        addr,
        session_ctx,
        local_ctx,
    })
}

/// Build, initialize, and spawn the Ballista `SchedulerServer` on `listen_addr`,
/// installing the pull-staged scheduling policy and the `VaireAffinityPolicy`
/// task distribution. Returns the actually-bound address (the port may be
/// OS-assigned when `listen_addr` uses port 0).
async fn start_scheduler_on_addr(
    session_state: &SessionState,
    listen_addr: &str,
) -> Result<SocketAddr> {
    let logical = session_state.config().ballista_logical_extension_codec();
    let physical = session_state.config().ballista_physical_extension_codec();
    let codec = BallistaCodec::new(logical, physical);
    let session_config = session_state.config().clone();
    let session_state_clone = session_state.clone();
    let session_builder = Arc::new(move |_: SessionConfig| Ok(session_state_clone.clone()));
    let config_producer = Arc::new(move || session_config.clone());

    let config = config_producer();

    let cluster = BallistaCluster::new_memory(listen_addr, session_builder, config_producer);

    let metrics_collector = default_metrics_collector().map_err(|e| {
        CoordinatorError::Internal(format!("failed to create metrics collector: {}", e))
    })?;

    let mut scheduler_server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode> =
        SchedulerServer::new(
            listen_addr.to_owned(),
            cluster,
            codec,
            Arc::new(
                SchedulerConfig::default()
                    .with_scheduler_policy(ballista_core::config::TaskSchedulingPolicy::PullStaged)
                    .with_task_distribution(
                        ballista_scheduler::config::TaskDistributionPolicy::Custom(Arc::new(
                            super::affinity_policy::VaireAffinityPolicy,
                        )),
                    ),
            ),
            metrics_collector,
        );

    scheduler_server.init().await.map_err(|e| {
        CoordinatorError::Internal(format!("failed to init scheduler server: {}", e))
    })?;

    let server = SchedulerGrpcServer::new(scheduler_server.clone())
        .max_decoding_message_size(config.ballista_grpc_client_max_message_size())
        .max_encoding_message_size(config.ballista_grpc_client_max_message_size());

    let listener = TcpListener::bind(listen_addr).await.map_err(|e| {
        CoordinatorError::Internal(format!(
            "failed to bind Ballista scheduler to {}: {}",
            listen_addr, e
        ))
    })?;
    let addr = listener
        .local_addr()
        .map_err(|e| CoordinatorError::Internal(format!("failed to get local addr: {}", e)))?;

    tokio::spawn(
        create_grpc_server(&GrpcServerConfig::default())
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    Ok(addr)
}

/// Register any catalog tables not yet present in `ctx` as
/// `SchedulerTableProvider`s, so distributed queries can plan scans against
/// them. Existing registrations are left untouched (the function is idempotent).
pub fn refresh_ballista_catalog_tables(
    ctx: &SessionContext,
    catalog: &MetadataCatalog,
) -> Result<()> {
    let tables = catalog.list_tables()?;

    for table_meta in &tables {
        if ctx.table_exist(&table_meta.table_name).unwrap_or(false) {
            continue;
        }
        let fields: Vec<Field> = table_meta
            .columns
            .iter()
            .map(|col| {
                let dt = parse_data_type(&col.data_type);
                Field::new(&col.name, dt, col.nullable)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));

        let shards = catalog.get_shards_for_table(&table_meta.table_name)?;

        let provider = Arc::new(SchedulerTableProvider {
            table_name: table_meta.table_name.clone(),
            shards,
            schema,
        });

        ctx.register_table(&table_meta.table_name, provider)
            .map_err(|e| {
                CoordinatorError::Internal(format!(
                    "failed to register table '{}' in query engine: {}",
                    table_meta.table_name, e
                ))
            })?;
    }

    Ok(())
}

/// Map a SQL/DuckDB column type name to its Arrow `DataType`. Matching is
/// case-insensitive; unrecognized types fall back to `Utf8`.
pub fn parse_data_type(type_str: &str) -> DataType {
    use datafusion::arrow::datatypes::TimeUnit;
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,
        "SMALLINT" | "INT2" => DataType::Int16,
        "TINYINT" => DataType::Int8,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
        "BLOB" | "BYTEA" => DataType::Binary,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "DATE" => DataType::Date32,
        "JSON" | "JSONB" => DataType::Utf8,
        _ if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") => {
            DataType::Decimal128(38, 10)
        }
        _ => DataType::Utf8,
    }
}

/// A DataFusion `TableProvider` backed by a table's shard layout. Its `scan`
/// produces one `RemoteDuckDbScanExec` per shard (unioned when multiple),
/// carrying node affinity so the scheduler can route each scan to the node
/// holding the shard.
#[derive(Debug)]
pub struct SchedulerTableProvider {
    table_name: String,
    shards: Vec<ShardMeta>,
    schema: Arc<Schema>,
}

impl SchedulerTableProvider {
    /// Create a provider for `table_name` over the given `shards` and `schema`.
    pub fn new(table_name: String, shards: Vec<ShardMeta>, schema: Arc<Schema>) -> Self {
        Self {
            table_name,
            shards,
            schema,
        }
    }

    /// Logical table name this provider serves.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Shard metadata used to build per-shard remote scans.
    pub fn shards(&self) -> &[ShardMeta] {
        &self.shards
    }

    /// Build a `RemoteDuckDbScanExec` for one shard: its physical table name plus
    /// primary/replica node affinity. Shared by the single-shard and per-shard
    /// (UnionExec child) branches of `scan` so the construction lives in one place.
    fn scan_exec_for_shard(
        &self,
        shard: &ShardMeta,
        projected_schema: &Arc<Schema>,
        projection: Option<&Vec<usize>>,
        filter_exprs: &[String],
    ) -> RemoteDuckDbScanExec {
        RemoteDuckDbScanExec::new(
            crate::util::shard_table_name(&self.table_name, shard.hash_bucket),
            Arc::clone(projected_schema),
            projection.cloned(),
            filter_exprs.to_vec(),
            Some(shard.primary_node_id.clone()),
            shard.replica_node_ids.clone(),
        )
    }
}

#[async_trait::async_trait]
impl TableProvider for SchedulerTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = if let Some(proj) = projection {
            Arc::new(self.schema.project(proj)?)
        } else {
            Arc::clone(&self.schema)
        };

        let filter_exprs: Vec<String> = filters.iter().map(|f| f.to_string()).collect();

        if self.shards.len() <= 1 {
            let scan = match self.shards.first() {
                Some(shard) => {
                    self.scan_exec_for_shard(shard, &projected_schema, projection, &filter_exprs)
                }
                // A table with no assigned shards scans its bare (un-suffixed)
                // name with no node affinity — there is no shard to target.
                None => RemoteDuckDbScanExec::new(
                    self.table_name.clone(),
                    projected_schema,
                    projection.cloned(),
                    filter_exprs,
                    None,
                    Vec::new(),
                ),
            };
            return Ok(Arc::new(scan));
        }

        let children: Vec<Arc<dyn ExecutionPlan>> = self
            .shards
            .iter()
            .map(|shard| {
                Arc::new(self.scan_exec_for_shard(
                    shard,
                    &projected_schema,
                    projection,
                    &filter_exprs,
                )) as Arc<dyn ExecutionPlan>
            })
            .collect();

        datafusion::physical_plan::union::UnionExec::try_new(children)
    }
}

/// Register the emulated PostgreSQL `pg_catalog` schema into the context. The pg_catalog
/// providers read live metadata from the context's own catalog list, so they reflect whatever
/// user tables and schemas are registered at query time.
pub fn setup_pg_catalog_schema(ctx: &SessionContext) -> Result<()> {
    use datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
    use datafusion_pg_catalog::pg_catalog::setup_pg_catalog;

    setup_pg_catalog(ctx, "datafusion", EmptyContextProvider)
        .map_err(|e| CoordinatorError::Internal(format!("failed to set up pg_catalog: {e}")))?;

    Ok(())
}

/// Register the `vairedb_catalog` schema (backed by the metadata catalog) under
/// the context's `datafusion` catalog, exposing VaireDB system tables to queries.
pub fn register_vairedb_catalog_schema(
    ctx: &SessionContext,
    catalog: Arc<MetadataCatalog>,
) -> Result<()> {
    let schema_provider = Arc::new(VaireDbCatalogSchema::new(catalog));
    let catalog_provider = ctx.catalog("datafusion").ok_or_else(|| {
        CoordinatorError::Internal("internal query engine configuration error".to_string())
    })?;
    catalog_provider
        .register_schema("vairedb_catalog", schema_provider)
        .map_err(|e| {
            CoordinatorError::Internal(format!("failed to register catalog schema: {}", e))
        })?;
    Ok(())
}
