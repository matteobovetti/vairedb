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
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::SessionState;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use tokio::net::TcpListener;

use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;

use crate::catalog::{MetadataCatalog, ShardMeta, VaireDbCatalogSchema};
use crate::column_types::parse_data_type;
use crate::error::{CoordinatorError, Result};

use super::codec::VairePhysicalCodec;
use super::filter_pushdown::OpaqueTextColumns;
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
    let session_config = with_postgres_sql_options(
        SessionConfig::new_with_ballista()
            .with_ballista_logical_extension_codec(Arc::new(VaireLogicalCodec))
            .with_ballista_physical_extension_codec(Arc::new(VairePhysicalCodec::new())),
    );

    // Use a regular DataFusion session for the scheduler's internal planning.
    // new_ballista_state installs a distributed query planner that wraps plans in
    // DistributedQueryExec — the scheduler must plan locally so that
    // SchedulerTableProvider.scan() produces RemoteDuckDbScanExec instead.
    let session_state = {
        use datafusion::execution::session_state::SessionStateBuilder;
        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            // Both appended after the defaults, so they see the distribution and the
            // ordering the built-in rules settled on, and both repair a shape that is
            // valid as one plan and wrong once Ballista cuts it into stages — see
            // `super::window_partition_sort` and `super::nested_loop_join_one_task`. This
            // is the state that plans every distributed read, so it is the only place
            // either repair can happen.
            .with_physical_optimizer_rule(Arc::new(
                super::window_partition_sort::SortWindowPartitionsWithinTheStage,
            ))
            .with_physical_optimizer_rule(Arc::new(
                super::nested_loop_join_one_task::RunTheNestedLoopJoinInOneTask,
            ))
            .build();
        register_postgres_functions(&mut state);
        state
    };

    let addr = start_scheduler_on_addr(&session_state, listen_addr).await?;

    let scheduler_url = format!("http://{}", addr);
    let client_config = with_postgres_sql_options(
        SessionConfig::new_with_ballista()
            .with_ballista_logical_extension_codec(Arc::new(VaireLogicalCodec))
            .with_ballista_physical_extension_codec(Arc::new(VairePhysicalCodec::new()))
            .with_information_schema(true),
    );
    let client_state = {
        use datafusion::execution::session_state::SessionStateBuilder;
        let base = SessionStateBuilder::new()
            .with_default_features()
            .with_config(client_config)
            // Not for correctness — this state does not plan the distributed read, it ships
            // the logical plan to the scheduler, which plans it with the state above. It is
            // for `EXPLAIN`, which *is* planned here and would otherwise show a probe side
            // partitioned the way the plan that actually runs is not.
            .with_physical_optimizer_rule(Arc::new(
                super::nested_loop_join_one_task::RunTheNestedLoopJoinInOneTask,
            ))
            .build();
        base.upgrade_for_ballista(scheduler_url).map_err(|e| {
            CoordinatorError::Internal(format!("failed to create client session state: {}", e))
        })?
    };

    let session_ctx = Arc::new({
        let mut ctx = SessionContext::new_with_state(client_state);
        register_postgres_functions(&mut ctx);
        ctx
    });
    register_vairedb_catalog_schema(&session_ctx, Arc::clone(&catalog))?;
    setup_pg_catalog_schema(&session_ctx)?;
    refresh_catalog_tables(&session_ctx, &catalog)?;

    let local_ctx = Arc::new({
        let mut ctx = SessionContext::new_with_config(with_postgres_sql_options(
            SessionConfig::new().with_information_schema(true),
        ));
        register_postgres_functions(&mut ctx);
        ctx
    });
    register_vairedb_catalog_schema(&local_ctx, Arc::clone(&catalog))?;
    setup_pg_catalog_schema(&local_ctx)?;
    // The user's own tables are registered here too, and not only in the distributed
    // context, because `pg_class` and `information_schema.tables` are answered *from
    // this context* — a table missing here is a table no client can see, which is what
    // used to make `\d` and `\dt` come back empty.
    //
    // Registering them makes them resolvable here, not executable: the per-shard scan
    // these providers plan only runs once a core node holds it. A statement that reads
    // both metadata and a user table is therefore refused by name rather than planned —
    // see `catalog_routing::reject_catalog_join_to_user_data`.
    refresh_catalog_tables(&local_ctx, &catalog)?;

    tracing::info!(%addr, "Ballista scheduler started");

    Ok(BallistaSchedulerHandle {
        addr,
        session_ctx,
        local_ctx,
    })
}

/// Apply the session settings that make DataFusion answer the way PostgreSQL does.
///
/// `parse_float_as_decimal` is the one a client can observe directly. DataFusion reads
/// an unsuffixed decimal literal as `Float64`, so `0.1 + 0.2 = 0.3` answers **false**
/// on the read path — while on the write path the same literal reaches DuckDB, which
/// follows PostgreSQL and reads it as `numeric`, and answers **true**. One database
/// cannot hold both answers, and the contract is PostgreSQL's: the literal is exact.
/// It also keeps a large integer literal from losing digits to a float mantissa.
///
/// **`prefer_hash_join` is deliberately left as `SessionConfig::new_with_ballista` sets
/// it — `false`** — even though DataFusion's own default is `true` and only the hash join
/// is null-aware. That looks like the obvious place to fix `NOT IN (subquery)`, and it is
/// not; it was tried on a 5-node cluster and is worse than the defect it targets:
///
/// * DataFusion's null-aware anti join is only correct as a **broadcast**, since one NULL
///   on the right has to suppress *every* left row, so `JoinSelection` stamps it
///   `CollectLeft`.
/// * Ballista's distributed planner then sees a `CollectLeft` join whose output is driven
///   by the build side, which is not broadcast-safe, demotes it to a shuffle join, and on
///   the way tries to swap the sides — `LeftAnti` becomes `RightAnti`, which
///   `HashJoinExec` refuses to build with `null_aware` set.
/// * The job therefore dies inside the scheduler with
///   `null_aware can only be true for LeftAnti joins, got RightAnti`, **and the client
///   never hears about it**: the failed stage reports no status, so `psql` hangs.
///
/// A null-aware anti join simply cannot be shipped by this Ballista version, and turning
/// the option on also changes the operator under every other join in the cluster. So
/// `NOT IN (subquery)` is made null-correct where it can be — in the AST, before
/// planning, by `compat_rewrite::rewrite_not_in_subqueries`, which spells the predicate so
/// that a *non*-null-aware anti join answers what PostgreSQL answers. That rewrite is
/// itself constrained by a second Ballista limit measured on the same cluster: a semi or
/// anti join with **no equijoin key** returns no rows at all, so the respelling may only
/// use an anti join on a bare equality and has to ask the two NULL questions as
/// uncorrelated aggregates instead.
fn with_postgres_sql_options(config: SessionConfig) -> SessionConfig {
    let mut config = config;
    config.options_mut().sql_parser.parse_float_as_decimal = true;
    config
}

/// Register the PostgreSQL built-in functions DataFusion does not ship.
///
/// Every context that plans **or executes** any part of a read needs the identical
/// set. A UDF crosses the wire as a name, so one registered on the planner alone
/// resolves at planning time and then fails on the node that runs the stage — which
/// is why this is called on the client context, on the scheduler's own state, and on
/// the executor's state in `vairedb-core`.
///
/// Only the `math` category is compiled in: in `datafusion-pg-functions` 0.1 the other
/// categories exist as empty modules, so enabling their features would register
/// nothing while implying VaireDB had gained them. Revisit per category as upstream
/// fills them in, skipping the ones whose types VaireDB does not store.
///
/// Giving a PostgreSQL *aggregate* the result type PostgreSQL gives it is not done here
/// and not by a session rule at all — see
/// [`crate::pgwire_handler::pg_aggregate_widening`] for why it has to be a rewrite of the
/// plan the client is told about.
pub(crate) fn register_postgres_functions(
    registry: &mut dyn datafusion::execution::FunctionRegistry,
) {
    let count = datafusion_pg_functions::register_all(registry);
    tracing::debug!(count, "registered PostgreSQL built-in functions");
    // The `pg_catalog` scalar functions. `setup_pg_catalog_schema` registers these on the
    // two contexts that answer catalog queries, and that is not the same set of contexts:
    // the scheduler's own state never calls it, so a plan carrying `format_type` over a
    // column could be planned and then not be decoded. See that module's doc.
    if let Err(e) = vairedb_common::pg_udf::register_pg_catalog_scalar_functions(registry) {
        tracing::warn!(error = %e, "failed to register the pg_catalog scalar functions");
    }
    // The ordered-set aggregates, which DataFusion either lacks (`percentile_disc`) or
    // answers imprecisely (`percentile_cont`). They live in `vairedb-common` because the
    // executor has to register the identical pair — see that module's doc.
    if let Err(e) = vairedb_common::udaf::register_ordered_set_aggregates(registry) {
        tracing::warn!(error = %e, "failed to register the ordered-set aggregates");
    }
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
/// `SchedulerTableProvider`s, so queries can plan scans against them. Existing
/// registrations are left untouched (the function is idempotent).
///
/// Called for **both** session contexts: the distributed one plans the reads, and the
/// local one is what `pg_class` and `information_schema` are answered from, so a table
/// absent there is a table the client cannot see.
pub fn refresh_catalog_tables(ctx: &SessionContext, catalog: &MetadataCatalog) -> Result<()> {
    let tables = catalog.list_tables()?;

    for table_meta in &tables {
        // Register/look up under a bare TableReference so the canonical logical
        // name is used verbatim — passing a &str would re-run identifier
        // normalization and lowercase quoted names, breaking resolution.
        let table_ref = datafusion::common::TableReference::bare(table_meta.table_name.clone());
        if ctx.table_exist(table_ref.clone()).unwrap_or(false) {
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

        // The declared type strings are only in hand here, at the one place the schema is
        // built from the catalog, so this is where the text-in-name-only columns are named.
        let opaque_text_columns = OpaqueTextColumns::from_declared_types(
            table_meta
                .columns
                .iter()
                .map(|col| (col.name.as_str(), col.data_type.as_str())),
        );

        let provider = Arc::new(SchedulerTableProvider {
            table_name: table_meta.table_name.clone(),
            shards,
            schema,
            opaque_text_columns,
        });

        ctx.register_table(table_ref, provider).map_err(|e| {
            CoordinatorError::Internal(format!(
                "failed to register table '{}' in query engine: {}",
                table_meta.table_name, e
            ))
        })?;
    }

    Ok(())
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
    /// Which of `schema`'s text columns the shard does not store as text, which decides
    /// what may be pushed into the shard's own `SELECT`. Not derivable from `schema`: the
    /// whole point is that these columns *look* like `Utf8` there.
    opaque_text_columns: OpaqueTextColumns,
}

impl SchedulerTableProvider {
    /// Create a provider for `table_name` over the given `shards` and `schema`.
    ///
    /// The declared column types are not available here, so no predicate will be pushed
    /// onto a text column. Use [`with_opaque_text_columns`](Self::with_opaque_text_columns)
    /// to say which of them the shard really stores as text.
    pub fn new(table_name: String, shards: Vec<ShardMeta>, schema: Arc<Schema>) -> Self {
        Self {
            table_name,
            shards,
            schema,
            opaque_text_columns: OpaqueTextColumns::default(),
        }
    }

    /// Record which text columns are text in name only. See [`OpaqueTextColumns`].
    pub fn with_opaque_text_columns(mut self, opaque: OpaqueTextColumns) -> Self {
        self.opaque_text_columns = opaque;
        self
    }

    /// The text columns the shard does not store as text.
    pub fn opaque_text_columns(&self) -> &OpaqueTextColumns {
        &self.opaque_text_columns
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
        limit: Option<usize>,
    ) -> RemoteDuckDbScanExec {
        RemoteDuckDbScanExec::new(
            crate::util::shard_table_name(&self.table_name, shard.hash_bucket),
            Arc::clone(projected_schema),
            projection.cloned(),
            filter_exprs.to_vec(),
            Some(shard.primary_node_id.clone()),
            shard.replica_node_ids.clone(),
        )
        .with_limit(limit)
    }
}

#[async_trait::async_trait]
impl TableProvider for SchedulerTableProvider {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Which predicates a shard can evaluate — see
    /// [`filter_pushdown`](super::filter_pushdown) for the allow-list and for why nothing
    /// is ever reported `Exact`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(super::filter_pushdown::pushdown_support(
            &self.schema,
            &self.opaque_text_columns,
            filters,
        ))
    }

    /// `limit` is passed through to every shard, which is sound because it is a *hint*:
    /// each shard may return fewer rows than the whole query wants, and returning at most
    /// `limit` rows per shard can only ever be a superset of the `limit` rows the
    /// coordinator's own `LIMIT` then keeps. DataFusion does not offer a limit here when a
    /// `FilterExec` sits between the limit and the scan, which is exactly the case where
    /// truncating early would drop rows the predicate was going to admit.
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = if let Some(proj) = projection {
            Arc::new(self.schema.project(proj)?)
        } else {
            Arc::clone(&self.schema)
        };

        let filter_exprs = super::filter_pushdown::duckdb_predicates(
            &self.schema,
            &self.opaque_text_columns,
            filters,
        );

        if self.shards.len() <= 1 {
            let scan = match self.shards.first() {
                Some(shard) => self.scan_exec_for_shard(
                    shard,
                    &projected_schema,
                    projection,
                    &filter_exprs,
                    limit,
                ),
                // A table with no assigned shards scans its bare (un-suffixed)
                // name with no node affinity — there is no shard to target.
                None => RemoteDuckDbScanExec::new(
                    self.table_name.clone(),
                    projected_schema,
                    projection.cloned(),
                    filter_exprs,
                    None,
                    Vec::new(),
                )
                .with_limit(limit),
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
                    limit,
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array, Int32Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    /// A context registered the way every read-path context is, holding `t(i integer)`
    /// with one partition per element of `partitions`.
    ///
    /// The partitioning is the point: an aggregate over more than one partition is
    /// planned as a partial/final pair, which is the same split a distributed read makes
    /// across shards — so it exercises the accumulator's `state`/`merge_batch` rather
    /// than only its `evaluate`.
    fn context_over(partitions: &[&[i32]]) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let batches: Vec<Vec<RecordBatch>> = partitions
            .iter()
            .map(|values| {
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int32Array::from(values.to_vec()))],
                    )
                    .expect("the test batch is well formed"),
                ]
            })
            .collect();
        let table = MemTable::try_new(schema, batches).expect("the test table is well formed");

        let mut ctx = SessionContext::new_with_config(with_postgres_sql_options(
            SessionConfig::new().with_target_partitions(4),
        ));
        register_postgres_functions(&mut ctx);
        ctx.register_table("t", Arc::new(table))
            .expect("registering the test table");
        ctx
    }

    async fn one_row(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must run: {e}"));
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "`{sql}` must answer exactly one row");
        batches
            .into_iter()
            .find(|b| b.num_rows() == 1)
            .expect("the one row")
    }

    async fn float8(ctx: &SessionContext, sql: &str) -> f64 {
        let batch = one_row(ctx, sql).await;
        let column = batch.column(0);
        assert_eq!(
            column.data_type(),
            &DataType::Float64,
            "`{sql}` must answer float8, PostgreSQL's type for percentile_cont"
        );
        column
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a float8 column")
            .value(0)
    }

    // The whole reason `percentile_cont` is shadowed: DataFusion's own version floors the
    // interpolation weight to six decimals and answers 9.099999 here.
    #[tokio::test]
    async fn percentile_cont_interpolates_exactly() {
        let ctx = context_over(&[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
        assert_eq!(
            float8(
                &ctx,
                "SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY i) FROM t"
            )
            .await,
            9.1
        );
        // The alias DataFusion registers has to survive the shadowing.
        assert_eq!(
            float8(
                &ctx,
                "SELECT quantile_cont(0.9) WITHIN GROUP (ORDER BY i) FROM t"
            )
            .await,
            9.1
        );
    }

    // Split across partitions the aggregate runs as partial + final, so this is the test
    // that proves the state a shard sends can be merged — the property a distributed read
    // depends on.
    #[tokio::test]
    async fn a_percentile_survives_being_split_and_merged() {
        let split = context_over(&[&[1, 2, 3, 4], &[5, 6, 7], &[8, 9, 10]]);
        let whole = context_over(&[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);

        // Assert the split is real, so the comparison below cannot pass by planning the
        // same single-pass aggregate twice.
        let plan = split
            .sql("SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY i) FROM t")
            .await
            .expect("the percentile must plan")
            .create_physical_plan()
            .await
            .expect("the percentile must have a physical plan");
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert!(
            rendered.contains("mode=Partial") && rendered.contains("mode=Final"),
            "the aggregate must be split into a partial and a final pass: {rendered}"
        );

        for sql in [
            "SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY i) FROM t",
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM t",
            "SELECT percentile_cont(0.33) WITHIN GROUP (ORDER BY i DESC) FROM t",
        ] {
            assert_eq!(
                float8(&split, sql).await,
                float8(&whole, sql).await,
                "`{sql}` must not depend on how the rows were partitioned"
            );
        }
    }

    // PostgreSQL's `percentile_disc` returns one of the input values, with the ordered
    // column's own type — so an integer column answers `integer`, not `float8`.
    #[tokio::test]
    async fn percentile_disc_returns_an_input_value_with_its_own_type() {
        let ctx = context_over(&[&[1, 2, 3, 4], &[5, 6, 7, 8, 9, 10]]);
        let batch = one_row(
            &ctx,
            "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY i) FROM t",
        )
        .await;
        assert_eq!(batch.column(0).data_type(), &DataType::Int32);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("an integer column")
                .value(0),
            9
        );
    }

    // A fraction outside 0..1 is an error, not a wrong answer.
    #[tokio::test]
    async fn a_fraction_outside_the_range_is_refused() {
        let ctx = context_over(&[&[1, 2, 3]]);
        for sql in [
            "SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY i) FROM t",
            "SELECT percentile_disc(-0.5) WITHIN GROUP (ORDER BY i) FROM t",
        ] {
            let outcome = match ctx.sql(sql).await {
                Ok(frame) => frame.collect().await.map(|_| ()),
                Err(e) => Err(e),
            };
            let message = outcome
                .expect_err("the fraction must be refused")
                .to_string();
            assert!(
                message.contains("is not between 0 and 1"),
                "`{sql}` must say why: {message}"
            );
        }
    }

    // An empty group is a null, the way every aggregate answers one.
    #[tokio::test]
    async fn an_empty_group_is_null() {
        let ctx = context_over(&[&[1, 2, 3]]);
        for sql in [
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM t WHERE i > 100",
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY i) FROM t WHERE i > 100",
        ] {
            let batch = one_row(&ctx, sql).await;
            assert!(batch.column(0).is_null(0), "`{sql}` must answer null");
        }
    }

    /// A context holding `l(k)` and `r(k)`, both nullable, split across partitions so
    /// the join is repartitioned the way a distributed read's is.
    fn join_context(config: SessionConfig, l: &[Option<i32>], r: &[Option<i32>]) -> SessionContext {
        let ctx = SessionContext::new_with_config(config.with_target_partitions(4));
        for (name, values) in [("l", l), ("r", r)] {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
            let (head, tail) = values.split_at(values.len() / 2);
            let batches: Vec<Vec<RecordBatch>> = [head, tail]
                .iter()
                .map(|slice| {
                    vec![
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(Int32Array::from(slice.to_vec()))],
                        )
                        .expect("the test batch is well formed"),
                    ]
                })
                .collect();
            let table = MemTable::try_new(schema, batches).expect("the test table is well formed");
            ctx.register_table(name, Arc::new(table))
                .expect("registering the test table");
        }
        ctx
    }

    /// The keys `sql` answers, sorted, with a NULL rendered as `-1` so it is visible.
    async fn keys(ctx: &SessionContext, sql: &str) -> Vec<i32> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must run: {e}"));
        let mut keys = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("an int4 column");
            for row in 0..batch.num_rows() {
                keys.push(if column.is_null(row) {
                    -1
                } else {
                    column.value(row)
                });
            }
        }
        keys.sort_unstable();
        keys
    }

    /// `l.k = 10, 20, 30, NULL` and `r.k = 20, 99, 50` — the fixture the cluster
    /// measurement used.
    const L_KEYS: [Option<i32>; 4] = [Some(10), Some(20), Some(30), None];
    const R_KEYS: [Option<i32>; 3] = [Some(20), Some(99), Some(50)];

    // Why `with_postgres_sql_options` leaves `prefer_hash_join` alone, kept as a test so
    // the measurement cannot be lost. Under Ballista's own value the anti join behind
    // `NOT IN (subquery)` is a `SortMergeJoinExec`, which treats a NULL as merely unequal,
    // and two of these three answers are wrong. Turning the hash join on makes them right
    // *in process* and unshippable on a cluster — see the function's own doc — so the fix
    // is `compat_rewrite::rewrite_not_in_subqueries`, which respells the predicate before
    // it is planned. `pgwire_handler::compat_rewrite` holds the tests for the answers a
    // client sees; these are the operator's.
    //
    // If DataFusion ever makes the sort-merge anti join null-aware, this test fails and
    // the rewrite can go.
    #[tokio::test]
    async fn the_anti_join_ballista_plans_is_not_null_aware() {
        let mut config = with_postgres_sql_options(SessionConfig::new());
        config.options_mut().optimizer.prefer_hash_join = false;
        let ctx = join_context(config, &L_KEYS, &R_KEYS);

        // PostgreSQL answers `10, 30`: `NULL NOT IN (20, 99, 50)` is NULL, not true, so
        // the NULL row is not returned. Here it is.
        assert_eq!(
            keys(&ctx, "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r)").await,
            vec![-1, 10, 30]
        );
        // PostgreSQL answers nothing: a NULL among the candidates makes every
        // non-matching row NULL too. Here the NULL row comes back.
        assert_eq!(
            keys(&ctx, "SELECT k FROM l WHERE k NOT IN (SELECT k FROM l)").await,
            vec![-1]
        );
        // The empty subquery is the one case that is already right — there is no row to
        // compare against, so the predicate is true for every left row including the
        // NULL, and the join is optimized away before the operator matters.
        assert_eq!(
            keys(
                &ctx,
                "SELECT k FROM l WHERE k NOT IN (SELECT k FROM r WHERE k > 1000)"
            )
            .await,
            vec![-1, 10, 20, 30]
        );
    }
}
