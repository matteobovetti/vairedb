//! PostgreSQL wire-protocol query handlers for the coordinator.
//!
//! Implements both the simple- and extended-query protocols (via the `pgwire`
//! crate) and dispatches each statement to the right subsystem: SELECTs run on a
//! DataFusion context (distributed `session_ctx` or local catalog `local_ctx`),
//! while writes and DDL are routed to the shard-aware DML/DDL handlers. This file
//! holds the top-level handler wiring and the read/parameter-decoding paths;
//! `dml.rs` and `ddl.rs` carry the write and schema-change logic.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_pg::datatypes::df::deserialize_parameters;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ParamValues;
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use futures::sink::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::catalog::MetadataCatalog;
use crate::channel_pool::ChannelPool;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::catalog_routing::{catalog_table_names, references_catalog_schema};
use crate::pgwire_handler::encoding;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_datafusion_error, make_vdb_error,
};
use crate::pgwire_handler::introspection;
use crate::pgwire_handler::parser::{self, VairePrepared, VaireQueryParser};
use crate::pgwire_handler::query_router::{self, QueryType};
use crate::pgwire_handler::sequences;
use crate::pgwire_handler::session::SessionState;
use crate::pgwire_handler::session_params;
use crate::pgwire_handler::user_types;
use crate::replication::ReplicationManager;
use crate::write_router::WriteRouter;

/// Bundles the per-connection handlers required by `pgwire`'s server interface
/// (startup, simple/extended query, copy, cancel, error). One instance is shared
/// across all client connections.
pub struct VaireDbHandlers {
    query_handler: Arc<VaireDbQueryHandler>,
    startup_handler: Arc<VaireDbStartupHandler>,
}

impl VaireDbHandlers {
    /// Construct the handler set, wiring the catalog, replication manager, gRPC
    /// channel pool, and the two DataFusion contexts into a shared query handler.
    /// `default_replication_factor` applies to tables created without an explicit
    /// factor; `allow_cross_shard_transactions` lets a transaction block spanning
    /// shard groups commit non-atomically instead of being refused.
    pub fn new(
        catalog: Arc<MetadataCatalog>,
        replication_manager: Arc<ReplicationManager>,
        pool: Arc<ChannelPool>,
        session_ctx: Arc<SessionContext>,
        local_ctx: Arc<SessionContext>,
        default_replication_factor: u32,
        allow_cross_shard_transactions: bool,
    ) -> Self {
        let catalog_table_names = Arc::new(catalog_table_names(&local_ctx));
        let query_parser = Arc::new(VaireQueryParser::new(
            Arc::clone(&session_ctx),
            Arc::clone(&local_ctx),
            Arc::clone(&catalog_table_names),
            Arc::clone(&catalog),
        ));
        Self {
            query_handler: Arc::new(VaireDbQueryHandler {
                catalog: Arc::clone(&catalog),
                replication_manager,
                pool,
                write_router: WriteRouter::new(catalog),
                session_ctx,
                local_ctx,
                default_replication_factor,
                allow_cross_shard_transactions,
                query_parser,
                catalog_table_names,
            }),
            startup_handler: Arc::new(VaireDbStartupHandler),
        }
    }
}

impl PgWireServerHandlers for VaireDbHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup_handler)
    }

    /// The same handler that ran the `COPY` statement: importing a row is an
    /// INSERT, so the copy sub-protocol reaches the write path through the object
    /// that already owns it rather than through a second one. The per-connection
    /// state a copy needs lives in the session — see
    /// [`crate::pgwire_handler::copy_stream`].
    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        Arc::clone(&self.query_handler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NoopHandler)
    }
}

/// Build the error returned for a statement the coordinator does not implement
/// (SET/SHOW, CREATE VIEW, EXPLAIN, etc.). These used to fall through to a fake
/// `OK`, silently misleading clients; now they fail with `FeatureNotSupported`
/// (SQLSTATE `0A000`) naming the command.
fn unsupported_statement_error(stmt: &crate::sqlparser::ast::Statement) -> PgWireError {
    // Sequences and user-defined types are the commands here that are refused by
    // decision rather than by not being built yet, so they answer with the reason
    // instead of a bare "not supported" a client would reasonably read as "not
    // yet".
    if matches!(
        stmt,
        crate::sqlparser::ast::Statement::CreateSequence { .. }
    ) {
        return sequences::sequence_error("CREATE SEQUENCE");
    }
    if let Some(command) = user_types::refused_user_type(stmt) {
        return user_types::user_type_error(command);
    }

    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{} is not supported by VaireDB",
            unsupported_statement_label(stmt)
        ),
    )
}

/// Human-readable command name for an unsupported statement, used in the error
/// message so the client learns which command was rejected.
///
/// The fallback `"this statement"` is a last resort, not a default: a client that
/// gets it cannot tell which of the statements it sent was refused. Every command
/// the gap analysis lists as reaching this rejection point is named here.
fn unsupported_statement_label(stmt: &crate::sqlparser::ast::Statement) -> &'static str {
    use crate::sqlparser::ast::Statement;

    // The user-type family names itself in one place, so the label here and the
    // explained refusal above cannot drift apart.
    if let Some(command) = user_types::refused_user_type(stmt) {
        return command;
    }

    match stmt {
        // `SET` and `SHOW` are not listed: both classify as
        // [`QueryType::SessionParam`] and are answered (or refused by parameter
        // name, which is more specific than this label could be) in
        // [`crate::pgwire_handler::session_params`]. `EXPLAIN` and `DESCRIBE` are
        // absent for the same reason — they classify as [`QueryType::Explain`], and
        // the forms that cannot be answered truthfully name the *form* in
        // [`crate::pgwire_handler::introspection`].
        Statement::CreateSequence { .. } => "CREATE SEQUENCE",
        Statement::Comment { .. } => "COMMENT ON",
        Statement::Analyze { .. } => "ANALYZE",
        Statement::Call(_) => "CALL",
        Statement::Use(_) => "USE",
        _ => "this statement",
    }
}

/// Startup handler that performs no authentication or parameter negotiation;
/// accepts every connection as-is.
pub(crate) struct VaireDbStartupHandler;

#[async_trait]
impl NoopStartupHandler for VaireDbStartupHandler {}

/// Handles every SQL statement on a connection, routing reads, writes, and DDL
/// to the appropriate subsystem. Shared (via `Arc`) across all connections, so it
/// holds no per-connection state.
pub(crate) struct VaireDbQueryHandler {
    pub(super) catalog: Arc<MetadataCatalog>,
    pub(super) replication_manager: Arc<ReplicationManager>,
    pub(super) pool: Arc<ChannelPool>,
    pub(super) write_router: WriteRouter,
    pub(super) session_ctx: Arc<SessionContext>,
    pub(super) local_ctx: Arc<SessionContext>,
    pub(super) default_replication_factor: u32,
    /// When set, a transaction block whose writes span shard groups is committed
    /// group by group instead of being refused — faster to adopt, but a failure
    /// part-way through leaves the earlier groups applied.
    pub(super) allow_cross_shard_transactions: bool,
    query_parser: Arc<VaireQueryParser>,
    /// Lowercased bare names of `pg_catalog` tables, used to route unqualified
    /// catalog introspection (e.g. `pg_class`) to `local_ctx`.
    pub(super) catalog_table_names: Arc<std::collections::HashSet<String>>,
}

#[cfg(test)]
impl VaireDbQueryHandler {
    /// A handler wired to an empty catalog and no reachable core nodes, for unit
    /// tests of the paths that decide *whether* to run a statement. Anything that
    /// actually ships a write will fail to resolve a shard, which is what makes
    /// this cheap enough to build per test.
    pub(super) fn for_tests(allow_cross_shard_transactions: bool) -> Self {
        let catalog = Arc::new(super::test_catalog::scratch_catalog("handler"));
        let pool = Arc::new(ChannelPool::new());
        let replication_manager = Arc::new(ReplicationManager::new(
            Arc::clone(&catalog),
            Arc::clone(&pool),
            crate::replication::RetryConfig::default(),
        ));
        let session_ctx = Arc::new(SessionContext::new());
        let local_ctx = Arc::new(SessionContext::new());
        let catalog_table_names = Arc::new(catalog_table_names(&local_ctx));

        Self {
            write_router: WriteRouter::new(Arc::clone(&catalog)),
            query_parser: Arc::new(VaireQueryParser::new(
                Arc::clone(&session_ctx),
                Arc::clone(&local_ctx),
                Arc::clone(&catalog_table_names),
                Arc::clone(&catalog),
            )),
            catalog,
            replication_manager,
            pool,
            session_ctx,
            local_ctx,
            default_replication_factor: 1,
            allow_cross_shard_transactions,
            catalog_table_names,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for VaireDbQueryHandler {
    /// Parse and execute every statement in a simple-protocol query string,
    /// returning one response per statement. Returns a `SqlSyntaxError` if parsing
    /// fails. No bind parameters are possible on this path.
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = SessionState::for_client(client);
        // The error carries its own code: a parse failure is a syntax error, but a
        // statement refused at parse time — a `COLLATE` the compatibility parser would
        // otherwise have discarded — is `feature_not_supported`, and a client that
        // branches on SQLSTATE needs to be able to tell those apart.
        let statements = parser::parse_sql(query)
            .map_err(|e| make_vdb_error(e.vdb_error_code(), e.to_string()))?;

        let mut responses = Vec::with_capacity(statements.len());

        for stmt in &statements {
            responses.push(self.execute_one_statement(stmt, &session).await?);
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for VaireDbQueryHandler {
    type Statement = VairePrepared;
    type QueryParser = VaireQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    /// Execute a bound portal. SELECTs bind their decoded parameters into the
    /// cached logical plan and stream rows; writes/DDL decode parameters to
    /// `ScalarValue`s and route through the write path. Returns `EmptyQuery` for
    /// an empty statement.
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let session = SessionState::for_client(client);
        let prepared = &portal.statement.statement;
        let Some(stmt) = &prepared.stmt else {
            return Ok(Response::EmptyQuery);
        };

        if prepared.query_type == QueryType::SessionParam {
            // Answered from the connection's own parameter map — no plan, no shard,
            // and no bind parameters to decode: a runtime parameter takes a literal
            // or a bare word, never a `$1`. Routed before the write path because a
            // `SHOW` returns rows, which the write path has no way to produce.
            let result = async {
                self.check_transaction_allows(stmt, &prepared.query_type, &session)
                    .await?;
                session_params::handle_session_param(stmt, &session, &portal.result_column_format)
                    .await
            }
            .await;
            self.note_failure_in_transaction(result.is_err(), &session)
                .await;
            return result;
        }

        if prepared.query_type == QueryType::Explain {
            // The plan is the answer, so it was built at Parse and is reused here
            // verbatim. Bind parameters are not decoded: `EXPLAIN` reports the shape
            // of a query, and a placeholder's value does not change it — a client
            // that binds one gets the same plan a `$1` in a SELECT would produce.
            let result = async {
                self.check_transaction_allows(stmt, &prepared.query_type, &session)
                    .await?;
                let plan = prepared.plan.as_ref().ok_or_else(|| {
                    make_vdb_error(VdbErrorCode::InternalError, "missing plan for EXPLAIN")
                })?;
                let is_catalog = self.is_catalog_query(stmt);
                let ctx = if is_catalog {
                    &self.local_ctx
                } else {
                    &self.session_ctx
                };
                introspection::execute_introspection(
                    ctx,
                    plan,
                    &portal.result_column_format,
                    &introspection::error_context(stmt),
                )
                .await
            }
            .await;
            self.note_failure_in_transaction(result.is_err(), &session)
                .await;
            return result;
        }

        if prepared.query_type == QueryType::Select {
            // Read path: bind typed parameters into the cached logical plan.
            let result = async {
                self.check_transaction_allows(stmt, &prepared.query_type, &session)
                    .await?;
                let plan = prepared.plan.as_ref().ok_or_else(|| {
                    make_vdb_error(VdbErrorCode::InternalError, "missing plan for SELECT")
                })?;
                let param_values = self.decode_param_values(portal)?;
                self.execute_select_plan(prepared, plan, param_values, &portal.result_column_format)
                    .await
            }
            .await;
            self.note_failure_in_transaction(result.is_err(), &session)
                .await;
            return result;
        }

        // Write/DDL path: parameters (if any) are bound on DuckDB. Decode them to
        // ScalarValues for shard routing and transport.
        let params = match self.decode_param_scalars(portal) {
            Ok(params) => params,
            Err(e) => {
                session.transaction().await.mark_failed();
                return Err(e);
            }
        };
        self.execute_write_statement(stmt, &prepared.query_type, &params, &session)
            .await
    }

    /// Describe a prepared statement: report its parameter OIDs and, for SELECT,
    /// the result row description. Writes/DDL report an empty row description.
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parser = self.query_parser();
        let param_types = parser.get_parameter_types(&target.statement)?;
        // get_result_schema yields no columns for non-SELECT, so this reports the
        // parameter OIDs plus an empty row description for writes/DDL.
        let fields = parser.get_result_schema(&target.statement, None)?;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    /// Describe a bound portal: for SELECT, advertise columns using the
    /// per-column format the client requested at Bind so the RowDescription
    /// matches the DataRows that Execute will send. Non-SELECT portals carry no
    /// data.
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // `SHOW` and `EXPLAIN` produce rows too, so they have to be described like a
        // SELECT: a client that was told `no data` would not read the rows that
        // follow.
        if matches!(
            portal.statement.statement.query_type,
            QueryType::Select | QueryType::SessionParam | QueryType::Explain
        ) {
            // Advertise the columns with the same per-column format the client
            // requested in Bind, so the RowDescription matches the DataRows
            // Execute will send.
            let fields = self.query_parser().get_result_schema(
                &portal.statement.statement,
                Some(&portal.result_column_format),
            )?;
            Ok(DescribePortalResponse::new(fields))
        } else {
            Ok(DescribePortalResponse::no_data())
        }
    }
}

impl VaireDbQueryHandler {
    /// A query is catalog-introspection if it references any of the metadata schemas. Such
    /// queries execute on `local_ctx` (plain DataFusion) rather than the Ballista
    /// `session_ctx`, since they are metadata — not sharded user data — and frequently join
    /// across catalog tables in ways that should not be distributed.
    pub(super) fn is_catalog_query(&self, stmt: &crate::sqlparser::ast::Statement) -> bool {
        references_catalog_schema(stmt, &self.catalog_table_names)
    }

    /// Dispatch a statement parsed by the simple-query protocol. No bind
    /// parameters are possible here, so writes run with an empty parameter list.
    async fn execute_one_statement(
        &self,
        stmt: &crate::sqlparser::ast::Statement,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let query_type = query_router::classify_statement(stmt);
        let result = async {
            if query_type == QueryType::TransactionControl {
                return self.handle_transaction_control(stmt, session).await;
            }
            sequences::reject_sequence_use(stmt)?;
            self.check_transaction_allows(stmt, &query_type, session)
                .await?;
            self.reject_view_as_table(stmt, &query_type)?;
            match query_type {
                QueryType::Select => self.handle_select(stmt).await,
                QueryType::Explain => self.handle_introspection(stmt, &Format::UnifiedText).await,
                QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    self.handle_dml(stmt, &query_type, &[], session).await
                }
                QueryType::Merge => self.handle_merge(stmt, &[]).await,
                QueryType::CreateTable => self.handle_create_table(stmt, session).await,
                QueryType::DropTable => self.handle_drop_table(stmt).await,
                QueryType::AlterTable => self.handle_alter_table(stmt).await,
                QueryType::TruncateTable => self.handle_truncate(stmt).await,
                QueryType::CreateIndex => self.handle_create_index(stmt).await,
                QueryType::DropIndex => self.handle_drop_index(stmt).await,
                QueryType::CreateView => self.handle_create_view(stmt).await,
                QueryType::AlterView => self.handle_alter_view(stmt).await,
                QueryType::DropView => self.handle_drop_view(stmt).await,
                QueryType::CreateSchema => self.handle_create_schema(stmt).await,
                QueryType::DropSchema => self.handle_drop_schema(stmt).await,
                QueryType::Copy => self.handle_copy(stmt, session).await,
                // The simple-query protocol has no Bind, so every value on the wire
                // is text.
                QueryType::SessionParam => {
                    session_params::handle_session_param(stmt, session, &Format::UnifiedText).await
                }
                QueryType::TransactionControl | QueryType::Other => {
                    Err(unsupported_statement_error(stmt))
                }
            }
        }
        .await;

        self.note_failure_in_transaction(result.is_err(), session)
            .await;
        result
    }

    /// Dispatch a write/DDL statement from the extended protocol, carrying any
    /// decoded bind parameters into the write path. Transaction control arrives
    /// here too: it is not a read, so the extended handler routes it this way.
    async fn execute_write_statement(
        &self,
        stmt: &crate::sqlparser::ast::Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let result = async {
            if *query_type == QueryType::TransactionControl {
                return self.handle_transaction_control(stmt, session).await;
            }
            sequences::reject_sequence_use(stmt)?;
            self.check_transaction_allows(stmt, query_type, session)
                .await?;
            self.reject_view_as_table(stmt, query_type)?;
            match query_type {
                QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    self.handle_dml(stmt, query_type, params, session).await
                }
                QueryType::Merge => self.handle_merge(stmt, params).await,
                QueryType::CreateTable => self.handle_create_table(stmt, session).await,
                QueryType::DropTable => self.handle_drop_table(stmt).await,
                QueryType::AlterTable => self.handle_alter_table(stmt).await,
                QueryType::TruncateTable => self.handle_truncate(stmt).await,
                QueryType::CreateIndex => self.handle_create_index(stmt).await,
                QueryType::DropIndex => self.handle_drop_index(stmt).await,
                QueryType::CreateView => self.handle_create_view(stmt).await,
                QueryType::AlterView => self.handle_alter_view(stmt).await,
                QueryType::DropView => self.handle_drop_view(stmt).await,
                QueryType::CreateSchema => self.handle_create_schema(stmt).await,
                QueryType::DropSchema => self.handle_drop_schema(stmt).await,
                QueryType::Copy => self.handle_copy(stmt, session).await,
                // A read, an `EXPLAIN` and a session parameter each reach the
                // extended protocol through their own branch in `do_query`, so none
                // of them arrives here.
                QueryType::Select
                | QueryType::Explain
                | QueryType::SessionParam
                | QueryType::TransactionControl
                | QueryType::Other => Err(unsupported_statement_error(stmt)),
            }
        }
        .await;

        self.note_failure_in_transaction(result.is_err(), session)
            .await;
        result
    }

    /// Abort the client's transaction block when `failed`, matching PostgreSQL:
    /// after an error inside a block, every following statement is refused with
    /// `25P02` until the client ends the block or rolls back to a savepoint. A
    /// no-op outside a block, and for a statement that already ended the block (a
    /// failed `COMMIT` leaves the session idle).
    ///
    /// Takes a `bool` rather than the response: a `Response` is `Send` but not
    /// `Sync`, so borrowing one across this `await` would make the enclosing
    /// handler future non-`Send`.
    async fn note_failure_in_transaction(&self, failed: bool, session: &SessionState) {
        if failed {
            session.transaction().await.mark_failed();
        }
    }

    /// Decode the portal's bound parameters into DataFusion `ParamValues`, using
    /// the cached plan's inferred placeholder types as coercion targets when
    /// available (both SELECT and write plans carry these).
    fn decode_param_values(&self, portal: &Portal<VairePrepared>) -> PgWireResult<ParamValues> {
        let inferred = match &portal.statement.statement.plan {
            Some(plan) => plan
                .get_parameter_types()
                .map_err(|e| make_vdb_error(VdbErrorCode::InternalError, e.to_string()))?,
            None => HashMap::new(),
        };
        let ordered = parser::ordered_param_types(&inferred);
        deserialize_parameters(portal, &ordered)
    }

    /// Decode the portal's bound parameters into positional `ScalarValue`s for
    /// the write path (shard routing + DuckDB prepared-statement binding). Uses
    /// the write plan's inferred column types as coercion targets when present.
    fn decode_param_scalars(
        &self,
        portal: &Portal<VairePrepared>,
    ) -> PgWireResult<Vec<ScalarValue>> {
        Ok(match self.decode_param_values(portal)? {
            ParamValues::List(list) => list.into_iter().map(|s| s.value).collect(),
            ParamValues::Map(map) => map.into_values().map(|s| s.value).collect(),
        })
    }

    /// Bind decoded parameters into the cached SELECT plan and execute it on the
    /// appropriate DataFusion context, streaming the result rows back.
    async fn execute_select_plan(
        &self,
        prepared: &VairePrepared,
        plan: &LogicalPlan,
        param_values: ParamValues,
        result_format: &Format,
    ) -> PgWireResult<Response> {
        let select_ctx = prepared
            .stmt
            .as_ref()
            .and_then(query_router::extract_select_table_name)
            .map(|t| ErrorContext::for_table(&t))
            .unwrap_or_default();

        let bound = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;

        let ctx = if prepared.is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };
        let df = ctx
            .execute_logical_plan(bound)
            .await
            .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;

        encoding::encode_dataframe_response(df, result_format, &select_ctx).await
    }

    /// Plan `query` on the read path, bind `params` into it, run it, and collect
    /// every row. Returns the result schema alongside the batches, so a caller
    /// can check the shape of a result that turned out to be empty.
    ///
    /// This is how a write whose rows come from a query gets rows it can route:
    /// the source is read *to completion first*, then written. That ordering is
    /// the snapshot — `INSERT INTO t SELECT * FROM t` reads the old contents of
    /// `t` and terminates, rather than consuming rows it is appending.
    ///
    /// The whole result is held in memory, exactly as the read path already holds
    /// it to encode a response for the same query.
    pub(super) async fn collect_query_rows(
        &self,
        query: &crate::sqlparser::ast::Statement,
        params: &[ScalarValue],
    ) -> PgWireResult<(SchemaRef, Vec<RecordBatch>)> {
        let is_catalog = self.is_catalog_query(query);
        let ctx = if is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };

        let (plan, select_ctx) = parser::plan_select(ctx, query, is_catalog, &self.catalog).await?;
        let plan = if params.is_empty() {
            plan
        } else {
            let bindings = params
                .iter()
                .map(|value| ScalarAndMetadata::new(value.clone(), None))
                .collect();
            plan.replace_params_with_values(&ParamValues::List(bindings))
                .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?
        };

        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .collect()
            .await
            .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;

        Ok((schema, batches))
    }

    /// Execute a simple-protocol SELECT, choosing the local catalog context for
    /// introspection queries and the distributed context otherwise, and encode
    /// the result as text (the only format the simple protocol uses).
    async fn handle_select(
        &self,
        stmt: &crate::sqlparser::ast::Statement,
    ) -> PgWireResult<Response> {
        let is_catalog = self.is_catalog_query(stmt);
        let ctx = if is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };

        let (plan, select_ctx) = parser::plan_select(ctx, stmt, is_catalog, &self.catalog).await?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;

        // The simple query protocol always returns results in text format.
        encoding::encode_dataframe_response(df, &Format::UnifiedText, &select_ctx).await
    }

    /// Answer an `EXPLAIN`/`DESCRIBE`, planning it on the same context the query it
    /// is about would run on — the plan reported is worthless if it was built
    /// against a different set of registered relations than the read path uses.
    async fn handle_introspection(
        &self,
        stmt: &crate::sqlparser::ast::Statement,
        format: &Format,
    ) -> PgWireResult<Response> {
        let is_catalog = self.is_catalog_query(stmt);
        let ctx = if is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };

        let (plan, err_ctx) =
            introspection::plan_introspection(ctx, stmt, is_catalog, &self.catalog).await?;
        introspection::execute_introspection(ctx, &plan, format, &err_ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label_of(sql: &str) -> &'static str {
        let stmt = parser::parse_sql(sql)
            .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
            .into_iter()
            .next()
            .unwrap();
        unsupported_statement_label(&stmt)
    }

    /// Every statement the gap analysis pins to the classification rejection point
    /// must be named in the error, so a client can tell which command was refused.
    ///
    /// `SET` and `SHOW` are deliberately absent: they classify as
    /// [`QueryType::SessionParam`] and are answered, or refused by *parameter* name,
    /// in [`session_params`] — which is more specific than a command label could be.
    /// `EXPLAIN` and `DESCRIBE` are absent for the same reason: they classify as
    /// [`QueryType::Explain`] and are answered, or refused by *form*, in
    /// [`introspection`].
    #[test]
    fn rejected_commands_are_named_in_the_error() {
        for (sql, want) in [
            ("CREATE SEQUENCE s", "CREATE SEQUENCE"),
            ("CREATE TYPE ty AS ENUM ('a', 'b')", "CREATE TYPE"),
            ("CREATE DOMAIN d AS INTEGER", "CREATE DOMAIN"),
            ("ALTER TYPE ty ADD VALUE 'c'", "ALTER TYPE"),
            ("COMMENT ON TABLE t IS 'x'", "COMMENT ON"),
            ("ANALYZE t", "ANALYZE"),
            ("CALL p()", "CALL"),
            ("USE db", "USE"),
        ] {
            assert_eq!(label_of(sql), want, "wrong label for `{sql}`");
        }
    }

    /// The three statements must reach [`QueryType::SessionParam`] — including
    /// `RESET`, which no parser has and which
    /// [`parser::parse_sql`] rewrites to the `SET … TO DEFAULT` PostgreSQL defines
    /// it to be. If any of them fell back to `Other` it would be refused by name
    /// again, silently undoing the routing.
    #[test]
    fn session_parameter_statements_are_routed_not_refused() {
        for sql in [
            "SET client_encoding = 'UTF8'",
            "SET TIME ZONE 'UTC'",
            "SET LOCAL application_name = 'x'",
            "SET ROLE readonly",
            "SHOW client_encoding",
            "SHOW ALL",
            "SHOW TRANSACTION ISOLATION LEVEL",
            "RESET application_name",
            "RESET ALL",
        ] {
            let stmt = parser::parse_sql(sql)
                .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(
                query_router::classify_statement(&stmt),
                QueryType::SessionParam,
                "`{sql}` should route to the session-parameter handler"
            );
        }
    }

    /// A session parameter is neither shipped to a shard nor planned, so it must
    /// stay off both those paths: the write path would render it back to SQL and
    /// send it to DuckDB, and `wants_verbatim_ast` would re-parse it for no reason.
    #[test]
    fn a_session_parameter_is_not_a_write() {
        assert!(!QueryType::SessionParam.is_write_path());
        assert!(!QueryType::SessionParam.wants_verbatim_ast());
    }

    /// Every spelling of the two introspection commands must reach
    /// [`QueryType::Explain`], including the `DESC` abbreviation and the
    /// `DESCRIBE <query>` form, which parses to a different node than
    /// `DESCRIBE <relation>`. One falling back to `Other` would be refused by name
    /// instead of answered.
    #[test]
    fn introspection_statements_are_routed_not_refused() {
        for sql in [
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE SELECT 1",
            "EXPLAIN (ANALYZE, VERBOSE) SELECT 1",
            "DESCRIBE t",
            "DESC t",
            "DESCRIBE SELECT 1",
        ] {
            let stmt = parser::parse_sql(sql)
                .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(
                query_router::classify_statement(&stmt),
                QueryType::Explain,
                "`{sql}` should route to the introspection handler"
            );
        }
    }

    /// An `EXPLAIN` is planned in the coordinator and never rendered back to SQL, so
    /// it must stay off the write path — which would ship it to a shard — while still
    /// being rewritten by the pg-compatibility parser, exactly as the query inside it
    /// would be on its own.
    #[test]
    fn an_explain_is_a_read_not_a_write() {
        assert!(!QueryType::Explain.is_write_path());
        assert!(!QueryType::Explain.wants_verbatim_ast());
    }

    #[test]
    fn the_error_message_quotes_the_command_name() {
        let stmt = parser::parse_sql("COMMENT ON TABLE t IS 'x'")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let msg = unsupported_statement_error(&stmt).to_string();
        assert!(msg.contains("COMMENT ON"), "got: {msg}");
    }

    /// The two decided limitations answer with the reason, not with the generic
    /// "not supported" — which for them would read as "not yet".
    #[test]
    fn a_decided_limitation_explains_itself() {
        for (sql, reason) in [
            ("CREATE SEQUENCE s", "no sequences"),
            ("CREATE TYPE ty AS ENUM ('a')", "no user-defined types"),
            ("CREATE DOMAIN d AS INTEGER", "no user-defined types"),
            ("ALTER TYPE ty ADD VALUE 'c'", "no user-defined types"),
        ] {
            let stmt = parser::parse_sql(sql)
                .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
                .into_iter()
                .next()
                .unwrap();
            let msg = unsupported_statement_error(&stmt).to_string();
            assert!(msg.contains(reason), "`{sql}` got: {msg}");
        }
    }
}
