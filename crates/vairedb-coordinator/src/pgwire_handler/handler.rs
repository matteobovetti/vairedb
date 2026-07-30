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
use datafusion::common::ParamValues;
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
use crate::pgwire_handler::error_enrichment::{ErrorContext, enrich_generic_error, make_vdb_error};
use crate::pgwire_handler::parser::{self, VairePrepared, VaireQueryParser};
use crate::query_router::{self, QueryType};
use crate::replication::ReplicationManager;
use crate::sql_compat;
use crate::write_router::WriteRouter;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

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
    /// factor.
    pub fn new(
        catalog: Arc<MetadataCatalog>,
        replication_manager: Arc<ReplicationManager>,
        pool: Arc<ChannelPool>,
        session_ctx: Arc<SessionContext>,
        local_ctx: Arc<SessionContext>,
        default_replication_factor: u32,
    ) -> Self {
        let catalog_table_names = Arc::new(catalog_table_names(&local_ctx));
        let query_parser = Arc::new(VaireQueryParser::new(
            Arc::clone(&session_ctx),
            Arc::clone(&local_ctx),
            Arc::clone(&catalog_table_names),
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
                query_parser,
                pg_compat_parser: PostgresCompatibilityParser::new(),
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

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        Arc::new(NoopHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(NoopHandler)
    }
}

/// Build the error returned for a statement the coordinator does not implement
/// (transaction control, SET/SHOW, TRUNCATE, CREATE VIEW, EXPLAIN, etc.). These
/// used to fall through to a fake `OK`, silently misleading clients; now they
/// fail with `FeatureNotSupported` (SQLSTATE `0A000`) naming the command.
fn unsupported_statement_error(stmt: &sqlparser::ast::Statement) -> PgWireError {
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
fn unsupported_statement_label(stmt: &sqlparser::ast::Statement) -> &'static str {
    use sqlparser::ast::Statement;
    match stmt {
        Statement::StartTransaction { .. } => "transaction control (BEGIN)",
        Statement::Commit { .. } => "transaction control (COMMIT)",
        Statement::Rollback { .. } => "transaction control (ROLLBACK)",
        Statement::Savepoint { .. } | Statement::ReleaseSavepoint { .. } => "SAVEPOINT",
        Statement::Set(_) => "SET",
        Statement::ShowVariable { .. } => "SHOW",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::Explain { .. } | Statement::ExplainTable { .. } => "EXPLAIN",
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
    query_parser: Arc<VaireQueryParser>,
    pg_compat_parser: PostgresCompatibilityParser,
    /// Lowercased bare names of `pg_catalog` tables, used to route unqualified
    /// catalog introspection (e.g. `pg_class`) to `local_ctx`.
    pub(super) catalog_table_names: Arc<std::collections::HashSet<String>>,
}

#[async_trait]
impl SimpleQueryHandler for VaireDbQueryHandler {
    /// Rewrite, parse, and execute every statement in a simple-protocol query
    /// string, returning one response per statement. Returns a `SqlSyntaxError`
    /// if parsing fails. No bind parameters are possible on this path.
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query = parser::rewrite_pg_sql(&self.pg_compat_parser, query);
        let statements = sql_compat::parse_sql(&query)
            .map_err(|e| make_vdb_error(VdbErrorCode::SqlSyntaxError, e.to_string()))?;

        let mut responses = Vec::with_capacity(statements.len());

        for stmt in &statements {
            responses.push(self.execute_one_statement(stmt).await?);
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
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let prepared = &portal.statement.statement;
        let Some(stmt) = &prepared.stmt else {
            return Ok(Response::EmptyQuery);
        };

        if prepared.query_type == QueryType::Select {
            // Read path: bind typed parameters into the cached logical plan.
            let plan = prepared.plan.as_ref().ok_or_else(|| {
                make_vdb_error(VdbErrorCode::InternalError, "missing plan for SELECT")
            })?;
            let param_values = self.decode_param_values(portal)?;
            return self
                .execute_select_plan(prepared, plan, param_values, &portal.result_column_format)
                .await;
        }

        // Write/DDL path: parameters (if any) are bound on DuckDB. Decode them to
        // ScalarValues for shard routing and transport.
        let params = self.decode_param_scalars(portal)?;
        self.execute_write_statement(stmt, &prepared.query_type, &params)
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
        if portal.statement.statement.query_type == QueryType::Select {
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
    fn is_catalog_query(&self, stmt: &sqlparser::ast::Statement) -> bool {
        references_catalog_schema(stmt, &self.catalog_table_names)
    }

    /// Dispatch a statement parsed by the simple-query protocol. No bind
    /// parameters are possible here, so writes run with an empty parameter list.
    async fn execute_one_statement(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> PgWireResult<Response> {
        let query_type = query_router::classify_statement(stmt);
        match query_type {
            QueryType::Select => self.handle_select(stmt).await,
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                self.handle_dml(stmt, &query_type, &[]).await
            }
            QueryType::CreateTable => self.handle_create_table(stmt).await,
            QueryType::DropTable => self.handle_drop_table(stmt).await,
            QueryType::AlterTable => self.handle_alter_table(stmt).await,
            QueryType::Other => Err(unsupported_statement_error(stmt)),
        }
    }

    /// Dispatch a write/DDL statement from the extended protocol, carrying any
    /// decoded bind parameters into the write path.
    async fn execute_write_statement(
        &self,
        stmt: &sqlparser::ast::Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
    ) -> PgWireResult<Response> {
        match query_type {
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                self.handle_dml(stmt, query_type, params).await
            }
            QueryType::CreateTable => self.handle_create_table(stmt).await,
            QueryType::DropTable => self.handle_drop_table(stmt).await,
            QueryType::AlterTable => self.handle_alter_table(stmt).await,
            QueryType::Select | QueryType::Other => Err(unsupported_statement_error(stmt)),
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
            .map_err(|e| enrich_generic_error(&e, &select_ctx))?;

        let ctx = if prepared.is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };
        let df = ctx
            .execute_logical_plan(bound)
            .await
            .map_err(|e| enrich_generic_error(&e, &select_ctx))?;

        encoding::encode_dataframe_response(df, result_format, &select_ctx).await
    }

    /// Execute a simple-protocol SELECT, choosing the local catalog context for
    /// introspection queries and the distributed context otherwise, and encode
    /// the result as text (the only format the simple protocol uses).
    async fn handle_select(&self, stmt: &sqlparser::ast::Statement) -> PgWireResult<Response> {
        // Translate PG TO_CHAR format strings to strftime specifiers so DataFusion's
        // native to_char formats correctly on the read path.
        let mut stmt_read = stmt.clone();
        sql_compat::transform_to_char_format_for_read(&mut stmt_read);

        let is_catalog = self.is_catalog_query(stmt);
        // User schemas are flat qualifiers: collapse `schema.tbl` to the bare
        // registered name. Skip catalog queries so `pg_catalog.*` etc. keep their
        // qualifier for the local catalog context.
        if !is_catalog {
            sql_compat::collapse_schema_qualified_relations(&mut stmt_read);
        }
        let stmt = &stmt_read;
        let sql = sql_compat::statement_to_sql(stmt);

        let ctx = if is_catalog {
            &self.local_ctx
        } else {
            &self.session_ctx
        };

        let select_ctx = query_router::extract_select_table_name(stmt)
            .map(|t| ErrorContext::for_table(&t))
            .unwrap_or_default();

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| enrich_generic_error(&e, &select_ctx))?;

        // The simple query protocol always returns results in text format.
        encoding::encode_dataframe_response(df, &Format::UnifiedText, &select_ctx).await
    }
}
