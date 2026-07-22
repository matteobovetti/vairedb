//! Extended-protocol SQL parsing and statement preparation for the pgwire handler.
//!
//! Runs at the protocol Parse step: each statement is normalized through the
//! PostgreSQL-compatibility rewriter, parsed, and classified for routing.
//! SELECTs are planned against the appropriate DataFusion context so Describe can
//! report true parameter/result OIDs and Execute can bind typed values; writes
//! and DDL are routed to DuckDB and bound there, so no plan is cached for them
//! (except as a best-effort source of inferred parameter types).

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::catalog_routing::references_catalog_schema;
use crate::pgwire_handler::error_enrichment::{ErrorContext, enrich_generic_error, make_vdb_error};
use crate::query_router::{self, QueryType};
use crate::sql_compat;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

/// A parsed extended-protocol statement. For SELECTs we cache the DataFusion
/// `LogicalPlan` (with `$N` placeholders intact) so that Describe can report
/// true parameter/result OIDs and Execute can bind typed values via
/// `replace_params_with_values`. For writes/DDL there is no plan — those are
/// routed to DuckDB, and parameters are bound there as prepared-statement values.
#[derive(Clone)]
pub(crate) struct VairePrepared {
    /// vairedb sqlparser AST, used for routing and shard-local rewriting.
    /// `None` for an empty query.
    pub(super) stmt: Option<sqlparser::ast::Statement>,
    /// Classification driving dispatch (read vs. write vs. DDL).
    pub(super) query_type: QueryType,
    /// True when the statement references a metadata schema, so it runs on
    /// `local_ctx` rather than the distributed `session_ctx`.
    pub(super) is_catalog: bool,
    /// `Some` only for SELECT — the logical plan with placeholders unresolved.
    pub(super) plan: Option<LogicalPlan>,
}

/// Parses incoming SQL once at the protocol Parse step. SELECTs are planned
/// against the appropriate DataFusion context (`local_ctx` for catalog
/// introspection, `session_ctx` for distributed user-data reads); everything
/// else is classified for routing without planning.
pub(crate) struct VaireQueryParser {
    session_ctx: Arc<SessionContext>,
    local_ctx: Arc<SessionContext>,
    pg_compat_parser: PostgresCompatibilityParser,
    catalog_table_names: Arc<HashSet<String>>,
}

impl VaireQueryParser {
    /// Construct the parser, capturing the distributed and local contexts and the
    /// set of `pg_catalog` table names used to route introspection queries.
    pub(super) fn new(
        session_ctx: Arc<SessionContext>,
        local_ctx: Arc<SessionContext>,
        catalog_table_names: Arc<HashSet<String>>,
    ) -> Self {
        Self {
            session_ctx,
            local_ctx,
            pg_compat_parser: PostgresCompatibilityParser::new(),
            catalog_table_names,
        }
    }
}

#[async_trait]
impl QueryParser for VaireQueryParser {
    type Statement = VairePrepared;

    /// Rewrite, parse, and classify an incoming statement, planning SELECTs (and
    /// best-effort planning writes for type inference). Returns an empty
    /// `VairePrepared` for an empty query, a `SqlSyntaxError` on parse failure,
    /// or an enriched error if a SELECT fails to plan.
    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let rendered_sql = rewrite_pg_sql(&self.pg_compat_parser, sql);
        let statements = sql_compat::parse_sql(&rendered_sql)
            .map_err(|e| make_vdb_error(VdbErrorCode::SqlSyntaxError, e.to_string()))?;
        let Some(stmt) = statements.into_iter().next() else {
            return Ok(VairePrepared {
                stmt: None,
                query_type: QueryType::Other,
                is_catalog: false,
                plan: None,
            });
        };

        let query_type = query_router::classify_statement(&stmt);
        let is_catalog = references_catalog_schema(&stmt, &self.catalog_table_names);
        let rendered = sql_compat::statement_to_sql(&stmt);

        let plan = match query_type {
            // The read path executes this plan, so planning must succeed.
            QueryType::Select => {
                let ctx = if is_catalog {
                    &self.local_ctx
                } else {
                    &self.session_ctx
                };
                let select_ctx = query_router::extract_select_table_name(&stmt)
                    .map(|t| ErrorContext::for_table(&t))
                    .unwrap_or_default();
                let plan = ctx
                    .state()
                    .create_logical_plan(&rendered)
                    .await
                    .map_err(|e| enrich_generic_error(&e, &select_ctx))?;
                Some(plan)
            }
            // Writes execute on DuckDB, not via this plan — but DataFusion can
            // still logical-plan them to infer placeholder types from the target
            // columns, which is what Describe reports. Best-effort: if planning
            // fails (e.g. an UPDATE form DataFusion can't plan), fall back to an
            // AST-derived parameter count with UNKNOWN types in get_parameter_types.
            QueryType::Insert | QueryType::Update | QueryType::Delete => self
                .session_ctx
                .state()
                .create_logical_plan(&rendered)
                .await
                .ok(),
            _ => None,
        };

        Ok(VairePrepared {
            stmt: Some(stmt),
            query_type,
            is_catalog,
            plan,
        })
    }

    /// Report parameter OIDs ordered `$1..$N`. Prefer types inferred from the
    /// cached logical plan; when no plan is available (a write DataFusion could
    /// not plan), fall back to the AST placeholder count with `UNKNOWN` types so
    /// the client still sees the correct parameter count and can bind.
    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        if let Some(plan) = &stmt.plan {
            let inferred = plan
                .get_parameter_types()
                .map_err(|e| make_vdb_error(VdbErrorCode::InternalError, e.to_string()))?;
            return ordered_param_types(&inferred)
                .into_iter()
                .map(|dt| match dt {
                    Some(dt) => into_pg_type(dt),
                    None => Ok(Type::UNKNOWN),
                })
                .collect();
        }
        let count = stmt
            .stmt
            .as_ref()
            .map(sql_compat::max_placeholder_index)
            .unwrap_or(0);
        Ok(vec![Type::UNKNOWN; count])
    }

    /// Report the result row schema. Only SELECT statements produce a row set;
    /// writes/DDL report no columns (even though a write may have a cached plan
    /// used solely for parameter-type inference).
    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if stmt.query_type != QueryType::Select {
            return Ok(vec![]);
        }
        let Some(plan) = &stmt.plan else {
            return Ok(vec![]);
        };
        arrow_schema_to_pg_fields(
            plan.schema().as_arrow(),
            column_format.unwrap_or(&Format::UnifiedText),
            None,
        )
    }
}

/// Normalize incoming SQL through the PostgreSQL-compatibility rewriter so that client
/// introspection queries (regclass casts, `::oid`, `ANY(array)`, known driver probe
/// queries, etc.) become executable against DataFusion + the emulated `pg_catalog`.
/// On parse failure or empty output, falls back to the original SQL unchanged.
///
/// The boundary here is intentionally a SQL *string*: the rewriter operates on
/// datafusion's sqlparser version, which differs from the one vairedb uses directly, so we
/// never share AST types across the two. Shared by the simple-protocol handler and the
/// extended-protocol parser, which both rewrite before parsing.
pub(super) fn rewrite_pg_sql(pg_compat_parser: &PostgresCompatibilityParser, sql: &str) -> String {
    match pg_compat_parser.parse(sql) {
        Ok(stmts) if !stmts.is_empty() => stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; "),
        _ => sql.to_string(),
    }
}

/// Order a DataFusion parameter-type map (`{"$1": .., "$2": ..}`) by positional
/// index. DataFusion keys placeholders by name; sorting them lexicographically
/// would misorder `$10` before `$2`, so parse the numeric suffix instead.
pub(super) fn ordered_param_types(
    types: &HashMap<String, Option<datafusion::arrow::datatypes::DataType>>,
) -> Vec<Option<&datafusion::arrow::datatypes::DataType>> {
    let mut entries: Vec<(usize, Option<&datafusion::arrow::datatypes::DataType>)> = types
        .iter()
        .filter_map(|(k, v)| {
            let idx = k.strip_prefix('$')?.parse::<usize>().ok()?;
            Some((idx, v.as_ref()))
        })
        .collect();
    entries.sort_by_key(|(idx, _)| *idx);
    entries.into_iter().map(|(_, v)| v).collect()
}
