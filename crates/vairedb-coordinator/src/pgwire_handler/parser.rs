//! SQL parsing for the pgwire handler: the coordinator's single parse, the
//! read-path AST rewrites, and extended-protocol statement preparation.
//!
//! Everything a client sends enters through the wire protocol, so the parse lives
//! here rather than in either execution path: [`parse_sql`] is called once per
//! statement by the simple-query handler and by the extended-protocol `Parse`
//! step, and both the read path (DataFusion) and the write path
//! ([`crate::write_sql_cl`]) consume the AST it produces. The two paths need
//! different ASTs of the same text — the pg-compatibility rewrites serve
//! DataFusion's planner and would corrupt a write — so [`parse_sql`] returns
//! rewritten statements to the read path and verbatim ones to the write path.
//!
//! The rewrites here are read-path only — they prepare a statement for
//! DataFusion's planner, which both protocols reach through [`plan_select`].
//! The write path's PG→DuckDB rewrites live in [`crate::write_sql_cl`];
//! [`translate_format_arg`] is the one piece both paths share.
//!
//! At the protocol Parse step each statement is parsed *once* and classified for
//! routing. SELECTs are planned from that AST against the appropriate DataFusion
//! context so Describe can report true parameter/result OIDs and Execute can bind
//! typed values; writes and DDL are routed to DuckDB and bound there, so no plan
//! is cached for them (except as a best-effort source of inferred parameter types).

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use arrow_pg::datatypes::{arrow_schema_to_pg_fields, into_pg_type};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::MetadataCatalog;
use crate::error::Result;
use crate::pgwire_handler::catalog_routing::references_catalog_schema;
use crate::pgwire_handler::error_enrichment::{ErrorContext, enrich_generic_error, make_vdb_error};
use crate::pgwire_handler::query_router::{self, QueryType};
use crate::pgwire_handler::views;
use crate::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectNamePart, Statement, Value,
    visit_expressions_mut, visit_relations_mut,
};
use crate::write_sql_cl;

/// The process-wide PostgreSQL-compatibility parser backing [`parse_sql`].
///
/// `PostgresCompatibilityParser::new` tokenizes its whole blacklist table, so it is
/// built once and shared; the type is stateless and `Send + Sync`.
fn pg_parser() -> &'static PostgresCompatibilityParser {
    static PARSER: OnceLock<PostgresCompatibilityParser> = OnceLock::new();
    PARSER.get_or_init(PostgresCompatibilityParser::new)
}

/// Parse a SQL string into statements — the coordinator's single entry to a
/// parser, for both paths.
///
/// Read-path statements come back from `datafusion_pg_catalog`'s PostgreSQL-
/// compatibility parser, which tokenizes with sqlparser's `PostgreSqlDialect`,
/// substitutes known driver probe queries, and applies the pg-compat rewrite
/// rules (regclass casts, `::oid`, `ANY(array)`, unqualified `pg_catalog` names,
/// …) so client introspection queries become executable against DataFusion plus
/// the emulated `pg_catalog`.
///
/// Write-path statements come back **verbatim** instead. Those rewrites exist to
/// make a statement planable by DataFusion against an emulated catalog; a write
/// is never planned, it is rendered back to SQL and shipped to DuckDB, so a
/// rewrite there does not adapt the statement — it changes the data written.
/// `INSERT INTO t VALUES ('users'::regclass)` would store a rewritten expression
/// rather than the value the client sent. A write statement is therefore
/// re-parsed with the plain PostgreSQL dialect and that AST is used in its place.
///
/// The resulting AST is `datafusion::sql::sqlparser`'s own `Statement` either
/// way, so it is handed straight to DataFusion's planner on the read path and
/// rendered back to text only on the write path.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let statements = pg_parser().parse(sql)?;

    if !statements
        .iter()
        .any(|stmt| query_router::classify_statement(stmt).wants_verbatim_ast())
    {
        return Ok(statements);
    }

    // Fall back to the pg-compat AST if the verbatim parse disagrees about the
    // statement batch: the rewrites are the lesser risk against an AST we cannot
    // line up positionally. In practice this cannot trigger — the compat parser
    // tokenizes with the same dialect — so the guard is belt-and-braces.
    let Ok(verbatim) = parse_verbatim(sql) else {
        return Ok(statements);
    };
    if verbatim.len() != statements.len() {
        return Ok(statements);
    }

    Ok(statements
        .into_iter()
        .zip(verbatim)
        .map(|(compat, raw)| {
            let compat_type = query_router::classify_statement(&compat);
            // Only swap statements both parsers classify identically, so a
            // substituted probe query is never replaced by whatever the raw text
            // happened to be.
            if compat_type.wants_verbatim_ast()
                && query_router::classify_statement(&raw) == compat_type
            {
                raw
            } else {
                compat
            }
        })
        .collect())
}

/// Parse `sql` with sqlparser's plain `PostgreSqlDialect` — no pg-compat probe
/// substitution and no rewrite rules — so the AST reflects exactly what the
/// client sent. Used for write-path statements by [`parse_sql`].
fn parse_verbatim(sql: &str) -> Result<Vec<Statement>> {
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    Ok(Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)?
        .parse_statements()?)
}

/// Rewrite the format string of every `to_char` call in a SELECT (read path) from
/// PostgreSQL template patterns to strftime `%`-specifiers, keeping the function
/// name and argument order. DataFusion executes read-path projections with its
/// native `to_char`, which formats via chrono/strftime specifiers, so only the
/// format literal needs translating.
pub fn transform_to_char_format_for_read(stmt: &mut Statement) {
    let _ = visit_expressions_mut(stmt, |expr| {
        if let Expr::Function(func) = expr
            && func.name.to_string().eq_ignore_ascii_case("to_char")
            && let FunctionArguments::List(ref mut arg_list) = func.args
            && arg_list.args.len() == 2
        {
            // PG/DataFusion `to_char(value, format)`: the format is at position 1.
            translate_format_arg(&mut arg_list.args[1]);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Collapse every schema-qualified relation in `stmt` to the single quoted
/// identifier holding its canonical catalog key, so a `SELECT ... FROM sales.orders`
/// resolves against the name the table provider is registered under.
///
/// A schema is a namespace in the *coordinator's* catalog, not a DataFusion one:
/// providers are registered under a bare `TableReference` whose name is the catalog
/// key (`orders`, `sales.orders`), so a `Partial{schema, table}` reference would not
/// resolve. Quoting the collapsed name keeps DataFusion from folding it again, which
/// matters for a table created with a quoted mixed-case name.
///
/// Only multi-part relations are touched; a single-part name is already its own key
/// and is left byte-identical, so an unquoted one still folds the way PostgreSQL
/// folds it. Apply on the read path only for non-catalog queries, so
/// `vairedb_catalog.*` / `pg_catalog.*` references keep their qualifier — those
/// *are* real schemas in the local context.
pub fn collapse_schema_qualified_relations(stmt: &mut Statement) {
    let _ = visit_relations_mut(stmt, |relation| {
        if relation.0.len() > 1
            && let Some(key) = query_router::canonical_table_name(relation)
        {
            relation.0 = vec![ObjectNamePart::Identifier(Ident::with_quote('"', key))];
        }
        ControlFlow::<()>::Continue(())
    });
}

/// If `arg` is a single-quoted string literal, translate it in place from a
/// PostgreSQL datetime template to strftime `%`-specifiers.
///
/// Shared by both paths: the read path applies it to the format argument of a
/// `to_char` call it leaves in place, the write path to the format argument it has
/// just swapped into DuckDB's `STRFTIME(format, value)` order.
pub(crate) fn translate_format_arg(arg: &mut FunctionArg) {
    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))) = arg
        && let Value::SingleQuotedString(fmt) = &v.value
    {
        v.value = Value::SingleQuotedString(translate_pg_datetime_format(fmt));
    }
}

/// Translate a PostgreSQL `TO_CHAR` datetime template into strftime `%`-specifiers.
/// chrono (DataFusion's `to_char`) and DuckDB's `strftime` share this specifier
/// syntax, so the same output serves both the read and write paths.
///
/// Recognized patterns are matched longest-first; a leading `FM` fill-mode prefix
/// on a token is dropped (no specifier equivalent); unrecognized characters pass
/// through literally, and a literal `%` is escaped to `%%`.
fn translate_pg_datetime_format(pg: &str) -> String {
    // (PG pattern, strftime specifier), ordered longest-first within each prefix
    // group so greedy matching picks e.g. YYYY over YY and HH24 over HH.
    const PATTERNS: &[(&str, &str)] = &[
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MONTH", "%B"),
        ("Month", "%B"),
        ("month", "%B"),
        ("MON", "%b"),
        ("Mon", "%b"),
        ("mon", "%b"),
        ("MM", "%m"),
        ("MI", "%M"),
        ("DDD", "%j"),
        ("DD", "%d"),
        ("DAY", "%A"),
        ("Day", "%A"),
        ("day", "%A"),
        ("DY", "%a"),
        ("Dy", "%a"),
        ("dy", "%a"),
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),
        ("SS", "%S"),
        ("AM", "%p"),
        ("PM", "%p"),
        ("am", "%p"),
        ("pm", "%p"),
        ("TZ", "%Z"),
    ];

    let bytes = pg.as_bytes();
    let mut out = String::with_capacity(pg.len() + 8);
    let mut i = 0;
    while i < bytes.len() {
        let rest = &pg[i..];
        // Drop a fill-mode prefix; it has no strftime equivalent.
        if rest.starts_with("FM") {
            i += 2;
            continue;
        }
        if let Some((pat, spec)) = PATTERNS.iter().find(|(pat, _)| rest.starts_with(pat)) {
            out.push_str(spec);
            i += pat.len();
            continue;
        }
        if bytes[i] == b'%' {
            out.push_str("%%");
            i += 1;
            continue;
        }
        let ch = rest.chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Apply the read-path rewrites to a copy of `stmt`, readying it for DataFusion's
/// planner. `is_catalog` comes from [`references_catalog_schema`]: catalog queries
/// keep their `pg_catalog.*` / `vairedb_catalog.*` qualifiers because those *are*
/// real schemas in the local context.
///
/// Fallible because view expansion reads the catalog and can refuse the statement
/// — see [`views::expand_views`].
fn prepare_select_for_planning(
    stmt: &Statement,
    is_catalog: bool,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<Statement> {
    let mut prepared = stmt.clone();
    // Views first, so a definition's own body goes through the two rewrites below
    // as if the client had written it out: a view over `myschema.orders` or using
    // `to_char` has to be rewritten too. A catalog query is skipped — a view may
    // not be defined over a metadata schema, so there is nothing to expand, and
    // this keeps the catalog lookups off the path client introspection takes.
    if !is_catalog {
        views::expand_views(&mut prepared, catalog)?;
    }
    // Translate PG TO_CHAR format strings to strftime specifiers so DataFusion's
    // native to_char formats correctly on the read path.
    transform_to_char_format_for_read(&mut prepared);
    // A schema is a coordinator-catalog namespace, not a DataFusion one: collapse
    // `schema.tbl` to the single registered name that is its catalog key.
    if !is_catalog {
        collapse_schema_qualified_relations(&mut prepared);
    }
    Ok(prepared)
}

/// Rewrite and logical-plan a SELECT against `ctx`, the single read-path entry to
/// DataFusion's planner shared by both protocols. Returns the plan alongside the
/// [`ErrorContext`] derived from the statement, which the caller reuses to enrich
/// errors raised while executing the plan.
///
/// Plans the AST directly rather than rendering it back to SQL for DataFusion to
/// re-parse: `Display` is lossy, and the re-parse used DataFusion's default
/// (generic) dialect, narrowing the reachable expression surface to the
/// intersection of two dialects.
pub(super) async fn plan_select(
    ctx: &SessionContext,
    stmt: &Statement,
    is_catalog: bool,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<(LogicalPlan, ErrorContext)> {
    let prepared = prepare_select_for_planning(stmt, is_catalog, catalog)?;
    let select_ctx = query_router::extract_select_table_name(&prepared)
        .map(|t| ErrorContext::for_table(&t))
        .unwrap_or_default();
    let plan = ctx
        .state()
        .statement_to_plan(DFStatement::Statement(Box::new(prepared)))
        .await
        .map_err(|e| enrich_generic_error(&e, &select_ctx))?;
    Ok((plan, select_ctx))
}

/// A parsed extended-protocol statement. For SELECTs we cache the DataFusion
/// `LogicalPlan` (with `$N` placeholders intact) so that Describe can report
/// true parameter/result OIDs and Execute can bind typed values via
/// `replace_params_with_values`. For writes/DDL there is no plan — those are
/// routed to DuckDB, and parameters are bound there as prepared-statement values.
#[derive(Clone)]
pub(crate) struct VairePrepared {
    /// The parsed AST, used for routing and shard-local rewriting.
    /// `None` for an empty query.
    pub(super) stmt: Option<Statement>,
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
    catalog_table_names: Arc<HashSet<String>>,
    /// The metadata catalog, needed because planning a SELECT means resolving the
    /// views it reads — see [`plan_select`].
    catalog: Arc<MetadataCatalog>,
}

impl VaireQueryParser {
    /// Construct the parser, capturing the distributed and local contexts, the
    /// set of `pg_catalog` table names used to route introspection queries, and the
    /// metadata catalog that holds the view definitions.
    pub(super) fn new(
        session_ctx: Arc<SessionContext>,
        local_ctx: Arc<SessionContext>,
        catalog_table_names: Arc<HashSet<String>>,
        catalog: Arc<MetadataCatalog>,
    ) -> Self {
        Self {
            session_ctx,
            local_ctx,
            catalog_table_names,
            catalog,
        }
    }
}

#[async_trait]
impl QueryParser for VaireQueryParser {
    type Statement = VairePrepared;

    /// Parse and classify an incoming statement, planning SELECTs (and
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
        // `self::` picks the module-level parser, not this trait method of the
        // same name.
        let statements = self::parse_sql(sql)
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

        let plan = match query_type {
            // The read path executes this plan, so planning must succeed.
            QueryType::Select => {
                let ctx = if is_catalog {
                    &self.local_ctx
                } else {
                    &self.session_ctx
                };
                let (plan, _) = plan_select(ctx, &stmt, is_catalog, &self.catalog).await?;
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
                .statement_to_plan(DFStatement::Statement(Box::new(stmt.clone())))
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
            .map(write_sql_cl::max_placeholder_index)
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
