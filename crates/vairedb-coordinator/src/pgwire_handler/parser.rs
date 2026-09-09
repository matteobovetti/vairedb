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
use crate::pgwire_handler::anonymized_reads;
use crate::pgwire_handler::catalog_routing::{self, references_catalog_schema};
use crate::pgwire_handler::column_labels;
use crate::pgwire_handler::compat_rewrite;
use crate::pgwire_handler::copy;
use crate::pgwire_handler::encoding;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_datafusion_error, make_vdb_error,
};
use crate::pgwire_handler::introspection;
use crate::pgwire_handler::pg_aggregate_widening;
use crate::pgwire_handler::pg_operators;
use crate::pgwire_handler::pg_param_types;
use crate::pgwire_handler::pg_set_op_types;
use crate::pgwire_handler::pg_using_join_merge;
use crate::pgwire_handler::query_router::{self, QueryType};
use crate::pgwire_handler::session_params;
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
/// `RESET` has one exception: neither parser has the statement at all, so it is
/// recognized here and rewritten to the `SET … TO DEFAULT` PostgreSQL defines it
/// to be — see [`session_params::parse_reset`].
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    if let Some(statements) = session_params::parse_reset(sql) {
        return Ok(statements);
    }

    let statements = pg_parser().parse(sql)?;

    let wants_verbatim = statements
        .iter()
        .any(|stmt| query_router::classify_statement(stmt).wants_verbatim_ast());
    // A `COLLATE` clause is gone from the compat AST — its `StripCollate` rule deletes
    // it — so the read path's refusal has to read the client's own text. The keyword
    // scan is a guard against parsing every statement twice; a match inside a string
    // literal costs one extra parse and no false refusal, since the check that follows
    // is on the AST.
    let mentions_collate = mentions_collate(sql);
    // A projected `NULL` may be a subquery `RemoveSubqueryFromProjection` folded away,
    // in which case the read path takes the statement over — see
    // [`compat_rewrite`]. Also a guard and not a decision: it matches a `NULL` the
    // client wrote too.
    let maybe_folded = statements.iter().any(compat_rewrite::projects_a_bare_null);
    // An `array_contains` over a subquery is what `RewriteArrayAnyAllOperation` leaves
    // behind for `= ANY (SELECT …)` / `<> ALL (SELECT …)`, which no planner can resolve.
    // A guard on the same terms as the two above: it also matches an `array_contains`
    // over a scalar subquery the client wrote, and that statement is left alone once the
    // decision is taken against its own AST.
    let maybe_mangled_any_all = statements
        .iter()
        .any(compat_rewrite::holds_a_mangled_any_all);
    if !wants_verbatim && !mentions_collate && !maybe_folded && !maybe_mangled_any_all {
        let mut statements = statements;
        for stmt in &mut statements {
            compat_rewrite::rewrite_not_in_subqueries(stmt);
        }
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

    let mut prepared = Vec::with_capacity(statements.len());
    for (compat, raw) in statements.into_iter().zip(verbatim) {
        let compat_type = query_router::classify_statement(&compat);
        // Only swap statements both parsers classify identically, so a
        // substituted probe query is never replaced by whatever the raw text
        // happened to be.
        if compat_type.wants_verbatim_ast() {
            // The one point every write statement passes through, and the last one at
            // which the client's own expressions are still visible: from here the AST is
            // rendered back to SQL and run verbatim by a shard's DuckDB, which reads
            // several PostgreSQL expressions differently. What can be rewritten is
            // rewritten later by `transform_to_duckdb`; what cannot is refused now, so
            // that a write whose predicate would mean something else never runs.
            //
            // This is also why the check is here rather than beside the read path's:
            // control leaves the loop in this branch, before `reject_unsupported_collation`
            // — so a write used to be the one place no expression check ran at all.
            if query_router::classify_statement(&raw) == compat_type {
                write_sql_cl::reject_duckdb_divergent(&raw)?;
                prepared.push(raw);
            } else {
                write_sql_cl::reject_duckdb_divergent(&compat)?;
                prepared.push(compat);
            }
            continue;
        }
        if mentions_collate {
            pg_operators::reject_unsupported_collation(&raw)?;
        }
        // `x = ANY (SELECT …)` and `x <> ALL (SELECT …)` are ordinary PostgreSQL that
        // upstream's `RewriteArrayAnyAllOperation` hands to `array_contains` as though
        // the subquery were an array. Normalizing to `IN`/`NOT IN` on the client's own
        // AST both fixes the meaning and puts the expression out of that rule's reach —
        // see [`compat_rewrite::normalize_any_all_subqueries`]. Checked before the
        // subquery take-over below because the same statement can need both, and
        // `rewrite_for_read_path` applies to whatever it is handed.
        if compat_rewrite::mentions_any_all_subquery(&raw) && !catalog_routing::reads_metadata(&raw)
        {
            let mut raw = raw;
            compat_rewrite::normalize_any_all_subqueries(&mut raw);
            push_read(&mut prepared, compat_rewrite::rewrite_for_read_path(raw));
            continue;
        }
        // Where a rule folded the client's own scalar subquery to `NULL`, plan from an
        // AST VaireDB rewrites itself instead. Metadata queries keep the folded one:
        // that fallback is what a driver's introspection relies on, and a NULL is a
        // serviceable answer about the catalog where a plan failure is not.
        if compat_rewrite::upstream_would_null_a_subquery(&raw)
            && !catalog_routing::reads_metadata(&raw)
        {
            push_read(&mut prepared, compat_rewrite::rewrite_for_read_path(raw));
            continue;
        }
        push_read(&mut prepared, compat);
    }
    Ok(prepared)
}

/// Push a read-path statement, first respelling any `NOT IN (subquery)` it carries so a
/// NULL among the candidates means what PostgreSQL says it means — see
/// [`compat_rewrite::rewrite_not_in_subqueries`].
///
/// Every read-path exit of [`parse_sql`] goes through here, including the two take-over
/// branches: `x <> ALL (SELECT …)` *becomes* a `NOT IN` on the way, so normalizing it
/// without this would trade one wrong answer for another. Write statements deliberately
/// do not — their AST is rendered back to SQL and run by a shard's DuckDB, which reads
/// `NOT IN` the way PostgreSQL does.
fn push_read(prepared: &mut Vec<Statement>, mut stmt: Statement) {
    compat_rewrite::rewrite_not_in_subqueries(&mut stmt);
    prepared.push(stmt);
}

/// Whether `sql` uses `COLLATE` as a keyword — cheaply, and erring towards yes.
///
/// A bare lowercase search would miss `COLLATE`, and a case-insensitive one matches
/// `'collated'` inside a string literal too. Both are acceptable in the direction they
/// err: the answer only decides whether [`parse_sql`] parses the text a second time to
/// look at the AST, which is what actually decides the refusal.
fn mentions_collate(sql: &str) -> bool {
    sql.as_bytes()
        .windows(7)
        .any(|w| w.eq_ignore_ascii_case(b"collate"))
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
/// Fallible because two of the steps read the catalog and can refuse the statement:
/// view expansion ([`views::expand_views`]) and the pseudonymized-column check
/// ([`anonymized_reads::reject_meaningless_reads`]).
///
/// Every rewrite here applies to a query, so a statement that is not one comes
/// back unchanged: [`crate::pgwire_handler::introspection`] therefore unwraps the
/// query out of an `EXPLAIN`, prepares it here, and rewraps it, rather than handing
/// the `EXPLAIN` over whole.
pub(super) fn prepare_select_for_planning(
    stmt: &Statement,
    is_catalog: bool,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<Statement> {
    let mut prepared = stmt.clone();
    if is_catalog {
        catalog_routing::reject_catalog_join_to_user_data(&prepared, catalog)?;
    }
    // Views first, so a definition's own body goes through the two rewrites below
    // as if the client had written it out: a view over `myschema.orders` or using
    // `to_char` has to be rewritten too. A catalog query is skipped — a view may
    // not be defined over a metadata schema, so there is nothing to expand, and
    // this keeps the catalog lookups off the path client introspection takes.
    if !is_catalog {
        views::expand_views(&mut prepared, catalog)?;
        // Straight after the expansion, so a view's own body is checked too, and before
        // the rewrites below, so the expressions are still spelled the way the client
        // wrote them — `SIMILAR TO` is a `SIMILAR TO` here and a function call after.
        anonymized_reads::reject_meaningless_reads(&prepared, catalog)?;
    }
    // Translate PG TO_CHAR format strings to strftime specifiers so DataFusion's
    // native to_char formats correctly on the read path.
    transform_to_char_format_for_read(&mut prepared);
    // Label a function's result column the way PostgreSQL does, before the rewrites
    // below rename the function — the label the client gets is then the name the client
    // wrote. Not for a catalog query: those come from upstream's own rewrites, aimed at
    // the column names particular drivers look for.
    if !is_catalog {
        column_labels::label_function_columns(&mut prepared);
    }
    // Rewrite the PostgreSQL operators DataFusion has no node for, and refuse the
    // expressions it would accept while ignoring half of what they ask for.
    pg_operators::rewrite_pg_expressions(&mut prepared)?;
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
        .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;
    // On the plan and not on the session, because this changes a result column's *type*
    // and the type the client is told is read off this plan — by Describe and by the row
    // encoder both. See `pg_aggregate_widening`.
    let plan = pg_aggregate_widening::widen_bigint_aggregates(plan)
        .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;
    // On the plan and **after** the planner, because the merged column PostgreSQL's
    // `USING` promises only exists once names are resolved and wildcards expanded: it is
    // the columns the planner has already picked that this puts the merged value into.
    // See `pg_using_join_merge`.
    let plan = pg_using_join_merge::merge_using_join_keys(plan)
        .map_err(|e| enrich_datafusion_error(&e, &select_ctx))?;
    // Also on the plan, and for the same reason: an untyped `$N` is decoded using the type
    // this plan reports, so a type the plan does not carry yet is one the client's value
    // never gets. See `pg_param_types`.
    let plan = pg_param_types::resolve_placeholder_types(plan);
    // Before `coerce_types`, and only before it: coercion is the pass that inserts the casts
    // making a set operation's branches agree, so afterwards there is no disagreement left to
    // refuse. See `pg_set_op_types`.
    pg_set_op_types::reject_incompatible_set_operation_types(&plan)?;
    let plan = coerce_types(ctx, plan, &select_ctx)?;
    Ok((plan, select_ctx))
}

/// Run DataFusion's analyzer, so the plan the rest of the read path holds carries the
/// types the query will actually produce.
///
/// It has to happen here because **the type a client is told is read off this one plan**
/// — by Describe (`get_result_schema`) and by the row encoder
/// ([`super::encoding::encode_dataframe_response`], through `df.schema()`) — while
/// `execute_logical_plan` analyzes a *copy* on its way to a physical plan and never
/// reports back. Where the two disagree, the encoder's cast into the advertised type is
/// what the client sees.
///
/// A set operation is where they disagree. DataFusion's SQL planner gives `UNION` the
/// schema of its **leading branch**, and only `TypeCoercion` widens it to the common
/// type both branches are cast to. So `SELECT int4_col … UNION ALL SELECT float8_col …`
/// used to be advertised as `int4`, and the `float8` rows the query really produced were
/// then cast back down to it: `2.5` was answered as `2`, and an `int8` past `int4`'s
/// range as an out-of-range *error*. Reversing the two branches answered correctly,
/// which is not a property PostgreSQL has — `UNION` there resolves one common type
/// regardless of the order the branches are written in.
///
/// Ordering inside this function's caller matters and is not incidental:
/// `widen_bigint_aggregates` must see the plan **before** coercion (it resolves the
/// PostgreSQL overload from the argument type coercion would have already erased), and
/// placeholders are typed before it too, since coercion needs a type for every `$N` it
/// meets.
fn coerce_types(
    ctx: &SessionContext,
    plan: LogicalPlan,
    select_ctx: &ErrorContext,
) -> PgWireResult<LogicalPlan> {
    let state = ctx.state();
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
        .map_err(|e| enrich_datafusion_error(&e, select_ctx))
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
        let statements =
            self::parse_sql(sql).map_err(|e| make_vdb_error(e.vdb_error_code(), e.to_string()))?;
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
            // An `EXPLAIN` is planned here too, and for the same reason: the plan
            // *is* the answer, and Describe has to report the columns it will
            // produce before Execute runs. Which context matters as much as it does
            // for a SELECT — the plan a client is shown must be the one the read
            // path would build.
            QueryType::Explain => {
                let ctx = if is_catalog {
                    &self.local_ctx
                } else {
                    &self.session_ctx
                };
                let (plan, _) =
                    introspection::plan_introspection(ctx, &stmt, is_catalog, &self.catalog)
                        .await?;
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
            // A `COPY ... FROM STDIN` is the one statement whose refusal has to
            // happen at Parse rather than Execute — see
            // [`copy::precheck_copy_from_stdin`] for what a driver does to the
            // connection when it happens later. There is no plan either way: a copy
            // executes through the INSERT lane, not through DataFusion.
            QueryType::Copy => {
                copy::precheck_copy_from_stdin(&stmt, &self.catalog)?;
                None
            }
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

    /// Report the result row schema. SELECT and `SHOW` produce a row set;
    /// writes/DDL report no columns (even though a write may have a cached plan
    /// used solely for parameter-type inference).
    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let format = column_format.unwrap_or(&Format::UnifiedText);

        if stmt.query_type == QueryType::SessionParam {
            // Built directly rather than from a plan: a `SHOW`'s columns come from
            // the parameter registry, and nothing planned it. Execute derives its
            // fields from the same function, so the RowDescription promised here is
            // the one the DataRows answer.
            let Some(inner) = &stmt.stmt else {
                return Ok(vec![]);
            };
            return session_params::result_fields(inner, format);
        }

        if !matches!(stmt.query_type, QueryType::Select | QueryType::Explain) {
            return Ok(vec![]);
        }
        let Some(plan) = &stmt.plan else {
            return Ok(vec![]);
        };
        if stmt.query_type == QueryType::Explain {
            // An `EXPLAIN`'s columns are not its plan's: the plan node's schema is
            // DataFusion's `plan_type`/`plan` pair, and what goes on the wire is
            // PostgreSQL's single `QUERY PLAN` column. Execute reshapes through the
            // same function, so the two agree.
            return introspection::result_fields(plan, format);
        }
        // Through `wire_schema`, because Describe has to promise the type Execute
        // will actually send: the encoder widens a `UInt64` column to `bigint`, and a
        // client told `numeric` here would decode the following DataRow wrongly.
        arrow_schema_to_pg_fields(
            &encoding::wire_schema(plan.schema().as_arrow()),
            format,
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

#[cfg(test)]
mod tests {
    /// The whole-path assertion for the ANY/ALL normalization: `parse_sql` returns the
    /// statement early unless one of its cheap guards fires, so a fix inside
    /// [`compat_rewrite`] is only reachable if the guard is wired in. This test is on
    /// `parse_sql` rather than on the rewrite for exactly that reason.
    #[test]
    fn parse_sql_normalizes_an_any_all_subquery() {
        for (sql, expected) in [
            (
                "SELECT id FROM orders WHERE id = ANY (SELECT oid FROM lines)",
                "id IN (SELECT oid FROM lines)",
            ),
            // `<> ALL` normalizes to `NOT IN`, which [`push_read`] then respells as the
            // null-aware anti join — so the two fixes compose, in that order, and this is
            // where that is asserted.
            (
                "SELECT id FROM orders WHERE id <> ALL (SELECT oid FROM lines)",
                "NOT EXISTS (SELECT 1 FROM (SELECT oid FROM lines) AS vaire_notin_0",
            ),
        ] {
            let parsed = super::parse_sql(sql)
                .expect("the statement parses")
                .remove(0);
            let parsed = parsed.to_string();

            assert!(
                parsed.contains(expected),
                "`{sql}` was not normalized: {parsed}"
            );
            assert!(
                !parsed.contains("array_contains"),
                "`{sql}` still reaches array_contains: {parsed}"
            );
        }
    }

    /// The array form goes the other way through the same function: it must still come
    /// back as the `array_contains` call DataFusion resolves, since that rewrite is what
    /// makes a driver's "id in list" parameter binding work.
    #[test]
    fn parse_sql_keeps_the_array_form_as_array_contains() {
        let parsed = super::parse_sql("SELECT id FROM orders WHERE id = ANY(ARRAY[1, 2])")
            .expect("the statement parses")
            .remove(0)
            .to_string();

        assert!(
            parsed.contains("array_contains(ARRAY[1, 2], id)"),
            "{parsed}"
        );
    }
}
