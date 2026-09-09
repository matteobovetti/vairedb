//! `EXPLAIN`, `EXPLAIN ANALYZE` and `DESCRIBE`: the statements that ask about a
//! query instead of asking for its rows.
//!
//! These reach no shard router and change nothing, but they are not free either:
//! what they must report is *what VaireDB would actually do*, and the coordinator
//! is the only place that knows. A SELECT already builds a DataFusion
//! `LogicalPlan` on the way to Ballista ([`crate::pgwire_handler::parser::plan_select`]),
//! so an `EXPLAIN` is that same plan rendered rather than executed — which is why
//! the query inside an `EXPLAIN` goes through the *same* read-path preparation
//! (view expansion, schema collapsing, `to_char` translation) as if the client had
//! sent it on its own. Explaining the unprepared statement would print a plan for
//! a query VaireDB never runs.
//!
//! Three things follow from that, and they are the whole of this module:
//!
//! * **The plan is rendered locally, the ANALYZE is not.** A non-`ANALYZE`
//!   `EXPLAIN` executes nothing, so it is planned with DataFusion's own physical
//!   planner instead of the Ballista one the read path installs — Ballista would
//!   ship the `Explain` node itself to the scheduler as if it were a query.
//!   `EXPLAIN ANALYZE` is the opposite: it *does* run the query, and only the
//!   distributed execution can report per-stage metrics, so it goes through the
//!   context's normal path.
//! * **PostgreSQL's shape, not DataFusion's.** PostgreSQL returns one `text`
//!   column named `QUERY PLAN`, one row per line; DataFusion returns
//!   `plan_type`/`plan` pairs. The pairs are flattened into the PostgreSQL shape,
//!   because a client's plan display keys on that column.
//! * **A form that cannot be answered truthfully is refused by name.** `EXPLAIN`
//!   of a write would print a DataFusion plan for a statement the write path
//!   executes on DuckDB instead — a plausible plan for something that never runs.
//!   `FORMAT JSON` and the other PostgreSQL utility options would change the
//!   output a client parses. Both are refused rather than silently ignored.

use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::collect;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::sql::parser::Statement as DFStatement;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldInfo, Response};
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::MetadataCatalog;
use crate::pgwire_handler::encoding;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_datafusion_error, make_vdb_error,
};
use crate::pgwire_handler::{parser, query_router};
use crate::sqlparser::ast::{DescribeAlias, Expr, Statement, UtilityOption, Value, ValueWithSpan};

/// The single column PostgreSQL's `EXPLAIN` returns. `text`, because the Arrow
/// `Utf8` this is built as maps to `text` through arrow-pg — the same mapping a
/// SELECT's columns go through.
const QUERY_PLAN: &str = "QUERY PLAN";

/// What the client asked to inspect, once the statement has been checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Inspection {
    /// `EXPLAIN [ANALYZE] [VERBOSE] <query>` — a plan, rendered as PostgreSQL's
    /// `QUERY PLAN` column. `analyze` decides whether the query runs.
    Plan { analyze: bool, verbose: bool },
    /// `DESCRIBE <relation>` / `DESCRIBE <query>` — one row per result column,
    /// with its name, type and nullability.
    Shape,
}

/// The query an `EXPLAIN` is about, or `None` when the statement is not one or
/// names a relation rather than a query.
///
/// Used by the transaction rules: an `EXPLAIN` is a read of whatever its inner
/// query reads, so it is subject to the same buffered-write rule a bare SELECT is.
/// A `DESCRIBE <relation>` reads only the shape, which a block cannot have changed
/// (DDL is refused inside one), so it has no inner query to check.
pub(super) fn explained_query(stmt: &Statement) -> Option<&Statement> {
    match stmt {
        Statement::Explain { statement, .. } if matches!(**statement, Statement::Query(_)) => {
            Some(statement)
        }
        _ => None,
    }
}

/// The relation an `EXPLAIN`/`DESCRIBE` is about, for error enrichment.
///
/// Derived from the client's own statement rather than the prepared one, because
/// the extended protocol plans at Parse and executes at Bind: the two steps are
/// far apart, and the statement is the only thing both hold. Both spellings of a
/// relation canonicalize to the same catalog key, so it makes no difference which
/// one is read.
pub(super) fn error_context(stmt: &Statement) -> ErrorContext {
    let table = match stmt {
        Statement::ExplainTable { table_name, .. } => {
            query_router::canonical_table_name(table_name)
        }
        _ => explained_query(stmt).and_then(query_router::extract_select_table_name),
    };
    table
        .map(|t| ErrorContext::for_table(&t))
        .unwrap_or_default()
}

/// Classify an `EXPLAIN`/`DESCRIBE` statement, refusing the forms VaireDB cannot
/// answer truthfully. Every refusal names what was refused.
fn inspection(stmt: &Statement) -> PgWireResult<Inspection> {
    match stmt {
        // `DESCRIBE <relation>`. sqlparser also parses `EXPLAIN <relation>` into
        // this node; neither PostgreSQL nor DuckDB has that form, so it is refused
        // rather than silently treated as a DESCRIBE.
        Statement::ExplainTable {
            describe_alias,
            hive_format,
            ..
        } => {
            if matches!(describe_alias, DescribeAlias::Explain) {
                return Err(unsupported(
                    "EXPLAIN of a relation is not supported by VaireDB; use DESCRIBE <relation> for its columns, or EXPLAIN <query> for a plan",
                ));
            }
            if hive_format.is_some() {
                return Err(unsupported(
                    "DESCRIBE FORMATTED / EXTENDED is not supported by VaireDB; plain DESCRIBE reports each column's name, type and nullability",
                ));
            }
            Ok(Inspection::Shape)
        }
        Statement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            // Dialect-specific plan modes that would each mean a different output
            // than the one built here.
            if *query_plan {
                return Err(unsupported(
                    "EXPLAIN QUERY PLAN is not supported by VaireDB; use EXPLAIN <query>",
                ));
            }
            if *estimate {
                return Err(unsupported(
                    "EXPLAIN ESTIMATE is not supported by VaireDB; use EXPLAIN <query>",
                ));
            }
            if format.is_some() {
                return Err(unsupported(
                    "EXPLAIN ... FORMAT is not supported by VaireDB; the plan is returned as text in the QUERY PLAN column",
                ));
            }
            // `EXPLAIN (analyze, verbose)` is the PostgreSQL spelling of the two
            // keywords, and the one psql users and tooling write. Every other
            // utility option changes what is reported, so it is refused by name
            // instead of accepted and dropped.
            let (mut analyze, mut verbose) = (*analyze, *verbose);
            for option in options.iter().flatten() {
                match option.name.value.to_ascii_uppercase().as_str() {
                    "ANALYZE" => analyze = option_flag(option)?,
                    "VERBOSE" => verbose = option_flag(option)?,
                    other => {
                        return Err(unsupported(format!(
                            "EXPLAIN option {other} is not supported by VaireDB; only ANALYZE and VERBOSE are"
                        )));
                    }
                }
            }

            // `DESCRIBE <query>` shares this node with `EXPLAIN <query>`, keyed on
            // the alias the client used.
            if matches!(
                describe_alias,
                DescribeAlias::Describe | DescribeAlias::Desc
            ) {
                return Ok(Inspection::Shape);
            }

            // A write is not planned by DataFusion at all — it is rendered back to
            // SQL and shipped to DuckDB — so a DataFusion plan for one would
            // describe execution that never happens.
            if !matches!(**statement, Statement::Query(_)) {
                return Err(unsupported(format!(
                    "EXPLAIN of {} is not supported by VaireDB: only a read is planned in the coordinator — a write is routed to the shards and planned there, so there is no plan here to show. EXPLAIN a SELECT instead",
                    explained_statement_label(statement)
                )));
            }

            Ok(Inspection::Plan { analyze, verbose })
        }
        // Unreachable: the classifier routes exactly the two statements above here.
        _ => Err(make_vdb_error(
            VdbErrorCode::InternalError,
            "unhandled introspection statement",
        )),
    }
}

/// Read a PostgreSQL utility option's boolean argument. A bare option means true,
/// as in `EXPLAIN (ANALYZE) …`; anything that is not a boolean is refused rather
/// than coerced, so `EXPLAIN (ANALYZE maybe)` cannot quietly become `ANALYZE`.
fn option_flag(option: &UtilityOption) -> PgWireResult<bool> {
    let Some(arg) = &option.arg else {
        return Ok(true);
    };
    let word = match arg {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => return Ok(*b),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ..
        }) => s.clone(),
        other => other.to_string(),
    };
    match word.to_ascii_lowercase().as_str() {
        "true" | "on" | "1" | "yes" => Ok(true),
        "false" | "off" | "0" | "no" => Ok(false),
        _ => Err(make_vdb_error(
            VdbErrorCode::InvalidParameterValue,
            format!(
                "EXPLAIN option {} takes a boolean, got `{word}`",
                option.name.value
            ),
        )),
    }
}

/// Name the command inside an `EXPLAIN` so the refusal says which one it was.
fn explained_statement_label(stmt: &Statement) -> &'static str {
    match query_router::classify_statement(stmt) {
        query_router::QueryType::Insert => "an INSERT",
        query_router::QueryType::Update => "an UPDATE",
        query_router::QueryType::Delete => "a DELETE",
        query_router::QueryType::Merge => "a MERGE",
        query_router::QueryType::Copy => "a COPY",
        _ => "this statement",
    }
}

/// `0A000`, the classification refusal every unsupported form here reports.
fn unsupported(message: impl Into<String>) -> PgWireError {
    make_vdb_error(VdbErrorCode::FeatureNotSupported, message)
}

/// Plan an `EXPLAIN`/`DESCRIBE` against `ctx`, after putting the query it is about
/// through the read path's own preparation.
///
/// The inner query is unwrapped, prepared and rewrapped rather than prepared in
/// place: [`parser::prepare_select_for_planning`] expands views and collapses
/// schema-qualified relations on a *query*, and returns early for anything else,
/// so an `EXPLAIN` handed to it whole would come back untouched — and print a plan
/// for a query with the views not expanded and the relations unresolved.
///
/// `DESCRIBE <relation>` is rewritten to `DESCRIBE SELECT * FROM <relation>`,
/// which is the same plan by construction (DataFusion derives both from the
/// resolved schema) and reaches a view for free: a view is not a registered
/// relation, it is a definition the preparation inlines.
pub(super) async fn plan_introspection(
    ctx: &SessionContext,
    stmt: &Statement,
    is_catalog: bool,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<(LogicalPlan, ErrorContext)> {
    // Checked before anything is rewritten, so a refused form is refused by name
    // rather than by whatever the planner makes of it.
    let inspection = inspection(stmt)?;
    let prepared = prepare(stmt, &inspection, is_catalog, catalog)?;

    let err_ctx = error_context(stmt);

    let plan = ctx
        .state()
        .statement_to_plan(DFStatement::Statement(Box::new(prepared)))
        .await
        .map_err(|e| enrich_datafusion_error(&e, &err_ctx))?;

    Ok((plan, err_ctx))
}

/// Rebuild the statement with its inner query prepared for the planner, and with
/// the accepted `ANALYZE`/`VERBOSE` folded into the fields DataFusion reads (it
/// looks at the keyword flags, not at the PostgreSQL option list).
fn prepare(
    stmt: &Statement,
    inspection: &Inspection,
    is_catalog: bool,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<Statement> {
    let (analyze, verbose) = match inspection {
        Inspection::Plan { analyze, verbose } => (*analyze, *verbose),
        Inspection::Shape => (false, false),
    };

    let inner = match stmt {
        Statement::Explain { statement, .. } => (**statement).clone(),
        Statement::ExplainTable { table_name, .. } => select_star_from(table_name.to_string())?,
        // Unreachable: `inspection` already refused anything else.
        _ => {
            return Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "unhandled introspection statement",
            ));
        }
    };

    let describe_alias = match inspection {
        Inspection::Plan { .. } => DescribeAlias::Explain,
        Inspection::Shape => DescribeAlias::Describe,
    };

    Ok(Statement::Explain {
        describe_alias,
        analyze,
        verbose,
        query_plan: false,
        estimate: false,
        statement: Box::new(parser::prepare_select_for_planning(
            &inner, is_catalog, catalog,
        )?),
        format: None,
        options: None,
    })
}

/// Parse `SELECT * FROM <relation>` so a `DESCRIBE <relation>` becomes a
/// `DESCRIBE <query>`.
///
/// Built from text rather than as an AST literal on purpose: `Select` carries
/// twenty-odd fields that a sqlparser upgrade can add to, and the relation name
/// has to be re-quoted exactly as the client wrote it — which is what
/// `ObjectName`'s own `Display` does.
fn select_star_from(relation: String) -> PgWireResult<Statement> {
    let sql = format!("SELECT * FROM {relation}");
    parser::parse_sql(&sql)
        .map_err(|e| make_vdb_error(VdbErrorCode::SqlSyntaxError, e.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::InternalError,
                "failed to build the query for DESCRIBE",
            )
        })
}

/// Run a planned `EXPLAIN`/`DESCRIBE` and encode its rows.
///
/// `EXPLAIN ANALYZE` executes the query, so it goes through `ctx` and gets the
/// distributed execution (and, on the Ballista context, its per-stage metrics).
/// Everything else executes nothing of the query and is planned with DataFusion's
/// own physical planner: the Ballista planner would hand the `Explain` node to the
/// scheduler as though it were the query to distribute.
pub(super) async fn execute_introspection(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    format: &Format,
    err_ctx: &ErrorContext,
) -> PgWireResult<Response> {
    if matches!(plan, LogicalPlan::Analyze(_)) {
        let df = ctx
            .execute_logical_plan(plan.clone())
            .await
            .map_err(|e| enrich_datafusion_error(&e, err_ctx))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| enrich_datafusion_error(&e, err_ctx))?;
        let (schema, rows) = query_plan_batch(&batches)?;
        return encoding::encode_batches_response(&schema, &rows, format);
    }

    let (schema, batches) = collect_locally(ctx, plan, err_ctx).await?;

    if matches!(plan, LogicalPlan::Explain(_)) {
        let (schema, rows) = query_plan_batch(&batches)?;
        return encoding::encode_batches_response(&schema, &rows, format);
    }
    encoding::encode_batches_response(&schema, &batches, format)
}

/// Plan and run `plan` in this process, with DataFusion's own physical planner.
///
/// Two steps rather than `SessionState::create_physical_plan`, which would reach
/// the context's own (Ballista) query planner: `optimize` first, because that is
/// where an `Explain` collects the logical stages it reports, then the default
/// physical planner, which is also what renders the physical stage — so the plan a
/// client is shown is the one the read path would build, per-shard scans included.
async fn collect_locally(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    err_ctx: &ErrorContext,
) -> PgWireResult<(Schema, Vec<RecordBatch>)> {
    let state = ctx.state();
    let optimized = state
        .optimize(plan)
        .map_err(|e| enrich_datafusion_error(&e, err_ctx))?;
    let physical = DefaultPhysicalPlanner::default()
        .create_physical_plan(&optimized, &state)
        .await
        .map_err(|e| enrich_datafusion_error(&e, err_ctx))?;
    let schema = physical.schema().as_ref().clone();
    let batches = collect(physical, state.task_ctx())
        .await
        .map_err(|e| enrich_datafusion_error(&e, err_ctx))?;
    Ok((schema, batches))
}

/// Flatten DataFusion's `plan_type`/`plan` rows into PostgreSQL's shape: one
/// `text` column named `QUERY PLAN`, one row per line, each stage introduced by
/// its name.
///
/// A plan is many lines in one cell for DataFusion and one line per row for
/// PostgreSQL, and the difference is not cosmetic — a client renders the column
/// row by row, so leaving the newlines inside a cell prints the whole plan on one
/// line.
fn query_plan_batch(batches: &[RecordBatch]) -> PgWireResult<(Schema, Vec<RecordBatch>)> {
    let mut lines: Vec<String> = Vec::new();

    for batch in batches {
        let plan_type = string_column(batch, 0);
        let plan = string_column(batch, 1);
        for row in 0..batch.num_rows() {
            if let Some(stage) = plan_type.and_then(|c| cell(c, row)) {
                lines.push(stage.to_string());
            }
            for line in plan.and_then(|c| cell(c, row)).unwrap_or("").lines() {
                lines.push(line.to_string());
            }
        }
    }

    let schema = Schema::new(vec![Field::new(QUERY_PLAN, DataType::Utf8, false)]);
    let column = Arc::new(StringArray::from(lines));
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![column]).map_err(|e| {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!("failed to build the plan output: {e}"),
        )
    })?;
    Ok((schema, vec![batch]))
}

/// Column `idx` of `batch` as a `StringArray`, or `None` if the batch is narrower
/// than that or holds something else there. Tolerant rather than fallible: the
/// shape comes from DataFusion's explain schema, and a plan is still worth showing
/// with one of its two columns missing.
fn string_column(batch: &RecordBatch, idx: usize) -> Option<&StringArray> {
    batch
        .columns()
        .get(idx)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Cell `row` of `column`, or `None` when it is null.
fn cell(column: &StringArray, row: usize) -> Option<&str> {
    (!column.is_null(row)).then(|| column.value(row))
}

/// The columns an `EXPLAIN`/`DESCRIBE` returns, for the extended protocol's
/// Describe step. Derived from the planned statement so Describe and Execute agree
/// — an `EXPLAIN` reports `QUERY PLAN` however DataFusion shaped the plan node,
/// and a `DESCRIBE` reports DataFusion's three metadata columns.
pub(super) fn result_fields(plan: &LogicalPlan, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let schema = if matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_)) {
        Schema::new(vec![Field::new(QUERY_PLAN, DataType::Utf8, false)])
    } else {
        plan.schema().as_arrow().clone()
    };
    // Through `wire_schema` for the same reason a SELECT's Describe is: these rows are
    // encoded by `encoding::encode_batches_response`, which widens a `UInt64` column,
    // and a Describe that skipped the widening would promise a type Execute then does
    // not send. Neither shape above can hold a `UInt64` today, so this is what keeps
    // that from becoming a silent divergence if one ever does.
    arrow_pg::datatypes::arrow_schema_to_pg_fields(&encoding::wire_schema(&schema), format, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Statement {
        parser::parse_sql(sql)
            .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
            .into_iter()
            .next()
            .unwrap()
    }

    fn classify(sql: &str) -> Inspection {
        inspection(&parse(sql)).unwrap_or_else(|e| panic!("`{sql}` should be accepted: {e}"))
    }

    fn refusal(sql: &str) -> String {
        match inspection(&parse(sql)) {
            Ok(other) => panic!("`{sql}` should be refused, got {other:?}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn explain_and_describe_are_told_apart() {
        assert_eq!(
            classify("EXPLAIN SELECT 1"),
            Inspection::Plan {
                analyze: false,
                verbose: false
            }
        );
        assert_eq!(
            classify("EXPLAIN ANALYZE SELECT 1"),
            Inspection::Plan {
                analyze: true,
                verbose: false
            }
        );
        assert_eq!(
            classify("EXPLAIN VERBOSE SELECT 1"),
            Inspection::Plan {
                analyze: false,
                verbose: true
            }
        );
        assert_eq!(classify("DESCRIBE t"), Inspection::Shape);
        assert_eq!(classify("DESC t"), Inspection::Shape);
        assert_eq!(classify("DESCRIBE SELECT 1"), Inspection::Shape);
    }

    /// `EXPLAIN (ANALYZE)` is the PostgreSQL spelling, so it has to mean the same
    /// thing as the bare keyword — a client that writes it and gets a plan without
    /// execution would read timings that are not there.
    #[test]
    fn postgres_utility_options_mean_the_same_as_the_keywords() {
        assert_eq!(
            classify("EXPLAIN (ANALYZE) SELECT 1"),
            Inspection::Plan {
                analyze: true,
                verbose: false
            }
        );
        assert_eq!(
            classify("EXPLAIN (ANALYZE true, VERBOSE true) SELECT 1"),
            Inspection::Plan {
                analyze: true,
                verbose: true
            }
        );
        assert_eq!(
            classify("EXPLAIN (ANALYZE false) SELECT 1"),
            Inspection::Plan {
                analyze: false,
                verbose: false
            }
        );
    }

    /// Every option that would change the output is refused by its own name, so a
    /// client can tell which one it was.
    #[test]
    fn an_option_that_would_change_the_output_is_refused_by_name() {
        assert!(refusal("EXPLAIN (COSTS false) SELECT 1").contains("COSTS"));
        assert!(refusal("EXPLAIN (BUFFERS) SELECT 1").contains("BUFFERS"));
        assert!(refusal("EXPLAIN (FORMAT JSON) SELECT 1").contains("FORMAT"));
        assert!(refusal("EXPLAIN (SETTINGS) SELECT 1").contains("SETTINGS"));
    }

    /// An `EXPLAIN` of a write must not print a DataFusion plan: the write path
    /// ships the statement to DuckDB, which plans it there, so the plan shown
    /// would be for execution that never happens.
    #[test]
    fn explaining_a_write_is_refused_and_says_which_one() {
        for (sql, want) in [
            ("EXPLAIN INSERT INTO t (a) VALUES (1)", "an INSERT"),
            ("EXPLAIN UPDATE t SET a = 1", "an UPDATE"),
            ("EXPLAIN DELETE FROM t", "a DELETE"),
        ] {
            let message = refusal(sql);
            assert!(
                message.contains(want),
                "`{sql}` should name {want}, got: {message}"
            );
        }
    }

    #[test]
    fn dialect_only_plan_modes_are_refused() {
        assert!(refusal("EXPLAIN QUERY PLAN SELECT 1").contains("QUERY PLAN"));
    }

    /// The transaction rules need the query an `EXPLAIN` is about, and must not
    /// mistake a `DESCRIBE <relation>` for one — its shape cannot have been changed
    /// by the block, since DDL is refused inside one.
    #[test]
    fn the_inner_query_is_reachable_for_the_transaction_rules() {
        let stmt = parse("EXPLAIN SELECT a FROM orders");
        let inner = explained_query(&stmt).expect("an EXPLAIN of a query has one");
        assert_eq!(
            query_router::extract_select_table_name(inner).as_deref(),
            Some("orders")
        );
        assert!(explained_query(&parse("DESCRIBE orders")).is_none());
    }

    /// `DESCRIBE <relation>` becomes `DESCRIBE SELECT * FROM <relation>`, so a
    /// quoted, schema-qualified or mixed-case name has to survive the round trip
    /// through text and still resolve to the catalog key it started as.
    #[test]
    fn describe_rewrites_the_relation_into_a_query() {
        for (relation, want) in [
            ("orders", "orders"),
            ("sales.orders", "sales.orders"),
            ("\"Mixed Case\"", "Mixed Case"),
            ("Orders", "orders"),
        ] {
            let stmt = select_star_from(relation.to_string())
                .unwrap_or_else(|e| panic!("`{relation}` should rewrite: {e}"));
            assert_eq!(
                query_router::extract_select_table_name(&stmt).as_deref(),
                Some(want),
                "`DESCRIBE {relation}` should describe `{want}`"
            );
        }
    }

    /// The rewrite has to reach the *prepared* statement, not just the parsed one:
    /// a plan built from an unprepared `EXPLAIN` would print a query with its views
    /// unexpanded and its schema-qualified relations unresolved.
    /// A catalog in a temp file: preparation reads it to expand views, so it needs
    /// a real one even when the statement names no view.
    fn empty_catalog() -> Arc<MetadataCatalog> {
        Arc::new(crate::pgwire_handler::test_catalog::scratch_catalog(
            "introspection",
        ))
    }

    #[test]
    fn preparation_reaches_the_query_inside_an_explain() {
        let catalog = empty_catalog();
        let stmt = parse("EXPLAIN SELECT * FROM sales.orders");
        let inspection = inspection(&stmt).expect("accepted");
        let prepared = prepare(&stmt, &inspection, false, &catalog).expect("prepared");

        let inner = explained_query(&prepared).expect("still an EXPLAIN of a query");
        // `sales.orders` collapsed to the single quoted name that is its catalog
        // key, which is what the table provider is registered under.
        assert!(
            inner.to_string().contains("\"sales.orders\""),
            "the inner query should carry the collapsed name, got: {inner}"
        );
    }
}
