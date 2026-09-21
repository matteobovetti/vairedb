//! `pg_catalog.pg_settings`, answered for the connection that is asking.
//!
//! `SHOW` reads the session's own parameter map ([`super::session_params`]), so it has always
//! been right. The table could not be: it is a `TableProvider` registered once on a
//! `SessionContext` that every connection shares, and a provider has no way to ask *which*
//! connection is scanning it. So the row set was one hardcoded entry, and a client that reads
//! the table rather than issuing `SHOW` — which is what a driver's settings inspector and
//! most introspection tooling do — saw nothing it had set.
//!
//! ## How a shared table is made to answer per session
//!
//! The connection is not in the provider, but it *is* in the task: every statement is
//! executed on the task that owns its connection. So the session's parameters are put in a
//! [`tokio::task_local`] for the duration of the statement ([`with_session_settings`]), and
//! the provider reads them from there.
//!
//! The read happens in `TableProvider::scan`, which is **planning**, not execution — and
//! planning is awaited on the statement's own task, so the task-local is in scope. The rows
//! are materialized there into a `MemTable`, so by the time execution runs (on whatever task
//! DataFusion picks for it) the answer is already in the plan and no longer needs the
//! task-local. Putting the read in a `PartitionStream::execute` instead — which is how the
//! upstream view is built — would have read it from the wrong task.
//!
//! A scan with no task-local in scope returns no rows rather than failing. That is not an
//! error case a client can reach: it means something other than a statement handler is
//! scanning the table, and an empty settings list is the truthful answer to "what has this
//! session set" when there is no session.
//!
//! ## Why the schema is wrapped rather than the table replaced
//!
//! `datafusion-pg-catalog` registers `pg_catalog` as its own `SchemaProvider`, which
//! implements neither `register_table` nor any way to reach inside it, and its
//! `PgSettingsView` is `pub(crate)`. [`VaireDbPgCatalog`] therefore sits in front of the
//! registered provider and answers `pg_settings` itself, delegating every other name
//! unchanged. That keeps upstream's twenty-odd other tables — and the OID cache they share —
//! exactly as they were.

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::pgwire_handler::session_params::Setting;

/// The table this module owns, as `pg_catalog` spells it.
const PG_SETTINGS: &str = "pg_settings";

tokio::task_local! {
    /// The parameters of the connection whose statement is running on this task.
    static SESSION_SETTINGS: Arc<Vec<Setting>>;
}

/// Run `statement` with `settings` visible to `pg_catalog.pg_settings`.
///
/// Wrapped around one statement rather than one connection, so a `SET` earlier in the same
/// simple-query string is already in the snapshot the next statement reads.
pub(crate) async fn with_session_settings<F>(settings: Arc<Vec<Setting>>, statement: F) -> F::Output
where
    F: Future,
{
    SESSION_SETTINGS.scope(settings, statement).await
}

/// The settings of the statement running on this task, or none if there is no statement.
fn current_settings() -> Option<Arc<Vec<Setting>>> {
    SESSION_SETTINGS.try_with(Arc::clone).ok()
}

/// The `pg_catalog` schema VaireDB serves: upstream's, with `pg_settings` answered here.
#[derive(Debug)]
pub(crate) struct VaireDbPgCatalog {
    upstream: Arc<dyn SchemaProvider>,
    pg_settings: Arc<PgSettingsTable>,
}

impl VaireDbPgCatalog {
    /// Wrap the `pg_catalog` provider `upstream` registered.
    pub(crate) fn wrapping(upstream: Arc<dyn SchemaProvider>) -> Self {
        Self {
            upstream,
            pg_settings: Arc::new(PgSettingsTable::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for VaireDbPgCatalog {
    /// Upstream's list unchanged: `pg_settings` is one of its names, so replacing the
    /// provider behind that name adds nothing to the list.
    fn table_names(&self) -> Vec<String> {
        self.upstream.table_names()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case(PG_SETTINGS) {
            return Ok(Some(Arc::clone(&self.pg_settings) as Arc<dyn TableProvider>));
        }
        self.upstream.table(name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.upstream.table_exist(name)
    }
}

/// `pg_catalog.pg_settings`, materialized per scan from the scanning session's parameters.
#[derive(Debug)]
struct PgSettingsTable {
    schema: SchemaRef,
}

impl PgSettingsTable {
    /// PostgreSQL's own column list and order.
    ///
    /// Every column is nullable, including the ones always filled: a catalog view is read by
    /// tools that were written against PostgreSQL's own, and PostgreSQL's `pg_settings` marks
    /// nothing `NOT NULL`.
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("setting", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
            Field::new("short_desc", DataType::Utf8, true),
            Field::new("extra_desc", DataType::Utf8, true),
            Field::new("context", DataType::Utf8, true),
            Field::new("vartype", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("min_val", DataType::Utf8, true),
            Field::new("max_val", DataType::Utf8, true),
            Field::new("enumvals", DataType::Utf8, true),
            // `boot_val`, which is the column's name in PostgreSQL. Upstream spells it
            // `bool_val`; a tool that reads the real name would have found nothing.
            Field::new("boot_val", DataType::Utf8, true),
            Field::new("reset_val", DataType::Utf8, true),
            Field::new("sourcefile", DataType::Utf8, true),
            Field::new("sourceline", DataType::Int32, true),
            Field::new("pending_restart", DataType::Boolean, true),
        ]));
        Self { schema }
    }

    /// One row per setting, in the column order [`Self::new`] declares.
    fn batch(&self, settings: &[Setting]) -> DfResult<RecordBatch> {
        let strings =
            |values: Vec<Option<String>>| -> ArrayRef { Arc::new(StringArray::from(values)) };
        let nulls: Vec<Option<String>> = vec![None; settings.len()];
        let columns: Vec<ArrayRef> = vec![
            strings(settings.iter().map(|s| Some(s.name.to_string())).collect()),
            strings(settings.iter().map(|s| Some(s.setting.clone())).collect()),
            // `unit` and `category`: see [`Setting`] for why these are not invented.
            strings(nulls.clone()),
            strings(nulls.clone()),
            strings(
                settings
                    .iter()
                    .map(|s| Some(s.short_desc.to_string()))
                    .collect(),
            ),
            strings(nulls.clone()),
            strings(
                settings
                    .iter()
                    .map(|s| Some(s.context.to_string()))
                    .collect(),
            ),
            strings(
                settings
                    .iter()
                    .map(|s| Some(s.vartype.to_string()))
                    .collect(),
            ),
            strings(
                settings
                    .iter()
                    .map(|s| Some(s.source.to_string()))
                    .collect(),
            ),
            strings(nulls.clone()),
            strings(nulls.clone()),
            strings(nulls.clone()),
            strings(
                settings
                    .iter()
                    .map(|s| Some(s.boot_val.to_string()))
                    .collect(),
            ),
            strings(settings.iter().map(|s| Some(s.reset_val.clone())).collect()),
            strings(nulls.clone()),
            Arc::new(Int32Array::from(vec![None::<i32>; settings.len()])),
            // Nothing VaireDB accepts needs a restart to take effect, so this is never
            // true rather than never known.
            Arc::new(BooleanArray::from(vec![Some(false); settings.len()])),
        ];
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
    }
}

#[async_trait]
impl TableProvider for PgSettingsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    /// Materialize the scanning session's settings and scan them as a `MemTable`.
    ///
    /// The task-local is read *here* — see the module docs: this runs on the statement's own
    /// task, and everything downstream reads the rows out of the plan.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let settings = current_settings().unwrap_or_default();
        let batch = self.batch(&settings)?;
        MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// The `pg_settings` schema, for a caller that needs the column list without a session.
#[cfg(test)]
pub(crate) fn settings_schema() -> SchemaRef {
    PgSettingsTable::new().schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::execution::context::SessionContext;

    fn setting(name: &'static str, value: &str, source: &'static str) -> Setting {
        Setting {
            name,
            setting: value.to_string(),
            short_desc: "a parameter",
            vartype: "string",
            context: "user",
            source,
            reset_val: "the default".to_string(),
            boot_val: "the default",
        }
    }

    /// A context holding nothing but this table under its real name, which is all the
    /// per-session behaviour needs to be observed through.
    async fn context() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_table("pg_settings", Arc::new(PgSettingsTable::new()))
            .expect("the table registers");
        ctx
    }

    async fn settings_seen(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the projected column is a string")
                    .clone();
                (0..column.len())
                    .map(move |row| column.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn the_table_reports_what_the_scanning_session_set() {
        let ctx = context().await;
        let rows = with_session_settings(
            Arc::new(vec![setting("TimeZone", "UTC", "default")]),
            settings_seen(
                &ctx,
                "SELECT setting FROM pg_settings WHERE name = 'TimeZone'",
            ),
        )
        .await;
        assert_eq!(rows, vec!["UTC"]);
    }

    #[tokio::test]
    async fn two_sessions_reading_one_table_get_their_own_values() {
        // The point of the whole module: one registered provider, two answers.
        let ctx = context().await;
        let sql = "SELECT setting FROM pg_settings WHERE name = 'application_name'";

        let first = with_session_settings(
            Arc::new(vec![setting("application_name", "psql", "session")]),
            settings_seen(&ctx, sql),
        )
        .await;
        let second = with_session_settings(
            Arc::new(vec![setting("application_name", "dbeaver", "session")]),
            settings_seen(&ctx, sql),
        )
        .await;

        assert_eq!(first, vec!["psql"]);
        assert_eq!(second, vec!["dbeaver"]);
    }

    #[tokio::test]
    async fn a_scan_outside_a_statement_reports_nothing_rather_than_failing() {
        let ctx = context().await;
        let rows = settings_seen(&ctx, "SELECT name FROM pg_settings").await;
        assert!(
            rows.is_empty(),
            "no session means no settings, not an error: {rows:?}"
        );
    }

    #[tokio::test]
    async fn every_column_postgresql_has_is_present_and_the_known_ones_are_filled() {
        let ctx = context().await;
        let names: Vec<String> = settings_schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "name",
                "setting",
                "unit",
                "category",
                "short_desc",
                "extra_desc",
                "context",
                "vartype",
                "source",
                "min_val",
                "max_val",
                "enumvals",
                "boot_val",
                "reset_val",
                "sourcefile",
                "sourceline",
                "pending_restart",
            ]
        );

        let filled = with_session_settings(
            Arc::new(vec![setting("TimeZone", "UTC", "session")]),
            async {
                ctx.sql(
                    "SELECT name, setting, short_desc, context, vartype, source, boot_val, \
                     reset_val, pending_restart, unit FROM pg_settings",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
            },
        )
        .await;
        let batch = &filled[0];
        assert_eq!(batch.num_rows(), 1);
        for (column, expected) in [
            (0, "TimeZone"),
            (1, "UTC"),
            (2, "a parameter"),
            (3, "user"),
            (4, "string"),
            (5, "session"),
            (6, "the default"),
            (7, "the default"),
        ] {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a string column");
            assert_eq!(values.value(0), expected, "column {column}");
        }
        assert!(
            !batch
                .column(8)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("pending_restart is a boolean")
                .value(0),
            "nothing VaireDB accepts needs a restart"
        );
        assert!(batch.column(9).is_null(0), "unit is not invented");
    }

    #[tokio::test]
    async fn the_wrapper_answers_pg_settings_and_delegates_everything_else() {
        use datafusion::catalog::MemorySchemaProvider;

        let upstream = Arc::new(MemorySchemaProvider::new());
        // Stand-ins for upstream's own tables: one under the name VaireDB takes over, so the
        // delegation can be shown to be *not* happening for it.
        upstream
            .register_table(PG_SETTINGS.to_string(), Arc::new(PgSettingsTable::new()))
            .unwrap();
        upstream
            .register_table(
                "pg_class".to_string(),
                Arc::new(MemTable::try_new(Arc::new(Schema::empty()), vec![vec![]]).unwrap()),
            )
            .unwrap();

        let wrapper = VaireDbPgCatalog::wrapping(upstream.clone());
        let ours = wrapper.table(PG_SETTINGS).await.unwrap().unwrap();
        assert!(
            Arc::ptr_eq(
                &(Arc::clone(&wrapper.pg_settings) as Arc<dyn TableProvider>),
                &ours
            ),
            "pg_settings is VaireDB's own provider, not the registered one"
        );
        // Asked by the name a client may spell in any case.
        assert!(wrapper.table("PG_SETTINGS").await.unwrap().is_some());

        assert!(wrapper.table("pg_class").await.unwrap().is_some());
        assert!(wrapper.table("pg_nonexistent").await.unwrap().is_none());
        assert_eq!(
            {
                let mut names = wrapper.table_names();
                names.sort();
                names
            },
            vec!["pg_class".to_string(), PG_SETTINGS.to_string()]
        );
        assert!(wrapper.table_exist("pg_class"));
    }

    #[tokio::test]
    async fn the_snapshot_is_the_one_the_statement_started_with() {
        // A projection and a filter both push down through the `MemTable`, so the rows a
        // client selects are the rows this session has and not the whole list.
        let ctx = context().await;
        let rows = with_session_settings(
            Arc::new(vec![
                setting("DateStyle", "ISO, YMD", "default"),
                setting("TimeZone", "UTC", "default"),
            ]),
            settings_seen(
                &ctx,
                "SELECT name FROM pg_settings WHERE source = 'default' ORDER BY name",
            ),
        )
        .await;
        assert_eq!(rows, vec!["DateStyle", "TimeZone"]);
    }
}
