//! `COPY ... TO/FROM` a server-side CSV file: the bulk import/export path.
//!
//! Both directions are built from parts that already exist, which is what keeps
//! them consistent with ordinary statements:
//!
//! * `COPY <table|query> TO '<file>'` runs the source on the read path — so it
//!   gathers every shard's rows through the same planner a `SELECT` uses — and
//!   writes the collected batches out as CSV.
//! * `COPY <table> FROM '<file>'` reads the CSV into record batches and hands them
//!   to the INSERT lane, so every row is routed by its shard key, split per shard,
//!   and counted exactly like a client `INSERT ... VALUES` would be.
//!
//! The file is on the **coordinator's** filesystem, not the client's: this is
//! PostgreSQL's server-side `COPY`, and `COPY ... FROM STDIN`/`TO STDOUT` (the
//! client-side forms, which need the copy sub-protocol) are refused by name.
//!
//! Only CSV is accepted, and it must be asked for explicitly: PostgreSQL's default
//! `TEXT` format is a different encoding, and silently writing CSV where a client
//! expects `TEXT` would corrupt whatever reads the file next.

use std::fs::File;
use std::io::BufWriter;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::WriterBuilder;
use datafusion::prelude::CsvReadOptions;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::error::CoordinatorError;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::parser::parse_sql;
use crate::pgwire_handler::query_router::{
    canonical_table_name, canonicalize_ident, canonicalize_ident_str,
};
use crate::pgwire_handler::session::SessionState;
use crate::sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Query, Statement,
};
use crate::write_sql_cl;

/// The CSV dialect a COPY reads or writes.
///
/// Deliberately small: the options kept are the ones that mean the same thing in
/// both directions, so a file written by `COPY ... TO` is read back identically by
/// `COPY ... FROM` with the same options. Anything else is refused rather than
/// accepted-and-ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CsvDialect {
    /// `HEADER`: on export, write the column names first; on import, read them.
    header: bool,
    delimiter: u8,
    quote: u8,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            header: false,
            delimiter: b',',
            quote: b'"',
        }
    }
}

/// What a `COPY ... TO` exports.
#[derive(Debug, Clone, PartialEq)]
enum CopyOutSource {
    /// A table, with the columns to export (empty means every column).
    Table { name: String, columns: Vec<String> },
    /// `COPY (SELECT ...) TO`: an arbitrary query.
    Query(Box<Query>),
}

/// A validated COPY: everything decidable from the statement alone is settled
/// here, so the I/O steps have no rules left to apply.
#[derive(Debug, Clone, PartialEq)]
enum CopyPlan {
    Out {
        source: CopyOutSource,
        path: String,
        dialect: CsvDialect,
    },
    In {
        table: String,
        /// The columns the file's fields map onto, canonical. Empty means "decide
        /// from the file's header, or the table's leading columns".
        columns: Vec<String>,
        path: String,
        dialect: CsvDialect,
    },
}

impl VaireDbQueryHandler {
    /// Run a `COPY`, exporting to or importing from a CSV file on the coordinator.
    ///
    /// Returns PostgreSQL's `COPY <n>` tag, counting the rows written to the file
    /// or to the table.
    pub(super) async fn handle_copy(
        &self,
        stmt: &Statement,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let rows = match plan_copy(stmt)? {
            CopyPlan::Out {
                source,
                path,
                dialect,
            } => self.copy_to_file(&source, &path, &dialect).await?,
            CopyPlan::In {
                table,
                columns,
                path,
                dialect,
            } => {
                self.copy_from_file(&table, &columns, &path, &dialect, session)
                    .await?
            }
        };

        Ok(Response::Execution(
            Tag::new("COPY").with_rows(rows as usize),
        ))
    }

    /// Export a table or query to a CSV file, returning the number of rows written.
    ///
    /// The source runs on the read path, which is what makes the export complete:
    /// gathering the shards is the planner's job, not this function's.
    async fn copy_to_file(
        &self,
        source: &CopyOutSource,
        path: &str,
        dialect: &CsvDialect,
    ) -> PgWireResult<u64> {
        let query = match source {
            CopyOutSource::Table { name, columns } => {
                // Reported before the query is planned so a missing table is a
                // table error rather than whatever the planner makes of it.
                let ctx = ErrorContext::for_table(name);
                if self
                    .catalog
                    .get_table(name)
                    .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
                    .is_none()
                {
                    let err = CoordinatorError::TableNotFound(name.clone());
                    return Err(enrich_coordinator_error(&err, &ctx, &self.catalog));
                }
                select_from_table(name, columns)?
            }
            CopyOutSource::Query(query) => Statement::Query(query.clone()),
        };

        let (_schema, batches) = self.collect_query_rows(&query, &[]).await?;
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        write_csv_file(path.to_string(), batches, dialect.clone()).await?;
        Ok(rows as u64)
    }

    /// Import a CSV file into a table, returning the number of rows written.
    ///
    /// The rows go through the INSERT lane, so each is routed by its shard key and
    /// the count is the number actually shipped. Like any multi-statement write,
    /// the whole file is planned before any of it ships: a file with a row that
    /// cannot be routed is refused with nothing written.
    async fn copy_from_file(
        &self,
        table: &str,
        columns: &[String],
        path: &str,
        dialect: &CsvDialect,
        session: &SessionState,
    ) -> PgWireResult<u64> {
        let ctx = ErrorContext::for_table(table);
        let table_meta = self
            .catalog
            .get_table(table)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(table.to_string());
                enrich_coordinator_error(&err, &ctx, &self.catalog)
            })?;

        // The reader resolves the path as an object-store listing, which yields an
        // empty schema for a path that matches nothing instead of failing. Checked
        // here so a typo'd path is a file error rather than an import that reports
        // success having written nothing.
        let metadata = std::fs::metadata(path).map_err(|e| file_error("read", path, &e))?;
        if !metadata.is_file() {
            return Err(make_vdb_error(
                VdbErrorCode::InternalError,
                format!("COPY could not read \"{path}\" on the coordinator: not a file"),
            ));
        }

        let options = CsvReadOptions::new()
            .has_header(dialect.header)
            .delimiter(dialect.delimiter)
            .quote(dialect.quote)
            // The path names one file, so its extension is the client's business:
            // the default `.csv` filter would refuse `COPY ... FROM '/tmp/export'`.
            .file_extension("");
        // `local_ctx`, not `session_ctx`: the file is on the coordinator's disk and
        // `session_ctx` is upgraded for Ballista, so it would plan the scan as a
        // distributed one and hand it to an executor — a core node, where the path
        // does not exist. The rows are re-emitted as literals afterwards anyway, so
        // reading them locally costs nothing distributed.
        let frame = self
            .local_ctx
            .read_csv(path, options)
            .await
            .map_err(|e| file_error("read", path, &e))?;
        let schema = frame.schema().as_arrow().clone();
        if schema.fields().is_empty() {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!("COPY could not find any columns in \"{path}\": the file is empty"),
            ));
        }
        let batches = frame
            .collect()
            .await
            .map_err(|e| file_error("read", path, &e))?;

        let table_columns: Vec<String> =
            table_meta.columns.iter().map(|c| c.name.clone()).collect();
        let target = target_columns(
            columns,
            &schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            &table_columns,
            dialect.header,
        )?;

        for column in &target {
            if !table_columns.contains(column) {
                return Err(make_vdb_error(
                    VdbErrorCode::ColumnNotFound,
                    format!("column \"{column}\" does not exist in table \"{table}\""),
                ));
            }
        }
        // Every row is placed by hashing its shard key, so a file that does not
        // carry that column cannot be imported — refused before anything is read
        // into the table rather than routed somewhere arbitrary.
        if !target.contains(&table_meta.shard_key) {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!(
                    "COPY ... FROM must include the shard key \"{}\": every row is routed by it. Add the column to the file, or name the columns in COPY {table} (...) FROM",
                    table_meta.shard_key
                ),
            ));
        }

        let column_refs: Vec<&str> = target.iter().map(String::as_str).collect();
        let template = write_sql_cl::insert_template(table, &column_refs)
            .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;
        let statements = write_sql_cl::insert_statements_from_batches(
            &template,
            &batches,
            write_sql_cl::ROWS_PER_STATEMENT,
        )
        .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;

        self.write_row_statements(&statements, &[], session, table, "COPY")
            .await
    }
}

/// True for the direction that puts rows into a table, `COPY ... FROM`.
///
/// Used by the transaction guard, which has to tell a bulk *write* from a bulk
/// *read* before either has been planned.
pub(super) fn copy_writes_rows(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Copy { to: false, .. })
}

/// Canonical names of every relation a `COPY ... TO` reads — the table it exports,
/// or every relation of the query it exports. Empty for `COPY ... FROM`, which
/// reads a file rather than a table.
pub(super) fn copy_source_tables(stmt: &Statement) -> Vec<String> {
    let Statement::Copy {
        source, to: true, ..
    } = stmt
    else {
        return Vec::new();
    };
    match source {
        CopySource::Table { table_name, .. } => {
            canonical_table_name(table_name).into_iter().collect()
        }
        CopySource::Query(query) => write_sql_cl::relations_read(query.as_ref()),
    }
}

/// Validate a `COPY` and reduce it to the plan the I/O steps run.
fn plan_copy(stmt: &Statement) -> PgWireResult<CopyPlan> {
    let Statement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        values,
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a COPY statement",
        ));
    };

    let path = copy_file_path(target, *to)?;
    let dialect = csv_dialect(options, legacy_options)?;

    // Inline data belongs to `COPY ... FROM STDIN`, which `copy_file_path` has
    // already refused; a non-empty `values` here would mean data with nowhere to go.
    if !values.is_empty() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY with inline data is not supported by VaireDB: write the rows to a file on the coordinator and use COPY ... FROM '<file>' (FORMAT CSV), or use INSERT",
        ));
    }

    if *to {
        let source = match source {
            CopySource::Table {
                table_name,
                columns,
            } => CopyOutSource::Table {
                name: canonical_table_name(table_name).ok_or_else(|| {
                    make_vdb_error(
                        VdbErrorCode::SqlSyntaxError,
                        "could not determine the table to copy from",
                    )
                })?,
                columns: columns.iter().map(canonicalize_ident).collect(),
            },
            CopySource::Query(query) => CopyOutSource::Query(query.clone()),
        };
        return Ok(CopyPlan::Out {
            source,
            path,
            dialect,
        });
    }

    // `COPY (SELECT ...) FROM` has no meaning: a query is not a place to put rows.
    let CopySource::Table {
        table_name,
        columns,
    } = source
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "COPY ... FROM must name a table to import into",
        ));
    };

    Ok(CopyPlan::In {
        table: canonical_table_name(table_name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine the table to copy into",
            )
        })?,
        columns: columns.iter().map(canonicalize_ident).collect(),
        path,
        dialect,
    })
}

/// The server-side file a COPY reads or writes.
///
/// `STDIN`/`STDOUT` are the client-side forms: they hand the connection to the
/// copy sub-protocol, which VaireDB does not drive, so they are refused by name
/// rather than mistaken for the file form. `PROGRAM` would run a shell command on
/// the coordinator, which VaireDB does not do at all.
fn copy_file_path(target: &CopyTarget, to: bool) -> PgWireResult<String> {
    match target {
        CopyTarget::File { filename } => Ok(filename.clone()),
        CopyTarget::Stdin | CopyTarget::Stdout => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "COPY ... {} is not supported by VaireDB: the streaming copy sub-protocol is not implemented. Use a file on the coordinator: COPY ... {} '<file>' (FORMAT CSV)",
                if to { "TO STDOUT" } else { "FROM STDIN" },
                if to { "TO" } else { "FROM" }
            ),
        )),
        CopyTarget::Program { .. } => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY ... PROGRAM is not supported by VaireDB: the coordinator does not run shell commands. Use a file on the coordinator: COPY ... '<file>' (FORMAT CSV)",
        )),
    }
}

/// The CSV dialect asked for by a COPY's options, in either the modern
/// `(FORMAT CSV, HEADER)` spelling or the legacy `CSV HEADER` one.
///
/// `FORMAT CSV` is required rather than defaulted: PostgreSQL's default is `TEXT`,
/// a different encoding, and a client that omitted the format is expecting that
/// one. An option VaireDB does not honor is refused by name — accepting and
/// ignoring `NULL 'x'` or `FORCE_QUOTE` would write a file that does not say what
/// the client asked it to say.
fn csv_dialect(
    options: &[CopyOption],
    legacy_options: &[CopyLegacyOption],
) -> PgWireResult<CsvDialect> {
    let mut dialect = CsvDialect::default();
    let mut csv_requested = false;

    for option in options {
        match option {
            CopyOption::Format(name) if name.value.eq_ignore_ascii_case("csv") => {
                csv_requested = true;
            }
            CopyOption::Format(name) => return Err(unsupported_format(&name.value)),
            CopyOption::Header(header) => dialect.header = *header,
            CopyOption::Delimiter(c) => dialect.delimiter = single_byte("DELIMITER", *c)?,
            CopyOption::Quote(c) => dialect.quote = single_byte("QUOTE", *c)?,
            other => return Err(unsupported_option(&other.to_string())),
        }
    }

    for option in legacy_options {
        match option {
            CopyLegacyOption::Csv(csv_options) => {
                csv_requested = true;
                for csv_option in csv_options {
                    match csv_option {
                        CopyLegacyCsvOption::Header => dialect.header = true,
                        CopyLegacyCsvOption::Quote(c) => {
                            dialect.quote = single_byte("QUOTE", *c)?;
                        }
                        other => return Err(unsupported_option(&other.to_string())),
                    }
                }
            }
            CopyLegacyOption::Binary => return Err(unsupported_format("BINARY")),
            CopyLegacyOption::Delimiter(c) => {
                dialect.delimiter = single_byte("DELIMITER", *c)?;
            }
            other => return Err(unsupported_option(&other.to_string())),
        }
    }

    if !csv_requested {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "COPY without FORMAT CSV is not supported by VaireDB: PostgreSQL's default TEXT format is a different encoding, so it is refused rather than written as CSV. Add (FORMAT CSV)",
        ));
    }

    Ok(dialect)
}

/// `0A000` for a format VaireDB cannot read or write.
fn unsupported_format(name: &str) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!("COPY format {name} is not supported by VaireDB: only CSV is. Use (FORMAT CSV)"),
    )
}

/// `0A000` for an option VaireDB would have to ignore.
fn unsupported_option(rendered: &str) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "COPY option `{rendered}` is not supported by VaireDB, and is refused rather than ignored: a file written or read against options that were dropped would not hold what the statement said. Supported options are FORMAT CSV, HEADER, DELIMITER and QUOTE"
        ),
    )
}

/// A delimiter or quote character as the single byte the CSV reader/writer takes.
/// A multi-byte character is refused rather than truncated into a byte that would
/// split the file's rows somewhere the client never asked for.
fn single_byte(option: &str, c: char) -> PgWireResult<u8> {
    if c.is_ascii() {
        Ok(c as u8)
    } else {
        Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("COPY {option} must be a single ASCII character, got '{c}'"),
        ))
    }
}

/// The table columns a CSV file's fields map onto, in file order.
///
/// The statement's own column list wins. Failing that, a file with a header names
/// its columns, and a file without one is positional against the table's leading
/// columns — the same rule PostgreSQL applies to `INSERT INTO t VALUES (...)`.
fn target_columns(
    stated: &[String],
    file_fields: &[String],
    table_columns: &[String],
    header: bool,
) -> PgWireResult<Vec<String>> {
    let columns = if !stated.is_empty() {
        stated.to_vec()
    } else if header {
        file_fields
            .iter()
            .map(|f| canonicalize_ident_str(f))
            .collect()
    } else {
        table_columns
            .iter()
            .take(file_fields.len())
            .cloned()
            .collect()
    };

    if columns.len() != file_fields.len() {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!(
                "COPY names {} column(s) but the file has {} field(s)",
                columns.len(),
                file_fields.len()
            ),
        ));
    }
    Ok(columns)
}

/// `SELECT <columns> FROM <table>` for a `COPY <table> TO`, built as SQL and
/// re-parsed so it goes through exactly the parse the read path expects. Names are
/// quoted because they are catalog-canonical by now: unquoted, DuckDB and
/// DataFusion would fold a name whose case survived `CREATE TABLE`.
fn select_from_table(table: &str, columns: &[String]) -> PgWireResult<Statement> {
    let projection = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|c| quoted(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let sql = format!("SELECT {projection} FROM {}", quoted(table));
    parse_sql(&sql)
        .map_err(|e| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!("could not build a query for COPY {table} TO: {e}"),
            )
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::InternalError,
                format!("could not build a query for COPY {table} TO"),
            )
        })
}

/// A double-quoted identifier, with any embedded quote doubled.
fn quoted(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Write collected batches to `path` as CSV.
///
/// Runs on a blocking thread: the file is on the coordinator's disk and an export
/// is as large as the result, which is not something to hold an async worker for.
/// The file is truncated, matching PostgreSQL's `COPY ... TO`.
async fn write_csv_file(
    path: String,
    batches: Vec<RecordBatch>,
    dialect: CsvDialect,
) -> PgWireResult<()> {
    let reported_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let file = File::create(&path).map_err(|e| file_error("write", &path, &e))?;
        let mut writer = WriterBuilder::new()
            .with_header(dialect.header)
            .with_delimiter(dialect.delimiter)
            .with_quote(dialect.quote)
            .build(BufWriter::new(file));
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| file_error("write", &path, &e))?;
        }
        // The batches are written through a `BufWriter`, so the last of them only
        // reaches the file when it is flushed — a failure here is a short file,
        // which the client has to hear about.
        writer
            .into_inner()
            .into_inner()
            .map_err(|e| file_error("write", &path, &e))?
            .sync_all()
            .map_err(|e| file_error("write", &path, &e))?;
        Ok(())
    })
    .await
    .map_err(|e| {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!("COPY could not finish writing \"{reported_path}\": {e}"),
        )
    })?
}

/// A file the coordinator could not read or write, named with the cause. The
/// coordinator's filesystem is the one that matters here, so the message says so:
/// a client looking for the file on its own machine will not find it.
fn file_error(
    verb: &str,
    path: &str,
    cause: &impl std::fmt::Display,
) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("COPY could not {verb} \"{path}\" on the coordinator: {cause}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::error::PgWireError;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    fn plan(sql: &str) -> CopyPlan {
        plan_copy(&parse_one(sql)).unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
    }

    /// The SQLSTATE and message a refused COPY reports.
    fn rejection(sql: &str) -> (String, String) {
        match plan_copy(&parse_one(sql)).expect_err("`{sql}` must be refused") {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    #[test]
    fn copy_to_a_file_plans_an_export_of_the_named_table() {
        assert_eq!(
            plan("COPY orders TO '/tmp/orders.csv' (FORMAT CSV, HEADER)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                path: "/tmp/orders.csv".to_string(),
                dialect: CsvDialect {
                    header: true,
                    ..Default::default()
                },
            }
        );
    }

    // The names are catalog-canonical from here on: a quoted column keeps its case,
    // an unquoted one folds, and a schema qualifier is dropped like everywhere else.
    #[test]
    fn an_export_canonicalizes_the_table_and_column_names() {
        assert_eq!(
            plan("COPY public.Orders (ID, \"V\") TO '/tmp/o.csv' (FORMAT CSV)"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec!["id".to_string(), "V".to_string()],
                },
                path: "/tmp/o.csv".to_string(),
                dialect: CsvDialect::default(),
            }
        );
    }

    #[test]
    fn copy_from_a_file_plans_an_import_into_the_named_table() {
        assert_eq!(
            plan("COPY orders (id, v) FROM '/tmp/orders.csv' (FORMAT CSV, HEADER)"),
            CopyPlan::In {
                table: "orders".to_string(),
                columns: vec!["id".to_string(), "v".to_string()],
                path: "/tmp/orders.csv".to_string(),
                dialect: CsvDialect {
                    header: true,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn a_query_export_keeps_its_query() {
        let plan = plan("COPY (SELECT id FROM orders WHERE id > 1) TO '/tmp/o.csv' (FORMAT CSV)");
        assert!(
            matches!(
                plan,
                CopyPlan::Out {
                    source: CopyOutSource::Query(_),
                    ..
                }
            ),
            "got: {plan:?}"
        );
    }

    // The streaming forms need the copy sub-protocol, which VaireDB does not drive.
    // Refused by name so a client is not left waiting on a stream that never opens.
    #[test]
    fn the_streaming_forms_are_refused_by_name() {
        // `FROM STDIN` only parses terminated: the parser reads what follows as the
        // statement's inline data, which is how a client sends it.
        for (sql, named) in [
            ("COPY orders TO STDOUT (FORMAT CSV)", "TO STDOUT"),
            ("COPY orders FROM STDIN (FORMAT CSV);", "FROM STDIN"),
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(named), "`{sql}` got: {msg}");
            assert!(msg.contains("file"), "`{sql}` got: {msg}");
        }
    }

    #[test]
    fn copy_program_is_refused() {
        let (code, msg) = rejection("COPY orders TO PROGRAM 'gzip > /tmp/o.gz' (FORMAT CSV)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("shell"), "got: {msg}");
    }

    // PostgreSQL's default is TEXT, a different encoding. Writing CSV instead would
    // silently produce a file that whatever reads it next cannot parse.
    #[test]
    fn a_copy_that_does_not_ask_for_csv_is_refused() {
        for sql in [
            "COPY orders TO '/tmp/o.csv'",
            "COPY orders FROM '/tmp/o.csv'",
            "COPY orders TO '/tmp/o.csv' (DELIMITER '|')",
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("FORMAT CSV"), "`{sql}` got: {msg}");
        }
    }

    #[test]
    fn a_non_csv_format_is_refused_by_name() {
        for (sql, named) in [
            ("COPY orders TO '/tmp/o.txt' (FORMAT TEXT)", "TEXT"),
            ("COPY orders TO '/tmp/o.bin' (FORMAT BINARY)", "BINARY"),
            ("COPY orders TO '/tmp/o.bin' WITH BINARY", "BINARY"),
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(named), "`{sql}` got: {msg}");
        }
    }

    // Accepting an option and then ignoring it produces a file that does not hold
    // what the statement said it would.
    #[test]
    fn an_option_vairedb_would_have_to_ignore_is_refused() {
        for sql in [
            "COPY orders TO '/tmp/o.csv' (FORMAT CSV, NULL '\\N')",
            "COPY orders TO '/tmp/o.csv' (FORMAT CSV, FORCE_QUOTE (id))",
            "COPY orders FROM '/tmp/o.csv' (FORMAT CSV, ENCODING 'LATIN1')",
            "COPY orders FROM '/tmp/o.csv' (FORMAT CSV, FREEZE)",
        ] {
            let (code, msg) = rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("not supported"), "`{sql}` got: {msg}");
        }
    }

    // The legacy spelling means the same thing, so it plans the same way.
    #[test]
    fn the_legacy_csv_spelling_plans_like_the_modern_one() {
        assert_eq!(
            plan("COPY orders TO '/tmp/o.csv' WITH CSV HEADER"),
            CopyPlan::Out {
                source: CopyOutSource::Table {
                    name: "orders".to_string(),
                    columns: vec![],
                },
                path: "/tmp/o.csv".to_string(),
                dialect: CsvDialect {
                    header: true,
                    ..Default::default()
                },
            }
        );
    }

    #[test]
    fn a_delimiter_and_quote_are_carried_into_the_dialect() {
        let CopyPlan::Out { dialect, .. } =
            plan("COPY orders TO '/tmp/o.csv' (FORMAT CSV, DELIMITER ';', QUOTE '''')")
        else {
            panic!("expected an export");
        };
        assert_eq!(dialect.delimiter, b';');
        assert_eq!(dialect.quote, b'\'');
    }

    // --- target_columns ---

    #[test]
    fn a_stated_column_list_decides_the_mapping() {
        let columns = target_columns(
            &["v".to_string(), "id".to_string()],
            &["one".to_string(), "two".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns, vec!["v".to_string(), "id".to_string()]);
    }

    // A header names the file's columns, so the mapping is by name and the file's
    // order need not match the table's.
    #[test]
    fn a_header_names_the_columns_when_the_statement_does_not() {
        let columns = target_columns(
            &[],
            &["V".to_string(), "id".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns, vec!["v".to_string(), "id".to_string()]);
    }

    // Without a header there is nothing to map by name, so the fields land on the
    // table's leading columns — the rule PostgreSQL uses for a positional INSERT.
    #[test]
    fn a_headerless_file_is_positional_against_the_leading_columns() {
        let columns = target_columns(
            &[],
            &["column_1".to_string()],
            &["id".to_string(), "v".to_string()],
            false,
        )
        .unwrap();
        assert_eq!(columns, vec!["id".to_string()]);
    }

    #[test]
    fn a_column_list_that_does_not_match_the_files_width_is_refused() {
        let err = target_columns(
            &["id".to_string()],
            &["id".to_string(), "v".to_string()],
            &["id".to_string(), "v".to_string()],
            true,
        )
        .expect_err("a narrower column list than the file must be refused");
        let PgWireError::UserError(info) = err else {
            panic!("expected a user-facing error");
        };
        assert_eq!(info.code, "42601");
        assert!(info.message.contains("1 column(s)"), "{}", info.message);
        assert!(info.message.contains("2 field(s)"), "{}", info.message);
    }

    // --- select_from_table ---

    #[test]
    fn an_export_of_every_column_selects_star() {
        let sql = crate::write_sql_cl::statement_to_sql(&select_from_table("orders", &[]).unwrap());
        assert_eq!(sql, "SELECT * FROM \"orders\"");
    }

    #[test]
    fn an_export_of_named_columns_projects_them_quoted() {
        let columns = ["id".to_string(), "Total".to_string()];
        let sql =
            crate::write_sql_cl::statement_to_sql(&select_from_table("orders", &columns).unwrap());
        assert_eq!(sql, "SELECT \"id\", \"Total\" FROM \"orders\"");
    }

    // --- the handler, against an empty catalog and no reachable nodes ---

    use crate::catalog::{ColumnDef, TableMeta};

    /// Register a table sharded by `id` so a COPY can resolve it.
    fn register(handler: &VaireDbQueryHandler, name: &str, columns: &[&str]) {
        handler
            .catalog
            .put_table(&TableMeta {
                table_name: name.to_string(),
                columns: columns
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.to_string(),
                        data_type: "INTEGER".to_string(),
                        nullable: true,
                        default_expr: String::new(),
                    })
                    .collect(),
                shard_key: "id".to_string(),
                shard_count: 2,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
    }

    /// Write a CSV file for a COPY to read, returning its path.
    fn write_csv(name: &str, contents: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "vairedb_copy_test_{}_{name}.csv",
            std::process::id()
        ));
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// The SQLSTATE and message a COPY reports to the client.
    async fn copy_rejection(handler: &VaireDbQueryHandler, sql: &str) -> (String, String) {
        let session = SessionState::default();
        let err = handler
            .handle_copy(&parse_one(sql), &session)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be rejected"));
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    // Both directions name the table they could not find, rather than failing later
    // as a planner or file error.
    #[tokio::test]
    async fn a_copy_of_an_unknown_table_is_a_table_error() {
        let handler = VaireDbQueryHandler::for_tests(false);
        for sql in [
            "COPY nowhere TO '/tmp/nowhere.csv' (FORMAT CSV)",
            "COPY nowhere FROM '/tmp/nowhere.csv' (FORMAT CSV)",
        ] {
            let (code, msg) = copy_rejection(&handler, sql).await;
            assert_eq!(code, "42P01", "`{sql}`");
            assert!(msg.contains("nowhere"), "`{sql}` got: {msg}");
        }
    }

    // The file is the coordinator's, so the message says whose filesystem was
    // looked at — a client hunting for it locally would not find it.
    #[tokio::test]
    async fn a_missing_import_file_is_reported_against_the_coordinator() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);

        let missing = std::env::temp_dir().join("vairedb_copy_does_not_exist.csv");
        let (_code, msg) = copy_rejection(
            &handler,
            &format!(
                "COPY orders FROM '{}' (FORMAT CSV, HEADER)",
                missing.display()
            ),
        )
        .await;
        assert!(msg.contains("coordinator"), "got: {msg}");
        assert!(msg.contains("vairedb_copy_does_not_exist"), "got: {msg}");
    }

    // A header naming a column the table does not have is refused by name: the
    // alternative is dropping the field, which loses data without saying so.
    #[tokio::test]
    async fn a_file_column_the_table_does_not_have_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("unknown_column", "id,nope\n1,x\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert_eq!(code, "42703");
        assert!(msg.contains("nope"), "got: {msg}");
    }

    // Every row is placed by hashing its shard key, so a file without that column
    // has nowhere to go. Refused before a single row is written.
    #[tokio::test]
    async fn an_import_without_the_shard_key_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("no_shard_key", "v\nx\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders (v) FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert_eq!(code, "42601");
        assert!(msg.contains("shard key"), "got: {msg}");
        assert!(msg.contains("\"id\""), "got: {msg}");
    }

    // A file wider than the columns it is being mapped onto would silently shift
    // every value one column left.
    #[tokio::test]
    async fn an_import_whose_width_does_not_match_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("too_wide", "1,x,extra\n");

        let (code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders (id, v) FROM '{path}' (FORMAT CSV)"),
        )
        .await;
        assert_eq!(code, "42601");
        assert!(msg.contains("2 column(s)"), "got: {msg}");
        assert!(msg.contains("3 field(s)"), "got: {msg}");
    }

    // The rows in the file are read, mapped and validated; what stops this import
    // is that the cluster has no shards to ship them to — reported as such, with
    // nothing written.
    #[tokio::test]
    async fn a_valid_import_gets_as_far_as_routing_the_rows() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders", &["id", "v"]);
        let path = write_csv("routable", "id,v\n1,x\n2,y\n");

        // No shards are assigned in a test handler, so routing is where it stops.
        let (_code, msg) = copy_rejection(
            &handler,
            &format!("COPY orders FROM '{path}' (FORMAT CSV, HEADER)"),
        )
        .await;
        assert!(
            !msg.contains("column") && !msg.contains("shard key"),
            "the file must have been read and mapped, got: {msg}"
        );
    }
}
