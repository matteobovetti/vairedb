//! DML (INSERT/UPDATE/DELETE) handling for the coordinator's pgwire handler.
//!
//! Resolves the target table's shard layout from the catalog, validates
//! shard-key constraints, then routes each write to the owning shard(s) and
//! executes it under quorum via the replication manager. Multi-row INSERTs whose
//! rows span shards are split so each shard receives only the rows it owns.

use std::collections::{BTreeMap, HashMap};

use crate::sqlparser::ast::{
    Expr, FromTable, ObjectName, SetExpr, Statement, TableFactor, TableObject, Value,
};
use datafusion::scalar::ScalarValue;
use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::{AnonymizationSecret, VdbErrorCode};

use crate::anonymization::{self, HMAC_SHA256_ALGO, Secret, SecretResolver};
use crate::catalog::{MetadataCatalog, ShardMeta, TableMeta};
use crate::error::CoordinatorError;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, enrich_generic_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router::{self, QueryType};
use crate::pgwire_handler::session::{BufferedWrite, SessionState, Transaction};
use crate::replication::BatchStatement;
use crate::util::insert_column_ident;
use crate::write_router::{compute_shard_index, shard_for_bucket};
use crate::write_sql_cl;

/// Schema and table identifiers of the system table that stores anonymization
/// secrets. Writes to it are intercepted by the coordinator and never routed to
/// a shard. Matched on the parsed identifiers (not a rendered string), so
/// quoting and case do not matter — `vairedb_catalog.anonymization_secret`,
/// `"vairedb_catalog"."anonymization_secret"`, and mixed case all resolve here.
const SECRET_TABLE_SCHEMA: &str = "vairedb_catalog";
const SECRET_TABLE_NAME: &str = "anonymization_secret";

/// Return `true` if `stmt` targets `vairedb_catalog.anonymization_secret`,
/// comparing the parsed identifier parts case-insensitively and ignoring quoting.
/// The table lives in the `vairedb_catalog` schema, so an unqualified bare name
/// is deliberately *not* matched (it could be a user table); this mirrors the
/// read path, which only routes `vairedb_catalog`-qualified names to the catalog.
fn targets_secret_table(stmt: &Statement) -> bool {
    let name = match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => Some(name),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => Some(name),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) => t,
                FromTable::WithoutKeyword(t) => t,
            };
            tables.first().and_then(|twj| match &twj.relation {
                TableFactor::Table { name, .. } => Some(name),
                _ => None,
            })
        }
        _ => None,
    };
    name.is_some_and(is_secret_table_name)
}

/// Whether `name` is the schema-qualified `vairedb_catalog.anonymization_secret`,
/// matched on the trailing (schema, table) identifier parts, case-insensitively.
fn is_secret_table_name(name: &ObjectName) -> bool {
    let idents: Vec<&str> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|i| i.value.as_str()))
        .collect();
    matches!(
        idents.as_slice(),
        [.., schema, table]
            if schema.eq_ignore_ascii_case(SECRET_TABLE_SCHEMA)
                && table.eq_ignore_ascii_case(SECRET_TABLE_NAME)
    )
}

/// A [`SecretResolver`] backed by the metadata catalog, so the pure
/// anonymization rewriter can look up secrets without knowing about redb.
struct CatalogSecretResolver<'a> {
    catalog: &'a MetadataCatalog,
}

impl SecretResolver for CatalogSecretResolver<'_> {
    fn resolve(&self, secret_id: &str) -> Option<Secret> {
        self.catalog
            .get_anonymization_secret(secret_id)
            .ok()
            .flatten()
            .map(|s| Secret {
                algo: s.algo,
                secret_key: s.secret_key,
            })
    }
}

/// A shard-local write ready to ship: the shard it applies to (which also names
/// the nodes it must reach), the rewritten statement, and the quorum its table
/// requires.
pub(super) struct PlannedWrite {
    pub(super) shard: ShardMeta,
    pub(super) statement: BatchStatement,
    pub(super) quorum_size: usize,
}

/// A DML statement resolved to the shard-local writes that apply it.
///
/// Planning is separate from execution so a statement inside a transaction block
/// can be planned now and shipped at `COMMIT`: see
/// [`crate::pgwire_handler::session`].
pub(super) struct DmlPlan {
    pub(super) writes: Vec<PlannedWrite>,
    /// Logical table the statement targets.
    pub(super) table_name: String,
    /// Rows the statement affects, when the statement text alone settles it (an
    /// `INSERT ... VALUES`). `None` when only the shards can report the count,
    /// which is why an UPDATE/DELETE cannot be deferred to `COMMIT`.
    pub(super) static_row_count: Option<u64>,
    /// Error context for the table, reused when shipping the plan.
    pub(super) ctx: ErrorContext,
}

impl VaireDbQueryHandler {
    /// Route a single INSERT/UPDATE/DELETE, returning the pgwire command tag with
    /// the rows-affected count. Resolves the target table, enforces shard-key
    /// rules (INSERTs must specify the shard key; UPDATEs may not mutate it), and
    /// dispatches to the owning shards under quorum. Returns an error if the table
    /// is unknown or a shard-key constraint is violated.
    ///
    /// Inside a transaction block the write is planned but **not** shipped: it is
    /// buffered on the session and applied at `COMMIT`. Which statements may be
    /// buffered is decided beforehand by
    /// [`VaireDbQueryHandler::check_transaction_allows`].
    pub(super) async fn handle_dml(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
        session: &SessionState,
    ) -> PgWireResult<Response> {
        // Writes to the anonymization-secret system table are handled in the
        // coordinator's catalog, never routed to a shard.
        if targets_secret_table(stmt) {
            if session.transaction().await.is_open() {
                return Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    "writing an anonymization secret inside a transaction block is not supported: the secret is stored in the coordinator catalog rather than on a shard, so ROLLBACK could not undo it",
                ));
            }
            return self.handle_anonymization_secret_insert(stmt, query_type);
        }

        // An INSERT whose rows come from a query carries no shard key to hash, so
        // it is run on the read path first and re-emitted as literal rows.
        if write_sql_cl::insert_source_is_query(stmt) {
            return self
                .handle_insert_from_query(stmt, query_type, params, session)
                .await;
        }

        let plan = self.plan_dml(stmt, query_type, params)?;

        let mut txn = session.transaction().await;
        if txn.is_open() {
            return buffer_plan(&mut txn, plan, query_type);
        }
        drop(txn);

        let total_rows = self.execute_plan(&plan).await?;
        Ok(Response::Execution(dml_tag(query_type, total_rows)))
    }

    /// Route an INSERT whose rows come from a query (`INSERT INTO t SELECT …`, a
    /// `UNION`, a CTE): run the source on the read path, then write its rows back
    /// as literal `INSERT ... VALUES` statements the router can place.
    ///
    /// The source is read to completion *before* anything is written, so
    /// `INSERT INTO t SELECT * FROM t` reads the table's committed contents and
    /// terminates. The rows then go through the ordinary INSERT lane — shard-key
    /// validation, the anonymization rewrite, the per-shard split — because by
    /// then they *are* an ordinary multi-row INSERT.
    ///
    /// Not atomic: the rows are shipped in chunks, each chunk to the shards it
    /// belongs on. A failure part-way through leaves the earlier chunks written
    /// and says so ([`VdbErrorCode::PartialCommit`]) rather than reporting a
    /// rollback that did not happen.
    async fn handle_insert_from_query(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let Statement::Insert(insert) = stmt else {
            return Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "expected an INSERT statement",
            ));
        };
        // The rows come back from every shard the INSERT touched, in whatever
        // order they were shipped; the coordinator has no row description to send
        // them under on this path, and inventing one is worse than saying no.
        if insert.returning.is_some() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "RETURNING is not supported on an INSERT whose rows come from a query: the rows are written to several shards and VaireDB cannot return them as one result set. Run the SELECT separately",
            ));
        }
        let Some(source) = insert.source.as_ref() else {
            return Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "expected an INSERT with a query source",
            ));
        };

        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine target table",
            )
        })?;
        let dml_ctx = ErrorContext::for_table(&table_name);
        let table_meta = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &dml_ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(table_name.clone());
                enrich_coordinator_error(&err, &dml_ctx, &self.catalog)
            })?;

        self.reject_hashing_a_digest(
            "INSERT ... SELECT",
            &table_name,
            &table_meta.anonymized_columns,
            &write_sql_cl::relations_read(source.as_ref()),
        )?;

        let source_query = Statement::Query(source.clone());
        let (schema, batches) = self.collect_query_rows(&source_query, params).await?;

        // PostgreSQL matches an implicit column list to the table's leading
        // columns; here the count comes from the source query's result schema,
        // which is known even when the query returned no rows — so a mismatched
        // `INSERT ... SELECT` is reported as one either way.
        let mut template = stmt.clone();
        let table_columns: Vec<&str> = table_meta.columns.iter().map(|c| c.name.as_str()).collect();
        write_sql_cl::materialize_insert_columns_for_arity(
            &mut template,
            &table_columns,
            schema.fields().len(),
        )
        .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;

        let target_columns = match &template {
            Statement::Insert(insert) => insert.columns.len(),
            _ => 0,
        };
        if target_columns == 0 {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!(
                    "INSERT must specify an explicit column list including shard key \"{}\"",
                    table_meta.shard_key
                ),
            ));
        }
        if target_columns != schema.fields().len() {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!(
                    "INSERT has {target_columns} target column(s) but the source query produces {}",
                    schema.fields().len()
                ),
            ));
        }

        let statements = write_sql_cl::insert_statements_from_batches(
            &template,
            &batches,
            write_sql_cl::ROWS_PER_STATEMENT,
        )
        .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;

        let rows = self
            .write_row_statements(&statements, params, session, &table_name, "INSERT")
            .await?;
        Ok(Response::Execution(dml_tag(query_type, rows)))
    }

    /// Refuse a row copy that would hash an already-hashed value.
    ///
    /// A pseudonymized column stores an HMAC digest and there is no way back, so a
    /// read of one yields the digest. A write whose rows come from a table that has
    /// pseudonymized columns, landing in a table that pseudonymizes too, therefore
    /// stores the digest *of a digest* — and the read path hashes a literal exactly
    /// once when it compares, so no lookup on the destination can ever match. Nothing
    /// about the write fails, which is precisely why it has to be refused here.
    ///
    /// Deliberately coarse: it asks which tables the source reads, not which of their
    /// columns reach which destination column. A source that projects only
    /// non-pseudonymized columns is refused too — the message says what to do instead,
    /// and over-refusing is the safe direction when the alternative is storing values
    /// that silently match nothing.
    pub(super) fn reject_hashing_a_digest(
        &self,
        command: &str,
        destination: &str,
        destination_anonymized: &HashMap<String, String>,
        source_tables: &[String],
    ) -> PgWireResult<()> {
        if destination_anonymized.is_empty() {
            return Ok(());
        }
        for source in source_tables {
            let Ok(Some(meta)) = self.catalog.get_table(source) else {
                continue;
            };
            if meta.anonymized_columns.is_empty() {
                continue;
            }
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "{command} is not supported by VaireDB when \"{destination}\" and the source \
                     table \"{source}\" both have pseudonymized columns: reading \"{source}\" \
                     yields HMAC digests, which \"{destination}\" would hash a second time, and \
                     the result would match no lookup. Create the destination without \
                     anonymized_columns to store the digests as they are, or load it from the \
                     original plaintext"
                ),
            ));
        }
        Ok(())
    }

    /// Ship the literal `INSERT ... VALUES` statements a materialized row source
    /// was rendered into, returning the number of rows written.
    ///
    /// Every chunk is planned before any is shipped, so a set of rows that cannot
    /// be routed — an unroutable shard key in some row, an `ON CONFLICT` no shard
    /// can decide — is refused with nothing written. Inside a transaction block the
    /// chunks are buffered instead, which is exact: the rows are literals by now,
    /// so their count is known without asking a shard.
    ///
    /// `params` carries over unchanged even though the rows are literals: a
    /// placeholder that lived outside the row source — `ON CONFLICT ... DO UPDATE
    /// SET v = $2` — survives into each chunk under its original number, which is
    /// what indexes this list.
    ///
    /// `command` names the client's statement (`INSERT`, `COPY`) in the
    /// partial-failure message. Shipping is not atomic across chunks or shards: a
    /// failure after the first chunk leaves the earlier rows written and says so
    /// with [`VdbErrorCode::PartialCommit`] rather than reporting a rollback that
    /// did not happen.
    pub(super) async fn write_row_statements(
        &self,
        statements: &[Statement],
        params: &[ScalarValue],
        session: &SessionState,
        table_name: &str,
        command: &str,
    ) -> PgWireResult<u64> {
        let plans = statements
            .iter()
            .map(|chunk| self.plan_dml(chunk, &QueryType::Insert, params))
            .collect::<PgWireResult<Vec<DmlPlan>>>()?;

        let mut txn = session.transaction().await;
        if txn.is_open() {
            let mut rows = 0u64;
            for plan in plans {
                rows += buffer_plan_rows(&mut txn, plan)?;
            }
            return Ok(rows);
        }
        drop(txn);

        let mut rows = 0u64;
        for (idx, plan) in plans.iter().enumerate() {
            match self.execute_plan(plan).await {
                Ok(chunk_rows) => rows += chunk_rows,
                // The first chunk failing is the ordinary multi-shard INSERT
                // failure the client already gets from a `VALUES` write of the
                // same shape; a later one means rows are already stored.
                Err(e) if idx > 0 => {
                    return Err(make_vdb_error(
                        VdbErrorCode::PartialCommit,
                        format!(
                            "{command} partially applied: {rows} row(s) were written to \"{table_name}\" and cannot be undone, then the write failed. Inspect the table before retrying. Cause: {e}"
                        ),
                    ));
                }
                Err(e) => return Err(e),
            }
        }

        Ok(rows)
    }

    /// Resolve a DML statement into the shard-local writes that apply it, without
    /// executing any of them. Enforces every shard-key rule and applies the
    /// anonymization rewrite, so a plan is safe to ship as-is.
    ///
    /// The caller must have ruled out the anonymization-secret table, whose writes
    /// land in the coordinator's catalog rather than on a shard.
    pub(super) fn plan_dml(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
    ) -> PgWireResult<DmlPlan> {
        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine target table",
            )
        })?;

        let dml_ctx = ErrorContext::for_table(&table_name);

        let table_meta = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &dml_ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(table_name.clone());
                enrich_coordinator_error(&err, &dml_ctx, &self.catalog)
            })?;

        // PostgreSQL matches a positional `INSERT INTO t VALUES (…)` to the
        // table's declared column order. Resolve that list into the statement
        // first, because everything after this point locates columns *by name*:
        // the shard-key check, the `ON CONFLICT` arbiter check, the anonymization
        // rewrite, and the per-shard row split. With no list they all find
        // nothing, which would broadcast the row to every shard and ship an
        // anonymized column as plaintext.
        let materialized;
        let stmt: &Statement = if write_sql_cl::insert_omits_column_list(stmt) {
            let columns: Vec<&str> = table_meta.columns.iter().map(|c| c.name.as_str()).collect();
            let mut owned = stmt.clone();
            write_sql_cl::materialize_insert_columns(&mut owned, &columns)
                .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;
            materialized = owned;
            &materialized
        } else {
            stmt
        };

        // Pseudonymize any anonymized column before routing: replace plaintext
        // with the HMAC-SHA256 digest so no plaintext ever leaves the coordinator.
        // Owned only when a rewrite is actually needed, to avoid cloning hot-path
        // statements for the common (non-anonymized) case.
        let anonymized;
        let stmt: &Statement = if table_meta.anonymized_columns.is_empty() {
            stmt
        } else {
            let resolver = CatalogSecretResolver {
                catalog: &self.catalog,
            };
            let mut owned = stmt.clone();
            anonymization::anonymize_statement(
                &mut owned,
                &table_meta.anonymized_columns,
                &resolver,
            )
            .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;
            anonymized = owned;
            &anonymized
        };

        match query_type {
            QueryType::Insert => {
                write_sql_cl::validate_insert_shard_key(stmt, &table_meta.shard_key, params)
                    .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;
                // A conflict is only ever detected within one shard, so an
                // `ON CONFLICT` arbiter that does not include the shard key would
                // upsert per shard and duplicate globally.
                write_sql_cl::validate_on_conflict(stmt, &table_meta.shard_key)
                    .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;
            }
            QueryType::Update
                if write_sql_cl::update_targets_shard_key(stmt, &table_meta.shard_key) =>
            {
                return Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    format!(
                        "cannot UPDATE shard key column \"{}\"; relocating a row to a different shard is not supported in v0.1",
                        table_meta.shard_key
                    ),
                ));
            }
            _ => {}
        }

        let quorum_size = self
            .write_router
            .compute_quorum_size(table_meta.replication_factor);
        let dml_ctx = dml_ctx.with_replication(table_meta.replication_factor);

        let writes = if *query_type == QueryType::Insert {
            self.plan_insert_with_split(stmt, &table_meta, quorum_size, params, &dml_ctx)?
        } else {
            let target_shards = self
                .write_router
                .resolve_target_shards(stmt, &table_meta, params)
                .map_err(|e| enrich_coordinator_error(&e, &dml_ctx, &self.catalog))?;

            self.plan_writes_on_each_shard(&target_shards, stmt, params, quorum_size, &dml_ctx)?
        };

        Ok(DmlPlan {
            writes,
            table_name,
            static_row_count: write_sql_cl::insert_values_row_count(stmt).map(|n| n as u64),
            ctx: dml_ctx,
        })
    }

    /// Ship every write of `plan` under quorum and return the total rows affected.
    /// Each write gets its own id, suffixed by position, so a node can dedup a
    /// retried statement without conflating it with its siblings.
    pub(super) async fn execute_plan(&self, plan: &DmlPlan) -> PgWireResult<u64> {
        let write_id = uuid::Uuid::new_v4().to_string();

        let mut total_rows = 0u64;
        for (idx, write) in plan.writes.iter().enumerate() {
            let shard_write_id = format!("{}-{}", write_id, idx);
            total_rows += self
                .replication_manager
                .execute_write_with_quorum(
                    &write.shard,
                    &write.statement.sql,
                    &write.statement.params,
                    &shard_write_id,
                    write.quorum_size,
                )
                .await
                .map_err(|e| enrich_coordinator_error(&e, &plan.ctx, &self.catalog))?;
        }

        Ok(total_rows)
    }

    /// Handle an `INSERT INTO vairedb_catalog.anonymization_secret (...)`: parse
    /// the row(s), validate the algorithm, and store each secret in the catalog.
    /// The secret never leaves the coordinator, so this is not routed to a shard.
    /// Only INSERT is supported; UPDATE/DELETE on the secret table are rejected.
    fn handle_anonymization_secret_insert(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
    ) -> PgWireResult<Response> {
        if *query_type != QueryType::Insert {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "only INSERT is supported on vairedb_catalog.anonymization_secret",
            ));
        }

        let secrets = parse_anonymization_secret_insert(stmt)?;
        let count = secrets.len();
        for secret in secrets {
            self.catalog
                .put_anonymization_secret(&secret)
                .map_err(|e| {
                    enrich_coordinator_error(&e, &ErrorContext::default(), &self.catalog)
                })?;
        }

        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(count),
        ))
    }

    /// Plan an INSERT, splitting a multi-row statement across shards when its
    /// rows hash to different buckets so each shard receives only the rows it
    /// owns. Single-row or single-shard INSERTs route whole via the shared
    /// resolver. Returns an error if the table has no shards or the statement
    /// cannot be split.
    fn plan_insert_with_split(
        &self,
        stmt: &Statement,
        table_meta: &TableMeta,
        quorum_size: usize,
        params: &[ScalarValue],
        insert_ctx: &ErrorContext,
    ) -> PgWireResult<Vec<PlannedWrite>> {
        let all_shards = self
            .catalog
            .get_shards_for_table(&table_meta.table_name)
            .map_err(|e| enrich_coordinator_error(&e, insert_ctx, &self.catalog))?;

        if all_shards.is_empty() {
            let err = CoordinatorError::ShardNotAssigned(format!(
                "no shards for table {}",
                table_meta.table_name
            ));
            return Err(enrich_coordinator_error(&err, insert_ctx, &self.catalog));
        }

        let row_keys =
            write_sql_cl::extract_insert_row_shard_keys(stmt, &table_meta.shard_key, params);

        // A multi-row INSERT whose rows hash to different shards must be split:
        // each shard receives only the rows it owns. A single-row (or single-shard)
        // INSERT routes whole via the shared resolver.
        match row_keys {
            Some(keys) if keys.len() > 1 => {
                // Keyed and iterated by hash bucket, not by position in
                // `all_shards`: the bucket is what names the physical table the
                // rows land in. A `BTreeMap` so the shards are planned in bucket
                // order, which makes the plan reproducible for one statement.
                let mut shard_rows: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
                for (row_idx, key_value) in &keys {
                    let bucket = compute_shard_index(key_value, all_shards.len());
                    shard_rows.entry(bucket).or_default().push(*row_idx);
                }

                let mut writes = Vec::with_capacity(shard_rows.len());
                for (bucket, row_indices) in &shard_rows {
                    let shard = shard_for_bucket(&all_shards, *bucket, &table_meta.table_name)
                        .map_err(|e| enrich_coordinator_error(&e, insert_ctx, &self.catalog))?;
                    let split_stmt = write_sql_cl::split_insert_by_rows(stmt, row_indices)
                        .ok_or_else(|| {
                            enrich_generic_error(&"failed to split INSERT by shard", insert_ctx)
                        })?;
                    writes.push(self.plan_write(
                        shard,
                        &split_stmt,
                        params,
                        quorum_size,
                        insert_ctx,
                    )?);
                }
                Ok(writes)
            }
            _ => {
                let target_shards = self
                    .write_router
                    .resolve_target_shards(stmt, table_meta, params)
                    .map_err(|e| enrich_coordinator_error(&e, insert_ctx, &self.catalog))?;

                self.plan_writes_on_each_shard(
                    &target_shards,
                    stmt,
                    params,
                    quorum_size,
                    insert_ctx,
                )
            }
        }
    }

    /// Plan `stmt` for every shard in `shards` — the broadcast form, used when a
    /// write cannot be narrowed to one shard.
    pub(super) fn plan_writes_on_each_shard(
        &self,
        shards: &[ShardMeta],
        stmt: &Statement,
        params: &[ScalarValue],
        quorum_size: usize,
        ctx: &ErrorContext,
    ) -> PgWireResult<Vec<PlannedWrite>> {
        shards
            .iter()
            .map(|shard| self.plan_write(shard, stmt, params, quorum_size, ctx))
            .collect()
    }

    /// Rewrite `stmt` to its shard-local form for one shard. The single choke
    /// point through which every sharded write — split INSERT, broadcast
    /// UPDATE/DELETE, split MERGE, or single-shard route — passes on its way to a
    /// node.
    pub(super) fn plan_write(
        &self,
        shard: &ShardMeta,
        stmt: &Statement,
        params: &[ScalarValue],
        quorum_size: usize,
        ctx: &ErrorContext,
    ) -> PgWireResult<PlannedWrite> {
        let (shard_sql, shard_params) = self
            .write_router
            .generate_shard_local_sql(stmt, shard, params)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?;

        Ok(PlannedWrite {
            statement: BatchStatement {
                sql: shard_sql,
                params: shard_params,
                shard_id: crate::util::shard_table_name(&shard.table_name, shard.hash_bucket),
            },
            shard: shard.clone(),
            quorum_size,
        })
    }
}

/// Buffer a planned write on the open transaction block instead of shipping it,
/// reporting the rows it will affect.
///
/// The count must be exact, not optimistic: the client is told now what `COMMIT`
/// will do later, and an ORM may branch on it. Only an `INSERT ... VALUES` with no
/// clause that can drop or return rows qualifies (see
/// [`write_sql_cl::insert_values_row_count`]); anything else is refused rather
/// than guessed at. `check_transaction_allows` already turns away the statement
/// kinds that can never qualify, so this rejection is the backstop for the rest
/// (`INSERT ... SELECT`, `ON CONFLICT`, `RETURNING`).
fn buffer_plan(
    txn: &mut Transaction,
    plan: DmlPlan,
    query_type: &QueryType,
) -> PgWireResult<Response> {
    let rows = buffer_plan_rows(txn, plan)?;
    Ok(Response::Execution(dml_tag(query_type, rows)))
}

/// [`buffer_plan`], returning the rows buffered instead of a response, so a
/// statement shipped as several plans (an `INSERT ... SELECT` chunked by row) can
/// report their total.
fn buffer_plan_rows(txn: &mut Transaction, plan: DmlPlan) -> PgWireResult<u64> {
    let Some(rows) = plan.static_row_count else {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "this statement cannot be run inside a transaction block: VaireDB buffers a block's writes until COMMIT, and the rows this one affects are only known once the shards run it. Only an INSERT ... VALUES (without ON CONFLICT or RETURNING) can be buffered",
        ));
    };

    for write in plan.writes {
        txn.push_write(BufferedWrite {
            shard: write.shard,
            statement: write.statement,
            quorum_size: write.quorum_size,
            table_name: plan.table_name.clone(),
        });
    }

    Ok(rows)
}

/// The pgwire command tag a DML statement completes with, carrying the rows it
/// affected.
pub(super) fn dml_tag(query_type: &QueryType, rows: u64) -> Tag {
    match query_type {
        QueryType::Insert => Tag::new("INSERT").with_oid(0).with_rows(rows as usize),
        QueryType::Update => Tag::new("UPDATE").with_rows(rows as usize),
        QueryType::Delete => Tag::new("DELETE").with_rows(rows as usize),
        QueryType::Merge => Tag::new("MERGE").with_rows(rows as usize),
        _ => Tag::new("OK"),
    }
}

/// Parse `INSERT INTO vairedb_catalog.anonymization_secret (...) VALUES (...)`
/// into one [`AnonymizationSecret`] per row. Requires an explicit column list
/// naming `id`, `algo`, and `secret_key`, string-literal values, and the only
/// supported algorithm. Returns a client-facing error otherwise.
fn parse_anonymization_secret_insert(stmt: &Statement) -> PgWireResult<Vec<AnonymizationSecret>> {
    let Statement::Insert(insert) = stmt else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected INSERT INTO vairedb_catalog.anonymization_secret",
        ));
    };

    // A column-list entry that is not a bare identifier (a dotted composite-field
    // target) cannot name one of the three required columns, so it maps to the empty
    // string and `column_index` rejects the statement below.
    let columns: Vec<&str> = insert
        .columns
        .iter()
        .map(|c| insert_column_ident(c).map_or("", |i| i.value.as_str()))
        .collect();
    let id_idx = column_index(&columns, "id")?;
    let algo_idx = column_index(&columns, "algo")?;
    let secret_idx = column_index(&columns, "secret_key")?;

    let source = insert.source.as_ref().ok_or_else(|| {
        make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "INSERT INTO vairedb_catalog.anonymization_secret must specify VALUES",
        )
    })?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "only INSERT ... VALUES is supported for vairedb_catalog.anonymization_secret",
        ));
    };

    let mut secrets = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let id = string_literal_at(row, id_idx, "id")?;
        let algo = string_literal_at(row, algo_idx, "algo")?;
        let secret_key = string_literal_at(row, secret_idx, "secret_key")?;

        if algo != HMAC_SHA256_ALGO {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "unsupported anonymization algorithm '{algo}'; only {HMAC_SHA256_ALGO} is supported"
                ),
            ));
        }

        secrets.push(AnonymizationSecret {
            id,
            algo,
            secret_key,
        });
    }

    Ok(secrets)
}

/// Position of the required column `name` in the INSERT column list, or a
/// client-facing error if it is absent.
fn column_index(columns: &[&str], name: &str) -> PgWireResult<usize> {
    columns.iter().position(|c| *c == name).ok_or_else(|| {
        make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!(
                "INSERT INTO vairedb_catalog.anonymization_secret must include column \"{name}\""
            ),
        )
    })
}

/// Extract the string-literal value at `idx` in a VALUES row, or a client-facing
/// error if the position is missing or not a string literal.
fn string_literal_at(row: &[Expr], idx: usize, col: &str) -> PgWireResult<String> {
    match row.get(idx) {
        Some(Expr::Value(v)) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
            _ => Err(make_vdb_error(
                VdbErrorCode::TypeMismatch,
                format!("column \"{col}\" must be a string literal"),
            )),
        },
        _ => Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!("missing value for column \"{col}\""),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pgwire::error::PgWireError;

    fn parse_one(sql: &str) -> Statement {
        crate::pgwire_handler::parser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    /// The (SQLSTATE, message) a client would receive.
    fn reported(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code, info.message),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// Route a DML statement as the handler does, with no parameters bound.
    async fn dml(handler: &VaireDbQueryHandler, sql: &str) -> PgWireResult<Response> {
        let stmt = parse_one(sql);
        let query_type = query_router::classify_statement(&stmt);
        let session = SessionState::default();
        handler.handle_dml(&stmt, &query_type, &[], &session).await
    }

    #[test]
    fn secret_table_matched_qualified() {
        for sql in [
            "INSERT INTO vairedb_catalog.anonymization_secret (id, algo, secret_key) VALUES ('a', 'HMAC-SHA256', 'k')",
            "UPDATE vairedb_catalog.anonymization_secret SET secret_key = 'k' WHERE id = 'a'",
            "DELETE FROM vairedb_catalog.anonymization_secret WHERE id = 'a'",
        ] {
            assert!(targets_secret_table(&parse_one(sql)), "should match: {sql}");
        }
    }

    #[test]
    fn secret_table_matched_when_quoted() {
        let stmt = parse_one(
            "INSERT INTO \"vairedb_catalog\".\"anonymization_secret\" (id, algo, secret_key) VALUES ('a', 'HMAC-SHA256', 'k')",
        );
        assert!(targets_secret_table(&stmt));
    }

    #[test]
    fn secret_table_matched_case_insensitively() {
        let stmt = parse_one(
            "INSERT INTO VaireDB_Catalog.Anonymization_Secret (id, algo, secret_key) VALUES ('a', 'HMAC-SHA256', 'k')",
        );
        assert!(targets_secret_table(&stmt));
    }

    #[test]
    fn bare_unqualified_name_is_not_matched() {
        // The table lives in the `vairedb_catalog` schema; an unqualified
        // `anonymization_secret` could be a user table, so it is not intercepted
        // (mirrors the read path, which only routes schema-qualified names).
        let stmt = parse_one(
            "INSERT INTO anonymization_secret (id, algo, secret_key) VALUES ('a', 'HMAC-SHA256', 'k')",
        );
        assert!(!targets_secret_table(&stmt));
    }

    #[test]
    fn other_catalog_and_user_tables_are_not_matched() {
        for sql in [
            "INSERT INTO vairedb_catalog.tables (x) VALUES (1)",
            "INSERT INTO public.anonymization_secret (x) VALUES (1)",
            "INSERT INTO foo_table (id) VALUES (1)",
            "DELETE FROM other_schema.anonymization_secret WHERE id = 1",
        ] {
            assert!(
                !targets_secret_table(&parse_one(sql)),
                "should not match: {sql}"
            );
        }
    }

    // --- INSERT whose rows come from a query ---

    /// Register a table the coordinator can resolve, with `id` as its shard key.
    fn register_table(handler: &VaireDbQueryHandler, name: &str, columns: &[&str]) {
        handler
            .catalog
            .put_table(&TableMeta {
                table_name: name.to_string(),
                columns: columns
                    .iter()
                    .map(|c| vairedb_common::proto::vairedb::v1::ColumnDef {
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

    // The rows come back from every shard the INSERT touched, so there is no one
    // result set to return them in. Refused before the source query is even run:
    // nothing is read and nothing is written.
    #[tokio::test]
    async fn returning_is_refused_on_an_insert_from_a_query() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, message) = reported(
            dml(
                &handler,
                "INSERT INTO orders (id) SELECT id FROM staging RETURNING id",
            )
            .await
            .unwrap_err(),
        );
        assert_eq!(code, "0A000");
        assert!(message.contains("RETURNING"), "got: {message}");
    }

    // The target is resolved before the source runs, so an unknown target fails on
    // the catalog rather than after a read the client cannot see the result of.
    #[tokio::test]
    async fn an_unknown_target_table_is_reported_before_the_source_runs() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, message) = reported(
            dml(&handler, "INSERT INTO nowhere (id) SELECT 1")
                .await
                .unwrap_err(),
        );
        assert_eq!(code, "42P01");
        assert!(message.contains("nowhere"), "got: {message}");
    }

    // The source query's result schema settles the arity before a row is fetched,
    // so a mismatch is reported without writing anything — whichever side is wider.
    #[tokio::test]
    async fn a_source_query_of_the_wrong_width_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_table(&handler, "orders", &["id", "amount"]);

        // Wider than the table: no positional mapping exists at all.
        let (code, message) = reported(
            dml(&handler, "INSERT INTO orders SELECT 1, 2, 3")
                .await
                .unwrap_err(),
        );
        assert_eq!(code, "42601");
        assert!(
            message.contains("more expressions than target columns"),
            "got: {message}"
        );

        // Wider than an explicit column list: writing the columns that do line up
        // would store a row the source query did not produce.
        let (code, message) = reported(
            dml(&handler, "INSERT INTO orders (id) SELECT 1, 2")
                .await
                .unwrap_err(),
        );
        assert_eq!(code, "42601");
        assert!(
            message.contains("1 target column(s) but the source query produces 2"),
            "got: {message}"
        );
    }

    /// Register a table whose `email` column is pseudonymized, so reads of it yield
    /// digests rather than plaintext.
    fn register_anonymized_table(handler: &VaireDbQueryHandler, name: &str) {
        let mut meta = TableMeta {
            table_name: name.to_string(),
            columns: ["id", "email"]
                .iter()
                .map(|c| vairedb_common::proto::vairedb::v1::ColumnDef {
                    name: c.to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                })
                .collect(),
            shard_key: "id".to_string(),
            shard_count: 2,
            replication_factor: 1,
            ..Default::default()
        };
        meta.anonymized_columns
            .insert("email".to_string(), "secret1".to_string());
        handler.catalog.put_table(&meta).unwrap();
    }

    // Copying between two pseudonymizing tables would store the digest of a digest,
    // and a lookup on the destination hashes its literal once — so the row would be
    // there and match nothing. Refused before the source query runs.
    #[tokio::test]
    async fn copying_digests_into_a_table_that_hashes_them_again_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_anonymized_table(&handler, "dst");
        register_anonymized_table(&handler, "src");

        let (code, message) = reported(
            dml(
                &handler,
                "INSERT INTO dst (id, email) SELECT id, email FROM src",
            )
            .await
            .unwrap_err(),
        );
        assert_eq!(code, "0A000");
        assert!(
            message.contains("dst") && message.contains("src"),
            "both tables should be named: {message}"
        );
        assert!(
            message.contains("hash"),
            "the reason should be stated: {message}"
        );
    }

    // Same shape, one table: `INSERT INTO t SELECT ... FROM t` re-hashes its own
    // digests, which is the same defect and must not slip through as a self-copy.
    #[tokio::test]
    async fn a_pseudonymizing_table_copying_from_itself_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_anonymized_table(&handler, "people");

        let (code, _) = reported(
            dml(
                &handler,
                "INSERT INTO people (id, email) SELECT id, email FROM people",
            )
            .await
            .unwrap_err(),
        );
        assert_eq!(code, "0A000");
    }

    // The guard is about digests meeting a second hash, so it must not fire when only
    // one side pseudonymizes: plaintext into a hashing table is the ordinary bulk-load
    // case, and digests into a plain table copies them as they are.
    #[tokio::test]
    async fn a_copy_with_only_one_pseudonymizing_side_is_allowed_through() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_anonymized_table(&handler, "hashed");
        register_table(&handler, "plain", &["id", "email"]);

        for sql in [
            "INSERT INTO hashed (id, email) SELECT id, email FROM plain",
            "INSERT INTO plain (id, email) SELECT id, email FROM hashed",
        ] {
            let (code, message) = reported(dml(&handler, sql).await.unwrap_err());
            // Both get as far as needing a cluster to write to, which is where a
            // handler with no live nodes stops — the guard did not refuse them.
            assert_ne!(code, "0A000", "`{sql}` should not be refused: {message}");
        }
    }

    // --- splitting a multi-row INSERT across shards ---

    /// Eleven shards: the smallest layout in which a shard's position in the
    /// catalog's list is not its hash bucket, because the records are keyed by the
    /// string `"{table}:shard{n}"` and `shard10` sorts before `shard2`.
    const WIDE_SHARDS: u32 = 11;

    /// Register a table sharded by `id` over `shard_count` shards, with the shard
    /// records a write is routed on.
    fn register_sharded_table(
        handler: &VaireDbQueryHandler,
        name: &str,
        shard_count: u32,
    ) -> TableMeta {
        let meta = TableMeta {
            table_name: name.to_string(),
            columns: vec![vairedb_common::proto::vairedb::v1::ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_expr: String::new(),
            }],
            shard_key: "id".to_string(),
            shard_count,
            replication_factor: 1,
            ..Default::default()
        };
        handler.catalog.put_table(&meta).unwrap();
        for bucket in 0..shard_count {
            handler
                .catalog
                .put_shard(&ShardMeta {
                    shard_id: crate::util::logical_shard_id(bucket),
                    table_name: name.to_string(),
                    primary_node_id: "node-0".to_string(),
                    replica_node_ids: Vec::new(),
                    hash_bucket: bucket,
                    range_lower: String::new(),
                    range_upper: String::new(),
                })
                .unwrap();
        }
        meta
    }

    // Each row must be written to the shard its key hashes to. Resolving the shard
    // by its position in the catalog's list instead of by its bucket sends rows to
    // the wrong shard from eleven shards on, and nothing fails while it happens:
    // every shard can execute the statement.
    #[tokio::test]
    async fn a_split_insert_sends_each_row_to_the_shard_that_owns_its_key() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let meta = register_sharded_table(&handler, "orders", WIDE_SHARDS);

        // Enough ids to reach every bucket, including those whose bucket and
        // position disagree.
        let ids: Vec<i64> = (1..=40).collect();
        let values = ids
            .iter()
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        let stmt = parse_one(&format!("INSERT INTO orders (id) VALUES {values}"));

        let writes = handler
            .plan_insert_with_split(&stmt, &meta, 1, &[], &ErrorContext::default())
            .unwrap();

        let mut planned: Vec<i64> = Vec::new();
        for write in &writes {
            let bucket = write.shard.hash_bucket;
            assert_eq!(
                write.statement.shard_id,
                format!("orders_shard{bucket}"),
                "the write must name the shard it is planned for"
            );
            assert!(
                write
                    .statement
                    .sql
                    .contains(&format!("orders_shard{bucket}")),
                "got: {}",
                write.statement.sql
            );
            for id in &ids {
                let owned =
                    compute_shard_index(&id.to_string(), WIDE_SHARDS as usize) == bucket as usize;
                let present = write.statement.sql.contains(&format!("({id})"));
                assert_eq!(
                    present,
                    owned,
                    "id {id} {} on the shard for bucket {bucket}: {}",
                    if present { "is" } else { "is not" },
                    write.statement.sql
                );
                if present {
                    planned.push(*id);
                }
            }
        }

        planned.sort_unstable();
        assert_eq!(planned, ids, "every row must be planned exactly once");
    }

    // A layout missing the bucket a row hashes to is reported, not routed around:
    // any other shard would be a shard that does not own the row.
    #[tokio::test]
    async fn a_split_insert_reports_a_bucket_with_no_shard() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let meta = register_sharded_table(&handler, "orders", WIDE_SHARDS);

        // Drop one shard record, then insert a row that hashes to it (plus one that
        // does not, so the statement is a multi-row split).
        let orphan = 3u32;
        handler.catalog.delete_shards_for_table("orders").unwrap();
        for bucket in (0..WIDE_SHARDS).filter(|bucket| *bucket != orphan) {
            handler
                .catalog
                .put_shard(&ShardMeta {
                    shard_id: crate::util::logical_shard_id(bucket),
                    table_name: "orders".to_string(),
                    primary_node_id: "node-0".to_string(),
                    replica_node_ids: Vec::new(),
                    hash_bucket: bucket,
                    range_lower: String::new(),
                    range_upper: String::new(),
                })
                .unwrap();
        }

        // `compute_shard_index` divides by the number of records, so the ids are
        // chosen against the layout as it now stands.
        let shard_count = (WIDE_SHARDS - 1) as usize;
        let orphaned_id = (1i64..)
            .find(|id| compute_shard_index(&id.to_string(), shard_count) == orphan as usize)
            .unwrap();
        let other_id = (1i64..)
            .find(|id| compute_shard_index(&id.to_string(), shard_count) != orphan as usize)
            .unwrap();
        let stmt = parse_one(&format!(
            "INSERT INTO orders (id) VALUES ({orphaned_id}), ({other_id})"
        ));

        let (code, message) = reported(
            handler
                .plan_insert_with_split(&stmt, &meta, 1, &[], &ErrorContext::default())
                .err()
                .unwrap_or_else(|| panic!("a row with no shard to go to must be refused")),
        );
        assert_eq!(code, "55000");
        assert!(
            message.contains(&format!("bucket {orphan}")),
            "the message must name the bucket, got: {message}"
        );
    }
}
