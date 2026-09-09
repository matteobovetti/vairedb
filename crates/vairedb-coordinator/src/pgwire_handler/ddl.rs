//! DDL (CREATE/DROP/ALTER/TRUNCATE TABLE) handling for the coordinator's pgwire
//! handler.
//!
//! Updates the metadata catalog and broadcasts shard-local DDL to every replica
//! of every shard. CREATE TABLE claims its name in the catalog first — atomically,
//! so concurrent creates of one name cannot both proceed — then rolls that back on
//! partial broadcast failure; DROP and ALTER broadcast best-effort first and
//! mutate the catalog only once every node was reached, so a failed command
//! leaves the catalog unchanged. TRUNCATE changes no metadata at all.
//! `ALTER TABLE ... RENAME TO` is the one ALTER that moves metadata rather than
//! reshaping it, so it claims the destination name up front like CREATE TABLE
//! does and undoes the renames it managed to apply if the broadcast fell short.
//! After any DDL that altered the catalog, the local and distributed DataFusion
//! catalog views are refreshed.

use std::collections::HashMap;

use crate::sqlparser::ast::{
    AlterTableOperation, CreateTable, ObjectType, RenameTableNameKind, Statement, TableConstraint,
    TruncateIdentityOption,
};
use datafusion::arrow::array::RecordBatch;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use tonic::transport::Channel;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ShardMeta, ShardStrategy, TableMeta};
use crate::pgwire_handler::constraints;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router;
use crate::pgwire_handler::schemas;
use crate::pgwire_handler::session::SessionState;
use crate::pgwire_handler::table_meta_ops::{
    self, apply_alter_operation, parse_create_table_config, reject_unserviceable_column_types,
    reject_unsupported_create_table_form,
};
use crate::scheduler;
use crate::util::{now_unix_secs, shard_table_name};
use crate::write_sql_cl;

/// The duplicate-relation error, worded like PostgreSQL's. Raised from three
/// places that must not disagree — CREATE TABLE's fast-path check, its atomic name
/// claim, and CREATE INDEX, whose name shares the relation namespace with tables.
pub(super) fn already_exists(table_name: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::TableAlreadyExists,
        format!("relation \"{table_name}\" already exists"),
    )
}

impl VaireDbQueryHandler {
    /// Create a table: validate the requested replication factor against alive
    /// nodes, claim the table name in the catalog, assign shards round-robin, then
    /// broadcast the shard-local CREATE to each replica. If any node rejects the DDL the
    /// partial creation is rolled back. Returns `TableAlreadyExists` when the
    /// relation exists without `IF NOT EXISTS`, or `FeatureNotSupported` if the
    /// replication factor exceeds the node count or the statement supplies no
    /// column list to derive a shard key from (see
    /// [`reject_unsupported_create_table_form`]).
    pub(super) async fn handle_create_table(
        &self,
        stmt: &Statement,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let Statement::CreateTable(create) = stmt else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "expected CREATE TABLE statement",
            ));
        };

        // A CTAS has no column list of its own to shard by: it takes the longer
        // route of materializing the query first, and comes back here with the
        // column list that describes the result.
        if create.query.is_some() {
            return self.handle_create_table_as_select(create, session).await;
        }

        self.create_table_from_column_list(stmt, create).await
    }

    /// The whole of CREATE TABLE once the column list is known — whether the client
    /// wrote it or a CTAS derived it from a query's result schema. Kept apart from
    /// [`Self::handle_create_table`] so the CTAS path can re-enter it without
    /// recursing through the dispatcher.
    async fn create_table_from_column_list(
        &self,
        stmt: &Statement,
        create: &CreateTable,
    ) -> PgWireResult<Response> {
        // Before the catalog is read or written: a form whose shard key cannot be
        // derived must not reach `put_table`, or a half-created table outlives the
        // error the client sees.
        reject_unsupported_create_table_form(create)?;
        // Likewise before the catalog: a column whose type cannot be read back must not
        // become a table that accepts rows it can never return.
        reject_unserviceable_column_types(create)?;

        let table_name = query_router::canonical_table_name(&create.name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;

        let ddl_ctx = ErrorContext::for_table(&table_name);

        // Before anything is claimed: the namespace the table asks for has to exist,
        // and its physical per-shard name has to be free. Both are properties of the
        // name alone, so they are decided before the cluster is involved.
        self.require_schema_exists(&table_name, &ddl_ctx)?;
        self.reject_physical_name_conflict(&table_name, &ddl_ctx)?;

        // Fast path only: an already-existing table is refused here so the cluster
        // is never queried for a CREATE that cannot proceed. It is *not* what makes
        // the name unique — `create_table_if_absent` below is. Keeping it means a
        // duplicate CREATE keeps reporting the duplicate rather than whichever
        // cluster-state error the validation below would hit first.
        if create.if_not_exists {
            if let Ok(Some(_)) = self.catalog.get_table(&table_name) {
                return Ok(Response::Execution(Tag::new("CREATE TABLE")));
            }
        } else if let Ok(Some(_)) = self.catalog.get_table(&table_name) {
            return Err(already_exists(&table_name));
        }

        let config = parse_create_table_config(create, self.default_replication_factor)?;

        let node_count = self
            .catalog
            .list_alive_nodes()
            .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?
            .len();

        let replication_factor = config.replication_factor;
        if replication_factor as usize > node_count {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "replication_factor {replication_factor} exceeds the number of available core nodes ({node_count})"
                ),
            ));
        }

        // A statement-specified shard count of 0 means "unspecified"; default to
        // one shard per alive node (at least one).
        let shard_count = if config.shard_count == 0 {
            node_count.max(1) as u32
        } else {
            config.shard_count
        };

        let table_meta = TableMeta {
            table_name: table_name.clone(),
            columns: config.columns,
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: config.shard_key,
            shard_count,
            replication_factor,
            created_at: Some(prost_types::Timestamp {
                seconds: now_unix_secs() as i64,
                nanos: 0,
            }),
            anonymized_columns: config.anonymized_columns,
            // A brand-new table has no indexes; they arrive later through
            // [`crate::pgwire_handler::indexes`].
            indexes: Vec::new(),
            // Constraints, unlike indexes, are mostly a CREATE TABLE affair: the
            // shards' engine can add almost none of them afterwards, so this is
            // where the enforceable ones are declared. See
            // [`crate::pgwire_handler::constraints`].
            constraints: config.constraints,
        };

        // Claim the name atomically. Losing the claim means a concurrent CREATE of
        // the same name committed first, so this one stops here: it must not assign
        // a second shard layout over the winner's, nor broadcast DDL for it.
        let claimed = self
            .catalog
            .create_table_if_absent(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?;
        if !claimed {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE TABLE")));
            }
            return Err(already_exists(&table_name));
        }

        let shards = self
            .catalog
            .assign_shards_round_robin(&table_name, shard_count, replication_factor)
            .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?;

        for shard in &shards {
            self.catalog
                .put_shard(shard)
                .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?;
        }

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?;

        let mut successful_sends: Vec<(String, String, String)> = Vec::new();

        let ddl_result: Result<(), PgWireError> = async {
            for shard in &shards {
                let shard_sql = shard_local_ddl_sql(stmt, shard);
                let shard_id = shard_table_name(&table_name, shard.hash_bucket);
                let drop_sql = format!("DROP TABLE IF EXISTS {}", shard_id);

                let write_id = uuid::Uuid::new_v4().to_string();

                for node_id in &self.write_router.get_target_nodes(shard) {
                    if let Some(address) = node_addresses.get(node_id) {
                        let channel = self.pool.get(address).await.map_err(|_| {
                            make_vdb_error(
                                VdbErrorCode::NodeCommunicationError,
                                format!("connection to node {} failed", node_id),
                            )
                        })?;
                        send_ddl_to_node(channel, &write_id, &shard_sql, &shard_id)
                            .await
                            .map_err(|_| {
                                make_vdb_error(
                                    VdbErrorCode::NodeCommunicationError,
                                    format!("DDL broadcast to node {} failed", node_id),
                                )
                            })?;
                        successful_sends.push((
                            address.clone(),
                            drop_sql.clone(),
                            shard_id.clone(),
                        ));
                    }
                }
            }
            Ok(())
        }
        .await;

        if let Err(e) = ddl_result {
            self.rollback_partial_create(&successful_sends, &table_name)
                .await;
            return Err(e);
        }

        self.refresh_catalog_after_ddl("CREATE TABLE");

        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    /// Create a table from a query's result: `CREATE TABLE t WITH (...) AS SELECT ...`.
    ///
    /// The columns come from the query's result schema, so the work is ordered to
    /// keep the failure modes honest:
    ///
    /// 1. Everything decidable from the statement alone is refused first — a
    ///    missing `shard_by`, a client-written column list — so a CTAS that cannot
    ///    work never runs its query nor touches the catalog.
    /// 2. An existing name is reported before the query runs, for the same reason.
    /// 3. The query is materialized, its schema turned into column definitions, and
    ///    the `shard_by` column checked against them.
    /// 4. The statement is re-entered as a plain `CREATE TABLE` with those columns,
    ///    which reuses the atomic name claim, shard assignment, DDL broadcast and
    ///    rollback rather than reimplementing them.
    /// 5. The collected rows are routed through the INSERT lane.
    ///
    /// Step 5 is not atomic with step 4: if the rows cannot be written the table is
    /// dropped again so the client is not left with an empty table it did not ask
    /// for, and only if *that* fails is a `PartialCommit` reported.
    ///
    /// Returns PostgreSQL's `SELECT <n>` tag, counting the rows written.
    async fn handle_create_table_as_select(
        &self,
        create: &CreateTable,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let Some(query) = create.query.as_ref() else {
            return Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "expected a CREATE TABLE with a query",
            ));
        };

        // A client column list would have to agree with the result schema in both
        // width and order, and PostgreSQL's spelling (names only, no types) has no
        // room for the types this needs. Aliasing in the SELECT says the same thing
        // unambiguously.
        if !create.columns.is_empty() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "CREATE TABLE ... AS SELECT does not accept a column list: the columns are taken from the query's result. Name them with aliases in the SELECT instead",
            ));
        }

        // The shard key is fixed for the table's lifetime, so it must be stated
        // rather than inferred from whichever column the SELECT happens to project
        // first. Refused here, before the query runs or the catalog is touched.
        let Some(shard_key) = table_meta_ops::explicit_shard_by(create) else {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "CREATE TABLE ... AS SELECT must name its shard key: the columns come from the query, so VaireDB will not guess which one to shard on. Add WITH (shard_by = '<column>')",
            ));
        };

        let table_name = query_router::canonical_table_name(&create.name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;

        // Reported before the query runs, for the same reason an existing name is:
        // a destination in a namespace that does not exist, or whose physical name
        // is taken, cannot be created however the query turns out. Checked again on
        // the way back through `create_table_from_column_list`, which is what
        // actually guards the claim.
        let ddl_ctx = ErrorContext::for_table(&table_name);
        self.require_schema_exists(&table_name, &ddl_ctx)?;
        self.reject_physical_name_conflict(&table_name, &ddl_ctx)?;

        // Reported before the query runs: an existing name makes the whole statement
        // moot, and running the SELECT first would charge the client for it.
        if let Ok(Some(_)) = self.catalog.get_table(&table_name) {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE TABLE AS")));
            }
            return Err(already_exists(&table_name));
        }

        // Also decidable from the statement alone: a destination that pseudonymizes
        // cannot be filled from a table whose values are already digests.
        self.reject_hashing_a_digest(
            "CREATE TABLE ... AS SELECT",
            &table_name,
            &table_meta_ops::declared_anonymized_columns(create)?,
            &write_sql_cl::relations_read(query.as_ref()),
        )?;

        let source = Statement::Query(query.clone());
        let (schema, batches) = self.collect_query_rows(&source, &[]).await?;

        let columns = table_meta_ops::column_defs_from_result_schema(&schema)
            .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;

        let column_names: Vec<String> = columns
            .iter()
            .map(|c| query_router::canonicalize_ident(&c.name))
            .collect();
        if !column_names.contains(&shard_key) {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!(
                    "shard key \"{shard_key}\" is not among the query's result columns ({})",
                    column_names.join(", ")
                ),
            ));
        }

        // Re-entering as a plain CREATE TABLE is what keeps one code path
        // responsible for claiming the name, assigning shards, broadcasting the
        // shard-local DDL and rolling back a partial broadcast.
        let mut plain = create.clone();
        plain.query = None;
        plain.columns = columns;
        let plain_stmt = Statement::CreateTable(plain.clone());
        self.create_table_from_column_list(&plain_stmt, &plain)
            .await?;

        let column_refs: Vec<&str> = column_names.iter().map(String::as_str).collect();
        let rows = self
            .fill_created_table(&table_name, &column_refs, &batches, session)
            .await?;

        Ok(Response::Execution(
            Tag::new("SELECT").with_rows(rows as usize),
        ))
    }

    /// Write the rows a CTAS collected into the table it just created, dropping
    /// that table again if they cannot all be written.
    ///
    /// Splitting this out keeps the compensation in one place: whatever goes wrong
    /// between "the table exists" and "the rows are in it", the client ends up with
    /// either a filled table or no table — never an empty one that silently lost a
    /// query's result. A failed drop is the one case that cannot be tidied, and is
    /// reported as such.
    async fn fill_created_table(
        &self,
        table_name: &str,
        columns: &[&str],
        batches: &[RecordBatch],
        session: &SessionState,
    ) -> PgWireResult<u64> {
        let write = async {
            let template = write_sql_cl::insert_template(table_name, columns)
                .map_err(|msg| make_vdb_error(VdbErrorCode::InternalError, msg))?;
            let statements = write_sql_cl::insert_statements_from_batches(
                &template,
                batches,
                write_sql_cl::ROWS_PER_STATEMENT,
            )
            .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;
            self.write_row_statements(
                &statements,
                &[],
                session,
                table_name,
                "CREATE TABLE ... AS SELECT",
            )
            .await
        }
        .await;

        match write {
            Ok(rows) => Ok(rows),
            Err(e) => {
                let dropped = self
                    .drop_table_after_failed_ctas(table_name)
                    .await
                    .map_err(|drop_err| {
                        make_vdb_error(
                            VdbErrorCode::PartialCommit,
                            format!(
                                "CREATE TABLE ... AS SELECT could not write the query's rows, and the empty table \"{table_name}\" it created could not be dropped again. Drop it manually before retrying. Write failure: {e}. Drop failure: {drop_err}"
                            ),
                        )
                    });
                dropped?;
                Err(e)
            }
        }
    }

    /// Best-effort `DROP TABLE` used to undo the table a CTAS created when its rows
    /// could not be written. Goes through the ordinary DROP path so the shards and
    /// catalog rows are removed the same way a client DROP removes them.
    async fn drop_table_after_failed_ctas(&self, table_name: &str) -> PgWireResult<()> {
        let sql = format!(
            "DROP TABLE IF EXISTS \"{}\"",
            table_name.replace('"', "\"\"")
        );
        let stmt = crate::pgwire_handler::parser::parse_sql(&sql)
            .map_err(|e| make_vdb_error(VdbErrorCode::InternalError, e.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                make_vdb_error(VdbErrorCode::InternalError, "could not build a DROP TABLE")
            })?;
        self.handle_drop_table(&stmt).await.map(|_| ())
    }

    /// Undo a CREATE TABLE that failed partway through broadcasting: send a
    /// best-effort `DROP TABLE` to every node that already created its shard,
    /// then delete the catalog rows. Failures here are logged, not surfaced —
    /// the original DDL error is what the client should see.
    async fn rollback_partial_create(
        &self,
        successful_sends: &[(String, String, String)],
        table_name: &str,
    ) {
        self.send_compensating_ddl(successful_sends, "DROP").await;

        let _ = self.catalog.delete_shards_for_table(table_name);
        let _ = self.catalog.delete_table(table_name);
    }

    /// Send one already-rendered undo statement to each node that applied the
    /// statement being compensated for. `sends` is `(address, undo SQL, shard id)`,
    /// and `op_label` names the undo verb for the logs. Failures here are logged,
    /// not surfaced — the error that triggered the compensation is what the client
    /// should see, and a node that cannot be reached to undo is exactly the case
    /// the caller is already reporting as a partial failure.
    async fn send_compensating_ddl(&self, sends: &[(String, String, String)], op_label: &str) {
        for (address, undo_sql, shard_id) in sends {
            let write_id = uuid::Uuid::new_v4().to_string();
            let channel = match self.pool.get(address).await {
                Ok(ch) => ch,
                Err(ce) => {
                    tracing::error!(
                        address = %address,
                        error = %ce,
                        "DDL rollback connection failed"
                    );
                    continue;
                }
            };
            if let Err(re) = send_ddl_to_node(channel, &write_id, undo_sql, shard_id).await {
                tracing::error!(
                    address = %address,
                    shard_id = %shard_id,
                    error = %re,
                    "DDL rollback ({op_label}) failed"
                );
            }
        }
    }

    /// Drop a table: broadcast a best-effort shard-local `DROP TABLE IF EXISTS`
    /// to every replica, then remove the catalog rows and deregister it from the
    /// local DataFusion session. Returns `TableNotFound` when the relation is
    /// absent without `IF EXISTS`, or a `NodeCommunicationError` if any node was
    /// unreachable.
    ///
    /// Only `DROP TABLE` is accepted. An index has its own path
    /// ([`crate::pgwire_handler::indexes`]) and the catalog models nothing else, so
    /// every other object kind is refused rather than resolved against the table
    /// namespace — `DROP VIEW t` naming a table used to drop it.
    pub(super) async fn handle_drop_table(&self, stmt: &Statement) -> PgWireResult<Response> {
        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;
        let drop_ctx = ErrorContext::for_table(&table_name);

        let table_exists = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?
            .is_some();

        match plan_drop(stmt, &table_name, table_exists)? {
            DropPlan::Nothing(tag) => return Ok(Response::Execution(Tag::new(&tag))),
            DropPlan::DropTable => {}
        }

        let shards = self
            .catalog
            .get_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?;

        let failed = self
            .broadcast_ddl_best_effort(&shards, &node_addresses, "DROP", &table_name, |shard| {
                format!(
                    "DROP TABLE IF EXISTS {}",
                    shard_table_name(&table_name, shard.hash_bucket)
                )
            })
            .await;
        fail_if_unreachable("DROP TABLE", failed)?;

        self.catalog
            .delete_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?;
        self.catalog
            .delete_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?;

        self.deregister_table_after_ddl(&table_name);
        self.refresh_catalog_after_ddl("DROP TABLE");

        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }

    /// Empty a table: broadcast a shard-local `TRUNCATE TABLE` to every replica of
    /// every shard. The catalog is not touched — the table, its shard layout and
    /// its schema all survive; only the rows go. Returns `TableNotFound` when the
    /// relation is absent without `IF EXISTS`, `FeatureNotSupported` for a form
    /// whose meaning VaireDB cannot honor (see [`plan_truncate`]), or a
    /// `NodeCommunicationError` if any node was unreachable.
    ///
    /// **Not atomic across shards.** Each shard is emptied by its own statement,
    /// so a node that fails midway leaves the earlier shards empty and the rest
    /// populated; the error says the command partially failed, and re-running it is
    /// safe because emptying an already-empty shard is a no-op. That idempotence is
    /// why TRUNCATE can be honored at all while cross-shard atomicity is still
    /// missing — unlike a multi-statement write, retrying converges.
    pub(super) async fn handle_truncate(&self, stmt: &Statement) -> PgWireResult<Response> {
        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;
        let truncate_ctx = ErrorContext::for_table(&table_name);

        let table_exists = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &truncate_ctx, &self.catalog))?
            .is_some();

        match plan_truncate(stmt, &table_name, table_exists)? {
            TruncatePlan::Nothing => return Ok(Response::Execution(Tag::new("TRUNCATE TABLE"))),
            TruncatePlan::Truncate => {}
        }

        let shards = self
            .catalog
            .get_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &truncate_ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &truncate_ctx, &self.catalog))?;

        // Emitted from the shard name rather than rendered from the parsed
        // statement: the client's decorations (`ONLY`, a trailing `*`, `TABLE`)
        // describe inheritance the catalog does not model, so the node should see
        // one canonical form regardless of how the client spelled it.
        let failed = self
            .broadcast_ddl_best_effort(&shards, &node_addresses, "TRUNCATE", &table_name, |shard| {
                format!(
                    "TRUNCATE TABLE {}",
                    shard_table_name(&table_name, shard.hash_bucket)
                )
            })
            .await;
        fail_if_unreachable("TRUNCATE TABLE", failed)?;

        Ok(Response::Execution(Tag::new("TRUNCATE TABLE")))
    }

    /// Alter a table: apply each operation to the cached metadata, broadcast the
    /// rewritten shard-local ALTER to every replica best-effort, and persist the
    /// updated metadata only once every node was reached. Returns `TableNotFound`
    /// when the relation is absent without `IF EXISTS`, or a
    /// `NodeCommunicationError` if any node was unreachable — in which case the
    /// catalog is left unchanged.
    ///
    /// `RENAME TO` is not a schema change and takes its own path; see
    /// [`Self::rename_table`].
    pub(super) async fn handle_alter_table(&self, stmt: &Statement) -> PgWireResult<Response> {
        let (table_name, operations, if_exists) = match stmt {
            Statement::AlterTable(alter) => {
                let table_name =
                    query_router::canonical_table_name(&alter.name).ok_or_else(|| {
                        make_vdb_error(
                            VdbErrorCode::SqlSyntaxError,
                            "could not determine table name",
                        )
                    })?;
                (table_name, &alter.operations, alter.if_exists)
            }
            _ => {
                return Err(make_vdb_error(
                    VdbErrorCode::SqlSyntaxError,
                    "expected ALTER TABLE statement",
                ));
            }
        };

        // Decided from the statement alone, before the catalog is read: a
        // malformed rename is a syntax error whether or not the table exists,
        // exactly as PostgreSQL reports it.
        let plan = plan_alter(operations)?;

        let alter_ctx = ErrorContext::for_table(&table_name);

        let mut table_meta = match self.catalog.get_table(&table_name) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                if if_exists {
                    return Ok(Response::Execution(Tag::new("ALTER TABLE")));
                }
                return Err(make_vdb_error(
                    VdbErrorCode::TableNotFound,
                    format!("relation \"{}\" does not exist", table_name),
                ));
            }
            Err(e) => return Err(enrich_coordinator_error(&e, &alter_ctx, &self.catalog)),
        };

        match plan {
            // The destination is unqualified (a qualified one is refused in
            // `plan_alter`), so it lands in the schema the table is already in: a
            // rename renames, it does not move the table to another namespace.
            AlterPlan::Rename(new_name) => {
                let new_name = schemas::rename_within_schema(&table_name, &new_name);
                return self.rename_table(table_meta, &new_name).await;
            }
            AlterPlan::AddConstraint {
                constraint,
                not_valid,
            } => {
                return self
                    .add_constraint(table_meta, &constraint, not_valid)
                    .await;
            }
            AlterPlan::DropConstraint { name, if_exists } => {
                return self.drop_constraint(table_meta, &name, if_exists).await;
            }
            AlterPlan::ColumnOps => {}
        }

        for op in operations {
            apply_alter_operation(&mut table_meta, op)?;
        }

        let shards = self
            .catalog
            .get_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &alter_ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &alter_ctx, &self.catalog))?;

        let failed = self
            .broadcast_ddl_best_effort(
                &shards,
                &node_addresses,
                "ALTER TABLE",
                &table_name,
                |shard| shard_local_ddl_sql(stmt, shard),
            )
            .await;
        fail_if_unreachable("ALTER TABLE", failed)?;

        // Persist the schema change only after the broadcast reached every node,
        // so a partial failure never leaves the catalog claiming a column the
        // cluster does not have.
        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &alter_ctx, &self.catalog))?;

        self.deregister_table_after_ddl(&table_name);
        self.refresh_catalog_after_ddl("ALTER TABLE");

        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// Rename a table: claim `new_name` in the catalog, rename every
    /// `{table}_shard{n}` on every replica, then release the old name. `table_meta`
    /// is the existing metadata of the table being renamed, which moves to the new
    /// key unchanged — the shard layout, shard key, columns and anonymization rules
    /// all follow the name. Returns `TableAlreadyExists` if `new_name` is taken, or
    /// a `NodeCommunicationError` if any node was unreachable.
    ///
    /// **Not atomic across shards, and not idempotent.** A rename that fails
    /// part-way cannot simply be retried the way a TRUNCATE can — the shards that
    /// already moved no longer answer to the old name — so the shards that did
    /// rename are renamed back and the claim on `new_name` is released before the
    /// error is returned. Any node that cannot be reached for that undo is logged
    /// and left with the new physical name; the client sees the partial failure.
    ///
    /// Between the claim and the release the catalog holds both names, the new one
    /// authoritative. A concurrent statement naming either one during that window
    /// sees a table whose physical shards are being moved under it; nothing
    /// serializes DDL against DML yet, so that window is real, just narrow.
    async fn rename_table(&self, table_meta: TableMeta, new_name: &str) -> PgWireResult<Response> {
        let old_name = table_meta.table_name.clone();
        let rename_ctx = ErrorContext::for_table(&old_name);

        // The shards' engine treats an index as a dependency on the table and
        // refuses to rename one that has any, so the broadcast would fail on every
        // node after the destination name was already claimed. Refused up front
        // instead, naming what to drop — the same rule the column changes follow
        // (`reject_if_indexed`). A constraint VaireDB enforces with a per-shard
        // index counts, and is removed by a statement of its own.
        let blocking: Vec<String> = table_meta
            .indexes
            .iter()
            .map(|idx| format!("DROP INDEX \"{}\"", idx.name))
            .chain(
                table_meta
                    .constraints
                    .iter()
                    .filter(|c| c.index_backed)
                    .map(|c| format!("ALTER TABLE {old_name} DROP CONSTRAINT \"{}\"", c.name)),
            )
            .collect();
        if !blocking.is_empty() {
            // One index gives a statement the client can copy; several cannot,
            // since each removal statement takes one name — the same wording the
            // column changes use.
            let hint = match blocking.as_slice() {
                [only] => format!("{only} first"),
                many => format!("remove all of them first: {}", many.join("; ")),
            };
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "cannot rename table \"{old_name}\" because it carries an index, and the \
                     shards' engine refuses to alter a table an index depends on; {hint} and \
                     recreate it on the renamed table"
                ),
            ));
        }

        // The destination's physical name has to be free too — it is the name the
        // per-shard tables are renamed to, and a rename inside a schema can collide
        // with a relation whose flat name folds to the same thing.
        self.reject_physical_name_conflict(new_name, &rename_ctx)?;

        let shards = self
            .catalog
            .get_shards_for_table(&old_name)
            .map_err(|e| enrich_coordinator_error(&e, &rename_ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &rename_ctx, &self.catalog))?;

        // Claim the destination the way CREATE TABLE does, so a rename and a
        // concurrent create of the same name cannot both believe they own it. This
        // also covers `RENAME TO` naming the table itself, which PostgreSQL reports
        // as a duplicate relation.
        let mut renamed_meta = table_meta;
        renamed_meta.table_name = new_name.to_string();
        let claimed = self
            .catalog
            .create_table_if_absent(&renamed_meta)
            .map_err(|e| enrich_coordinator_error(&e, &rename_ctx, &self.catalog))?;
        if !claimed {
            return Err(already_exists(new_name));
        }

        // Same shards, same placement, new key: the rows move with the table.
        for shard in &shards {
            let mut renamed_shard = shard.clone();
            renamed_shard.table_name = new_name.to_string();
            if let Err(e) = self.catalog.put_shard(&renamed_shard) {
                self.release_claimed_name(new_name);
                return Err(enrich_coordinator_error(&e, &rename_ctx, &self.catalog));
            }
        }

        let RenameBroadcast {
            failed_nodes,
            applied,
        } = self
            .broadcast_table_rename(&shards, &node_addresses, &old_name, new_name)
            .await;

        if let Err(e) = fail_if_unreachable("ALTER TABLE ... RENAME TO", failed_nodes) {
            self.send_compensating_ddl(&applied, "RENAME TO").await;
            self.release_claimed_name(new_name);
            return Err(e);
        }

        self.catalog
            .delete_shards_for_table(&old_name)
            .map_err(|e| enrich_coordinator_error(&e, &rename_ctx, &self.catalog))?;
        self.catalog
            .delete_table(&old_name)
            .map_err(|e| enrich_coordinator_error(&e, &rename_ctx, &self.catalog))?;

        // Both names: the old one so the vacated name stops resolving, the new one
        // in case a dropped-and-recreated predecessor left a registration behind.
        self.deregister_table_after_ddl(&old_name);
        self.deregister_table_after_ddl(new_name);
        self.refresh_catalog_after_ddl("ALTER TABLE");

        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// Broadcast the per-shard `ALTER TABLE ... RENAME TO` to every replica of
    /// every shard, best-effort, recording which nodes applied it so a partial
    /// failure can be put back.
    ///
    /// The statements are built from the shard names rather than rewritten from the
    /// client's AST: `RENAME TO`'s destination is not a relation reference the
    /// shard-local rewriter descends into, so rendering the parsed statement would
    /// ship `ALTER TABLE orders_shard0 RENAME TO new_orders` — the old table
    /// renamed to a name no shard uses.
    async fn broadcast_table_rename(
        &self,
        shards: &[ShardMeta],
        node_addresses: &HashMap<String, String>,
        old_name: &str,
        new_name: &str,
    ) -> RenameBroadcast {
        let mut failed_nodes: Vec<String> = Vec::new();
        let mut applied: Vec<(String, String, String)> = Vec::new();

        for shard in shards {
            let from = shard_table_name(old_name, shard.hash_bucket);
            let to = shard_table_name(new_name, shard.hash_bucket);
            let rename_sql = format!("ALTER TABLE {from} RENAME TO {to}");
            let undo_sql = format!("ALTER TABLE {to} RENAME TO {from}");
            let write_id = uuid::Uuid::new_v4().to_string();

            for node_id in &self.write_router.get_target_nodes(shard) {
                let Some(address) = node_addresses.get(node_id) else {
                    continue;
                };
                let channel = match self.pool.get(address).await {
                    Ok(ch) => ch,
                    Err(e) => {
                        tracing::error!("RENAME TO connection to node {node_id} failed: {e}");
                        failed_nodes.push(node_id.clone());
                        continue;
                    }
                };
                if let Err(e) = send_ddl_to_node(channel, &write_id, &rename_sql, &from).await {
                    tracing::error!("RENAME TO broadcast to node {node_id} failed: {e}");
                    failed_nodes.push(node_id.clone());
                    continue;
                }
                applied.push((address.clone(), undo_sql.clone(), to.clone()));
            }
        }

        RenameBroadcast {
            failed_nodes,
            applied,
        }
    }

    /// Give up a table name claimed by a rename that could not be completed:
    /// remove its shard rows and its table row. Logged, never fatal — the error
    /// that abandoned the rename is what the client should see.
    fn release_claimed_name(&self, table_name: &str) {
        if let Err(e) = self.catalog.delete_shards_for_table(table_name) {
            tracing::error!("failed to release claimed shards for '{table_name}': {e}");
        }
        if let Err(e) = self.catalog.delete_table(table_name) {
            tracing::error!("failed to release claimed name '{table_name}': {e}");
        }
    }

    /// Broadcast a per-shard DDL statement to every replica of every shard,
    /// best-effort. `make_shard_sql` produces the SQL to run on the shard (a
    /// shard-local `DROP`, rewritten `ALTER`, etc.). Returns the de-dup-pending
    /// list of node IDs that could not be reached or rejected the statement, so
    /// the caller can decide whether a partial failure is fatal.
    pub(super) async fn broadcast_ddl_best_effort(
        &self,
        shards: &[ShardMeta],
        node_addresses: &HashMap<String, String>,
        op_label: &str,
        table_name: &str,
        make_shard_sql: impl Fn(&ShardMeta) -> String,
    ) -> Vec<String> {
        let mut failed_nodes: Vec<String> = Vec::new();
        for shard in shards {
            let shard_sql = make_shard_sql(shard);
            let shard_id = shard_table_name(table_name, shard.hash_bucket);
            let write_id = uuid::Uuid::new_v4().to_string();

            for node_id in &self.write_router.get_target_nodes(shard) {
                let Some(address) = node_addresses.get(node_id) else {
                    continue;
                };
                let channel = match self.pool.get(address).await {
                    Ok(ch) => ch,
                    Err(e) => {
                        tracing::error!("{op_label} connection to node {node_id} failed: {e}");
                        failed_nodes.push(node_id.clone());
                        continue;
                    }
                };
                if let Err(e) = send_ddl_to_node(channel, &write_id, &shard_sql, &shard_id).await {
                    tracing::error!("{op_label} broadcast to node {node_id} failed: {e}");
                    failed_nodes.push(node_id.clone());
                }
            }
        }
        failed_nodes
    }

    /// Drop the table from both DataFusion sessions so a later re-create or schema
    /// change re-registers it fresh. Logged, never fatal.
    ///
    /// Both, because both hold a registration: the distributed context plans the reads
    /// and the local one is what `pg_class` is answered from. Leaving the local one
    /// behind would keep a dropped table visible to every client that lists tables.
    fn deregister_table_after_ddl(&self, table_name: &str) {
        for (label, ctx) in [
            ("distributed", &self.session_ctx),
            ("local", &self.local_ctx),
        ] {
            let table_ref = datafusion::common::TableReference::bare(table_name.to_string());
            if let Err(e) = ctx.deregister_table(table_ref) {
                tracing::warn!(
                    "failed to deregister table '{}' from the {} DataFusion session: {}",
                    table_name,
                    label,
                    e
                );
            }
        }
    }

    /// Refresh the DataFusion catalog view after a successful DDL so reads see the new
    /// schema and introspection sees the new table. Logged, never fatal.
    fn refresh_catalog_after_ddl(&self, op_label: &str) {
        for (label, ctx) in [
            ("distributed", &self.session_ctx),
            ("local", &self.local_ctx),
        ] {
            if let Err(e) = scheduler::refresh_catalog_tables(ctx, &self.catalog) {
                tracing::warn!(
                    "failed to refresh the {label} DataFusion catalog after {op_label}: {e}"
                );
            }
        }
    }
}

/// The outcome of broadcasting a per-shard table rename.
struct RenameBroadcast {
    /// Nodes that could not be reached, or that rejected the rename.
    failed_nodes: Vec<String>,
    /// `(node address, undo SQL, shard id)` for every replica that did rename its
    /// shard, so a partial failure can be put back.
    applied: Vec<(String, String, String)>,
}

/// What a parsed `ALTER TABLE`'s operation list asks
/// [`VaireDbQueryHandler::handle_alter_table`] to do.
#[derive(Debug, PartialEq)]
enum AlterPlan {
    /// `RENAME TO`: move the table to this canonical name, changing no schema.
    Rename(String),
    /// `ADD CONSTRAINT`: give the table a constraint, enforced by an object of its
    /// own on every shard — see [`crate::pgwire_handler::constraints`].
    AddConstraint {
        constraint: Box<TableConstraint>,
        /// `NOT VALID`: leave the rows already stored unchecked.
        not_valid: bool,
    },
    /// `DROP CONSTRAINT`: take one away again, by its canonical name.
    DropConstraint { name: String, if_exists: bool },
    /// Everything else: operations that reshape the table's metadata in place.
    ColumnOps,
}

/// The `ALTER TABLE` actions that are not column reshaping, and the label to name
/// each one by in an error. Every one of them has its own execution path — a rename
/// moves catalog rows and physical tables, a constraint change creates or drops a
/// per-shard object — so none can ride the generic per-shard `ALTER TABLE`
/// broadcast, and none can share a statement with another action.
fn standalone_label(op: &AlterTableOperation) -> Option<&'static str> {
    match op {
        AlterTableOperation::RenameTable { .. } => Some("RENAME TO"),
        AlterTableOperation::AddConstraint { .. } => Some("ADD CONSTRAINT"),
        AlterTableOperation::DropConstraint { .. } => Some("DROP CONSTRAINT"),
        _ => None,
    }
}

/// Decide whether an `ALTER TABLE` renames the relation, changes a constraint, or
/// reshapes its columns.
///
/// A rename moves catalog rows and physical tables, a constraint change builds or
/// drops one index per shard, and every other operation edits a column list. None
/// of the three can share a code path, and PostgreSQL does not let a rename share a
/// statement either — `RENAME` is its own `ALTER TABLE` form, so a list mixing it
/// with other actions is a syntax error there and here; a constraint change is
/// refused in a mixed list for the stronger reason that the parts would be applied
/// by different broadcasts, with no way to undo the first if the second failed. A
/// rename's destination may not be schema-qualified: as in PostgreSQL, a rename stays
/// inside the table's existing schema, so a qualifier could only agree with that
/// schema or contradict it.
///
/// Pure, so the rules that decide whether a statement re-keys a table are testable
/// without a catalog.
fn plan_alter(operations: &[AlterTableOperation]) -> PgWireResult<AlterPlan> {
    // Operations that name a constraint by kind rather than by name reach the
    // shards' engine as something it rejects, so they are named here instead.
    if let Some(err) = operations
        .iter()
        .find_map(constraints::refused_alter_operation)
    {
        return Err(err);
    }

    let Some(op) = operations.iter().find(|op| standalone_label(op).is_some()) else {
        return Ok(AlterPlan::ColumnOps);
    };

    if operations.len() > 1 {
        let label = standalone_label(op).unwrap_or("this action");
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!(
                "ALTER TABLE ... {label} cannot be combined with other actions; \
                 apply it with its own statement"
            ),
        ));
    }

    match op {
        // `AS` is MySQL's spelling of the same operation; both name the destination.
        AlterTableOperation::RenameTable { table_name } => {
            let new_name = match table_name {
                RenameTableNameKind::To(name) | RenameTableNameKind::As(name) => name,
            };
            if new_name.0.len() > 1 {
                return Err(make_vdb_error(
                    VdbErrorCode::SqlSyntaxError,
                    "the new name in ALTER TABLE ... RENAME TO may not be schema-qualified",
                ));
            }
            let new_name = query_router::canonical_table_name(new_name).ok_or_else(|| {
                make_vdb_error(
                    VdbErrorCode::SqlSyntaxError,
                    "could not determine the new table name",
                )
            })?;
            Ok(AlterPlan::Rename(new_name))
        }
        AlterTableOperation::AddConstraint {
            constraint,
            not_valid,
        } => Ok(AlterPlan::AddConstraint {
            constraint: Box::new(constraint.clone()),
            not_valid: *not_valid,
        }),
        AlterTableOperation::DropConstraint {
            if_exists,
            name,
            drop_behavior,
        } => {
            // The catalog models no object that depends on a constraint, so a
            // referential action could only be ignored.
            if drop_behavior.is_some() {
                return Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    "ALTER TABLE ... DROP CONSTRAINT ... CASCADE/RESTRICT is not supported by \
                     VaireDB; the catalog tracks no dependent objects",
                ));
            }
            Ok(AlterPlan::DropConstraint {
                name: query_router::canonicalize_ident(name),
                if_exists: *if_exists,
            })
        }
        // `standalone_label` returned a label for it, so this is unreachable unless
        // the two go out of step.
        _ => Ok(AlterPlan::ColumnOps),
    }
}

/// What [`VaireDbQueryHandler::handle_drop_table`] should do with a parsed
/// `DROP`, once the statement shape and the catalog agree it can be honored.
#[derive(Debug, PartialEq)]
enum DropPlan {
    /// Drop the table from every shard, then from the catalog.
    DropTable,
    /// Report success without touching anything: `IF EXISTS` naming something
    /// VaireDB does not have.
    Nothing(String),
}

/// Decide what a `DROP` means, given the canonical target name and whether a
/// table of that name exists.
///
/// Every kind that reaches here but `TABLE` names something the catalog does not
/// model, and must never be resolved against the table namespace: `DROP VIEW t`
/// naming a table used to silently drop it. PostgreSQL semantics are mirrored — an
/// existing table under a non-table kind is `42809` (wrong object type), and a name
/// that resolves to nothing reports the kind the client asked for rather than
/// "table". `DROP INDEX` does not reach here: an index is a real object with its
/// own namespace, handled in [`crate::pgwire_handler::indexes`].
///
/// Pure, so the rules that keep a mistyped object kind from destroying a table
/// are testable without a cluster.
fn plan_drop(stmt: &Statement, table_name: &str, table_exists: bool) -> PgWireResult<DropPlan> {
    let Statement::Drop {
        object_type,
        if_exists,
        names,
        cascade,
        restrict,
        ..
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a DROP statement",
        ));
    };

    if *object_type != ObjectType::Table {
        let kind = object_type.to_string();
        if table_exists {
            return Err(make_vdb_error(
                VdbErrorCode::WrongObjectType,
                format!("\"{table_name}\" is not a {kind}; use DROP TABLE to drop a table"),
            ));
        }
        if *if_exists {
            return Ok(DropPlan::Nothing(format!("DROP {kind}")));
        }
        return Err(make_vdb_error(
            VdbErrorCode::TableNotFound,
            format!("{} \"{table_name}\" does not exist", kind.to_lowercase()),
        ));
    }

    // Only the first name is ever acted on, so accepting a multi-object DROP
    // would report success for objects that were never touched.
    if names.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP TABLE with more than one table is not supported by VaireDB; \
             drop each table with its own statement",
        ));
    }

    // Referential actions have no meaning while the catalog models no dependent
    // objects; honoring them silently would be a lie.
    if *cascade || *restrict {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP TABLE ... CASCADE/RESTRICT is not supported by VaireDB; \
             the catalog tracks no dependent objects",
        ));
    }

    if !table_exists {
        if *if_exists {
            return Ok(DropPlan::Nothing("DROP TABLE".to_string()));
        }
        return Err(make_vdb_error(
            VdbErrorCode::TableNotFound,
            format!("table \"{table_name}\" does not exist"),
        ));
    }

    Ok(DropPlan::DropTable)
}

/// What [`VaireDbQueryHandler::handle_truncate`] should do with a parsed
/// `TRUNCATE`, once the statement shape and the catalog agree it can be honored.
#[derive(Debug, PartialEq)]
enum TruncatePlan {
    /// Empty the table on every shard.
    Truncate,
    /// Report success without touching anything: `IF EXISTS` naming a table
    /// VaireDB does not have.
    Nothing,
}

/// Decide what a `TRUNCATE` means, given the canonical target name and whether a
/// table of that name exists.
///
/// PostgreSQL empties every named table in one transaction; VaireDB empties one
/// shard at a time, so anything whose meaning depends on that transaction — a
/// second table, a cascade to referencing tables, a sequence restart — is refused
/// rather than approximated. `ONLY` and a trailing `*` are the exception: they
/// select between a table and its inheritance children, and the catalog models no
/// inheritance, so both spellings already mean this one table.
///
/// Pure, so the rules that decide whether a table's rows are destroyed are
/// testable without a cluster.
fn plan_truncate(
    stmt: &Statement,
    table_name: &str,
    table_exists: bool,
) -> PgWireResult<TruncatePlan> {
    let Statement::Truncate(truncate) = stmt else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a TRUNCATE statement",
        ));
    };

    // Only the first name is ever acted on, and PostgreSQL's guarantee for the
    // multi-table form is that all of them are emptied together — which is exactly
    // what a per-shard broadcast cannot give.
    if truncate.table_names.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "TRUNCATE with more than one table is not supported by VaireDB; \
             truncate each table with its own statement",
        ));
    }

    // Referential actions have no meaning while the catalog models no dependent
    // objects; honoring them silently would be a lie.
    if truncate.cascade.is_some() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "TRUNCATE ... CASCADE/RESTRICT is not supported by VaireDB; \
             the catalog tracks no dependent objects",
        ));
    }

    // CONTINUE IDENTITY is the default and is what a truncate without sequences
    // does anyway; RESTART IDENTITY asks for something the catalog cannot do.
    if matches!(truncate.identity, Some(TruncateIdentityOption::Restart)) {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "TRUNCATE ... RESTART IDENTITY is not supported by VaireDB; \
             the catalog tracks no sequences to restart",
        ));
    }

    if truncate.partitions.is_some() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "TRUNCATE of a partition is not supported by VaireDB; \
             a table's only partitioning is its shard layout",
        ));
    }

    if truncate.on_cluster.is_some() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "TRUNCATE ... ON CLUSTER is not supported by VaireDB; \
             every TRUNCATE already reaches the whole cluster",
        ));
    }

    if !table_exists {
        if truncate.if_exists {
            return Ok(TruncatePlan::Nothing);
        }
        return Err(make_vdb_error(
            VdbErrorCode::TableNotFound,
            format!("relation \"{table_name}\" does not exist"),
        ));
    }

    Ok(TruncatePlan::Truncate)
}

/// Turn a partial-broadcast failure list into a client-facing error, or `Ok` if
/// every node was reached. Deduplicates the node list first so the count
/// reflects distinct unreachable nodes rather than per-shard attempts.
pub(super) fn fail_if_unreachable(
    op_label: &str,
    mut failed_nodes: Vec<String>,
) -> PgWireResult<()> {
    if failed_nodes.is_empty() {
        return Ok(());
    }
    failed_nodes.dedup();
    Err(make_vdb_error(
        VdbErrorCode::NodeCommunicationError,
        format!(
            "{op_label} partially failed: could not reach {} node(s)",
            failed_nodes.len()
        ),
    ))
}

/// Rewrite a DDL statement to its shard-local form (suffixed relation names,
/// DuckDB-compatible types) and render it back to SQL. Shared by CREATE and
/// ALTER, which broadcast the same statement to every shard.
fn shard_local_ddl_sql(stmt: &Statement, shard: &ShardMeta) -> String {
    let mut ddl_stmt = stmt.clone();
    write_sql_cl::rewrite_to_shard_local(&mut ddl_stmt, &format!("shard{}", shard.hash_bucket));
    write_sql_cl::transform_to_duckdb(&mut ddl_stmt);
    write_sql_cl::statement_to_sql(&ddl_stmt)
}

/// Send a single shard-local DDL statement to one node over its `WriteService`
/// gRPC channel. `write_id` lets the node dedup retries. Returns `Err` with a
/// formatted message on transport failure or if the node reports the write
/// failed.
async fn send_ddl_to_node(
    channel: Channel,
    write_id: &str,
    sql: &str,
    shard_id: &str,
) -> Result<(), String> {
    use vairedb_common::proto::vairedb::v1::{
        ExecuteWriteRequest, WriteOperation, WriteStatement,
        write_service_client::WriteServiceClient,
    };

    let mut client = WriteServiceClient::new(channel);

    let request = tonic::Request::new(ExecuteWriteRequest {
        write_id: write_id.to_string(),
        statements: vec![WriteStatement {
            sql: sql.to_string(),
            shard_id: shard_id.to_string(),
            operation: WriteOperation::Unspecified.into(),
            params: vec![],
        }],
        atomic: false,
    });

    let response = client
        .execute_write(request)
        .await
        .map_err(|e| format!("[{}] {}", e.code(), e.message()))?;
    let resp = response.into_inner();

    if let Some(result) = resp.results.first()
        && !result.success
    {
        let msg = result
            .error
            .as_ref()
            .map(|e| e.message.clone())
            .unwrap_or_else(|| "unknown error from node".to_string());
        return Err(msg);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    /// Parse a single statement, panicking on anything else.
    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    /// The SQLSTATE and message a `PgWireError` reports to the client.
    fn user_error(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// The operation list of a single `ALTER TABLE`, panicking on anything else.
    fn parse_alter_ops(sql: &str) -> Vec<AlterTableOperation> {
        match parse_one(sql) {
            Statement::AlterTable(alter) => alter.operations,
            other => panic!("expected ALTER TABLE, got {other:?}"),
        }
    }

    /// The SQLSTATE and message a rejected `DROP` plan reports to the client.
    fn rejection(sql: &str, table_name: &str, table_exists: bool) -> (String, String) {
        user_error(
            plan_drop(&parse_one(sql), table_name, table_exists)
                .expect_err("`{sql}` must be rejected"),
        )
    }

    /// The SQLSTATE and message a rejected `TRUNCATE` plan reports to the client.
    fn truncate_rejection(sql: &str, table_name: &str, table_exists: bool) -> (String, String) {
        user_error(
            plan_truncate(&parse_one(sql), table_name, table_exists)
                .expect_err("`{sql}` must be rejected"),
        )
    }

    #[test]
    fn drop_table_on_an_existing_table_proceeds() {
        assert_eq!(
            plan_drop(&parse_one("DROP TABLE t"), "t", true).unwrap(),
            DropPlan::DropTable
        );
    }

    #[test]
    fn drop_table_if_exists_on_a_missing_table_is_a_no_op() {
        assert_eq!(
            plan_drop(&parse_one("DROP TABLE IF EXISTS t"), "t", false).unwrap(),
            DropPlan::Nothing("DROP TABLE".to_string())
        );
    }

    #[test]
    fn drop_table_on_a_missing_table_reports_table_not_found() {
        let (code, msg) = rejection("DROP TABLE t", "t", false);
        assert_eq!(code, "42P01");
        assert!(msg.contains("table \"t\" does not exist"), "{msg}");
    }

    // The regression this whole helper exists for: before object_type was
    // checked, `DROP VIEW t` naming a table dropped the table and its data.
    // `DROP INDEX` no longer arrives here — it resolves through the index
    // namespace in `pgwire_handler::indexes`, which makes the same guarantee.
    #[test]
    fn dropping_a_table_under_a_non_table_kind_is_a_wrong_object_type_error() {
        for (sql, kind) in [
            ("DROP VIEW t", "VIEW"),
            ("DROP SEQUENCE t", "SEQUENCE"),
            ("DROP SCHEMA t", "SCHEMA"),
        ] {
            let (code, msg) = rejection(sql, "t", true);
            assert_eq!(code, "42809", "`{sql}` must be a wrong-object-type error");
            assert!(msg.contains(kind), "`{sql}` must name the kind, got: {msg}");
            assert!(
                msg.contains("DROP TABLE"),
                "`{sql}` must point at DROP TABLE, got: {msg}"
            );
        }
    }

    // `IF EXISTS` still has to be honored: VaireDB models no views, so there is
    // nothing that could exist and nothing to report.
    #[test]
    fn drop_non_table_kind_if_exists_is_a_no_op_when_no_table_shadows_it() {
        assert_eq!(
            plan_drop(&parse_one("DROP VIEW IF EXISTS v"), "v", false).unwrap(),
            DropPlan::Nothing("DROP VIEW".to_string())
        );
    }

    // Without a shadowing table the name resolves to nothing — the error must
    // name the object kind the client asked for, not "table".
    #[test]
    fn drop_non_table_kind_reports_the_requested_kind_not_table() {
        let (code, msg) = rejection("DROP VIEW v", "v", false);
        assert_eq!(code, "42P01");
        assert!(msg.contains("view \"v\" does not exist"), "{msg}");
    }

    // Only the first name is ever acted on, so reporting success would claim
    // work that never happened.
    #[test]
    fn drop_table_with_several_names_is_rejected() {
        let (code, msg) = rejection("DROP TABLE a, b", "a", true);
        assert_eq!(code, "0A000");
        assert!(msg.contains("more than one table"), "{msg}");
    }

    #[test]
    fn drop_table_cascade_or_restrict_is_rejected() {
        for sql in ["DROP TABLE t CASCADE", "DROP TABLE t RESTRICT"] {
            let (code, msg) = rejection(sql, "t", true);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("CASCADE/RESTRICT"), "`{sql}`: {msg}");
        }
    }

    #[test]
    fn a_non_drop_statement_is_a_syntax_error() {
        let (code, _) = rejection("DELETE FROM t", "t", true);
        assert_eq!(code, "42601");
    }

    // --- TRUNCATE ---

    // Both spellings mean the same command; `TABLE` is an optional keyword in
    // PostgreSQL and must not change the verdict.
    #[test]
    fn truncate_an_existing_table_proceeds_with_or_without_the_table_keyword() {
        for sql in ["TRUNCATE TABLE t", "TRUNCATE t"] {
            assert_eq!(
                plan_truncate(&parse_one(sql), "t", true).unwrap(),
                TruncatePlan::Truncate,
                "`{sql}` must be honored"
            );
        }
    }

    // `ONLY` and a trailing `*` choose between a table and its inheritance
    // children. VaireDB models no inheritance, so both already denote this one
    // table and neither is a reason to refuse the command.
    #[test]
    fn truncate_inheritance_decorations_are_accepted_as_this_table() {
        for sql in ["TRUNCATE TABLE ONLY t", "TRUNCATE TABLE t *"] {
            assert_eq!(
                plan_truncate(&parse_one(sql), "t", true).unwrap(),
                TruncatePlan::Truncate,
                "`{sql}` must be honored"
            );
        }
    }

    // CONTINUE IDENTITY is PostgreSQL's default and asks for nothing VaireDB
    // cannot do, so spelling it out must not turn a valid command into an error.
    #[test]
    fn truncate_continue_identity_is_accepted() {
        assert_eq!(
            plan_truncate(&parse_one("TRUNCATE TABLE t CONTINUE IDENTITY"), "t", true).unwrap(),
            TruncatePlan::Truncate
        );
    }

    #[test]
    fn truncate_a_missing_table_reports_table_not_found() {
        let (code, msg) = truncate_rejection("TRUNCATE TABLE t", "t", false);
        assert_eq!(code, "42P01");
        assert!(msg.contains("relation \"t\" does not exist"), "{msg}");
    }

    // PostgreSQL empties every named table in one transaction. A per-shard
    // broadcast cannot, and only the first name is ever acted on, so reporting
    // success would claim work that never happened.
    #[test]
    fn truncate_with_several_tables_is_rejected() {
        let (code, msg) = truncate_rejection("TRUNCATE TABLE a, b", "a", true);
        assert_eq!(code, "0A000");
        assert!(msg.contains("more than one table"), "{msg}");
    }

    #[test]
    fn truncate_cascade_or_restrict_is_rejected() {
        for sql in ["TRUNCATE TABLE t CASCADE", "TRUNCATE TABLE t RESTRICT"] {
            let (code, msg) = truncate_rejection(sql, "t", true);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("CASCADE/RESTRICT"), "`{sql}`: {msg}");
        }
    }

    #[test]
    fn truncate_restart_identity_is_rejected() {
        let (code, msg) = truncate_rejection("TRUNCATE TABLE t RESTART IDENTITY", "t", true);
        assert_eq!(code, "0A000");
        assert!(msg.contains("RESTART IDENTITY"), "{msg}");
    }

    #[test]
    fn a_non_truncate_statement_is_a_syntax_error() {
        let (code, _) = truncate_rejection("DELETE FROM t", "t", true);
        assert_eq!(code, "42601");
    }

    // --- ALTER TABLE: rename vs. reshape ---

    /// The plan a `ALTER TABLE` operation list produces.
    fn alter_plan(sql: &str) -> AlterPlan {
        plan_alter(&parse_alter_ops(sql)).unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
    }

    /// The SQLSTATE and message a rejected `ALTER TABLE` plan reports.
    fn alter_rejection(sql: &str) -> (String, String) {
        user_error(plan_alter(&parse_alter_ops(sql)).expect_err("`{sql}` must be rejected"))
    }

    #[test]
    fn rename_to_is_planned_as_a_rename() {
        assert_eq!(
            alter_plan("ALTER TABLE orders RENAME TO archived_orders"),
            AlterPlan::Rename("archived_orders".to_string())
        );
    }

    // The destination becomes a catalog key, so it must be folded the same way
    // every other table name is: unquoted lowercased, quoted verbatim. A raw-cased
    // key would leave the table unreachable by the name the client just chose.
    #[test]
    fn the_rename_destination_is_canonicalized() {
        assert_eq!(
            alter_plan("ALTER TABLE orders RENAME TO ArchivedOrders"),
            AlterPlan::Rename("archivedorders".to_string())
        );
        assert_eq!(
            alter_plan("ALTER TABLE orders RENAME TO \"ArchivedOrders\""),
            AlterPlan::Rename("ArchivedOrders".to_string())
        );
    }

    // `RENAME COLUMN` edits a column list; it must not be mistaken for a table
    // rename, which would re-key the catalog and leave the column untouched.
    #[test]
    fn column_operations_are_not_renames() {
        for sql in [
            "ALTER TABLE orders ADD COLUMN status VARCHAR",
            "ALTER TABLE orders DROP COLUMN status",
            "ALTER TABLE orders RENAME COLUMN amount TO total",
            "ALTER TABLE orders ALTER COLUMN amount SET NOT NULL",
        ] {
            assert_eq!(alter_plan(sql), AlterPlan::ColumnOps, "`{sql}`");
        }
    }

    // A rename moves rows between catalog keys and a column op rewrites one of
    // them; running both would either lose the schema change or apply it to a
    // table that no longer exists. PostgreSQL calls the combination a syntax
    // error, so we do too.
    #[test]
    fn a_rename_combined_with_another_action_is_a_syntax_error() {
        let ops = [
            parse_alter_ops("ALTER TABLE orders RENAME TO archived_orders").remove(0),
            parse_alter_ops("ALTER TABLE orders ADD COLUMN status VARCHAR").remove(0),
        ];
        let (code, msg) = user_error(plan_alter(&ops).expect_err("mixed actions must be rejected"));
        assert_eq!(code, "42601");
        assert!(msg.contains("RENAME TO"), "{msg}");
    }

    // A rename stays in the table's schema, so a qualifier is either redundant or a
    // request to move the table — and honoring it as the former silently would rename
    // the table to something the client did not ask for.
    #[test]
    fn a_schema_qualified_rename_destination_is_a_syntax_error() {
        let (code, msg) = alter_rejection("ALTER TABLE orders RENAME TO myschema.orders");
        assert_eq!(code, "42601");
        assert!(msg.contains("schema-qualified"), "{msg}");
    }

    // A constraint change builds or drops one object per shard, so it cannot ride
    // the generic per-shard `ALTER TABLE` broadcast the column ops use.
    #[test]
    fn constraint_actions_take_their_own_lane() {
        assert!(matches!(
            alter_plan("ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id)"),
            AlterPlan::AddConstraint {
                not_valid: false,
                ..
            }
        ));
        assert_eq!(
            alter_plan("ALTER TABLE orders DROP CONSTRAINT uq"),
            AlterPlan::DropConstraint {
                name: "uq".to_string(),
                if_exists: false,
            }
        );
        assert_eq!(
            alter_plan("ALTER TABLE orders DROP CONSTRAINT IF EXISTS uq"),
            AlterPlan::DropConstraint {
                name: "uq".to_string(),
                if_exists: true,
            }
        );
    }

    // The name resolves back to per-shard index names, which are built from the
    // folded form; a raw-cased key would make the constraint undroppable.
    #[test]
    fn the_dropped_constraint_name_is_canonicalized() {
        assert_eq!(
            alter_plan("ALTER TABLE orders DROP CONSTRAINT UqOrders"),
            AlterPlan::DropConstraint {
                name: "uqorders".to_string(),
                if_exists: false,
            }
        );
        assert_eq!(
            alter_plan("ALTER TABLE orders DROP CONSTRAINT \"UqOrders\""),
            AlterPlan::DropConstraint {
                name: "UqOrders".to_string(),
                if_exists: false,
            }
        );
    }

    // Two broadcasts in one statement: if the second failed there would be no way
    // to undo the first, so the combination is refused before either runs.
    #[test]
    fn a_constraint_action_combined_with_another_action_is_a_syntax_error() {
        for (first, expected) in [
            (
                "ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id)",
                "ADD CONSTRAINT",
            ),
            ("ALTER TABLE orders DROP CONSTRAINT uq", "DROP CONSTRAINT"),
        ] {
            let ops = [
                parse_alter_ops(first).remove(0),
                parse_alter_ops("ALTER TABLE orders ADD COLUMN status VARCHAR").remove(0),
            ];
            let (code, msg) =
                user_error(plan_alter(&ops).expect_err("mixed actions must be rejected"));
            assert_eq!(code, "42601", "`{first}`");
            assert!(msg.contains(expected), "`{first}`: {msg}");
        }
    }

    // Nothing in the catalog depends on a constraint, so a referential action
    // could only be accepted and ignored.
    #[test]
    fn a_drop_constraint_with_a_drop_behavior_is_rejected() {
        for sql in [
            "ALTER TABLE orders DROP CONSTRAINT uq CASCADE",
            "ALTER TABLE orders DROP CONSTRAINT uq RESTRICT",
        ] {
            let (code, msg) = alter_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("CASCADE/RESTRICT"), "`{sql}`: {msg}");
        }
    }

    // These name a constraint by kind rather than by name, so they cannot be
    // resolved to the per-shard objects that enforce it.
    #[test]
    fn constraint_operations_without_a_name_are_refused_before_the_catalog_is_read() {
        for (sql, expected) in [
            ("ALTER TABLE orders DROP PRIMARY KEY", "PRIMARY KEY"),
            ("ALTER TABLE orders DROP FOREIGN KEY fk", "FOREIGN KEY"),
            ("ALTER TABLE orders DROP INDEX idx_id", "DROP INDEX"),
        ] {
            let (code, msg) = alter_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(expected), "`{sql}`: {msg}");
        }
    }

    // The shards' engine refuses to rename a table an index depends on, so the
    // broadcast would fail on every node — after the destination name had already
    // been claimed. The guard runs first, names what to drop, and leaves the
    // catalog exactly as it was.
    #[tokio::test]
    async fn renaming_an_indexed_table_is_rejected_naming_the_indexes() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_test_table(&handler, "orders");
        let mut table_meta = handler.catalog.get_table("orders").unwrap().unwrap();
        for name in ["idx_id", "idx_id_desc"] {
            table_meta.indexes.push(crate::catalog::IndexMeta {
                name: name.to_string(),
                columns: vec!["id".to_string()],
                unique: false,
            });
        }

        let (code, msg) = user_error(
            handler
                .rename_table(table_meta, "invoices")
                .await
                .err()
                .unwrap_or_else(|| panic!("a rename of an indexed table must be rejected")),
        );
        assert_eq!(code, "0A000");
        assert!(
            msg.contains("\"idx_id\"") && msg.contains("\"idx_id_desc\""),
            "got: {msg}"
        );
        assert!(
            msg.contains("DROP INDEX"),
            "the message must say what to do, got: {msg}"
        );
        // The destination was never claimed, and the source still resolves.
        assert!(!is_registered(&handler, "invoices"));
        assert!(is_registered(&handler, "orders"));
    }

    // A constraint enforced by per-shard indexes is the same dependency, and the
    // statement that removes it is not DROP INDEX.
    #[tokio::test]
    async fn renaming_a_table_with_an_index_backed_constraint_names_drop_constraint() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_test_table(&handler, "orders");
        let mut table_meta = handler.catalog.get_table("orders").unwrap().unwrap();
        table_meta.constraints.push(crate::catalog::ConstraintMeta {
            name: "uq_id".to_string(),
            kind: crate::catalog::ConstraintKind::Unique as i32,
            columns: vec!["id".to_string()],
            definition: "UNIQUE (id)".to_string(),
            index_backed: true,
        });

        let (code, msg) = user_error(
            handler
                .rename_table(table_meta, "invoices")
                .await
                .err()
                .unwrap_or_else(|| panic!("a rename must be rejected")),
        );
        assert_eq!(code, "0A000");
        assert!(msg.contains("DROP CONSTRAINT \"uq_id\""), "got: {msg}");
        assert!(!is_registered(&handler, "invoices"));
    }

    // --- CREATE TABLE ... AS SELECT ---

    /// Run a CREATE TABLE against a handler with an empty catalog and no reachable
    /// nodes. Enough for every rule that decides whether the statement may run.
    async fn create(handler: &VaireDbQueryHandler, sql: &str) -> PgWireResult<Response> {
        let session = SessionState::default();
        handler.handle_create_table(&parse_one(sql), &session).await
    }

    /// The SQLSTATE and message a rejected CREATE TABLE reports.
    async fn create_rejection(handler: &VaireDbQueryHandler, sql: &str) -> (String, String) {
        user_error(
            create(handler, sql)
                .await
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be rejected")),
        )
    }

    /// Whether the catalog holds a table under this name.
    fn is_registered(handler: &VaireDbQueryHandler, name: &str) -> bool {
        matches!(handler.catalog.get_table(name), Ok(Some(_)))
    }

    // The columns come from the query, so a client list would have to agree with
    // the result schema in width, order and type. Aliases in the SELECT say the
    // same thing once, so the two-places-to-disagree form is refused.
    #[tokio::test]
    async fn a_column_list_alongside_as_select_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = create_rejection(
            &handler,
            "CREATE TABLE dst (id INT, v TEXT) WITH (shard_by = 'id') AS SELECT 1 AS id, 'a' AS v",
        )
        .await;
        assert_eq!(code, "0A000");
        assert!(msg.contains("column list"), "got: {msg}");
        assert!(!is_registered(&handler, "dst"));
    }

    // A table's shard key is fixed for its lifetime. Taking it from whichever
    // column the SELECT projects first would pick it by accident, so the statement
    // has to say it — and is refused before the query runs.
    #[tokio::test]
    async fn a_ctas_without_a_shard_key_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        for sql in [
            "CREATE TABLE dst AS SELECT 1 AS id",
            "CREATE TABLE dst WITH (shards = 3) AS SELECT 1 AS id",
        ] {
            let (code, msg) = create_rejection(&handler, sql).await;
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("shard key"), "`{sql}` got: {msg}");
            assert!(msg.contains("shard_by"), "`{sql}` got: {msg}");
            assert!(!is_registered(&handler, "dst"), "`{sql}`");
        }
    }

    // The name is taken, so the query is moot: reported without running it.
    #[tokio::test]
    async fn an_existing_name_is_reported_before_the_query_runs() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_test_table(&handler, "dst");

        let (code, msg) = create_rejection(
            &handler,
            "CREATE TABLE dst WITH (shard_by = 'id') AS SELECT 1 AS id",
        )
        .await;
        assert_eq!(code, "42P07");
        assert!(msg.contains("already exists"), "got: {msg}");

        // `IF NOT EXISTS` turns the same case into a no-op, again without reading.
        let tag = create(
            &handler,
            "CREATE TABLE IF NOT EXISTS dst WITH (shard_by = 'id') AS SELECT 1 AS id",
        )
        .await
        .expect("IF NOT EXISTS must succeed against an existing table");
        assert!(matches!(tag, Response::Execution(_)));
    }

    // The shard key has to name a column the query actually produces, or every
    // later write would route on a column the table does not have.
    #[tokio::test]
    async fn a_shard_key_absent_from_the_result_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = create_rejection(
            &handler,
            "CREATE TABLE dst WITH (shard_by = 'customer') AS SELECT 1 AS id, 'a' AS v",
        )
        .await;
        assert_eq!(code, "42703");
        assert!(msg.contains("shard key"), "got: {msg}");
        assert!(msg.contains("customer"), "got: {msg}");
        // The columns it could have named are listed, so the fix is obvious.
        assert!(msg.contains("id") && msg.contains('v'), "got: {msg}");
        assert!(!is_registered(&handler, "dst"));
    }

    // A result column VaireDB cannot re-emit as a literal would create a table
    // that could never be filled, so the type is refused with the column named.
    #[tokio::test]
    async fn a_result_column_with_no_storable_type_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = create_rejection(
            &handler,
            "CREATE TABLE dst WITH (shard_by = 'id') AS SELECT 1 AS id, NULL AS v",
        )
        .await;
        assert_eq!(code, "0A000");
        assert!(
            msg.contains("\"v\""),
            "the column must be named, got: {msg}"
        );
        assert!(msg.contains("cast"), "got: {msg}");
        assert!(!is_registered(&handler, "dst"));
    }

    // Everything the statement alone can be judged on has passed and the query has
    // run: what stops this CTAS is the cluster, reported as such — and it stops
    // before the name is claimed, so a retry against a healthy cluster is clean.
    #[tokio::test]
    async fn a_valid_ctas_gets_as_far_as_the_cluster_check() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = create_rejection(
            &handler,
            "CREATE TABLE dst WITH (shard_by = 'id') AS SELECT 1 AS id, 'a' AS v",
        )
        .await;
        assert_eq!(code, "0A000");
        assert!(
            msg.contains("replication_factor") && msg.contains("core nodes"),
            "the derived columns must reach the create, got: {msg}"
        );
        assert!(!is_registered(&handler, "dst"));
    }

    /// Register a minimal table so a name is taken in the catalog.
    fn register_test_table(handler: &VaireDbQueryHandler, name: &str) {
        handler
            .catalog
            .put_table(&TableMeta {
                table_name: name.to_string(),
                columns: vec![crate::catalog::ColumnDef {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                }],
                shard_key: "id".to_string(),
                shard_count: 1,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
    }
}
