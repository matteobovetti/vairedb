//! DDL (CREATE/DROP/ALTER TABLE) handling for the coordinator's pgwire handler.
//!
//! Updates the metadata catalog and broadcasts shard-local DDL to every replica
//! of every shard. CREATE TABLE writes the catalog first, then rolls it back on
//! partial broadcast failure; DROP and ALTER broadcast best-effort first and
//! mutate the catalog only once every node was reached, so a failed command
//! leaves the catalog unchanged. After any successful DDL the local and
//! distributed DataFusion catalog views are refreshed.

use std::collections::HashMap;

use crate::sqlparser::ast::Statement;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use tonic::transport::Channel;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ShardMeta, ShardStrategy, TableMeta};
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::table_meta_ops::{apply_alter_operation, parse_create_table_config};
use crate::query_router;
use crate::scheduler;
use crate::sql_compat;
use crate::util::{now_unix_secs, shard_table_name};

impl VaireDbQueryHandler {
    /// Create a table: validate the requested replication factor against alive
    /// nodes, persist its metadata, assign shards round-robin, then broadcast the
    /// shard-local CREATE to each replica. If any node rejects the DDL the
    /// partial creation is rolled back. Returns `TableAlreadyExists` when the
    /// relation exists without `IF NOT EXISTS`, or `FeatureNotSupported` if the
    /// replication factor exceeds the node count.
    pub(super) async fn handle_create_table(&self, stmt: &Statement) -> PgWireResult<Response> {
        let Statement::CreateTable(create) = stmt else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "expected CREATE TABLE statement",
            ));
        };

        let table_name = query_router::canonical_table_name(&create.name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;

        if create.if_not_exists {
            if let Ok(Some(_)) = self.catalog.get_table(&table_name) {
                return Ok(Response::Execution(Tag::new("CREATE TABLE")));
            }
        } else if let Ok(Some(_)) = self.catalog.get_table(&table_name) {
            return Err(make_vdb_error(
                VdbErrorCode::TableAlreadyExists,
                format!("relation \"{}\" already exists", table_name),
            ));
        }

        let config = parse_create_table_config(create, self.default_replication_factor)?;

        let ddl_ctx = ErrorContext::for_table(&table_name);

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
        };

        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ddl_ctx, &self.catalog))?;

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

    /// Undo a CREATE TABLE that failed partway through broadcasting: send a
    /// best-effort `DROP TABLE` to every node that already created its shard,
    /// then delete the catalog rows. Failures here are logged, not surfaced —
    /// the original DDL error is what the client should see.
    async fn rollback_partial_create(
        &self,
        successful_sends: &[(String, String, String)],
        table_name: &str,
    ) {
        for (address, drop_sql, shard_id) in successful_sends {
            let rollback_write_id = uuid::Uuid::new_v4().to_string();
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
            if let Err(re) = send_ddl_to_node(channel, &rollback_write_id, drop_sql, shard_id).await
            {
                tracing::error!(
                    address = %address,
                    shard_id = %shard_id,
                    error = %re,
                    "DDL rollback (DROP) failed"
                );
            }
        }

        let _ = self.catalog.delete_shards_for_table(table_name);
        let _ = self.catalog.delete_table(table_name);
    }

    /// Drop a table: broadcast a best-effort shard-local `DROP TABLE IF EXISTS`
    /// to every replica, then remove the catalog rows and deregister it from the
    /// local DataFusion session. Returns `TableNotFound` when the relation is
    /// absent without `IF EXISTS`, or a `NodeCommunicationError` if any node was
    /// unreachable.
    pub(super) async fn handle_drop_table(&self, stmt: &Statement) -> PgWireResult<Response> {
        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;

        let if_exists = matches!(
            stmt,
            Statement::Drop {
                if_exists: true,
                ..
            }
        );

        let drop_ctx = ErrorContext::for_table(&table_name);

        let table_exists = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &drop_ctx, &self.catalog))?
            .is_some();

        if !table_exists {
            if if_exists {
                return Ok(Response::Execution(Tag::new("DROP TABLE")));
            }
            return Err(make_vdb_error(
                VdbErrorCode::TableNotFound,
                format!("table \"{}\" does not exist", table_name),
            ));
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

    /// Alter a table: apply each operation to the cached metadata, broadcast the
    /// rewritten shard-local ALTER to every replica best-effort, and persist the
    /// updated metadata only once every node was reached. Returns `TableNotFound`
    /// when the relation is absent without `IF EXISTS`, or a
    /// `NodeCommunicationError` if any node was unreachable — in which case the
    /// catalog is left unchanged.
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

    /// Broadcast a per-shard DDL statement to every replica of every shard,
    /// best-effort. `make_shard_sql` produces the SQL to run on the shard (a
    /// shard-local `DROP`, rewritten `ALTER`, etc.). Returns the de-dup-pending
    /// list of node IDs that could not be reached or rejected the statement, so
    /// the caller can decide whether a partial failure is fatal.
    async fn broadcast_ddl_best_effort(
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

    /// Drop the table from the local DataFusion session so a later re-create or
    /// schema change re-registers it fresh. Logged, never fatal.
    fn deregister_table_after_ddl(&self, table_name: &str) {
        let table_ref = datafusion::common::TableReference::bare(table_name.to_string());
        if let Err(e) = self.session_ctx.deregister_table(table_ref) {
            tracing::warn!(
                "failed to deregister table '{}' from DataFusion session: {}",
                table_name,
                e
            );
        }
    }

    /// Refresh the Ballista/DataFusion catalog view after a successful DDL so
    /// distributed reads see the new schema. Logged, never fatal.
    fn refresh_catalog_after_ddl(&self, op_label: &str) {
        if let Err(e) = scheduler::refresh_ballista_catalog_tables(&self.session_ctx, &self.catalog)
        {
            tracing::warn!("failed to refresh DataFusion catalog after {op_label}: {e}");
        }
    }
}

/// Turn a partial-broadcast failure list into a client-facing error, or `Ok` if
/// every node was reached. Deduplicates the node list first so the count
/// reflects distinct unreachable nodes rather than per-shard attempts.
fn fail_if_unreachable(op_label: &str, mut failed_nodes: Vec<String>) -> PgWireResult<()> {
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
    sql_compat::rewrite_to_shard_local(&mut ddl_stmt, &format!("shard{}", shard.hash_bucket));
    sql_compat::transform_to_duckdb(&mut ddl_stmt);
    sql_compat::statement_to_sql(&ddl_stmt)
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
