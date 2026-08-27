//! DML (INSERT/UPDATE/DELETE) handling for the coordinator's pgwire handler.
//!
//! Resolves the target table's shard layout from the catalog, validates
//! shard-key constraints, then routes each write to the owning shard(s) and
//! executes it under quorum via the replication manager. Multi-row INSERTs whose
//! rows span shards are split so each shard receives only the rows it owns.

use std::collections::HashMap;

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
use crate::query_router::{self, QueryType};
use crate::sql_compat;
use crate::write_router::compute_shard_index;

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

impl VaireDbQueryHandler {
    /// Route and execute a single INSERT/UPDATE/DELETE, returning the pgwire
    /// command tag with the rows-affected count. Resolves the target table,
    /// enforces shard-key rules (INSERTs must specify the shard key; UPDATEs may
    /// not mutate it), and dispatches to the owning shards under quorum. Returns
    /// an error if the table is unknown or a shard-key constraint is violated.
    pub(super) async fn handle_dml(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
        params: &[ScalarValue],
    ) -> PgWireResult<Response> {
        let table_name = query_router::extract_table_name(stmt).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine target table",
            )
        })?;

        // Writes to the anonymization-secret system table are handled in the
        // coordinator's catalog, never routed to a shard.
        if targets_secret_table(stmt) {
            return self.handle_anonymization_secret_insert(stmt, query_type);
        }

        let dml_ctx = ErrorContext::for_table(&table_name);

        let table_meta = self
            .catalog
            .get_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &dml_ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(table_name.clone());
                enrich_coordinator_error(&err, &dml_ctx, &self.catalog)
            })?;

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
                sql_compat::validate_insert_shard_key(stmt, &table_meta.shard_key, params)
                    .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;
            }
            QueryType::Update
                if sql_compat::update_targets_shard_key(stmt, &table_meta.shard_key) =>
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
        let write_id = uuid::Uuid::new_v4().to_string();

        let total_rows = if *query_type == QueryType::Insert {
            self.handle_insert_with_split(stmt, &table_meta, quorum_size, &write_id, params)
                .await?
        } else {
            let target_shards = self
                .write_router
                .resolve_target_shards(stmt, &table_meta, params)
                .map_err(|e| enrich_coordinator_error(&e, &dml_ctx, &self.catalog))?;

            self.execute_write_on_each_shard(
                &target_shards,
                stmt,
                params,
                &write_id,
                quorum_size,
                &dml_ctx,
            )
            .await?
        };

        let tag = match query_type {
            QueryType::Insert => Tag::new("INSERT")
                .with_oid(0)
                .with_rows(total_rows as usize),
            QueryType::Update => Tag::new("UPDATE").with_rows(total_rows as usize),
            QueryType::Delete => Tag::new("DELETE").with_rows(total_rows as usize),
            _ => Tag::new("OK"),
        };

        Ok(Response::Execution(tag))
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

    /// Execute an INSERT, splitting a multi-row statement across shards when its
    /// rows hash to different buckets so each shard receives only the rows it
    /// owns. Single-row or single-shard INSERTs route whole via the shared
    /// resolver. Returns the total rows inserted, or an error if the table has no
    /// shards or the statement cannot be split.
    async fn handle_insert_with_split(
        &self,
        stmt: &Statement,
        table_meta: &TableMeta,
        quorum_size: usize,
        write_id: &str,
        params: &[ScalarValue],
    ) -> PgWireResult<u64> {
        let insert_ctx = ErrorContext::for_table(&table_meta.table_name)
            .with_replication(table_meta.replication_factor);

        let all_shards = self
            .catalog
            .get_shards_for_table(&table_meta.table_name)
            .map_err(|e| enrich_coordinator_error(&e, &insert_ctx, &self.catalog))?;

        if all_shards.is_empty() {
            let err = CoordinatorError::ShardNotAssigned(format!(
                "no shards for table {}",
                table_meta.table_name
            ));
            return Err(enrich_coordinator_error(&err, &insert_ctx, &self.catalog));
        }

        let row_keys =
            sql_compat::extract_insert_row_shard_keys(stmt, &table_meta.shard_key, params);

        // A multi-row INSERT whose rows hash to different shards must be split:
        // each shard receives only the rows it owns. A single-row (or single-shard)
        // INSERT routes whole via the shared resolver.
        match row_keys {
            Some(keys) if keys.len() > 1 => {
                let mut shard_rows: HashMap<usize, Vec<usize>> = HashMap::new();
                for (row_idx, key_value) in &keys {
                    let shard_idx = compute_shard_index(key_value, all_shards.len());
                    shard_rows.entry(shard_idx).or_default().push(*row_idx);
                }

                let mut total_rows = 0u64;
                for (shard_idx, row_indices) in &shard_rows {
                    let shard = &all_shards[*shard_idx];
                    let split_stmt = sql_compat::split_insert_by_rows(stmt, row_indices)
                        .ok_or_else(|| {
                            enrich_generic_error(&"failed to split INSERT by shard", &insert_ctx)
                        })?;
                    let shard_write_id = format!("{}-{}", write_id, shard_idx);
                    total_rows += self
                        .dispatch_write(
                            shard,
                            &split_stmt,
                            params,
                            &shard_write_id,
                            quorum_size,
                            &insert_ctx,
                        )
                        .await?;
                }
                Ok(total_rows)
            }
            _ => {
                let target_shards = self
                    .write_router
                    .resolve_target_shards(stmt, table_meta, params)
                    .map_err(|e| enrich_coordinator_error(&e, &insert_ctx, &self.catalog))?;

                self.execute_write_on_each_shard(
                    &target_shards,
                    stmt,
                    params,
                    write_id,
                    quorum_size,
                    &insert_ctx,
                )
                .await
            }
        }
    }

    /// Apply `stmt` to every shard in `shards`, summing the rows affected. Each
    /// shard gets a write id suffixed by its position so the storage layer can
    /// dedup retries per shard.
    async fn execute_write_on_each_shard(
        &self,
        shards: &[ShardMeta],
        stmt: &Statement,
        params: &[ScalarValue],
        write_id: &str,
        quorum_size: usize,
        ctx: &ErrorContext,
    ) -> PgWireResult<u64> {
        let mut total_rows = 0u64;
        for (idx, shard) in shards.iter().enumerate() {
            let shard_write_id = format!("{}-{}", write_id, idx);
            total_rows += self
                .dispatch_write(shard, stmt, params, &shard_write_id, quorum_size, ctx)
                .await?;
        }
        Ok(total_rows)
    }

    /// Rewrite `stmt` to its shard-local form and execute it against one shard
    /// under quorum, returning the rows affected. The single choke point through
    /// which every sharded write — split INSERT, broadcast UPDATE/DELETE, or
    /// single-shard route — passes.
    async fn dispatch_write(
        &self,
        shard: &ShardMeta,
        stmt: &Statement,
        params: &[ScalarValue],
        shard_write_id: &str,
        quorum_size: usize,
        ctx: &ErrorContext,
    ) -> PgWireResult<u64> {
        let (shard_sql, shard_params) = self
            .write_router
            .generate_shard_local_sql(stmt, shard, params)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?;
        self.replication_manager
            .execute_write_with_quorum(
                shard,
                &shard_sql,
                &shard_params,
                shard_write_id,
                quorum_size,
            )
            .await
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))
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

    let columns: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();
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

    fn parse_one(sql: &str) -> Statement {
        sql_compat::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
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
}
