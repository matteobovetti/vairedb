//! `MERGE INTO`: one statement that updates, inserts and deletes rows of a
//! sharded table in a single pass over a source relation.
//!
//! A MERGE is the first write VaireDB routes that is about **two** relations. An
//! `UPDATE` can be broadcast to every shard because each shard evaluates the
//! `WHERE` against its own rows and the answer is the same as it would be
//! centrally. A MERGE cannot, in general: a shard evaluating `ON t.k = s.k` sees
//! only the source rows *it* holds, so a target row whose match lives on another
//! shard looks unmatched — and `WHEN NOT MATCHED THEN INSERT` would then store a
//! duplicate. Broadcasting a MERGE blindly is therefore not a partial answer but a
//! wrong one.
//!
//! What makes it work is the hash: **equal shard keys always hash to the same
//! shard.** So when the `ON` clause requires the target's shard key to equal a
//! source column, and the source's rows are placed by that same column, a target
//! row's only possible match is on its own shard. Every shard can then run the
//! whole statement against its own two tables, and the union of their results is
//! exactly the centralized answer. That is the one property this module checks
//! for, in two shapes:
//!
//! **A source table, fanned out.** `USING other_table` where the other table is
//! sharded the same way (same shard count), on the column the `ON` clause matches,
//! and every replica of a target shard also holds the matching source shard — so
//! the statement the coordinator ships can actually read both. The MERGE then runs
//! once per shard, rewritten so both relations name that shard's physical tables.
//! `WHEN NOT MATCHED BY SOURCE` is correct here too: a target row's key can only
//! appear in its own shard's source table, so a row no shard matched is a row the
//! source does not contain.
//!
//! **An inline row list, split per shard.** `USING (VALUES …) AS s(k, v)` — the
//! coordinator hashes each row's key itself and sends each shard a MERGE carrying
//! only the rows it owns, the way it splits a multi-row `INSERT`. `WHEN NOT
//! MATCHED BY SOURCE` is refused for this shape alone: a shard that owns none of
//! the rows receives no statement, and its target rows are exactly the ones that
//! clause is about, so it would apply to part of the table and report success.
//!
//! Everything else is refused with `0A000` naming what to change, rather than
//! broadcast and hoped for. See [`crate::write_sql_cl::merge`] for the AST-level
//! rules and the reasons behind each one.
//!
//! **Not atomic across shards.** Like every other multi-shard write in v0.1: each
//! shard's MERGE commits on its own, and a failure part-way through leaves the
//! shards that already ran it applied. The reported row count is the sum of what
//! the shards actually did.

use std::collections::{BTreeMap, HashMap, HashSet};

use datafusion::scalar::ScalarValue;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ShardMeta, TableMeta};
use crate::error::CoordinatorError;
use crate::pgwire_handler::dml::{DmlPlan, PlannedWrite, dml_tag};
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, enrich_generic_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router::QueryType;
use crate::sqlparser::ast::Statement;
use crate::write_router::{compute_shard_index, shard_for_bucket};
use crate::write_sql_cl::{
    MergeSource, ensure_merge_relation_aliases, materialize_merge_insert_columns,
    merge_has_not_matched_by_source, merge_key_column, merge_row_shard_keys, merge_shape,
    normalize_merge_column_qualifiers, split_merge_by_rows, validate_merge,
};

impl VaireDbQueryHandler {
    /// Apply a `MERGE INTO`, shard by shard.
    ///
    /// Returns `TableNotFound` (`42P01`) if the target or a table source does not
    /// exist, `FeatureNotSupported` (`0A000`) for a shape whose rows VaireDB cannot
    /// prove will meet the right target rows (see the module documentation), and a
    /// `NodeCommunicationError` if a shard could not be reached — in which case the
    /// shards already reached have applied their part.
    pub(super) async fn handle_merge(
        &self,
        stmt: &Statement,
        params: &[ScalarValue],
    ) -> PgWireResult<Response> {
        // Normalized first, so every check below reads the statement the shards
        // will actually receive: the relations carry explicit aliases (the
        // shard-local rewrite renames them out from under the ON clause otherwise)
        // and the written column names have lost the target qualifier the shards'
        // engine would reject.
        let mut stmt = stmt.clone();
        ensure_merge_relation_aliases(&mut stmt);
        normalize_merge_column_qualifiers(&mut stmt).map_err(unsupported)?;
        let shape = merge_shape(&stmt).map_err(unsupported)?;

        let ctx = ErrorContext::for_table(&shape.target_table);
        let target = self.require_table(&shape.target_table, &ctx)?;

        // The pseudonymization rewrite covers INSERT and UPDATE statements only, so
        // a MERGE would reach the shards with the plaintext the client wrote —
        // exactly what a pseudonymized column exists to prevent.
        if !target.anonymized_columns.is_empty() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "MERGE is not supported on \"{}\" because it has pseudonymized columns: \
                     VaireDB hashes a written value before it leaves the coordinator, and it \
                     cannot yet do so for a MERGE. Use INSERT ... ON CONFLICT or UPDATE instead",
                    shape.target_table
                ),
            ));
        }

        // `INSERT VALUES (...)` without a column list means the table's leading
        // columns, and every check after this point locates a column by name.
        let columns: Vec<&str> = target.columns.iter().map(|c| c.name.as_str()).collect();
        materialize_merge_insert_columns(&mut stmt, &columns)
            .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;

        // The predicate everything rests on: without it, a target row's match could
        // live on any shard.
        let source_key = merge_key_column(&stmt, &shape, &target.shard_key).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "MERGE requires the ON clause to match shard key column \"{}\" of \"{}\" \
                     against a source column, as in ON {}.\"{}\" = {}.<column>, with both sides \
                     qualified and joined by AND. Rows with equal shard keys always live on the \
                     same shard, which is what lets VaireDB apply the MERGE shard by shard; any \
                     other join condition could match rows on a shard the statement never reaches",
                    target.shard_key,
                    shape.target_table,
                    shape.target_qualifier,
                    target.shard_key,
                    source_qualifier(&shape.source),
                ),
            )
        })?;

        validate_merge(&stmt, &target.shard_key, &source_key).map_err(unsupported)?;

        let quorum_size = self
            .write_router
            .compute_quorum_size(target.replication_factor);
        let ctx = ctx.with_replication(target.replication_factor);
        let target_shards = self.shards_of(&target, &ctx)?;

        let writes = match &shape.source {
            MergeSource::Table { name, .. } => {
                let source = self.require_table(name, &ctx)?;
                let source_shards = self.shards_of(&source, &ctx)?;
                check_table_source(
                    &target,
                    &source,
                    &source_key,
                    &target_shards,
                    &source_shards,
                )
                .map_err(unsupported)?;

                // Every shard runs the whole statement against its own pair of
                // tables. The fan-out is not a fallback: `WHEN NOT MATCHED` inserts
                // source rows the ON clause matched nothing for, and `WHEN NOT
                // MATCHED BY SOURCE` touches target rows the source does not
                // contain — both of which exist on shards no single-shard narrowing
                // would visit, so narrowing would silently skip work.
                self.plan_writes_on_each_shard(&target_shards, &stmt, params, quorum_size, &ctx)?
            }
            MergeSource::Values { columns, .. } => {
                if merge_has_not_matched_by_source(&stmt) {
                    return Err(make_vdb_error(
                        VdbErrorCode::FeatureNotSupported,
                        "WHEN NOT MATCHED BY SOURCE is not supported on a MERGE whose source is a \
                         VALUES list: VaireDB sends each shard only the rows it owns, so a shard \
                         holding none of them would never run the clause and its rows — the ones \
                         the clause is about — would be left untouched. Merge from a table sharded \
                         the same way, or use DELETE with an explicit condition",
                    ));
                }

                let key_index = columns
                    .iter()
                    .position(|column| *column == source_key)
                    .ok_or_else(|| {
                        enrich_generic_error(
                            &"the MERGE source's key column is not one of its named columns",
                            &ctx,
                        )
                    })?;

                let row_keys = merge_row_shard_keys(&stmt, key_index, params).ok_or_else(|| {
                    make_vdb_error(
                        VdbErrorCode::FeatureNotSupported,
                        format!(
                            "every row of a MERGE's VALUES source must supply a literal (or \
                                 bound parameter) in column \"{source_key}\", the one the ON \
                                 clause matches shard key \"{}\" against: VaireDB hashes it to \
                                 decide which shard the row is merged on, and a value it cannot \
                                 evaluate would have to be sent to every shard",
                            target.shard_key
                        ),
                    )
                })?;

                self.plan_merge_by_rows(&stmt, &row_keys, &target, &target_shards, params, &ctx)?
            }
        };

        let plan = DmlPlan {
            writes,
            table_name: shape.target_table,
            // Only the shards can say how many rows each clause matched, which is
            // also why a MERGE cannot be buffered inside a transaction block.
            static_row_count: None,
            ctx,
        };
        let rows = self.execute_plan(&plan).await?;
        Ok(Response::Execution(dml_tag(&QueryType::Merge, rows)))
    }

    /// One write per shard for a MERGE from a VALUES list: each shard receives the
    /// same MERGE carrying only the rows whose key it owns.
    ///
    /// `row_keys` pairs a row's index in the VALUES list with the canonical form of
    /// its key. Rows are hashed and resolved by bucket exactly as
    /// [`VaireDbQueryHandler::plan_insert_with_split`] does, so a MERGE looks for a
    /// row on the shard the INSERT that stored it chose. Keyed by a `BTreeMap` so
    /// the shards are planned in bucket order, which makes the plan for one
    /// statement reproducible.
    fn plan_merge_by_rows(
        &self,
        stmt: &Statement,
        row_keys: &[(usize, String)],
        target: &TableMeta,
        target_shards: &[ShardMeta],
        params: &[ScalarValue],
        ctx: &ErrorContext,
    ) -> PgWireResult<Vec<PlannedWrite>> {
        let quorum_size = self
            .write_router
            .compute_quorum_size(target.replication_factor);

        let mut shard_rows: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (row_idx, key_value) in row_keys {
            let bucket = compute_shard_index(key_value, target_shards.len());
            shard_rows.entry(bucket).or_default().push(*row_idx);
        }

        let mut writes = Vec::with_capacity(shard_rows.len());
        for (bucket, row_indices) in &shard_rows {
            let shard = shard_for_bucket(target_shards, *bucket, &target.table_name)
                .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?;
            let split = split_merge_by_rows(stmt, row_indices)
                .ok_or_else(|| enrich_generic_error(&"failed to split MERGE by shard", ctx))?;
            writes.push(self.plan_write(shard, &split, params, quorum_size, ctx)?);
        }
        Ok(writes)
    }

    /// Look up a table the MERGE names, reporting `42P01` when it does not exist.
    fn require_table(&self, name: &str, ctx: &ErrorContext) -> PgWireResult<TableMeta> {
        self.catalog
            .get_table(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .ok_or_else(|| {
                let err = CoordinatorError::TableNotFound(name.to_string());
                enrich_coordinator_error(&err, ctx, &self.catalog)
            })
    }

    /// The shards of `table`, refusing a table that has none rather than silently
    /// merging into nothing.
    fn shards_of(&self, table: &TableMeta, ctx: &ErrorContext) -> PgWireResult<Vec<ShardMeta>> {
        let shards = self
            .catalog
            .get_shards_for_table(&table.table_name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?;
        if shards.is_empty() {
            let err = CoordinatorError::ShardNotAssigned(format!(
                "no shards for table {}",
                table.table_name
            ));
            return Err(enrich_coordinator_error(&err, ctx, &self.catalog));
        }
        Ok(shards)
    }
}

/// Whichever name the source's columns are qualified by, for the error message
/// that explains what the ON clause has to look like.
fn source_qualifier(source: &MergeSource) -> &str {
    match source {
        MergeSource::Table { qualifier, .. } | MergeSource::Values { qualifier, .. } => qualifier,
    }
}

/// Refuse a table source the target cannot be merged from shard by shard.
///
/// Three things have to hold, and none of them is visible in the statement:
///
/// - **The same shard count.** The whole argument is that equal keys hash to the
///   same shard *number*; with different counts, `hash % 3` and `hash % 5` place
///   the same key differently and shard `i` of the target would be joined against
///   rows that have nothing to do with it.
/// - **The source is placed by the column the `ON` clause matches.** If the source
///   is sharded by something else, its rows are scattered by that other column and
///   the match for a target row can be on any shard.
/// - **Co-placement.** The statement is one piece of SQL executed by a node against
///   its local DuckDB, so every node holding a target shard must also hold the
///   matching source shard, or the statement names a table that is not there.
///   Placement is decided per table at `CREATE` time from the nodes alive then, so
///   two tables sharded identically are still not guaranteed to be stored together.
fn check_table_source(
    target: &TableMeta,
    source: &TableMeta,
    source_key: &str,
    target_shards: &[ShardMeta],
    source_shards: &[ShardMeta],
) -> std::result::Result<(), String> {
    if target.shard_count != source.shard_count {
        return Err(format!(
            "MERGE requires the source table \"{}\" to have the same number of shards as \"{}\" \
             ({} vs {}): rows are placed by hash(key) % shards, so with different shard counts the \
             same key lives on unrelated shards of the two tables and no shard could see both",
            source.table_name, target.table_name, source.shard_count, target.shard_count
        ));
    }

    if source_key != source.shard_key {
        return Err(format!(
            "MERGE requires the ON clause to match on the shard key of both tables: it matches \
             \"{}\".\"{}\" against \"{}\".\"{}\", but \"{}\" is sharded by \"{}\". Its rows are \
             spread by that column, so the row matching a given target row could be on any shard",
            target.table_name,
            target.shard_key,
            source.table_name,
            source_key,
            source.table_name,
            source.shard_key
        ));
    }

    // Matched by bucket rather than by position: the bucket is what names the
    // physical table (`inbox_shard3`) in the statement each node receives.
    let source_nodes: HashMap<u32, HashSet<&str>> = source_shards
        .iter()
        .map(|shard| (shard.hash_bucket, shard_nodes(shard)))
        .collect();

    for shard in target_shards {
        let holders = source_nodes.get(&shard.hash_bucket);
        let missing = match holders {
            None => true,
            Some(holders) => !shard_nodes(shard).is_subset(holders),
        };
        if missing {
            return Err(format!(
                "MERGE requires every node holding a shard of \"{}\" to hold the matching shard of \
                 \"{}\", and shard {} is not stored together: the MERGE runs on the node, which \
                 would have to read a source shard it does not have. Recreate \"{}\" with the same \
                 shards and replication_factor as \"{}\", or merge from a VALUES list",
                target.table_name,
                source.table_name,
                shard.hash_bucket,
                source.table_name,
                target.table_name
            ));
        }
    }

    Ok(())
}

/// Every node holding `shard`: its primary and its replicas.
fn shard_nodes(shard: &ShardMeta) -> HashSet<&str> {
    std::iter::once(shard.primary_node_id.as_str())
        .chain(shard.replica_node_ids.iter().map(String::as_str))
        .collect()
}

/// `0A000` for a MERGE shape VaireDB cannot place.
fn unsupported(message: String) -> pgwire::error::PgWireError {
    make_vdb_error(VdbErrorCode::FeatureNotSupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, ShardStrategy};
    use crate::pgwire_handler::parser::parse_sql;
    use crate::util::logical_shard_id;
    use pgwire::error::PgWireError;

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

    fn table(name: &str, shard_key: &str, shard_count: u32) -> TableMeta {
        TableMeta {
            table_name: name.to_string(),
            columns: ["id", "amount"]
                .into_iter()
                .map(|column| ColumnDef {
                    name: column.to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                })
                .collect(),
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: shard_key.to_string(),
            shard_count,
            replication_factor: 1,
            created_at: None,
            anonymized_columns: Default::default(),
            indexes: Vec::new(),
            constraints: Vec::new(),
        }
    }

    /// `shard_count` shards of `table`, shard `i` primary on `nodes[i % len]` with
    /// no replicas.
    fn shards(table: &str, shard_count: u32, nodes: &[&str]) -> Vec<ShardMeta> {
        (0..shard_count)
            .map(|bucket| ShardMeta {
                shard_id: logical_shard_id(bucket),
                table_name: table.to_string(),
                primary_node_id: nodes[bucket as usize % nodes.len()].to_string(),
                replica_node_ids: Vec::new(),
                hash_bucket: bucket,
                range_lower: String::new(),
                range_upper: String::new(),
            })
            .collect()
    }

    // The shape the whole design is built around: same shard count, matched on both
    // tables' shard keys, and every shard of the two stored on the same node.
    #[test]
    fn a_co_located_source_on_the_shard_key_is_accepted() {
        let target = table("orders", "id", 3);
        let source = table("inbox", "id", 3);
        let nodes = ["n1", "n2"];
        assert_eq!(
            check_table_source(
                &target,
                &source,
                "id",
                &shards("orders", 3, &nodes),
                &shards("inbox", 3, &nodes),
            ),
            Ok(())
        );
    }

    // `hash % 3` and `hash % 5` place the same key on unrelated shards, so no shard
    // could see both sides of a match.
    #[test]
    fn a_source_with_a_different_shard_count_is_refused() {
        let target = table("orders", "id", 3);
        let source = table("inbox", "id", 5);
        let message = check_table_source(
            &target,
            &source,
            "id",
            &shards("orders", 3, &["n1"]),
            &shards("inbox", 5, &["n1"]),
        )
        .expect_err("must be refused");
        assert!(message.contains("same number of shards"), "got: {message}");
    }

    // Matching the target's shard key against a non-key column of the source leaves
    // the source's matching row on any shard at all.
    #[test]
    fn a_source_not_sharded_by_the_matched_column_is_refused() {
        let target = table("orders", "id", 3);
        let source = table("inbox", "amount", 3);
        let message = check_table_source(
            &target,
            &source,
            "id",
            &shards("orders", 3, &["n1"]),
            &shards("inbox", 3, &["n1"]),
        )
        .expect_err("must be refused");
        assert!(message.contains("sharded by"), "got: {message}");
    }

    // The MERGE is one statement run by one node against its local DuckDB, so a
    // source shard stored elsewhere is a table that is simply not there.
    #[test]
    fn a_source_shard_stored_on_another_node_is_refused() {
        let target = table("orders", "id", 2);
        let source = table("inbox", "id", 2);
        let message = check_table_source(
            &target,
            &source,
            "id",
            &shards("orders", 2, &["n1", "n2"]),
            // Same buckets, opposite nodes.
            &shards("inbox", 2, &["n2", "n1"]),
        )
        .expect_err("must be refused");
        assert!(message.contains("stored together"), "got: {message}");
    }

    // A replicated target shard is only safe if *every* replica can read the source
    // shard: any of them may be the node that runs the statement.
    #[test]
    fn a_replica_that_cannot_read_the_source_is_refused() {
        let target = table("orders", "id", 1);
        let source = table("inbox", "id", 1);
        let mut target_shards = shards("orders", 1, &["n1"]);
        target_shards[0].replica_node_ids = vec!["n2".to_string()];
        let source_shards = shards("inbox", 1, &["n1"]);

        let message = check_table_source(&target, &source, "id", &target_shards, &source_shards)
            .expect_err("must be refused");
        assert!(message.contains("stored together"), "got: {message}");

        // With the source replicated the same way, it is accepted.
        let mut source_shards = source_shards;
        source_shards[0].replica_node_ids = vec!["n2".to_string()];
        assert_eq!(
            check_table_source(&target, &source, "id", &target_shards, &source_shards),
            Ok(())
        );
    }

    // A source held on *more* nodes than the target is fine: every node that could
    // run the statement can read it.
    #[test]
    fn a_source_replicated_more_widely_than_the_target_is_accepted() {
        let target = table("orders", "id", 1);
        let source = table("inbox", "id", 1);
        let target_shards = shards("orders", 1, &["n1"]);
        let mut source_shards = shards("inbox", 1, &["n1"]);
        source_shards[0].replica_node_ids = vec!["n2".to_string(), "n3".to_string()];
        assert_eq!(
            check_table_source(&target, &source, "id", &target_shards, &source_shards),
            Ok(())
        );
    }

    // --- the statement-level refusals, through the handler ---

    /// Run `sql` through the handler with no cluster behind it. Every rejection
    /// tested here is decided from the statement alone, before the catalog is read.
    async fn merge_error(sql: &str) -> (String, String) {
        let handler = VaireDbQueryHandler::for_tests(false);
        let err = handler
            .handle_merge(&parse_one(sql), &[])
            .await
            .expect_err("must be refused");
        user_error(err)
    }

    // A source whose rows the coordinator cannot place, refused by name rather
    // than broadcast: see `write_sql_cl::merge::merge_shape`.
    #[tokio::test]
    async fn a_subquery_source_is_refused_before_the_catalog_is_read() {
        let (code, message) =
            merge_error("MERGE INTO orders t USING (SELECT id FROM inbox) s ON t.id = s.id WHEN MATCHED THEN DELETE")
                .await;
        assert_eq!(code, "0A000");
        assert!(message.contains("Materialize the source"), "got: {message}");
    }

    // The qualifier on a written column can only be the target's; anything else
    // would be silently dropped and write a column the client did not name.
    #[tokio::test]
    async fn a_foreign_qualifier_on_a_written_column_is_refused() {
        let (code, message) = merge_error(
            "MERGE INTO orders t USING inbox s ON t.id = s.id WHEN MATCHED THEN UPDATE SET s.amount = 1",
        )
        .await;
        assert_eq!(code, "0A000");
        assert!(message.contains("merged into"), "got: {message}");
    }

    // Without a table in the catalog the target cannot be resolved, and the error
    // has to name the table rather than the shape.
    #[tokio::test]
    async fn an_unknown_target_is_reported_as_a_missing_table() {
        let (code, _) = merge_error(
            "MERGE INTO orders t USING inbox s ON t.id = s.id WHEN MATCHED THEN DELETE",
        )
        .await;
        assert_eq!(code, "42P01");
    }

    // --- splitting a VALUES source across shards ---

    /// Eleven shards: the smallest layout in which a shard's position in the
    /// catalog's list is not its hash bucket, because the records are keyed by the
    /// string `"{table}:shard{n}"` and `shard10` sorts before `shard2`.
    const WIDE_SHARDS: u32 = 11;

    /// A MERGE from a VALUES list holding one row `(id, id * 10)` per id.
    fn merge_over(ids: &[i64]) -> String {
        let values = ids
            .iter()
            .map(|id| format!("({id}, {})", id * 10))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "MERGE INTO orders t USING (VALUES {values}) AS s(id, amount) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount \
             WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)"
        )
    }

    /// `shard_count` shards of `table` in the lexicographic shard-id order a raw
    /// prefix scan of the catalog produces — `shard10` before `shard2`, so a shard's
    /// position in the list is not its bucket. Planning must place rows correctly
    /// whatever order the list arrives in.
    fn shards_in_scan_order(table: &str, shard_count: u32) -> Vec<ShardMeta> {
        let mut shards = shards(table, shard_count, &["n1"]);
        shards.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
        shards
    }

    /// Parse `sql` and pair each source row with the shard-key value the coordinator
    /// hashes, applying the same normalization `handle_merge` does before splitting.
    fn merge_row_keys(sql: &str) -> (Statement, Vec<(usize, String)>) {
        let mut stmt = parse_one(sql);
        ensure_merge_relation_aliases(&mut stmt);
        normalize_merge_column_qualifiers(&mut stmt).expect("the qualifiers are the target's");
        // `id` is the first of the source's named columns.
        let row_keys =
            merge_row_shard_keys(&stmt, 0, &[]).expect("every source row supplies a literal key");
        (stmt, row_keys)
    }

    // A merged row must go to the shard its key hashes to — the same shard the
    // INSERT that stored it chose. Resolving the shard by its position in the
    // catalog's list instead of by its bucket sends rows to the wrong shard from
    // eleven shards on, where they match nothing and are then inserted a second
    // time.
    #[tokio::test]
    async fn a_split_merge_sends_each_row_to_the_shard_that_owns_its_key() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let target = table("orders", "id", WIDE_SHARDS);
        let target_shards = shards_in_scan_order("orders", WIDE_SHARDS);

        // Enough ids to reach every bucket, including those whose bucket and
        // position disagree.
        let ids: Vec<i64> = (1..=40).collect();
        let (stmt, row_keys) = merge_row_keys(&merge_over(&ids));

        let writes = handler
            .plan_merge_by_rows(
                &stmt,
                &row_keys,
                &target,
                &target_shards,
                &[],
                &ErrorContext::for_table("orders"),
            )
            .unwrap_or_else(|_| panic!("the MERGE must be planned"));

        let mut planned: Vec<i64> = Vec::new();
        for write in &writes {
            let bucket = write.shard.hash_bucket;
            assert_eq!(
                write.statement.shard_id,
                format!("orders_shard{bucket}"),
                "the write must name the shard it is planned for"
            );
            for id in &ids {
                let owned =
                    compute_shard_index(&id.to_string(), WIDE_SHARDS as usize) == bucket as usize;
                let present = write
                    .statement
                    .sql
                    .contains(&format!("({id}, {})", id * 10));
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
        assert_eq!(planned, ids, "every row must be merged exactly once");
    }

    // A layout missing the bucket a row hashes to is reported, not routed around:
    // merging the row on any other shard would look for it where it cannot be.
    #[tokio::test]
    async fn a_split_merge_reports_a_bucket_with_no_shard() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let target = table("orders", "id", WIDE_SHARDS);

        let orphan = 3u32;
        let target_shards: Vec<ShardMeta> = shards_in_scan_order("orders", WIDE_SHARDS)
            .into_iter()
            .filter(|shard| shard.hash_bucket != orphan)
            .collect();

        // `compute_shard_index` divides by the number of shards it is given, so the
        // id is chosen against the layout as it now stands.
        let shard_count = target_shards.len();
        let orphaned_id = (1i64..)
            .find(|id| compute_shard_index(&id.to_string(), shard_count) == orphan as usize)
            .unwrap();
        let (stmt, row_keys) = merge_row_keys(&merge_over(&[orphaned_id]));

        let (code, message) = user_error(
            handler
                .plan_merge_by_rows(
                    &stmt,
                    &row_keys,
                    &target,
                    &target_shards,
                    &[],
                    &ErrorContext::for_table("orders"),
                )
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
