//! The bulk lane of the INSERT path: a materialized batch of rows — a `COPY ... FROM`'s
//! — written to the shards as data rather than as literal `INSERT ... VALUES` text.
//!
//! The literal lane spells every value into SQL, clones the statement once per shard it
//! splits into, renders each part back to text, and the shard parses all of it again.
//! For a bulk load that work is paid per value, and on a wide table it is what bounds
//! the load — the shards' storage engine is idle most of the time. This lane routes and
//! splits the batch as columns and ships each shard its rows as one Arrow payload (see
//! `BulkRows` in `write_service.proto`).
//!
//! It is the literal lane made cheaper, never a different statement: it takes a batch
//! only where it stores exactly what the literal lane would ([`crate::write_sql_cl`]'s
//! `bulk_rows` states that contract), and hands every other batch back:
//!
//! * a table with pseudonymized columns — the digest rewrite works on literals, and
//!   reproducing it on columns would be a second implementation of the one rule that
//!   must never let plaintext reach a shard;
//! * a batch with a row whose shard key the literal lane refuses to route — it refuses
//!   the batch in its own words.
//!
//! Past planning the two lanes are one: the same [`DmlPlan`]s, shipped, buffered inside
//! a transaction block and reported as partially applied by
//! [`VaireDbQueryHandler::ship_row_plans`].

use std::collections::BTreeMap;

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::take_record_batch;
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::{BulkRows, VdbErrorCode};

use crate::pgwire_handler::dml::{DmlPlan, PlannedWrite};
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error, require_shards, require_table,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::session::SessionState;
use crate::replication::BatchStatement;
use crate::sqlparser::ast::Statement;
use crate::write_router::{compute_quorum_size, compute_shard_index, shard_for_bucket};
use crate::write_sql_cl;

/// The most bytes one bulk payload may hold.
///
/// A core node's write service takes requests of up to gRPC's default 4 MiB. A request
/// is one payload plus a little framing; half the limit leaves that framing, and the
/// estimate a batch is first cut by, room to be wrong.
const MAX_BULK_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

/// Where a materialized batch of rows is written: the table, the columns the batch's
/// fields land on in order, and the `INSERT` the literal lane spells its rows into.
pub(super) struct RowTarget {
    table: String,
    columns: Vec<String>,
    template: Statement,
}

impl RowTarget {
    /// The target for `columns` of `table`, both catalog-canonical and already checked
    /// against the table by the caller.
    pub(super) fn new(table: &str, columns: Vec<String>) -> PgWireResult<Self> {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let template = write_sql_cl::insert_template(table, &refs)
            .map_err(|msg| make_vdb_error(VdbErrorCode::SqlSyntaxError, msg))?;
        Ok(Self {
            table: table.to_string(),
            columns,
            template,
        })
    }

    /// `batch` as the literal lane's `INSERT ... VALUES` statements, chunked the way a
    /// client's own multi-row `INSERT` is.
    pub(super) fn literal_statements(&self, batch: &RecordBatch) -> PgWireResult<Vec<Statement>> {
        write_sql_cl::insert_statements_from_batches(
            &self.template,
            std::slice::from_ref(batch),
            write_sql_cl::ROWS_PER_STATEMENT,
        )
        .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))
    }
}

impl VaireDbQueryHandler {
    /// Write one materialized batch of rows into `target`, returning the rows written:
    /// in bulk where that stores exactly what the literal lane would, through the
    /// literal lane otherwise.
    ///
    /// `command` names the client's statement in a partial-failure message, as for
    /// [`write_row_statements`](Self::write_row_statements).
    pub(super) async fn write_row_batch(
        &self,
        target: &RowTarget,
        batch: &RecordBatch,
        session: &SessionState,
        command: &str,
    ) -> PgWireResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        match self.plan_bulk_insert(target, batch)? {
            Some(plans) => {
                self.ship_row_plans(plans, session, &target.table, command)
                    .await
            }
            None => {
                let statements = target.literal_statements(batch)?;
                self.write_row_statements(&statements, &[], session, &target.table, command)
                    .await
            }
        }
    }

    /// Plan `batch` as bulk writes, one per payload in shard order — or `None` where
    /// the literal lane has to take it.
    ///
    /// Everything the literal lane checks before it routes is checked first and in the
    /// same order, so which lane a batch took never shows in which error it gets.
    fn plan_bulk_insert(
        &self,
        target: &RowTarget,
        batch: &RecordBatch,
    ) -> PgWireResult<Option<Vec<DmlPlan>>> {
        let rows = write_sql_cl::bulk_wire_rows(batch)
            .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?;

        let ctx = ErrorContext::for_table(&target.table);
        let table_meta = require_table(&self.catalog, &target.table, &ctx)?;
        if !table_meta.anonymized_columns.is_empty() {
            return Ok(None);
        }
        let Some(key_column) = target
            .columns
            .iter()
            .position(|c| *c == table_meta.shard_key)
        else {
            return Ok(None);
        };
        let Some(keys) = write_sql_cl::bulk_routing_values(batch, key_column)
            .map_err(|msg| make_vdb_error(VdbErrorCode::FeatureNotSupported, msg))?
        else {
            return Ok(None);
        };

        let ctx = ctx.with_replication(table_meta.replication_factor);
        let quorum_size = compute_quorum_size(table_meta.replication_factor);
        let all_shards = require_shards(&self.catalog, &table_meta, &ctx)?;

        // Bucketed as the literal lane buckets a split INSERT — keyed by bucket, rows
        // in batch order within one — so the same rows reach the same shard in the
        // same sequence whichever lane wrote them.
        let mut shard_rows: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
        for (row, key) in keys.iter().enumerate() {
            let bucket = compute_shard_index(key, all_shards.len());
            shard_rows.entry(bucket).or_default().push(row as u32);
        }

        let mut plans = Vec::new();
        for (bucket, row_indices) in shard_rows {
            let shard = shard_for_bucket(&all_shards, bucket, &table_meta.table_name)
                .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
            let shard_table = crate::util::shard_table_name(&shard.table_name, shard.hash_bucket);
            let owned = take_record_batch(&rows, &UInt32Array::from(row_indices))
                .map_err(|e| internal(format!("COPY could not split rows by shard: {e}")))?;

            for payload in write_sql_cl::bulk_payloads(&owned, MAX_BULK_PAYLOAD_BYTES)
                .map_err(|msg| internal(format!("COPY could not ship rows in bulk: {msg}")))?
            {
                plans.push(DmlPlan {
                    writes: vec![PlannedWrite {
                        statement: BatchStatement {
                            sql: String::new(),
                            params: Vec::new(),
                            shard_id: shard_table.clone(),
                            bulk: Some(BulkRows {
                                table: shard_table.clone(),
                                columns: target.columns.clone(),
                                arrow_ipc: payload.arrow_ipc,
                            }),
                        },
                        shard: shard.clone(),
                        quorum_size,
                    }],
                    table_name: target.table.clone(),
                    // Exact: a bulk insert stores every row it carries or fails, which
                    // is what lets a transaction block buffer it.
                    static_row_count: Some(payload.rows as u64),
                    ctx: ctx.clone(),
                });
            }
        }
        Ok(Some(plans))
    }
}

fn internal(message: String) -> PgWireError {
    make_vdb_error(VdbErrorCode::InternalError, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::ipc::reader::StreamReader;

    use super::super::write_path_test_helper::{parse_one, user_error};
    use super::*;
    use crate::catalog::TableMeta;
    use crate::catalog::catalog_test_helper::{shard_meta, table_meta};
    use crate::pgwire_handler::query_router::QueryType;

    /// `orders (id, v)` sharded by `id` over `shard_count` buckets, on a node the test
    /// never reaches: planning is all these tests do.
    fn sharded(handler: &VaireDbQueryHandler, shard_count: u32) -> TableMeta {
        let mut meta = table_meta("orders", &["id", "v"], "id");
        meta.shard_count = shard_count;
        handler.catalog.put_table(&meta).unwrap();
        for bucket in 0..shard_count {
            handler
                .catalog
                .put_shard(&shard_meta("orders", bucket, "node-0", &[]))
                .unwrap();
        }
        meta
    }

    fn target() -> RowTarget {
        RowTarget::new("orders", vec!["id".to_string(), "v".to_string()]).unwrap()
    }

    fn rows(ids: &[Option<i64>]) -> RecordBatch {
        let v: Vec<String> = ids.iter().map(|id| format!("v{id:?}")).collect();
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(ids.to_vec())) as Arc<dyn Array>,
            ),
            ("v", Arc::new(StringArray::from(v)) as Arc<dyn Array>),
        ])
        .unwrap()
    }

    /// The ids a planned bulk write carries.
    fn carried_ids(statement: &BatchStatement) -> Vec<i64> {
        let bulk = statement.bulk.as_ref().expect("a bulk write");
        StreamReader::try_new(std::io::Cursor::new(bulk.arrow_ipc.to_vec()), None)
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // The routing half of the contract, end to end: each row's bulk write goes to the
    // shard a single-row INSERT of the same row is planned onto. Eleven shards, so a
    // bucket read as a position in the shard list would show.
    #[tokio::test]
    async fn every_row_lands_on_the_shard_its_insert_would() {
        let handler = VaireDbQueryHandler::for_tests(false);
        sharded(&handler, 11);
        let ids: Vec<Option<i64>> = (0..300).map(Some).collect();

        let plans = handler
            .plan_bulk_insert(&target(), &rows(&ids))
            .unwrap()
            .expect("a plain table takes the bulk lane");

        let mut bulk_shard: HashMap<i64, String> = HashMap::new();
        for plan in &plans {
            assert_eq!(plan.writes.len(), 1);
            let statement = &plan.writes[0].statement;
            let bulk = statement.bulk.as_ref().unwrap();
            assert_eq!(bulk.table, statement.shard_id);
            assert_eq!(bulk.columns, ["id", "v"]);
            assert!(statement.sql.is_empty());
            let carried = carried_ids(statement);
            assert_eq!(plan.static_row_count, Some(carried.len() as u64));
            for id in carried {
                assert!(
                    bulk_shard.insert(id, statement.shard_id.clone()).is_none(),
                    "row {id} was planned twice"
                );
            }
        }
        assert_eq!(
            bulk_shard.len(),
            300,
            "every row must be planned exactly once"
        );

        for id in 0..300 {
            let literal = handler
                .plan_dml(
                    &parse_one(&format!(
                        "INSERT INTO \"orders\" (\"id\", \"v\") VALUES ({id}, 'x')"
                    )),
                    &QueryType::Insert,
                    &[],
                )
                .unwrap();
            assert_eq!(
                bulk_shard[&id], literal.writes[0].statement.shard_id,
                "row {id} must land where its INSERT would"
            );
        }
    }

    // The digest rewrite lives in the literal lane, so a pseudonymized table never
    // takes this one.
    #[tokio::test]
    async fn a_pseudonymized_table_is_left_to_the_literal_lane() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let mut meta = sharded(&handler, 3);
        meta.anonymized_columns
            .insert("v".to_string(), "secret".to_string());
        handler.catalog.put_table(&meta).unwrap();

        assert!(
            handler
                .plan_bulk_insert(&target(), &rows(&[Some(1)]))
                .unwrap()
                .is_none()
        );
    }

    // A key the literal lane refuses is refused by it, in its words: the batch never
    // gets a bulk-lane error of its own.
    #[tokio::test]
    async fn a_null_shard_key_is_refused_as_the_literal_lane_refuses_it() {
        let handler = VaireDbQueryHandler::for_tests(false);
        sharded(&handler, 3);
        let session = SessionState::default();

        let err = handler
            .write_row_batch(&target(), &rows(&[Some(1), None]), &session, "COPY")
            .await
            .expect_err("a NULL shard key cannot be routed");
        let (_code, message) = user_error(err);
        assert!(message.contains("cannot be NULL"), "got: {message}");
    }

    // Inside a transaction block the payloads are buffered for COMMIT like any other
    // INSERT, and the count reported is exact.
    #[tokio::test]
    async fn inside_a_transaction_block_bulk_writes_are_buffered() {
        let handler = VaireDbQueryHandler::for_tests(false);
        sharded(&handler, 3);
        let session = SessionState::default();
        session.transaction().await.begin(false);

        let written = handler
            .write_row_batch(
                &target(),
                &rows(&[Some(1), Some(2), Some(3), Some(4)]),
                &session,
                "COPY",
            )
            .await
            .unwrap();
        assert_eq!(written, 4);

        let txn = session.transaction().await;
        assert!(!txn.writes().is_empty());
        let buffered: usize = txn
            .writes()
            .iter()
            .map(|w| carried_ids(&w.statement).len())
            .sum();
        assert_eq!(buffered, 4);
    }

    // With nowhere to put the rows, the refusal is the literal lane's: the table has no
    // shards.
    #[tokio::test]
    async fn a_table_without_shards_is_refused_before_anything_is_planned() {
        let handler = VaireDbQueryHandler::for_tests(false);
        handler
            .catalog
            .put_table(&table_meta("orders", &["id", "v"], "id"))
            .unwrap();

        let session = SessionState::default();
        let bulk = handler
            .write_row_batch(&target(), &rows(&[Some(1)]), &session, "COPY")
            .await
            .expect_err("no shards");
        let literal = handler
            .write_row_statements(
                &target().literal_statements(&rows(&[Some(1)])).unwrap(),
                &[],
                &session,
                "orders",
                "COPY",
            )
            .await
            .expect_err("no shards");
        assert_eq!(user_error(bulk), user_error(literal));
    }

    #[tokio::test]
    async fn an_empty_batch_writes_nothing() {
        let handler = VaireDbQueryHandler::for_tests(false);
        sharded(&handler, 3);
        let session = SessionState::default();
        assert_eq!(
            handler
                .write_row_batch(&target(), &rows(&[]), &session, "COPY")
                .await
                .unwrap(),
            0
        );
    }
}
