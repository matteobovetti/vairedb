//! Transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`) and
//! the rules governing what a client may do inside a transaction block.
//!
//! VaireDB has no cross-shard commit protocol, so a transaction block is not held
//! open on the core nodes. Instead the coordinator **buffers** the block's writes
//! in the connection's [`SessionState`] and ships them at `COMMIT`:
//!
//! - `ROLLBACK` is a real rollback — nothing was ever sent.
//! - A block whose writes all reach the same node set commits as one genuine
//!   DuckDB transaction, which is the common ORM case.
//! - A block spanning node sets cannot be made atomic, so it is refused at
//!   `COMMIT` unless the operator opts into a non-atomic commit.
//!
//! Buffering is also what constrains the block: a statement is only allowed
//! inside one if the coordinator can answer it truthfully without running it.
//! That is why [`VaireDbQueryHandler::check_transaction_allows`] refuses an
//! UPDATE/DELETE (their row count is unknowable until the shards run them), a
//! read of a table the block has already written, and DDL (which is not buffered
//! at all). Each rejection names what to do instead; none of them silently
//! invents an answer.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::copy;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::introspection;
use crate::pgwire_handler::query_router::{self, QueryType, canonicalize_ident};
use crate::pgwire_handler::session::{BufferedWrite, SessionState, Transaction, TransactionStatus};
use crate::sqlparser::ast::{
    Ident, Statement, TransactionAccessMode, TransactionMode, TransactionModifier,
};
use crate::write_sql_cl;

impl VaireDbQueryHandler {
    /// Dispatch a transaction-control statement. Never reaches a shard: it only
    /// moves the connection's session state, plus the `COMMIT` flush.
    pub(super) async fn handle_transaction_control(
        &self,
        stmt: &Statement,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        match stmt {
            Statement::StartTransaction {
                modes,
                modifier,
                statements,
                exception,
                has_end_keyword,
                ..
            } => {
                self.begin_transaction(
                    modes,
                    *modifier,
                    !statements.is_empty() || exception.is_some() || *has_end_keyword,
                    session,
                )
                .await
            }
            Statement::Commit {
                chain, modifier, ..
            } => {
                reject_chain(*chain, "COMMIT")?;
                reject_modifier(*modifier, "COMMIT")?;
                self.commit_transaction(session).await
            }
            Statement::Rollback { chain, savepoint } => {
                reject_chain(*chain, "ROLLBACK")?;
                self.rollback_transaction(savepoint.as_ref(), session).await
            }
            Statement::Savepoint { name } => self.declare_savepoint(name, session).await,
            Statement::ReleaseSavepoint { name } => self.release_savepoint(name, session).await,
            // Unreachable: the classifier routes exactly the five statements above
            // here. A new transaction statement must be handled, not defaulted.
            _ => Err(make_vdb_error(
                VdbErrorCode::InternalError,
                "unhandled transaction control statement",
            )),
        }
    }

    /// `BEGIN` / `START TRANSACTION`: open a block and start buffering writes.
    ///
    /// Isolation levels are accepted and ignored — a buffered block applies its
    /// writes in one node-local transaction, so the level DuckDB uses is the one
    /// that governs. `READ ONLY` is honored by refusing writes inside the block.
    async fn begin_transaction(
        &self,
        modes: &[TransactionMode],
        modifier: Option<TransactionModifier>,
        has_inline_block: bool,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        if has_inline_block {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "BEGIN ... END blocks are not supported; send BEGIN, the statements, and COMMIT as separate statements",
            ));
        }
        reject_modifier(modifier, "BEGIN")?;

        let read_only = modes.iter().any(|mode| {
            matches!(
                mode,
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            )
        });

        let mut txn = session.transaction().await;
        // PostgreSQL warns and keeps the existing block. A warning is impossible
        // on the extended protocol (its handler has no message sink), so the
        // no-op is silent — but it must stay a no-op: resetting here would
        // discard writes the client believes are still pending.
        if !txn.is_open() {
            txn.begin(read_only);
        }
        Ok(Response::TransactionStart(Tag::new("BEGIN")))
    }

    /// `COMMIT`: ship the buffered writes and close the block.
    ///
    /// The block ends whatever happens, as in PostgreSQL — a failed `COMMIT`
    /// leaves the session idle, not still in a transaction.
    async fn commit_transaction(&self, session: &SessionState) -> PgWireResult<Response> {
        let mut txn = session.transaction().await;
        match txn.status() {
            // No block open: PostgreSQL warns and reports COMMIT anyway.
            TransactionStatus::Idle => Ok(Response::TransactionEnd(Tag::new("COMMIT"))),
            // A failed block can only roll back, and PostgreSQL says so in the
            // tag: the client is told ROLLBACK even though it asked to commit.
            TransactionStatus::Failed => {
                txn.end();
                Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
            }
            TransactionStatus::Active => {
                let writes = txn.take_writes();
                txn.end();
                drop(txn);

                self.flush_transaction(writes).await?;
                Ok(Response::TransactionEnd(Tag::new("COMMIT")))
            }
        }
    }

    /// Ship a committed block's buffered writes.
    ///
    /// Writes reaching the same node set go out as one atomic batch, so a
    /// single-node-set transaction is genuinely all-or-nothing. Several node sets
    /// cannot be, so the commit is refused before anything is sent unless
    /// `allow_cross_shard_transactions` is on — and if a later group then fails,
    /// the error says which groups already applied rather than implying the whole
    /// commit was rolled back.
    async fn flush_transaction(&self, writes: Vec<BufferedWrite>) -> PgWireResult<()> {
        let groups = Transaction::group_by_node_set(writes);
        if groups.len() > 1 && !self.allow_cross_shard_transactions {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "this transaction wrote to {} independent shard groups and VaireDB has no cross-shard commit protocol, so it cannot be committed atomically; nothing was written. Keep a transaction's writes on one shard group, or set allow_cross_shard_transactions: true in the coordinator config to commit such a transaction non-atomically",
                    groups.len()
                ),
            ));
        }

        let write_id = uuid::Uuid::new_v4().to_string();
        for (idx, group) in groups.iter().enumerate() {
            let ctx = ErrorContext::for_table(&group.shard.table_name);
            let result = self
                .replication_manager
                .execute_transaction_with_quorum(
                    &group.shard,
                    group.statements.clone(),
                    &format!("{}-{}", write_id, idx),
                    group.quorum_size,
                )
                .await;

            if let Err(e) = result {
                let enriched = enrich_coordinator_error(&e, &ctx, &self.catalog);
                if idx == 0 {
                    // Nothing was applied: the transaction rolled back cleanly.
                    return Err(enriched);
                }
                return Err(make_vdb_error(
                    VdbErrorCode::PartialCommit,
                    format!(
                        "COMMIT partially applied: {} of {} shard groups were written and cannot be undone, then the commit failed. Inspect the affected tables before retrying. Cause: {}",
                        idx,
                        groups.len(),
                        enriched
                    ),
                ));
            }
        }

        Ok(())
    }

    /// `ROLLBACK`, with or without `TO SAVEPOINT`.
    async fn rollback_transaction(
        &self,
        savepoint: Option<&Ident>,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let mut txn = session.transaction().await;

        let Some(name) = savepoint else {
            // Discarding the buffer *is* the rollback: nothing was ever shipped.
            // Outside a block PostgreSQL warns and reports ROLLBACK anyway.
            txn.end();
            return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
        };

        if !txn.is_open() {
            return Err(no_active_transaction("ROLLBACK TO SAVEPOINT"));
        }
        let name = canonicalize_ident(name);
        if !txn.rollback_to_savepoint(&name) {
            return Err(unknown_savepoint(&name));
        }
        // `Execution`, not `TransactionEnd`: the block is still open, and reporting
        // its end would flip the connection's transaction status to idle.
        Ok(Response::Execution(Tag::new("ROLLBACK")))
    }

    /// `SAVEPOINT name`: mark the current buffer position.
    async fn declare_savepoint(
        &self,
        name: &Ident,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let mut txn = session.transaction().await;
        if !txn.is_open() {
            return Err(no_active_transaction("SAVEPOINT"));
        }
        txn.savepoint(&canonicalize_ident(name));
        Ok(Response::Execution(Tag::new("SAVEPOINT")))
    }

    /// `RELEASE SAVEPOINT name`: forget the mark, keeping its writes.
    async fn release_savepoint(
        &self,
        name: &Ident,
        session: &SessionState,
    ) -> PgWireResult<Response> {
        let mut txn = session.transaction().await;
        if !txn.is_open() {
            return Err(no_active_transaction("RELEASE SAVEPOINT"));
        }
        let name = canonicalize_ident(name);
        if !txn.release_savepoint(&name) {
            return Err(unknown_savepoint(&name));
        }
        Ok(Response::Execution(Tag::new("RELEASE")))
    }

    /// Refuse a statement the client's current transaction state cannot honor.
    /// Called for every statement except transaction control itself, on both
    /// protocols, and a no-op outside a transaction block.
    ///
    /// Each rejection exists because the coordinator would otherwise have to
    /// answer with something it cannot know: see the module documentation.
    pub(super) async fn check_transaction_allows(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
        session: &SessionState,
    ) -> PgWireResult<()> {
        let txn = session.transaction().await;
        if !txn.is_open() {
            return Ok(());
        }
        if txn.status() == TransactionStatus::Failed {
            return Err(in_failed_transaction());
        }

        match query_type {
            QueryType::Select => {
                let written = query_router::extract_select_table_name(stmt)
                    .is_some_and(|table| txn.has_buffered_writes_for(&table));
                if written {
                    return Err(reads_a_buffered_table());
                }
                Ok(())
            }
            QueryType::Insert if txn.is_read_only() => Err(read_only_transaction("INSERT")),
            QueryType::Insert => {
                // An `INSERT ... SELECT` is a read as much as a write: its source
                // query runs now, against a database that does not yet hold the
                // block's buffered writes. Refused for the same reason a plain
                // SELECT of a written table is.
                if write_sql_cl::insert_source_tables(stmt)
                    .iter()
                    .any(|table| txn.has_buffered_writes_for(table))
                {
                    return Err(reads_a_buffered_table());
                }
                Ok(())
            }
            QueryType::Update | QueryType::Delete => {
                let command = if *query_type == QueryType::Update {
                    "UPDATE"
                } else {
                    "DELETE"
                };
                if txn.is_read_only() {
                    return Err(read_only_transaction(command));
                }
                Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    format!(
                        "{command} is not supported inside a transaction block: the statement is buffered until COMMIT, and how many rows it affects is only known once the shards run it — so the row count reported now would be a guess, which a client that branches on it can act on destructively. Run the {command} outside a transaction block"
                    ),
                ))
            }
            // A MERGE is refused for the same reason an UPDATE is, and one more: it
            // reads the target and the source to decide what to do to each row, and
            // inside a block neither yet holds the writes the block has buffered —
            // so the branch each row took would be decided against a state the
            // client cannot see.
            QueryType::Merge => {
                if txn.is_read_only() {
                    return Err(read_only_transaction("MERGE"));
                }
                Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    "MERGE is not supported inside a transaction block: it decides row by row whether to update, insert or delete, so both the rows it affects and how many there are are only known once the shards run it. Run the MERGE outside a transaction block",
                ))
            }
            // A COPY splits by direction, for the same reasons an INSERT and a
            // SELECT do: the import is a write whose rows are literals by the time
            // they are buffered, so its count is exact; the export is a read, and a
            // read of a table the block has written would silently miss those rows.
            QueryType::Copy if copy::copy_writes_rows(stmt) => {
                if txn.is_read_only() {
                    return Err(read_only_transaction("COPY ... FROM"));
                }
                Ok(())
            }
            QueryType::Copy => {
                if copy::copy_source_tables(stmt)
                    .iter()
                    .any(|table| txn.has_buffered_writes_for(table))
                {
                    return Err(reads_a_buffered_table());
                }
                Ok(())
            }
            QueryType::CreateTable
            | QueryType::AlterTable
            | QueryType::DropTable
            | QueryType::TruncateTable
            | QueryType::CreateIndex
            | QueryType::DropIndex => Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "DDL is not supported inside a transaction block: schema changes update the coordinator catalog and every shard immediately, so COMMIT could not roll them back. Run the statement outside a transaction block",
            )),
            // Refused for the same reason, with the reason it actually has: view
            // DDL reaches no shard, but it does update the catalog at once, and a
            // ROLLBACK would leave the change in place.
            QueryType::CreateView | QueryType::AlterView | QueryType::DropView => {
                Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    "view DDL is not supported inside a transaction block: the definition is written to the coordinator catalog immediately, so ROLLBACK could not undo it. Run the statement outside a transaction block",
                ))
            }
            // And again for a schema, which is a catalog record like a view's
            // definition — nothing to broadcast, and nothing a ROLLBACK could take
            // back.
            QueryType::CreateSchema | QueryType::DropSchema => Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "schema DDL is not supported inside a transaction block: the namespace is written to the coordinator catalog immediately, so ROLLBACK could not undo it. Run the statement outside a transaction block",
            )),
            // A runtime parameter is allowed inside a block, as in PostgreSQL: it
            // touches no relation, so there is nothing for the block's buffered
            // writes to be inconsistent with. The one form whose scope *is* the
            // block, `SET LOCAL`, is refused in
            // [`crate::pgwire_handler::session_params`] rather than here, because it
            // has to be refused outside a block too.
            QueryType::SessionParam => Ok(()),
            // An `EXPLAIN` inherits its inner query's rule. The non-`ANALYZE` form
            // runs nothing, but it is still *planned* against a database that does
            // not hold the block's buffered writes, so the plan it prints — the
            // scans chosen, the shards involved — is a plan for the wrong state; the
            // `ANALYZE` form additionally runs the query and would return the rows a
            // refused SELECT would have. A `DESCRIBE <relation>` reports no inner
            // query and is allowed: only DDL could change a relation's shape, and DDL
            // cannot have run inside the block.
            QueryType::Explain => {
                let written = introspection::explained_query(stmt)
                    .and_then(query_router::extract_select_table_name)
                    .is_some_and(|table| txn.has_buffered_writes_for(&table));
                if written {
                    return Err(reads_a_buffered_table());
                }
                Ok(())
            }
            // Transaction control never reaches here, and an unsupported
            // statement is rejected by name a moment later either way.
            QueryType::TransactionControl | QueryType::Other => Ok(()),
        }
    }
}

/// `25P02`: a statement was issued after an earlier one failed inside the block.
pub(super) fn in_failed_transaction() -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InFailedTransaction,
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

/// `0A000`: a statement inside the block reads a table the block has written.
/// The write is buffered in the coordinator until `COMMIT`, so the read would run
/// against a database that does not hold it — and quietly return the wrong rows,
/// which is worse than refusing.
fn reads_a_buffered_table() -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        "cannot read a table this transaction block has written to: VaireDB buffers a block's writes in the coordinator until COMMIT, so the read would silently miss them. Read the table before writing it, or after COMMIT",
    )
}

/// `25006`: a write was issued inside a `READ ONLY` block.
fn read_only_transaction(command: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::ReadOnlyTransaction,
        format!("cannot execute {command} in a read-only transaction"),
    )
}

/// `25P01`: a savepoint statement was issued outside a transaction block.
fn no_active_transaction(command: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::NoActiveTransaction,
        format!("{command} can only be used in a transaction block"),
    )
}

/// `3B001`: the named savepoint was never established, or has been released.
fn unknown_savepoint(name: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InvalidSavepoint,
        format!("savepoint \"{name}\" does not exist"),
    )
}

/// Refuse `AND CHAIN`: it would commit and immediately reopen a block, and
/// silently dropping the chain would leave the client's next statement running
/// outside a transaction it believes is open.
fn reject_chain(chain: bool, command: &str) -> PgWireResult<()> {
    if chain {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("{command} AND CHAIN is not supported; issue {command} followed by BEGIN"),
        ));
    }
    Ok(())
}

/// Refuse a transaction modifier (`DEFERRED`, `IMMEDIATE`, `EXCLUSIVE`, `TRY`,
/// `CATCH`): none of them describe how a buffered block behaves, so accepting one
/// would promise semantics VaireDB does not implement.
fn reject_modifier(modifier: Option<TransactionModifier>, command: &str) -> PgWireResult<()> {
    match modifier {
        Some(modifier) => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("{command} {modifier} is not supported"),
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pgwire::messages::response::CommandComplete;

    use crate::catalog::ShardMeta;
    use crate::replication::BatchStatement;

    /// The (SQLSTATE, message) a client would receive.
    fn reported(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code, info.message),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// Which `Response` variant was returned, and the completion tag it carries.
    /// The variant is as load-bearing as the tag: pgwire derives the connection's
    /// reported transaction status from it, so an `Execution` sent where the
    /// client expects a `TransactionEnd` leaves the two sides disagreeing about
    /// whether a block is open.
    fn reported_response(response: Response) -> (&'static str, String) {
        match response {
            Response::TransactionStart(tag) => ("start", CommandComplete::from(tag).tag),
            Response::TransactionEnd(tag) => ("end", CommandComplete::from(tag).tag),
            Response::Execution(tag) => ("execution", CommandComplete::from(tag).tag),
            _ => panic!("expected a tag-carrying response"),
        }
    }

    fn parse_one(sql: &str) -> Statement {
        crate::pgwire_handler::parser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    /// Run a statement through transaction control, as the handler does.
    async fn control(
        handler: &VaireDbQueryHandler,
        session: &SessionState,
        sql: &str,
    ) -> PgWireResult<(&'static str, String)> {
        let stmt = parse_one(sql);
        handler
            .handle_transaction_control(&stmt, session)
            .await
            .map(reported_response)
    }

    /// Ask the transaction guard whether `sql` may run in the session's current
    /// state, classifying the statement exactly as the handler does.
    async fn allows(
        handler: &VaireDbQueryHandler,
        session: &SessionState,
        sql: &str,
    ) -> PgWireResult<()> {
        let stmt = parse_one(sql);
        let query_type = query_router::classify_statement(&stmt);
        handler
            .check_transaction_allows(&stmt, &query_type, session)
            .await
    }

    fn buffered_write(table: &str, primary: &str, replicas: &[&str]) -> BufferedWrite {
        BufferedWrite {
            shard: ShardMeta {
                shard_id: format!("{table}-0"),
                table_name: table.to_string(),
                primary_node_id: primary.to_string(),
                replica_node_ids: replicas.iter().map(|r| r.to_string()).collect(),
                hash_bucket: 0,
                range_lower: String::new(),
                range_upper: String::new(),
            },
            statement: BatchStatement {
                sql: format!("INSERT INTO {table}_shard0 VALUES (1)"),
                params: Vec::new(),
                shard_id: crate::util::shard_table_name(table, 0),
            },
            quorum_size: 1,
            table_name: table.to_string(),
        }
    }

    // Savepoint statements happen *inside* a block, so all three must report as
    // plain executions. Reporting `ROLLBACK TO SAVEPOINT` as a transaction end
    // would flip the connection to idle and silently drop the rest of the block.
    #[tokio::test]
    async fn savepoint_statements_leave_the_block_open() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        assert_eq!(
            control(&handler, &session, "BEGIN").await.unwrap(),
            ("start", "BEGIN".to_string())
        );
        for (sql, tag) in [
            ("SAVEPOINT sp", "SAVEPOINT"),
            ("ROLLBACK TO SAVEPOINT sp", "ROLLBACK"),
            ("RELEASE SAVEPOINT sp", "RELEASE"),
        ] {
            assert_eq!(
                control(&handler, &session, sql).await.unwrap(),
                ("execution", tag.to_string()),
                "`{sql}` must not end the block"
            );
        }
        assert_eq!(
            session.transaction().await.status(),
            TransactionStatus::Active
        );
    }

    // Nothing was ever shipped, so discarding the buffer *is* the rollback.
    #[tokio::test]
    async fn rollback_discards_the_buffered_writes() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session
            .transaction()
            .await
            .push_write(buffered_write("orders", "node-1", &[]));

        assert_eq!(
            control(&handler, &session, "ROLLBACK").await.unwrap(),
            ("end", "ROLLBACK".to_string())
        );
        let txn = session.transaction().await;
        assert_eq!(txn.status(), TransactionStatus::Idle);
        assert!(txn.writes().is_empty());
    }

    #[tokio::test]
    async fn commit_of_an_empty_block_ends_it() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        assert_eq!(
            control(&handler, &session, "COMMIT").await.unwrap(),
            ("end", "COMMIT".to_string())
        );
        assert_eq!(
            session.transaction().await.status(),
            TransactionStatus::Idle
        );
    }

    // PostgreSQL answers a COMMIT of a failed block with the ROLLBACK tag: the
    // client asked to commit and must be told it did not.
    #[tokio::test]
    async fn commit_of_a_failed_block_reports_rollback() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session.transaction().await.mark_failed();

        assert_eq!(
            control(&handler, &session, "COMMIT").await.unwrap(),
            ("end", "ROLLBACK".to_string())
        );
        assert_eq!(
            session.transaction().await.status(),
            TransactionStatus::Idle
        );
    }

    // Outside a block both are no-ops that PostgreSQL accepts with a warning;
    // failing them would break clients that end a block defensively.
    #[tokio::test]
    async fn commit_and_rollback_outside_a_block_are_accepted() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        assert_eq!(
            control(&handler, &session, "COMMIT").await.unwrap(),
            ("end", "COMMIT".to_string())
        );
        assert_eq!(
            control(&handler, &session, "ROLLBACK").await.unwrap(),
            ("end", "ROLLBACK".to_string())
        );
    }

    // A second BEGIN must not reset the block: the client believes its earlier
    // statements are still pending, and re-opening would discard them.
    #[tokio::test]
    async fn a_nested_begin_keeps_the_open_block_and_its_writes() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session
            .transaction()
            .await
            .push_write(buffered_write("orders", "node-1", &[]));

        assert_eq!(
            control(&handler, &session, "BEGIN").await.unwrap(),
            ("start", "BEGIN".to_string())
        );
        assert_eq!(session.transaction().await.writes().len(), 1);
    }

    // `BEGIN ... END` is a procedural block, not a transaction: accepting it as a
    // BEGIN would leave the client's statements unexecuted and unreported.
    #[tokio::test]
    async fn an_inline_begin_end_block_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        let err = handler
            .begin_transaction(&[], None, true, &session)
            .await
            .unwrap_err();
        let (code, message) = reported(err);
        assert_eq!(code, "0A000");
        assert!(message.contains("BEGIN ... END"), "got: {message}");
        assert!(
            !session.transaction().await.is_open(),
            "a refused BEGIN must not open a block"
        );
    }

    // BEGIN READ ONLY is honored by refusing writes, not by ignoring the mode.
    #[tokio::test]
    async fn a_read_only_block_refuses_writes_but_allows_reads() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN READ ONLY")
            .await
            .unwrap();
        assert!(session.transaction().await.is_read_only());

        for sql in [
            "INSERT INTO orders (id) VALUES (1)",
            "UPDATE orders SET amount = 1 WHERE id = 1",
            "DELETE FROM orders WHERE id = 1",
            "COPY orders FROM '/tmp/orders.csv' (FORMAT CSV)",
        ] {
            let (code, _) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "25006", "`{sql}` must be refused as read-only");
        }
        for sql in [
            "SELECT * FROM orders",
            // An export writes a file, not a table: nothing about it is a database
            // write, so a read-only block has no reason to refuse it.
            "COPY orders TO '/tmp/orders.csv' (FORMAT CSV)",
        ] {
            assert!(
                allows(&handler, &session, sql).await.is_ok(),
                "`{sql}` reads, so a read-only block must allow it"
            );
        }
    }

    // View DDL reaches no shard, but it is written to the catalog the moment it is
    // accepted — so a block that later rolls back would leave the definition
    // behind, and the refusal has to say that rather than talk about shards.
    #[tokio::test]
    async fn view_ddl_inside_a_block_is_refused_with_the_reason_it_has() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();

        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "ALTER VIEW v AS SELECT 2",
            "DROP VIEW v",
        ] {
            let (code, message) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "0A000", "`{sql}` must be refused inside a block");
            assert!(message.contains("ROLLBACK"), "got: {message}");
            assert!(
                !message.contains("shard"),
                "a view reaches no shard, got: {message}"
            );
        }
    }

    // An export is a read, so it is refused for exactly the reason a SELECT is: the
    // block's writes are still in the coordinator, so the file would be written
    // without them and look complete.
    #[tokio::test]
    async fn an_export_of_a_table_the_block_has_written_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session
            .transaction()
            .await
            .push_write(buffered_write("orders", "node-1", &[]));

        for sql in [
            "COPY orders TO '/tmp/orders.csv' (FORMAT CSV)",
            "COPY (SELECT id FROM customers JOIN orders USING (id)) TO '/tmp/j.csv' (FORMAT CSV)",
        ] {
            let (code, message) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "0A000", "`{sql}` must be refused inside a block");
            assert!(message.contains("COMMIT"), "got: {message}");
        }

        // An import reads a file rather than a table, and its rows are literals by
        // the time they are buffered — so the count reported at COMMIT is exact and
        // there is nothing to refuse.
        assert!(
            allows(
                &handler,
                &session,
                "COPY orders FROM '/tmp/orders.csv' (FORMAT CSV)"
            )
            .await
            .is_ok(),
            "an import into a table the block has written is still just a write"
        );
        assert!(
            allows(
                &handler,
                &session,
                "COPY customers TO '/tmp/customers.csv' (FORMAT CSV)"
            )
            .await
            .is_ok(),
            "exporting a table the block has not written is fine"
        );
    }

    // The guard only speaks for a session that is inside a block; outside one it
    // must wave everything through, including the statements it would refuse.
    #[tokio::test]
    async fn outside_a_block_nothing_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        for sql in [
            "SELECT * FROM orders",
            "INSERT INTO orders (id) VALUES (1)",
            "UPDATE orders SET amount = 1 WHERE id = 1",
            "DELETE FROM orders WHERE id = 1",
            "CREATE TABLE t (id INTEGER)",
            "TRUNCATE TABLE orders",
        ] {
            assert!(
                allows(&handler, &session, sql).await.is_ok(),
                "`{sql}` needs no transaction to run"
            );
        }
    }

    // Inside a block the buffer decides: an INSERT's row count is known from the
    // statement, an UPDATE/DELETE's is not, and DDL is not buffered at all. Each
    // refusal names the command so the client knows what to move out of the block.
    #[tokio::test]
    async fn inside_a_block_only_statements_with_a_knowable_answer_are_allowed() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();

        assert!(
            allows(&handler, &session, "INSERT INTO orders (id) VALUES (1)")
                .await
                .is_ok(),
            "an INSERT ... VALUES can be buffered and its row count reported honestly"
        );

        for (sql, named) in [
            ("UPDATE orders SET amount = 1 WHERE id = 1", "UPDATE"),
            ("DELETE FROM orders WHERE id = 1", "DELETE"),
        ] {
            let (code, message) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "0A000");
            assert!(message.contains(named), "got: {message}");
        }

        for sql in [
            "CREATE TABLE t (id INTEGER)",
            "ALTER TABLE orders ADD COLUMN x INTEGER",
            "DROP TABLE orders",
            "TRUNCATE TABLE orders",
        ] {
            let (code, message) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "0A000", "`{sql}` must be refused inside a block");
            assert!(message.contains("DDL"), "got: {message}");
        }
    }

    // The block's writes sit in the coordinator, so a read of a written table
    // would quietly miss them. Other tables are unaffected.
    #[tokio::test]
    async fn reading_a_table_the_block_has_written_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session
            .transaction()
            .await
            .push_write(buffered_write("orders", "node-1", &[]));

        let (code, message) = reported(
            allows(&handler, &session, "SELECT * FROM orders")
                .await
                .unwrap_err(),
        );
        assert_eq!(code, "0A000");
        assert!(message.contains("COMMIT"), "got: {message}");

        assert!(
            allows(&handler, &session, "SELECT * FROM customers")
                .await
                .is_ok(),
            "a table the block has not written is still readable"
        );
    }

    // An `INSERT ... SELECT` runs its source query now, on a database that does
    // not hold the block's buffered writes — so it is a read of the source tables
    // and refused for the same reason a plain SELECT of them is. Every relation
    // counts, not just the first `FROM`: a join or a subquery reads its tables too.
    #[tokio::test]
    async fn an_insert_from_a_query_reading_a_written_table_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        session
            .transaction()
            .await
            .push_write(buffered_write("orders", "node-1", &[]));

        for sql in [
            "INSERT INTO archive (id) SELECT id FROM orders",
            "INSERT INTO archive (id) SELECT c.id FROM customers c JOIN orders o ON o.id = c.id",
            "INSERT INTO archive (id) SELECT id FROM customers WHERE id IN (SELECT id FROM orders)",
            // The target table is read as much as any other when it is its own source.
            "INSERT INTO orders (id) SELECT id + 1 FROM orders",
        ] {
            let (code, message) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "0A000", "`{sql}` must be refused inside a block");
            assert!(message.contains("COMMIT"), "got: {message}");
        }

        assert!(
            allows(
                &handler,
                &session,
                "INSERT INTO orders (id) SELECT id FROM customers"
            )
            .await
            .is_ok(),
            "reading a table the block has not written is fine, even to write one it has"
        );
    }

    // Once a statement inside the block has failed, everything but transaction
    // control is refused with 25P02 — the state drivers recognize as "roll back".
    #[tokio::test]
    async fn a_failed_block_refuses_everything_until_it_ends() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let session = SessionState::default();

        control(&handler, &session, "BEGIN").await.unwrap();
        control(&handler, &session, "SAVEPOINT sp").await.unwrap();
        session.transaction().await.mark_failed();

        for sql in [
            "SELECT * FROM orders",
            "INSERT INTO orders (id) VALUES (1)",
            "CREATE TABLE t (id INTEGER)",
        ] {
            let (code, _) = reported(allows(&handler, &session, sql).await.unwrap_err());
            assert_eq!(code, "25P02", "`{sql}` must be refused in a failed block");
        }

        // Rolling back to a savepoint is how a client recovers without losing the
        // whole block, so transaction control still runs while the block is failed.
        control(&handler, &session, "ROLLBACK TO SAVEPOINT sp")
            .await
            .unwrap();
        assert_eq!(
            session.transaction().await.status(),
            TransactionStatus::Active
        );
        assert!(
            allows(&handler, &session, "INSERT INTO orders (id) VALUES (1)")
                .await
                .is_ok(),
            "the block is usable again after rolling back to the savepoint"
        );
    }

    // Two node sets cannot be committed atomically, and the refusal happens
    // before anything is sent: the client is told nothing was written, and that
    // must be true.
    #[tokio::test]
    async fn a_commit_spanning_node_sets_is_refused_before_anything_is_sent() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let writes = vec![
            buffered_write("orders", "node-1", &["node-2"]),
            buffered_write("customers", "node-3", &["node-4"]),
        ];

        let (code, message) = reported(handler.flush_transaction(writes).await.unwrap_err());
        assert_eq!(code, "0A000");
        assert!(
            message.contains("2 independent shard groups"),
            "got: {message}"
        );
        assert!(
            message.contains("nothing was written"),
            "the client must be told the commit applied nothing, got: {message}"
        );
        assert!(
            message.contains("allow_cross_shard_transactions"),
            "the error should name the opt-in, got: {message}"
        );
    }

    // Nothing to ship is a successful commit, whatever the flag says.
    #[tokio::test]
    async fn committing_no_writes_reaches_no_node() {
        for allow_cross_shard in [false, true] {
            let handler = VaireDbQueryHandler::for_tests(allow_cross_shard);
            assert!(handler.flush_transaction(Vec::new()).await.is_ok());
        }
    }

    // A client that asked to chain and got a plain COMMIT would run its next
    // statement outside the transaction it believes is open, so the request has to
    // fail rather than be partly honored.
    #[test]
    fn and_chain_is_refused_rather_than_quietly_dropped() {
        assert!(reject_chain(false, "COMMIT").is_ok());
        let (code, message) = reported(reject_chain(true, "COMMIT").unwrap_err());
        assert_eq!(code, "0A000");
        assert!(message.contains("COMMIT AND CHAIN"), "got: {message}");
        assert!(
            message.contains("followed by BEGIN"),
            "the error should say what to do instead, got: {message}"
        );
    }

    #[test]
    fn a_transaction_modifier_is_refused_by_name() {
        assert!(reject_modifier(None, "BEGIN").is_ok());
        let (code, message) =
            reported(reject_modifier(Some(TransactionModifier::Exclusive), "BEGIN").unwrap_err());
        assert_eq!(code, "0A000");
        assert!(message.contains("BEGIN EXCLUSIVE"), "got: {message}");
    }

    // These are the SQLSTATEs drivers branch on, so each error must carry the one
    // that matches the state the session is actually in.
    #[test]
    fn transaction_state_errors_carry_the_expected_sqlstate() {
        assert_eq!(reported(in_failed_transaction()).0, "25P02");
        assert_eq!(reported(read_only_transaction("INSERT")).0, "25006");
        assert_eq!(reported(no_active_transaction("SAVEPOINT")).0, "25P01");
        assert_eq!(reported(unknown_savepoint("sp")).0, "3B001");
    }

    #[test]
    fn savepoint_errors_name_the_savepoint_and_the_command() {
        let (_, message) = reported(unknown_savepoint("sp1"));
        assert!(message.contains("\"sp1\""), "got: {message}");
        let (_, message) = reported(no_active_transaction("RELEASE SAVEPOINT"));
        assert!(message.contains("RELEASE SAVEPOINT"), "got: {message}");
    }
}
