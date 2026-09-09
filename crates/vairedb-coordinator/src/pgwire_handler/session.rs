//! Per-connection session state for the pgwire handler.
//!
//! The query handler is one `Arc` shared by every connection, so anything that
//! belongs to a single client lives here instead and is stored in pgwire's
//! per-connection [`SessionExtensions`](pgwire::api::SessionExtensions), which
//! drops with the connection.
//!
//! Today that state is the explicit transaction block a client opens with
//! `BEGIN`. VaireDB has no cross-shard commit protocol, so a transaction is not
//! held open on the core nodes: the statements are **buffered** in the
//! coordinator and shipped at `COMMIT`. That makes `ROLLBACK` a real rollback
//! (nothing was shipped) and lets a single-shard transaction — the common ORM
//! case — be applied as one genuine DuckDB transaction.
//!
//! The connection's runtime parameters live here for the same reason — `SET`
//! changes one client's view and nothing else's. Their own logic is in
//! [`crate::pgwire_handler::session_params`].
//!
//! So does a `COPY ... FROM STDIN` in progress: the client is mid-upload, and the
//! sink taking its rows has to be the same one the `COPY` statement opened.

use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use crate::catalog::ShardMeta;
use crate::pgwire_handler::copy_stream::CopySink;
use crate::pgwire_handler::session_params::SessionParams;
use crate::replication::BatchStatement;

/// Where a connection stands with respect to an explicit transaction block.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum TransactionStatus {
    /// No transaction block open; every statement applies on its own.
    #[default]
    Idle,
    /// Inside a `BEGIN` block, buffering writes until `COMMIT`.
    Active,
    /// A statement inside the block failed. Further statements are refused with
    /// `25P02` until the client ends the block (or rolls back to a savepoint).
    Failed,
}

/// A shard-local write buffered until `COMMIT`.
#[derive(Debug, Clone)]
pub(crate) struct BufferedWrite {
    /// The shard the statement applies to. Also names the nodes it must reach.
    pub(crate) shard: ShardMeta,
    /// The rewritten shard-local statement and its bind parameters.
    pub(crate) statement: BatchStatement,
    /// Acknowledgments the statement's table requires.
    pub(crate) quorum_size: usize,
    /// Logical table the client wrote to, so a read inside the block can tell it
    /// would not see these rows.
    pub(crate) table_name: String,
}

/// The buffered transaction block of one connection.
#[derive(Debug, Default)]
pub(crate) struct Transaction {
    status: TransactionStatus,
    /// Writes accumulated since `BEGIN`, in the order the client issued them.
    writes: Vec<BufferedWrite>,
    /// Savepoint marks: the name and how many writes were buffered when it was
    /// set. Rolling back to one truncates the buffer to that length.
    savepoints: Vec<(String, usize)>,
    /// Set by `BEGIN READ ONLY`: writes inside the block are refused.
    read_only: bool,
}

impl Transaction {
    pub(crate) fn status(&self) -> TransactionStatus {
        self.status
    }

    pub(crate) fn is_open(&self) -> bool {
        self.status != TransactionStatus::Idle
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// The buffered writes in client order. Only the tests inspect the buffer in
    /// place; the commit path takes it with [`Self::take_writes`].
    #[cfg(test)]
    pub(crate) fn writes(&self) -> &[BufferedWrite] {
        &self.writes
    }

    /// Open a transaction block. `read_only` comes from `BEGIN READ ONLY`.
    pub(crate) fn begin(&mut self, read_only: bool) {
        self.status = TransactionStatus::Active;
        self.writes.clear();
        self.savepoints.clear();
        self.read_only = read_only;
    }

    /// Close the block and discard everything buffered, returning to `Idle`.
    /// Used by both `COMMIT` (after the writes have been shipped) and `ROLLBACK`.
    pub(crate) fn end(&mut self) {
        self.status = TransactionStatus::Idle;
        self.writes.clear();
        self.savepoints.clear();
        self.read_only = false;
    }

    /// Take the buffered writes, leaving the block open and empty. `COMMIT` ships
    /// what it gets back; on failure nothing is re-buffered, because the block is
    /// ending either way.
    pub(crate) fn take_writes(&mut self) -> Vec<BufferedWrite> {
        self.savepoints.clear();
        std::mem::take(&mut self.writes)
    }

    /// Buffer a write to be shipped at `COMMIT`.
    pub(crate) fn push_write(&mut self, write: BufferedWrite) {
        self.writes.push(write);
    }

    /// Mark the block failed after a statement error, so following statements are
    /// refused until it ends. PostgreSQL semantics: the client must `ROLLBACK`
    /// (or roll back to a savepoint) to make progress.
    pub(crate) fn mark_failed(&mut self) {
        if self.status == TransactionStatus::Active {
            self.status = TransactionStatus::Failed;
        }
    }

    /// Establish a savepoint at the current buffer position. A repeated name
    /// shadows the earlier one, as in PostgreSQL.
    pub(crate) fn savepoint(&mut self, name: &str) {
        self.savepoints.push((name.to_string(), self.writes.len()));
    }

    /// Discard the writes buffered since `name` was established, keeping the
    /// savepoint itself so it can be rolled back to again. Clears a failed block,
    /// which is how a client recovers from an error without losing the whole
    /// transaction. Returns `false` if no such savepoint exists.
    pub(crate) fn rollback_to_savepoint(&mut self, name: &str) -> bool {
        let Some(pos) = self.find_savepoint(name) else {
            return false;
        };
        let mark = self.savepoints[pos].1;
        // Savepoints established after this one are destroyed by rolling back.
        self.savepoints.truncate(pos + 1);
        self.writes.truncate(mark);
        self.status = TransactionStatus::Active;
        true
    }

    /// Forget `name` (and any savepoint established after it) while keeping its
    /// writes: they become part of the enclosing block. Returns `false` if no
    /// such savepoint exists.
    pub(crate) fn release_savepoint(&mut self, name: &str) -> bool {
        let Some(pos) = self.find_savepoint(name) else {
            return false;
        };
        self.savepoints.truncate(pos);
        true
    }

    /// Position of the most recent savepoint named `name`, matched
    /// case-sensitively on the already-canonicalized name.
    fn find_savepoint(&self, name: &str) -> Option<usize> {
        self.savepoints.iter().rposition(|(n, _)| n == name)
    }

    /// True if any buffered write targets `table`, meaning a read of that table
    /// inside the block would not see them.
    pub(crate) fn has_buffered_writes_for(&self, table: &str) -> bool {
        self.writes.iter().any(|w| w.table_name == table)
    }

    /// Group the buffered writes by the set of nodes they must reach, preserving
    /// client order within each group.
    ///
    /// Grouping by node set rather than by shard is deliberate: two shards that
    /// live on the same nodes can still be committed as one node-local
    /// transaction, so an ORM that writes two tables sharded the same way stays
    /// atomic. One group means the whole transaction is atomic; more than one
    /// means atomicity would need a cross-shard commit protocol VaireDB does not
    /// have yet.
    pub(crate) fn group_by_node_set(writes: Vec<BufferedWrite>) -> Vec<NodeSetGroup> {
        let mut groups: Vec<NodeSetGroup> = Vec::new();

        for write in writes {
            let key = node_set_key(&write.shard);
            match groups.iter_mut().find(|g| g.key == key) {
                Some(group) => {
                    group.quorum_size = group.quorum_size.max(write.quorum_size);
                    group.statements.push(write.statement);
                }
                None => groups.push(NodeSetGroup {
                    key,
                    shard: write.shard,
                    quorum_size: write.quorum_size,
                    statements: vec![write.statement],
                }),
            }
        }

        groups
    }
}

/// Buffered statements that all reach the same set of nodes, ready to ship as one
/// node-local transaction.
#[derive(Debug)]
pub(crate) struct NodeSetGroup {
    /// Primary plus sorted replica node ids; identifies the group.
    pub(crate) key: String,
    /// Any shard of the group, naming the nodes the batch is sent to.
    pub(crate) shard: ShardMeta,
    /// The strictest quorum any statement in the group requires.
    pub(crate) quorum_size: usize,
    pub(crate) statements: Vec<BatchStatement>,
}

/// Identify a shard's node set: primary first, then replicas sorted so the key
/// does not depend on the order the catalog happened to store them in.
fn node_set_key(shard: &ShardMeta) -> String {
    let mut replicas = shard.replica_node_ids.clone();
    replicas.sort();
    format!("{}|{}", shard.primary_node_id, replicas.join(","))
}

/// One connection's session state, held in pgwire's session extensions.
///
/// Each field is behind its own async mutex because both are mutated across
/// `await` points — the transaction across planning and shipping a write, the
/// parameters across the handler's own async dispatch. Separate locks so a `SHOW`
/// never waits on a write being shipped.
#[derive(Default)]
pub(crate) struct SessionState {
    transaction: Mutex<Transaction>,
    params: Mutex<SessionParams>,
    copy_in: Mutex<Option<CopySink>>,
}

impl SessionState {
    /// Fetch (creating on first use) the session state of the connection `client`
    /// belongs to. pgwire drops the extensions when the connection closes, so a
    /// buffered transaction cannot outlive its client.
    pub(crate) fn for_client<C: pgwire::api::ClientInfo>(client: &C) -> Arc<Self> {
        client.session_extensions().get_or_insert_with(|| Self {
            transaction: Mutex::default(),
            // Seeded from the `ParameterStatus` values this client was sent at
            // startup, so `SHOW` reports what it was already told rather than a
            // second copy of the same defaults.
            params: Mutex::new(SessionParams::for_client(client)),
            copy_in: Mutex::default(),
        })
    }

    pub(crate) async fn transaction(&self) -> MutexGuard<'_, Transaction> {
        self.transaction.lock().await
    }

    /// The `COPY ... FROM STDIN` this connection is in the middle of, if any.
    ///
    /// A copy in progress belongs to one connection: the client is mid-upload and
    /// the protocol allows nothing else until it finishes, so the sink lives here
    /// rather than on the handler — which is a single `Arc` shared by every
    /// connection. It is dropped with the connection, so a client that disconnects
    /// mid-copy cannot leave a half-fed sink behind.
    pub(crate) async fn copy_in(&self) -> MutexGuard<'_, Option<CopySink>> {
        self.copy_in.lock().await
    }

    /// The connection's runtime parameters — what `SET`, `SHOW` and `RESET` read
    /// and write.
    pub(crate) async fn params(&self) -> MutexGuard<'_, SessionParams> {
        self.params.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(table: &str, bucket: u32, primary: &str, replicas: &[&str]) -> ShardMeta {
        ShardMeta {
            shard_id: format!("{table}-{bucket}"),
            table_name: table.to_string(),
            primary_node_id: primary.to_string(),
            replica_node_ids: replicas.iter().map(|r| r.to_string()).collect(),
            hash_bucket: bucket,
            range_lower: String::new(),
            range_upper: String::new(),
        }
    }

    fn write(
        table: &str,
        bucket: u32,
        primary: &str,
        replicas: &[&str],
        sql: &str,
    ) -> BufferedWrite {
        let shard = shard(table, bucket, primary, replicas);
        BufferedWrite {
            statement: BatchStatement {
                sql: sql.to_string(),
                params: Vec::new(),
                shard_id: crate::util::shard_table_name(table, bucket),
            },
            shard,
            quorum_size: 1,
            table_name: table.to_string(),
        }
    }

    #[test]
    fn a_new_transaction_is_idle() {
        let txn = Transaction::default();
        assert_eq!(txn.status(), TransactionStatus::Idle);
        assert!(!txn.is_open());
    }

    #[test]
    fn begin_opens_an_empty_block() {
        let mut txn = Transaction::default();
        txn.push_write(write("t", 0, "n1", &[], "INSERT INTO t_shard0 VALUES (1)"));
        txn.begin(false);
        assert_eq!(txn.status(), TransactionStatus::Active);
        assert!(txn.writes().is_empty(), "BEGIN must not inherit writes");
        assert!(!txn.is_read_only());
    }

    #[test]
    fn begin_read_only_is_remembered() {
        let mut txn = Transaction::default();
        txn.begin(true);
        assert!(txn.is_read_only());
        txn.end();
        assert!(
            !txn.is_read_only(),
            "the flag belongs to the block, not the session"
        );
    }

    #[test]
    fn rollback_discards_the_buffer() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.push_write(write("t", 0, "n1", &[], "INSERT INTO t_shard0 VALUES (1)"));
        txn.end();
        assert_eq!(txn.status(), TransactionStatus::Idle);
        assert!(txn.writes().is_empty());
    }

    #[test]
    fn a_failed_block_stays_failed_until_it_ends() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.mark_failed();
        assert_eq!(txn.status(), TransactionStatus::Failed);
        txn.mark_failed();
        assert_eq!(txn.status(), TransactionStatus::Failed);
        txn.end();
        assert_eq!(txn.status(), TransactionStatus::Idle);
    }

    #[test]
    fn marking_an_idle_session_failed_does_nothing() {
        let mut txn = Transaction::default();
        txn.mark_failed();
        assert_eq!(txn.status(), TransactionStatus::Idle);
    }

    #[test]
    fn rollback_to_savepoint_keeps_earlier_writes() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.push_write(write("t", 0, "n1", &[], "one"));
        txn.savepoint("sp1");
        txn.push_write(write("t", 0, "n1", &[], "two"));
        txn.push_write(write("t", 0, "n1", &[], "three"));

        assert!(txn.rollback_to_savepoint("sp1"));
        assert_eq!(txn.writes().len(), 1);
        assert_eq!(txn.writes()[0].statement.sql, "one");
    }

    #[test]
    fn rollback_to_savepoint_clears_a_failed_block() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.savepoint("sp1");
        txn.mark_failed();
        assert!(txn.rollback_to_savepoint("sp1"));
        assert_eq!(txn.status(), TransactionStatus::Active);
    }

    #[test]
    fn rollback_to_savepoint_can_repeat_and_destroys_later_savepoints() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.savepoint("sp1");
        txn.push_write(write("t", 0, "n1", &[], "one"));
        txn.savepoint("sp2");

        assert!(txn.rollback_to_savepoint("sp1"));
        assert!(!txn.rollback_to_savepoint("sp2"), "sp2 was destroyed");
        assert!(
            txn.rollback_to_savepoint("sp1"),
            "sp1 survives its rollback"
        );
    }

    #[test]
    fn a_repeated_savepoint_name_shadows_the_earlier_one() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.savepoint("sp");
        txn.push_write(write("t", 0, "n1", &[], "one"));
        txn.savepoint("sp");
        txn.push_write(write("t", 0, "n1", &[], "two"));

        assert!(txn.rollback_to_savepoint("sp"));
        assert_eq!(txn.writes().len(), 1, "rolled back to the most recent 'sp'");
    }

    #[test]
    fn release_savepoint_keeps_its_writes() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.savepoint("sp1");
        txn.push_write(write("t", 0, "n1", &[], "one"));

        assert!(txn.release_savepoint("sp1"));
        assert_eq!(txn.writes().len(), 1);
        assert!(
            !txn.rollback_to_savepoint("sp1"),
            "released savepoint is gone"
        );
    }

    #[test]
    fn unknown_savepoint_names_are_reported() {
        let mut txn = Transaction::default();
        txn.begin(false);
        assert!(!txn.rollback_to_savepoint("nope"));
        assert!(!txn.release_savepoint("nope"));
    }

    #[test]
    fn buffered_tables_are_visible_to_the_read_guard() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.push_write(write("orders", 0, "n1", &[], "one"));
        assert!(txn.has_buffered_writes_for("orders"));
        assert!(!txn.has_buffered_writes_for("customers"));
    }

    #[test]
    fn take_writes_empties_the_buffer_but_keeps_the_block_open() {
        let mut txn = Transaction::default();
        txn.begin(false);
        txn.push_write(write("t", 0, "n1", &[], "one"));
        let taken = txn.take_writes();
        assert_eq!(taken.len(), 1);
        assert!(txn.writes().is_empty());
        assert_eq!(txn.status(), TransactionStatus::Active);
    }

    #[test]
    fn writes_to_the_same_node_set_form_one_group() {
        // Two different shards of two different tables that happen to share the
        // same primary and replicas: one node-local transaction can carry both.
        let writes = vec![
            write("orders", 0, "n1", &["n2"], "one"),
            write("customers", 2, "n1", &["n2"], "two"),
        ];
        let groups = Transaction::group_by_node_set(writes);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].statements.len(), 2);
        assert_eq!(groups[0].statements[0].sql, "one");
        assert_eq!(groups[0].statements[1].sql, "two");
    }

    #[test]
    fn replica_order_does_not_split_a_group() {
        let writes = vec![
            write("orders", 0, "n1", &["n2", "n3"], "one"),
            write("orders", 1, "n1", &["n3", "n2"], "two"),
        ];
        let groups = Transaction::group_by_node_set(writes);
        assert_eq!(groups.len(), 1, "the same node set in a different order");
    }

    #[test]
    fn different_node_sets_form_separate_groups() {
        let writes = vec![
            write("orders", 0, "n1", &["n2"], "one"),
            write("orders", 1, "n3", &["n4"], "two"),
            write("orders", 2, "n1", &["n2"], "three"),
        ];
        let groups = Transaction::group_by_node_set(writes);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].statements.len(), 2, "first node set, in order");
        assert_eq!(groups[0].statements[1].sql, "three");
        assert_eq!(groups[1].statements.len(), 1);
    }

    #[test]
    fn a_group_takes_the_strictest_quorum_of_its_writes() {
        let mut writes = vec![
            write("orders", 0, "n1", &["n2"], "one"),
            write("orders", 1, "n1", &["n2"], "two"),
        ];
        writes[1].quorum_size = 2;
        let groups = Transaction::group_by_node_set(writes);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].quorum_size, 2);
    }

    #[test]
    fn grouping_nothing_yields_no_groups() {
        assert!(Transaction::group_by_node_set(Vec::new()).is_empty());
    }
}
