//! SQL statement inspection used to decide how to route a query: classifying a
//! parsed statement and extracting the target table name from it.

use datafusion::common::TableReference;

use crate::sqlparser::ast::{
    FromTable, Ident, ObjectName, ObjectType, SetExpr, Statement, TableFactor, TableObject,
};

/// Coarse category of a SQL statement, used to choose between the read path
/// (scheduler) and the write path (write router), and to detect DDL.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    /// `MERGE INTO`: one statement that updates, inserts and deletes in the same
    /// pass. Kept apart from the three because deciding where it can run is a
    /// question about *two* relations, not one — see
    /// [`crate::pgwire_handler::merge`].
    Merge,
    CreateTable,
    AlterTable,
    DropTable,
    TruncateTable,
    /// `CREATE INDEX`: broadcast DDL that creates one physical index per shard —
    /// see [`crate::pgwire_handler::indexes`].
    CreateIndex,
    /// `DROP INDEX`: the counterpart, kept apart from [`QueryType::DropTable`]
    /// because an index resolves through its own namespace and its own catalog
    /// lookup, and because `DROP INDEX` naming a table must stay a wrong-object
    /// -type error rather than dropping it.
    DropIndex,
    /// `CREATE VIEW` (including `CREATE OR REPLACE VIEW`): records a query under a
    /// name. Nothing is broadcast — a view is the one relation that lives only in
    /// the coordinator, see [`crate::pgwire_handler::views`].
    CreateView,
    /// `ALTER VIEW ... AS <query>`: redefines a recorded view. Only this form
    /// parses; `ALTER VIEW ... RENAME TO` is a syntax error in the dialect.
    AlterView,
    /// `DROP VIEW`: the counterpart, kept apart from [`QueryType::DropTable`] for
    /// the reason [`QueryType::DropIndex`] is — a view resolves through its own
    /// catalog lookup, and `DROP VIEW` naming a table must stay a wrong-object-type
    /// error rather than dropping it.
    DropView,
    /// `CREATE SCHEMA`: records a namespace. Like view DDL, nothing is broadcast —
    /// a schema holds no rows, and the per-shard tables carry their schema inside
    /// their own name — see [`crate::pgwire_handler::schemas`].
    CreateSchema,
    /// `DROP SCHEMA`: the counterpart, kept apart from [`QueryType::DropTable`] so
    /// it resolves against the schema namespace instead of reporting a missing
    /// table.
    DropSchema,
    /// `ALTER SCHEMA ... RENAME TO`: renames the namespace record. Nothing is
    /// broadcast for the same reason `CREATE SCHEMA` broadcasts nothing, and only an
    /// *empty* schema can be renamed — see [`crate::pgwire_handler::schemas`].
    AlterSchema,
    /// `COPY ... TO/FROM '<file>'`: bulk export and import over a CSV file on the
    /// coordinator. Both directions run in the coordinator — the export on the read
    /// path, the import through the INSERT lane — see
    /// [`crate::pgwire_handler::copy`].
    Copy,
    /// `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`: statements that steer a
    /// connection's transaction block rather than touch data. Handled entirely in
    /// the coordinator's session state — see
    /// [`crate::pgwire_handler::transaction`].
    TransactionControl,
    /// `SET`/`SHOW`/`RESET`: statements that read or change one connection's
    /// runtime parameters. Answered entirely in the coordinator, from the session's
    /// own parameter map — see [`crate::pgwire_handler::session_params`]. Nothing is
    /// broadcast: a runtime parameter is a property of the client's connection, and
    /// no shard could answer it differently.
    SessionParam,
    /// `EXPLAIN` / `EXPLAIN ANALYZE` / `DESCRIBE`: statements that report how a
    /// query would run, or what shape a relation has, instead of returning rows.
    /// Answered on the read path — an `EXPLAIN` is the plan a SELECT already builds,
    /// rendered rather than executed — see
    /// [`crate::pgwire_handler::introspection`].
    Explain,
    /// Anything not handled specially (e.g. `CALL`).
    Other,
}

impl QueryType {
    /// True for the statements the write path executes on DuckDB — DML plus the
    /// DDL that is broadcast to every shard.
    ///
    /// Read-path statements go to DataFusion instead, which is why the two are
    /// prepared differently: see [`crate::pgwire_handler::parser::parse_sql`].
    /// Add new write statement kinds here as they gain support, or they will keep
    /// being prepared for a planner that never sees them.
    pub fn is_write_path(&self) -> bool {
        matches!(
            self,
            QueryType::Insert
                | QueryType::Update
                | QueryType::Delete
                | QueryType::Merge
                | QueryType::CreateTable
                | QueryType::AlterTable
                | QueryType::DropTable
                | QueryType::TruncateTable
                | QueryType::CreateIndex
                | QueryType::DropIndex
                // A COPY does its own planning: the export plans its source as a
                // read when it runs, and the import plans the INSERTs it builds
                // from the file. Neither is a statement the extended protocol can
                // usefully plan up front.
                | QueryType::Copy
        )
    }

    /// True for the statements whose AST must be the client's own, unrewritten by
    /// the pg-compatibility parser — see [`crate::pgwire_handler::parser::parse_sql`].
    ///
    /// That is every write-path statement, because a rewrite would be shipped to
    /// the shards as data or as DDL, plus view DDL: a view's definition is *stored*
    /// and re-planned on every read, and the read-path rewrites are applied then, so
    /// storing a rewritten body would bake one client's compatibility shim into the
    /// catalog.
    pub fn wants_verbatim_ast(&self) -> bool {
        self.is_write_path() || matches!(self, QueryType::CreateView | QueryType::AlterView)
    }
}

/// Classify a parsed statement into its [`QueryType`].
///
/// Transaction control is deliberately *not* write-path: the statements never
/// reach DuckDB — `BEGIN` and friends only move the coordinator's session state,
/// and the buffered writes they release at `COMMIT` were each classified (and
/// parsed verbatim) as DML in their own right.
pub fn classify_statement(stmt: &Statement) -> QueryType {
    match stmt {
        Statement::Query(_) => QueryType::Select,
        Statement::Insert(_) => QueryType::Insert,
        Statement::Update { .. } => QueryType::Update,
        Statement::Delete(_) => QueryType::Delete,
        Statement::Merge(_) => QueryType::Merge,
        Statement::CreateTable(_) => QueryType::CreateTable,
        Statement::AlterTable { .. } => QueryType::AlterTable,
        Statement::CreateIndex(_) => QueryType::CreateIndex,
        Statement::CreateSchema { .. } => QueryType::CreateSchema,
        Statement::AlterSchema(_) => QueryType::AlterSchema,
        Statement::CreateView { .. } => QueryType::CreateView,
        Statement::AlterView { .. } => QueryType::AlterView,
        // Split on the object kind rather than on the statement: `DROP INDEX`,
        // `DROP VIEW` and `DROP SCHEMA` each resolve through a namespace of their
        // own, and every other kind must keep reaching the table path, which is
        // what reports `42809` for a kind that names a table.
        Statement::Drop {
            object_type: ObjectType::Index,
            ..
        } => QueryType::DropIndex,
        Statement::Drop {
            object_type: ObjectType::View,
            ..
        } => QueryType::DropView,
        Statement::Drop {
            object_type: ObjectType::Schema,
            ..
        } => QueryType::DropSchema,
        Statement::Drop { .. } => QueryType::DropTable,
        Statement::Truncate(_) => QueryType::TruncateTable,
        Statement::Copy { .. } => QueryType::Copy,
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => QueryType::TransactionControl,
        // `RESET` arrives here as a `Set` too: the parser rewrites it to the
        // `SET x TO DEFAULT` PostgreSQL defines it to be — see
        // [`crate::pgwire_handler::session_params::parse_reset`]. The `SHOW TABLES`
        // family does *not*: each parses into a statement of its own, so only
        // `SHOW <parameter>` reaches `ShowVariable`.
        Statement::Set(_) | Statement::ShowVariable { .. } => QueryType::SessionParam,
        // `DESCRIBE <relation>` parses to `ExplainTable` and `EXPLAIN <statement>`
        // / `DESCRIBE <query>` to `Explain`; both are sorted out by alias in
        // [`crate::pgwire_handler::introspection`], which is also where the forms
        // that cannot be answered truthfully are refused by name.
        Statement::Explain { .. } | Statement::ExplainTable { .. } => QueryType::Explain,
        _ => QueryType::Other,
    }
}

/// The schema every unqualified relation belongs to, as in PostgreSQL. It is not
/// a catalog record: it always exists, cannot be created and cannot be dropped —
/// see [`crate::pgwire_handler::schemas`].
pub const DEFAULT_SCHEMA: &str = "public";

/// Reduce a (possibly quoted or schema-qualified) relation to its canonical
/// logical name — the catalog key, the physical shard-name input, and the
/// DataFusion registration key.
///
/// The key is the bare relation name for a relation in the default schema and
/// `"{schema}.{relation}"` for any other, so a name that carries no qualifier and
/// one that spells out `public.` are the same key. Normalization mirrors
/// PostgreSQL/DataFusion identifier folding, part by part: an unquoted part is
/// lowercased, a quoted part is taken verbatim so its case survives. A leading
/// database/catalog part is ignored — only the last two parts are read. Returns
/// `None` if either part is not a plain identifier (e.g. a function part).
///
/// Because the key joins the two parts with `.`, a *quoted* relation name that
/// itself contains a `.` reads back as a qualified name (`"sales.orders"` is the
/// same key as `sales.orders`). That is deliberate: the alternative is a physical
/// name with a `.` in it, which the storage nodes cannot splice unquoted.
pub fn canonical_table_name(name: &ObjectName) -> Option<String> {
    let (schema, relation) = canonical_schema_and_table(name)?;
    Some(qualified_name(&schema, &relation))
}

/// Split a relation into its canonical schema and relation parts, defaulting the
/// schema to [`DEFAULT_SCHEMA`] when the name carries no qualifier. The pieces
/// [`canonical_table_name`] joins — separate for the callers that have to name the
/// schema on its own (schema-existence checks, error messages).
pub fn canonical_schema_and_table(name: &ObjectName) -> Option<(String, String)> {
    let mut parts = name.0.iter().rev();
    let relation = canonicalize_ident(parts.next()?.as_ident()?);
    let schema = match parts.next() {
        Some(part) => canonicalize_ident(part.as_ident()?),
        None => DEFAULT_SCHEMA.to_string(),
    };
    Some((schema, relation))
}

/// Join a canonical schema and relation into a catalog key, dropping the
/// qualifier for the default schema so the common case keeps its bare name.
pub fn qualified_name(schema: &str, relation: &str) -> String {
    if schema == DEFAULT_SCHEMA {
        relation.to_string()
    } else {
        format!("{schema}.{relation}")
    }
}

/// The schema a canonical key names, [`DEFAULT_SCHEMA`] for a bare key.
pub fn schema_of(key: &str) -> &str {
    match key.split_once('.') {
        Some((schema, _)) => schema,
        None => DEFAULT_SCHEMA,
    }
}

/// The relation part of a canonical key, without its schema qualifier.
pub fn relation_of(key: &str) -> &str {
    match key.split_once('.') {
        Some((_, relation)) => relation,
        None => key,
    }
}

/// The [`TableReference`] a canonical key is registered under in DataFusion — the one
/// place the flat key becomes a *structured* name.
///
/// A qualified key registers as `Partial { schema, table }`, not as a bare name holding
/// the dot, because DataFusion's expression tree carries a column's qualifier as a
/// [`TableReference`] too, and `datafusion-proto` encodes that qualifier by printing it and
/// decodes it by re-parsing the printed string. A bare `"sales.orders"` survives the print
/// and comes back from the parse as `Partial { sales, orders }` — a qualifier that matches
/// no registered relation, which is why a *filtered* or *sorted* read of a qualified
/// relation used to fail `42703` while an unfiltered one (whose plan carries no column
/// qualifier) worked. Registering the structured name in the first place makes the
/// round trip an identity.
///
/// The residue is the key whose *relation* part still contains a dot, which
/// [`canonical_table_name`] produces for a relation quoted with a dot in a non-default
/// schema (`CREATE TABLE sales."a.b"`). There is no two-part reference for it, so it keeps
/// the bare key and keeps today's behaviour.
pub fn table_reference(key: &str) -> TableReference {
    match key.split_once('.') {
        Some((schema, relation)) if !relation.contains('.') => {
            TableReference::partial(schema.to_string(), relation.to_string())
        }
        _ => TableReference::bare(key.to_string()),
    }
}

/// Canonical logical name for a single identifier: verbatim when quoted,
/// lowercased when unquoted.
///
/// Every catalog key — table name, column name, shard key — is stored in this
/// form, and every comparison against one must go through here (or
/// [`canonicalize_ident_str`]). Comparing a raw `Ident::value` instead makes
/// `INSERT INTO t (ID, v)` miss a shard key declared `id`, which does not fail
/// loudly: it falls back to broadcasting the row to every shard.
pub fn canonicalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Render a canonical identifier back into SQL: bare when it folds to itself,
/// double-quoted when it carries case an engine would fold away.
///
/// The inverse of [`canonicalize_ident`], and needed wherever the coordinator emits
/// SQL built from catalog metadata rather than from the client's own statement — a
/// column stored as `"Amount"` named unquoted would look for `amount` instead.
pub fn quoted_if_folded(name: &str) -> String {
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        format!("\"{name}\"")
    } else {
        name.to_string()
    }
}

/// Canonicalize an identifier that reached us as a string rather than a parsed
/// [`Ident`] — e.g. the `shard_by = 'customer_id'` table option, whose value is a
/// string literal. Folds like [`canonicalize_ident`]: a `"..."`-wrapped name is
/// taken verbatim, anything else is lowercased.
pub fn canonicalize_ident_str(name: &str) -> String {
    match name
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(quoted) => quoted.to_string(),
        None => name.to_ascii_lowercase(),
    }
}

/// Return the target table name for a write or DDL statement (INSERT, UPDATE,
/// DELETE, MERGE, CREATE/ALTER/DROP/TRUNCATE TABLE), or `None` if the statement has no single
/// resolvable target. The name is canonicalized via [`canonical_table_name`].
/// Use `extract_select_table_name` for SELECTs.
pub fn extract_table_name(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => canonical_table_name(name),
            _ => None,
        },
        Statement::Update(update) => relation_name(&update.table.relation),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) => t,
                FromTable::WithoutKeyword(t) => t,
            };
            relation_name(&tables.first()?.relation)
        }
        // The table being merged into, not the one being read from: the error
        // context and the shard lookup are both about the target.
        Statement::Merge(merge) => relation_name(&merge.table),
        Statement::CreateTable(create) => canonical_table_name(&create.name),
        Statement::AlterTable(alter) => canonical_table_name(&alter.name),
        // The table an index is built on, not the index: this is what the error
        // context and the shard lookup need. `DROP INDEX` names no table at all
        // and resolves its own through the catalog.
        Statement::CreateIndex(create) => canonical_table_name(&create.table_name),
        // A view's own name, unlike an index's: a view is the relation the
        // statement is about, and its body names whatever tables it likes.
        Statement::CreateView(create) => canonical_table_name(&create.name),
        Statement::AlterView { name, .. } => canonical_table_name(name),
        Statement::Drop { names, .. } => names.first().and_then(canonical_table_name),
        // Only the first target is reported; a multi-table TRUNCATE has no single
        // resolvable target and is refused by `plan_truncate` before this matters.
        Statement::Truncate(truncate) => truncate
            .table_names
            .first()
            .and_then(|target| canonical_table_name(&target.name)),
        _ => None,
    }
}

/// The canonical name of a relation that is a plain table, `None` for any other
/// source (a subquery, a join, a table function).
///
/// `None` rather than a best guess: every caller uses the name to find a relation,
/// and a derived table is not one. Naming the relation inside it would route a write
/// at the wrong rows.
///
/// A table *function* is a `TableFactor::Table` too — sqlparser distinguishes
/// `generate_series(1, 3)` from the relation `generate_series` only by the presence of
/// `args` — so the arguments have to be read, not just the variant. Treating the call
/// as the relation of the same name is not merely an inexact error context: at
/// `transaction::guard`, a table with buffered writes whose name a client also calls as
/// a function made the call read as a read of that table, and refused it.
fn relation_name(relation: &TableFactor) -> Option<String> {
    match relation {
        TableFactor::Table {
            name, args: None, ..
        } => canonical_table_name(name),
        _ => None,
    }
}

/// Return the canonical table name of a simple top-level SELECT's first FROM
/// relation, or `None` for non-SELECT statements, set operations, or non-table
/// sources (subqueries, joins, table functions).
pub fn extract_select_table_name(stmt: &Statement) -> Option<String> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    relation_name(&select.from.first()?.relation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser;
    use crate::util::shard_table_name;
    use crate::write_sql_cl;

    /// Parse through the coordinator's own entry rather than a bare sqlparser call: the
    /// statements classified in production are the ones `parse_sql` returns, and two of
    /// the classifications below exist only because it respells something first —
    /// `RESET` and `ALTER TABLE … SET SCHEMA` are in neither parser's grammar.
    fn statement(sql: &str) -> Statement {
        let mut statements = parser::parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}`: {e}"));
        assert_eq!(statements.len(), 1, "`{sql}` is not one statement");
        statements.pop().expect("one statement")
    }

    fn classify(sql: &str) -> QueryType {
        classify_statement(&statement(sql))
    }

    /// The relation a statement names, as an [`ObjectName`] — taken off a `DROP TABLE`
    /// because it is a write statement, so `parse_sql` hands back the client's own name
    /// with its quoting and its qualifiers intact.
    fn name(spelling: &str) -> ObjectName {
        match statement(&format!("DROP TABLE {spelling}")) {
            Statement::Drop { names, .. } => names.into_iter().next().expect("one name"),
            other => panic!("`{spelling}` did not parse as a DROP: {other:?}"),
        }
    }

    fn canonical(spelling: &str) -> Option<String> {
        canonical_table_name(&name(spelling))
    }

    /// One statement per [`QueryType`] a client can reach, because the variant chosen
    /// here decides which of the two execution paths runs — and a statement that lands
    /// on `Other` is answered by neither.
    #[test]
    fn every_reachable_query_type_is_reached() {
        let cases = [
            ("SELECT 1", QueryType::Select),
            ("INSERT INTO t VALUES (1)", QueryType::Insert),
            ("UPDATE t SET v = 1", QueryType::Update),
            ("DELETE FROM t", QueryType::Delete),
            (
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
                QueryType::Merge,
            ),
            ("CREATE TABLE t (id INT)", QueryType::CreateTable),
            ("ALTER TABLE t ADD COLUMN v INT", QueryType::AlterTable),
            ("DROP TABLE t", QueryType::DropTable),
            ("TRUNCATE TABLE t", QueryType::TruncateTable),
            ("CREATE INDEX i ON t (id)", QueryType::CreateIndex),
            ("DROP INDEX i", QueryType::DropIndex),
            ("CREATE VIEW v AS SELECT 1", QueryType::CreateView),
            ("ALTER VIEW v AS SELECT 2", QueryType::AlterView),
            ("DROP VIEW v", QueryType::DropView),
            ("CREATE SCHEMA s", QueryType::CreateSchema),
            ("DROP SCHEMA s", QueryType::DropSchema),
            ("ALTER SCHEMA s RENAME TO s2", QueryType::AlterSchema),
            ("COPY t TO '/tmp/t.csv'", QueryType::Copy),
            ("BEGIN", QueryType::TransactionControl),
            ("COMMIT", QueryType::TransactionControl),
            ("ROLLBACK", QueryType::TransactionControl),
            ("SAVEPOINT s", QueryType::TransactionControl),
            ("RELEASE SAVEPOINT s", QueryType::TransactionControl),
            ("SET search_path TO public", QueryType::SessionParam),
            ("SHOW search_path", QueryType::SessionParam),
            // Only a `SessionParam` because `parse_sql` respells it: `RESET` is in
            // neither parser's grammar, so classifying it is a joint claim.
            ("RESET search_path", QueryType::SessionParam),
            ("EXPLAIN SELECT 1", QueryType::Explain),
            ("EXPLAIN ANALYZE SELECT 1", QueryType::Explain),
        ];
        for (sql, expected) in cases {
            assert_eq!(classify(sql), expected, "`{sql}`");
        }
    }

    /// `ALTER TABLE … SET SCHEMA` is the other respelling, and it must stay an
    /// `AlterTable`: it is broadcast DDL, and a misclassification would answer it on the
    /// read path, where no shard is touched and the rename silently does not happen.
    #[test]
    fn set_schema_stays_alter_table() {
        assert_eq!(
            classify("ALTER TABLE t SET SCHEMA sales"),
            QueryType::AlterTable
        );
    }

    /// `DROP` splits on the object kind, not on the statement. Each namespace has its
    /// own catalog lookup, and the wrong one reports a missing table instead of the
    /// `42809` that tells a client it named the wrong kind of object.
    #[test]
    fn drop_splits_on_the_object_kind() {
        assert_eq!(classify("DROP INDEX i"), QueryType::DropIndex);
        assert_eq!(classify("DROP VIEW v"), QueryType::DropView);
        assert_eq!(classify("DROP SCHEMA s"), QueryType::DropSchema);
        assert_eq!(classify("DROP TABLE t"), QueryType::DropTable);
        // Every other kind keeps reaching the table path, which is what reports `42809`
        // for a kind that names a table. A `DROP SEQUENCE t` classified as anything else
        // would report "does not exist" and leave the client thinking `t` was gone.
        assert_eq!(classify("DROP SEQUENCE s"), QueryType::DropTable);
    }

    /// `Other` has to stay reachable: it is what turns a command no subsystem claims
    /// into a refusal that names it, rather than into a fake `OK`.
    #[test]
    fn an_unclaimed_command_falls_through_to_other() {
        assert_eq!(classify("CALL p()"), QueryType::Other);
    }

    /// Every spelling of transaction control, because one of them falling through to
    /// `Other` would be refused as unsupported — which is the whole failure the variant
    /// exists to prevent.
    #[test]
    fn every_spelling_of_transaction_control_is_recognized() {
        for sql in [
            "BEGIN",
            "BEGIN TRANSACTION",
            "BEGIN WORK",
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN READ ONLY",
            "START TRANSACTION",
            "COMMIT",
            "COMMIT WORK",
            "END",
            "ROLLBACK",
            "ROLLBACK TRANSACTION",
            "ABORT",
            "ROLLBACK TO SAVEPOINT sp",
            "SAVEPOINT sp",
            "RELEASE SAVEPOINT sp",
        ] {
            assert_eq!(classify(sql), QueryType::TransactionControl, "`{sql}`");
        }
    }

    /// The optional and decorated spellings of the statements whose keyword is not the
    /// whole grammar. `TRUNCATE orders` losing its classification would be refused as
    /// unsupported while `TRUNCATE TABLE orders` emptied the table; a `TRUNCATE ONLY`
    /// losing its *target* would empty nothing and report success.
    #[test]
    fn optional_keywords_and_decorations_do_not_change_the_route() {
        for sql in ["TRUNCATE TABLE orders", "TRUNCATE orders"] {
            assert_eq!(classify(sql), QueryType::TruncateTable, "`{sql}`");
        }
        for sql in [
            "CREATE INDEX idx ON orders (amount)",
            "CREATE UNIQUE INDEX idx ON orders (id)",
        ] {
            assert_eq!(classify(sql), QueryType::CreateIndex, "`{sql}`");
        }
        for sql in [
            "TRUNCATE TABLE ONLY orders",
            "TRUNCATE TABLE orders *",
            "TRUNCATE orders",
        ] {
            assert_eq!(
                extract_table_name(&statement(sql)).as_deref(),
                Some("orders"),
                "`{sql}` must resolve its target"
            );
        }
    }

    /// The write path is what executes on DuckDB. A statement wrongly inside it is
    /// broadcast to every shard; one wrongly outside is handed to a planner that never
    /// sees it — so the membership is pinned, not derived.
    #[test]
    fn the_write_path_is_dml_plus_broadcast_ddl() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET v = 1",
            "DELETE FROM t",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "CREATE TABLE t (id INT)",
            "ALTER TABLE t ADD COLUMN v INT",
            "DROP TABLE t",
            "TRUNCATE TABLE t",
            "CREATE INDEX i ON t (id)",
            "DROP INDEX i",
            "COPY t TO '/tmp/t.csv'",
        ] {
            assert!(classify(sql).is_write_path(), "`{sql}` left the write path");
        }

        for sql in [
            "SELECT 1",
            "EXPLAIN SELECT 1",
            // View and schema DDL are coordinator-only: a view lives in the catalog and
            // a schema holds no rows, so neither is broadcast.
            "CREATE VIEW v AS SELECT 1",
            "ALTER VIEW v AS SELECT 2",
            "DROP VIEW v",
            "CREATE SCHEMA s",
            "DROP SCHEMA s",
            "ALTER SCHEMA s RENAME TO s2",
            // Transaction control moves session state only. The writes it releases at
            // COMMIT were each classified as DML in their own right.
            "BEGIN",
            "COMMIT",
            "SET search_path TO public",
        ] {
            assert!(
                !classify(sql).is_write_path(),
                "`{sql}` joined the write path"
            );
        }
    }

    /// `wants_verbatim_ast` is wider than `is_write_path` by exactly view DDL, and that
    /// difference is the whole reason the two predicates exist: a view body is *stored*
    /// and re-planned on every read, so storing a rewritten one would bake one client's
    /// compatibility shim into the catalog permanently.
    #[test]
    fn view_ddl_wants_a_verbatim_ast_without_being_a_write() {
        for sql in ["CREATE VIEW v AS SELECT 1", "ALTER VIEW v AS SELECT 2"] {
            let query_type = classify(sql);
            assert!(!query_type.is_write_path(), "`{sql}`");
            assert!(query_type.wants_verbatim_ast(), "`{sql}`");
        }
        // Everything else agrees with `is_write_path`, in both directions.
        for sql in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t (id INT)",
            "SELECT 1",
            "DROP VIEW v",
            "BEGIN",
        ] {
            let query_type = classify(sql);
            assert_eq!(
                query_type.wants_verbatim_ast(),
                query_type.is_write_path(),
                "`{sql}`"
            );
        }
    }

    /// The fold, part by part: an unquoted part lowercases as PostgreSQL folds it, a
    /// quoted part keeps its case. Comparing a raw `Ident::value` instead does not fail
    /// loudly — it misses a shard key and broadcasts the row to every shard.
    #[test]
    fn identifier_folding_follows_postgresql() {
        assert_eq!(canonical("Orders").as_deref(), Some("orders"));
        assert_eq!(canonical("ORDERS").as_deref(), Some("orders"));
        assert_eq!(canonical("\"Orders\"").as_deref(), Some("Orders"));
        // Per part, not per name: one quoted part does not protect the other.
        assert_eq!(
            canonical("Sales.\"Orders\"").as_deref(),
            Some("sales.Orders")
        );
        assert_eq!(
            canonical("\"Sales\".Orders").as_deref(),
            Some("Sales.orders")
        );
    }

    /// A name carrying no qualifier and one spelling out `public.` are the same key, so
    /// `orders` and `public.orders` cannot become two relations.
    #[test]
    fn the_default_schema_is_folded_away() {
        assert_eq!(canonical("orders").as_deref(), Some("orders"));
        assert_eq!(canonical("public.orders").as_deref(), Some("orders"));
        assert_eq!(canonical("PUBLIC.orders").as_deref(), Some("orders"));
        // Quoted, it is still the default schema — the fold is on the canonical part.
        assert_eq!(canonical("\"public\".orders").as_deref(), Some("orders"));
        assert_eq!(canonical("sales.orders").as_deref(), Some("sales.orders"));
    }

    /// Only the last two parts are read: a client that spells out the database has named
    /// the same relation, and there is one catalog.
    #[test]
    fn a_leading_catalog_part_is_ignored() {
        assert_eq!(
            canonical("vairedb.sales.orders").as_deref(),
            Some("sales.orders")
        );
        assert_eq!(
            canonical("vairedb.public.orders").as_deref(),
            Some("orders")
        );
    }

    /// The pieces and the key are the same decision, so they cannot disagree about which
    /// schema a relation is in.
    #[test]
    fn the_key_is_its_parts_rejoined() {
        for spelling in [
            "orders",
            "public.orders",
            "sales.orders",
            "\"Sales\".orders",
        ] {
            let (schema, relation) = canonical_schema_and_table(&name(spelling)).expect("a name");
            let key = qualified_name(&schema, &relation);
            assert_eq!(
                canonical(spelling).as_deref(),
                Some(key.as_str()),
                "{spelling}"
            );
            assert_eq!(schema_of(&key), schema, "{spelling}");
            assert_eq!(relation_of(&key), relation, "{spelling}");
        }
    }

    /// A bare key is in the default schema — the qualifier's absence is the statement,
    /// not a missing value.
    #[test]
    fn a_bare_key_reads_back_as_the_default_schema() {
        assert_eq!(schema_of("orders"), DEFAULT_SCHEMA);
        assert_eq!(relation_of("orders"), "orders");
        assert_eq!(schema_of("sales.orders"), "sales");
        assert_eq!(relation_of("sales.orders"), "orders");
    }

    /// A qualified key registers as a *structured* reference. A bare `"sales.orders"`
    /// survives `datafusion-proto`'s print and comes back from its re-parse as
    /// `Partial { sales, orders }`, so a filtered or sorted read of a qualified relation
    /// used to fail `42703` while an unfiltered one worked.
    #[test]
    fn a_qualified_key_registers_as_a_two_part_reference() {
        assert_eq!(
            table_reference("sales.orders"),
            TableReference::partial("sales", "orders")
        );
        assert_eq!(table_reference("orders"), TableReference::bare("orders"));
        // Case is carried verbatim: the key is already canonical, so re-folding it here
        // would lose the case a quoted name was created with.
        assert_eq!(
            table_reference("Sales.MyTable"),
            TableReference::partial("Sales", "MyTable")
        );
    }

    /// And the round trip the structured key exists for: printing a reference and
    /// re-parsing it — what `datafusion-proto` does to every column qualifier in a
    /// distributed plan — has to be the identity, or a filtered read of the relation
    /// cannot resolve its own columns.
    #[test]
    fn a_structured_reference_survives_the_proto_round_trip() {
        for key in ["orders", "sales.orders", "Sales.MyTable"] {
            let reference = table_reference(key);
            assert_eq!(
                TableReference::parse_str_normalized(&reference.to_string(), true),
                reference,
                "the qualifier for `{key}` must survive being printed and re-parsed"
            );
        }
    }

    /// The residue, pinned as residue: a relation quoted with a dot inside a non-default
    /// schema has no two-part reference, so it keeps the bare key and today's behaviour.
    #[test]
    fn a_dotted_relation_part_keeps_the_bare_reference() {
        let key = canonical("sales.\"a.b\"").expect("a key");
        assert_eq!(key, "sales.a.b");
        assert_eq!(table_reference(&key), TableReference::bare("sales.a.b"));
    }

    /// A source that is not a relation has no name to report, and `None` is the answer
    /// rather than a guess: the callers use it to find a relation.
    ///
    /// The table-function case is the one that is not obvious. sqlparser parses
    /// `generate_series(1, 3)` as a `TableFactor::Table` carrying `args`, so matching the
    /// variant alone reports `generate_series` as the relation read — and
    /// `transaction::guard` then refuses the query as a read of a table with buffered
    /// writes, if a table of that name has any.
    #[test]
    fn a_source_that_is_not_a_relation_has_no_name() {
        for sql in [
            "SELECT * FROM (SELECT 1) AS s",
            "SELECT * FROM generate_series(1, 3)",
        ] {
            assert_eq!(extract_select_table_name(&statement(sql)), None, "`{sql}`");
        }
        // An alias is not a source of its own: the relation underneath is still the one
        // being read.
        assert_eq!(
            extract_select_table_name(&statement("SELECT o.id FROM Orders AS o")).as_deref(),
            Some("orders")
        );
    }

    /// Only the first target of a multi-relation statement is reported. That is not a
    /// resolution — it is why `plan_truncate` refuses the multi-table form before the
    /// name is used, and dropping the guard would empty one table of several.
    #[test]
    fn only_the_first_of_several_targets_is_reported() {
        assert_eq!(
            extract_table_name(&statement("DROP TABLE t1, t2")).as_deref(),
            Some("t1")
        );
        assert_eq!(
            extract_table_name(&statement("TRUNCATE TABLE t1, t2")).as_deref(),
            Some("t1")
        );
    }

    /// A statement with no target at all reports none, rather than the relation its
    /// inner query happens to name.
    #[test]
    fn a_statement_with_no_target_reports_none() {
        for sql in ["EXPLAIN SELECT 1", "BEGIN", "SET search_path TO public"] {
            assert_eq!(extract_table_name(&statement(sql)), None, "`{sql}`");
        }
    }

    /// [`quoted_if_folded`] is [`canonicalize_ident_str`]'s inverse, which is what lets
    /// the coordinator emit SQL built from catalog metadata: a column stored as
    /// `"Amount"` and named unquoted would look for `amount` instead.
    #[test]
    fn rendering_a_canonical_name_round_trips() {
        for canonical in ["amount", "Amount", "AMOUNT", "order_id"] {
            let rendered = quoted_if_folded(canonical);
            assert_eq!(canonicalize_ident_str(&rendered), canonical);
        }
        assert_eq!(quoted_if_folded("amount"), "amount");
        assert_eq!(quoted_if_folded("Amount"), "\"Amount\"");
        // A string that reached us unquoted folds like an unquoted identifier.
        assert_eq!(canonicalize_ident_str("Amount"), "amount");
        assert_eq!(canonicalize_ident_str("\"Amount\""), "Amount");
    }

    /// The target of a write or DDL statement, per kind — the name the error context and
    /// the shard lookup are both built from.
    #[test]
    fn the_target_of_a_write_is_its_own_relation() {
        let cases = [
            ("INSERT INTO Sales.Orders VALUES (1)", "sales.orders"),
            ("UPDATE orders SET v = 1", "orders"),
            ("DELETE FROM public.orders", "orders"),
            ("CREATE TABLE sales.orders (id INT)", "sales.orders"),
            ("ALTER TABLE orders ADD COLUMN v INT", "orders"),
            ("DROP TABLE orders", "orders"),
            ("TRUNCATE TABLE orders", "orders"),
            // A view's own name, since a view *is* the relation the statement is about.
            ("CREATE VIEW v AS SELECT 1 FROM orders", "v"),
            ("ALTER VIEW v AS SELECT 2 FROM orders", "v"),
            // An index's *table*, not the index: that is what the shard lookup needs.
            ("CREATE INDEX i ON orders (id)", "orders"),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                extract_table_name(&statement(sql)).as_deref(),
                Some(expected),
                "`{sql}`"
            );
        }
    }

    /// A MERGE reports the table being merged *into*, not the one being read from. The
    /// other way round routes the write at the source's shards.
    #[test]
    fn a_merge_reports_its_target_and_not_its_source() {
        let sql = "MERGE INTO target USING source ON target.id = source.id \
                   WHEN MATCHED THEN DELETE";
        assert_eq!(
            extract_table_name(&statement(sql)).as_deref(),
            Some("target")
        );
    }

    /// A SELECT has no single write target, and a write statement has no SELECT source:
    /// the two extractors are not interchangeable, which is what their doc comments say.
    #[test]
    fn the_two_extractors_answer_about_different_statements() {
        let select = statement("SELECT id FROM Sales.Orders");
        assert_eq!(extract_table_name(&select), None);
        assert_eq!(
            extract_select_table_name(&select).as_deref(),
            Some("sales.orders")
        );

        let insert = statement("INSERT INTO orders VALUES (1)");
        assert_eq!(extract_table_name(&insert).as_deref(), Some("orders"));
        assert_eq!(extract_select_table_name(&insert), None);
    }

    /// A set operation has no single FROM relation, and reporting its first branch's
    /// would name one side of a query that reads both.
    #[test]
    fn a_set_operation_has_no_single_source() {
        assert_eq!(
            extract_select_table_name(&statement("SELECT id FROM a UNION SELECT id FROM b")),
            None
        );
    }

    /// The cross-node contract: the physical name the write path rewrites a relation to
    /// must be the one [`shard_table_name`] builds from the same canonical key, byte for
    /// byte, because the core node's `CREATE TABLE` uses the second and its DML the
    /// first. Both already route through [`canonical_table_name`], and this is what keeps
    /// them doing so.
    ///
    /// Compared as *rendered SQL*, not by re-extracting the name: the rewrite emits one
    /// unquoted identifier, so reading it back through [`canonical_table_name`] would fold
    /// its case away and the assertion would pass on a name that had not matched.
    #[test]
    fn the_write_rewrite_agrees_with_the_shard_name() {
        for spelling in [
            "orders",
            "Orders",
            "public.orders",
            "sales.orders",
            "Sales.Orders",
            "\"Sales\".\"Orders\"",
            "vairedb.sales.orders",
        ] {
            let key = canonical(spelling).expect("a key");
            let mut stmt = statement(&format!("DELETE FROM {spelling}"));
            write_sql_cl::rewrite_to_shard_local(&mut stmt, "shard3");
            assert_eq!(
                stmt.to_string(),
                format!("DELETE FROM {}", shard_table_name(&key, 3)),
                "`{spelling}` rewrote to a name the storage node does not build"
            );
        }
    }

    /// The fold onto one flat identifier is **not injective**, which is not a defect to
    /// repair here: the storage node splices the physical name into SQL unquoted, so it
    /// may carry neither a dot nor quotes. The collision is instead refused at
    /// `CREATE TABLE` — see `schemas::physical_name_conflict` — and this pins the
    /// premise that refusal rests on.
    #[test]
    fn two_logical_names_can_fold_to_one_physical_name() {
        let qualified = canonical("sales.orders").expect("a key");
        let flat = canonical("\"sales_orders\"").expect("a key");
        assert_ne!(qualified, flat);
        assert_eq!(shard_table_name(&qualified, 0), shard_table_name(&flat, 0));
    }
}
