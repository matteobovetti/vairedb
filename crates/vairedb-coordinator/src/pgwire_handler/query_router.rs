//! SQL statement inspection used to decide how to route a query: classifying a
//! parsed statement and extracting the target table name from it.

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
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => canonical_table_name(name),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) => t,
                FromTable::WithoutKeyword(t) => t,
            };
            match &tables.first()?.relation {
                TableFactor::Table { name, .. } => canonical_table_name(name),
                _ => None,
            }
        }
        // The table being merged into, not the one being read from: the error
        // context and the shard lookup are both about the target.
        Statement::Merge(merge) => match &merge.table {
            TableFactor::Table { name, .. } => canonical_table_name(name),
            _ => None,
        },
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
    let table_with_joins = select.from.first()?;
    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => canonical_table_name(name),
        _ => None,
    }
}
