//! `CREATE INDEX` / `DROP INDEX`: secondary indexes, one physical index per
//! shard.
//!
//! An index is not a distributed object. Each shard is a self-contained DuckDB
//! table, so a logical index on `orders` is N real indexes — `idx_shard0` on
//! `orders_shard0`, `idx_shard1` on `orders_shard1`, and so on — and a lookup is
//! fast because every shard's index is consulted on its own node. That is why the
//! index name is suffixed per shard as well as the table's: without it, every
//! shard on one node would try to create the same name and all but the first
//! would collide.
//!
//! The catalog keeps each index inside its table's `TableMeta` (see
//! [`crate::catalog::IndexMeta`]), which is what lets `DROP INDEX` — a statement
//! that names only the index — find the table it belongs to.
//!
//! **An index lives in its table's schema.** `CREATE INDEX` takes a bare name, as
//! in PostgreSQL, and the catalog records it under that name qualified into the
//! table's namespace: an index on `sales.orders` is `sales.idx`, and `DROP INDEX`
//! needs the qualifier to find it. The physical per-shard name folds the qualifier
//! in like any other relation's (`sales_idx_shard0`), so the same collision guard
//! `CREATE TABLE` uses applies here — see
//! [`crate::pgwire_handler::schemas`].
//!
//! **`UNIQUE` is only accepted when the indexed columns include the shard key.**
//! Uniqueness is enforced per shard, and equal shard keys always hash to the same
//! shard, so an index covering the shard key gives a globally correct constraint
//! from N local ones. Off the shard key, two equal values can land on different
//! shards, where neither node can see the other's row — the index would look like
//! a global constraint and silently not be one. The narrowing decorations
//! (`WHERE`, `NULLS NOT DISTINCT`) are refused on a `UNIQUE` index for the same
//! reason: DuckDB cannot express them, and stripping them would change which rows
//! the constraint admits.
//!
//! Non-unique indexes carry no promise beyond speed, so PostgreSQL's decorations
//! on them are dropped rather than refused; see
//! [`crate::write_sql_cl::transform_to_duckdb`].
//!
//! **Not atomic across shards.** Like TRUNCATE, and for the same reason it is
//! acceptable: each shard's index is created (or dropped) by its own statement,
//! carrying `IF NOT EXISTS`/`IF EXISTS` so re-running the command converges.
//! The catalog is updated only once every node was reached, so a partial failure
//! leaves the metadata unchanged and a retry finishes the job.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{IndexMeta, TableMeta};
use crate::pgwire_handler::ddl::{already_exists, fail_if_unreachable};
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router::{
    canonical_table_name, canonicalize_ident, qualified_name, schema_of,
};
use crate::sqlparser::ast::{
    CreateIndex, Expr, Ident, ObjectName, ObjectNamePart, ObjectType, Statement,
};
use crate::util::shard_table_name;
use crate::write_sql_cl;

impl VaireDbQueryHandler {
    /// Create a secondary index: validate what the statement asks for against the
    /// table's metadata, broadcast one shard-local `CREATE INDEX` per shard, then
    /// record the index in the table's `TableMeta`.
    ///
    /// Returns `TableNotFound` if the table does not exist, `TableAlreadyExists`
    /// if the index name is taken (an index shares the relation namespace with
    /// tables, as in PostgreSQL), `FeatureNotSupported` for a form VaireDB cannot
    /// honor (see [`plan_create_index`]), or a `NodeCommunicationError` if any node
    /// was unreachable — in which case the catalog is left unchanged.
    pub(super) async fn handle_create_index(&self, stmt: &Statement) -> PgWireResult<Response> {
        let Statement::CreateIndex(create) = stmt else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "expected a CREATE INDEX statement",
            ));
        };

        let table_name = canonical_table_name(&create.table_name).ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine table name",
            )
        })?;
        let ctx = ErrorContext::for_table(&table_name);

        let mut table_meta = match self.catalog.get_table(&table_name) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                return Err(make_vdb_error(
                    VdbErrorCode::TableNotFound,
                    format!("relation \"{table_name}\" does not exist"),
                ));
            }
            Err(e) => return Err(enrich_coordinator_error(&e, &ctx, &self.catalog)),
        };

        // Everything decidable from the statement and the table's metadata, before
        // the cluster is touched: a form that cannot be honored must not leave
        // indexes behind on the shards it reached first.
        let index_meta = plan_create_index(create, &table_meta)?;

        // An index shares the relation namespace with tables, so both are checked.
        // `IF NOT EXISTS` then means what it means in PostgreSQL: the name is
        // taken, so there is nothing to do.
        if self.name_is_taken(&index_meta.name, &ctx)? {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE INDEX")));
            }
            return Err(already_exists(&index_meta.name));
        }

        // The per-shard indexes are named by folding the schema into the
        // identifier, so a free logical name can still collide physically with an
        // existing relation. A no-op unless a name is schema-qualified.
        self.reject_physical_name_conflict(&index_meta.name, &ctx)?;

        let shards = self
            .catalog
            .get_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        let failed = self
            .broadcast_ddl_best_effort(
                &shards,
                &node_addresses,
                "CREATE INDEX",
                &table_name,
                |shard| shard_local_create_index_sql(create, &index_meta.name, shard.hash_bucket),
            )
            .await;
        fail_if_unreachable("CREATE INDEX", failed)?;

        // Recorded only after every node has the index, so the catalog never
        // claims an index some shard is missing — and a retry of the failed
        // command is a fresh CREATE INDEX rather than a duplicate-name error.
        table_meta.indexes.push(index_meta);
        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        // No DataFusion refresh: an index changes no column, type or shard
        // layout, so nothing the planner reads has moved.
        Ok(Response::Execution(Tag::new("CREATE INDEX")))
    }

    /// Drop a secondary index: resolve the index name to the table that owns it,
    /// broadcast a shard-local `DROP INDEX IF EXISTS` to every shard, then remove
    /// the index from that table's `TableMeta`.
    ///
    /// Returns `WrongObjectType` when the name belongs to a table — `DROP INDEX`
    /// must never destroy one — `TableNotFound` when no index of that name exists
    /// without `IF EXISTS`, or a `NodeCommunicationError` if any node was
    /// unreachable, leaving the catalog unchanged.
    pub(super) async fn handle_drop_index(&self, stmt: &Statement) -> PgWireResult<Response> {
        let request = plan_drop_index(stmt)?;
        let index_name = &request.name;

        let owner = self
            .catalog
            .table_with_index(index_name)
            .map_err(|e| enrich_coordinator_error(&e, &ErrorContext::default(), &self.catalog))?;

        let Some(mut table_meta) = owner else {
            // The regression this ordering protects: the name resolves to a table,
            // and dropping it would be unrecoverable data loss for a one-word typo.
            let shadowing_table = self
                .catalog
                .get_table(index_name)
                .map_err(|e| enrich_coordinator_error(&e, &ErrorContext::default(), &self.catalog))?
                .is_some();
            if shadowing_table {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!("\"{index_name}\" is not an index; use DROP TABLE to drop a table"),
                ));
            }
            // A constraint enforced by per-shard indexes does own physical indexes
            // of that name, so "does not exist" would be a lie. It has to go through
            // the statement that also forgets the constraint, or the catalog would
            // keep promising uniqueness nothing enforces.
            let constraint_owner = self
                .catalog
                .table_with_index_backed_constraint(index_name)
                .map_err(|e| {
                    enrich_coordinator_error(&e, &ErrorContext::default(), &self.catalog)
                })?;
            if let Some(owner) = constraint_owner {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!(
                        "\"{index_name}\" is a constraint of relation \"{}\", not an index; \
                         drop it with ALTER TABLE {} DROP CONSTRAINT \"{index_name}\"",
                        owner.table_name, owner.table_name
                    ),
                ));
            }
            // A view shares this namespace too, so the name resolving to one is a
            // wrong-object-type error rather than a missing index.
            let shadowing_view = self
                .catalog
                .get_view(index_name)
                .map_err(|e| enrich_coordinator_error(&e, &ErrorContext::default(), &self.catalog))?
                .is_some();
            if shadowing_view {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!("\"{index_name}\" is not an index; use DROP VIEW to drop a view"),
                ));
            }
            if request.if_exists {
                return Ok(Response::Execution(Tag::new("DROP INDEX")));
            }
            return Err(make_vdb_error(
                VdbErrorCode::TableNotFound,
                format!("index \"{index_name}\" does not exist"),
            ));
        };

        let table_name = table_meta.table_name.clone();
        let ctx = ErrorContext::for_table(&table_name);

        let shards = self
            .catalog
            .get_shards_for_table(&table_name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        let node_addresses = self
            .catalog
            .get_node_address_map()
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        let failed = self
            .broadcast_ddl_best_effort(
                &shards,
                &node_addresses,
                "DROP INDEX",
                &table_name,
                |shard| {
                    format!(
                        "DROP INDEX IF EXISTS {}",
                        shard_index_name(index_name, shard.hash_bucket)
                    )
                },
            )
            .await;
        fail_if_unreachable("DROP INDEX", failed)?;

        table_meta.indexes.retain(|idx| &idx.name != index_name);
        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("DROP INDEX")))
    }

    /// Whether `name` already denotes a relation — a table, a view, an index of any
    /// table, or an index-backed constraint, whose per-shard indexes carry its name.
    /// PostgreSQL keeps all of them in one namespace, so an index may not take a
    /// table's name and two indexes may not share one.
    pub(super) fn name_is_taken(&self, name: &str, ctx: &ErrorContext) -> PgWireResult<bool> {
        let table = self
            .catalog
            .get_table(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some();
        if table {
            return Ok(true);
        }
        let view = self
            .catalog
            .get_view(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some();
        if view {
            return Ok(true);
        }
        let index = self
            .catalog
            .table_with_index(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some();
        if index {
            return Ok(true);
        }
        Ok(self
            .catalog
            .table_with_index_backed_constraint(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some())
    }
}

/// What a validated `DROP INDEX` asks for.
#[derive(Debug, PartialEq)]
struct DropIndexRequest {
    /// Canonical name of the index to drop.
    name: String,
    /// `IF EXISTS`: a name that resolves to nothing is success, not an error.
    if_exists: bool,
}

/// Validate the shape of a parsed `CREATE INDEX` against the table it targets and
/// return the metadata to record for it.
///
/// Pure, so the rules that decide whether a `UNIQUE` index is a real constraint
/// are testable without a cluster. What is refused, and why:
///
/// - **No name.** VaireDB derives each shard's index name from the one given, and
///   `DROP INDEX` resolves that name back to its table; a server-generated name
///   would have to be invented consistently on every shard and could not be
///   dropped by the name the client never saw.
/// - **An expression instead of a column.** The expression would have to be
///   translated to DuckDB and stored well enough to compare against a later
///   `DROP`; indexing a column is the supported form.
/// - **A `UNIQUE` index that does not cover the shard key.** It could not enforce
///   anything globally — see the module documentation.
/// - **A `UNIQUE` index with `WHERE` or `NULLS NOT DISTINCT`.** Both change which
///   rows the constraint admits, and DuckDB can express neither, so honoring the
///   statement as written is impossible and stripping the clause would enforce a
///   constraint the client did not ask for.
fn plan_create_index(create: &CreateIndex, table_meta: &TableMeta) -> PgWireResult<IndexMeta> {
    let Some(name) = &create.name else {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "CREATE INDEX without a name is not supported by VaireDB: each shard's index is named after the one you give, and DROP INDEX resolves that name back to the table it belongs to. Name the index",
        ));
    };

    // As in PostgreSQL, where `CREATE INDEX` takes a bare name: an index is
    // created in the schema of the table it indexes, so a qualifier could only
    // agree with that schema or contradict it.
    if name.0.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "an index name may not be schema-qualified: an index is created in the schema of the table it indexes. Give the index a bare name",
        ));
    }

    let relation = canonical_table_name(name).ok_or_else(|| {
        make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "could not determine the index name",
        )
    })?;
    // The index's catalog key: the bare name placed in the table's schema, so
    // two tables in different schemas can carry same-named indexes the way the
    // same DDL applied per schema would produce.
    let index_name = qualified_name(schema_of(&table_meta.table_name), &relation);

    let mut columns: Vec<String> = Vec::with_capacity(create.columns.len());
    for column in &create.columns {
        let Expr::Identifier(ident) = &column.column.expr else {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "an index over an expression is not supported by VaireDB; index a column instead",
            ));
        };
        let column_name = canonicalize_ident(ident);
        if !table_meta.columns.iter().any(|c| c.name == column_name) {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!(
                    "column \"{column_name}\" does not exist in relation \"{}\"",
                    table_meta.table_name
                ),
            ));
        }
        columns.push(column_name);
    }

    if create.unique {
        if !columns.contains(&table_meta.shard_key) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "a UNIQUE index must include the shard key \"{}\" of relation \"{}\": uniqueness is enforced by each shard on its own rows, and only equal shard keys are guaranteed to land on the same shard — off the shard key VaireDB would report a constraint it cannot enforce. Add \"{}\" to the index, or create the index without UNIQUE",
                    table_meta.shard_key, table_meta.table_name, table_meta.shard_key
                ),
            ));
        }

        if create.predicate.is_some() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "a partial UNIQUE index (CREATE UNIQUE INDEX ... WHERE ...) is not supported by VaireDB: the WHERE clause decides which rows the constraint covers, and the shards' engine cannot express it — so the index would enforce uniqueness over more rows than you asked for",
            ));
        }

        if create.nulls_distinct == Some(false) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "CREATE UNIQUE INDEX ... NULLS NOT DISTINCT is not supported by VaireDB: the shards' engine treats NULLs as distinct, so the extra rows it would reject cannot be rejected",
            ));
        }
    }

    Ok(IndexMeta {
        name: index_name,
        columns,
        unique: create.unique,
    })
}

/// Validate the shape of a parsed `DROP INDEX` and return the index it names.
///
/// Pure, so the rules that keep a `DROP INDEX` from reaching a table are testable
/// without a catalog. Only the first name is ever acted on, so a multi-object drop
/// is refused rather than reported as a success that touched one object;
/// `CASCADE`/`RESTRICT` are refused because the catalog models no dependents; and
/// MySQL's `DROP INDEX i ON t` is refused rather than resolved with the `ON t`
/// silently ignored, which could drop an index of another table.
fn plan_drop_index(stmt: &Statement) -> PgWireResult<DropIndexRequest> {
    let Statement::Drop {
        object_type: ObjectType::Index,
        if_exists,
        names,
        cascade,
        restrict,
        table,
        ..
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a DROP INDEX statement",
        ));
    };

    if names.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP INDEX with more than one index is not supported by VaireDB; \
             drop each index with its own statement",
        ));
    }

    if *cascade || *restrict {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP INDEX ... CASCADE/RESTRICT is not supported by VaireDB; \
             the catalog tracks no dependent objects",
        ));
    }

    if table.is_some() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP INDEX ... ON <table> is not supported by VaireDB: an index name \
             resolves on its own, so drop it with DROP INDEX <name>",
        ));
    }

    let name = names
        .first()
        .and_then(canonical_table_name)
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "could not determine the index name",
            )
        })?;

    Ok(DropIndexRequest {
        name,
        if_exists: *if_exists,
    })
}

/// The physical name of `index_name`'s copy on the shard in `hash_bucket`.
///
/// Suffixed exactly like a shard-local table, and through the same helper so the
/// two can never drift: [`write_sql_cl::rewrite_index_name_to_shard_local`]
/// produces this name when the index is created, and `DROP INDEX` has to
/// recompute it from the logical name alone.
fn shard_index_name(index_name: &str, hash_bucket: u32) -> String {
    shard_table_name(index_name, hash_bucket)
}

/// Render the shard-local `CREATE INDEX` for one shard: both names suffixed, the
/// PostgreSQL-only decorations stripped, and `IF NOT EXISTS` forced on.
///
/// `IF NOT EXISTS` is what makes a partially-broadcast CREATE INDEX safe to retry
/// — the shards that already have the index accept the statement again — and it
/// costs nothing, because the coordinator's catalog check, not the shards, is what
/// reports a duplicate index name to the client.
fn shard_local_create_index_sql(
    create: &CreateIndex,
    index_name: &str,
    hash_bucket: u32,
) -> String {
    let suffix = format!("shard{hash_bucket}");
    let mut idx = create.clone();
    idx.if_not_exists = true;
    // Name the index by its catalog key rather than by the bare name the client
    // wrote: the key carries the schema the index lives in, and it is the key
    // `DROP INDEX` recomputes the physical name from. Quoted so the rewrite reads
    // the key back as written instead of folding its case away.
    idx.name = Some(ObjectName(vec![ObjectNamePart::Identifier(
        Ident::with_quote('"', index_name),
    )]));
    let mut stmt = Statement::CreateIndex(idx);
    write_sql_cl::rewrite_to_shard_local(&mut stmt, &suffix);
    write_sql_cl::rewrite_index_name_to_shard_local(&mut stmt, &suffix);
    write_sql_cl::transform_to_duckdb(&mut stmt);
    write_sql_cl::statement_to_sql(&stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, ShardStrategy};
    use crate::pgwire_handler::parser::parse_sql;
    use pgwire::error::PgWireError;

    /// Parse a single statement, panicking on anything else.
    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    /// The `CreateIndex` of a single `CREATE INDEX`, panicking on anything else.
    fn parse_create_index(sql: &str) -> CreateIndex {
        match parse_one(sql) {
            Statement::CreateIndex(create) => create,
            other => panic!("expected CREATE INDEX, got {other:?}"),
        }
    }

    /// The SQLSTATE and message a `PgWireError` reports to the client.
    fn user_error(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// A three-column table sharded on `customer_id`, so a UNIQUE index on the
    /// shard key and one off it are both expressible.
    fn sample_table() -> TableMeta {
        TableMeta {
            table_name: "orders".to_string(),
            columns: ["id", "customer_id", "amount"]
                .into_iter()
                .map(|name| ColumnDef {
                    name: name.to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                })
                .collect(),
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: "customer_id".to_string(),
            shard_count: 2,
            replication_factor: 1,
            ..Default::default()
        }
    }

    /// The index metadata a `CREATE INDEX` on [`sample_table`] would record.
    fn plan(sql: &str) -> IndexMeta {
        plan_create_index(&parse_create_index(sql), &sample_table())
            .unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"))
    }

    /// The SQLSTATE and message a rejected `CREATE INDEX` reports.
    fn plan_rejection(sql: &str) -> (String, String) {
        user_error(
            plan_create_index(&parse_create_index(sql), &sample_table())
                .expect_err("`{sql}` must be rejected"),
        )
    }

    #[test]
    fn a_plain_index_records_its_columns_in_order() {
        assert_eq!(
            plan("CREATE INDEX idx_amount ON orders (customer_id, amount)"),
            IndexMeta {
                name: "idx_amount".to_string(),
                columns: vec!["customer_id".to_string(), "amount".to_string()],
                unique: false,
            }
        );
    }

    // Every recorded name is a catalog key, folded like any other identifier —
    // otherwise `DROP INDEX IDX_Amount` would not find what `CREATE INDEX
    // idx_amount` stored.
    #[test]
    fn names_and_columns_are_canonicalized() {
        assert_eq!(
            plan("CREATE INDEX IDX_Amount ON orders (AMOUNT)"),
            IndexMeta {
                name: "idx_amount".to_string(),
                columns: vec!["amount".to_string()],
                unique: false,
            }
        );
        assert_eq!(
            plan("CREATE INDEX \"IDX_Amount\" ON orders (amount)").name,
            "IDX_Amount"
        );
    }

    // Performance-only decorations are honored as far as they can be: the index is
    // built, the decoration dropped. Refusing them would fail statements whose
    // meaning VaireDB *can* deliver.
    #[test]
    fn a_non_unique_index_accepts_postgresql_decorations() {
        for sql in [
            "CREATE INDEX idx ON orders USING btree (amount)",
            "CREATE INDEX CONCURRENTLY idx ON orders (amount)",
            "CREATE INDEX idx ON orders (amount DESC NULLS LAST)",
            "CREATE INDEX idx ON orders (amount) WHERE amount > 0",
            "CREATE INDEX idx ON orders (amount) INCLUDE (id)",
        ] {
            let meta = plan_create_index(&parse_create_index(sql), &sample_table())
                .unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"));
            assert_eq!(meta.columns, vec!["amount".to_string()], "`{sql}`");
        }
    }

    // The one rule that keeps a UNIQUE index honest: per-shard enforcement is
    // global only for columns that decide the shard.
    #[test]
    fn a_unique_index_on_the_shard_key_is_accepted() {
        assert_eq!(
            plan("CREATE UNIQUE INDEX idx ON orders (customer_id)"),
            IndexMeta {
                name: "idx".to_string(),
                columns: vec!["customer_id".to_string()],
                unique: true,
            }
        );
        // A composite index counts as covering it, because equal values of the
        // whole tuple imply equal shard keys.
        assert!(plan("CREATE UNIQUE INDEX idx ON orders (id, customer_id)").unique);
    }

    #[test]
    fn a_unique_index_off_the_shard_key_is_refused_naming_the_shard_key() {
        let (code, msg) = plan_rejection("CREATE UNIQUE INDEX idx ON orders (id)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("customer_id"), "got: {msg}");
        assert!(msg.contains("UNIQUE"), "got: {msg}");
    }

    // Both decorations narrow which rows a UNIQUE index admits, and DuckDB can
    // express neither — stripping them would enforce a different constraint than
    // the client asked for, in opposite directions.
    #[test]
    fn a_unique_index_that_narrows_its_scope_is_refused() {
        for sql in [
            "CREATE UNIQUE INDEX idx ON orders (customer_id) WHERE amount > 0",
            "CREATE UNIQUE INDEX idx ON orders (customer_id) NULLS NOT DISTINCT",
        ] {
            let (code, msg) = plan_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("UNIQUE"), "`{sql}`: {msg}");
        }
    }

    // NULLS DISTINCT is PostgreSQL's default and asks for nothing DuckDB does not
    // already do, so spelling it out must not turn a valid statement into an error.
    #[test]
    fn a_unique_index_with_nulls_distinct_spelled_out_is_accepted() {
        assert!(plan("CREATE UNIQUE INDEX idx ON orders (customer_id) NULLS DISTINCT").unique);
    }

    #[test]
    fn an_unnamed_index_is_refused() {
        let (code, msg) = plan_rejection("CREATE INDEX ON orders (amount)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("Name the index"), "got: {msg}");
    }

    #[test]
    fn an_expression_index_is_refused() {
        let (code, msg) = plan_rejection("CREATE INDEX idx ON orders (amount + 1)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("expression"), "got: {msg}");
    }

    // A column that does not exist would fail on every shard, turning a plain
    // 42703 into "the broadcast partially failed".
    #[test]
    fn an_index_on_a_missing_column_reports_the_column() {
        let (code, msg) = plan_rejection("CREATE INDEX idx ON orders (ghost)");
        assert_eq!(code, "42703");
        assert!(msg.contains("\"ghost\""), "got: {msg}");
    }

    #[test]
    fn a_schema_qualified_index_name_is_a_syntax_error() {
        let (code, msg) = plan_rejection("CREATE INDEX myschema.idx ON orders (amount)");
        assert_eq!(code, "42601");
        assert!(msg.contains("schema-qualified"), "got: {msg}");
    }

    // --- the shard-local render ---

    // Both names must move: the table's, so the index is built on the right
    // relation, and the index's own, or the second shard on a node collides with
    // the first.
    #[test]
    fn the_shard_local_sql_suffixes_both_names() {
        let sql = shard_local_create_index_sql(
            &parse_create_index("CREATE UNIQUE INDEX idx ON orders (customer_id)"),
            "idx",
            3,
        );
        assert!(sql.contains("idx_shard3"), "got: {sql}");
        assert!(sql.contains("orders_shard3"), "got: {sql}");
        assert!(sql.contains("UNIQUE"), "got: {sql}");
        // Forced on, so a retry after a partial broadcast converges.
        assert!(sql.contains("IF NOT EXISTS"), "got: {sql}");
    }

    // DuckDB has one index type and none of these decorations; they describe how
    // the index is built, not which rows it admits, so the statement must reach the
    // node without them rather than being refused.
    #[test]
    fn the_shard_local_sql_strips_postgresql_only_decorations() {
        let sql = shard_local_create_index_sql(
            &parse_create_index(
                "CREATE INDEX CONCURRENTLY idx ON orders USING btree (amount DESC NULLS LAST) INCLUDE (id) WHERE amount > 0",
            ),
            "idx",
            0,
        );
        for absent in ["USING", "CONCURRENTLY", "INCLUDE", "WHERE", "DESC", "NULLS"] {
            assert!(!sql.contains(absent), "`{absent}` must be stripped: {sql}");
        }
        assert!(sql.contains("idx_shard0"), "got: {sql}");
        assert!(sql.contains("(amount)"), "got: {sql}");
    }

    // The suffix must be recomputable from the logical name, because DROP INDEX
    // only ever has the logical name to work from.
    #[test]
    fn the_drop_targets_the_same_name_the_create_built() {
        let created = shard_local_create_index_sql(
            &parse_create_index("CREATE INDEX idx ON orders (amount)"),
            "idx",
            7,
        );
        let shard_name = shard_index_name("idx", 7);
        assert!(created.contains(&shard_name), "got: {created}");
    }

    // An index in a schema: the catalog key carries the qualifier, and both the
    // physical index name and the table it is built on fold it into the
    // identifier — so what CREATE builds is what DROP recomputes.
    #[test]
    fn the_shard_local_sql_folds_a_schema_into_both_names() {
        let created = shard_local_create_index_sql(
            &parse_create_index("CREATE INDEX idx ON sales.orders (amount)"),
            "sales.idx",
            2,
        );
        assert!(created.contains("sales_idx_shard2"), "got: {created}");
        assert!(created.contains("sales_orders_shard2"), "got: {created}");
        assert!(!created.contains('.'), "no name may carry a dot: {created}");
        assert_eq!(shard_index_name("sales.idx", 2), "sales_idx_shard2");
    }

    // The index is recorded in its table's schema, so the same DDL applied per
    // schema does not collide on one index name.
    #[test]
    fn an_index_takes_the_schema_of_its_table() {
        let mut meta = sample_table();
        meta.table_name = "sales.orders".to_string();
        let index = plan_create_index(
            &parse_create_index("CREATE INDEX idx_amount ON sales.orders (amount)"),
            &meta,
        )
        .unwrap();
        assert_eq!(index.name, "sales.idx_amount");
    }

    // --- DROP INDEX shapes ---

    /// The SQLSTATE and message a rejected `DROP INDEX` reports.
    fn drop_rejection(sql: &str) -> (String, String) {
        user_error(plan_drop_index(&parse_one(sql)).expect_err("`{sql}` must be rejected"))
    }

    #[test]
    fn drop_index_takes_the_canonical_name_and_if_exists() {
        assert_eq!(
            plan_drop_index(&parse_one("DROP INDEX IF EXISTS IDX_Amount")).unwrap(),
            DropIndexRequest {
                name: "idx_amount".to_string(),
                if_exists: true,
            }
        );
        assert_eq!(
            plan_drop_index(&parse_one("DROP INDEX idx")).unwrap(),
            DropIndexRequest {
                name: "idx".to_string(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn drop_index_with_several_names_is_refused() {
        let (code, msg) = drop_rejection("DROP INDEX a, b");
        assert_eq!(code, "0A000");
        assert!(msg.contains("more than one index"), "got: {msg}");
    }

    #[test]
    fn drop_index_cascade_or_restrict_is_refused() {
        for sql in ["DROP INDEX idx CASCADE", "DROP INDEX idx RESTRICT"] {
            let (code, msg) = drop_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("CASCADE/RESTRICT"), "`{sql}`: {msg}");
        }
    }

    #[test]
    fn a_non_drop_statement_is_a_syntax_error() {
        let (code, _) = drop_rejection("DROP TABLE t");
        assert_eq!(code, "42601");
    }

    // --- the handler, against a catalog with no reachable nodes ---
    //
    // A table registered without shard rows broadcasts to nothing, so these tests
    // exercise the whole of the catalog bookkeeping — the name checks, the
    // recording, the removal — without a cluster.

    /// Register [`sample_table`] under `name` so the catalog holds a real table.
    fn register(handler: &VaireDbQueryHandler, name: &str) {
        let mut meta = sample_table();
        meta.table_name = name.to_string();
        handler.catalog.put_table(&meta).unwrap();
    }

    /// The indexes the catalog records for `table`.
    fn recorded(handler: &VaireDbQueryHandler, table: &str) -> Vec<IndexMeta> {
        handler.catalog.get_table(table).unwrap().unwrap().indexes
    }

    /// Run a statement through the index handlers, choosing by statement kind.
    async fn run(handler: &VaireDbQueryHandler, sql: &str) -> PgWireResult<Response> {
        let stmt = parse_one(sql);
        match stmt {
            Statement::CreateIndex(_) => handler.handle_create_index(&stmt).await,
            _ => handler.handle_drop_index(&stmt).await,
        }
    }

    /// The SQLSTATE and message a rejected statement reports.
    async fn rejection(handler: &VaireDbQueryHandler, sql: &str) -> (String, String) {
        user_error(
            run(handler, sql)
                .await
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be rejected")),
        )
    }

    #[tokio::test]
    async fn an_index_is_recorded_on_its_table_and_removed_again() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders");

        run(&handler, "CREATE INDEX idx_amount ON orders (amount)")
            .await
            .expect("the index must be created");
        assert_eq!(
            recorded(&handler, "orders"),
            vec![IndexMeta {
                name: "idx_amount".to_string(),
                columns: vec!["amount".to_string()],
                unique: false,
            }]
        );

        // DROP INDEX names only the index: the table it belongs to is resolved
        // through the catalog, which is the whole reason indexes live in TableMeta.
        run(&handler, "DROP INDEX idx_amount")
            .await
            .expect("the index must be dropped");
        assert!(recorded(&handler, "orders").is_empty());
    }

    #[tokio::test]
    async fn creating_an_index_on_a_missing_table_reports_the_table() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = rejection(&handler, "CREATE INDEX idx ON ghost (amount)").await;
        assert_eq!(code, "42P01");
        assert!(msg.contains("\"ghost\""), "got: {msg}");
    }

    // An index shares the relation namespace with tables, as in PostgreSQL, so
    // both kinds of collision are the same error.
    #[tokio::test]
    async fn a_taken_name_is_refused_and_if_not_exists_is_a_no_op() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders");
        register(&handler, "customers");
        run(&handler, "CREATE INDEX idx ON orders (amount)")
            .await
            .unwrap();

        // Taken by an index of the same table...
        let (code, _) = rejection(&handler, "CREATE INDEX idx ON orders (id)").await;
        assert_eq!(code, "42P07");
        // ...of another table...
        let (code, _) = rejection(&handler, "CREATE INDEX idx ON customers (id)").await;
        assert_eq!(code, "42P07");
        // ...and by a table.
        let (code, msg) = rejection(&handler, "CREATE INDEX orders ON customers (id)").await;
        assert_eq!(code, "42P07");
        assert!(msg.contains("already exists"), "got: {msg}");

        run(&handler, "CREATE INDEX IF NOT EXISTS idx ON orders (id)")
            .await
            .expect("IF NOT EXISTS must succeed against a taken name");
        // Still the original index: the no-op recorded nothing.
        assert_eq!(recorded(&handler, "orders").len(), 1);
        assert_eq!(recorded(&handler, "orders")[0].columns, vec!["amount"]);
    }

    // The counterpart of `DROP VIEW` naming a table: a `DROP INDEX` that resolves
    // to a table must refuse, never drop it.
    #[tokio::test]
    async fn dropping_an_index_that_names_a_table_is_a_wrong_object_type_error() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register(&handler, "orders");

        let (code, msg) = rejection(&handler, "DROP INDEX orders").await;
        assert_eq!(code, "42809");
        assert!(msg.contains("not an index"), "got: {msg}");
        assert!(msg.contains("DROP TABLE"), "got: {msg}");
        assert!(
            handler.catalog.get_table("orders").unwrap().is_some(),
            "the table must survive"
        );
    }

    #[tokio::test]
    async fn dropping_a_missing_index_reports_it_unless_if_exists() {
        let handler = VaireDbQueryHandler::for_tests(false);
        let (code, msg) = rejection(&handler, "DROP INDEX ghost").await;
        assert_eq!(code, "42P01");
        assert!(msg.contains("index \"ghost\" does not exist"), "got: {msg}");

        run(&handler, "DROP INDEX IF EXISTS ghost")
            .await
            .expect("IF EXISTS must succeed against a missing index");
    }

    /// Register `orders` carrying an index-backed constraint, as
    /// `ALTER TABLE ... ADD CONSTRAINT` leaves it.
    fn register_with_constraint(handler: &VaireDbQueryHandler, name: &str) {
        let mut meta = sample_table();
        meta.table_name = name.to_string();
        meta.constraints.push(crate::catalog::ConstraintMeta {
            name: "uq_customer".to_string(),
            kind: crate::catalog::ConstraintKind::Unique as i32,
            columns: vec!["customer_id".to_string()],
            definition: "UNIQUE (customer_id)".to_string(),
            index_backed: true,
        });
        handler.catalog.put_table(&meta).unwrap();
    }

    // The per-shard indexes really are called `uq_customer_shard{n}`, so "does not
    // exist" would be a lie — and dropping them behind the constraint's back would
    // leave the catalog promising uniqueness nothing enforces.
    #[tokio::test]
    async fn dropping_an_index_backed_constraint_as_an_index_names_the_right_statement() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_with_constraint(&handler, "orders");

        let (code, msg) = rejection(&handler, "DROP INDEX uq_customer").await;
        assert_eq!(code, "42809");
        assert!(
            msg.contains("is a constraint of relation \"orders\""),
            "got: {msg}"
        );
        assert!(msg.contains("DROP CONSTRAINT"), "got: {msg}");
    }

    // An index-backed constraint owns real indexes of that name across the cluster,
    // so nothing else may claim it.
    #[tokio::test]
    async fn an_index_may_not_take_an_index_backed_constraints_name() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_with_constraint(&handler, "orders");

        let (code, msg) =
            rejection(&handler, "CREATE INDEX uq_customer ON orders (customer_id)").await;
        assert_eq!(code, "42P07");
        assert!(msg.contains("already exists"), "got: {msg}");
    }
}
