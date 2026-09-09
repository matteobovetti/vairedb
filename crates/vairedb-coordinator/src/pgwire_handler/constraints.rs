//! Table constraints: the ones a `CREATE TABLE` may declare, and the ones a table
//! that already exists may still be given.
//!
//! A constraint is enforced by each shard on its own rows, so only the kinds that
//! are globally correct that way are accepted at all:
//!
//! - **`CHECK`** is a per-row predicate. Every shard applying it to its own rows is
//!   the same thing as applying it to all of them, so a `CHECK` is always exact.
//! - **`UNIQUE` / `PRIMARY KEY`** are accepted only when the constrained columns
//!   include the shard key. Equal shard keys always hash to one shard, so one shard
//!   sees every row that could collide; off the shard key two equal values can land
//!   on different nodes, neither of which can see the other's row, and VaireDB
//!   would report a constraint it does not enforce. This is the same rule a
//!   `UNIQUE` index follows — see [`crate::pgwire_handler::indexes`].
//! - **`FOREIGN KEY`** is refused outright. A referencing row and the row it
//!   references are routed by their own tables' shard keys, so the referenced row
//!   generally lives on another shard, and no shard can check a reference it cannot
//!   see.
//!
//! **What can be added later is narrower than what can be declared**, because the
//! shards' engine implements almost none of `ALTER TABLE ... ADD CONSTRAINT`:
//! adding a `CHECK`, adding a `UNIQUE` constraint, and `DROP CONSTRAINT` all come
//! back as "No support for that ALTER TABLE option yet". So a `UNIQUE` constraint
//! is added as one unique index per shard instead — the only uniqueness the engine
//! can put on a table that already has rows, and the only one that can be dropped
//! again. [`ConstraintMeta::index_backed`] records which of the two shapes a
//! constraint has, because that is exactly what decides whether a later
//! `DROP CONSTRAINT` can be honored.
//!
//! **Not atomic across shards**, like `CREATE INDEX` and for the same reason: each
//! shard's index carries `IF NOT EXISTS`/`IF EXISTS`, and the catalog is updated
//! only once every node was reached, so a partial failure leaves the metadata
//! unchanged and a retry converges.

use std::collections::HashSet;
use std::ops::ControlFlow;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ColumnDef, ConstraintKind, ConstraintMeta, TableMeta};
use crate::pgwire_handler::ddl::{already_exists, fail_if_unreachable};
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router::{
    canonicalize_ident, qualified_name, relation_of, schema_of,
};
use crate::sqlparser::ast::{
    AlterTableOperation, CheckConstraint, ColumnOption, ColumnOptionDef, CreateTable, Expr, Ident,
    IndexColumn, KeyOrIndexDisplay, NullsDistinctOption, PrimaryKeyConstraint, TableConstraint,
    UniqueConstraint, visit_expressions, visit_expressions_mut,
};
use crate::util::shard_table_name;

/// One constraint as the client declared it, normalized: its columns resolved to
/// canonical table columns and its body rendered in the single spelling VaireDB
/// stores, so a column-level `UNIQUE` and the table-level `UNIQUE (col)` that means
/// the same thing produce the same record.
#[derive(Debug, PartialEq)]
struct Declared {
    /// The name the client gave the constraint, canonical; `None` when unnamed.
    name: Option<String>,
    kind: ConstraintKind,
    /// The table columns the constraint covers, canonical and in declaration
    /// order. For a `CHECK` these are the columns its expression mentions.
    columns: Vec<String>,
    /// The constraint body without the `CONSTRAINT <name>` prefix, e.g.
    /// `CHECK (amount > 0)`.
    definition: String,
}

/// The constraints a `CREATE TABLE` declares, column-level ones first, ready to
/// store in the table's [`TableMeta`].
///
/// Every kind that cannot be enforced correctly per shard is refused here, before
/// the table is claimed in the catalog or any shard DDL is sent — including a
/// `UNIQUE` or `PRIMARY KEY` that does not cover the shard key, which is what keeps
/// VaireDB from accepting a uniqueness promise it cannot keep.
pub(super) fn constraints_from_create(
    create: &CreateTable,
    table_name: &str,
    shard_key: &str,
    columns: &[ColumnDef],
) -> PgWireResult<Vec<ConstraintMeta>> {
    let mut declared: Vec<Declared> = Vec::new();

    for column in &create.columns {
        let column_name = canonicalize_ident(&column.name);
        for option in &column.options {
            if let Some(one) = declared_from_column_option(&column_name, option, columns)? {
                declared.push(one);
            }
        }
    }
    for constraint in &create.constraints {
        declared.push(declared_from_table_constraint(constraint, columns)?);
    }

    let mut recorded: Vec<ConstraintMeta> = Vec::with_capacity(declared.len());
    for one in declared {
        let meta = into_meta(one, table_name, shard_key, columns, &recorded, false)?;
        recorded.push(meta);
    }
    Ok(recorded)
}

/// The constraint metadata an `ALTER TABLE ... ADD CONSTRAINT` would record, or the
/// reason it cannot be honored.
///
/// Pure, so the rules that decide what a table with rows in it can still be given
/// are testable without a cluster. Only a named `UNIQUE` constraint over the shard
/// key survives: it becomes one unique index per shard, which validates the rows
/// already there, can be retried after a partial broadcast, and can be dropped
/// again. See the module documentation for why the other kinds cannot.
pub(super) fn plan_add_constraint(
    table_meta: &TableMeta,
    constraint: &TableConstraint,
    not_valid: bool,
) -> PgWireResult<ConstraintMeta> {
    // The kinds the shards' engine cannot add to an existing table are refused
    // before the shared validation, so the message names the real obstacle instead
    // of the shard key.
    match constraint {
        TableConstraint::Check(_) => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "ALTER TABLE ... ADD CONSTRAINT ... CHECK is not supported by VaireDB: the shards' engine cannot add a CHECK to a table that already exists. Declare the CHECK in CREATE TABLE, where it is enforced on every shard",
            ));
        }
        TableConstraint::PrimaryKey(_) => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "ALTER TABLE ... ADD PRIMARY KEY is not supported by VaireDB: the shards' engine accepts it only once per table and cannot drop it again, so a broadcast that reached some shards and not others could neither be retried nor undone. Declare the primary key in CREATE TABLE, or add a droppable equivalent with ALTER TABLE {} ADD CONSTRAINT <name> UNIQUE ({})",
                    table_meta.table_name, table_meta.shard_key
                ),
            ));
        }
        _ => {}
    }

    // `NOT VALID` asks for the existing rows to be left unchecked, and a unique
    // index checks them all as it is built — the constraint would be stricter than
    // the statement asked for.
    if not_valid {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "ALTER TABLE ... ADD CONSTRAINT ... NOT VALID is not supported by VaireDB: each shard enforces the constraint with an index that validates the rows already stored, so the rows cannot be exempted",
        ));
    }

    let declared = declared_from_table_constraint(constraint, &table_meta.columns)?;

    if declared.name.is_none() {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "ALTER TABLE ... ADD CONSTRAINT without a name is not supported by VaireDB: the constraint is enforced by one index per shard, named after it, and DROP CONSTRAINT resolves that name back. Name the constraint",
        ));
    }

    into_meta(
        declared,
        &table_meta.table_name,
        &table_meta.shard_key,
        &table_meta.columns,
        &table_meta.constraints,
        true,
    )
}

/// The refusal for an `ALTER TABLE` operation that drops a constraint by kind
/// rather than by name, or `None` for anything else.
///
/// These reach [`super::ddl::plan_alter`] as ordinary operations and would
/// otherwise fall through to the generic per-shard broadcast, where the shards'
/// engine rejects them one node at a time and the client is told the *cluster*
/// failed. Named here instead, with the reason and what to do.
pub(super) fn refused_alter_operation(op: &AlterTableOperation) -> Option<PgWireError> {
    match op {
        AlterTableOperation::DropPrimaryKey { .. } => Some(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "ALTER TABLE ... DROP PRIMARY KEY is not supported by VaireDB: a primary key is part of the table's definition on every shard, and the shards' engine cannot remove it. Recreate the table without it",
        )),
        AlterTableOperation::DropForeignKey { .. } => Some(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "ALTER TABLE ... DROP FOREIGN KEY is not supported by VaireDB, which has no foreign keys to drop: a reference cannot be enforced across shards, so FOREIGN KEY is refused when it is declared",
        )),
        AlterTableOperation::DropIndex { name } => Some(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "ALTER TABLE ... DROP INDEX is not supported by VaireDB; drop the index with DROP INDEX {name}"
            ),
        )),
        _ => None,
    }
}

impl VaireDbQueryHandler {
    /// Add a constraint to an existing table: validate it against the table's
    /// metadata, build one unique index per shard, then record the constraint in
    /// the table's `TableMeta`.
    ///
    /// `table_meta` is the metadata of the table being altered, already resolved by
    /// [`Self::handle_alter_table`]. Returns `FeatureNotSupported` for a constraint
    /// VaireDB cannot enforce (see [`plan_add_constraint`]), `TableAlreadyExists`
    /// when the constraint's name is taken — the per-shard indexes are named after
    /// it, so it shares the relation namespace — or a `NodeCommunicationError` if
    /// any node was unreachable, leaving the catalog unchanged.
    pub(super) async fn add_constraint(
        &self,
        mut table_meta: TableMeta,
        constraint: &TableConstraint,
        not_valid: bool,
    ) -> PgWireResult<Response> {
        let meta = plan_add_constraint(&table_meta, constraint, not_valid)?;
        let table_name = table_meta.table_name.clone();
        let ctx = ErrorContext::for_table(&table_name);

        // Each shard's index takes the constraint's name, so the name has to be
        // free in the relation namespace for the same reason an index's own name
        // does.
        if self.name_is_taken(&meta.name, &ctx)? {
            return Err(already_exists(&meta.name));
        }

        // The per-shard indexes fold the schema into their name, so a free logical
        // name can still collide physically. A no-op unless a name is qualified.
        self.reject_physical_name_conflict(&meta.name, &ctx)?;

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
                ADD_CONSTRAINT,
                &table_name,
                |shard| shard_local_unique_index_sql(&meta, &table_name, shard.hash_bucket),
            )
            .await;
        fail_if_unreachable(ADD_CONSTRAINT, failed)?;

        // Recorded only once every shard enforces it, so the catalog never claims a
        // constraint some shard is missing — and a retry is a fresh ADD CONSTRAINT
        // rather than a duplicate-name error.
        table_meta.constraints.push(meta);
        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        // No DataFusion refresh: a constraint changes no column, type or shard
        // layout, so nothing the planner reads has moved.
        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// Drop a constraint by name: drop the index that enforces it on every shard,
    /// then remove it from the table's `TableMeta`.
    ///
    /// Only a constraint VaireDB added to the table after the fact is droppable —
    /// one declared in `CREATE TABLE` is part of every shard's table definition, and
    /// the shards' engine cannot remove it. Returns `TableNotFound` when the table
    /// has no constraint of that name and `IF EXISTS` was not given,
    /// `FeatureNotSupported` for a declared constraint, or a
    /// `NodeCommunicationError` if any node was unreachable, leaving the catalog
    /// unchanged.
    pub(super) async fn drop_constraint(
        &self,
        mut table_meta: TableMeta,
        name: &str,
        if_exists: bool,
    ) -> PgWireResult<Response> {
        let table_name = table_meta.table_name.clone();

        // `DROP CONSTRAINT` names a constraint of the table, so it is matched
        // table-locally: an index-backed constraint is recorded qualified into the
        // table's schema, and the client writes the bare name it declared.
        let local = relation_of(name);
        let Some(existing) = table_meta
            .constraints
            .iter()
            .find(|c| relation_of(&c.name) == local)
        else {
            if if_exists {
                return Ok(Response::Execution(Tag::new("ALTER TABLE")));
            }
            return Err(make_vdb_error(
                VdbErrorCode::TableNotFound,
                format!("constraint \"{name}\" of relation \"{table_name}\" does not exist"),
            ));
        };

        if !existing.index_backed {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "constraint \"{name}\" of relation \"{table_name}\" cannot be dropped: it is part of the table's definition on every shard, and the shards' engine does not implement DROP CONSTRAINT. Recreate the table without it"
                ),
            ));
        }

        // The recorded name, not the one written: it is what the per-shard index
        // names were built from.
        let recorded = existing.name.clone();
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
                DROP_CONSTRAINT,
                &table_name,
                |shard| {
                    format!(
                        "DROP INDEX IF EXISTS {}",
                        shard_table_name(&recorded, shard.hash_bucket)
                    )
                },
            )
            .await;
        fail_if_unreachable(DROP_CONSTRAINT, failed)?;

        table_meta.constraints.retain(|c| c.name != recorded);
        self.catalog
            .put_table(&table_meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }
}

/// Operation labels, shared by the broadcast log lines and the partial-failure
/// error so the two can never disagree.
const ADD_CONSTRAINT: &str = "ALTER TABLE ... ADD CONSTRAINT";
const DROP_CONSTRAINT: &str = "ALTER TABLE ... DROP CONSTRAINT";

/// The shard-local statement that enforces an index-backed constraint on one
/// shard.
///
/// `IF NOT EXISTS` is what makes a partially-broadcast ADD CONSTRAINT safe to retry
/// — the shards that already have the index accept the statement again — and it
/// costs nothing, because the coordinator's catalog check, not the shards, reports
/// a duplicate name to the client. Building the index is also what validates the
/// rows already stored: a shard holding duplicates rejects the statement, and the
/// catalog is left unchanged.
fn shard_local_unique_index_sql(
    meta: &ConstraintMeta,
    table_name: &str,
    hash_bucket: u32,
) -> String {
    let columns = meta
        .columns
        .iter()
        .map(|c| quote_if_folded(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
        shard_table_name(&meta.name, hash_bucket),
        shard_table_name(table_name, hash_bucket),
        columns
    )
}

/// Render a canonical identifier for shard SQL: bare when it folds to itself,
/// double-quoted when it carries case the engine would fold away. A column created
/// as `"Amount"` is stored canonical with its case, and naming it unquoted here
/// would look for `amount` instead.
fn quote_if_folded(name: &str) -> String {
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        format!("\"{name}\"")
    } else {
        name.to_string()
    }
}

// --- Keeping constraints and columns consistent ---
//
// A column change and a constraint over that column can contradict each other, and
// the shards' engine has a definite answer for each combination. The coordinator
// gives the same answer, so the catalog never describes a table the shards do not
// have:
//
// | change              | UNIQUE / PRIMARY KEY | CHECK                    |
// | ------------------- | -------------------- | ------------------------ |
// | `DROP COLUMN`       | refused              | allowed, constraint gone |
// | `SET DATA TYPE`     | refused              | refused                  |
// | `RENAME COLUMN`     | allowed, follows     | allowed, follows         |
// | `DROP NOT NULL`     | refused for a PK     | allowed                  |
// | `ADD COLUMN`, `SET DEFAULT`, `SET NOT NULL` | allowed | allowed   |
//
// Only constraints the shards declared in their own `CREATE TABLE` are considered
// here. An index-backed one is a physical index as far as the shards are concerned,
// and they refuse to alter *any* column of a table an index depends on, so
// `table_meta_ops`'s index guard speaks for those — naming `DROP CONSTRAINT`.

/// The declared constraints of `table_meta` that cover `column`, whose kind is one
/// of `kinds`.
fn declared_covering<'a>(
    table_meta: &'a TableMeta,
    column: &str,
    kinds: &[ConstraintKind],
) -> Vec<&'a ConstraintMeta> {
    table_meta
        .constraints
        .iter()
        .filter(|c| !c.index_backed)
        .filter(|c| kinds.iter().any(|kind| c.kind == *kind as i32))
        .filter(|c| c.columns.iter().any(|covered| covered == column))
        .collect()
}

/// Refuse dropping a column a declared `UNIQUE` or `PRIMARY KEY` covers, which the
/// shards' engine refuses too ("Cannot drop column … because it is referenced in
/// unique constraint"). A `CHECK` over the column does not block the drop — see
/// [`forget_constraints_on_dropped_column`].
pub(super) fn reject_drop_of_constrained_column(
    table_meta: &TableMeta,
    column: &str,
) -> PgWireResult<()> {
    reject_if_constrained(
        table_meta,
        column,
        "drop",
        &[ConstraintKind::Unique, ConstraintKind::PrimaryKey],
    )
}

/// Refuse retyping a column any declared constraint covers, which the shards'
/// engine refuses too ("Cannot change the type of a column that has a UNIQUE or
/// PRIMARY KEY constraint specified" / "… a CHECK constraint specified").
pub(super) fn reject_retype_of_constrained_column(
    table_meta: &TableMeta,
    column: &str,
) -> PgWireResult<()> {
    reject_if_constrained(
        table_meta,
        column,
        "change the type of",
        &[
            ConstraintKind::Unique,
            ConstraintKind::PrimaryKey,
            ConstraintKind::Check,
        ],
    )
}

/// Refuse making a primary-key column nullable.
///
/// The shards' engine accepts the statement and then goes on rejecting NULLs, since
/// the primary key implies `NOT NULL` on its own — so honoring it would leave the
/// catalog advertising a nullable column every shard treats as mandatory, and the
/// write path would let a NULL through only to have every shard reject it.
pub(super) fn reject_nullable_primary_key_column(
    table_meta: &TableMeta,
    column: &str,
) -> PgWireResult<()> {
    reject_if_constrained(
        table_meta,
        column,
        "drop NOT NULL on",
        &[ConstraintKind::PrimaryKey],
    )
}

/// The shared refusal: `op` names the verb, `kinds` the constraint kinds that stand
/// in the way. Worded like the index guard's — "cannot {op} column … because …;
/// {hint}" — and pointing at the only way out, since a declared constraint cannot be
/// dropped.
fn reject_if_constrained(
    table_meta: &TableMeta,
    column: &str,
    op: &str,
    kinds: &[ConstraintKind],
) -> PgWireResult<()> {
    let Some(blocking) = declared_covering(table_meta, column, kinds)
        .first()
        .copied()
    else {
        return Ok(());
    };
    Err(make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "cannot {op} column \"{column}\" because constraint \"{}\" of relation \"{}\" \
             covers it: {}, declared in the table's definition on every shard, and the shards' \
             engine cannot remove it; recreate the table without the constraint",
            blocking.name, table_meta.table_name, blocking.definition
        ),
    ))
}

/// Forget the constraints a just-dropped column carried.
///
/// The shards' engine drops a `CHECK` over a dropped column silently, along with the
/// column, so the catalog has to do the same or it would go on reporting a constraint
/// no shard enforces. Only a `CHECK` reaches here: a `UNIQUE` or `PRIMARY KEY` over
/// the column was already refused, and an index-backed constraint blocked the drop
/// outright.
pub(super) fn forget_constraints_on_dropped_column(table_meta: &mut TableMeta, column: &str) {
    table_meta
        .constraints
        .retain(|c| c.index_backed || !c.columns.iter().any(|covered| covered == column));
}

/// Follow a `RENAME COLUMN` through the constraints that mention the column.
///
/// The shards' engine rewrites a constraint around the new name rather than dropping
/// it, so the catalog does the same: the covered-column list is re-keyed — which is
/// what keeps the guards above pointing at the right column — and the stored body
/// re-rendered, so what a client reads back names the column that exists.
pub(super) fn rename_column_in_constraints(table_meta: &mut TableMeta, old: &str, new: &str) {
    for constraint in &mut table_meta.constraints {
        if !constraint.columns.iter().any(|covered| covered == old) {
            continue;
        }
        for covered in &mut constraint.columns {
            if covered == old {
                *covered = new.to_string();
            }
        }
        let columns = rendered_column_list(&constraint.columns);
        constraint.definition = match ConstraintKind::try_from(constraint.kind) {
            Ok(ConstraintKind::Unique) => format!("UNIQUE ({columns})"),
            Ok(ConstraintKind::PrimaryKey) => format!("PRIMARY KEY ({columns})"),
            Ok(ConstraintKind::Check) => rename_column_in_check(&constraint.definition, old, new)
                .unwrap_or_else(|| constraint.definition.clone()),
            _ => constraint.definition.clone(),
        };
    }
}

/// Rewrite one column name inside a stored `CHECK (…)` body.
///
/// `None` when the body is not one this function can re-parse, in which case the
/// caller keeps what it had: the rendering is what a client reads back, while the
/// column list the guards use is re-keyed either way, so a stale body is cosmetic.
fn rename_column_in_check(definition: &str, old: &str, new: &str) -> Option<String> {
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    let inner = definition.strip_prefix("CHECK (")?.strip_suffix(')')?;
    let mut expr = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(inner)
        .ok()?
        .parse_expr()
        .ok()?;
    let _ = visit_expressions_mut(&mut expr, |inner| {
        match inner {
            Expr::Identifier(ident) if canonicalize_ident(ident) == old => {
                *ident = renamed_ident(new);
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last_mut()
                    && canonicalize_ident(last) == old
                {
                    *last = renamed_ident(new);
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    Some(format!("CHECK ({expr})"))
}

/// A canonical column name as an identifier that reads back as itself: quoted when
/// it carries case the engine would fold away.
fn renamed_ident(name: &str) -> Ident {
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        Ident::with_quote('"', name)
    } else {
        Ident::new(name)
    }
}

/// The constraint a column option declares, or `None` for an option that is not a
/// constraint (a default, a collation, `NOT NULL` — whose nullability already lives
/// in the column's own metadata).
fn declared_from_column_option(
    column: &str,
    option: &ColumnOptionDef,
    table_columns: &[ColumnDef],
) -> PgWireResult<Option<Declared>> {
    // `CONSTRAINT <name>` written before a column option names that one
    // constraint, so it belongs to the option rather than to the column.
    let name = option.name.as_ref().map(canonicalize_ident);
    let columns = vec![column.to_string()];
    let declared = match &option.option {
        ColumnOption::Unique(unique) => {
            reject_unique_decorations(unique)?;
            Declared {
                name,
                kind: ConstraintKind::Unique,
                definition: format!("UNIQUE ({})", quote_if_folded(column)),
                columns,
            }
        }
        ColumnOption::PrimaryKey(primary_key) => {
            reject_primary_key_decorations(primary_key)?;
            Declared {
                name,
                kind: ConstraintKind::PrimaryKey,
                definition: format!("PRIMARY KEY ({})", quote_if_folded(column)),
                columns,
            }
        }
        ColumnOption::Check(check) => {
            reject_check_decorations(check)?;
            Declared {
                name,
                kind: ConstraintKind::Check,
                columns: check_columns(&check.expr, table_columns),
                definition: format!("CHECK ({})", check.expr),
            }
        }
        ColumnOption::ForeignKey(foreign_key) => {
            return Err(foreign_key_refusal(&foreign_key.foreign_table.to_string()));
        }
        _ => return Ok(None),
    };
    Ok(Some(declared))
}

/// The constraint a table-level constraint clause declares. Every kind either
/// produces a record or is refused, so there is no "not a constraint" case here.
fn declared_from_table_constraint(
    constraint: &TableConstraint,
    table_columns: &[ColumnDef],
) -> PgWireResult<Declared> {
    let declared = match constraint {
        TableConstraint::Unique(unique) => {
            reject_unique_decorations(unique)?;
            let columns = index_column_names(&unique.columns)?;
            Declared {
                name: unique.name.as_ref().map(canonicalize_ident),
                kind: ConstraintKind::Unique,
                definition: format!("UNIQUE ({})", rendered_column_list(&columns)),
                columns,
            }
        }
        TableConstraint::PrimaryKey(primary_key) => {
            reject_primary_key_decorations(primary_key)?;
            let columns = index_column_names(&primary_key.columns)?;
            Declared {
                name: primary_key.name.as_ref().map(canonicalize_ident),
                kind: ConstraintKind::PrimaryKey,
                definition: format!("PRIMARY KEY ({})", rendered_column_list(&columns)),
                columns,
            }
        }
        TableConstraint::Check(check) => {
            reject_check_decorations(check)?;
            Declared {
                name: check.name.as_ref().map(canonicalize_ident),
                kind: ConstraintKind::Check,
                columns: check_columns(&check.expr, table_columns),
                definition: format!("CHECK ({})", check.expr),
            }
        }
        TableConstraint::ForeignKey(foreign_key) => {
            return Err(foreign_key_refusal(&foreign_key.foreign_table.to_string()));
        }
        // MySQL's `INDEX`/`KEY`/`FULLTEXT`/`SPATIAL` clauses inside a table
        // definition declare an index, not a constraint: they admit every row.
        TableConstraint::Index(_) | TableConstraint::FulltextOrSpatial(_) => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "declaring an index inside CREATE TABLE is not supported by VaireDB; create it afterwards with CREATE INDEX",
            ));
        }
        // PostgreSQL's `PRIMARY KEY|UNIQUE USING INDEX <name>` promotes an
        // existing unique index into a constraint. A constraint record is stored
        // from its column list, and this form names an index instead — so there is
        // nothing to record. Refused by name rather than stored column-less, which
        // would let the shard-key rule pass a constraint it never checked.
        TableConstraint::PrimaryKeyUsingIndex(_) | TableConstraint::UniqueUsingIndex(_) => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "PRIMARY KEY / UNIQUE USING INDEX is not supported by VaireDB; declare the constraint on its columns instead",
            ));
        }
    };
    Ok(declared)
}

/// Validate a declared constraint against the table it belongs to and turn it into
/// the record to store: the constraint's name resolved, its columns checked, and
/// the shard-key rule applied.
///
/// `existing` is what has been recorded for the table so far, used both to reject a
/// duplicate name and to keep a generated one unique. `index_backed` says whether
/// the constraint will be enforced by a per-shard index rather than by the shards'
/// table definition — see the module documentation.
fn into_meta(
    declared: Declared,
    table_name: &str,
    shard_key: &str,
    table_columns: &[ColumnDef],
    existing: &[ConstraintMeta],
    index_backed: bool,
) -> PgWireResult<ConstraintMeta> {
    for column in &declared.columns {
        if !table_columns.iter().any(|c| c.name == *column) {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!("column \"{column}\" does not exist in relation \"{table_name}\""),
            ));
        }
    }

    // The one rule that keeps uniqueness honest across shards; a CHECK needs no
    // such rule, being a predicate on one row at a time.
    if matches!(
        declared.kind,
        ConstraintKind::Unique | ConstraintKind::PrimaryKey
    ) && !declared.columns.iter().any(|c| c == shard_key)
    {
        let kind = if declared.kind == ConstraintKind::PrimaryKey {
            "PRIMARY KEY"
        } else {
            "UNIQUE"
        };
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "a {kind} constraint must include the shard key \"{shard_key}\" of relation \"{table_name}\": uniqueness is enforced by each shard on its own rows, and only equal shard keys are guaranteed to land on the same shard — off the shard key VaireDB would report a constraint it cannot enforce. Add \"{shard_key}\" to the constraint, or drop the constraint from the statement"
            ),
        ));
    }

    // A constraint name is unique within its table, as in PostgreSQL, so names are
    // compared without the schema qualifier an index-backed one carries.
    let name = match declared.name {
        Some(name) => {
            if existing
                .iter()
                .any(|c| relation_of(&c.name) == relation_of(&name))
            {
                return Err(make_vdb_error(
                    VdbErrorCode::TableAlreadyExists,
                    format!("constraint \"{name}\" of relation \"{table_name}\" already exists"),
                ));
            }
            name
        }
        None => generated_name(table_name, declared.kind, &declared.columns, existing),
    };

    // An index-backed constraint *is* a physical index on every shard, named after
    // it, so its name enters the relation namespace and is recorded in the table's
    // schema the way an index's own name is. A declared constraint creates no named
    // index and keeps the table-local name the client wrote.
    let name = if index_backed {
        qualified_name(schema_of(table_name), relation_of(&name))
    } else {
        name
    };

    Ok(ConstraintMeta {
        name,
        kind: declared.kind as i32,
        columns: declared.columns,
        definition: declared.definition,
        index_backed,
    })
}

/// A name for a constraint the client left unnamed, spelled the way PostgreSQL
/// spells it — `t_pkey`, `t_col_key`, `t_col_check` — so a client that knows
/// PostgreSQL can guess the name a later `DROP CONSTRAINT` needs. A collision with
/// a name already recorded for the table takes a numeric suffix, as PostgreSQL's
/// does.
///
/// Built from the table's *relation* name, without the schema qualifier: a
/// constraint of `sales.orders` is `orders_pkey`, exactly as PostgreSQL names it,
/// and the name stays a plain identifier.
fn generated_name(
    table_name: &str,
    kind: ConstraintKind,
    columns: &[String],
    existing: &[ConstraintMeta],
) -> String {
    let relation = relation_of(table_name);
    let base = match kind {
        ConstraintKind::PrimaryKey => format!("{relation}_pkey"),
        ConstraintKind::Unique => format!("{relation}_{}_key", columns.join("_")),
        // A CHECK over several columns is named after the table alone, as is one
        // whose expression mentions no column at all.
        _ => match columns {
            [column] => format!("{relation}_{column}_check"),
            _ => format!("{relation}_check"),
        },
    };
    if !existing.iter().any(|c| relation_of(&c.name) == base) {
        return base;
    }
    (1..)
        .map(|n| format!("{base}{n}"))
        .find(|candidate| !existing.iter().any(|c| relation_of(&c.name) == *candidate))
        .unwrap_or(base)
}

/// The canonical column names a `UNIQUE`/`PRIMARY KEY` clause lists.
///
/// An expression instead of a column is refused: the constraint would have to be
/// translated for the shards and stored well enough to compare against a later
/// column change, and neither PostgreSQL nor the shards' engine accepts one in a
/// table constraint anyway.
fn index_column_names(columns: &[IndexColumn]) -> PgWireResult<Vec<String>> {
    columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(ident) => Ok(canonicalize_ident(ident)),
            _ => Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "a UNIQUE or PRIMARY KEY constraint over an expression is not supported by VaireDB; constrain a column instead",
            )),
        })
        .collect()
}

/// The table columns a `CHECK` expression mentions, in first-mention order.
///
/// Identifiers that are not columns of the table are ignored rather than refused:
/// an expression can name things that only look like columns, and a CHECK that
/// really does name a missing column is rejected by every shard, which rolls the
/// `CREATE TABLE` back. What this list is for is the opposite question — which
/// column changes a recorded CHECK stands in the way of.
fn check_columns(expr: &Expr, table_columns: &[ColumnDef]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let _ = visit_expressions(expr, |inner| {
        let name = match inner {
            Expr::Identifier(ident) => Some(canonicalize_ident(ident)),
            Expr::CompoundIdentifier(parts) => parts.last().map(canonicalize_ident),
            _ => None,
        };
        if let Some(name) = name
            && table_columns.iter().any(|c| c.name == name)
            && seen.insert(name.clone())
        {
            found.push(name);
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// Render a stored column list for a constraint definition.
fn rendered_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| quote_if_folded(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Refuse the `UNIQUE` spellings VaireDB cannot deliver as written.
fn reject_unique_decorations(unique: &UniqueConstraint) -> PgWireResult<()> {
    if unique.nulls_distinct == NullsDistinctOption::NotDistinct {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "UNIQUE ... NULLS NOT DISTINCT is not supported by VaireDB: the shards' engine treats NULLs as distinct, so the extra rows it would reject cannot be rejected",
        ));
    }
    if unique.characteristics.is_some() {
        return Err(deferrable_refusal("UNIQUE"));
    }
    if unique.index_name.is_some()
        || unique.index_type.is_some()
        || !unique.index_options.is_empty()
        || unique.index_type_display != KeyOrIndexDisplay::None
    {
        return Err(index_decoration_refusal("UNIQUE"));
    }
    Ok(())
}

/// Refuse the `PRIMARY KEY` spellings VaireDB cannot deliver as written.
fn reject_primary_key_decorations(primary_key: &PrimaryKeyConstraint) -> PgWireResult<()> {
    if primary_key.characteristics.is_some() {
        return Err(deferrable_refusal("PRIMARY KEY"));
    }
    if primary_key.index_name.is_some()
        || primary_key.index_type.is_some()
        || !primary_key.index_options.is_empty()
    {
        return Err(index_decoration_refusal("PRIMARY KEY"));
    }
    Ok(())
}

/// Refuse a `CHECK` the shards would not enforce the way it was written.
fn reject_check_decorations(check: &CheckConstraint) -> PgWireResult<()> {
    if check.enforced == Some(false) {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "a NOT ENFORCED CHECK constraint is not supported by VaireDB: the shards' engine enforces every CHECK it is given, so the constraint would reject rows the statement said to admit",
        ));
    }
    Ok(())
}

/// The refusal for `DEFERRABLE`/`INITIALLY DEFERRED` on a constraint.
fn deferrable_refusal(kind: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "a DEFERRABLE {kind} constraint is not supported by VaireDB: each shard checks its own rows as they are written, and there is no cluster-wide moment at which a deferred check could run"
        ),
    )
}

/// The refusal for MySQL's index decorations on a constraint clause.
fn index_decoration_refusal(kind: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "index decorations on a {kind} constraint (an index name, USING, or an index option) are not supported by VaireDB; write it as a plain {kind} (<columns>) constraint"
        ),
    )
}

/// The refusal for a `FOREIGN KEY`, naming the table it referenced.
fn foreign_key_refusal(foreign_table: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "FOREIGN KEY is not supported by VaireDB: a referencing row and the row it references in \"{foreign_table}\" are routed by their own tables' shard keys, so the referenced row generally lives on another shard and no shard can check a reference it cannot see. Enforce the relationship in the application"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ShardStrategy;
    use crate::pgwire_handler::parser::parse_sql;
    use crate::sqlparser::ast::Statement;

    /// Parse a single statement, panicking on anything else.
    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    /// The `CreateTable` of a single `CREATE TABLE`, panicking on anything else.
    fn parse_create(sql: &str) -> CreateTable {
        match parse_one(sql) {
            Statement::CreateTable(create) => create,
            other => panic!("expected CREATE TABLE, got {other:?}"),
        }
    }

    /// The single operation of an `ALTER TABLE`, panicking on anything else.
    fn parse_alter_op(sql: &str) -> AlterTableOperation {
        match parse_one(sql) {
            Statement::AlterTable(mut alter) => {
                assert_eq!(alter.operations.len(), 1, "`{sql}` must have one operation");
                alter.operations.remove(0)
            }
            other => panic!("expected ALTER TABLE, got {other:?}"),
        }
    }

    /// The SQLSTATE and message a `PgWireError` reports to the client.
    fn user_error(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// A three-column table sharded on `customer_id`, so a constraint on the shard
    /// key and one off it are both expressible.
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

    /// The constraints a `CREATE TABLE` on a table sharded by `customer_id` records.
    fn declared(sql: &str) -> Vec<ConstraintMeta> {
        let create = parse_create(sql);
        let table = sample_table();
        constraints_from_create(&create, &table.table_name, &table.shard_key, &table.columns)
            .unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"))
    }

    /// The SQLSTATE and message a rejected `CREATE TABLE` reports.
    fn declared_rejection(sql: &str) -> (String, String) {
        let create = parse_create(sql);
        let table = sample_table();
        user_error(
            constraints_from_create(&create, &table.table_name, &table.shard_key, &table.columns)
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be rejected")),
        )
    }

    /// The metadata an `ALTER TABLE ... ADD CONSTRAINT` on [`sample_table`] records.
    fn added(sql: &str) -> ConstraintMeta {
        let AlterTableOperation::AddConstraint {
            constraint,
            not_valid,
        } = parse_alter_op(sql)
        else {
            panic!("`{sql}` must be an ADD CONSTRAINT");
        };
        plan_add_constraint(&sample_table(), &constraint, not_valid)
            .unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"))
    }

    /// The SQLSTATE and message a rejected `ADD CONSTRAINT` reports.
    fn added_rejection(sql: &str) -> (String, String) {
        let AlterTableOperation::AddConstraint {
            constraint,
            not_valid,
        } = parse_alter_op(sql)
        else {
            panic!("`{sql}` must be an ADD CONSTRAINT");
        };
        user_error(
            plan_add_constraint(&sample_table(), &constraint, not_valid)
                .err()
                .unwrap_or_else(|| panic!("`{sql}` must be rejected")),
        )
    }

    // --- CREATE TABLE ---

    // A column-level and a table-level constraint that mean the same thing must
    // produce the same record, so a later column change sees one shape.
    #[test]
    fn a_column_level_and_table_level_constraint_record_the_same_thing() {
        let column_level = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER UNIQUE, amount INTEGER)",
        );
        let table_level = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, UNIQUE (customer_id))",
        );
        assert_eq!(column_level, table_level);
        assert_eq!(
            column_level,
            vec![ConstraintMeta {
                name: "orders_customer_id_key".to_string(),
                kind: ConstraintKind::Unique as i32,
                columns: vec!["customer_id".to_string()],
                definition: "UNIQUE (customer_id)".to_string(),
                index_backed: false,
            }]
        );
    }

    #[test]
    fn a_check_records_the_columns_its_expression_mentions() {
        let recorded = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             CONSTRAINT amount_positive CHECK (amount > 0 AND amount < id))",
        );
        assert_eq!(
            recorded,
            vec![ConstraintMeta {
                name: "amount_positive".to_string(),
                kind: ConstraintKind::Check as i32,
                columns: vec!["amount".to_string(), "id".to_string()],
                definition: "CHECK (amount > 0 AND amount < id)".to_string(),
                index_backed: false,
            }]
        );
    }

    // A CHECK is a predicate on one row, so it needs no relationship to the shard
    // key at all — this is the kind that always works.
    #[test]
    fn a_check_off_the_shard_key_is_accepted() {
        assert_eq!(
            declared("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER CHECK (amount > 0))")
                .len(),
            1
        );
    }

    // Identifiers an expression mentions that are not columns are ignored, not
    // refused: `CURRENT_DATE` and friends would otherwise fail a valid CHECK.
    #[test]
    fn a_check_over_a_non_column_identifier_records_no_column() {
        let recorded = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             CHECK (amount > 0 OR ghost IS NULL))",
        );
        assert_eq!(recorded[0].columns, vec!["amount".to_string()]);
        assert_eq!(recorded[0].name, "orders_amount_check");
    }

    // The rule that closes the uniqueness-off-the-shard-key gap: it is refused when
    // it is declared, rather than accepted and quietly enforced per shard.
    #[test]
    fn unique_and_primary_key_off_the_shard_key_are_refused() {
        for sql in [
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
            "CREATE TABLE orders (id INTEGER UNIQUE, customer_id INTEGER, amount INTEGER)",
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, PRIMARY KEY (id))",
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, UNIQUE (id, amount))",
        ] {
            let (code, msg) = declared_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("customer_id"), "`{sql}`: {msg}");
        }
    }

    // A composite key counts as covering the shard key: equal values of the whole
    // tuple imply equal shard keys, so one shard sees every possible collision.
    #[test]
    fn a_composite_key_covering_the_shard_key_is_accepted() {
        let recorded = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             PRIMARY KEY (customer_id, id))",
        );
        assert_eq!(
            recorded,
            vec![ConstraintMeta {
                name: "orders_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey as i32,
                columns: vec!["customer_id".to_string(), "id".to_string()],
                definition: "PRIMARY KEY (customer_id, id)".to_string(),
                index_backed: false,
            }]
        );
    }

    #[test]
    fn a_foreign_key_is_refused_naming_the_referenced_table() {
        for sql in [
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER REFERENCES customers (id), amount INTEGER)",
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             FOREIGN KEY (customer_id) REFERENCES customers (id))",
        ] {
            let (code, msg) = declared_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains("customers"), "`{sql}`: {msg}");
            assert!(msg.contains("FOREIGN KEY"), "`{sql}`: {msg}");
        }
    }

    #[test]
    fn a_constraint_on_a_missing_column_reports_the_column() {
        let (code, msg) = declared_rejection(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             UNIQUE (customer_id, ghost))",
        );
        assert_eq!(code, "42703");
        assert!(msg.contains("\"ghost\""), "got: {msg}");
    }

    // Generated names have to be unique within the table, or a later DROP
    // CONSTRAINT could not say which one it meant.
    #[test]
    fn two_unnamed_checks_on_one_column_get_distinct_names() {
        let recorded = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, \
             amount INTEGER CHECK (amount > 0) CHECK (amount < 100))",
        );
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].name, "orders_amount_check");
        assert_eq!(recorded[1].name, "orders_amount_check1");
    }

    #[test]
    fn a_reused_explicit_name_is_refused() {
        let (code, msg) = declared_rejection(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             CONSTRAINT dup CHECK (amount > 0), CONSTRAINT dup CHECK (amount < 100))",
        );
        assert_eq!(code, "42P07");
        assert!(msg.contains("\"dup\""), "got: {msg}");
    }

    // Every recorded name is a catalog key, folded like any other identifier —
    // otherwise `DROP CONSTRAINT UQ_Orders` would not find what `CONSTRAINT
    // uq_orders` stored.
    #[test]
    fn names_and_columns_are_canonicalized() {
        let recorded = declared(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
             CONSTRAINT UQ_Orders UNIQUE (CUSTOMER_ID))",
        );
        assert_eq!(recorded[0].name, "uq_orders");
        assert_eq!(recorded[0].columns, vec!["customer_id".to_string()]);
    }

    #[test]
    fn a_table_with_no_constraints_records_none() {
        assert!(
            declared("CREATE TABLE orders (id INTEGER NOT NULL, customer_id INTEGER DEFAULT 1, amount INTEGER)")
                .is_empty()
        );
    }

    // Each of these changes which rows the constraint admits, or when it is
    // checked, in a way no shard can reproduce — so the statement is refused rather
    // than honored as something else.
    #[test]
    fn the_spellings_that_change_a_constraints_meaning_are_refused() {
        for (sql, needle) in [
            (
                "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
                 UNIQUE NULLS NOT DISTINCT (customer_id))",
                "NULLS NOT DISTINCT",
            ),
            (
                "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
                 UNIQUE (customer_id) DEFERRABLE)",
                "DEFERRABLE",
            ),
            (
                "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, \
                 CHECK (amount > 0) NOT ENFORCED)",
                "NOT ENFORCED",
            ),
        ] {
            let (code, msg) = declared_rejection(sql);
            assert_eq!(code, "0A000", "`{sql}` must be refused");
            assert!(msg.contains(needle), "`{sql}`: {msg}");
        }
    }

    // --- ALTER TABLE ... ADD CONSTRAINT ---

    #[test]
    fn a_named_unique_constraint_on_the_shard_key_is_added_index_backed() {
        assert_eq!(
            added("ALTER TABLE orders ADD CONSTRAINT uq_customer UNIQUE (customer_id)"),
            ConstraintMeta {
                name: "uq_customer".to_string(),
                kind: ConstraintKind::Unique as i32,
                columns: vec!["customer_id".to_string()],
                definition: "UNIQUE (customer_id)".to_string(),
                // The whole point: only an index-backed constraint can be dropped
                // again.
                index_backed: true,
            }
        );
    }

    // The shards' engine implements neither, so honoring the statement is
    // impossible; the message has to say what to do instead.
    #[test]
    fn adding_a_check_or_a_primary_key_is_refused_with_the_alternative() {
        let (code, msg) =
            added_rejection("ALTER TABLE orders ADD CONSTRAINT ck CHECK (amount > 0)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("CREATE TABLE"), "got: {msg}");

        let (code, msg) = added_rejection("ALTER TABLE orders ADD PRIMARY KEY (customer_id)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("ADD CONSTRAINT"), "got: {msg}");
        assert!(msg.contains("customer_id"), "got: {msg}");
    }

    // The per-shard index takes the constraint's name, so there has to be one.
    #[test]
    fn an_unnamed_added_constraint_is_refused() {
        let (code, msg) = added_rejection("ALTER TABLE orders ADD UNIQUE (customer_id)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("Name the constraint"), "got: {msg}");
    }

    #[test]
    fn an_added_unique_constraint_off_the_shard_key_is_refused() {
        let (code, msg) = added_rejection("ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (amount)");
        assert_eq!(code, "0A000");
        assert!(msg.contains("customer_id"), "got: {msg}");
    }

    #[test]
    fn an_added_foreign_key_is_refused() {
        let (code, _) = added_rejection(
            "ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES customers (id)",
        );
        assert_eq!(code, "0A000");
    }

    #[test]
    fn not_valid_is_refused_because_the_index_checks_every_row() {
        let (code, msg) =
            added_rejection("ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id) NOT VALID");
        assert_eq!(code, "0A000");
        assert!(msg.contains("NOT VALID"), "got: {msg}");
    }

    // --- the shard-local render ---

    #[test]
    fn the_shard_local_sql_suffixes_both_names_and_is_retryable() {
        let meta = added("ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id)");
        let sql = shard_local_unique_index_sql(&meta, "orders", 3);
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_shard3 ON orders_shard3 (customer_id)"
        );
    }

    // A column stored with its case was created quoted, so it has to be named
    // quoted or the engine folds it to something that does not exist.
    #[test]
    fn a_column_carrying_case_is_quoted_in_the_shard_sql() {
        let meta = ConstraintMeta {
            name: "uq".to_string(),
            kind: ConstraintKind::Unique as i32,
            columns: vec!["CustomerId".to_string()],
            definition: "UNIQUE (\"CustomerId\")".to_string(),
            index_backed: true,
        };
        assert_eq!(
            shard_local_unique_index_sql(&meta, "orders", 0),
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_shard0 ON orders_shard0 (\"CustomerId\")"
        );
    }

    // --- the operations that drop a constraint by kind ---

    #[test]
    fn dropping_a_constraint_by_kind_is_refused_with_a_reason() {
        for (sql, needle) in [
            ("ALTER TABLE orders DROP PRIMARY KEY", "Recreate the table"),
            (
                "ALTER TABLE orders DROP FOREIGN KEY fk",
                "no foreign keys to drop",
            ),
            ("ALTER TABLE orders DROP INDEX idx", "DROP INDEX idx"),
        ] {
            let err = refused_alter_operation(&parse_alter_op(sql))
                .unwrap_or_else(|| panic!("`{sql}` must be refused"));
            let (code, msg) = user_error(err);
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains(needle), "`{sql}`: {msg}");
        }
    }

    #[test]
    fn a_column_operation_is_not_a_refused_constraint_operation() {
        assert!(
            refused_alter_operation(&parse_alter_op("ALTER TABLE orders DROP COLUMN amount"))
                .is_none()
        );
    }

    // --- constraints under column changes ---

    /// [`sample_table`] with the constraints a `CREATE TABLE` would have declared.
    fn constrained_table(sql: &str) -> TableMeta {
        let mut table = sample_table();
        table.constraints = declared(sql);
        table
    }

    /// The table with one index-backed `UNIQUE` on the shard key, as
    /// `ALTER TABLE ... ADD CONSTRAINT` leaves it.
    fn table_with_added_constraint() -> TableMeta {
        let mut table = sample_table();
        table.constraints.push(added(
            "ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id)",
        ));
        table
    }

    // The shards' engine refuses to drop a column a UNIQUE or PRIMARY KEY covers, so
    // letting it through would fail the broadcast on every node after the metadata
    // already forgot the column.
    #[test]
    fn dropping_a_column_a_unique_constraint_covers_is_refused() {
        for sql in [
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, CONSTRAINT uq UNIQUE (customer_id))",
            "CREATE TABLE orders (id INT, customer_id INT PRIMARY KEY, amount INT)",
        ] {
            let table = constrained_table(sql);
            let (code, msg) = user_error(
                reject_drop_of_constrained_column(&table, "customer_id")
                    .err()
                    .unwrap_or_else(|| panic!("`{sql}`: the drop must be refused")),
            );
            assert_eq!(code, "0A000", "`{sql}`");
            assert!(msg.contains("recreate the table"), "`{sql}`: {msg}");
        }
    }

    // A CHECK does not block the drop: the shards discard it along with the column,
    // and the catalog has to discard it too, or it would report a constraint nothing
    // enforces.
    #[test]
    fn dropping_a_column_a_check_covers_forgets_the_check() {
        let mut table = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, \
             CONSTRAINT ck CHECK (amount > 0), CONSTRAINT ck_id CHECK (id > 0))",
        );
        reject_drop_of_constrained_column(&table, "amount").expect("a CHECK must not block a drop");
        forget_constraints_on_dropped_column(&mut table, "amount");
        assert_eq!(
            table
                .constraints
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["ck_id"]
        );
    }

    // A constraint enforced by a per-shard index is a real index on the shards, and
    // they refuse to alter a table an index depends on at all — so it is the index
    // guard, not this one, that has to catch it. Here it must stay silent, or the
    // client would be told to recreate the table when dropping the constraint is
    // enough.
    #[test]
    fn an_index_backed_constraint_is_left_to_the_index_guard() {
        let table = table_with_added_constraint();
        reject_drop_of_constrained_column(&table, "customer_id")
            .expect("an index-backed constraint is the index guard's business");
        reject_retype_of_constrained_column(&table, "customer_id")
            .expect("an index-backed constraint is the index guard's business");
    }

    // The shards refuse to retype a column under any constraint, CHECK included.
    #[test]
    fn retyping_a_constrained_column_is_refused_for_every_kind() {
        for (sql, column) in [
            (
                "CREATE TABLE orders (id INT, customer_id INT, amount INT, CONSTRAINT uq UNIQUE (customer_id))",
                "customer_id",
            ),
            (
                "CREATE TABLE orders (id INT, customer_id INT PRIMARY KEY, amount INT)",
                "customer_id",
            ),
            (
                "CREATE TABLE orders (id INT, customer_id INT, amount INT, CONSTRAINT ck CHECK (amount > 0))",
                "amount",
            ),
        ] {
            let table = constrained_table(sql);
            let (code, _) = user_error(
                reject_retype_of_constrained_column(&table, column)
                    .err()
                    .unwrap_or_else(|| panic!("`{sql}`: the retype must be refused")),
            );
            assert_eq!(code, "0A000", "`{sql}`");
        }
    }

    // Only the column the constraint covers is blocked; unlike an index, a declared
    // constraint is not a dependency on the whole table.
    #[test]
    fn an_unconstrained_column_of_a_constrained_table_is_free() {
        let table = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, \
             CONSTRAINT uq UNIQUE (customer_id))",
        );
        reject_drop_of_constrained_column(&table, "amount").expect("amount is unconstrained");
        reject_retype_of_constrained_column(&table, "amount").expect("amount is unconstrained");
    }

    // The shards accept `DROP NOT NULL` on a primary-key column and then go on
    // rejecting NULLs, so honoring it would leave the catalog advertising a
    // nullability no shard has.
    #[test]
    fn making_a_primary_key_column_nullable_is_refused() {
        let table = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT PRIMARY KEY, amount INT)",
        );
        let (code, msg) = user_error(
            reject_nullable_primary_key_column(&table, "customer_id")
                .expect_err("a primary-key column cannot be made nullable"),
        );
        assert_eq!(code, "0A000");
        assert!(msg.contains("PRIMARY KEY (customer_id)"), "{msg}");
        // A UNIQUE constraint carries no such implication.
        let unique = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, \
             CONSTRAINT uq UNIQUE (customer_id))",
        );
        reject_nullable_primary_key_column(&unique, "customer_id")
            .expect("UNIQUE does not imply NOT NULL");
    }

    // The recorded column list is what the guards above read, so a rename that did
    // not follow it would leave them blocking a column that no longer exists — and
    // waving through the one that does.
    #[test]
    fn a_renamed_column_is_followed_into_the_constraints() {
        let mut table = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, \
             CONSTRAINT uq UNIQUE (customer_id), \
             CONSTRAINT ck CHECK (amount > 0 AND amount < id))",
        );
        rename_column_in_constraints(&mut table, "amount", "total");

        let unique = &table.constraints[0];
        assert_eq!(unique.columns, vec!["customer_id"]);
        assert_eq!(unique.definition, "UNIQUE (customer_id)");

        let check = &table.constraints[1];
        assert_eq!(check.columns, vec!["total", "id"]);
        assert_eq!(check.definition, "CHECK (total > 0 AND total < id)");
    }

    // A folded-away name has to come back quoted, or the rewritten body would name a
    // different column than the one the constraint covers.
    #[test]
    fn a_renamed_column_that_needs_quoting_is_quoted() {
        let mut table = sample_table();
        table.columns.push(ColumnDef {
            name: "amount".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: true,
            default_expr: String::new(),
        });
        table.constraints = vec![ConstraintMeta {
            name: "ck".to_string(),
            kind: ConstraintKind::Check as i32,
            columns: vec!["amount".to_string()],
            definition: "CHECK (amount > 0)".to_string(),
            index_backed: false,
        }];
        rename_column_in_constraints(&mut table, "amount", "Total");
        assert_eq!(table.constraints[0].definition, "CHECK (\"Total\" > 0)");
    }

    // --- ADD / DROP CONSTRAINT against a catalog ---

    /// A handler whose catalog holds [`sample_table`], with `constraints` recorded.
    fn handler_with(constraints: Vec<ConstraintMeta>) -> VaireDbQueryHandler {
        let handler = VaireDbQueryHandler::for_tests(false);
        let mut table = sample_table();
        table.constraints = constraints;
        handler.catalog.put_table(&table).unwrap();
        handler
    }

    /// The constraints the catalog records for `orders`.
    fn recorded(handler: &VaireDbQueryHandler) -> Vec<ConstraintMeta> {
        handler
            .catalog
            .get_table("orders")
            .unwrap()
            .unwrap()
            .constraints
    }

    // The whole round trip: an added constraint is recorded index-backed, which is
    // what makes it droppable again.
    #[tokio::test]
    async fn an_added_constraint_is_recorded_index_backed_and_dropped_again() {
        let handler = handler_with(Vec::new());
        let table = handler.catalog.get_table("orders").unwrap().unwrap();
        let AlterTableOperation::AddConstraint {
            constraint,
            not_valid,
        } = parse_alter_op("ALTER TABLE orders ADD CONSTRAINT uq UNIQUE (customer_id)")
        else {
            panic!("expected an ADD CONSTRAINT");
        };
        handler
            .add_constraint(table, &constraint, not_valid)
            .await
            .expect("the constraint must be added");
        assert_eq!(recorded(&handler).len(), 1);
        assert!(recorded(&handler)[0].index_backed);

        let table = handler.catalog.get_table("orders").unwrap().unwrap();
        handler
            .drop_constraint(table, "uq", false)
            .await
            .expect("an index-backed constraint must be droppable");
        assert!(recorded(&handler).is_empty());
    }

    // A name already spoken for cluster-wide cannot be taken by a constraint whose
    // per-shard indexes would carry it.
    #[tokio::test]
    async fn an_added_constraint_may_not_take_a_relations_name() {
        let handler = handler_with(Vec::new());
        let table = handler.catalog.get_table("orders").unwrap().unwrap();
        let AlterTableOperation::AddConstraint {
            constraint,
            not_valid,
        } = parse_alter_op("ALTER TABLE orders ADD CONSTRAINT orders UNIQUE (customer_id)")
        else {
            panic!("expected an ADD CONSTRAINT");
        };
        let (code, msg) = user_error(
            handler
                .add_constraint(table, &constraint, not_valid)
                .await
                .err()
                .unwrap_or_else(|| panic!("a taken name must be refused")),
        );
        assert_eq!(code, "42P07");
        assert!(msg.contains("already exists"), "{msg}");
        assert!(recorded(&handler).is_empty(), "nothing may be recorded");
    }

    // A constraint the shards declared in their own CREATE TABLE cannot be dropped
    // at all, and saying so beats a broadcast that fails on every node.
    #[tokio::test]
    async fn dropping_a_declared_constraint_is_refused_with_the_way_out() {
        let handler = handler_with(vec![ConstraintMeta {
            name: "ck".to_string(),
            kind: ConstraintKind::Check as i32,
            columns: vec!["amount".to_string()],
            definition: "CHECK (amount > 0)".to_string(),
            index_backed: false,
        }]);
        let table = handler.catalog.get_table("orders").unwrap().unwrap();
        let (code, msg) = user_error(
            handler
                .drop_constraint(table, "ck", false)
                .await
                .err()
                .unwrap_or_else(|| panic!("a declared constraint must not be droppable")),
        );
        assert_eq!(code, "0A000");
        assert!(msg.contains("Recreate the table"), "{msg}");
        assert_eq!(recorded(&handler).len(), 1, "it must still be recorded");
    }

    #[tokio::test]
    async fn dropping_a_missing_constraint_reports_it_unless_if_exists() {
        let handler = handler_with(Vec::new());
        let table = handler.catalog.get_table("orders").unwrap().unwrap();
        let (code, msg) = user_error(
            handler
                .drop_constraint(table.clone(), "ghost", false)
                .await
                .err()
                .unwrap_or_else(|| panic!("a missing constraint must be reported")),
        );
        assert_eq!(code, "42P01");
        assert!(msg.contains("constraint \"ghost\""), "{msg}");

        handler
            .drop_constraint(table, "ghost", true)
            .await
            .expect("IF EXISTS must succeed against a missing constraint");
    }

    // A constraint the rename does not touch must be left exactly as it was.
    #[test]
    fn a_rename_leaves_unrelated_constraints_alone() {
        let mut table = constrained_table(
            "CREATE TABLE orders (id INT, customer_id INT, amount INT, \
             CONSTRAINT ck CHECK (amount > 0))",
        );
        let before = table.constraints.clone();
        rename_column_in_constraints(&mut table, "id", "ident");
        assert_eq!(table.constraints, before);
    }
}
