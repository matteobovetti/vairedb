//! `CREATE SCHEMA` / `ALTER SCHEMA` / `DROP SCHEMA`, `ALTER TABLE … SET SCHEMA`, and
//! the namespace rules every relation name goes through.
//!
//! A schema is a namespace and nothing else: it holds no rows, so — like a view —
//! nothing about it is broadcast to a core node. What makes it real is that a
//! relation's catalog key carries it: `canonical_table_name` maps `sales.orders` to
//! the key `sales.orders` and `orders` to the key `orders`, so two tables that
//! differ only by schema are two tables, with two shard layouts and two sets of
//! physical per-shard tables.
//!
//! **`public` is not a record.** It is the default namespace: it always exists, it
//! cannot be created and it cannot be dropped, and a key in it carries no
//! qualifier — `public.orders` and `orders` are the same relation. That is what
//! keeps every pre-existing catalog key, physical shard name and error message
//! byte-identical to what they were before schemas existed.
//!
//! **The physical name folds the qualifier in.** A core node has one flat DuckDB
//! namespace and splices relation names into SQL unquoted, so the shards of
//! `sales.orders` are `sales_orders_shard0..n` (see
//! [`physical_base_name`](crate::util::physical_base_name)). Folding `.` to `_` is
//! not injective, so `CREATE TABLE` refuses a name whose physical form another
//! relation already owns — see [`VaireDbQueryHandler::reject_physical_name_conflict`].
//! Refusing is the only honest option: accepting would put two logical tables on
//! one set of physical tables, which is exactly the collision schemas are here to
//! remove.
//!
//! **Moving is renaming.** Because the qualifier is part of the physical name,
//! `ALTER TABLE sales.orders SET SCHEMA archive` has to rename every
//! `sales_orders_shard<n>` to `archive_orders_shard<n>` — the same work
//! `ALTER TABLE … RENAME TO` does. sqlparser 0.62 has no `SET SCHEMA` operation for
//! a table, so [`parse_alter_table_set_schema`] recognizes the statement on the text
//! and respells it as the schema-qualified `RENAME TO` the DDL path already
//! executes; `ALTER SCHEMA … RENAME TO` it *does* parse, and that one is handled
//! here ([`VaireDbQueryHandler::handle_alter_schema`]) for an **empty** schema only,
//! because renaming a schema that holds relations is one fan-out per relation with
//! no way to undo the ones that succeeded.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::SchemaMeta;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::parser;
use crate::pgwire_handler::query_router::{
    self, DEFAULT_SCHEMA, canonicalize_ident, relation_of, schema_of,
};
use crate::sqlparser::ast::helpers::attached_token::AttachedToken;
use crate::sqlparser::ast::{
    AlterSchemaOperation, AlterTableOperation, ObjectName, ObjectType, RenameTableNameKind,
    SchemaName, Statement,
};
use crate::sqlparser::dialect::PostgreSqlDialect;
use crate::sqlparser::keywords::Keyword;
use crate::sqlparser::tokenizer::{Token, Tokenizer};
use crate::util::{now_unix_secs, physical_base_name};

/// Namespaces that exist without being created and hold metadata rather than user
/// relations. A `CREATE TABLE` or `CREATE VIEW` aimed at one is refused: the
/// coordinator serves these from its own DataFusion context (see
/// [`crate::pgwire_handler::catalog_routing`]), so a relation stored under such a
/// name would never be the one a client's introspection resolves.
pub(super) const METADATA_SCHEMAS: [&str; 3] =
    ["pg_catalog", "information_schema", "vairedb_catalog"];

impl VaireDbQueryHandler {
    /// Create a schema: record the name so relations can be created in it. Nothing
    /// is sent to a core node.
    ///
    /// Returns `SchemaAlreadyExists` when the name is taken (including `public` and
    /// the metadata schemas, which exist without being recorded) and
    /// `IF NOT EXISTS` was not given, or `FeatureNotSupported` for a clause VaireDB
    /// cannot honor — see [`plan_create_schema`].
    pub(super) async fn handle_create_schema(&self, stmt: &Statement) -> PgWireResult<Response> {
        let request = plan_create_schema(stmt)?;
        let ctx = ErrorContext::default();

        if reserved_schema(&request.name) {
            if request.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE SCHEMA")));
            }
            return Err(schema_already_exists(&request.name));
        }

        let meta = SchemaMeta {
            schema_name: request.name.clone(),
            created_at: Some(prost_types::Timestamp {
                seconds: now_unix_secs() as i64,
                nanos: 0,
            }),
        };
        let claimed = self
            .catalog
            .create_schema_if_absent(&meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
        if !claimed && !request.if_not_exists {
            return Err(schema_already_exists(&request.name));
        }

        Ok(Response::Execution(Tag::new("CREATE SCHEMA")))
    }

    /// Rename a schema. Only an empty one, and only the name: nothing is sent to a
    /// core node, because nothing about an empty namespace is on one.
    ///
    /// A **non-empty** schema is refused rather than renamed. A relation's catalog key
    /// carries its schema and so does every per-shard physical table name, so renaming
    /// the schema means re-keying each relation and renaming each relation's shards on
    /// each replica — one fan-out per relation, with no way to put back the ones that
    /// succeeded. That is the reason `DROP SCHEMA … CASCADE` is refused, and the
    /// refusal names the per-relation statement that *is* atomic enough to offer:
    /// `ALTER TABLE … SET SCHEMA`.
    ///
    /// Returns `SchemaNotFound` for a name that does not exist without `IF EXISTS`,
    /// `SchemaAlreadyExists` when the destination is taken (including `public` and the
    /// metadata schemas), `DependentObjectsExist` when the schema still holds
    /// relations, and `FeatureNotSupported` for `public`, a metadata schema, or an
    /// operation other than `RENAME TO` — see [`plan_alter_schema`].
    pub(super) async fn handle_alter_schema(&self, stmt: &Statement) -> PgWireResult<Response> {
        let request = plan_alter_schema(stmt)?;
        let ctx = ErrorContext::default();

        let existing = self
            .catalog
            .get_schema(&request.name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
        let Some(mut meta) = existing else {
            if request.if_exists {
                return Ok(Response::Execution(Tag::new("ALTER SCHEMA")));
            }
            return Err(schema_not_found(&request.name));
        };

        let contents = self.relations_in_schema(&request.name, &ctx)?;
        if let Some(relation) = contents.first() {
            return Err(make_vdb_error(
                VdbErrorCode::DependentObjectsExist,
                format!(
                    "schema \"{}\" cannot be renamed because it still holds \"{}\"{}: the schema \
                     is part of every relation's per-shard table names, so renaming it is one \
                     shard fan-out per relation and cannot be undone part-way. Move the relations \
                     with ALTER TABLE ... SET SCHEMA, then rename the empty schema",
                    request.name,
                    relation,
                    match contents.len() {
                        1 => String::new(),
                        n => format!(" and {} other relation(s)", n - 1),
                    }
                ),
            ));
        }

        // `public` and the metadata schemas are namespaces without being records, so
        // the claim below would happily take one of their names.
        if reserved_schema(&request.new_name) {
            return Err(schema_already_exists(&request.new_name));
        }

        // Claim the destination the way `CREATE SCHEMA` does, so a rename and a
        // concurrent create of the same name cannot both believe they own it. This
        // also covers `RENAME TO` naming the schema itself, which PostgreSQL reports
        // as a duplicate schema.
        meta.schema_name = request.new_name.clone();
        let claimed = self
            .catalog
            .create_schema_if_absent(&meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
        if !claimed {
            return Err(schema_already_exists(&request.new_name));
        }

        self.catalog
            .delete_schema(&request.name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("ALTER SCHEMA")))
    }

    /// Drop a schema. Only an empty one: the catalog tracks no dependent objects,
    /// so `CASCADE` — which would have to drop every table in the schema from every
    /// shard, one fan-out per table, with no way to undo the ones that succeeded —
    /// is refused rather than half-implemented.
    ///
    /// Returns `SchemaNotFound` for a name that does not exist without
    /// `IF EXISTS`, `DependentObjectsExist` when the schema still holds relations,
    /// and `FeatureNotSupported` for `public`, a metadata schema, or `CASCADE`.
    pub(super) async fn handle_drop_schema(&self, stmt: &Statement) -> PgWireResult<Response> {
        let request = plan_drop_schema(stmt)?;
        let ctx = ErrorContext::default();

        if !self.schema_is_recorded(&request.name, &ctx)? {
            if request.if_exists {
                return Ok(Response::Execution(Tag::new("DROP SCHEMA")));
            }
            return Err(schema_not_found(&request.name));
        }

        let contents = self.relations_in_schema(&request.name, &ctx)?;
        if let Some(relation) = contents.first() {
            return Err(make_vdb_error(
                VdbErrorCode::DependentObjectsExist,
                format!(
                    "schema \"{}\" is not empty: it still holds \"{}\"{}. Drop the relations it \
                     contains first",
                    request.name,
                    relation,
                    match contents.len() {
                        1 => String::new(),
                        n => format!(" and {} other relation(s)", n - 1),
                    }
                ),
            ));
        }

        self.catalog
            .delete_schema(&request.name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("DROP SCHEMA")))
    }

    /// Refuse a relation name whose schema does not exist, so a table can only be
    /// created in a namespace someone asked for.
    ///
    /// `key` is a canonical catalog key (see
    /// [`query_router::canonical_table_name`]). The default schema always passes; a
    /// metadata schema never does, whether or not the client could see the
    /// difference.
    pub(super) fn require_schema_exists(&self, key: &str, ctx: &ErrorContext) -> PgWireResult<()> {
        let schema = schema_of(key);
        if schema == DEFAULT_SCHEMA {
            return Ok(());
        }
        if METADATA_SCHEMAS.contains(&schema) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "schema \"{schema}\" holds VaireDB's own metadata; a relation cannot be \
                     created in it"
                ),
            ));
        }
        if self.schema_is_recorded(schema, ctx)? {
            return Ok(());
        }
        Err(schema_not_found(schema))
    }

    /// Refuse a relation name that would share its physical per-shard name with a
    /// relation that already exists.
    ///
    /// The physical name folds the schema qualifier into the identifier, so
    /// `sales.orders` and `sales_orders` both want `sales_orders_shard0`. A no-op
    /// unless one of the two names is schema-qualified: for two names in the default
    /// schema the physical form *is* the key, and equal keys are already refused as
    /// a duplicate relation.
    pub(super) fn reject_physical_name_conflict(
        &self,
        key: &str,
        ctx: &ErrorContext,
    ) -> PgWireResult<()> {
        let physical = physical_base_name(key);
        let tables = self
            .catalog
            .list_tables()
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?;
        for table in &tables {
            let mut owners = vec![("table", table.table_name.as_str())];
            owners.extend(table.indexes.iter().map(|i| ("index", i.name.as_str())));
            owners.extend(
                table
                    .constraints
                    .iter()
                    .filter(|c| c.index_backed)
                    .map(|c| ("constraint", c.name.as_str())),
            );
            for (kind, name) in owners {
                if name != key && physical_base_name(name) == physical {
                    return Err(make_vdb_error(
                        VdbErrorCode::TableAlreadyExists,
                        format!(
                            "\"{key}\" cannot be created: its per-shard tables would be named \
                             \"{physical}_shard<n>\", which the {kind} \"{name}\" already uses. A \
                             schema qualifier becomes part of the physical name, so \"{key}\" and \
                             \"{name}\" cannot both exist"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Whether `name` is a recorded schema. `public` and the metadata schemas are
    /// not records, so they answer `false` here — callers that care about
    /// *existence* rather than *droppability* go through
    /// [`Self::require_schema_exists`].
    fn schema_is_recorded(&self, name: &str, ctx: &ErrorContext) -> PgWireResult<bool> {
        Ok(self
            .catalog
            .get_schema(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some())
    }

    /// The relations (tables and views) whose canonical key sits in `schema`, by
    /// name. What makes `DROP SCHEMA` able to refuse a non-empty schema.
    fn relations_in_schema(&self, schema: &str, ctx: &ErrorContext) -> PgWireResult<Vec<String>> {
        let mut names: Vec<String> = self
            .catalog
            .list_tables()
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .into_iter()
            .map(|t| t.table_name)
            .chain(
                self.catalog
                    .list_views()
                    .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
                    .into_iter()
                    .map(|v| v.view_name),
            )
            .filter(|key| schema_of(key) == schema)
            .collect();
        names.sort();
        Ok(names)
    }
}

/// A name that exists as a namespace without being a catalog record: the default
/// schema and the metadata schemas.
fn reserved_schema(name: &str) -> bool {
    name == DEFAULT_SCHEMA || METADATA_SCHEMAS.contains(&name)
}

fn schema_already_exists(name: &str) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::SchemaAlreadyExists,
        format!("schema \"{name}\" already exists"),
    )
}

fn schema_not_found(name: &str) -> pgwire::error::PgWireError {
    make_vdb_error(
        VdbErrorCode::SchemaNotFound,
        format!("schema \"{name}\" does not exist"),
    )
}

/// What a validated `CREATE SCHEMA` asks for.
#[derive(Debug, PartialEq)]
struct CreateSchemaRequest {
    /// Canonical schema name.
    name: String,
    /// `IF NOT EXISTS`: an existing name is success, not an error.
    if_not_exists: bool,
}

/// Validate the shape of a parsed `CREATE SCHEMA` and return what it asks for.
///
/// Every clause VaireDB cannot honor is refused by name rather than accepted and
/// dropped: `AUTHORIZATION` would claim an owner in a system with no roles, and
/// `WITH`/`OPTIONS`/`DEFAULT COLLATE`/`CLONE` each promise behavior a pure
/// namespace does not have.
///
/// Pure, so the refusals are testable without a cluster.
fn plan_create_schema(stmt: &Statement) -> PgWireResult<CreateSchemaRequest> {
    let Statement::CreateSchema {
        schema_name,
        if_not_exists,
        with,
        options,
        default_collate_spec,
        clone,
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a CREATE SCHEMA statement",
        ));
    };

    let name = match schema_name {
        SchemaName::Simple(name) => name,
        SchemaName::UnnamedAuthorization(_) | SchemaName::NamedAuthorization(..) => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "CREATE SCHEMA ... AUTHORIZATION is not supported by VaireDB: a schema has no \
                 owner, because VaireDB has no roles",
            ));
        }
    };

    for (present, clause) in [
        (with.is_some(), "WITH (...)"),
        (options.is_some(), "OPTIONS (...)"),
        (default_collate_spec.is_some(), "DEFAULT COLLATE"),
        (clone.is_some(), "CLONE"),
    ] {
        if present {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "CREATE SCHEMA ... {clause} is not supported by VaireDB: a schema is a \
                     namespace and carries no properties of its own"
                ),
            ));
        }
    }

    Ok(CreateSchemaRequest {
        name: single_schema_ident(name, "CREATE SCHEMA")?,
        if_not_exists: *if_not_exists,
    })
}

/// What a validated `DROP SCHEMA` asks for.
#[derive(Debug, PartialEq)]
struct DropSchemaRequest {
    /// Canonical schema name.
    name: String,
    /// `IF EXISTS`: a name that resolves to nothing is success, not an error.
    if_exists: bool,
}

/// Validate the shape of a parsed `DROP SCHEMA` and return what it asks for.
///
/// `CASCADE` is refused rather than approximated, `public` and the metadata
/// schemas are refused because they are not records to remove, and a multi-schema
/// `DROP` is refused for the reason a multi-table one is: only the first name would
/// be acted on.
///
/// Pure, so the rules that keep a `DROP SCHEMA` from destroying more than it says
/// are testable without a cluster.
fn plan_drop_schema(stmt: &Statement) -> PgWireResult<DropSchemaRequest> {
    let Statement::Drop {
        object_type: ObjectType::Schema,
        if_exists,
        names,
        cascade,
        ..
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a DROP SCHEMA statement",
        ));
    };

    if *cascade {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP SCHEMA ... CASCADE is not supported by VaireDB: dropping the relations inside \
             the schema is one shard fan-out per table and cannot be undone part-way. Drop them \
             first, then drop the schema",
        ));
    }

    if names.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP SCHEMA with more than one schema is not supported by VaireDB; drop them one at \
             a time",
        ));
    }

    let name = names.first().ok_or_else(|| {
        make_vdb_error(VdbErrorCode::SqlSyntaxError, "DROP SCHEMA names no schema")
    })?;
    let name = single_schema_ident(name, "DROP SCHEMA")?;

    if name == DEFAULT_SCHEMA {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "schema \"{DEFAULT_SCHEMA}\" is the default namespace and cannot be dropped: \
                 every relation with no schema qualifier lives in it"
            ),
        ));
    }
    if METADATA_SCHEMAS.contains(&name.as_str()) {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("schema \"{name}\" holds VaireDB's own metadata and cannot be dropped"),
        ));
    }

    Ok(DropSchemaRequest {
        name,
        if_exists: *if_exists,
    })
}

/// What a validated `ALTER SCHEMA` asks for.
#[derive(Debug, PartialEq)]
struct AlterSchemaRequest {
    /// Canonical name of the schema being renamed.
    name: String,
    /// Canonical name it is renamed to.
    new_name: String,
    /// `IF EXISTS`: a name that resolves to nothing is success, not an error. Not
    /// PostgreSQL syntax — sqlparser accepts it for BigQuery — but it has one
    /// meaning and only one, so it is honored rather than refused.
    if_exists: bool,
}

/// Validate the shape of a parsed `ALTER SCHEMA` and return what it asks for.
///
/// `RENAME TO` is the only operation VaireDB can honor. `OWNER TO` would claim an
/// owner in a system with no roles, and `SET DEFAULT COLLATE` / `ADD REPLICA` /
/// `DROP REPLICA` / `SET OPTIONS` each promise behavior a pure namespace does not
/// have — the same refusals `CREATE SCHEMA` makes, for the same reason. `public` and
/// the metadata schemas are refused because they are not records to re-key.
///
/// Pure, so the refusals are testable without a cluster.
fn plan_alter_schema(stmt: &Statement) -> PgWireResult<AlterSchemaRequest> {
    let Statement::AlterSchema(alter) = stmt else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected an ALTER SCHEMA statement",
        ));
    };

    // PostgreSQL's `ALTER SCHEMA` takes exactly one action; sqlparser parses a list.
    let [operation] = alter.operations.as_slice() else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "ALTER SCHEMA takes exactly one action; apply them one statement at a time",
        ));
    };

    let unsupported = |clause: &str, why: &str| {
        make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("ALTER SCHEMA ... {clause} is not supported by VaireDB: {why}"),
        )
    };
    let namespace_only = "a schema is a namespace and carries no properties of its own";
    let new_name = match operation {
        AlterSchemaOperation::Rename { name } => single_schema_ident(name, "ALTER SCHEMA")?,
        AlterSchemaOperation::OwnerTo { .. } => {
            return Err(unsupported(
                "OWNER TO",
                "a schema has no owner, because VaireDB has no roles",
            ));
        }
        AlterSchemaOperation::SetDefaultCollate { .. } => {
            return Err(unsupported(
                "SET DEFAULT COLLATE",
                "VaireDB compares and orders text by byte value, so a default collation would be \
                 ignored rather than applied",
            ));
        }
        AlterSchemaOperation::AddReplica { .. } => {
            return Err(unsupported("ADD REPLICA", namespace_only));
        }
        AlterSchemaOperation::DropReplica { .. } => {
            return Err(unsupported("DROP REPLICA", namespace_only));
        }
        AlterSchemaOperation::SetOptionsParens { .. } => {
            return Err(unsupported("SET OPTIONS (...)", namespace_only));
        }
    };

    let name = single_schema_ident(&alter.name, "ALTER SCHEMA")?;
    if name == DEFAULT_SCHEMA {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "schema \"{DEFAULT_SCHEMA}\" is the default namespace and cannot be renamed: \
                 every relation with no schema qualifier lives in it"
            ),
        ));
    }
    if METADATA_SCHEMAS.contains(&name.as_str()) {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("schema \"{name}\" holds VaireDB's own metadata and cannot be renamed"),
        ));
    }

    Ok(AlterSchemaRequest {
        name,
        new_name,
        if_exists: alter.if_exists,
    })
}

/// Recognize `ALTER TABLE [IF EXISTS] <relation> SET SCHEMA <schema>` on the text and
/// return it as the schema-qualified `ALTER TABLE ... RENAME TO <schema>.<relation>`
/// the DDL path executes, or `None` for anything else.
///
/// The statement has to be caught here because sqlparser 0.62 has no `SET SCHEMA`
/// among its table operations at all — `ALTER TABLE t SET SCHEMA s` fails at
/// `Expected: (, found: SCHEMA` — so there is no AST to rewrite, the same situation
/// `RESET` is in (see [`crate::pgwire_handler::session_params::parse_reset`]).
/// Respelling it as a qualified `RENAME TO` is exact rather than approximate: a
/// relation's schema *is* part of its per-shard table names, so moving it between
/// schemas is the per-shard rename `RENAME TO` already performs.
///
/// Only the tokens are read here; the head of the statement is handed back to the
/// real parser so the relation name, its quoting and `IF EXISTS` are resolved by the
/// same code every other statement goes through. Anything that does not reshape into
/// exactly one `ALTER TABLE ... RENAME TO` answers `None` and reaches the parser
/// unchanged, where it becomes the syntax error it is — a batch, an `ONLY`, a
/// qualified destination schema, or a `SET SCHEMA` that is not the whole tail of the
/// statement.
pub(super) fn parse_alter_table_set_schema(sql: &str) -> Option<Vec<Statement>> {
    // Two cheap scans before the tokenizer: the statement must start with `ALTER`
    // and mention `SCHEMA` somewhere. A match inside a string literal costs one
    // tokenizer pass and no false rewrite, since every decision below is on tokens.
    if !sql
        .trim_start()
        .get(..5)
        .is_some_and(|word| word.eq_ignore_ascii_case("ALTER"))
    {
        return None;
    }
    if !sql
        .as_bytes()
        .windows(6)
        .any(|w| w.eq_ignore_ascii_case(b"schema"))
    {
        return None;
    }

    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize().ok()?;
    // Whitespace is a token too, so the positions in `tokens` are what the head text
    // is rebuilt from, while the grammar below is read off the significant ones.
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .collect();
    // One optional terminator; anything after it is a second statement, which this
    // recognizer cannot split (a `;` may sit inside a literal) and does not try to.
    let significant = match significant.split_last() {
        Some(((_, Token::SemiColon), rest)) => rest,
        _ => significant.as_slice(),
    };

    // `ALTER TABLE <at least one token> SET SCHEMA <schema>`.
    let [
        (_, first),
        (_, second),
        ..,
        (set_at, set),
        (_, schema_kw),
        (_, destination),
    ] = significant
    else {
        return None;
    };
    // The destination has to be an identifier token and not a literal: sqlparser
    // accepts `'sales'` as an object name, and re-rendering that as a quoted
    // identifier would move the table to a schema the client did not name.
    if !is_keyword(first, Keyword::ALTER)
        || !is_keyword(second, Keyword::TABLE)
        || !is_keyword(set, Keyword::SET)
        || !is_keyword(schema_kw, Keyword::SCHEMA)
        || !matches!(destination, Token::Word(_))
    {
        return None;
    }

    // `ALTER TABLE <head> RENAME TO <schema>` — a statement sqlparser does parse, and
    // one that resolves both names the real statement needs. The head is the client's
    // own bytes, comments and all, because every token renders back to what it was.
    let head: String = tokens[..*set_at].iter().map(Token::to_string).collect();
    let probe = format!("{head} RENAME TO {destination}");
    let mut statements = parser::parse_verbatim(&probe).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::AlterTable(mut alter) = statements.remove(0) else {
        return None;
    };
    // `SET SCHEMA` is its own `ALTER TABLE` form in PostgreSQL and carries none of
    // these; a statement that spelled one reaches the parser and is refused there.
    if alter.only
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
        || alter.location.is_some()
    {
        return None;
    }
    let [
        AlterTableOperation::RenameTable {
            table_name: RenameTableNameKind::To(parsed_destination),
        },
    ] = alter.operations.as_slice()
    else {
        return None;
    };
    let [schema] = parsed_destination.0.as_slice() else {
        return None;
    };
    let schema = schema.as_ident()?.clone();
    let relation = alter.name.0.last()?.as_ident()?.clone();

    alter.operations = vec![AlterTableOperation::RenameTable {
        table_name: RenameTableNameKind::To(ObjectName::from(vec![schema, relation])),
    }];
    alter.end_token = AttachedToken::empty();
    Some(vec![Statement::AlterTable(alter)])
}

/// Whether `token` is the given unquoted keyword.
fn is_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.keyword == keyword)
}

/// Canonicalize a schema name, which must be one plain identifier. A qualified
/// spelling (`db.schema`) is refused rather than silently reduced: it would name a
/// database VaireDB does not have.
fn single_schema_ident(name: &ObjectName, command: &str) -> PgWireResult<String> {
    if name.0.len() != 1 {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!("the schema name in {command} must be a single identifier"),
        ));
    }
    let ident = name.0[0].as_ident().ok_or_else(|| {
        make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            format!("could not determine the schema name in {command}"),
        )
    })?;
    Ok(canonicalize_ident(ident))
}

/// Qualify `new_relation` into the same schema as `key`, for the statements that
/// rename a relation without saying where it lands. PostgreSQL's
/// `ALTER TABLE ... RENAME TO` renames inside the schema rather than moving the
/// table, and so does VaireDB's.
pub(super) fn rename_within_schema(key: &str, new_relation: &str) -> String {
    query_router::qualified_name(schema_of(key), relation_of(new_relation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    fn create_error(sql: &str) -> String {
        plan_create_schema(&parse_one(sql))
            .expect_err("statement should be refused")
            .to_string()
    }

    fn drop_error(sql: &str) -> String {
        plan_drop_schema(&parse_one(sql))
            .expect_err("statement should be refused")
            .to_string()
    }

    #[test]
    fn plans_a_plain_create_schema() {
        assert_eq!(
            plan_create_schema(&parse_one("CREATE SCHEMA Sales")).unwrap(),
            CreateSchemaRequest {
                name: "sales".to_string(),
                if_not_exists: false,
            }
        );
    }

    #[test]
    fn create_schema_folds_identifiers_like_postgresql() {
        // Unquoted lowercases, quoted keeps its case — the same folding every
        // other catalog key goes through.
        assert_eq!(
            plan_create_schema(&parse_one("CREATE SCHEMA IF NOT EXISTS \"Sales\"")).unwrap(),
            CreateSchemaRequest {
                name: "Sales".to_string(),
                if_not_exists: true,
            }
        );
    }

    #[test]
    fn create_schema_refuses_authorization() {
        assert!(
            create_error("CREATE SCHEMA sales AUTHORIZATION bob").contains("AUTHORIZATION"),
            "the refusal must name the clause"
        );
    }

    #[test]
    fn create_schema_refuses_a_qualified_name() {
        assert!(create_error("CREATE SCHEMA db.sales").contains("single identifier"));
    }

    #[test]
    fn plans_a_plain_drop_schema() {
        assert_eq!(
            plan_drop_schema(&parse_one("DROP SCHEMA IF EXISTS sales")).unwrap(),
            DropSchemaRequest {
                name: "sales".to_string(),
                if_exists: true,
            }
        );
    }

    // RESTRICT is the default behavior, so spelling it out changes nothing.
    #[test]
    fn drop_schema_accepts_explicit_restrict() {
        assert_eq!(
            plan_drop_schema(&parse_one("DROP SCHEMA sales RESTRICT"))
                .unwrap()
                .name,
            "sales"
        );
    }

    #[test]
    fn drop_schema_refuses_cascade() {
        assert!(drop_error("DROP SCHEMA sales CASCADE").contains("CASCADE"));
    }

    #[test]
    fn drop_schema_refuses_more_than_one_schema() {
        assert!(drop_error("DROP SCHEMA a, b").contains("more than one schema"));
    }

    // The default namespace is where every unqualified relation lives, so there is
    // no state in which dropping it could be honored.
    #[test]
    fn drop_schema_refuses_the_default_schema() {
        assert!(drop_error("DROP SCHEMA public").contains("default namespace"));
    }

    #[test]
    fn drop_schema_refuses_a_metadata_schema() {
        for schema in METADATA_SCHEMAS {
            assert!(
                drop_error(&format!("DROP SCHEMA {schema}")).contains("metadata"),
                "{schema} must be refused as a metadata schema"
            );
        }
    }

    #[test]
    fn reserved_schemas_are_the_default_and_the_metadata_ones() {
        assert!(reserved_schema("public"));
        assert!(reserved_schema("pg_catalog"));
        assert!(reserved_schema("information_schema"));
        assert!(reserved_schema("vairedb_catalog"));
        assert!(!reserved_schema("sales"));
    }

    // A rename stays in the schema it started in, as in PostgreSQL.
    #[test]
    fn rename_keeps_the_relation_in_its_schema() {
        assert_eq!(
            rename_within_schema("sales.orders", "invoices"),
            "sales.invoices"
        );
        assert_eq!(rename_within_schema("orders", "invoices"), "invoices");
    }

    fn alter_schema_error(sql: &str) -> String {
        plan_alter_schema(&parse_one(sql))
            .expect_err("statement should be refused")
            .to_string()
    }

    #[test]
    fn plans_an_alter_schema_rename() {
        assert_eq!(
            plan_alter_schema(&parse_one("ALTER SCHEMA Sales RENAME TO \"Archive\"")).unwrap(),
            AlterSchemaRequest {
                name: "sales".to_string(),
                new_name: "Archive".to_string(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn alter_schema_refuses_owner_to() {
        assert!(alter_schema_error("ALTER SCHEMA sales OWNER TO bob").contains("no roles"));
    }

    #[test]
    fn alter_schema_refuses_the_default_schema() {
        assert!(
            alter_schema_error("ALTER SCHEMA public RENAME TO archive")
                .contains("default namespace")
        );
    }

    #[test]
    fn alter_schema_refuses_a_metadata_schema() {
        for schema in METADATA_SCHEMAS {
            assert!(
                alter_schema_error(&format!("ALTER SCHEMA {schema} RENAME TO archive"))
                    .contains("metadata"),
                "{schema} must be refused as a metadata schema"
            );
        }
    }

    #[test]
    fn alter_schema_refuses_a_qualified_destination() {
        assert!(
            alter_schema_error("ALTER SCHEMA sales RENAME TO db.archive")
                .contains("single identifier")
        );
    }

    /// The destination of the qualified `RENAME TO` the recognizer respelled the
    /// statement into, or `None` when it did not recognize it.
    fn set_schema_destination(sql: &str) -> Option<String> {
        let mut statements = parse_alter_table_set_schema(sql)?;
        assert_eq!(statements.len(), 1, "one statement in, one statement out");
        match statements.remove(0) {
            Statement::AlterTable(alter) => match alter.operations.as_slice() {
                [
                    AlterTableOperation::RenameTable {
                        table_name: RenameTableNameKind::To(name),
                    },
                ] => Some(name.to_string()),
                other => panic!("expected one RENAME TO, got {other:?}"),
            },
            other => panic!("expected ALTER TABLE, got {other:?}"),
        }
    }

    // The respelling is a move, so the destination keeps the relation name and takes
    // the new schema as its qualifier.
    #[test]
    fn set_schema_becomes_a_qualified_rename() {
        assert_eq!(
            set_schema_destination("ALTER TABLE orders SET SCHEMA sales").as_deref(),
            Some("sales.orders")
        );
        assert_eq!(
            set_schema_destination("ALTER TABLE IF EXISTS sales.orders SET SCHEMA archive")
                .as_deref(),
            Some("archive.orders")
        );
    }

    // Quoting and case are the real parser's business: the head of the statement is
    // handed back to it verbatim, and the destination goes through it too.
    #[test]
    fn set_schema_keeps_identifier_quoting() {
        assert_eq!(
            set_schema_destination("alter table \"Orders\" set schema \"Sales\"").as_deref(),
            Some("\"Sales\".\"Orders\"")
        );
        // `public` is a keyword to sqlparser, so the commonest destination of all only
        // parses because the probe hands it to the real parser rather than checking it
        // by hand.
        assert_eq!(
            set_schema_destination("ALTER TABLE sales.orders SET SCHEMA public;").as_deref(),
            Some("public.orders")
        );
    }

    // Everything the recognizer does not answer for reaches the real parser, which
    // reports the syntax error it is. `None` here is that hand-off, not an acceptance.
    #[test]
    fn set_schema_declines_what_it_cannot_respell() {
        for sql in [
            // Not this statement at all.
            "ALTER TABLE orders RENAME TO invoices",
            "ALTER TABLE orders ADD COLUMN total INT",
            "CREATE SCHEMA sales",
            "SELECT 1",
            // A destination that is not one plain schema name.
            "ALTER TABLE orders SET SCHEMA db.sales",
            "ALTER TABLE orders SET SCHEMA 'sales'",
            "ALTER TABLE orders SET SCHEMA",
            // `SET SCHEMA` has to be the whole tail: a second action is a statement
            // PostgreSQL does not have either.
            "ALTER TABLE orders SET SCHEMA sales, RENAME TO invoices",
            // PostgreSQL's `SET SCHEMA` form takes no `ONLY`.
            "ALTER TABLE ONLY orders SET SCHEMA sales",
            // A batch: the text cannot be split here, because a `;` may sit inside a
            // literal.
            "ALTER TABLE orders SET SCHEMA sales; SELECT 1",
            // The keyword guards match text inside a literal; the token grammar does
            // not.
            "INSERT INTO t VALUES ('ALTER TABLE orders SET SCHEMA sales')",
            "ALTER TABLE orders RENAME TO \"SET SCHEMA sales\"",
        ] {
            assert!(
                parse_alter_table_set_schema(sql).is_none(),
                "`{sql}` must be left to the parser"
            );
        }
    }
}
