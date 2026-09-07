//! `CREATE SCHEMA` / `DROP SCHEMA`, and the namespace rules every relation name
//! goes through.
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

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::SchemaMeta;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::query_router::{
    self, DEFAULT_SCHEMA, canonicalize_ident, relation_of, schema_of,
};
use crate::sqlparser::ast::{ObjectName, ObjectType, SchemaName, Statement};
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
}
