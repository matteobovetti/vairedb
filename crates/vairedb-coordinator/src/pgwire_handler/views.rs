//! `CREATE VIEW` / `ALTER VIEW` / `DROP VIEW`: the one relation that lives only
//! in the coordinator.
//!
//! Every other relation VaireDB knows is broadcast — a table becomes N physical
//! tables, an index N physical indexes. A view is not, because a per-shard view
//! would be a query over one shard's rows: reading it would mean unioning N
//! partial answers back together, and a view has no shard key or column list of
//! its own for the write path to route or validate against. So nothing is shipped
//! to a core node. The catalog stores the view's query **as text**, and the read
//! path inlines it as a CTE every time the view is named.
//!
//! **Inlining, not registration.** `SELECT * FROM v` is rewritten to
//! `WITH "v" AS (<definition>) SELECT * FROM v` before it reaches DataFusion's
//! planner (see [`expand_views`]). That is why a view can never be stale: there is
//! no cached plan and no recorded column list to drift from the tables underneath.
//! The cost is that a view is re-planned on every read, and that a base-table
//! change surfaces as a planning error the next time the view is read rather than
//! when the table changed — the catalog tracks no dependent objects, here as
//! everywhere else.
//!
//! A view name shares the relation namespace with tables and indexes, as in
//! PostgreSQL: the claim is made atomically against the catalog's `tables` and
//! `views` records together (see
//! [`MetadataCatalog::create_view_if_absent`](crate::catalog::MetadataCatalog::create_view_if_absent)),
//! and every statement that needs a *table* refuses a name a view holds with
//! `42809` rather than reporting it missing — see
//! [`VaireDbQueryHandler::reject_view_as_table`].
//!
//! **A view is read-only.** `INSERT`/`UPDATE`/`DELETE`/`MERGE` on one is refused:
//! rows would have to be routed to the base tables by rules the coordinator does
//! not model, and PostgreSQL's own automatically-updatable views are a narrower
//! feature than clients would assume from a view existing.

use std::collections::HashSet;
use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{MetadataCatalog, ViewMeta};
use crate::pgwire_handler::ddl::already_exists;
use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_coordinator_error, make_vdb_error,
};
use crate::pgwire_handler::handler::VaireDbQueryHandler;
use crate::pgwire_handler::parser;
use crate::pgwire_handler::query_router::{
    self, QueryType, canonical_table_name, canonicalize_ident,
};
use crate::sqlparser::ast::helpers::attached_token::AttachedToken;
use crate::sqlparser::ast::{
    CreateTableOptions, CreateView, Cte, Ident, ObjectName, ObjectType, Query, Statement,
    TableAlias, With,
};
use crate::util::now_unix_secs;
use crate::write_sql_cl;

/// How deep a chain of views may nest before the read path gives up.
///
/// Cycles are caught exactly, by name, so this is not the cycle guard — it is a
/// bound on recursion depth, since expansion walks a view's body recursively and
/// a pathological chain would otherwise be limited only by the stack.
const MAX_VIEW_NESTING: usize = 32;

impl VaireDbQueryHandler {
    /// Create (or replace) a view: validate the shape of the statement, check that
    /// the definition can actually be planned *now*, then record it in the catalog.
    /// Nothing is sent to a core node.
    ///
    /// The definition is planned before it is stored because the definition is all
    /// that is stored: a view whose body names a missing table would otherwise be
    /// created happily and fail on every read, with the error arriving nowhere near
    /// the statement that caused it.
    ///
    /// Returns `TableAlreadyExists` when the name is taken (by a table, a view, an
    /// index or an index-backed constraint — one namespace, as in PostgreSQL) and
    /// `IF NOT EXISTS` was not given, `WrongObjectType` when `OR REPLACE` names a
    /// table, or `FeatureNotSupported` for a form VaireDB cannot honor (see
    /// [`plan_create_view`]).
    pub(super) async fn handle_create_view(&self, stmt: &Statement) -> PgWireResult<Response> {
        let Statement::CreateView(create) = stmt else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "expected a CREATE VIEW statement",
            ));
        };

        let meta = plan_create_view(create)?;
        let ctx = ErrorContext::for_table(&meta.view_name);

        // A view is a relation, so it needs a namespace to live in like any other —
        // decided before the definition is planned, since a missing schema makes the
        // statement moot however the body turns out. A view has no physical
        // per-shard name, so there is nothing to check for a name collision beyond
        // the relation namespace itself.
        self.require_schema_exists(&meta.view_name, &ctx)?;

        self.validate_view_definition(&meta, &create.query).await?;

        if create.or_replace {
            // `OR REPLACE` replaces a view, never a relation of another kind: a
            // table answering to the name means the client asked for something
            // this statement must not do.
            if self.table_named(&meta.view_name, &ctx)? {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!(
                        "\"{}\" is not a view; CREATE OR REPLACE VIEW cannot replace a table",
                        meta.view_name
                    ),
                ));
            }
            self.catalog
                .put_view(&meta)
                .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
            return Ok(Response::Execution(Tag::new("CREATE VIEW")));
        }

        // The index namespace is checked here and the table/view namespace by the
        // claim below, which does it in one write transaction so two racing
        // creators cannot both win.
        if self.name_is_taken(&meta.view_name, &ctx)? {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE VIEW")));
            }
            return Err(already_exists(&meta.view_name));
        }

        let claimed = self
            .catalog
            .create_view_if_absent(&meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
        if !claimed {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE VIEW")));
            }
            return Err(already_exists(&meta.view_name));
        }

        Ok(Response::Execution(Tag::new("CREATE VIEW")))
    }

    /// Redefine an existing view (`ALTER VIEW v AS <query>`), the only `ALTER VIEW`
    /// form the dialect parses — `ALTER VIEW ... RENAME TO` is a syntax error
    /// before it reaches here.
    ///
    /// Returns `TableNotFound` when no view of that name exists (this statement
    /// does not create one), `WrongObjectType` when the name belongs to a table, or
    /// `FeatureNotSupported` for a `WITH (...)` option.
    pub(super) async fn handle_alter_view(&self, stmt: &Statement) -> PgWireResult<Response> {
        let Statement::AlterView {
            name,
            columns,
            query,
            with_options,
        } = stmt
        else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "expected an ALTER VIEW statement",
            ));
        };

        if !with_options.is_empty() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "ALTER VIEW ... WITH (...) is not supported by VaireDB: a view is stored as its query text and re-planned on every read, so there is no view option to set",
            ));
        }

        let view_name = view_name_of(name)?;
        let ctx = ErrorContext::for_table(&view_name);

        let existing = self
            .catalog
            .get_view(&view_name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;
        if existing.is_none() {
            if self.table_named(&view_name, &ctx)? {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!("\"{view_name}\" is not a view; use ALTER TABLE to alter a table"),
                ));
            }
            return Err(make_vdb_error(
                VdbErrorCode::TableNotFound,
                format!("view \"{view_name}\" does not exist"),
            ));
        }

        let meta = ViewMeta {
            view_name,
            definition: query.to_string(),
            columns: view_column_names(columns.iter())?,
            created_at: Some(prost_types::Timestamp {
                seconds: now_unix_secs() as i64,
                nanos: 0,
            }),
        };

        self.validate_view_definition(&meta, query).await?;

        self.catalog
            .put_view(&meta)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("ALTER VIEW")))
    }

    /// Drop a view: remove its definition from the catalog. The tables it read are
    /// untouched — there is nothing else to undo, since a view never existed
    /// anywhere but here.
    ///
    /// Returns `WrongObjectType` when the name belongs to a table (`DROP VIEW` must
    /// never destroy one) or `TableNotFound` when no view of that name exists
    /// without `IF EXISTS`.
    pub(super) async fn handle_drop_view(&self, stmt: &Statement) -> PgWireResult<Response> {
        let request = plan_drop_view(stmt)?;
        let ctx = ErrorContext::for_table(&request.name);

        let exists = self
            .catalog
            .get_view(&request.name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
            .is_some();

        if !exists {
            if self.table_named(&request.name, &ctx)? {
                return Err(make_vdb_error(
                    VdbErrorCode::WrongObjectType,
                    format!(
                        "\"{}\" is not a view; use DROP TABLE to drop a table",
                        request.name
                    ),
                ));
            }
            if request.if_exists {
                return Ok(Response::Execution(Tag::new("DROP VIEW")));
            }
            return Err(make_vdb_error(
                VdbErrorCode::TableNotFound,
                format!("view \"{}\" does not exist", request.name),
            ));
        }

        self.catalog
            .delete_view(&request.name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?;

        Ok(Response::Execution(Tag::new("DROP VIEW")))
    }

    /// Refuse a statement that needs a table but names a view, with `42809` and the
    /// statement that *would* work.
    ///
    /// Without this the write and DDL paths would report the view as a missing
    /// relation — they resolve their target through the catalog's `tables` records,
    /// which a view is deliberately not in — telling a client that an object it can
    /// select from does not exist.
    ///
    /// `CREATE TABLE` is absent on purpose: its name claim already fails with
    /// "relation already exists", which is both true and what PostgreSQL says.
    pub(super) fn reject_view_as_table(
        &self,
        stmt: &Statement,
        query_type: &QueryType,
    ) -> PgWireResult<()> {
        let hint = match query_type {
            QueryType::Insert | QueryType::Update | QueryType::Delete | QueryType::Merge => {
                "a view is read-only in VaireDB; write to the tables it reads"
            }
            QueryType::DropTable => "use DROP VIEW to drop a view",
            QueryType::AlterTable => "use CREATE OR REPLACE VIEW to redefine a view",
            QueryType::TruncateTable => "a view holds no rows of its own",
            QueryType::CreateIndex => "an index can only be created on a table",
            // Every other statement kind either resolves no table name or has its
            // own object-kind check.
            _ => return Ok(()),
        };

        let Some(name) = query_router::extract_table_name(stmt) else {
            return Ok(());
        };
        let ctx = ErrorContext::for_table(&name);
        let is_view = self
            .catalog
            .get_view(&name)
            .map_err(|e| enrich_coordinator_error(&e, &ctx, &self.catalog))?
            .is_some();
        if !is_view {
            return Ok(());
        }
        Err(make_vdb_error(
            VdbErrorCode::WrongObjectType,
            format!("\"{name}\" is a view, not a table; {hint}"),
        ))
    }

    /// Whether a *table* answers to `name`. Used to tell "there is no such view"
    /// apart from "that name belongs to something else", which are different errors
    /// to a client and different SQLSTATEs.
    fn table_named(&self, name: &str, ctx: &ErrorContext) -> PgWireResult<bool> {
        Ok(self
            .catalog
            .get_table(name)
            .map_err(|e| enrich_coordinator_error(&e, ctx, &self.catalog))?
            .is_some())
    }

    /// Check a proposed definition before it is stored: that it does not reference
    /// the view itself, that it does not read a metadata schema, that it plans
    /// against the tables that exist now, and that an explicit column list has as
    /// many names as the query has columns.
    ///
    /// The self-reference check is what makes the read path's cycle detection a
    /// belt-and-braces measure rather than the only guard: a cycle can only be
    /// built by redefining a view, and a redefinition that reads the view — however
    /// indirectly — is refused here.
    async fn validate_view_definition(&self, meta: &ViewMeta, query: &Query) -> PgWireResult<()> {
        // A query naming a `pg_catalog` table — qualified or not — is routed to the
        // coordinator's local context, which never expands views. A view of that
        // name would therefore be stored and then never resolve, so it is refused
        // where the client can still see why.
        if self.catalog_table_names.contains(&meta.view_name) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "\"{}\" is the name of a pg_catalog table, so a view cannot take it: a query naming it is answered from the emulated catalog. Name the view something else",
                    meta.view_name
                ),
            ));
        }

        let relations = write_sql_cl::relations_read(query);
        if relations.contains(&meta.view_name) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "view \"{}\" cannot read itself: a view is inlined into the query that names it, so a self-reference has no rows to start from",
                    meta.view_name
                ),
            ));
        }

        let dependencies = collect_view_ctes(&relations, &HashSet::new(), &self.catalog)?;
        if dependencies.iter().any(|(name, _)| name == &meta.view_name) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "view \"{}\" cannot read itself: one of the views it reads reads \"{}\" in turn",
                    meta.view_name, meta.view_name
                ),
            ));
        }

        let probe = Statement::Query(Box::new(query.clone()));
        if self.is_catalog_query(&probe) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "a view over a metadata schema (vairedb_catalog, pg_catalog, information_schema) is not supported by VaireDB: those tables are served by the coordinator's local context, and a view is planned against the distributed one. Query the metadata schema directly",
            ));
        }

        // Planned on the distributed context, exactly as a read of the view will
        // be, so what is accepted here is what can be read afterwards.
        let (plan, _) =
            parser::plan_select(&self.session_ctx, &probe, false, &self.catalog).await?;

        if !meta.columns.is_empty() && meta.columns.len() != plan.schema().fields().len() {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                format!(
                    "view \"{}\" has {} column name(s) but its query has {} column(s)",
                    meta.view_name,
                    meta.columns.len(),
                    plan.schema().fields().len()
                ),
            ));
        }

        Ok(())
    }
}

/// Rewrite `stmt` so every view it reads is defined as a CTE ahead of it, in
/// dependency order — the whole of the read path's view support.
///
/// A no-op unless the statement is a query that names at least one view, which is
/// the common case: the only catalog work for a query over plain tables is one
/// point lookup per distinct relation.
///
/// Names already bound by a top-level `WITH` are left alone. Those shadow the
/// view for the whole statement, and a second CTE of the same name would be an
/// error rather than a shadow.
pub(super) fn expand_views(
    stmt: &mut Statement,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<()> {
    let Statement::Query(query) = stmt else {
        return Ok(());
    };
    let relations = write_sql_cl::relations_read(&**query);
    if relations.is_empty() {
        return Ok(());
    }
    let shadowed = top_level_cte_names(query);
    let views = collect_view_ctes(&relations, &shadowed, catalog)?;
    if views.is_empty() {
        return Ok(());
    }
    prepend_view_ctes(query, views);
    Ok(())
}

/// The canonical names a query's own top-level `WITH` already binds.
fn top_level_cte_names(query: &Query) -> HashSet<String> {
    query
        .with
        .as_ref()
        .map(|with| {
            with.cte_tables
                .iter()
                .map(|cte| canonicalize_ident(&cte.alias.name))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve `relations` to the views among them, plus every view those read,
/// returning one CTE body per view in dependency order — a view appears after the
/// views it reads, which is the order a non-recursive `WITH` requires.
///
/// `shadowed` names are treated as already bound and are not resolved.
fn collect_view_ctes(
    relations: &[String],
    shadowed: &HashSet<String>,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireResult<Vec<(String, Box<Query>)>> {
    let mut resolved: HashSet<String> = shadowed.clone();
    let mut path: Vec<String> = Vec::new();
    let mut ordered: Vec<(String, Box<Query>)> = Vec::new();
    for name in relations {
        collect_one_view(name, catalog, &mut resolved, &mut path, &mut ordered)?;
    }
    Ok(ordered)
}

/// Post-order walk of one relation name: resolve what it reads first, then emit it.
///
/// `resolved` holds every name already settled — emitted as a CTE, shadowed by a
/// user CTE, or found not to be a view at all — so each name costs at most one
/// catalog lookup. `path` is the chain currently being walked, which is what makes
/// a cycle reportable by name instead of as a stack overflow.
fn collect_one_view(
    name: &str,
    catalog: &Arc<MetadataCatalog>,
    resolved: &mut HashSet<String>,
    path: &mut Vec<String>,
    ordered: &mut Vec<(String, Box<Query>)>,
) -> PgWireResult<()> {
    if resolved.contains(name) {
        return Ok(());
    }

    let ctx = ErrorContext::for_table(name);
    let view = catalog
        .get_view(name)
        .map_err(|e| enrich_coordinator_error(&e, &ctx, catalog))?;
    let Some(view) = view else {
        // A table, or nothing at all — either way the planner resolves it.
        resolved.insert(name.to_string());
        return Ok(());
    };

    if path.iter().any(|seen| seen == name) {
        path.push(name.to_string());
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "the definitions of these views form a cycle: {}",
                path.join(" -> ")
            ),
        ));
    }
    if path.len() >= MAX_VIEW_NESTING {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "view \"{name}\" is nested more than {MAX_VIEW_NESTING} levels deep; VaireDB inlines a view into the query that reads it, and will not inline further"
            ),
        ));
    }

    let body = parse_view_definition(&view)?;

    path.push(name.to_string());
    for inner in write_sql_cl::relations_read(&*body) {
        collect_one_view(&inner, catalog, resolved, path, ordered)?;
    }
    path.pop();

    resolved.insert(name.to_string());
    ordered.push((name.to_string(), body));
    Ok(())
}

/// Re-parse a stored definition back into a query.
///
/// The text came from a `Query` this coordinator itself rendered, having already
/// planned it once, so a failure here means the stored record is corrupt — an
/// internal error, not the client's mistake.
fn parse_view_definition(view: &ViewMeta) -> PgWireResult<Box<Query>> {
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    let corrupt = |e: crate::sqlparser::parser::ParserError| {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!(
                "the stored definition of view \"{}\" could not be parsed: {e}",
                view.view_name
            ),
        )
    };

    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&view.definition)
        .map_err(corrupt)?;
    parser.parse_query().map_err(corrupt)
}

/// Prepend one CTE per view to `query`'s `WITH` clause, keeping the client's own
/// CTEs after them.
///
/// Before, not after: a CTE is visible to the ones that follow it, so views —
/// which know nothing of the client's CTEs — must come first for a view that reads
/// another view to resolve. `WITH RECURSIVE` is left as the client set it; a
/// non-recursive CTE is legal inside a recursive `WITH`.
fn prepend_view_ctes(query: &mut Query, views: Vec<(String, Box<Query>)>) {
    let mut view_ctes: Vec<Cte> = views
        .into_iter()
        .map(|(name, body)| Cte {
            alias: TableAlias {
                explicit: false,
                // Quoted: the stored name is already canonical, and quoting keeps
                // a name whose case the client protected from being folded again.
                name: Ident::with_quote('"', name),
                columns: Vec::new(),
                at: None,
            },
            query: body,
            from: None,
            materialized: None,
            closing_paren_token: AttachedToken::empty(),
        })
        .collect();

    match &mut query.with {
        Some(with) => {
            view_ctes.append(&mut with.cte_tables);
            with.cte_tables = view_ctes;
        }
        None => {
            query.with = Some(With {
                with_token: AttachedToken::empty(),
                recursive: false,
                cte_tables: view_ctes,
            });
        }
    }
}

/// Validate the shape of a parsed `CREATE VIEW` and return the metadata to store.
///
/// Pure, so what a view may be is testable without a catalog. What is refused, and
/// why:
///
/// - **`MATERIALIZED`.** The rows would have to live in the coordinator, which
///   holds no user data, and refreshing them would need a consistent snapshot of
///   every shard — the same guarantee cross-shard atomicity is still missing.
/// - **Every dialect decoration** (`OR ALTER`, `SECURE`, `TEMPORARY`, `TO`,
///   `CLUSTER BY`, `COMMENT`, `WITH (...)`, `WITH NO SCHEMA BINDING`, MySQL's
///   `ALGORITHM`/`DEFINER`/`SQL SECURITY`). A view here is its query text and
///   nothing else, so each of these would be accepted and then ignored.
/// - **A type or option on a view column.** The parenthesized list renames the
///   query's columns; it does not declare them.
fn plan_create_view(create: &CreateView) -> PgWireResult<ViewMeta> {
    if create.materialized {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "CREATE MATERIALIZED VIEW is not supported by VaireDB: its rows would have to be stored in the coordinator, which holds no user data, and refreshing them would need one consistent snapshot across every shard. Create a plain view, or a table you populate yourself",
        ));
    }

    for (present, clause) in [
        (create.or_alter, "OR ALTER"),
        (create.secure, "SECURE"),
        (create.temporary, "TEMPORARY"),
        (create.with_no_schema_binding, "WITH NO SCHEMA BINDING"),
        (create.to.is_some(), "TO"),
        (!create.cluster_by.is_empty(), "CLUSTER BY"),
        (create.comment.is_some(), "COMMENT"),
        (create.params.is_some(), "ALGORITHM/DEFINER/SQL SECURITY"),
        (
            !matches!(create.options, CreateTableOptions::None),
            "WITH (...)",
        ),
    ] {
        if present {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "CREATE VIEW ... {clause} is not supported by VaireDB: a view is stored as its query text and re-planned on every read, so there is nothing for {clause} to change. Create the view without it"
                ),
            ));
        }
    }

    let view_name = view_name_of(&create.name)?;

    for column in &create.columns {
        if column.data_type.is_some() || column.options.is_some() {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                "a type or an option on a view's column is not supported by VaireDB: the names in parentheses rename the columns the view's query returns, they do not declare them",
            ));
        }
    }

    Ok(ViewMeta {
        view_name,
        definition: create.query.to_string(),
        columns: view_column_names(create.columns.iter().map(|c| &c.name))?,
        created_at: Some(prost_types::Timestamp {
            seconds: now_unix_secs() as i64,
            nanos: 0,
        }),
    })
}

/// Canonicalize a view's name, rejecting a name that is not an identifier. A
/// `schema.` qualifier is kept as part of the key, as it is for a table, so a view
/// lives in a schema like every other relation — see
/// [`crate::pgwire_handler::schemas`].
fn view_name_of(name: &ObjectName) -> PgWireResult<String> {
    canonical_table_name(name).ok_or_else(|| {
        make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "could not determine the view name",
        )
    })
}

/// Canonicalize an explicit view column list, rejecting a repeated name — the
/// query's result would have two columns answering to it, and neither could be
/// selected unambiguously.
fn view_column_names<'a>(idents: impl Iterator<Item = &'a Ident>) -> PgWireResult<Vec<String>> {
    let mut columns: Vec<String> = Vec::new();
    for ident in idents {
        let name = canonicalize_ident(ident);
        if columns.contains(&name) {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnAlreadyExists,
                format!("column \"{name}\" is named twice in the view's column list"),
            ));
        }
        columns.push(name);
    }
    Ok(columns)
}

/// What a validated `DROP VIEW` asks for.
#[derive(Debug, PartialEq)]
struct DropViewRequest {
    /// Canonical name of the view to drop.
    name: String,
    /// `IF EXISTS`: a name that resolves to nothing is success, not an error.
    if_exists: bool,
}

/// Validate the shape of a parsed `DROP VIEW` and return what it asks for.
///
/// Refuses more than one name and any referential action, for the reasons
/// `DROP TABLE` does: only the first name would be acted on, and the catalog
/// tracks no dependent objects for `CASCADE` to follow.
fn plan_drop_view(stmt: &Statement) -> PgWireResult<DropViewRequest> {
    let Statement::Drop {
        object_type: ObjectType::View,
        if_exists,
        names,
        cascade,
        restrict,
        ..
    } = stmt
    else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "expected a DROP VIEW statement",
        ));
    };

    if names.len() > 1 {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP VIEW with more than one view is not supported by VaireDB; drop each view with its own statement",
        ));
    }

    if *cascade || *restrict {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            "DROP VIEW ... CASCADE/RESTRICT is not supported by VaireDB; the catalog tracks no dependent objects",
        ));
    }

    let name = names
        .first()
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "DROP VIEW names no view to drop",
            )
        })
        .and_then(view_name_of)?;

    Ok(DropViewRequest {
        name,
        if_exists: *if_exists,
    })
}

/// `0A000` for a `CREATE VIEW` form the coordinator will not honor, used by the
/// tests to name the refusal they expect.
#[cfg(test)]
fn refusal_message(sql: &str) -> String {
    let stmt = parse_one(sql);
    let Statement::CreateView(create) = &stmt else {
        panic!("`{sql}` is not a CREATE VIEW");
    };
    plan_create_view(create)
        .err()
        .unwrap_or_else(|| panic!("`{sql}` should be refused"))
        .to_string()
}

#[cfg(test)]
fn parse_one(sql: &str) -> Statement {
    crate::pgwire_handler::parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"))
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("`{sql}` parsed to no statement"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog in a temp file, for the expansion tests: they need real stored
    /// views, and the point lookups are what expansion actually does.
    fn catalog_with(views: &[(&str, &str)]) -> Arc<MetadataCatalog> {
        let catalog = crate::pgwire_handler::test_catalog::scratch_catalog("views");
        for (name, definition) in views {
            catalog
                .put_view(&ViewMeta {
                    view_name: (*name).to_string(),
                    definition: (*definition).to_string(),
                    columns: Vec::new(),
                    created_at: None,
                })
                .unwrap();
        }
        Arc::new(catalog)
    }

    fn expanded(sql: &str, views: &[(&str, &str)]) -> String {
        let mut stmt = parse_one(sql);
        expand_views(&mut stmt, &catalog_with(views)).unwrap_or_else(|e| panic!("{e}"));
        stmt.to_string()
    }

    fn expansion_error(sql: &str, views: &[(&str, &str)]) -> String {
        let mut stmt = parse_one(sql);
        expand_views(&mut stmt, &catalog_with(views))
            .err()
            .unwrap_or_else(|| panic!("`{sql}` should not expand"))
            .to_string()
    }

    #[test]
    fn a_view_becomes_a_cte_ahead_of_the_query() {
        let sql = expanded(
            "SELECT * FROM active_users",
            &[("active_users", "SELECT * FROM users WHERE active")],
        );
        assert!(sql.contains("WITH \"active_users\" AS"), "got: {sql}");
        assert!(
            sql.contains("SELECT * FROM users WHERE active"),
            "got: {sql}"
        );
    }

    #[test]
    fn a_query_over_tables_only_is_left_alone() {
        let sql = "SELECT * FROM users";
        assert_eq!(expanded(sql, &[("v", "SELECT 1")]), sql);
    }

    /// The view's own name resolved through a `schema.` qualifier, because the read
    /// path collapses the qualifier only *after* expansion.
    #[test]
    fn a_schema_qualified_reference_still_finds_the_view() {
        let sql = expanded("SELECT * FROM public.v", &[("v", "SELECT id FROM t")]);
        assert!(sql.contains("WITH \"v\" AS"), "got: {sql}");
    }

    #[test]
    fn a_view_over_a_view_is_defined_after_the_one_it_reads() {
        let sql = expanded(
            "SELECT * FROM outer_v",
            &[
                ("outer_v", "SELECT * FROM inner_v WHERE id > 1"),
                ("inner_v", "SELECT * FROM t"),
            ],
        );
        let inner = sql.find("\"inner_v\" AS").expect(&sql);
        let outer = sql.find("\"outer_v\" AS").expect(&sql);
        assert!(inner < outer, "dependency must come first: {sql}");
    }

    #[test]
    fn a_view_read_twice_is_defined_once() {
        let sql = expanded(
            "SELECT * FROM v AS a JOIN v AS b ON a.id = b.id",
            &[("v", "SELECT id FROM t")],
        );
        assert_eq!(sql.matches("\"v\" AS").count(), 1, "got: {sql}");
    }

    /// A name the client's own `WITH` binds must not gain a second definition: a
    /// duplicate CTE name is an error, and the client's CTE is what it meant.
    #[test]
    fn a_user_cte_shadows_a_view_of_the_same_name() {
        let sql = expanded(
            "WITH v AS (SELECT 1 AS id) SELECT * FROM v",
            &[("v", "SELECT id FROM t")],
        );
        assert_eq!(sql.matches("v AS").count(), 1, "got: {sql}");
        assert!(!sql.contains("FROM t"), "got: {sql}");
    }

    #[test]
    fn view_ctes_are_prepended_to_the_clients_own() {
        let sql = expanded(
            "WITH mine AS (SELECT 1 AS id) SELECT * FROM v JOIN mine ON true",
            &[("v", "SELECT id FROM t")],
        );
        let view = sql.find("\"v\" AS").expect(&sql);
        let mine = sql.find("mine AS").expect(&sql);
        assert!(view < mine, "views must come first: {sql}");
    }

    /// Only a redefinition can build a cycle, and `CREATE OR REPLACE VIEW` refuses
    /// one — so this is the guard behind that guard, and it must name the cycle
    /// rather than exhaust the stack.
    #[test]
    fn a_cycle_between_views_is_reported_by_name() {
        let message = expansion_error(
            "SELECT * FROM a",
            &[("a", "SELECT * FROM b"), ("b", "SELECT * FROM a")],
        );
        assert!(message.contains("form a cycle"), "got: {message}");
        assert!(message.contains("a -> b -> a"), "got: {message}");
    }

    #[test]
    fn a_non_query_statement_is_untouched() {
        let mut stmt = parse_one("INSERT INTO t (id) VALUES (1)");
        let before = stmt.to_string();
        expand_views(&mut stmt, &catalog_with(&[("v", "SELECT 1")])).unwrap();
        assert_eq!(stmt.to_string(), before);
    }

    #[test]
    fn a_materialized_view_is_refused_with_its_reason() {
        let message = refusal_message("CREATE MATERIALIZED VIEW v AS SELECT 1");
        assert!(message.contains("holds no user data"), "got: {message}");
    }

    #[test]
    fn every_dialect_decoration_is_named_in_its_refusal() {
        // Only the forms the PostgreSQL dialect parses are checked here; the rest
        // (`SECURE`, `TO`, `CLUSTER BY`, …) are other dialects' syntax and never
        // reach `plan_create_view` at all.
        for (sql, clause) in [
            ("CREATE TEMPORARY VIEW v AS SELECT 1", "TEMPORARY"),
            ("CREATE VIEW v WITH (a = 'b') AS SELECT 1", "WITH (...)"),
        ] {
            let message = refusal_message(sql);
            assert!(message.contains(clause), "`{sql}` got: {message}");
        }
    }

    #[test]
    fn a_plain_create_view_is_recorded_as_its_query_text() {
        let stmt = parse_one("CREATE VIEW Big_Orders AS SELECT id FROM orders WHERE amount > 10");
        let Statement::CreateView(create) = &stmt else {
            panic!("not a CREATE VIEW");
        };
        let meta = plan_create_view(create).unwrap();
        assert_eq!(meta.view_name, "big_orders");
        assert_eq!(meta.definition, "SELECT id FROM orders WHERE amount > 10");
        assert!(meta.columns.is_empty());
    }

    #[test]
    fn an_explicit_column_list_is_canonicalized() {
        let stmt = parse_one("CREATE VIEW v (Id, \"Note\") AS SELECT id, note FROM t");
        let Statement::CreateView(create) = &stmt else {
            panic!("not a CREATE VIEW");
        };
        let meta = plan_create_view(create).unwrap();
        assert_eq!(meta.columns, vec!["id".to_string(), "Note".to_string()]);
    }

    #[test]
    fn a_repeated_column_name_is_refused() {
        let stmt = parse_one("CREATE VIEW v (id, id) AS SELECT a, b FROM t");
        let Statement::CreateView(create) = &stmt else {
            panic!("not a CREATE VIEW");
        };
        let message = plan_create_view(create)
            .err()
            .unwrap_or_else(|| panic!("a repeated column name should be refused"))
            .to_string();
        assert!(message.contains("named twice"), "got: {message}");
    }

    #[test]
    fn drop_view_reads_its_name_and_if_exists() {
        let request = plan_drop_view(&parse_one("DROP VIEW IF EXISTS Public.V")).unwrap();
        assert_eq!(
            request,
            DropViewRequest {
                name: "v".to_string(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn drop_view_refuses_what_it_would_only_half_do() {
        for (sql, expected) in [
            ("DROP VIEW a, b", "more than one view"),
            ("DROP VIEW v CASCADE", "no dependent objects"),
        ] {
            let message = plan_drop_view(&parse_one(sql))
                .err()
                .unwrap_or_else(|| panic!("`{sql}` should be refused"))
                .to_string();
            assert!(message.contains(expected), "`{sql}` got: {message}");
        }
    }

    // --- the handlers, against a real catalog ---

    use crate::catalog::TableMeta;
    use crate::pgwire_handler::handler::VaireDbQueryHandler;
    use pgwire::error::PgWireError;

    fn reported(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("expected a user-facing error, got {other:?}"),
        }
    }

    /// Run a view statement the way the simple-query path does: classify it, then
    /// dispatch. Returns the SQLSTATE and message of whatever it refused.
    async fn run(handler: &VaireDbQueryHandler, sql: &str) -> PgWireResult<Response> {
        let stmt = parse_one(sql);
        match query_router::classify_statement(&stmt) {
            QueryType::CreateView => handler.handle_create_view(&stmt).await,
            QueryType::AlterView => handler.handle_alter_view(&stmt).await,
            QueryType::DropView => handler.handle_drop_view(&stmt).await,
            other => panic!("`{sql}` classified as {other:?}, not view DDL"),
        }
    }

    async fn refusal(handler: &VaireDbQueryHandler, sql: &str) -> (String, String) {
        reported(
            run(handler, sql)
                .await
                .err()
                .unwrap_or_else(|| panic!("`{sql}` should be refused")),
        )
    }

    /// The pre-dispatch object-kind check, classified as the handler classifies it.
    fn view_as_table(handler: &VaireDbQueryHandler, sql: &str) -> PgWireResult<()> {
        let stmt = parse_one(sql);
        let query_type = query_router::classify_statement(&stmt);
        handler.reject_view_as_table(&stmt, &query_type)
    }

    fn register_table(handler: &VaireDbQueryHandler, name: &str) {
        handler
            .catalog
            .put_table(&TableMeta {
                table_name: name.to_string(),
                shard_key: "id".to_string(),
                shard_count: 2,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
    }

    #[tokio::test]
    async fn a_created_view_is_stored_as_its_query_text() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap_or_else(|e| panic!("{e}"));

        let stored = handler.catalog.get_view("v").unwrap().unwrap();
        assert_eq!(stored.definition, "SELECT 1 AS id");
    }

    #[tokio::test]
    async fn creating_a_view_twice_is_refused_unless_if_not_exists() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();

        let (code, message) = refusal(&handler, "CREATE VIEW v AS SELECT 2 AS id").await;
        assert_eq!(code, "42P07");
        assert!(message.contains("\"v\""), "got: {message}");

        run(&handler, "CREATE VIEW IF NOT EXISTS v AS SELECT 2 AS id")
            .await
            .unwrap_or_else(|e| panic!("IF NOT EXISTS must succeed: {e}"));
        // …and must not have replaced the definition.
        let stored = handler.catalog.get_view("v").unwrap().unwrap();
        assert_eq!(stored.definition, "SELECT 1 AS id");
    }

    #[tokio::test]
    async fn or_replace_redefines_a_view() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();
        run(&handler, "CREATE OR REPLACE VIEW v AS SELECT 2 AS id")
            .await
            .unwrap_or_else(|e| panic!("{e}"));

        let stored = handler.catalog.get_view("v").unwrap().unwrap();
        assert_eq!(stored.definition, "SELECT 2 AS id");
    }

    /// One relation namespace: a view may not take a name a table, an index or an
    /// index-backed constraint already answers to.
    #[tokio::test]
    async fn a_view_cannot_take_a_name_another_relation_holds() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_table(&handler, "orders");
        let mut table = handler.catalog.get_table("orders").unwrap().unwrap();
        table.indexes.push(crate::catalog::IndexMeta {
            name: "idx_orders_id".to_string(),
            columns: vec!["id".to_string()],
            unique: false,
        });
        handler.catalog.put_table(&table).unwrap();

        for name in ["orders", "idx_orders_id"] {
            let (code, _) =
                refusal(&handler, &format!("CREATE VIEW {name} AS SELECT 1 AS id")).await;
            assert_eq!(code, "42P07", "`{name}` is already taken");
            assert!(handler.catalog.get_view(name).unwrap().is_none());
        }
    }

    /// The unrecoverable case: `CREATE OR REPLACE VIEW` naming a table must not be
    /// read as permission to do anything to the table.
    #[tokio::test]
    async fn or_replace_will_not_replace_a_table() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_table(&handler, "orders");

        let (code, message) =
            refusal(&handler, "CREATE OR REPLACE VIEW orders AS SELECT 1 AS id").await;
        assert_eq!(code, "42809");
        assert!(message.contains("is not a view"), "got: {message}");
        assert!(handler.catalog.get_table("orders").unwrap().is_some());
        assert!(handler.catalog.get_view("orders").unwrap().is_none());
    }

    /// The same guard from the other side, and the one this phase's manual check is
    /// about: `DROP VIEW <table>` must leave the table alone.
    #[tokio::test]
    async fn drop_view_will_not_drop_a_table() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_table(&handler, "orders");

        let (code, message) = refusal(&handler, "DROP VIEW orders").await;
        assert_eq!(code, "42809");
        assert!(message.contains("use DROP TABLE"), "got: {message}");
        assert!(handler.catalog.get_table("orders").unwrap().is_some());
    }

    #[tokio::test]
    async fn dropping_an_unknown_view_is_refused_unless_if_exists() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, message) = refusal(&handler, "DROP VIEW nowhere").await;
        assert_eq!(code, "42P01");
        assert!(message.contains("nowhere"), "got: {message}");

        run(&handler, "DROP VIEW IF EXISTS nowhere")
            .await
            .unwrap_or_else(|e| panic!("IF EXISTS must succeed: {e}"));
    }

    #[tokio::test]
    async fn dropping_a_view_forgets_its_definition() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();
        run(&handler, "DROP VIEW v").await.unwrap();
        assert!(handler.catalog.get_view("v").unwrap().is_none());
    }

    #[tokio::test]
    async fn alter_view_redefines_an_existing_view_and_creates_none() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, _) = refusal(&handler, "ALTER VIEW v AS SELECT 1 AS id").await;
        assert_eq!(code, "42P01");
        assert!(handler.catalog.get_view("v").unwrap().is_none());

        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();
        run(&handler, "ALTER VIEW v AS SELECT 2 AS id")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            handler.catalog.get_view("v").unwrap().unwrap().definition,
            "SELECT 2 AS id"
        );
    }

    /// A definition is planned before it is stored, so a view over a table that
    /// does not exist fails here rather than on every later read.
    #[tokio::test]
    async fn a_definition_that_cannot_be_planned_is_refused_at_create() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (_, message) = refusal(&handler, "CREATE VIEW v AS SELECT * FROM nowhere").await;
        assert!(message.contains("nowhere"), "got: {message}");
        assert!(handler.catalog.get_view("v").unwrap().is_none());
    }

    /// A metadata schema is served by the coordinator's local context; a view is
    /// planned against the distributed one, so the definition could never resolve.
    #[tokio::test]
    async fn a_view_over_a_metadata_schema_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, message) = refusal(
            &handler,
            "CREATE VIEW v AS SELECT * FROM pg_catalog.pg_class",
        )
        .await;
        assert_eq!(code, "0A000");
        assert!(message.contains("metadata schema"), "got: {message}");
    }

    #[tokio::test]
    async fn a_column_list_of_the_wrong_width_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);

        let (code, message) = refusal(&handler, "CREATE VIEW v (a, b) AS SELECT 1 AS id").await;
        assert_eq!(code, "42601");
        assert!(message.contains("2 column name(s)"), "got: {message}");
        assert!(message.contains("1 column"), "got: {message}");
    }

    /// Read-time cycle detection is the backstop; this is the guard in front of it.
    /// `CREATE OR REPLACE VIEW v AS SELECT * FROM v` would otherwise inline the
    /// *old* definition and silently mean something else than it says.
    #[tokio::test]
    async fn a_definition_that_reads_the_view_itself_is_refused() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();

        let (code, message) =
            refusal(&handler, "CREATE OR REPLACE VIEW v AS SELECT * FROM v").await;
        assert_eq!(code, "0A000");
        assert!(message.contains("cannot read itself"), "got: {message}");
        assert_eq!(
            handler.catalog.get_view("v").unwrap().unwrap().definition,
            "SELECT 1 AS id"
        );
    }

    /// The same, one step removed: `b` reads `a`, so `a` may not be redefined to
    /// read `b`.
    #[tokio::test]
    async fn an_indirect_cycle_is_refused_at_create() {
        let handler = VaireDbQueryHandler::for_tests(false);
        handler
            .catalog
            .put_view(&ViewMeta {
                view_name: "a".to_string(),
                definition: "SELECT 1 AS id".to_string(),
                columns: Vec::new(),
                created_at: None,
            })
            .unwrap();
        handler
            .catalog
            .put_view(&ViewMeta {
                view_name: "b".to_string(),
                definition: "SELECT * FROM a".to_string(),
                columns: Vec::new(),
                created_at: None,
            })
            .unwrap();

        let (code, message) =
            refusal(&handler, "CREATE OR REPLACE VIEW a AS SELECT * FROM b").await;
        assert_eq!(code, "0A000");
        assert!(message.contains("cannot read itself"), "got: {message}");
    }

    /// Every statement that needs a table must say the name belongs to a view, not
    /// that it does not exist — and must say which statement to use instead.
    #[tokio::test]
    async fn a_statement_that_needs_a_table_refuses_a_view_by_kind() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();

        for (sql, hint) in [
            ("INSERT INTO v (id) VALUES (1)", "read-only"),
            ("UPDATE v SET id = 1", "read-only"),
            ("DELETE FROM v", "read-only"),
            ("DROP TABLE v", "use DROP VIEW"),
            ("ALTER TABLE v RENAME TO w", "CREATE OR REPLACE VIEW"),
            ("TRUNCATE v", "no rows of its own"),
            ("CREATE INDEX idx ON v (id)", "only be created on a table"),
        ] {
            let (code, message) = reported(
                view_as_table(&handler, sql)
                    .err()
                    .unwrap_or_else(|| panic!("`{sql}` should be refused")),
            );
            assert_eq!(code, "42809", "`{sql}`");
            assert!(message.contains(hint), "`{sql}` got: {message}");
        }
    }

    /// And a statement naming a real table must pass the same check untouched.
    #[tokio::test]
    async fn a_statement_naming_a_table_is_left_alone() {
        let handler = VaireDbQueryHandler::for_tests(false);
        register_table(&handler, "orders");

        for sql in ["INSERT INTO orders (id) VALUES (1)", "DROP TABLE orders"] {
            assert!(view_as_table(&handler, sql).is_ok(), "`{sql}`");
        }
    }

    /// `DROP INDEX` shares the namespace too, and the view is not an index.
    #[tokio::test]
    async fn drop_index_naming_a_view_is_refused_by_kind() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();

        let (code, message) = reported(
            handler
                .handle_drop_index(&parse_one("DROP INDEX v"))
                .await
                .err()
                .unwrap_or_else(|| panic!("DROP INDEX naming a view should be refused")),
        );
        assert_eq!(code, "42809");
        assert!(message.contains("use DROP VIEW"), "got: {message}");
        assert!(handler.catalog.get_view("v").unwrap().is_some());
    }

    /// A table may not take a view's name either — the namespace claim is made in
    /// one write transaction, so the check cannot be raced.
    #[tokio::test]
    async fn a_table_cannot_take_a_views_name() {
        let handler = VaireDbQueryHandler::for_tests(false);
        run(&handler, "CREATE VIEW v AS SELECT 1 AS id")
            .await
            .unwrap();

        let claimed = handler
            .catalog
            .create_table_if_absent(&TableMeta {
                table_name: "v".to_string(),
                shard_key: "id".to_string(),
                shard_count: 1,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
        assert!(!claimed, "a view already holds the name \"v\"");
    }
}
