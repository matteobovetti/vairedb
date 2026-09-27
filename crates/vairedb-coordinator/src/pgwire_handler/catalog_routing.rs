//! Which of the coordinator's two read contexts a statement belongs to.
//!
//! `pg_catalog`, `information_schema` and `vairedb_catalog` are emulated in-process on
//! `local_ctx`; user tables live on the storage nodes and are reached through the
//! distributed `session_ctx`. Routing is therefore a property of the relations a
//! statement names, and the three questions asked here — does it read metadata at
//! routing time, does it at parse time, and does it read *both* kinds — are one walk
//! over those relations under three predicates. They belong together because they have
//! to agree: a statement routed to the local context by one rule and judged joinable by
//! another is exactly the failure [`reject_catalog_join_to_user_data`] exists to catch.

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::execution::context::SessionContext;
use pgwire::error::PgWireResult;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use super::error_enrichment::make_vdb_error;
use crate::catalog::MetadataCatalog;
use crate::sqlparser::ast::{ObjectName, ObjectNamePart, Statement, visit_relations};

/// Schema namespaces whose relations are metadata/introspection and should execute on the
/// local DataFusion context rather than the distributed Ballista context.
const CATALOG_SCHEMA_PREFIXES: [&str; 3] =
    ["pg_catalog.", "information_schema.", "vairedb_catalog."];

/// Collect the bare relation names exposed by the `pg_catalog` schema registered
/// in `ctx`, lowercased, so unqualified references (e.g. `pg_class`) route to the
/// local context just like qualified `pg_catalog.pg_class` does.
///
/// Only `pg_catalog` is enumerated: its tables are all `pg_*`-prefixed and so do
/// not collide with user table names. `information_schema` is resolved lazily by
/// DataFusion (not an enumerable provider), and `vairedb_catalog` exposes generic
/// names (`tables`, `nodes`, ...) that *would* collide with user tables — both are
/// matched only when explicitly schema-qualified, via [`CATALOG_SCHEMA_PREFIXES`].
pub(super) fn catalog_table_names(ctx: &SessionContext) -> HashSet<String> {
    let mut names = HashSet::new();
    let default_catalog = ctx
        .state()
        .config()
        .options()
        .catalog
        .default_catalog
        .clone();
    if let Some(catalog) = ctx.catalog(&default_catalog)
        && let Some(schema) = catalog.schema("pg_catalog")
    {
        for t in schema.table_names() {
            names.insert(t.to_lowercase());
        }
    }
    names
}

/// The first relation in `stmt` that `of_interest` yields a value for.
///
/// [`visit_relations`] sees joins, subqueries and CTEs — not just the first `FROM`
/// table — and it visits only relation identifiers, so a user value like
/// `'pg_catalog.foo'` cannot produce a false positive. The walk stops at the first
/// answer: every caller here wants existence, not a census.
fn first_relation<T>(
    stmt: &Statement,
    mut of_interest: impl FnMut(&ObjectName) -> Option<T>,
) -> Option<T> {
    visit_relations(stmt, |relation| match of_interest(relation) {
        Some(found) => ControlFlow::Break(found),
        None => ControlFlow::Continue(()),
    })
    .break_value()
}

/// Whether `relation` names metadata: schema-qualified with a catalog prefix, or a bare
/// name `bare_is_metadata` recognizes. The two callers differ only in that predicate,
/// and the qualified half must stay common to both — a statement one of them routes to
/// the local context and the other does not is a plan built for the wrong engine.
fn is_metadata(relation: &ObjectName, bare_is_metadata: impl Fn(&str) -> bool) -> bool {
    let qualified = relation.to_string().to_lowercase();
    if CATALOG_SCHEMA_PREFIXES
        .iter()
        .any(|prefix| qualified.starts_with(prefix))
    {
        return true;
    }
    trailing_identifier(relation).is_some_and(|bare| bare_is_metadata(&bare.to_lowercase()))
}

/// The relation's own name — the last identifier segment, unqualified — in the case the
/// client wrote it. Deliberately not lowercased: [`reject_catalog_join_to_user_data`]
/// looks this up in the metadata catalog and prints it back to the client.
fn trailing_identifier(relation: &ObjectName) -> Option<&str> {
    match relation.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => Some(&ident.value),
        _ => None,
    }
}

/// Every `pg_catalog` relation is `pg_`-prefixed, which is what makes the prefix a
/// usable stand-in for the registered set.
fn pg_prefixed(bare: &str) -> bool {
    bare.starts_with("pg_")
}

/// Whether any relation the statement references targets a catalog schema.
///
/// A relation matches when it is schema-qualified with a catalog prefix
/// ([`CATALOG_SCHEMA_PREFIXES`]) or when its bare name is a known `pg_catalog`
/// table in `catalog_names` (so unqualified `pg_class` is caught too).
pub(super) fn references_catalog_schema(stmt: &Statement, catalog_names: &HashSet<String>) -> bool {
    first_relation(stmt, |relation| {
        is_metadata(relation, |bare| catalog_names.contains(bare)).then_some(())
    })
    .is_some()
}

/// Whether the statement reads metadata, decided **without** the set of registered
/// `pg_catalog` relations — the parse-time approximation of
/// [`references_catalog_schema`].
///
/// The parse happens before any context is in hand, so the registered set is not
/// available there; what is available is the naming convention it follows. Every
/// `pg_catalog` relation is `pg_`-prefixed (the reason [`catalog_table_names`]
/// enumerates that schema and no other), and the two schemas with collidable names are
/// matched only when qualified — exactly as they are at routing time.
///
/// It errs towards *yes*: a user table called `pg_things` is treated as metadata. That
/// is the harmless direction for the one caller, which uses the answer to decide
/// whether to leave a statement to `datafusion-pg-catalog`'s own rewrites, and PG
/// reserves the prefix anyway.
pub(super) fn reads_metadata(stmt: &Statement) -> bool {
    first_relation(stmt, |relation| {
        is_metadata(relation, pg_prefixed).then_some(())
    })
    .is_some()
}

/// Refuse a statement that reads catalog metadata **and** a user table.
///
/// Such a statement routes to the local context, because that is where the emulated
/// `pg_catalog` lives, and the local context can resolve a user table — its providers
/// are registered there so `pg_class` can list them — but it cannot *execute* one. The
/// per-shard scan those providers plan (`RemoteDuckDbScanExec`) is a placeholder that
/// only means something once a core node has been handed it, so executing it in-process
/// fails with an internal error naming DataFusion. Distributing the statement instead is
/// not an option either: the `pg_catalog` half is an in-memory provider the distributed
/// plan cannot carry.
///
/// So the statement is refused by name, which is what this codebase does with a
/// construct it cannot answer correctly. The join has to be written as two statements.
pub(super) fn reject_catalog_join_to_user_data(
    stmt: &Statement,
    catalog: &MetadataCatalog,
) -> PgWireResult<()> {
    let user_table = first_relation(stmt, |relation| {
        if is_metadata(relation, pg_prefixed) {
            return None;
        }
        // A name the metadata catalog knows is user data by definition. A name it does
        // not know is left alone: it may be an `information_schema` relation reached
        // unqualified, and an unresolvable name is the planner's error to report.
        let bare = trailing_identifier(relation)?;
        matches!(catalog.get_table(bare), Ok(Some(_))).then(|| bare.to_string())
    });

    match user_table {
        None => Ok(()),
        Some(table) => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "a query that reads catalog metadata and the table '{table}' in one \
                 statement is not supported: catalog metadata is answered on the \
                 coordinator and '{table}' lives on the storage nodes, so the two cannot \
                 be joined in one plan; query them separately"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::write_path_test_helper::parse_one;
    use super::*;

    /// Stand-in for the set built from the registered `pg_catalog` provider at
    /// startup. Only `pg_*` names appear there in practice.
    fn catalog_names() -> HashSet<String> {
        ["pg_class", "pg_namespace", "pg_type", "pg_attribute"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn routes_local(sql: &str) -> bool {
        references_catalog_schema(&parse_one(sql), &catalog_names())
    }

    #[test]
    fn a_qualified_relation_in_any_catalog_schema_routes_local() {
        assert!(routes_local("SELECT * FROM pg_catalog.pg_class"));
        assert!(routes_local("SELECT * FROM information_schema.tables"));
        assert!(routes_local("SELECT * FROM vairedb_catalog.shards"));
    }

    #[test]
    fn a_catalog_relation_reached_through_a_join_routes_local() {
        assert!(routes_local(
            "SELECT c.relname FROM users u JOIN pg_catalog.pg_class c ON c.oid = u.id"
        ));
    }

    #[test]
    fn a_statement_of_only_user_tables_stays_distributed() {
        assert!(!routes_local("SELECT * FROM foo_table WHERE id = 1"));
        // A user table whose name is not a known catalog table must not match either.
        assert!(!routes_local("SELECT * FROM orders WHERE id = 1"));
    }

    #[test]
    fn a_string_literal_that_looks_like_a_catalog_name_does_not_route() {
        // A user value that merely looks like a catalog prefix must not trigger routing.
        assert!(!routes_local(
            "SELECT * FROM foo_table WHERE name = 'pg_catalog.x'"
        ));
    }

    #[test]
    fn an_unqualified_catalog_relation_routes_local() {
        // Driver introspection often uses unqualified catalog names relying on
        // the search_path; these must still route to the local context.
        assert!(routes_local("SELECT * FROM pg_class"));
        assert!(routes_local(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        ));
    }

    #[test]
    fn the_parse_time_approximation_agrees_on_the_qualified_forms() {
        // `reads_metadata` has no registered set to consult, so only the prefix rule is
        // shared. Where both can answer they must answer the same, or a statement is
        // rewritten for one context and planned for the other.
        for sql in [
            "SELECT * FROM pg_catalog.pg_class",
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM vairedb_catalog.shards",
            "SELECT * FROM pg_class",
        ] {
            assert!(reads_metadata(&parse_one(sql)), "{sql}");
            assert!(routes_local(sql), "{sql}");
        }
        assert!(!reads_metadata(&parse_one("SELECT * FROM orders")));
    }

    // --- the mixed catalog/user-data refusal ---

    /// A metadata catalog holding `orders`, so the refusal has a name to recognize as
    /// user data. Distinct file per call: redb takes an exclusive lock.
    fn catalog_with_orders() -> MetadataCatalog {
        let catalog = crate::catalog::catalog_test_helper::scratch_catalog("catalog_routing");
        catalog
            .put_table(&crate::catalog::catalog_test_helper::table_meta(
                "orders",
                &["id"],
                "id",
            ))
            .expect("the scratch catalog accepts a table");
        catalog
    }

    fn refusal(sql: &str) -> Option<pgwire::error::ErrorInfo> {
        match reject_catalog_join_to_user_data(&parse_one(sql), &catalog_with_orders()) {
            Ok(()) => None,
            Err(pgwire::error::PgWireError::UserError(info)) => Some(*info),
            Err(other) => panic!("expected a client-facing error, got {other}"),
        }
    }

    #[test]
    fn a_join_between_metadata_and_a_user_table_is_refused() {
        let info = refusal(
            "SELECT c.relname, o.id FROM pg_catalog.pg_class c JOIN orders o ON o.id = c.oid",
        )
        .expect("the two halves live on different nodes, so the join cannot be planned");
        assert_eq!(info.code, "0A000", "feature_not_supported");
        assert!(
            info.message.contains("orders"),
            "the message names the table that cannot be reached: {}",
            info.message
        );
    }

    #[test]
    fn a_user_table_in_a_subquery_is_refused_too() {
        // The visitor walks subqueries, so hiding the table one level down changes nothing:
        // it is still the local context that would have to execute the scan.
        assert!(
            refusal("SELECT relname FROM pg_class WHERE oid IN (SELECT id FROM orders)").is_some()
        );
    }

    #[test]
    fn a_pure_catalog_query_is_accepted() {
        assert!(
            refusal(
                "SELECT c.relname FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
            )
            .is_none(),
            "both halves are answered on the coordinator"
        );
    }

    #[test]
    fn an_unknown_relation_is_left_to_the_planner() {
        // `information_schema` relations are reached unqualified too, and the catalog does
        // not know them. Refusing on "not a known user table" would refuse those; an
        // unresolvable name is the planner's error to report, with its own message.
        assert!(refusal("SELECT * FROM pg_class, columns").is_none());
        assert!(refusal("SELECT * FROM pg_class, no_such_table").is_none());
    }
}
