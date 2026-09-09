use std::collections::HashSet;

use datafusion::execution::context::SessionContext;

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

/// Returns true if any relation referenced by the statement targets a catalog
/// schema. Walks relation names via the sqlparser visitor (so it sees joins, subqueries, and
/// CTEs — not just the first FROM table) and only inspects relation identifiers, never string
/// literals, so a user value like `'pg_catalog.foo'` won't trigger a false positive.
///
/// A relation matches when it is schema-qualified with a catalog prefix
/// ([`CATALOG_SCHEMA_PREFIXES`]) or when its bare name is a known `pg_catalog`
/// table in `catalog_names` (so unqualified `pg_class` is caught too).
pub(super) fn references_catalog_schema(
    stmt: &crate::sqlparser::ast::Statement,
    catalog_names: &HashSet<String>,
) -> bool {
    use std::ops::ControlFlow;
    let mut found = false;
    let mut stmt = stmt.clone();
    let _ = crate::sqlparser::ast::visit_relations_mut(&mut stmt, |relation| {
        let name = relation.to_string().to_lowercase();
        if CATALOG_SCHEMA_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            found = true;
            return ControlFlow::Break(());
        }
        // Unqualified bare name (last identifier segment) matching a known
        // pg_catalog table, e.g. `SELECT ... FROM pg_class`.
        if let Some(crate::sqlparser::ast::ObjectNamePart::Identifier(ident)) = relation.0.last()
            && catalog_names.contains(&ident.value.to_lowercase())
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
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
pub(super) fn reads_metadata(stmt: &crate::sqlparser::ast::Statement) -> bool {
    use std::ops::ControlFlow;
    let mut found = false;
    let mut stmt = stmt.clone();
    let _ = crate::sqlparser::ast::visit_relations_mut(&mut stmt, |relation| {
        let name = relation.to_string().to_lowercase();
        if CATALOG_SCHEMA_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            found = true;
            return ControlFlow::Break(());
        }
        if let Some(crate::sqlparser::ast::ObjectNamePart::Identifier(ident)) = relation.0.last()
            && ident.value.to_lowercase().starts_with("pg_")
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
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
    stmt: &crate::sqlparser::ast::Statement,
    catalog: &crate::catalog::MetadataCatalog,
) -> pgwire::error::PgWireResult<()> {
    use std::ops::ControlFlow;

    let mut user_table: Option<String> = None;
    let mut stmt = stmt.clone();
    let _ = crate::sqlparser::ast::visit_relations_mut(&mut stmt, |relation| {
        let name = relation.to_string().to_lowercase();
        if CATALOG_SCHEMA_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            return ControlFlow::Continue(());
        }
        let Some(crate::sqlparser::ast::ObjectNamePart::Identifier(ident)) = relation.0.last()
        else {
            return ControlFlow::Continue(());
        };
        if ident.value.to_lowercase().starts_with("pg_") {
            return ControlFlow::Continue(());
        }
        // A name the metadata catalog knows is user data by definition. A name it does
        // not know is left alone: it may be an `information_schema` relation reached
        // unqualified, and an unresolvable name is the planner's error to report.
        if matches!(catalog.get_table(&ident.value), Ok(Some(_))) {
            user_table = Some(ident.value.clone());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });

    match user_table {
        None => Ok(()),
        Some(table) => Err(crate::pgwire_handler::error_enrichment::make_vdb_error(
            vairedb_common::proto::vairedb::v1::VdbErrorCode::FeatureNotSupported,
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
    use super::*;
    use crate::pgwire_handler::parser;

    fn parse_one(sql: &str) -> crate::sqlparser::ast::Statement {
        parser::parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    /// Stand-in for the set built from the registered `pg_catalog` provider at
    /// startup. Only `pg_*` names appear there in practice.
    fn catalog_names() -> HashSet<String> {
        ["pg_class", "pg_namespace", "pg_type", "pg_attribute"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn test_references_catalog_pg_catalog() {
        let stmt = parse_one("SELECT * FROM pg_catalog.pg_class");
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_information_schema() {
        let stmt = parse_one("SELECT * FROM information_schema.tables");
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_vairedb_catalog() {
        let stmt = parse_one("SELECT * FROM vairedb_catalog.shards");
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_in_join() {
        let stmt =
            parse_one("SELECT c.relname FROM users u JOIN pg_catalog.pg_class c ON c.oid = u.id");
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_plain_user_table_is_false() {
        let stmt = parse_one("SELECT * FROM foo_table WHERE id = 1");
        assert!(!references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_string_literal_not_matched() {
        // A user value that merely looks like a catalog prefix must not trigger routing.
        let stmt = parse_one("SELECT * FROM foo_table WHERE name = 'pg_catalog.x'");
        assert!(!references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_unqualified_pg_class() {
        // Driver introspection often uses unqualified catalog names relying on
        // the search_path; these must still route to the local context.
        let stmt = parse_one("SELECT * FROM pg_class");
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_unqualified_in_join() {
        let stmt = parse_one(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
        );
        assert!(references_catalog_schema(&stmt, &catalog_names()));
    }

    #[test]
    fn test_references_catalog_unqualified_user_table_is_false() {
        // A user table whose name is not a known catalog table must not match.
        let stmt = parse_one("SELECT * FROM orders WHERE id = 1");
        assert!(!references_catalog_schema(&stmt, &catalog_names()));
    }

    // --- the mixed catalog/user-data refusal ---

    /// A metadata catalog holding `orders`, so the refusal has a name to recognize as
    /// user data. Distinct file per call: redb takes an exclusive lock.
    fn catalog_with_orders() -> crate::catalog::MetadataCatalog {
        use crate::catalog::{ColumnDef, TableMeta};

        let catalog = crate::pgwire_handler::test_catalog::scratch_catalog("catalog_routing");
        catalog
            .put_table(&TableMeta {
                table_name: "orders".to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                }],
                shard_key: "id".to_string(),
                shard_count: 2,
                replication_factor: 1,
                ..Default::default()
            })
            .unwrap();
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
    fn test_a_join_between_metadata_and_a_user_table_is_refused() {
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
    fn test_a_user_table_in_a_subquery_is_refused_too() {
        // The visitor walks subqueries, so hiding the table one level down changes nothing:
        // it is still the local context that would have to execute the scan.
        assert!(
            refusal("SELECT relname FROM pg_class WHERE oid IN (SELECT id FROM orders)").is_some()
        );
    }

    #[test]
    fn test_a_pure_catalog_query_is_accepted() {
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
    fn test_an_unknown_relation_is_left_to_the_planner() {
        // `information_schema` relations are reached unqualified too, and the catalog does
        // not know them. Refusing on "not a known user table" would refuse those; an
        // unresolvable name is the planner's error to report, with its own message.
        assert!(refusal("SELECT * FROM pg_class, columns").is_none());
        assert!(refusal("SELECT * FROM pg_class, no_such_table").is_none());
    }
}
