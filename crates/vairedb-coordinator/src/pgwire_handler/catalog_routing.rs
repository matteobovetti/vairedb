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
    stmt: &sqlparser::ast::Statement,
    catalog_names: &HashSet<String>,
) -> bool {
    use std::ops::ControlFlow;
    let mut found = false;
    let mut stmt = stmt.clone();
    let _ = sqlparser::ast::visit_relations_mut(&mut stmt, |relation| {
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
        if let Some(sqlparser::ast::ObjectNamePart::Identifier(ident)) = relation.0.last()
            && catalog_names.contains(&ident.value.to_lowercase())
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_compat;

    fn parse_one(sql: &str) -> sqlparser::ast::Statement {
        sql_compat::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
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
}
