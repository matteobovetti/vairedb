//! Write-path SQL compatibility layer: PostgreSQL wire-protocol SQL → DuckDB,
//! translated directly without DataFusion in the middle. Splits into:
//! - `duckdb_compat` — the two answers to a PostgreSQL/DuckDB divergence: rewrite a
//!   parsed statement to DuckDB-compatible form, and refuse what cannot be rewritten.
//! - `shard_routing` — decide which shard(s) a write targets.
//! - `routing_value` — canonicalize a shard-key value for stable hashing.
//! - `statement` — validate/split/renumber write statements for sharding.
//! - `merge` — the shape rules and rewrites that let a `MERGE INTO` be applied
//!   shard by shard.
//! - `rows` — re-emit materialized result rows as a literal `INSERT ... VALUES`,
//!   so a write whose rows come from a query can be routed like any other.
//!
//! The shard-local relation rewrite, the SQL render, and the small AST reads several
//! submodules share live here as the primitives they build on.
//!
//! Write-path only. The statements are parsed by
//! [`crate::pgwire_handler::parser::parse_sql`], which serves both paths and so lives at
//! the protocol layer alongside the read-path rewrites.

mod duckdb_compat;
mod merge;
mod routing_value;
mod rows;
mod shard_routing;
mod statement;

pub use duckdb_compat::{reject_duckdb_divergent, transform_to_duckdb};
pub use merge::{
    MergeShape, MergeSource, ensure_merge_relation_aliases, materialize_merge_insert_columns,
    merge_has_not_matched_by_source, merge_key_column, merge_row_shard_keys, merge_shape,
    normalize_merge_column_qualifiers, split_merge_by_rows, validate_merge,
};
pub use rows::{ROWS_PER_STATEMENT, insert_statements_from_batches, insert_template};
pub use shard_routing::{ShardRouting, extract_shard_key_value, route_target};
pub use statement::{
    extract_insert_row_shard_keys, insert_omits_column_list, insert_source_is_query,
    insert_source_tables, insert_values_row_count, materialize_insert_columns,
    materialize_insert_columns_for_arity, max_placeholder_index, relations_read,
    renumber_placeholders, split_insert_by_rows, update_targets_shard_key,
    validate_insert_shard_key, validate_on_conflict,
};

use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Statement, visit_relations_mut,
};

use crate::pgwire_handler::query_router::{canonical_table_name, canonicalize_ident};
use crate::util::physical_base_name;

/// Rewrite every relation in `stmt` to its bare shard-local table name so the
/// statement targets the physical per-shard DuckDB table (e.g. `orders` →
/// `orders_shard3`, `sales.orders` → `sales_orders_shard3`).
///
/// The whole (possibly quoted or schema-qualified) relation collapses to one unquoted
/// identifier `{physical_base}_{shard_suffix}`, `physical_base` being
/// [`physical_base_name`] of its canonical catalog key. That keeps the physical name a
/// plain identifier byte-matching `util::shard_table_name` on the same key — the storage
/// node splices it into SQL unquoted, so it may carry neither quotes nor a `.`.
pub fn rewrite_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    let _ = visit_relations_mut(stmt, |relation| {
        if let Some(name) = shard_local_name(relation, shard_suffix) {
            *relation = name;
        }
        ControlFlow::<()>::Continue(())
    });
}

/// The shard-local physical name for `relation`, as the single unquoted identifier
/// `{physical_base}_{shard_suffix}`. `None` when the relation has no canonical
/// catalog key, which leaves it untouched.
///
/// The one place the physical spelling is built, so the two rewrites below cannot
/// drift from each other — or from [`crate::util::shard_table_name`], which has to
/// produce the same bytes for the same key.
fn shard_local_name(relation: &ObjectName, shard_suffix: &str) -> Option<ObjectName> {
    let key = canonical_table_name(relation)?;
    let shard_local = format!("{}_{}", physical_base_name(&key), shard_suffix);
    Some(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
        shard_local,
    ))]))
}

/// Suffix a `CREATE INDEX`'s *own* name with `shard_suffix`, the way
/// [`rewrite_to_shard_local`] suffixes the relations it reads.
///
/// Separate from that rewrite because `CreateIndex.name` is not a relation reference —
/// `visit_relations_mut` reaches `table_name` beside it but never the name itself, so one
/// pass cannot do both. Left as-is, every shard on a node would create an index of the
/// same name and all but the first would collide: N shards, one index.
///
/// Unquoted `{physical_base}_{shard_suffix}` for the same reason
/// [`rewrite_to_shard_local`] uses it, plus one more: the suffix has to be recomputable
/// from the logical name when `DROP INDEX` comes to remove it. A no-op otherwise.
pub fn rewrite_index_name_to_shard_local(stmt: &mut Statement, shard_suffix: &str) {
    if let Statement::CreateIndex(create) = stmt
        && let Some(name) = &create.name
        && let Some(shard_local) = shard_local_name(name, shard_suffix)
    {
        create.name = Some(shard_local);
    }
}

/// Render a statement back to its SQL text form.
///
/// **Write path only.** The last render in the system, and it exists only because the
/// write path ships SQL *text* over gRPC for a core node to run on DuckDB (see
/// `write_router::generate_shard_local_sql`). The read path hands its AST to DataFusion
/// directly, so nothing re-parses this output — which matters, because `Display` is lossy
/// (`x'1F'` renders as `X'1F'`).
pub fn statement_to_sql(stmt: &Statement) -> String {
    stmt.to_string()
}

/// Whether the column `name` refers to is `column`, which is already canonical.
///
/// A qualified reference resolves on its last part, so `t.id` and `ID` both name the
/// column `id` — the comparison the shard key and the MERGE join key are matched by. A
/// dotted *composite field* target is a different thing, excluded where it matters by
/// [`crate::util::insert_column_ident`] rather than here.
fn column_name_is(name: &ObjectName, column: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| canonicalize_ident(ident) == column)
}

/// Strip any number of layers of parentheses, so `(id = 5)` and `((id) = 5)` read as the
/// same expression as `id = 5`.
///
/// Once the AST is built, parentheses carry no meaning — the grouping they expressed *is*
/// the tree shape. A match that does not look through them silently treats a
/// parenthesized statement as an unrecognized one.
fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

/// The positional index a `$N` placeholder binds to, zero-based. `None` for `$0`,
/// which PostgreSQL numbers from 1, and for any other placeholder spelling.
fn placeholder_index(name: &str) -> Option<usize> {
    name.strip_prefix('$')
        .and_then(|digits| digits.parse::<usize>().ok())
        .and_then(|n| n.checked_sub(1))
}

/// `names` as a double-quoted column list — the form an INSERT's column list takes when
/// the coordinator materializes one from the catalog.
///
/// Quoted because a catalog name is already the exact stored spelling: left bare, the
/// shard's parser would fold it, and a column created as `"Email"` would not be found.
fn quoted_column_names(names: &[&str]) -> Vec<ObjectName> {
    names
        .iter()
        .map(|name| ObjectName::from(vec![Ident::with_quote('"', *name)]))
        .collect()
}

/// The single statement `sql` parses to.
///
/// Goes through [`parse_sql`](crate::pgwire_handler::parser::parse_sql), the
/// coordinator's one and only parse, rather than a `Parser` built here — so a test sees
/// the AST production really hands this layer: verbatim for a write or DDL,
/// compat-rewritten for a read. A bespoke parser would let a test pass on an AST shape
/// that never occurs.
#[cfg(test)]
fn parse_one(sql: &str) -> Statement {
    let mut statements = crate::pgwire_handler::parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"));
    assert_eq!(statements.len(), 1, "`{sql}` should be one statement");
    statements.remove(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_survives_shard_rewrite_and_render() {
        let mut stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        rewrite_to_shard_local(&mut stmt, "shard3");
        transform_to_duckdb(&mut stmt);
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("t_shard3"), "got: {sql}");
        assert!(sql.contains("$1") && sql.contains("$2"), "got: {sql}");
    }

    /// A `CREATE INDEX`'s own name is suffixed too, because `visit_relations_mut`
    /// never reaches it — left alone, every shard on a node would try to create an
    /// index of the same name and all but the first would collide.
    #[test]
    fn a_create_index_gets_both_its_table_and_its_own_name_suffixed() {
        let mut stmt = parse_one("CREATE INDEX idx_email ON sales.users (email)");
        rewrite_to_shard_local(&mut stmt, "shard3");
        rewrite_index_name_to_shard_local(&mut stmt, "shard3");
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("idx_email_shard3"), "got: {sql}");
        assert!(sql.contains("sales_users_shard3"), "got: {sql}");
    }

    /// A reference resolves on its last part and folds case the way an unquoted
    /// identifier does, so `t.id` and `ID` both name the shard key `id`.
    #[test]
    fn a_qualified_or_case_folded_reference_names_the_same_column() {
        for parts in [vec!["id"], vec!["ID"], vec!["t", "id"], vec!["t", "ID"]] {
            let name = ObjectName::from(parts.iter().map(|p| Ident::new(*p)).collect::<Vec<_>>());
            assert!(column_name_is(&name, "id"), "{name} should name `id`");
        }
    }

    /// A *quoted* identifier keeps its case, because in PostgreSQL `"ID"` and `id`
    /// are two different columns — folding it here would route a write on the wrong
    /// one.
    #[test]
    fn a_quoted_reference_keeps_its_case() {
        let quoted = ObjectName::from(vec![Ident::with_quote('"', "ID")]);
        assert!(!column_name_is(&quoted, "id"));
        assert!(column_name_is(&quoted, "ID"));
    }

    #[test]
    fn placeholder_index_is_zero_based_and_rejects_other_spellings() {
        assert_eq!(placeholder_index("$1"), Some(0));
        assert_eq!(placeholder_index("$12"), Some(11));
        // PostgreSQL numbers parameters from 1, so `$0` binds to nothing.
        assert_eq!(placeholder_index("$0"), None);
        assert_eq!(placeholder_index("?"), None);
        assert_eq!(placeholder_index(":name"), None);
        assert_eq!(placeholder_index("$x"), None);
    }

    /// A catalog name is the exact stored spelling, so the materialized list quotes
    /// it — bare, a column created as `"Email"` would be folded to `email` by the
    /// shard's parser and not found.
    #[test]
    fn a_materialized_column_list_keeps_the_catalogs_own_spelling() {
        let names = quoted_column_names(&["id", "Email"]);
        assert_eq!(names[0].to_string(), "\"id\"");
        assert_eq!(names[1].to_string(), "\"Email\"");
    }
}
