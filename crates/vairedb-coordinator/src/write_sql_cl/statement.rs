//! AST-level inspection and rewriting of write statements for sharding: validate
//! that an INSERT carries a usable shard key, extract per-row keys, split a
//! multi-row INSERT by shard, detect shard-key UPDATEs, and renumber positional
//! placeholders so each shard-local statement binds a dense `$1..$k` list.

use std::collections::HashMap;
use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    Assignment, AssignmentTarget, ConflictTarget, Expr, Ident, ObjectName, OnConflictAction,
    OnInsert, SetExpr, Statement, Value, visit_expressions_mut, visit_relations,
};
use datafusion::scalar::ScalarValue;

use crate::pgwire_handler::query_router::{canonical_table_name, canonicalize_ident};

use super::routing_value::{RoutedValue, expr_routing_value};

/// Position of the shard-key column in an INSERT's explicit column list, or
/// `None` when the list does not name it.
///
/// `shard_key` is the catalog's canonical name, so the client's identifiers are
/// folded the same way before comparing: `INSERT INTO t (ID, v)` names the shard
/// key `id`, while `INSERT INTO t ("ID", v)` names a different column.
pub(super) fn shard_key_column_index(columns: &[Ident], shard_key: &str) -> Option<usize> {
    columns
        .iter()
        .position(|c| canonicalize_ident(c) == shard_key)
}

/// True for an `INSERT` that names no columns — the positional form
/// `INSERT INTO t VALUES (…)`, which needs [`materialize_insert_columns`] before
/// anything can locate a column in it by name.
///
/// Cheap on purpose: the caller uses it to decide whether the statement has to be
/// cloned at all, so the common (explicit-list) INSERT stays borrow-only.
pub fn insert_omits_column_list(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Insert(insert) if insert.columns.is_empty())
}

/// True for an `INSERT` whose rows come from a query rather than a `VALUES` list
/// — `INSERT INTO t SELECT …`, a `UNION`, a `WITH … SELECT`, `VALUES` wrapped in
/// a subquery.
///
/// Such a statement carries no shard key the coordinator can read, so it cannot
/// be routed as written; the write path runs the query first and re-emits its
/// rows as literals (see [`super::insert_statements_from_batches`]). `DEFAULT
/// VALUES` has no source at all and is not this case.
pub fn insert_source_is_query(stmt: &Statement) -> bool {
    let Statement::Insert(insert) = stmt else {
        return false;
    };
    insert
        .source
        .as_ref()
        .is_some_and(|source| !matches!(source.body.as_ref(), SetExpr::Values(_)))
}

/// Canonical names of every relation an `INSERT`'s source query reads, deduped in
/// first-encounter order. Empty for an `INSERT ... VALUES` and for
/// `DEFAULT VALUES`, neither of which reads anything.
///
/// Every relation, not just the first `FROM`: a join or a subquery reads its
/// tables just as much, and the caller's question — does this statement read a
/// table whose writes are still buffered? — is wrong if any of them is missed.
pub fn insert_source_tables(stmt: &Statement) -> Vec<String> {
    let Statement::Insert(insert) = stmt else {
        return Vec::new();
    };
    let Some(source) = insert.source.as_ref() else {
        return Vec::new();
    };
    relations_read(source.as_ref())
}

/// Canonical names of every relation a query (or any AST node containing one)
/// reads, deduped in first-encounter order.
///
/// Every relation, not just the first `FROM`: a join or a subquery reads its
/// tables just as much, and the caller's question — does this statement read a
/// table whose writes are still buffered? — is wrong if any of them is missed.
pub fn relations_read<V: crate::sqlparser::ast::Visit>(node: &V) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let _ = visit_relations(node, |relation| {
        if let Some(name) = canonical_table_name(relation)
            && !names.contains(&name)
        {
            names.push(name);
        }
        ControlFlow::<()>::Continue(())
    });
    names
}

/// Fill in the implicit column list of a positional `INSERT INTO t VALUES (…)`
/// from `table_columns`, the table's columns in declaration order.
///
/// PostgreSQL matches a positional row to the leading columns of the table, so
/// this makes that mapping explicit in the AST and every later step — the
/// shard-key check, `ON CONFLICT` validation, the anonymization rewrite, the
/// per-shard row split — works on the one code path it already has, keyed by
/// name. Without it the shard key is unlocatable and the row would be broadcast
/// to every shard (or, for an anonymized column, shipped as plaintext).
///
/// A row shorter than the table takes the *first* `n` columns, matching
/// PostgreSQL: the rest are left to their defaults. If the shard key is not among
/// them, [`validate_insert_shard_key`] rejects the statement — a column filled by
/// default has no value here to hash.
///
/// Names are emitted quoted because `table_columns` holds catalog-canonical names
/// (see [`canonicalize_ident`]), which is exactly how the column is spelled in the
/// per-shard DuckDB table; quoting stops DuckDB folding a name that survived
/// `CREATE TABLE` with its case intact.
///
/// A no-op for anything that is not a positional `INSERT ... VALUES`, and for a
/// table whose catalog entry lists no columns (nothing to map onto — the caller's
/// existing shard-key error is a better report than a guess). Returns
/// `Err(message)` for a row list that cannot be mapped positionally at all.
pub fn materialize_insert_columns(
    stmt: &mut Statement,
    table_columns: &[&str],
) -> std::result::Result<(), String> {
    let arity = {
        let Statement::Insert(insert) = &*stmt else {
            return Ok(());
        };
        if !insert.columns.is_empty() {
            return Ok(());
        }
        // `INSERT ... SELECT` and `DEFAULT VALUES` carry no VALUES rows to count
        // positions in. The former resolves its arity from the source query's
        // schema instead ([`materialize_insert_columns_for_arity`]); the latter is
        // rejected downstream by name.
        let Some(source) = insert.source.as_ref() else {
            return Ok(());
        };
        let SetExpr::Values(values) = source.body.as_ref() else {
            return Ok(());
        };
        let Some(arity) = values.rows.first().map(Vec::len) else {
            return Ok(());
        };
        if values.rows.iter().any(|row| row.len() != arity) {
            return Err("VALUES lists must all be the same length".to_string());
        }
        arity
    };

    materialize_insert_columns_for_arity(stmt, table_columns, arity)
}

/// Fill in the implicit column list of an `INSERT` with the first `arity` of
/// `table_columns`, where `arity` is the number of values each row supplies.
///
/// The mapping [`materialize_insert_columns`] makes for a `VALUES` list, for a
/// row count that comes from somewhere else: an `INSERT ... SELECT` takes its
/// arity from the source query's result schema, which is known before a single
/// row is fetched (so a mismatch is reported even for an empty result).
///
/// A no-op when the statement already names its columns, when the catalog lists
/// no columns for the table, and for `arity` 0 — in each case there is nothing to
/// map, and the caller's existing shard-key error reports it better than a guess.
pub fn materialize_insert_columns_for_arity(
    stmt: &mut Statement,
    table_columns: &[&str],
    arity: usize,
) -> std::result::Result<(), String> {
    let Statement::Insert(insert) = stmt else {
        return Ok(());
    };
    if !insert.columns.is_empty() || table_columns.is_empty() || arity == 0 {
        return Ok(());
    }
    if arity > table_columns.len() {
        return Err(format!(
            "INSERT has more expressions than target columns: {arity} values for a table with {} columns",
            table_columns.len()
        ));
    }

    insert.columns = table_columns[..arity]
        .iter()
        .map(|name| Ident::with_quote('"', *name))
        .collect();
    Ok(())
}

/// Extract the `(row_index, routing_value)` pair for every row of a multi-row
/// INSERT, so the caller can bucket rows by shard. Returns `None` for any
/// statement that is not an `INSERT ... VALUES` naming the shard key, or whose
/// rows do not *all* resolve to a routable key.
///
/// All-or-nothing on purpose: a row dropped from the returned list would be
/// dropped from the shard split too, so the INSERT would silently store fewer
/// rows than the client sent. `None` sends the caller to the whole-statement
/// route instead, which reports the reason.
/// [`validate_insert_shard_key`] rejects those statements up front, so in
/// practice this only guards against a caller skipping that check.
pub fn extract_insert_row_shard_keys(
    stmt: &Statement,
    shard_key: &str,
    params: &[ScalarValue],
) -> Option<Vec<(usize, String)>> {
    let Statement::Insert(insert) = stmt else {
        return None;
    };

    let key_idx = shard_key_column_index(&insert.columns, shard_key)?;

    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };

    let mut result = Vec::with_capacity(values.rows.len());
    for (row_idx, row) in values.rows.iter().enumerate() {
        let RoutedValue::Value(value) = expr_routing_value(row.get(key_idx)?, params) else {
            return None;
        };
        result.push((row_idx, value));
    }

    Some(result)
}

/// Number of rows an `INSERT ... VALUES` inserts, known from the statement text
/// alone. `None` for any other statement — including `INSERT ... SELECT`, whose
/// row count only the shards can report.
///
/// The one row count the coordinator may state before a statement runs, which is
/// what lets a buffered INSERT inside a transaction block report a truthful tag.
/// So the answer must be *exact*, not a good guess: `ON CONFLICT` can drop rows
/// and `RETURNING` owes the client the rows themselves, so both give up the count
/// rather than overstate it.
pub fn insert_values_row_count(stmt: &Statement) -> Option<usize> {
    let Statement::Insert(insert) = stmt else {
        return None;
    };
    if insert.on.is_some() || insert.returning.is_some() {
        return None;
    }
    let SetExpr::Values(values) = insert.source.as_ref()?.body.as_ref() else {
        return None;
    };
    Some(values.rows.len())
}

/// Validate that an INSERT supplies a usable, non-NULL shard key for every row.
/// Returns `Err(message)` describing why the statement must be rejected; `Ok(())`
/// when the INSERT can be routed. Requires a column list naming the shard key with
/// a non-NULL value in each row; `INSERT ... SELECT` cannot be verified by name
/// and is rejected.
///
/// The column list may be the client's or one resolved from the catalog by
/// [`materialize_insert_columns`], which runs first — so the empty-list branch
/// below is a backstop for a table whose catalog entry lists no columns, not the
/// verdict on a positional INSERT.
pub fn validate_insert_shard_key(
    stmt: &Statement,
    shard_key: &str,
    params: &[ScalarValue],
) -> std::result::Result<(), String> {
    let Statement::Insert(insert) = stmt else {
        return Ok(());
    };

    if insert.columns.is_empty() {
        return Err(format!(
            "INSERT must specify an explicit column list including shard key \"{shard_key}\""
        ));
    }

    let Some(key_idx) = shard_key_column_index(&insert.columns, shard_key) else {
        return Err(format!(
            "INSERT must specify a value for shard key column \"{shard_key}\""
        ));
    };

    let Some(source) = &insert.source else {
        return Err(format!(
            "INSERT must specify a value for shard key column \"{shard_key}\""
        ));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(format!(
            "INSERT ... SELECT is not supported for sharded tables; specify a non-NULL value for shard key column \"{shard_key}\""
        ));
    };

    for row in &values.rows {
        let Some(expr) = row.get(key_idx) else {
            return Err(format!(
                "INSERT must specify a value for shard key column \"{shard_key}\""
            ));
        };
        match expr_routing_value(expr, params) {
            RoutedValue::Value(_) => {}
            // Catches both a literal `NULL` and a `$N` placeholder bound to a
            // NULL parameter; either would otherwise route nowhere and be
            // broadcast (duplicating the row across every shard).
            RoutedValue::Null => {
                return Err(format!("shard key column \"{shard_key}\" cannot be NULL"));
            }
            // The row's shard is not computable here, so storing it would mean
            // guessing — and a guess is unrecoverable: no lookup of the same
            // value would ever visit the shard the guess picked.
            RoutedValue::Unroutable(reason) => {
                return Err(format!(
                    "cannot route INSERT on shard key column \"{shard_key}\": {reason}"
                ));
            }
        }
    }

    Ok(())
}

/// Returns `true` if an UPDATE assigns a new value to the shard-key column.
/// v0.1 does not support relocating a row to a different shard.
pub fn update_targets_shard_key(stmt: &Statement, shard_key: &str) -> bool {
    let Statement::Update(update) = stmt else {
        return false;
    };
    assignments_target_shard_key(&update.assignments, shard_key)
}

/// True if any of `assignments` writes to the shard-key column. Shared by the
/// UPDATE guard, the `ON CONFLICT … DO UPDATE` guard and the MERGE `WHEN MATCHED
/// … UPDATE SET` guard: each would move the row to a shard the router did not
/// write it to, and the row is only ever looked for on the shard its key hashes to.
pub(super) fn assignments_target_shard_key(assignments: &[Assignment], shard_key: &str) -> bool {
    assignments
        .iter()
        .any(|assignment| match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name_matches(name, shard_key),
            AssignmentTarget::Tuple(names) => names
                .iter()
                .any(|name| object_name_matches(name, shard_key)),
        })
}

/// Validate an INSERT's `ON CONFLICT` clause against the shard key.
///
/// The UNIQUE/PRIMARY KEY index that resolves a conflict exists once *per shard*,
/// so each shard only ever detects a conflict among the rows it holds. That is
/// globally correct exactly when every row sharing the arbiter value lands on the
/// same shard — which is guaranteed only if the arbiter includes the shard key.
/// An arbiter on any other column quietly degrades to per-shard uniqueness: the
/// same "unique" value inserted twice under different shard keys yields two rows
/// and no error, so the upsert the client asked for did not happen.
///
/// An untargeted `ON CONFLICT` (no column list, no constraint name) is left
/// alone: it resolves against whatever unique index the shard has, which is
/// correct whenever that index is on the shard key — the only kind VaireDB can
/// enforce globally in the first place.
///
/// Returns `Err(message)` describing why the statement must be rejected.
pub fn validate_on_conflict(stmt: &Statement, shard_key: &str) -> std::result::Result<(), String> {
    let Statement::Insert(insert) = stmt else {
        return Ok(());
    };
    let Some(on_insert) = &insert.on else {
        return Ok(());
    };

    let on_conflict = match on_insert {
        OnInsert::OnConflict(on_conflict) => on_conflict,
        // MySQL's `ON DUPLICATE KEY UPDATE` names no key, so there is nothing to
        // check it against.
        _ => {
            return Err("ON DUPLICATE KEY UPDATE is not supported by VaireDB; use \
                 ON CONFLICT (<shard key>) DO UPDATE"
                .to_string());
        }
    };

    match &on_conflict.conflict_target {
        Some(ConflictTarget::Columns(columns))
            if !columns.iter().any(|c| canonicalize_ident(c) == shard_key) =>
        {
            let named: Vec<String> = columns.iter().map(canonicalize_ident).collect();
            return Err(format!(
                "ON CONFLICT ({}) cannot be honored: the conflict target must include \
                 shard key column \"{shard_key}\". A unique index exists once per shard, \
                 so rows with different shard keys never conflict with each other and \
                 the upsert would silently insert a duplicate instead",
                named.join(", ")
            ));
        }
        // The catalog does record a table's constraints, but the shard-local
        // rewrite is handed the shard key alone — so which columns a named
        // constraint covers, and therefore whether it includes the shard key, is
        // not knowable here. Resolving the name would mean plumbing the table's
        // constraints this far down *and* rewriting the clause into the column
        // list the shards' engine understands, since it has no
        // `ON CONFLICT ON CONSTRAINT` of its own.
        Some(ConflictTarget::OnConstraint(name)) => {
            return Err(format!(
                "ON CONFLICT ON CONSTRAINT {name} is not supported by VaireDB; name the \
                 conflicting columns instead, including shard key column \"{shard_key}\""
            ));
        }
        // An arbiter that does name the shard key, or none at all.
        Some(ConflictTarget::Columns(_)) | None => {}
    }

    if let OnConflictAction::DoUpdate(do_update) = &on_conflict.action
        && assignments_target_shard_key(&do_update.assignments, shard_key)
    {
        return Err(format!(
            "ON CONFLICT ... DO UPDATE cannot assign shard key column \"{shard_key}\"; \
             relocating a row to a different shard is not supported in v0.1"
        ));
    }

    Ok(())
}

fn object_name_matches(name: &ObjectName, shard_key: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| canonicalize_ident(ident) == shard_key)
}

/// Build a new INSERT containing only the VALUES rows at `row_indices`,
/// preserving the original column list and query options. Returns `None` if
/// `stmt` is not an `INSERT ... VALUES` or no rows are selected. Used to send
/// each shard only the rows it owns.
pub fn split_insert_by_rows(stmt: &Statement, row_indices: &[usize]) -> Option<Statement> {
    let Statement::Insert(insert) = stmt else {
        return None;
    };

    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };

    let selected_rows: Vec<Vec<Expr>> = row_indices
        .iter()
        .filter_map(|&idx| values.rows.get(idx).cloned())
        .collect();

    if selected_rows.is_empty() {
        return None;
    }

    let mut new_insert = insert.clone();
    let new_values = crate::sqlparser::ast::Values {
        rows: selected_rows,
        ..values.clone()
    };
    let new_source = crate::sqlparser::ast::Query {
        body: Box::new(SetExpr::Values(new_values)),
        ..source.as_ref().clone()
    };
    new_insert.source = Some(Box::new(new_source));

    Some(Statement::Insert(new_insert))
}

/// Renumber positional placeholders in `stmt` to a contiguous `$1..$k` sequence
/// in first-encounter order, returning the original (1-based) indices in that
/// order. Used after splitting a multi-row INSERT by shard: DuckDB binds
/// parameters positionally, so each shard-local statement must carry a dense,
/// correctly-ordered parameter list. Returns `None` if any placeholder is
/// malformed.
pub fn renumber_placeholders(stmt: &mut Statement) -> Option<Vec<usize>> {
    let mut order: Vec<usize> = Vec::new();
    let mut mapping: HashMap<usize, usize> = HashMap::new();
    let mut malformed = false;

    let _ = visit_expressions_mut(stmt, |expr| {
        if let Expr::Value(v) = expr
            && let Value::Placeholder(name) = &v.value
        {
            match name.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()) {
                Some(orig) => {
                    let new_idx = *mapping.entry(orig).or_insert_with(|| {
                        order.push(orig);
                        order.len()
                    });
                    v.value = Value::Placeholder(format!("${new_idx}"));
                }
                None => malformed = true,
            }
        }
        ControlFlow::<()>::Continue(())
    });

    if malformed {
        return None;
    }
    // Return original zero-based indices in dense order.
    Some(order.into_iter().map(|n| n - 1).collect())
}

/// The number of distinct positional placeholders (`$1..$N`) in a statement,
/// taken as the highest 1-based index seen. Used to report a parameter count at
/// Describe for write statements that DataFusion cannot logical-plan (so no
/// inferred types are available) — the client still needs the right count.
pub fn max_placeholder_index(stmt: &Statement) -> usize {
    let mut stmt = stmt.clone();
    let mut max = 0usize;
    let _ = visit_expressions_mut(&mut stmt, |expr| {
        if let Expr::Value(v) = expr
            && let Value::Placeholder(name) = &v.value
            && let Some(n) = name.strip_prefix('$').and_then(|d| d.parse::<usize>().ok())
        {
            max = max.max(n);
        }
        ControlFlow::<()>::Continue(())
    });
    max
}

#[cfg(test)]
mod tests {
    use super::super::statement_to_sql;
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn null_bound_insert_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Int32(None),
            ScalarValue::Utf8(Some("x".into())),
        ];
        assert!(validate_insert_shard_key(&stmt, "id", &params).is_err());
    }

    #[test]
    fn max_placeholder_index_counts_highest() {
        let stmt = parse_one("INSERT INTO t (a, b, c) VALUES ($1, $3, $2)");
        assert_eq!(max_placeholder_index(&stmt), 3);
        let none = parse_one("INSERT INTO t (a) VALUES (1)");
        assert_eq!(max_placeholder_index(&none), 0);
    }

    #[test]
    fn renumber_placeholders_makes_contiguous() {
        // Simulate a split that retains only the second VALUES row ($3,$4).
        let mut stmt = parse_one("INSERT INTO t (id, v) VALUES ($3, $4)");
        let orig = renumber_placeholders(&mut stmt).unwrap();
        assert_eq!(orig, vec![2, 3]); // zero-based originals
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("$1") && sql.contains("$2"), "got: {sql}");
        assert!(!sql.contains("$3") && !sql.contains("$4"), "got: {sql}");
    }

    #[test]
    fn insert_with_shard_key_value_is_ok() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (1, 'a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_ok());
    }

    #[test]
    fn insert_omitting_shard_key_is_rejected() {
        let stmt = parse_one("INSERT INTO t (v) VALUES ('a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    // On its own this check cannot place a positional INSERT — the shard key is
    // not locatable without a column list. `materialize_insert_columns` supplies
    // one from the catalog first; reaching here without a list means the table's
    // catalog entry had no columns to supply.
    #[test]
    fn positional_insert_is_rejected_when_its_columns_were_not_resolved() {
        let stmt = parse_one("INSERT INTO t VALUES (1, 'a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    #[test]
    fn insert_null_shard_key_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (NULL, 'a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    #[test]
    fn insert_null_shard_key_in_one_of_many_rows_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (1, 'a'), (NULL, 'b')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    #[test]
    fn insert_select_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) SELECT id, v FROM other");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    // An INSERT whose shard key is not a constant cannot be placed: hashing the
    // expression's text would store the row on a shard no lookup of the stored
    // value visits, and the INSERT would still report success.
    #[test]
    fn insert_with_a_non_constant_shard_key_is_rejected() {
        for sql in [
            "INSERT INTO t (id, v) VALUES (1 + 1, 'a')",
            "INSERT INTO t (id, v) VALUES (nextval('s'), 'a')",
            "INSERT INTO t (id, v) VALUES (1, 'a'), (2 * 3, 'b')",
        ] {
            let err = validate_insert_shard_key(&parse_one(sql), "id", &[])
                .expect_err("`{sql}` must be refused");
            assert!(
                err.contains("cannot route INSERT"),
                "`{sql}` must be refused as unroutable, got: {err}"
            );
        }
    }

    #[test]
    fn insert_with_an_unbound_shard_key_parameter_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    // The split keys drive which rows each shard receives, so a row missing from
    // the list is a row dropped from the INSERT. All-or-nothing: one unroutable
    // row makes the whole statement fall back to the reporting path.
    #[test]
    fn row_keys_are_all_or_nothing_so_no_row_is_silently_dropped() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (1, 'a'), (2 + 2, 'b'), (3, 'c')");
        assert!(extract_insert_row_shard_keys(&stmt, "id", &[]).is_none());

        let routable = parse_one("INSERT INTO t (id, v) VALUES (1, 'a'), (4, 'b'), (3, 'c')");
        assert_eq!(
            extract_insert_row_shard_keys(&routable, "id", &[])
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn update_setting_shard_key_is_detected() {
        let stmt = parse_one("UPDATE t SET id = 2 WHERE id = 1");
        assert!(update_targets_shard_key(&stmt, "id"));
    }

    #[test]
    fn update_setting_other_column_is_allowed() {
        let stmt = parse_one("UPDATE t SET v = 'x' WHERE id = 1");
        assert!(!update_targets_shard_key(&stmt, "id"));
    }

    // The shard-key UPDATE guard is what stops a row silently landing on the
    // wrong shard, so it must fire regardless of the case the client used.
    #[test]
    fn update_setting_shard_key_is_detected_regardless_of_case() {
        for sql in [
            "UPDATE t SET ID = 2 WHERE id = 1",
            "UPDATE t SET Id = 2 WHERE id = 1",
        ] {
            let stmt = parse_one(sql);
            assert!(
                update_targets_shard_key(&stmt, "id"),
                "`{sql}` relocates the shard key and must be caught"
            );
        }
    }

    // A quoted `"ID"` is a distinct column from the canonical `id`, so assigning
    // it does not relocate the row.
    #[test]
    fn update_setting_a_quoted_different_case_column_is_not_the_shard_key() {
        let stmt = parse_one("UPDATE t SET \"ID\" = 2 WHERE id = 1");
        assert!(!update_targets_shard_key(&stmt, "id"));
    }

    #[test]
    fn insert_naming_the_shard_key_in_another_case_is_accepted() {
        let stmt = parse_one("INSERT INTO t (ID, v) VALUES (1, 'a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_ok());
    }

    #[test]
    fn insert_naming_a_quoted_different_case_column_is_rejected() {
        let stmt = parse_one("INSERT INTO t (\"ID\", v) VALUES (1, 'a')");
        assert!(validate_insert_shard_key(&stmt, "id", &[]).is_err());
    }

    // --- ON CONFLICT: the arbiter must be one a single shard can decide ---

    #[test]
    fn on_conflict_targeting_the_shard_key_is_accepted() {
        for sql in [
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING",
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = 'b'",
            // A composite arbiter is fine as long as it includes the shard key:
            // equal arbiter tuples then imply equal shard keys, hence one shard.
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (v, id) DO UPDATE SET v = 'b'",
            // Folded like PostgreSQL folds it: unquoted `ID` names `id`.
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (ID) DO NOTHING",
            // No arbiter named: the shard resolves it against its own unique
            // index, which is correct when that index is on the shard key.
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT DO NOTHING",
            // No ON CONFLICT at all.
            "INSERT INTO t (id, v) VALUES (1, 'a')",
        ] {
            assert!(
                validate_on_conflict(&parse_one(sql), "id").is_ok(),
                "`{sql}` should be accepted"
            );
        }
    }

    // The upsert the client asked for would not happen: two rows with the same
    // `v` but different `id` hash to different shards, neither sees the other's
    // row, and both INSERTs succeed.
    #[test]
    fn on_conflict_without_the_shard_key_is_rejected() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (v) DO NOTHING");
        let err = validate_on_conflict(&stmt, "id").expect_err("must be refused");
        assert!(
            err.contains("must include shard key column \"id\""),
            "the error should say what to change, got: {err}"
        );
    }

    // A quoted `"ID"` is a different column from the canonical `id`, so it does
    // not satisfy the requirement.
    #[test]
    fn on_conflict_on_a_quoted_different_case_column_is_rejected() {
        let stmt =
            parse_one("INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (\"ID\") DO NOTHING");
        assert!(validate_on_conflict(&stmt, "id").is_err());
    }

    // The catalog models columns, not constraints, so the coordinator cannot tell
    // whether a named constraint covers the shard key.
    #[test]
    fn on_conflict_on_constraint_is_rejected() {
        let stmt = parse_one(
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
        );
        let err = validate_on_conflict(&stmt, "id").expect_err("must be refused");
        assert!(err.contains("ON CONSTRAINT"), "got: {err}");
    }

    // DO UPDATE assigning the shard key relocates the row, exactly like a plain
    // UPDATE of it — and the guard for that only inspects `Statement::Update`.
    #[test]
    fn on_conflict_do_update_assigning_the_shard_key_is_rejected() {
        for sql in [
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET id = 2",
            "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = 'b', ID = 2",
        ] {
            let err = validate_on_conflict(&parse_one(sql), "id")
                .expect_err("assigning the shard key must be refused");
            assert!(err.contains("shard key"), "`{sql}` gave: {err}");
        }
    }

    #[test]
    fn update_setting_shard_key_among_others_is_detected() {
        let stmt = parse_one("UPDATE t SET v = 'x', id = 2 WHERE id = 1");
        assert!(update_targets_shard_key(&stmt, "id"));
    }

    #[test]
    fn insert_values_rows_are_counted_from_the_statement() {
        assert_eq!(
            insert_values_row_count(&parse_one("INSERT INTO t (id) VALUES (1)")),
            Some(1)
        );
        assert_eq!(
            insert_values_row_count(&parse_one("INSERT INTO t (id) VALUES (1), (2), (3)")),
            Some(3)
        );
    }

    // A buffered INSERT reports this count *before* it runs, so anything that can
    // make the real count differ has to give up the count instead of overstating
    // it — a client that branches on rows-affected would act on the lie.
    #[test]
    fn a_row_count_that_cannot_be_promised_is_not_reported() {
        for sql in [
            "INSERT INTO t (id) SELECT id FROM u",
            "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING",
            "INSERT INTO t (id) VALUES (1) RETURNING id",
            "UPDATE t SET v = 'x' WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
        ] {
            assert_eq!(
                insert_values_row_count(&parse_one(sql)),
                None,
                "`{sql}` must not report a row count"
            );
        }
    }
}
