//! AST-level inspection and rewriting of write statements for sharding: validate
//! that an INSERT carries a usable shard key, extract per-row keys, split a
//! multi-row INSERT by shard, detect shard-key UPDATEs, and renumber positional
//! placeholders so each shard-local statement binds a dense `$1..$k` list.

use std::collections::HashMap;
use std::ops::ControlFlow;

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{
    AssignmentTarget, Expr, ObjectName, SetExpr, Statement, Value, visit_expressions_mut,
};

use super::routing_value::{RoutedValue, expr_routing_value};

/// Extract the `(row_index, routing_value)` pair for every row of a multi-row
/// INSERT, so the caller can bucket rows by shard. Returns `None` for any
/// statement that is not an `INSERT ... VALUES` naming the shard key. NULL-keyed
/// rows are skipped (they are rejected earlier by [`validate_insert_shard_key`]).
pub fn extract_insert_row_shard_keys(
    stmt: &Statement,
    shard_key: &str,
    params: &[ScalarValue],
) -> Option<Vec<(usize, String)>> {
    let Statement::Insert(insert) = stmt else {
        return None;
    };

    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let key_idx = columns.iter().position(|c| c == shard_key)?;

    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };

    // A NULL-keyed row is rejected up front by `validate_insert_shard_key`, so
    // by the time we split rows here every key resolves to a concrete value.
    let mut result = Vec::with_capacity(values.rows.len());
    for (row_idx, row) in values.rows.iter().enumerate() {
        if let Some(expr) = row.get(key_idx)
            && let RoutedValue::Value(value) = expr_routing_value(expr, params)
        {
            result.push((row_idx, value));
        }
    }

    Some(result)
}

/// Validate that an INSERT supplies a usable, non-NULL shard key for every row.
/// Returns `Err(message)` describing why the statement must be rejected; `Ok(())`
/// when the INSERT can be routed. v0.1 requires an explicit column list naming the
/// shard key with a non-NULL value in each row — positional inserts and
/// `INSERT ... SELECT` cannot be verified by name and are rejected.
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

    let columns: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();
    let Some(key_idx) = columns.iter().position(|c| *c == shard_key) else {
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
        match row.get(key_idx) {
            // Catches both a literal `NULL` and a `$N` placeholder bound to a
            // NULL parameter; either would otherwise route nowhere and be
            // broadcast (duplicating the row across every shard).
            Some(expr) if matches!(expr_routing_value(expr, params), RoutedValue::Null) => {
                return Err(format!("shard key column \"{shard_key}\" cannot be NULL"));
            }
            None => {
                return Err(format!(
                    "INSERT must specify a value for shard key column \"{shard_key}\""
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

/// Returns `true` if an UPDATE assigns a new value to the shard-key column.
/// v0.1 does not support relocating a row to a different shard.
pub fn update_targets_shard_key(stmt: &Statement, shard_key: &str) -> bool {
    let Statement::Update { assignments, .. } = stmt else {
        return false;
    };

    assignments
        .iter()
        .any(|assignment| match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name_matches(name, shard_key),
            AssignmentTarget::Tuple(names) => names
                .iter()
                .any(|name| object_name_matches(name, shard_key)),
        })
}

fn object_name_matches(name: &ObjectName, shard_key: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| ident.value == shard_key)
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
    let new_values = sqlparser::ast::Values {
        explicit_row: values.explicit_row,
        rows: selected_rows,
    };
    let new_source = sqlparser::ast::Query {
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
    use super::super::{parse_sql, statement_to_sql};
    use super::*;

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

    #[test]
    fn positional_insert_is_rejected() {
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

    #[test]
    fn update_setting_shard_key_among_others_is_detected() {
        let stmt = parse_one("UPDATE t SET v = 'x', id = 2 WHERE id = 1");
        assert!(update_targets_shard_key(&stmt, "id"));
    }
}
