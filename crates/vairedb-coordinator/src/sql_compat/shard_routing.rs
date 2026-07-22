//! Decides how a write statement maps onto shards from its shard-key constraint:
//! a single owning shard, a reject (NULL key), or a legitimate broadcast. This is
//! the single source of truth for that route/reject/broadcast decision.

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr, SetExpr, Statement};

use super::routing_value::{RoutedValue, expr_routing_value};

/// How a write statement should be routed across shards.
pub enum ShardRouting {
    /// Route to the single shard that owns this (canonicalized) key value.
    One(String),
    /// A shard-key constraint is present but its value is SQL NULL. NULL cannot
    /// be hashed to a shard, so the write must be rejected rather than silently
    /// broadcast (which would duplicate an INSERT across every shard).
    Null,
    /// No shard-key constraint is present (e.g. `DELETE FROM t` with no WHERE);
    /// the write legitimately applies to every shard.
    Broadcast,
}

/// Resolve how `stmt` should be routed for the given shard key. This is the
/// single source of truth for the route/reject/broadcast decision; both
/// [`extract_shard_key_value`] and the write router build on it.
pub fn route_target(stmt: &Statement, shard_key: &str, params: &[ScalarValue]) -> ShardRouting {
    match stmt {
        Statement::Insert(insert) => {
            let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
            let Some(key_idx) = columns.iter().position(|c| c == shard_key) else {
                return ShardRouting::Broadcast;
            };

            if let Some(source) = &insert.source
                && let SetExpr::Values(values) = source.body.as_ref()
                && let Some(first_row) = values.rows.first()
                && let Some(expr) = first_row.get(key_idx)
            {
                return match expr_routing_value(expr, params) {
                    RoutedValue::Value(v) => ShardRouting::One(v),
                    RoutedValue::Null => ShardRouting::Null,
                };
            }
            ShardRouting::Broadcast
        }
        Statement::Update {
            selection: Some(where_clause),
            ..
        } => routing_from_equality(extract_equality_from_where(where_clause, shard_key, params)),
        Statement::Delete(delete) => match &delete.selection {
            Some(where_clause) => {
                routing_from_equality(extract_equality_from_where(where_clause, shard_key, params))
            }
            None => ShardRouting::Broadcast,
        },
        _ => ShardRouting::Broadcast,
    }
}

fn routing_from_equality(value: Option<RoutedValue>) -> ShardRouting {
    match value {
        Some(RoutedValue::Value(v)) => ShardRouting::One(v),
        Some(RoutedValue::Null) => ShardRouting::Null,
        None => ShardRouting::Broadcast,
    }
}

/// The single routable shard-key value for `stmt`, or `None` when the statement
/// has no usable single-shard key (no constraint, or a NULL value). Prefer
/// [`route_target`] when the NULL-vs-absent distinction matters.
pub fn extract_shard_key_value(
    stmt: &Statement,
    shard_key: &str,
    params: &[ScalarValue],
) -> Option<String> {
    match route_target(stmt, shard_key, params) {
        ShardRouting::One(value) => Some(value),
        ShardRouting::Null | ShardRouting::Broadcast => None,
    }
}

/// Find the routing value of `key_column` within a WHERE clause: matches a direct
/// `key = <expr>` (either operand order) and recurses through `AND` so the key
/// constraint is found among conjuncts. `OR` and other operators yield `None`.
fn extract_equality_from_where(
    expr: &Expr,
    key_column: &str,
    params: &[ScalarValue],
) -> Option<RoutedValue> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                if let Expr::Identifier(ident) = left.as_ref()
                    && ident.value == key_column
                {
                    return Some(expr_routing_value(right, params));
                }
                if let Expr::Identifier(ident) = right.as_ref()
                    && ident.value == key_column
                {
                    return Some(expr_routing_value(left, params));
                }
            }
            if matches!(op, sqlparser::ast::BinaryOperator::And) {
                if let Some(val) = extract_equality_from_where(left, key_column, params) {
                    return Some(val);
                }
                return extract_equality_from_where(right, key_column, params);
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse_sql;
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn shard_key_resolved_from_int_param() {
        // INSERT routes on the placeholder value, matching a literal `5`.
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Int32(Some(5)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (5, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);
    }

    #[test]
    fn shard_key_resolved_from_string_param_matches_literal() {
        let stmt = parse_one("UPDATE t SET v = 'x' WHERE id = $1");
        let params = vec![ScalarValue::Utf8(Some("abc".into()))];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("UPDATE t SET v = 'x' WHERE id = 'abc'");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);
        assert_eq!(from_param, "'abc'");
    }

    #[test]
    fn null_param_is_rejected_not_broadcast() {
        // A NULL-bound shard key on a DELETE is a reject signal, distinct from
        // an absent key (which legitimately broadcasts).
        let stmt = parse_one("DELETE FROM t WHERE id = $1");
        let params = vec![ScalarValue::Int32(None)];
        assert!(matches!(
            route_target(&stmt, "id", &params),
            ShardRouting::Null
        ));
        assert!(extract_shard_key_value(&stmt, "id", &params).is_none());
    }

    #[test]
    fn delete_without_where_is_broadcast() {
        let stmt = parse_one("DELETE FROM t");
        assert!(matches!(
            route_target(&stmt, "id", &[]),
            ShardRouting::Broadcast
        ));
    }

    #[test]
    fn quoted_numeric_key_routes_like_bare_number() {
        // Regression: a numeric shard key written as a quoted string on INSERT
        // (`VALUES ('2', ...)`, as in the docs' example) must route to the same
        // shard as the bare-number form a later DELETE/UPDATE uses
        // (`WHERE id = 2`). Otherwise the point delete targets the wrong shard
        // and silently affects zero rows.
        let insert = parse_one("INSERT INTO t (id, v) VALUES ('2', 'x')");
        let delete = parse_one("DELETE FROM t WHERE id = 2");
        let update = parse_one("UPDATE t SET v = 'y' WHERE id = 2");

        let from_insert = extract_shard_key_value(&insert, "id", &[]).unwrap();
        assert_eq!(from_insert, "2");
        assert_eq!(
            extract_shard_key_value(&delete, "id", &[]).unwrap(),
            from_insert
        );
        assert_eq!(
            extract_shard_key_value(&update, "id", &[]).unwrap(),
            from_insert
        );
    }

    #[test]
    fn float_param_matches_float_literal_shard() {
        // Float64(10.0) param and the literal 10.0 must canonicalize identically
        // so a parameterized write and a literal point lookup hash to one shard.
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Float64(Some(10.0)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (10.0, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);

        // Float32 has the same divergent Display form and must also match.
        let params32 = vec![
            ScalarValue::Float32(Some(10.0)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        assert_eq!(
            extract_shard_key_value(&stmt, "id", &params32).unwrap(),
            from_literal
        );
    }

    #[test]
    fn numeric_forms_canonicalize_equal() {
        for sql in [
            "INSERT INTO t (id, v) VALUES (10, 'a')",
            "INSERT INTO t (id, v) VALUES (10.0, 'a')",
            "INSERT INTO t (id, v) VALUES (10.00, 'a')",
        ] {
            let stmt = parse_one(sql);
            assert_eq!(
                extract_shard_key_value(&stmt, "id", &[]).unwrap(),
                "10",
                "form {sql} should canonicalize to 10"
            );
        }
    }

    #[test]
    fn float_exponent_param_matches_literal() {
        // Float64(1e20) param renders as plain decimal via ScalarValue Display,
        // while the literal 1e20 stays in exponent form — both must route to the
        // same shard after canonicalization.
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Float64(Some(1e20)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (1e20, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);
    }

    #[test]
    fn large_integer_routes_exactly() {
        // 2^53 + 1 cannot be represented exactly in f64, so canonicalization must
        // not round-trip through a float. Param and literal must match byte-exact.
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Int64(Some(9007199254740993)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (9007199254740993, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);
        assert_eq!(from_param, "9007199254740993");
    }

    #[test]
    fn decimal_param_matches_literal() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Decimal128(Some(123456), 6, 3),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (123.456, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_param, from_literal);
    }
}
