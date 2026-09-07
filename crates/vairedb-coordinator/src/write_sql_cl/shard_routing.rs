//! Decides how a write statement maps onto shards from its shard-key constraint:
//! a single owning shard, a reject (a NULL key, or one not reducible to a value),
//! or a legitimate broadcast. This is the single source of truth for that
//! route/reject/broadcast decision.

use crate::sqlparser::ast::{Expr, SetExpr, Statement};
use datafusion::scalar::ScalarValue;

use crate::pgwire_handler::query_router::canonicalize_ident;

use super::routing_value::{RoutedValue, expr_routing_value};
use super::statement::shard_key_column_index;

/// How a write statement should be routed across shards.
pub enum ShardRouting {
    /// Route to the single shard that owns this (canonicalized) key value.
    One(String),
    /// A shard-key constraint is present but its value is SQL NULL. NULL cannot
    /// be hashed to a shard, so the write must be rejected rather than silently
    /// broadcast (which would duplicate an INSERT across every shard).
    Null,
    /// An INSERT supplies a shard key the coordinator cannot reduce to a value
    /// (a computed expression, a function call, a bind parameter of a type whose
    /// bound and literal forms disagree). The row must be rejected: hashing the
    /// expression's source text would store it on a shard that no lookup of the
    /// same value visits. Carries the client-facing reason.
    Unroutable(String),
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
            let Some(key_idx) = shard_key_column_index(&insert.columns, shard_key) else {
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
                    RoutedValue::Unroutable(reason) => ShardRouting::Unroutable(reason),
                };
            }
            ShardRouting::Broadcast
        }
        Statement::Update(update) => match &update.selection {
            Some(where_clause) => {
                routing_from_equality(extract_equality_from_where(where_clause, shard_key, params))
            }
            None => ShardRouting::Broadcast,
        },
        Statement::Delete(delete) => match &delete.selection {
            Some(where_clause) => {
                routing_from_equality(extract_equality_from_where(where_clause, shard_key, params))
            }
            None => ShardRouting::Broadcast,
        },
        _ => ShardRouting::Broadcast,
    }
}

/// Turn the shard-key value found in an UPDATE/DELETE predicate into a route.
///
/// An unroutable value broadcasts rather than rejecting: each shard re-evaluates
/// the WHERE clause against its own rows, so `DELETE FROM t WHERE id = 1 + 1`
/// applied everywhere removes exactly the right rows — the key just cannot be
/// used to narrow the fan-out. (An INSERT has no predicate to fall back on, which
/// is why [`route_target`] rejects there instead.)
fn routing_from_equality(value: Option<RoutedValue>) -> ShardRouting {
    match value {
        Some(RoutedValue::Value(v)) => ShardRouting::One(v),
        Some(RoutedValue::Null) => ShardRouting::Null,
        Some(RoutedValue::Unroutable(_)) | None => ShardRouting::Broadcast,
    }
}

/// The single routable shard-key value for `stmt`, or `None` when the statement
/// has no usable single-shard key (no constraint, a NULL value, or a value the
/// coordinator cannot hash). Prefer [`route_target`] when those distinctions
/// matter.
pub fn extract_shard_key_value(
    stmt: &Statement,
    shard_key: &str,
    params: &[ScalarValue],
) -> Option<String> {
    match route_target(stmt, shard_key, params) {
        ShardRouting::One(value) => Some(value),
        ShardRouting::Null | ShardRouting::Unroutable(_) | ShardRouting::Broadcast => None,
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
            if matches!(op, crate::sqlparser::ast::BinaryOperator::Eq) {
                // `key_column` is the catalog's canonical name, so fold the
                // client's identifier the same way: `WHERE ID = 1` constrains a
                // shard key declared `id`.
                if let Expr::Identifier(ident) = left.as_ref()
                    && canonicalize_ident(ident) == key_column
                {
                    return Some(expr_routing_value(right, params));
                }
                if let Expr::Identifier(ident) = right.as_ref()
                    && canonicalize_ident(ident) == key_column
                {
                    return Some(expr_routing_value(left, params));
                }
            }
            if matches!(op, crate::sqlparser::ast::BinaryOperator::And) {
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
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

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

    // Identifiers fold like PostgreSQL's: an unquoted `ID` names the canonical
    // `id`. Missing that match does not fail loudly — it falls through to
    // Broadcast, duplicating an INSERT across every shard.
    #[test]
    fn unquoted_shard_key_matches_regardless_of_case() {
        let upper = parse_one("INSERT INTO t (ID, v) VALUES (5, 'a')");
        let mixed = parse_one("INSERT INTO t (Id, v) VALUES (5, 'a')");
        let lower = parse_one("INSERT INTO t (id, v) VALUES (5, 'a')");
        let expected = extract_shard_key_value(&lower, "id", &[]).unwrap();
        assert_eq!(
            extract_shard_key_value(&upper, "id", &[]).unwrap(),
            expected
        );
        assert_eq!(
            extract_shard_key_value(&mixed, "id", &[]).unwrap(),
            expected
        );
    }

    #[test]
    fn unquoted_shard_key_in_where_matches_regardless_of_case() {
        for sql in [
            "DELETE FROM t WHERE ID = 5",
            "DELETE FROM t WHERE Id = 5",
            "UPDATE t SET v = 'x' WHERE ID = 5",
            "UPDATE t SET v = 'x' WHERE a = 1 AND ID = 5",
        ] {
            let stmt = parse_one(sql);
            assert_eq!(
                extract_shard_key_value(&stmt, "id", &[]).as_deref(),
                Some("5"),
                "`{sql}` must route on the shard key, not broadcast"
            );
        }
    }

    // A quoted identifier keeps its case, so `"ID"` is a *different* column from
    // the canonical `id` and must not be mistaken for the shard key.
    #[test]
    fn quoted_shard_key_of_a_different_case_does_not_match() {
        let stmt = parse_one("INSERT INTO t (\"ID\", v) VALUES (5, 'a')");
        assert!(matches!(
            route_target(&stmt, "id", &[]),
            ShardRouting::Broadcast
        ));

        let del = parse_one("DELETE FROM t WHERE \"ID\" = 5");
        assert!(matches!(
            route_target(&del, "id", &[]),
            ShardRouting::Broadcast
        ));
    }

    // A table declared with a quoted mixed-case shard key stores it verbatim, so
    // only the same quoted form matches.
    #[test]
    fn quoted_shard_key_matches_its_own_case() {
        let stmt = parse_one("INSERT INTO t (\"Id\", v) VALUES (5, 'a')");
        assert_eq!(
            extract_shard_key_value(&stmt, "Id", &[]).as_deref(),
            Some("5")
        );
    }

    // --- Unroutable shard keys: reject, never guess from the source text ---
    //
    // Hashing an expression's text puts the row on a shard that no lookup of the
    // equivalent value ever visits, and nothing fails: the INSERT reports success
    // and the row is unreachable. So every form whose value the coordinator
    // cannot determine must be refused.

    #[test]
    fn computed_insert_shard_key_is_unroutable() {
        // `1 + 1` hashed as the text "1 + 1" lands on a different shard from the
        // `2` DuckDB actually stores.
        let stmt = parse_one("INSERT INTO t (id, v) VALUES (1 + 1, 'a')");
        let reason = match route_target(&stmt, "id", &[]) {
            ShardRouting::Unroutable(r) => r,
            _ => panic!("a computed shard key must be refused, not routed on its text"),
        };
        assert!(
            reason.contains("1 + 1"),
            "reason must name the expression: {reason}"
        );
    }

    #[test]
    fn non_constant_insert_shard_keys_are_unroutable() {
        for sql in [
            // Evaluated on the shard, so its value is unknown here.
            "INSERT INTO t (id, v) VALUES (nextval('s'), 'a')",
            // Another column's value; not available at routing time.
            "INSERT INTO t (id, v) VALUES (other_col, 'a')",
            // A subquery result.
            "INSERT INTO t (id, v) VALUES ((SELECT max(id) FROM u), 'a')",
            // A cast may change the value that is stored (`'10.5'::INTEGER`),
            // so the cast's text is not a safe routing key.
            "INSERT INTO t (id, v) VALUES ('10.5'::INTEGER, 'a')",
        ] {
            let stmt = parse_one(sql);
            assert!(
                matches!(route_target(&stmt, "id", &[]), ShardRouting::Unroutable(_)),
                "`{sql}` must be refused rather than routed on its source text"
            );
        }
    }

    // An UPDATE/DELETE carries its predicate to every shard, which re-evaluates it
    // against its own rows — so a broadcast is *correct* there, just unoptimized.
    // Rejecting would be a gratuitous refusal of a statement we can run.
    #[test]
    fn unroutable_predicate_broadcasts_rather_than_rejecting() {
        for sql in [
            "DELETE FROM t WHERE id = 1 + 1",
            "UPDATE t SET v = 'x' WHERE id = other_col",
        ] {
            let stmt = parse_one(sql);
            assert!(
                matches!(route_target(&stmt, "id", &[]), ShardRouting::Broadcast),
                "`{sql}` is correct on every shard, so it must broadcast"
            );
        }
    }

    // A negative literal parses as unary minus over a number, not as one numeric
    // token; it must still route, and route with the param of the same value.
    #[test]
    fn signed_numeric_literal_routes_like_the_param() {
        let literal = parse_one("INSERT INTO t (id, v) VALUES (-1, 'a')");
        let from_literal = extract_shard_key_value(&literal, "id", &[]).unwrap();
        assert_eq!(from_literal, "-1");

        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Int32(Some(-1)),
            ScalarValue::Utf8(Some("a".into())),
        ];
        assert_eq!(
            extract_shard_key_value(&stmt, "id", &params).unwrap(),
            from_literal
        );
    }

    #[test]
    fn parenthesized_literal_routes_like_the_bare_literal() {
        let nested = parse_one("INSERT INTO t (id, v) VALUES ((5), 'a')");
        let bare = parse_one("INSERT INTO t (id, v) VALUES (5, 'a')");
        assert_eq!(
            extract_shard_key_value(&nested, "id", &[]).unwrap(),
            extract_shard_key_value(&bare, "id", &[]).unwrap()
        );
    }

    // A `DATE '...'` literal and a bound Date32 of the same day must agree, so a
    // parameterized INSERT and a literal point lookup find the same shard.
    #[test]
    fn date_param_routes_like_a_date_literal() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Date32(Some(19000)), // 2022-01-08
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (DATE '2022-01-08', 'a')");
        assert_eq!(
            extract_shard_key_value(&literal, "id", &[]).unwrap(),
            from_param
        );
        // The bare-string spelling stores the same DATE value, so it routes there too.
        let bare = parse_one("INSERT INTO t (id, v) VALUES ('2022-01-08', 'a')");
        assert_eq!(
            extract_shard_key_value(&bare, "id", &[]).unwrap(),
            from_param
        );
    }

    // `ScalarValue` Display is not a SQL literal for these types: a timestamp
    // renders as a raw epoch count (a *different* count per TimeUnit), an interval
    // as a Rust struct, binary as hex. No literal of the same value hashes to that
    // string, so binding one as the shard key must be refused, not guessed.
    #[test]
    fn params_whose_literal_form_differs_are_unroutable() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let cases = [
            ScalarValue::TimestampMicrosecond(Some(1641024000000000), None),
            ScalarValue::TimestampNanosecond(Some(1641024000000000000), None),
            ScalarValue::Time64Microsecond(Some(3600000000)),
            ScalarValue::Binary(Some(vec![1, 2, 255])),
            ScalarValue::DurationSecond(Some(90)),
        ];
        for scalar in cases {
            let params = vec![scalar.clone(), ScalarValue::Utf8(Some("a".into()))];
            match route_target(&stmt, "id", &params) {
                ShardRouting::Unroutable(reason) => assert!(
                    reason.contains(&scalar.data_type().to_string()),
                    "reason must name the type: {reason}"
                ),
                _ => panic!(
                    "{:?} must not be routed on its Display form",
                    scalar.data_type()
                ),
            }
        }
    }

    // Under-binding is the client's error; picking a shard anyway would hide it.
    #[test]
    fn an_unbound_shard_key_parameter_is_unroutable() {
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        assert!(matches!(
            route_target(&stmt, "id", &[]),
            ShardRouting::Unroutable(_)
        ));
    }

    #[test]
    fn decimal256_param_matches_literal() {
        use datafusion::arrow::datatypes::i256;
        let stmt = parse_one("INSERT INTO t (id, v) VALUES ($1, $2)");
        let params = vec![
            ScalarValue::Decimal256(Some(i256::from_i128(123456)), 6, 3),
            ScalarValue::Utf8(Some("a".into())),
        ];
        let from_param = extract_shard_key_value(&stmt, "id", &params).unwrap();
        let literal = parse_one("INSERT INTO t (id, v) VALUES (123.456, 'a')");
        assert_eq!(
            extract_shard_key_value(&literal, "id", &[]).unwrap(),
            from_param
        );
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
