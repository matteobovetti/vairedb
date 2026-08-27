//! In-place rewriting of INSERT/UPDATE statements: replace the plaintext of each
//! anonymized column with its HMAC-SHA256 hex digest, so the statement that
//! leaves the coordinator carries only finished digests.

use std::collections::HashMap;

use crate::sqlparser::ast::{AssignmentTarget, Expr, SetExpr, Statement, Value};

use super::{HMAC_SHA256_ALGO, Secret, SecretResolver, hmac_sha256_hex};

/// Rewrite `stmt` in place, hashing every value written to a column named in
/// `anonymized_columns` (a map of column name -> secret id). Non-INSERT/UPDATE
/// statements and columns absent from the map are left untouched.
///
/// Returns `Err` with a client-facing message if a referenced secret is missing,
/// declares an unsupported algorithm, or if an anonymized column is given a value
/// that cannot be hashed at rewrite time (a bind placeholder or a non-literal
/// expression). NULL values are preserved as NULL — a hash of "nothing" would be
/// misleading and would defeat nullability.
pub fn anonymize_statement(
    stmt: &mut Statement,
    anonymized_columns: &HashMap<String, String>,
    resolver: &dyn SecretResolver,
) -> Result<(), String> {
    if anonymized_columns.is_empty() {
        return Ok(());
    }

    match stmt {
        Statement::Insert(insert) => {
            // Column names are matched case-insensitively: the map is keyed on
            // lowercased identifiers, so `EMAIL` still resolves to the `email`
            // rule. A case mismatch must never silently skip hashing.
            let target_positions: Vec<(usize, &String)> = insert
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    anonymized_columns
                        .get(&col.value.to_ascii_lowercase())
                        .map(|sid| (idx, sid))
                })
                .collect();

            if target_positions.is_empty() {
                return Ok(());
            }

            // Resolve each distinct secret once, up front, rather than per value:
            // a bulk INSERT of N rows over K anonymized columns would otherwise do
            // N*K catalog reads for the same handful of ids.
            let secrets = resolve_secrets(target_positions.iter().map(|(_, sid)| *sid), resolver)?;

            let Some(source) = insert.source.as_mut() else {
                return Ok(());
            };
            let SetExpr::Values(values) = source.body.as_mut() else {
                // INSERT ... SELECT is rejected earlier for sharded tables; guard
                // here too so a non-VALUES source is never silently un-anonymized.
                return Err("anonymized columns require an INSERT ... VALUES statement".to_string());
            };

            for row in &mut values.rows {
                for (idx, secret_id) in &target_positions {
                    if let Some(expr) = row.get_mut(*idx) {
                        anonymize_expr(expr, &secrets[secret_id.as_str()])?;
                    }
                }
            }
            Ok(())
        }
        Statement::Update(update) => {
            for assignment in &mut update.assignments {
                let col_name = match &assignment.target {
                    AssignmentTarget::ColumnName(name) => {
                        name.0.last().and_then(|p| p.as_ident()).map(|i| &i.value)
                    }
                    AssignmentTarget::Tuple(_) => None,
                };
                if let Some(name) = col_name
                    && let Some(secret_id) = anonymized_columns.get(&name.to_ascii_lowercase())
                {
                    let secret = resolve_secret(secret_id, resolver)?;
                    anonymize_expr(&mut assignment.value, &secret)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Resolve and validate every distinct secret id in `ids` once, returning a map
/// from id to its [`Secret`]. Resolving up front (rather than per value) keeps a
/// bulk INSERT to at-most-K catalog reads instead of one per hashed value.
fn resolve_secrets<'a>(
    ids: impl Iterator<Item = &'a String>,
    resolver: &dyn SecretResolver,
) -> Result<HashMap<&'a str, Secret>, String> {
    let mut secrets: HashMap<&str, Secret> = HashMap::new();
    for id in ids {
        if !secrets.contains_key(id.as_str()) {
            secrets.insert(id.as_str(), resolve_secret(id, resolver)?);
        }
    }
    Ok(secrets)
}

/// Resolve a single secret id and validate its algorithm.
fn resolve_secret(secret_id: &str, resolver: &dyn SecretResolver) -> Result<Secret, String> {
    let secret = resolver.resolve(secret_id).ok_or_else(|| {
        format!(
            "anonymization secret '{secret_id}' not found in vairedb_catalog.anonymization_secret"
        )
    })?;

    if secret.algo != HMAC_SHA256_ALGO {
        return Err(format!(
            "anonymization secret '{secret_id}' declares unsupported algorithm '{}'; only {HMAC_SHA256_ALGO} is supported",
            secret.algo
        ));
    }
    Ok(secret)
}

/// Replace a single literal `expr` with the hex digest of its plaintext, keyed by
/// the already-resolved `secret`. NULLs are left as NULL. Placeholders and
/// non-literal expressions are rejected, since their value is not known at
/// rewrite time and must never reach a node unhashed.
fn anonymize_expr(expr: &mut Expr, secret: &Secret) -> Result<(), String> {
    let plaintext = match expr {
        Expr::Value(v) => match &v.value {
            Value::Null => return Ok(()),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.clone(),
            Value::Boolean(b) => b.to_string(),
            Value::Placeholder(_) => {
                return Err(
                    "anonymized columns cannot be set from a bind parameter; use a literal value"
                        .to_string(),
                );
            }
            other => other.to_string(),
        },
        _ => {
            return Err(
                "anonymized columns must be set to a literal value, not an expression".to_string(),
            );
        }
    };

    let digest = hmac_sha256_hex(&secret.secret_key, &plaintext);
    *expr = Expr::Value(Value::SingleQuotedString(digest).into());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::sql_compat;

    struct StaticResolver {
        secrets: HashMap<String, Secret>,
    }

    impl StaticResolver {
        fn with(id: &str, algo: &str, key: &str) -> Self {
            let mut secrets = HashMap::new();
            secrets.insert(
                id.to_string(),
                Secret {
                    algo: algo.to_string(),
                    secret_key: key.to_string(),
                },
            );
            Self { secrets }
        }

        fn empty() -> Self {
            Self {
                secrets: HashMap::new(),
            }
        }
    }

    impl SecretResolver for StaticResolver {
        fn resolve(&self, secret_id: &str) -> Option<Secret> {
            self.secrets.get(secret_id).cloned()
        }
    }

    fn parse_one(sql: &str) -> Statement {
        sql_compat::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    fn anon_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(c, s)| (c.to_string(), s.to_string()))
            .collect()
    }

    #[test]
    fn insert_hashes_named_columns_only() {
        let mut stmt = parse_one("INSERT INTO t (id, name, email) VALUES (1, 'Alice', 'a@x.com')");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(
            &mut stmt,
            &anon_map(&[("name", "sid"), ("email", "sid")]),
            &resolver,
        )
        .unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);

        let name_digest = hmac_sha256_hex("key", "Alice");
        let email_digest = hmac_sha256_hex("key", "a@x.com");
        assert!(sql.contains(&name_digest), "got: {sql}");
        assert!(sql.contains(&email_digest), "got: {sql}");
        assert!(!sql.contains("Alice"), "plaintext leaked: {sql}");
        assert!(!sql.contains("a@x.com"), "plaintext leaked: {sql}");
        // Non-anonymized column is untouched.
        assert!(sql.contains('1'), "got: {sql}");
    }

    #[test]
    fn insert_multi_row_hashes_every_row() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, 'a@x.com'), (2, 'b@x.com')");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "a@x.com")),
            "got: {sql}"
        );
        assert!(
            sql.contains(&hmac_sha256_hex("key", "b@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("@x.com"), "plaintext leaked: {sql}");
    }

    /// Resolver that records how many times each secret id is resolved, to prove
    /// the rewriter memoizes rather than hitting the catalog per value.
    struct CountingResolver {
        secret: Secret,
        calls: Cell<usize>,
    }

    impl SecretResolver for CountingResolver {
        fn resolve(&self, _secret_id: &str) -> Option<Secret> {
            self.calls.set(self.calls.get() + 1);
            Some(self.secret.clone())
        }
    }

    #[test]
    fn secret_is_resolved_once_per_distinct_id() {
        // Two anonymized columns over three rows = 6 values, all referencing the
        // same secret id. The secret must be resolved exactly once, not per value.
        let mut stmt = parse_one(
            "INSERT INTO t (id, name, email) VALUES \
             (1, 'a', 'a@x.com'), (2, 'b', 'b@x.com'), (3, 'c', 'c@x.com')",
        );
        let resolver = CountingResolver {
            secret: Secret {
                algo: HMAC_SHA256_ALGO.to_string(),
                secret_key: "key".to_string(),
            },
            calls: Cell::new(0),
        };
        anonymize_statement(
            &mut stmt,
            &anon_map(&[("name", "sid"), ("email", "sid")]),
            &resolver,
        )
        .unwrap();
        assert_eq!(resolver.calls.get(), 1, "secret should be resolved once");
        // And every value is still hashed.
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(!sql.contains("@x.com"), "plaintext leaked: {sql}");
    }

    #[test]
    fn insert_column_case_mismatch_still_hashes() {
        // The map is keyed on the lowercased name (as parse_anonymized_columns
        // produces); an INSERT naming the column in a different case must still
        // be hashed, never written as plaintext.
        let mut stmt = parse_one("INSERT INTO t (id, EMAIL) VALUES (1, 'a@x.com')");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "a@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("a@x.com"), "plaintext leaked: {sql}");
    }

    #[test]
    fn update_column_case_mismatch_still_hashes() {
        let mut stmt = parse_one("UPDATE t SET EMAIL = 'new@x.com' WHERE id = 1");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "new@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("new@x.com"), "plaintext leaked: {sql}");
    }

    #[test]
    fn update_hashes_assigned_anonymized_column() {
        let mut stmt = parse_one("UPDATE t SET email = 'new@x.com' WHERE id = 1");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "new@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("new@x.com"), "plaintext leaked: {sql}");
    }

    #[test]
    fn null_value_is_preserved() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, NULL)");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(sql.to_uppercase().contains("NULL"), "got: {sql}");
    }

    #[test]
    fn missing_secret_is_an_error() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, 'a@x.com')");
        let resolver = StaticResolver::empty();
        let err =
            anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
    }

    #[test]
    fn unsupported_algorithm_is_an_error() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, 'a@x.com')");
        let resolver = StaticResolver::with("sid", "SHA1", "key");
        let err =
            anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap_err();
        assert!(err.contains("unsupported algorithm"), "got: {err}");
    }

    #[test]
    fn bind_placeholder_in_anonymized_column_is_rejected() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, $1)");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        let err =
            anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap_err();
        assert!(err.contains("bind parameter"), "got: {err}");
    }

    #[test]
    fn no_anonymized_columns_is_noop() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, 'a@x.com')");
        let resolver = StaticResolver::empty();
        anonymize_statement(&mut stmt, &HashMap::new(), &resolver).unwrap();
        let sql = sql_compat::statement_to_sql(&stmt);
        assert!(sql.contains("a@x.com"), "got: {sql}");
    }
}
