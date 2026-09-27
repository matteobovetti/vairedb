//! In-place rewriting of INSERT/UPDATE statements: replace the plaintext of each
//! anonymized column with its HMAC-SHA256 hex digest, so the statement that
//! leaves the coordinator carries only finished digests.

use std::collections::HashMap;

use crate::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Insert, ObjectName, SetExpr, Statement, Value,
};
use crate::util::insert_column_ident;

use super::{HMAC_SHA256_ALGO, Secret, SecretResolver, hmac_sha256_hex};

/// Rewrite `stmt` in place, hashing every value written to a column named in
/// `anonymized_columns` (a map of column name -> secret id). Non-INSERT/UPDATE
/// statements and columns absent from the map are left untouched.
///
/// Returns `Err` with a client-facing message if a referenced secret is missing,
/// declares an unsupported algorithm, or if an anonymized column is written by a
/// form whose plaintext is not hashable at rewrite time: a bind placeholder, a
/// non-literal expression, or a multi-column `SET (…) = (…)`. NULL values are
/// preserved as NULL — a hash of "nothing" would be misleading and would defeat
/// nullability.
///
/// Every such form is **refused**, never passed over. A column the rewrite cannot
/// reach is a column whose plaintext reaches a storage node, which is the one
/// outcome this module exists to prevent; a refusal is the weaker failure.
pub fn anonymize_statement(
    stmt: &mut Statement,
    anonymized_columns: &HashMap<String, String>,
    resolver: &dyn SecretResolver,
) -> Result<(), String> {
    if anonymized_columns.is_empty() {
        return Ok(());
    }

    match stmt {
        Statement::Insert(insert) => anonymize_insert(insert, anonymized_columns, resolver),
        Statement::Update(update) => {
            anonymize_assignments(&mut update.assignments, anonymized_columns, resolver)
        }
        _ => Ok(()),
    }
}

/// Hash the anonymized columns of an `INSERT ... VALUES`, in every row.
fn anonymize_insert(
    insert: &mut Insert,
    anonymized_columns: &HashMap<String, String>,
    resolver: &dyn SecretResolver,
) -> Result<(), String> {
    let mut target_positions: Vec<(usize, &String)> = Vec::new();
    for (idx, col) in insert.columns.iter().enumerate() {
        if let Some(sid) = column_secret(col, anonymized_columns)? {
            target_positions.push((idx, sid));
        }
    }

    if target_positions.is_empty() {
        return Ok(());
    }

    let secrets = resolve_secrets(target_positions.iter().map(|(_, sid)| *sid), resolver)?;

    let Some(source) = insert.source.as_mut() else {
        return Ok(());
    };
    let SetExpr::Values(values) = source.body.as_mut() else {
        // An `INSERT ... SELECT` has its rows materialized into literal `VALUES`
        // before it reaches here, so a query source at this point means something new
        // routed around that. Refuse rather than let a non-VALUES source pass through
        // silently un-anonymized.
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

/// Hash the anonymized columns an `UPDATE` assigns to.
///
/// Which assignments need hashing is decided before any of them is rewritten, so
/// that each distinct secret is resolved once — a statement setting K anonymized
/// columns from one secret did K catalog reads when this walked and mutated in a
/// single pass.
fn anonymize_assignments(
    assignments: &mut [Assignment],
    anonymized_columns: &HashMap<String, String>,
    resolver: &dyn SecretResolver,
) -> Result<(), String> {
    let mut targets: Vec<(usize, &String)> = Vec::new();
    for (idx, assignment) in assignments.iter().enumerate() {
        match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                if let Some(sid) = column_secret(name, anonymized_columns)? {
                    targets.push((idx, sid));
                }
            }
            // `SET (a, b) = (x, y)` assigns from a single row constructor, so the
            // value to hash is one element of this assignment's value rather than
            // the whole of it — and in the `= (SELECT …)` form it is not in the AST
            // at all. The form is refused when it names an anonymized column instead
            // of being passed over, which is what shipped the plaintext.
            AssignmentTarget::Tuple(names) => {
                for name in names {
                    if column_secret(name, anonymized_columns)?.is_some() {
                        return Err(format!(
                            "anonymized column '{name}' cannot be assigned by a multi-column \
                             SET (…) = (…); assign it in a SET clause of its own"
                        ));
                    }
                }
            }
        }
    }

    if targets.is_empty() {
        return Ok(());
    }

    let secrets = resolve_secrets(targets.iter().map(|(_, sid)| *sid), resolver)?;
    for (idx, secret_id) in targets {
        anonymize_expr(&mut assignments[idx].value, &secrets[secret_id.as_str()])?;
    }
    Ok(())
}

/// The secret id `column` is declared anonymized under, or `None` if it is not.
///
/// Matching is case-insensitive — the map is keyed on lowercased identifiers, so
/// `EMAIL` still resolves to the `email` rule and a case mismatch never skips
/// hashing.
///
/// A reference that is not a bare identifier — a dotted composite-field target — is
/// refused rather than skipped. Skipping would write plaintext into a column
/// declared anonymized, and resolving it by its last part would hash it under
/// another column's rule.
fn column_secret<'a>(
    column: &ObjectName,
    anonymized_columns: &'a HashMap<String, String>,
) -> Result<Option<&'a String>, String> {
    let ident = insert_column_ident(column).ok_or_else(|| {
        format!("unsupported column reference '{column}' on a table with anonymized columns")
    })?;
    Ok(anonymized_columns.get(&ident.value.to_ascii_lowercase()))
}

/// Resolve and validate every distinct secret id in `ids` once, returning a map
/// from id to its [`Secret`]. Resolving up front, rather than per value, keeps a
/// statement to at-most-K catalog reads instead of one per hashed value — a bulk
/// INSERT of N rows over K anonymized columns would otherwise do N*K reads for the
/// same handful of ids.
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
    use crate::write_sql_cl;

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
        crate::pgwire_handler::parser::parse_sql(sql)
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
        let sql = write_sql_cl::statement_to_sql(&stmt);

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
        let sql = write_sql_cl::statement_to_sql(&stmt);
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
        let sql = write_sql_cl::statement_to_sql(&stmt);
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
        let sql = write_sql_cl::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "a@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("a@x.com"), "plaintext leaked: {sql}");
    }

    /// An UPDATE's assignment is hashed however the client spelled the column: the
    /// map is keyed on the lowercased name (as `parse_anonymized_columns` produces),
    /// so a case mismatch must not be the thing that skips hashing.
    #[test]
    fn update_hashes_its_assignment_in_any_case() {
        for sql in [
            "UPDATE t SET email = 'new@x.com' WHERE id = 1",
            "UPDATE t SET EMAIL = 'new@x.com' WHERE id = 1",
        ] {
            let mut stmt = parse_one(sql);
            let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
            anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
            let rewritten = write_sql_cl::statement_to_sql(&stmt);
            assert!(
                rewritten.contains(&hmac_sha256_hex("key", "new@x.com")),
                "`{sql}` was not hashed: {rewritten}"
            );
            assert!(
                !rewritten.contains("new@x.com"),
                "plaintext leaked from `{sql}`: {rewritten}"
            );
        }
    }

    /// A multi-column `SET (a, b) = (x, y)` names its columns in the assignment
    /// target and carries their values in one row constructor, so a per-assignment
    /// rewrite has no single value to hash. It used to be passed over silently,
    /// which wrote the plaintext of an anonymized column to the shard; it is refused
    /// now, and the refusal names the column.
    ///
    /// Scoped to anonymized columns only: the same form over columns the table does
    /// not anonymize loses no clause and must still answer.
    #[test]
    fn update_refuses_a_multi_column_assignment_to_an_anonymized_column() {
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        let anonymized = anon_map(&[("email", "sid")]);

        let mut stmt = parse_one("UPDATE t SET (email, name) = ('a@x.com', 'Alice') WHERE id = 1");
        let err = anonymize_statement(&mut stmt, &anonymized, &resolver)
            .expect_err("an anonymized column cannot be hashed inside a row constructor");
        assert!(err.contains("email"), "the column must be named: {err}");
        assert!(
            err.contains("SET"),
            "the message must say which form was refused: {err}"
        );

        let mut untouched = parse_one("UPDATE t SET (id, name) = (2, 'Alice') WHERE id = 1");
        anonymize_statement(&mut untouched, &anonymized, &resolver)
            .expect("a multi-column assignment over non-anonymized columns still answers");
        assert!(
            write_sql_cl::statement_to_sql(&untouched).contains("Alice"),
            "a non-anonymized value must pass through unchanged"
        );
    }

    #[test]
    fn null_value_is_preserved() {
        let mut stmt = parse_one("INSERT INTO t (id, email) VALUES (1, NULL)");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        anonymize_statement(&mut stmt, &anon_map(&[("email", "sid")]), &resolver).unwrap();
        let sql = write_sql_cl::statement_to_sql(&stmt);
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
        let sql = write_sql_cl::statement_to_sql(&stmt);
        assert!(sql.contains("a@x.com"), "got: {sql}");
    }

    // This rewriter finds anonymized columns by their position in the INSERT's
    // column list, so a positional INSERT — which has none — would hash nothing
    // and ship plaintext. `handle_dml` resolves the list from the catalog first,
    // for exactly this reason; the two steps are only correct in that order.
    #[test]
    fn positional_insert_is_hashed_once_its_columns_are_resolved() {
        let mut stmt = parse_one("INSERT INTO t VALUES (1, 'a@x.com')");
        let resolver = StaticResolver::with("sid", HMAC_SHA256_ALGO, "key");
        let anonymized = anon_map(&[("email", "sid")]);

        // Without the resolved list there is no column to match, and the
        // plaintext survives the rewrite.
        let mut untouched = stmt.clone();
        anonymize_statement(&mut untouched, &anonymized, &resolver).unwrap();
        assert!(
            write_sql_cl::statement_to_sql(&untouched).contains("a@x.com"),
            "the rewriter cannot match a column the statement does not name"
        );

        write_sql_cl::materialize_insert_columns(&mut stmt, &["id", "email"]).unwrap();
        anonymize_statement(&mut stmt, &anonymized, &resolver).unwrap();
        let sql = write_sql_cl::statement_to_sql(&stmt);
        assert!(
            sql.contains(&hmac_sha256_hex("key", "a@x.com")),
            "got: {sql}"
        );
        assert!(!sql.contains("a@x.com"), "plaintext leaked: {sql}");
    }
}
