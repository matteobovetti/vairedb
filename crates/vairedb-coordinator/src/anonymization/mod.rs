//! Column pseudonymization: the coordinator hashes the plaintext of anonymized
//! columns with HMAC-SHA256 (keyed by a secret pepper from the catalog) and
//! inlines the resulting hex digest into the write statement, so no plaintext
//! and no hash-function call ever crosses to a core node.
//!
//! The module is split so the *policy* (which columns, resolving secrets) stays
//! separate from the *mechanism* (HMAC, AST rewriting):
//! - `secret` — the [`hmac_sha256_hex`] primitive and the [`Secret`] /
//!   [`SecretResolver`] abstraction over the catalog.
//! - `rewrite` — rewrites an INSERT/UPDATE in place via [`anonymize_statement`].

mod rewrite;
mod secret;

pub use rewrite::anonymize_statement;
pub use secret::{HMAC_SHA256_ALGO, Secret, SecretResolver, hmac_sha256_hex};
