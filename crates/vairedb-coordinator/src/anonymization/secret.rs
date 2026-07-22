//! The hashing primitive and the secret abstraction: [`hmac_sha256_hex`] plus
//! [`Secret`] / [`SecretResolver`]. The resolver keeps the catalog away from the
//! rewriter, so hashing logic is testable without a metadata store (Dependency
//! Inversion).

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

// Create alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

/// The only supported pseudonymization algorithm.
pub const HMAC_SHA256_ALGO: &str = "HMAC-SHA256";

/// A resolved anonymization secret: the algorithm and the pepper (HMAC key).
#[derive(Debug, Clone)]
pub struct Secret {
    pub algo: String,
    pub secret_key: String,
}

/// Resolves an anonymization-secret id to its [`Secret`]. Abstracts the catalog
/// away from the rewriter so hashing logic is testable without a metadata store.
pub trait SecretResolver {
    /// Return the secret registered under `secret_id`, or `None` if absent.
    fn resolve(&self, secret_id: &str) -> Option<Secret>;
}

/// Compute the HMAC-SHA256 digest of `plaintext` keyed by `secret_key`, returned
/// as a 64-character lowercase hex string. HMAC accepts a key of any length, so
/// this never fails.
pub fn hmac_sha256_hex(secret_key: &str, plaintext: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret_key.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(plaintext.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_hex_matches_reference_vector() {
        // Reference computed with the standard library: HMAC-SHA256 over the
        // documented example key/value.
        let digest = hmac_sha256_hex("my_awesome_and_secret_key", "Antony McDonald");
        assert_eq!(
            digest,
            "3fb5449f3175e3aa8cf1fa31fe880e31681583d6b5977ab183f5fc274c277eea"
        );
    }

    #[test]
    fn hmac_sha256_hex_is_deterministic_and_64_chars() {
        let a = hmac_sha256_hex("k", "value");
        let b = hmac_sha256_hex("k", "value");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hmac_sha256_hex_differs_by_key() {
        assert_ne!(
            hmac_sha256_hex("k1", "value"),
            hmac_sha256_hex("k2", "value")
        );
    }

    #[test]
    fn empty_key_and_value_still_produce_a_digest() {
        let digest = hmac_sha256_hex("", "");
        assert_eq!(digest.len(), 64);
    }
}
