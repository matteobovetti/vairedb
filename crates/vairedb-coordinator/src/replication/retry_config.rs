//! Tuning constants and configuration for the replication retry/backoff loop.

/// Per-node cap on queued retries; bounds memory when a node stays unreachable.
/// Writes beyond the cap are dropped and reconciled when the node rejoins.
pub const MAX_PENDING_RETRIES: usize = 4096;
/// Default base delay (ms) for the first retry; doubles with each attempt.
pub const DEFAULT_INITIAL_RETRY_MS: u64 = 100;
/// Default ceiling (ms) that exponential backoff is clamped to.
pub const DEFAULT_MAX_RETRY_MS: u64 = 5000;

/// Backoff parameters for retrying writes to lagging replica nodes.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Base delay in milliseconds; the backoff doubles per attempt from here.
    pub initial_retry_ms: u64,
    /// Upper bound in milliseconds that the computed backoff is capped to.
    pub max_retry_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_retry_ms: DEFAULT_INITIAL_RETRY_MS,
            max_retry_ms: DEFAULT_MAX_RETRY_MS,
        }
    }
}
