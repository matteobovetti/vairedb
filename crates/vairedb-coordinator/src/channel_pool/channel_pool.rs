//! Cache of gRPC channels to core nodes, keyed by address, so the coordinator
//! dials a node once instead of on every write, DDL broadcast and read plan.
//!
//! The pool only ever inserts, and needs no eviction to stay correct: a tonic
//! `Channel` re-establishes its own transport, so a node that restarts is reachable
//! again through the channel already cached for it. There used to be a `remove` for
//! "after a node is detected dead" — a policy nothing implemented, which left the
//! method describing an eviction that never happened and cost a resurrection race
//! against a concurrent dial for the same address.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

/// HTTP/2 keep-alive: how often to ping an idle node, and how long a ping may go
/// unanswered before the transport is considered dead.
///
/// Writes arrive in bursts with long gaps between them. Without keep-alive a
/// half-open connection is discovered by the *next write*, which then fails and is
/// queued for a node that is in fact healthy; the ping finds it in the gap instead.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Ceiling on a single dial, so a caller waiting on an unreachable node fails in
/// bounded time rather than holding the client's statement open indefinitely.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Pool of reusable tonic channels to core nodes, shared across the coordinator.
#[derive(Default)]
pub struct ChannelPool {
    channels: RwLock<HashMap<String, Channel>>,
}

impl ChannelPool {
    /// An empty pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a channel to `address`, dialing and caching one on first use.
    ///
    /// `get` rather than `connect`, following the pool convention (`r2d2`,
    /// `deadpool`): it acquires, and only sometimes creates. Cloning a `Channel` is
    /// cheap and shares the one connection, so the returned value is used directly.
    ///
    /// The dial deliberately happens with **no lock held**. Holding the write guard
    /// across it would serialize the entire pool behind one dial, so a single
    /// unreachable node would stall writes to every *other* node for
    /// [`CONNECT_TIMEOUT`] — the cluster made slow by the one node already excluded
    /// from it. The price is that concurrent first callers for one address may each
    /// dial; whichever result is inserted first is the channel they all go on to
    /// share, and the surplus are dropped.
    pub async fn get(&self, address: &str) -> Result<Channel, tonic::transport::Error> {
        if let Some(channel) = self.channels.read().await.get(address) {
            return Ok(channel.clone());
        }

        // An address that is not a URI is a catalog entry an operator has to fix, and
        // all tonic will say about it is "invalid URI" — so name the address here.
        let endpoint = Endpoint::from_shared(format!("http://{address}")).inspect_err(|e| {
            tracing::error!(address = %address, error = %e, "node address is not a URI");
        })?;

        let channel = endpoint
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await?;

        Ok(self
            .channels
            .write()
            .await
            .entry(address.to_string())
            .or_insert(channel)
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A malformed address is reported as the malformed address it is, and nothing is
    /// dialed to find that out.
    ///
    /// This used to be answered by connecting to `http://[::]:0` purely to harvest a
    /// `tonic::transport::Error` value — network I/O to manufacture an error, with an
    /// `unwrap_err()` that would have panicked had that doomed connect ever
    /// succeeded, and the real cause discarded. `Endpoint::from_shared` already fails
    /// with that error type, which is the whole reason the fabrication could go.
    ///
    /// Asserting the exact wording is what distinguishes the two: a connect failure
    /// reads "transport error", so this assertion fails if the address is dialed.
    #[tokio::test]
    async fn an_address_that_is_not_a_uri_is_refused_as_one() {
        let pool = ChannelPool::new();

        let err = pool
            .get("has a space:50041")
            .await
            .expect_err("that is not a URI");

        assert_eq!(err.to_string(), "invalid URI");
        assert!(pool.channels.read().await.is_empty());
    }
}
