//! Cache of long-lived gRPC channels to core nodes, keyed by address, so the
//! coordinator reuses connections instead of dialing on every request.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::RwLock;
use tonic::transport::Channel;

/// Thread-safe pool of reusable tonic `Channel`s to core nodes, configured with
/// HTTP/2 keep-alive and connect timeouts.
pub struct ChannelPool {
    channels: RwLock<HashMap<String, Channel>>,
    keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
    connect_timeout: Duration,
}

impl Default for ChannelPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelPool {
    /// Create an empty pool with default keep-alive and connect timeouts.
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
        }
    }

    /// Return a channel to `address`, creating and caching one on first use.
    ///
    /// Cloning a `Channel` is cheap and shares the underlying connection, so the
    /// returned value can be used directly. Uses double-checked locking so
    /// concurrent callers for the same address don't each open a connection.
    /// Returns `Err` if the connection cannot be established; a malformed address
    /// is logged and likewise surfaced as a connect error.
    pub async fn get(&self, address: &str) -> Result<Channel, tonic::transport::Error> {
        if let Some(ch) = self.channels.read().await.get(address) {
            return Ok(ch.clone());
        }

        let mut map = self.channels.write().await;
        if let Some(ch) = map.get(address) {
            return Ok(ch.clone());
        }

        let endpoint = match Channel::from_shared(format!("http://{}", address)) {
            Ok(ep) => ep,
            Err(e) => {
                tracing::error!(address = %address, error = %e, "malformed node address");
                return Err(Channel::from_static("http://[::]:0")
                    .connect()
                    .await
                    .unwrap_err());
            }
        }
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(self.keep_alive_interval)
        .keep_alive_timeout(self.keep_alive_timeout)
        .connect_timeout(self.connect_timeout);

        let channel = endpoint.connect().await?;
        map.insert(address.to_string(), channel.clone());
        Ok(channel)
    }

    /// Evict the cached channel for `address`, forcing a reconnect on next `get`
    /// (e.g. after a node is detected dead or its connection goes bad).
    pub async fn remove(&self, address: &str) {
        self.channels.write().await.remove(address);
    }
}
