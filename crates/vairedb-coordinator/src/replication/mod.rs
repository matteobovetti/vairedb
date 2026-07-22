//! Write replication to replica core nodes: quorum writes plus a background
//! retry/backoff loop that tails missed writes to lagging replicas.

mod replication;
mod retry_config;

pub use replication::ReplicationManager;
pub use retry_config::RetryConfig;
