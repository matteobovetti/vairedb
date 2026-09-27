//! Write replication to replica core nodes: quorum writes plus a background
//! retry/backoff loop that tails missed writes to lagging replicas.

mod replication;

pub use replication::{BatchStatement, ReplicationManager, RetryConfig};
