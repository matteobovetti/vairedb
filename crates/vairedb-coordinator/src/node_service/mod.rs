//! Core-node membership: the gRPC `NodeService` (register/heartbeat/report) and
//! the heartbeat-based failure detector.

mod failure_detector;
mod node_service;

pub use failure_detector::FailureDetector;
pub use node_service::*;
