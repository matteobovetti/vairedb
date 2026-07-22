//! Shared types and helpers used by both the coordinator and core nodes.
//!
//! This crate holds code that must be identical on both sides of the wire:
//! the protobuf-generated gRPC types ([`proto`]), error classification and
//! sanitization ([`error`]), the cross-node scan-plan payload ([`scan_plan`]),
//! and YAML config loading ([`config`]).

pub mod config;
pub mod error;
pub mod scan_plan;

/// Protobuf-generated gRPC types, compiled from `proto/vairedb/v1/` by
/// `build.rs`.
pub mod proto {
    pub mod vairedb {
        pub mod v1 {
            tonic::include_proto!("vairedb.v1");
        }
    }
}
