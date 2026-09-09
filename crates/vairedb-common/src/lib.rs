//! Shared types and helpers used by both the coordinator and core nodes.
//!
//! This crate holds code that must be identical on both sides of the wire:
//! the protobuf-generated gRPC types ([`proto`]), error classification and
//! sanitization ([`error`]), the cross-node scan-plan payload ([`scan_plan`]),
//! YAML config loading ([`config`]), and the functions a distributed stage
//! resolves by name on whichever node runs it ([`udaf`], [`pg_udf`]).

pub mod config;
pub mod error;
pub mod pg_udf;
pub mod scan_plan;
pub mod udaf;

/// Protobuf-generated gRPC types, compiled from `proto/vairedb/v1/` by
/// `build.rs`.
pub mod proto {
    pub mod vairedb {
        pub mod v1 {
            tonic::include_proto!("vairedb.v1");
        }
    }
}
