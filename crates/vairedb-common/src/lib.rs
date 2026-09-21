//! Shared types and helpers used by both the coordinator and core nodes.
//!
//! This crate holds code that must be identical on both sides of the wire:
//! the protobuf-generated gRPC types ([`proto`]), error classification and
//! sanitization ([`error`]), the cross-node scan-plan payload ([`scan_plan`]),
//! YAML config loading ([`config`]), and the functions a distributed stage
//! resolves by name on whichever node runs it ([`udaf`], [`avg_udaf`],
//! [`stats_udaf`], [`pg_udf`], [`float_div`], [`not_in`], [`bytea_in`],
//! [`json_pg`], [`json_agg`], [`uuid_in`], [`nth_value`], [`ntile`],
//! [`within_group`], [`pg_typeof`], [`pg_format`], [`pg_format_type`],
//! [`pg_datetime`]).

pub mod avg_udaf;
pub mod bytea_in;
pub mod config;
pub mod error;
pub mod float_div;
pub mod json_agg;
pub mod json_pg;
pub mod not_in;
pub mod nth_value;
pub mod ntile;
pub mod pg_datetime;
pub mod pg_format;
pub mod pg_format_type;
pub mod pg_typeof;
pub mod pg_udf;
pub mod scan_plan;
pub mod stats_udaf;
pub mod udaf;
pub mod uuid_in;
pub mod within_group;

/// Protobuf-generated gRPC types, compiled from `proto/vairedb/v1/` by
/// `build.rs`.
pub mod proto {
    pub mod vairedb {
        pub mod v1 {
            tonic::include_proto!("vairedb.v1");
        }
    }
}
