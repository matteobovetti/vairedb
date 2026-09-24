//! Shared types and helpers used by both the coordinator and core nodes.
//!
//! This crate holds code that must be identical on both sides of the wire:
//! the protobuf-generated gRPC types ([`proto`]), error classification and
//! sanitization ([`error`]), the cross-node scan-plan payload ([`scan_plan`]),
//! YAML config loading ([`config`]), and the functions a distributed stage
//! resolves by name on whichever node runs it ([`within_group`], [`avg_udaf`],
//! [`stats_udaf`], [`pg_udf`], [`float_div`], [`not_in`], [`bytea_in`],
//! [`json_pg`], [`json_agg`], [`uuid_in`], [`nth_value`], [`ntile`],
//! [`pg_typeof`], [`pg_format`], [`pg_format_type`], [`pg_datetime`]).
//!
//! Those function modules are reached through [`distributed_functions`], which is the
//! one list of them: a node registers the set with one call rather than naming each
//! module, because the rule is that every node holds the *identical* set and a
//! hand-copied list is how that rule gets broken.
//!
//! The list has one row fewer than there are function modules, which is correct rather
//! than a gap: [`pg_format_type`] travels inside [`pg_udf`]'s `pg_catalog` set instead of
//! on a row of its own, because the name it shadows is one of that set's own. Counting the
//! rows against the modules and adding the difference would give one function two
//! registration paths.

pub mod avg_udaf;
pub mod bytea_in;
/// Reading an argument's column in a known Arrow layout, shared by the
/// functions below that all had their own copy of it.
pub(crate) mod columns;
pub mod config;
pub mod distributed_functions;
pub mod error;
pub mod float_div;
pub mod json_agg;
pub mod json_pg;
pub mod not_in;
pub mod nth_value;
pub mod ntile;
/// The `numeric(38, 16)` the exact aggregates answer in, declared once for all of them.
pub(crate) mod numeric;
pub mod pg_datetime;
pub mod pg_format;
pub mod pg_format_type;
pub mod pg_typeof;
pub mod pg_udf;
pub mod scan_plan;
pub mod stats_udaf;
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
