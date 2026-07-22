#![allow(clippy::module_inception)]
//! VaireDB core node.
//!
//! A core node owns a local DuckDB database holding the table shards assigned to
//! it. It registers with the coordinator over gRPC, streams heartbeats to signal
//! liveness and observe drain requests, serves writes through a serialized write
//! queue, and exposes its shards to distributed reads as a Ballista executor.
//!
//! ## Modules
//!
//! - [`config`] — node configuration loaded from a YAML file.
//! - [`engine`] — the DuckDB connection and shard inventory.
//! - [`error`] — the crate-wide [`error::CoreError`] type.
//! - [`heartbeat`] — coordinator registration and the heartbeat/drain loop.
//! - [`ballista_exec`] — the Ballista executor and custom physical-plan codec.
//! - [`table_provider`] — the `DuckDbScanExec` plan that reads local shards.
//! - [`write_queue`] — single-writer serialization of DuckDB mutations.
//! - [`write_service`] — the gRPC `WriteService` with idempotent dedup.

pub mod ballista_exec;
pub mod config;
pub mod engine;
pub mod error;
pub mod heartbeat;
pub mod table_provider;
pub mod write_queue;
pub mod write_service;
