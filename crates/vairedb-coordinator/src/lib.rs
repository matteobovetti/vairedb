#![allow(clippy::module_inception)]

//! Coordinator node for VaireDB.
//!
//! The coordinator speaks the PostgreSQL wire protocol to clients, owns the
//! metadata catalog (tables, shards, node liveness), routes reads through a
//! Ballista scheduler and writes to the core nodes that hold DuckDB shards, and
//! tracks node liveness via heartbeats. The submodules here implement those
//! responsibilities.

pub mod anonymization;
pub mod catalog;
pub mod channel_pool;
pub mod config;
pub mod error;
pub mod node_service;
pub mod pgwire_handler;
pub mod query_router;
pub mod replication;
pub mod scheduler;
pub mod sql_compat;
pub mod util;
pub mod write_router;
