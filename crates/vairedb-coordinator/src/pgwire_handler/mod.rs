//! PostgreSQL wire-protocol handler: query routing, DDL/DML execution, result
//! encoding, catalog introspection, and error enrichment. Re-exports the
//! `handler` entry points.

mod catalog_routing;
mod ddl;
mod dml;
mod encoding;
pub(crate) mod error_enrichment;
mod handler;
mod parser;
mod table_meta_ops;

pub use handler::*;
