//! PostgreSQL wire-protocol handler: SQL parsing, query routing, DDL/DML
//! execution, result encoding, catalog introspection, and error enrichment.
//! Re-exports the `handler` entry points.
//!
//! The single SQL parse and the read-path AST rewrites live in `parser`; the
//! statement classification and table-name extraction that parse feeds live in
//! `query_router`; the write-path PG→DuckDB translation lives in
//! [`crate::write_sql_cl`].

mod catalog_routing;
mod constraints;
mod copy;
mod ddl;
mod dml;
pub(crate) mod encoding;
pub(crate) mod error_enrichment;
mod handler;
mod indexes;
mod merge;
pub mod parser;
pub mod query_router;
mod schemas;
mod sequences;
mod session;
mod table_meta_ops;
mod transaction;
mod user_types;
mod views;

pub use handler::*;
