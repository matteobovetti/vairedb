//! PostgreSQL wire-protocol handler: SQL parsing, query routing, DDL/DML
//! execution, result encoding, catalog introspection, and error enrichment.
//! Re-exports the `handler` entry points.
//!
//! The single SQL parse and the read-path AST rewrites live in `parser`; the
//! statement classification and table-name extraction that parse feeds live in
//! `query_router`; the write-path PG→DuckDB translation lives in
//! [`crate::write_sql_cl`].

mod anonymized_reads;
mod catalog_routing;
mod column_labels;
mod compat_rewrite;
mod constraints;
mod copy;
mod copy_stream;
mod ddl;
mod dml;
pub(crate) mod encoding;
pub(crate) mod error_enrichment;
mod handler;
mod indexes;
mod introspection;
mod merge;
pub mod parser;
mod pg_aggregate_widening;
// `pub(crate)` for the two items the write path shares with it: the `SIMILAR TO` pattern
// translation and the byte-order collation test. Both decide what PostgreSQL means rather
// than what either engine does, so the two paths read them from one place.
pub(crate) mod pg_operators;
mod pg_param_types;
mod pg_set_op_types;
mod pg_using_join_merge;
pub mod query_router;
mod schemas;
mod sequences;
mod session;
mod session_params;
mod table_meta_ops;
#[cfg(test)]
mod test_catalog;
mod transaction;
mod user_types;
mod views;

pub use handler::*;
