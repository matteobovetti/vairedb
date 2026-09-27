//! PostgreSQL wire-protocol handler: SQL parsing, query routing, DDL/DML
//! execution, result encoding, catalog introspection, and error enrichment.
//! Re-exports the `handler` entry points.
//!
//! The single SQL parse and the read-path AST rewrites live in `parser`; the
//! statement classification and table-name extraction that parse feeds live in
//! `query_router`; the write-path PG→DuckDB translation lives in
//! [`crate::write_sql_cl`].

pub(crate) mod encoding;
pub(crate) mod error_enrichment;
pub(crate) mod pg_operators;
pub(crate) mod pg_settings;
pub(crate) mod pg_subscripts;
pub(crate) mod wire_types;

pub mod parser;
pub mod query_router;

mod anonymized_reads;
mod catalog_routing;
mod column_labels;
mod compat_rewrite;
mod constraints;
mod copy;
mod copy_parquet;
mod copy_stream;
mod ddl;
mod dml;
mod handler;
mod indexes;
mod introspection;
mod merge;
mod pg_aggregate_widening;
mod pg_clock_functions;
mod pg_count_arity;
mod pg_float_division;
mod pg_grouping_sets;
mod pg_integer_literals;
mod pg_named_windows;
mod pg_not_in_nulls;
mod pg_param_types;
mod pg_projection_subqueries;
mod pg_quantified_subqueries;
mod pg_set_op_multiplicity;
mod pg_set_op_types;
mod pg_using_join_merge;
mod pg_using_join_qualifiers;
mod pg_using_join_where_keys;
/// Shared way for a `pg_*` module's tests to ask the read path for a verdict, so a pass
/// is tested through `parser::plan_select` rather than through a copy of part of it.
#[cfg(test)]
mod read_path_test_helper;
mod schemas;
mod sequences;
mod session;
mod session_params;
mod table_meta_ops;
mod transaction;
mod user_types;
mod views;

/// Shared way for a write-path module's tests to parse the statement under test the way the
/// write path parses it, and to read back the SQLSTATE and message a client would see.
#[cfg(test)]
mod write_path_test_helper;

pub use handler::*;
