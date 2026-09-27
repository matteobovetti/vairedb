//! Persistent metadata catalog and its DataFusion `SchemaProvider`, exposing
//! table/shard/node metadata both as a programmatic store and as queryable
//! virtual tables.

mod catalog;
mod schema_provider;

/// One scratch catalog per test, shared crate-wide so no module builds its own — see the
/// module docs for the cross-test contamination this exists to prevent.
#[cfg(test)]
pub(crate) mod catalog_test_helper;

pub use catalog::MetadataCatalog;
pub use schema_provider::VaireDbCatalogSchema;
pub use vairedb_common::proto::vairedb::v1::{
    AnonymizationSecret, ColumnDef, ConstraintKind, ConstraintMeta, IndexMeta, NodeMeta, NodeState,
    SchemaMeta, ShardMeta, ShardStrategy, TableMeta, ViewMeta,
};
