//! Persistent metadata catalog and its DataFusion `SchemaProvider`, exposing
//! table/shard/node metadata both as a programmatic store and as queryable
//! virtual tables.

mod catalog;
mod schema_provider;

pub use catalog::MetadataCatalog;
pub use schema_provider::VaireDbCatalogSchema;
pub use vairedb_common::proto::vairedb::v1::{
    AnonymizationSecret, ColumnDef, NodeMeta, NodeState, ShardMeta, ShardStrategy, TableMeta,
};
