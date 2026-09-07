//! Logical plan extension codec that serializes `SchedulerTableProvider` shard
//! layouts (table name, shard names, and per-shard node affinity) across the
//! distributed query boundary. Custom logical plan extensions themselves are not
//! supported and are rejected.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use serde::{Deserialize, Serialize};

use crate::catalog::ShardMeta;

use super::scheduler::SchedulerTableProvider;

/// Wire form of a `SchedulerTableProvider`: the table name plus, per shard, its
/// physical shard name, hash bucket, primary node id, and replica node ids.
/// Every field but the first two defaults to empty for backward compatibility
/// with older encodings.
#[derive(Debug, Serialize, Deserialize)]
struct EncodedTableProvider {
    table_name: String,
    shard_names: Vec<String>,
    /// The bucket each entry belongs to, carried rather than inferred from the
    /// entry's position: a shard's position in a list is not its bucket number
    /// (see `write_router::shard_for_bucket`), and the bucket is what names the
    /// physical table the scan reads.
    #[serde(default)]
    hash_buckets: Vec<u32>,
    #[serde(default)]
    primary_node_ids: Vec<String>,
    #[serde(default)]
    replica_node_ids_per_shard: Vec<Vec<String>>,
}

/// Logical extension codec for distributed query planning. Encodes/decodes
/// `SchedulerTableProvider`s; rejects arbitrary logical plan extensions.
#[derive(Debug)]
pub struct VaireLogicalCodec;

impl LogicalExtensionCodec for VaireLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        Err(DataFusionError::NotImplemented(
            "custom logical plan extensions are not supported in distributed queries".to_string(),
        ))
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "custom logical plan extensions are not supported in distributed queries".to_string(),
        ))
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let encoded: EncodedTableProvider = serde_json::from_slice(buf).map_err(|e| {
            tracing::error!(table_ref = %table_ref, error = %e, "failed to decode distributed scan plan");
            DataFusionError::Internal(format!(
                "failed to decode distributed scan plan for '{}'",
                table_ref
            ))
        })?;

        let shards: Vec<ShardMeta> = encoded
            .shard_names
            .iter()
            .enumerate()
            .map(|(i, _)| {
                // An encoding from before `hash_buckets` existed has none to read,
                // and its shards were listed in bucket order, so the position is
                // the best available answer for it.
                let bucket = encoded.hash_buckets.get(i).copied().unwrap_or(i as u32);
                ShardMeta {
                    shard_id: crate::util::logical_shard_id(bucket),
                    table_name: encoded.table_name.clone(),
                    hash_bucket: bucket,
                    primary_node_id: encoded.primary_node_ids.get(i).cloned().unwrap_or_default(),
                    replica_node_ids: encoded
                        .replica_node_ids_per_shard
                        .get(i)
                        .cloned()
                        .unwrap_or_default(),
                    range_lower: String::new(),
                    range_upper: String::new(),
                }
            })
            .collect();

        Ok(Arc::new(SchedulerTableProvider::new(
            encoded.table_name,
            shards,
            schema,
        )))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let provider = node
            .as_any()
            .downcast_ref::<SchedulerTableProvider>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "unsupported table provider type for distributed query planning".to_string(),
                )
            })?;

        let shard_names: Vec<String> = provider
            .shards()
            .iter()
            .map(|p| crate::util::shard_table_name(provider.table_name(), p.hash_bucket))
            .collect();

        let primary_node_ids: Vec<String> = provider
            .shards()
            .iter()
            .map(|p| p.primary_node_id.clone())
            .collect();

        let replica_node_ids_per_shard: Vec<Vec<String>> = provider
            .shards()
            .iter()
            .map(|p| p.replica_node_ids.clone())
            .collect();

        let hash_buckets: Vec<u32> = provider.shards().iter().map(|p| p.hash_bucket).collect();

        let encoded = EncodedTableProvider {
            table_name: provider.table_name().to_string(),
            shard_names,
            hash_buckets,
            primary_node_ids,
            replica_node_ids_per_shard,
        };

        let bytes = serde_json::to_vec(&encoded).map_err(|e| {
            tracing::error!(table_name = %provider.table_name(), error = %e, "failed to encode distributed scan plan");
            DataFusionError::Internal(format!(
                "failed to encode distributed scan plan for '{}'",
                provider.table_name()
            ))
        })?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }
}
