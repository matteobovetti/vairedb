//! The two halves of what a distributed read carries across the Ballista wire: the
//! logical plan's `SchedulerTableProvider` (which shards a table has and which node holds
//! each) and the physical plan's `RemoteDuckDbScanExec` (the per-shard scan that layout
//! expanded into). Custom logical plan *extensions* are rejected.
//!
//! One module because they are one decision. Both are installed on every session config
//! together (see `scheduler::ballista_session_config`), both exist only so the shard layout
//! survives the trip, and a field added to one side's wire form without the other's is a
//! plan that encodes on the coordinator and decodes into something else on the node — the
//! failure this pairing makes visible in a single file.
//!
//! Neither side re-derives anything the far side could look up: the executor has no
//! metadata catalog, so whatever the catalog decided has to travel. That is why the shard's
//! hash bucket and the opaque-text column set are fields here rather than inferences there.

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use serde::{Deserialize, Serialize};

use vairedb_common::scan_plan::DuckDbScanPlanBytes;

use crate::catalog::ShardMeta;

use super::filter_pushdown::OpaqueTextColumns;
use super::remote_scan_exec::RemoteDuckDbScanExec;
use super::scheduler::SchedulerTableProvider;

/// Physical extension codec that handles `RemoteDuckDbScanExec` and falls back to
/// the wrapped Ballista codec for every other physical plan node.
#[derive(Debug, Default)]
pub struct VairePhysicalCodec {
    ballista_codec: BallistaPhysicalExtensionCodec,
}

impl VairePhysicalCodec {
    /// Create a codec with the default Ballista fallback codec.
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalExtensionCodec for VairePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(plan_bytes) = DuckDbScanPlanBytes::decode(buf)
            && let Ok(schema) = decode_schema_ipc(&plan_bytes.schema_ipc)
        {
            return Ok(Arc::new(
                RemoteDuckDbScanExec::new(
                    plan_bytes.shard_table_name,
                    schema,
                    plan_bytes.projection,
                    plan_bytes.filter_exprs,
                    plan_bytes.target_executor_id,
                    plan_bytes.replica_executor_ids,
                )
                .with_limit(plan_bytes.limit),
            ));
        }

        self.ballista_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(remote_scan) = node.downcast_ref::<RemoteDuckDbScanExec>() {
            let plan_bytes = DuckDbScanPlanBytes {
                shard_table_name: remote_scan.shard_table_name().to_string(),
                schema_ipc: encode_schema_ipc(remote_scan.projected_schema())?,
                projection: remote_scan.projection().clone(),
                filter_exprs: remote_scan.filter_exprs().to_vec(),
                limit: remote_scan.limit(),
                target_executor_id: remote_scan.target_executor_id().map(|s| s.to_string()),
                replica_executor_ids: remote_scan.replica_executor_ids().to_vec(),
            };
            buf.extend_from_slice(&plan_bytes.encode());
            return Ok(());
        }

        self.ballista_codec.try_encode(node, buf)
    }
}

/// Serialize an Arrow schema to its IPC file representation for embedding in the
/// scan plan bytes.
fn encode_schema_ipc(schema: &SchemaRef) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer =
            datafusion::arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref())?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Reconstruct an Arrow schema from its IPC file bytes.
fn decode_schema_ipc(buf: &[u8]) -> Result<SchemaRef> {
    let reader =
        datafusion::arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)?;
    Ok(reader.schema())
}

/// Logical extension codec for distributed query planning. Encodes/decodes
/// `SchedulerTableProvider`s; rejects arbitrary logical plan extensions.
#[derive(Debug)]
pub struct VaireLogicalCodec;

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
    /// The columns advertised as text that the shard does not store as text — see
    /// [`OpaqueTextColumns`]. Carried rather than recomputed because the declared type
    /// strings live in the catalog, which the decoding side reads no schema from: it is
    /// handed the Arrow schema, where these columns are indistinguishable from real text.
    ///
    /// `None` (an encoding from before this field existed) means *not known*, and is read as
    /// "treat every text column as opaque" rather than as an empty set, so a rolling upgrade
    /// loses push-down on text columns instead of pushing a predicate that could error.
    #[serde(default)]
    opaque_text_columns: Option<Vec<String>>,
}

impl LogicalExtensionCodec for VaireLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        Err(unsupported_extension())
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        Err(unsupported_extension())
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
                "failed to decode distributed scan plan for '{table_ref}'"
            ))
        })?;

        let shards: Vec<ShardMeta> = (0..encoded.shard_names.len())
            .map(|i| {
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

        Ok(Arc::new(
            SchedulerTableProvider::new(encoded.table_name, shards, schema)
                .with_opaque_text_columns(OpaqueTextColumns::from_names(
                    encoded.opaque_text_columns,
                )),
        ))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let provider = node
            .downcast_ref::<SchedulerTableProvider>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "unsupported table provider type for distributed query planning".to_string(),
                )
            })?;

        let table_name = provider.table_name();
        let mut encoded = EncodedTableProvider {
            table_name: table_name.to_string(),
            shard_names: Vec::with_capacity(provider.shards().len()),
            hash_buckets: Vec::with_capacity(provider.shards().len()),
            primary_node_ids: Vec::with_capacity(provider.shards().len()),
            replica_node_ids_per_shard: Vec::with_capacity(provider.shards().len()),
            opaque_text_columns: provider.opaque_text_columns().names(),
        };
        for shard in provider.shards() {
            encoded
                .shard_names
                .push(crate::util::shard_table_name(table_name, shard.hash_bucket));
            encoded.hash_buckets.push(shard.hash_bucket);
            encoded.primary_node_ids.push(shard.primary_node_id.clone());
            encoded
                .replica_node_ids_per_shard
                .push(shard.replica_node_ids.clone());
        }

        let bytes = serde_json::to_vec(&encoded).map_err(|e| {
            tracing::error!(table_name = %table_name, error = %e, "failed to encode distributed scan plan");
            DataFusionError::Internal(format!(
                "failed to encode distributed scan plan for '{table_name}'"
            ))
        })?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }
}

/// The one refusal both `Extension` directions give, so the message a client sees cannot
/// differ between encoding and decoding one.
fn unsupported_extension() -> DataFusionError {
    DataFusionError::NotImplemented(
        "custom logical plan extensions are not supported in distributed queries".to_string(),
    )
}
