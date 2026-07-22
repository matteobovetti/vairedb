//! Physical plan extension codec that serializes `RemoteDuckDbScanExec` nodes so
//! Ballista can ship them to executors, delegating all other nodes to the
//! standard Ballista codec.

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use vairedb_common::scan_plan::DuckDbScanPlanBytes;

use super::remote_scan_exec::RemoteDuckDbScanExec;

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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Ok(plan_bytes) = DuckDbScanPlanBytes::decode(buf)
            && let Ok(schema) = decode_schema_ipc(&plan_bytes.schema_ipc)
        {
            return Ok(Arc::new(RemoteDuckDbScanExec::new(
                plan_bytes.shard_table_name,
                schema,
                plan_bytes.projection,
                plan_bytes.filter_exprs,
                plan_bytes.target_executor_id,
                plan_bytes.replica_executor_ids,
            )));
        }

        self.ballista_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(remote_scan) = node.as_any().downcast_ref::<RemoteDuckDbScanExec>() {
            let schema_ipc = encode_schema_ipc(remote_scan.projected_schema())?;

            let plan_bytes = DuckDbScanPlanBytes {
                shard_table_name: remote_scan.shard_table_name().to_string(),
                schema_ipc,
                projection: remote_scan.projection().clone(),
                filter_exprs: remote_scan.filter_exprs().to_vec(),
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
fn encode_schema_ipc(schema: &SchemaRef) -> datafusion::error::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer =
            datafusion::arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref())?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Reconstruct an Arrow schema from its IPC file bytes.
fn decode_schema_ipc(buf: &[u8]) -> datafusion::error::Result<SchemaRef> {
    let reader =
        datafusion::arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)?;
    Ok(reader.schema())
}
