use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use tokio::sync::Mutex;

use vairedb_common::scan_plan::DuckDbScanPlanBytes;

use crate::engine::DuckDbEngine;
use crate::table_provider::DuckDbScanExec;

/// Physical-plan codec that round-trips [`DuckDbScanExec`] nodes across the
/// Ballista wire, delegating every other plan type to the default Ballista codec.
///
/// The scheduler serializes physical plans and ships them to executors; this
/// codec teaches the executor how to reconstruct a `DuckDbScanExec` (wiring it
/// back to the local engine) and how to encode one for transmission.
#[derive(Debug)]
pub(crate) struct VaireExecutorPhysicalCodec {
    engine: Arc<Mutex<DuckDbEngine>>,
    ballista_codec: BallistaPhysicalExtensionCodec,
}

impl VaireExecutorPhysicalCodec {
    /// Create a codec bound to the local `engine`, which decoded scan plans are
    /// wired to so they execute against this node's shards.
    pub(crate) fn new(engine: Arc<Mutex<DuckDbEngine>>) -> Self {
        Self {
            engine,
            ballista_codec: BallistaPhysicalExtensionCodec::default(),
        }
    }
}

impl PhysicalExtensionCodec for VaireExecutorPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Ok(plan_bytes) = DuckDbScanPlanBytes::decode(buf)
            && let Ok(schema) = decode_schema_ipc(&plan_bytes.schema_ipc)
        {
            return Ok(Arc::new(DuckDbScanExec::new(
                plan_bytes.shard_table_name,
                schema,
                plan_bytes.projection,
                plan_bytes.filter_exprs,
                Arc::clone(&self.engine),
            )));
        }

        self.ballista_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(scan) = node.as_any().downcast_ref::<DuckDbScanExec>() {
            let schema_ipc = encode_schema_ipc(scan.projected_schema())?;

            let plan_bytes = DuckDbScanPlanBytes {
                shard_table_name: scan.shard_table_name().to_string(),
                schema_ipc,
                projection: scan.projection().clone(),
                filter_exprs: scan.filter_strings(),
                target_executor_id: None,
                replica_executor_ids: vec![],
            };

            buf.extend_from_slice(&plan_bytes.encode());
            return Ok(());
        }

        self.ballista_codec.try_encode(node, buf)
    }
}

/// Serialize an Arrow schema to bytes using the Arrow IPC file format, for
/// embedding in a transmitted scan plan.
fn encode_schema_ipc(schema: &SchemaRef) -> datafusion::error::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer =
            datafusion::arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref())?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Reconstruct an Arrow schema from Arrow IPC file-format bytes produced by
/// [`encode_schema_ipc`].
fn decode_schema_ipc(buf: &[u8]) -> datafusion::error::Result<SchemaRef> {
    let reader =
        datafusion::arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)?;
    Ok(reader.schema())
}
