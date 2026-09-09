//! Embedded Ballista scheduler: distributed read planning, plan codecs, the
//! `RemoteDuckDbScanExec` node, and the shard-affinity task distribution policy.

mod affinity_policy;
mod codec;
mod filter_pushdown;
mod logical_codec;
mod nested_loop_join_one_task;
mod remote_scan_exec;
mod scheduler;
mod window_partition_sort;

pub use affinity_policy::VaireAffinityPolicy;
pub use codec::VairePhysicalCodec;
pub use filter_pushdown::OpaqueTextColumns;
pub use logical_codec::VaireLogicalCodec;
pub use remote_scan_exec::RemoteDuckDbScanExec;
pub use scheduler::{
    BallistaSchedulerHandle, SchedulerTableProvider, refresh_catalog_tables,
    register_vairedb_catalog_schema, start_scheduler,
};
