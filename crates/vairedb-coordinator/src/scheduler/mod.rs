//! Embedded Ballista scheduler: distributed read planning, plan codecs, the
//! `RemoteDuckDbScanExec` node, and the shard-affinity task distribution policy.

mod affinity_policy;
mod codec;
mod logical_codec;
mod remote_scan_exec;
mod scheduler;

pub use affinity_policy::VaireAffinityPolicy;
pub use codec::VairePhysicalCodec;
pub use logical_codec::VaireLogicalCodec;
pub use remote_scan_exec::RemoteDuckDbScanExec;
pub use scheduler::{
    BallistaSchedulerHandle, SchedulerTableProvider, parse_data_type,
    refresh_ballista_catalog_tables, register_vairedb_catalog_schema, start_scheduler,
};
