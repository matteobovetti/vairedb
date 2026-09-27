//! Embedded Ballista scheduler: distributed read planning, the plan codec, the
//! `RemoteDuckDbScanExec` node, and the shard-affinity task distribution policy.
//!
//! Two of the modules here (`nested_loop_join_one_task`, `window_partition_sort`) are
//! physical optimizer rules that exist for one reason: a plan DataFusion considers correct
//! can be wrong once Ballista cuts it into stages that run in separate processes. They are
//! separate rules because they repair unrelated plan shapes.

mod affinity_policy;
mod filter_pushdown;
mod nested_loop_join_one_task;
mod plan_codec;
mod remote_scan_exec;
mod scheduler;
mod window_partition_sort;

#[cfg(test)]
mod scheduler_test_helper;

pub use affinity_policy::VaireAffinityPolicy;
pub use filter_pushdown::OpaqueTextColumns;
pub use plan_codec::{VaireLogicalCodec, VairePhysicalCodec};
pub use remote_scan_exec::RemoteDuckDbScanExec;
pub use scheduler::{
    BallistaSchedulerHandle, SchedulerTableProvider, refresh_catalog_tables,
    register_vairedb_catalog_schema, start_scheduler,
};

/// The two halves of a read-path context, re-exported so the read-path test helpers build
/// one the way `start_scheduler` does rather than approximating it — see
/// [`crate::pgwire_handler::read_path_test_helper::context`].
#[cfg(test)]
pub(crate) use scheduler::{register_postgres_functions, with_postgres_sql_options};
