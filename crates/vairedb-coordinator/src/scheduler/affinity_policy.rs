//! Shard-affinity task distribution policy for the embedded Ballista scheduler.
//!
//! When an executor pulls for work, this policy binds scan tasks to the executor
//! that holds the relevant shard: first tasks where it is the shard's primary,
//! then (as a fallback) tasks where it holds a replica. Affinity targets are
//! derived by walking the stage's physical plan for `RemoteDuckDbScanExec` nodes.

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::serde::protobuf::{AvailableTaskSlots, job_status};
use ballista_core::serde::scheduler::PartitionId;
use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
use ballista_scheduler::state::execution_graph::{TaskDescription, create_task_info};
use ballista_scheduler::state::execution_stage::RunningStage;
use ballista_scheduler::state::task_manager::JobInfoCache;
use datafusion::physical_plan::ExecutionPlan;

use super::remote_scan_exec::RemoteDuckDbScanExec;

/// Ballista `DistributionPolicy` that routes scan tasks to the executor holding
/// the shard (primary first, replicas as fallback) based on
/// `RemoteDuckDbScanExec` affinity hints.
#[derive(Debug)]
pub struct VaireAffinityPolicy;

#[async_trait::async_trait]
impl DistributionPolicy for VaireAffinityPolicy {
    async fn bind_tasks(
        &self,
        mut slots: Vec<&mut AvailableTaskSlots>,
        running_jobs: Arc<HashMap<String, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>> {
        let mut schedulable_tasks: Vec<BoundTask> = Vec::new();

        // This policy binds to a single executor's slots per call.
        let Some(slot) = slots.first_mut() else {
            return Ok(schedulable_tasks);
        };
        if slot.slots == 0 {
            return Ok(schedulable_tasks);
        }
        let executor_id = slot.executor_id.clone();

        for (job_id, job_info) in running_jobs.iter() {
            if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
                continue;
            }

            let mut graph = job_info.execution_graph.write().await;
            let session_id = graph.session_id().to_string();
            let mut black_list = vec![];

            while let Some((running_stage, task_id_gen)) = graph.fetch_running_stage(&black_list) {
                let affinity_map = extract_affinity_map(running_stage);

                let runnable_partitions: Vec<usize> = running_stage
                    .task_infos
                    .iter()
                    .enumerate()
                    .filter(|(_, info)| info.is_none())
                    .map(|(idx, _)| idx)
                    .collect();

                if runnable_partitions.is_empty() {
                    black_list.push(running_stage.stage_id);
                    continue;
                }

                // Classify once: partitions this executor is the primary for,
                // then those it can serve as a replica. Partitions owned by
                // another executor are dropped entirely.
                let mut primary: Vec<usize> = Vec::new();
                let mut replica: Vec<usize> = Vec::new();
                for &partition_id in &runnable_partitions {
                    match affinity_map.get(&partition_id) {
                        Some(target) if !target.primary.is_empty() => {
                            if target.primary == executor_id {
                                primary.push(partition_id);
                            } else if target.replicas.contains(&executor_id) {
                                replica.push(partition_id);
                            }
                        }
                        // No affinity hint: any executor may run it.
                        _ => primary.push(partition_id),
                    }
                }

                // Single binding loop over the priority-ordered partitions.
                let mut bound_any = false;
                for partition_id in primary.into_iter().chain(replica) {
                    let slot = slots.first_mut().unwrap();
                    if slot.slots == 0 {
                        break;
                    }
                    let task = bind_partition_task(
                        running_stage,
                        task_id_gen,
                        partition_id,
                        job_id,
                        &session_id,
                        &executor_id,
                    );
                    schedulable_tasks.push(task);
                    slot.slots -= 1;
                    bound_any = true;
                }

                if !bound_any {
                    black_list.push(running_stage.stage_id);
                }
            }
        }

        Ok(schedulable_tasks)
    }

    fn name(&self) -> &str {
        "VaireAffinityPolicy"
    }
}

/// Allocate the next task id, register the task on `running_stage` for
/// `partition_id`, and build the `BoundTask` assigning it to `executor_id`.
/// Both the primary and replica binding passes funnel through here so the
/// task-id bookkeeping and `TaskDescription` assembly have a single definition.
fn bind_partition_task(
    running_stage: &mut RunningStage,
    task_id_gen: &mut usize,
    partition_id: usize,
    job_id: &str,
    session_id: &str,
    executor_id: &str,
) -> BoundTask {
    let task_id = *task_id_gen;
    *task_id_gen += 1;
    running_stage.task_infos[partition_id] =
        Some(create_task_info(executor_id.to_string(), task_id));

    let partition = PartitionId {
        job_id: job_id.to_string(),
        stage_id: running_stage.stage_id,
        partition_id,
    };
    let task_desc = TaskDescription {
        session_id: session_id.to_string(),
        partition,
        stage_attempt_num: running_stage.stage_attempt_num,
        task_id,
        task_attempt: running_stage.task_failure_numbers[partition_id],
        plan: running_stage.plan.clone(),
        session_config: running_stage.session_config.clone(),
    };
    (executor_id.to_string(), task_desc)
}

/// Preferred executors for a partition: its shard's primary and any replicas.
struct AffinityTarget {
    primary: String,
    replicas: Vec<String>,
}

/// Build a map from partition index to its `AffinityTarget` by walking the
/// stage's physical plan for `RemoteDuckDbScanExec` leaves.
fn extract_affinity_map(stage: &RunningStage) -> HashMap<usize, AffinityTarget> {
    let mut map = HashMap::new();
    walk_plan_for_affinity(stage.plan.as_ref(), &mut map, 0);
    map
}

/// Recursively walk `plan`, recording affinity for each `RemoteDuckDbScanExec`
/// keyed by its partition index, and return how many output partitions the
/// subtree contributes. `UnionExec` lays its children out contiguously starting
/// at `partition_offset`; other nodes propagate their input's partitioning.
fn walk_plan_for_affinity(
    plan: &dyn ExecutionPlan,
    map: &mut HashMap<usize, AffinityTarget>,
    partition_offset: usize,
) -> usize {
    if let Some(remote_scan) = plan.as_any().downcast_ref::<RemoteDuckDbScanExec>() {
        if let Some(target) = remote_scan.target_executor_id() {
            map.insert(
                partition_offset,
                AffinityTarget {
                    primary: target.to_string(),
                    replicas: remote_scan.replica_executor_ids().to_vec(),
                },
            );
        }
        return 1;
    }

    // evolve in match for future expansion.
    if plan.name() == "UnionExec" {
        let mut offset = partition_offset;
        for child in plan.children() {
            let count = walk_plan_for_affinity(child.as_ref(), map, offset);
            offset += count;
        }
        return offset - partition_offset;
    }

    for child in plan.children() {
        walk_plan_for_affinity(child.as_ref(), map, partition_offset);
    }
    plan.properties().output_partitioning().partition_count()
}
