//! Shard-affinity task distribution policy for the embedded Ballista scheduler.
//!
//! When an executor pulls for work, this policy binds scan tasks to the executor
//! that holds the relevant shard: first tasks where it is the shard's primary,
//! then (as a fallback) tasks where it holds a replica. Affinity targets are
//! derived by walking the stage's physical plan for `RemoteDuckDbScanExec` nodes.

use std::collections::HashMap;
use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::serde::protobuf::{AvailableTaskSlots, job_status};
use ballista_core::serde::scheduler::PartitionId;
use ballista_scheduler::cluster::{BoundTask, DistributionPolicy};
use ballista_scheduler::state::execution_graph::{TaskDescription, create_task_info};
use ballista_scheduler::state::execution_stage::RunningStage;
use ballista_scheduler::state::task_manager::JobInfoCache;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;

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
        slots: Vec<&mut AvailableTaskSlots>,
        running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>> {
        let mut schedulable_tasks: Vec<BoundTask> = Vec::new();

        // This policy binds to a single executor's slots per call. Taken by value so the
        // binding loop below can decrement the same borrow it counts against.
        let Some(slot) = slots.into_iter().next() else {
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
    job_id: &JobId,
    session_id: &str,
    executor_id: &str,
) -> BoundTask {
    let task_id = *task_id_gen;
    *task_id_gen += 1;
    running_stage.task_infos[partition_id] =
        Some(create_task_info(executor_id.to_string(), task_id));

    let partition = PartitionId {
        job_id: job_id.clone(),
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
    if let Some(remote_scan) = plan.downcast_ref::<RemoteDuckDbScanExec>() {
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

    // Downcast rather than compare `plan.name()`: the name is a display string, and a
    // typo in it would silently fall through to the pass-through arm below, mapping every
    // shard's scan onto the first partition.
    if plan.downcast_ref::<UnionExec>().is_some() {
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

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    /// A remote scan of `shard`, held by `primary` with `replicas` behind it.
    fn scan(shard: &str, primary: &str, replicas: &[&str]) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        Arc::new(RemoteDuckDbScanExec::new(
            shard.to_string(),
            schema,
            None,
            Vec::new(),
            Some(primary.to_string()),
            replicas.iter().map(|r| r.to_string()).collect(),
        ))
    }

    /// The mapping the whole policy rests on: a union lays its children out contiguously,
    /// so the Nth shard's scan is the Nth partition, and that number is what
    /// `bind_tasks` looks a task's affinity up by. Get the offsets wrong and every task
    /// is routed by another shard's hint — a correct answer read over the network instead
    /// of off local disk, which no assertion on the result would catch.
    #[test]
    fn a_unions_children_are_mapped_to_consecutive_partitions() {
        let union = UnionExec::try_new(vec![
            scan("t_shard0", "node-a", &["node-b"]),
            scan("t_shard1", "node-b", &[]),
            scan("t_shard2", "node-c", &["node-a", "node-b"]),
        ])
        .expect("the test union is well formed");

        let mut map = HashMap::new();
        let count = walk_plan_for_affinity(union.as_ref(), &mut map, 0);

        assert_eq!(count, 3, "one partition per child");
        assert_eq!(map[&0].primary, "node-a");
        assert_eq!(map[&1].primary, "node-b");
        assert_eq!(map[&2].primary, "node-c");
        assert_eq!(map[&2].replicas, vec!["node-a", "node-b"]);
    }

    /// A scan under any other node is still found, because the plan a stage holds has the
    /// stage's own operators above its scans — an affinity hint reachable only from a bare
    /// scan would be a hint that never applies.
    #[test]
    fn a_scan_below_another_operator_is_still_found() {
        let union = UnionExec::try_new(vec![
            scan("t_shard0", "node-a", &[]),
            scan("t_shard1", "node-b", &[]),
        ])
        .expect("the test union is well formed");
        let plan = Arc::new(CoalescePartitionsExec::new(union)) as Arc<dyn ExecutionPlan>;

        let mut map = HashMap::new();
        walk_plan_for_affinity(plan.as_ref(), &mut map, 0);

        assert_eq!(map[&0].primary, "node-a");
        assert_eq!(map[&1].primary, "node-b");
    }

    /// A scan with no primary is left out of the map entirely, which is how `bind_tasks`
    /// reads "any executor may run this" — an empty-string primary there would instead
    /// mean "an executor named `""` owns it", and the task would never be bound.
    #[test]
    fn a_scan_with_no_affinity_is_absent_from_the_map() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let plan = Arc::new(RemoteDuckDbScanExec::new(
            "t".to_string(),
            schema,
            None,
            Vec::new(),
            None,
            Vec::new(),
        )) as Arc<dyn ExecutionPlan>;

        let mut map = HashMap::new();
        assert_eq!(walk_plan_for_affinity(plan.as_ref(), &mut map, 0), 1);
        assert!(map.is_empty());
    }
}
