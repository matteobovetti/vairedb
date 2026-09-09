//! Collect the probe side of a nested-loop join whose output is decided on the build
//! side, so the whole join runs as one task.
//!
//! ## The failure
//!
//! A join with no equijoin key — `EXISTS`, `NOT EXISTS`, or an `ON` clause carrying only
//! an inequality — is planned as a `NestedLoopJoinExec`. On a cluster, five of its join
//! types answered wrongly and none of them said so. Measured over `l(k) = 10, 20, 30,
//! NULL` and `r(k) = 20, 99, 50`, three shards each:
//!
//! | Query | Answered | PostgreSQL |
//! |---|---|---|
//! | `WHERE EXISTS (SELECT 1 FROM r)` | no rows | every row |
//! | `WHERE EXISTS (SELECT 1 FROM r WHERE r.k > l.k)` | no rows | `10, 20, 30` |
//! | `WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.k > l.k)` | no rows | `NULL` |
//! | `l LEFT JOIN r ON r.k > l.k` | the 7 matched rows | those 7 **and** the NULL row |
//! | `l FULL JOIN r ON r.k > l.k` | the same 7 | the same 8 |
//!
//! An `INNER` join and a `RIGHT` join over the identical plan shape were correct, and a
//! `CROSS JOIN` was correct. The same plan is correct in a single process, which is why
//! five axes of single-process measurement never saw any of it.
//!
//! ## Why
//!
//! `NestedLoopJoinExec` collects its left input and streams its right, so for the join
//! types whose output is decided on the *build* side — `Left`, `Full`, `LeftSemi`,
//! `LeftAnti`, `LeftMark` — the rows are not known until every probe partition has been
//! consumed. DataFusion coordinates that with a counter shared by the partitions:
//! `collect_left_input` seeds `probe_threads_counter` with the probe side's partition
//! count, each partition decrements it as it finishes, and the one that brings it to zero
//! emits the build-side rows from the shared match bitmap.
//!
//! Ballista runs each partition of a stage as a **separate task in a separate process**.
//! Every task therefore builds its own `JoinLeftData`, seeds its own counter with the
//! plan's probe partition count — three, here — and decrements it exactly once. No task
//! ever reaches zero, so the build-side emission never happens anywhere. For `LeftSemi`
//! and `LeftAnti`, that emission is the entire result, hence no rows at all. For `Left`
//! and `Full` the matched rows still stream out per partition and only the unmatched left
//! rows are lost, which is the subtler shape of the same defect. `Inner`, `Right`,
//! `RightSemi` and `RightAnti` are decided as the probe streams, need no final pass, and
//! were never affected.
//!
//! Nothing in the counter can be made to work across processes without changing Ballista,
//! and the counter is the only thing that makes the parallel probe correct.
//!
//! ## The repair
//!
//! Put a `CoalescePartitionsExec` on the probe side, so the probe partition count is one,
//! the single task's counter reaches zero, and it emits. Ballista then cuts the probe
//! subtree into its own stage — the shuffle it already builds for the *build* side, which
//! is why the collected left input has always been read correctly — and the join itself
//! becomes a one-partition stage: one task, reading both sides in full.
//!
//! That costs the probe's parallelism, which is the honest price. It is what the join
//! semantics ask for once the coordinating counter cannot be shared: correct and serial in
//! place of parallel and empty. Only the shapes that were broken pay it — the rule fires
//! on a `NestedLoopJoinExec` whose join type needs the final pass and whose probe side has
//! more than one partition, so an `INNER` or `RIGHT` nested-loop join, a `CROSS JOIN`, and
//! every join carrying an equijoin key (planned as a hash or sort-merge join, which
//! partition by key and need no shared counter) keep every partition they had.
//!
//! Being a physical rule appended after the defaults is what makes it reliable: it sees
//! the probe partitioning `EnforceDistribution` settled on, and it runs in the session
//! state that plans every distributed read, which is the only place the plan Ballista is
//! about to cut into stages exists.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::logical_expr::JoinType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct RunTheNestedLoopJoinInOneTask;

impl PhysicalOptimizerRule for RunTheNestedLoopJoinInOneTask {
    fn name(&self) -> &str {
        "run_the_nested_loop_join_in_one_task"
    }

    /// The rule only changes how many partitions the probe side arrives in, so no schema
    /// anywhere changes.
    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if !probe_side_must_be_one_partition(&node) {
                return Ok(Transformed::no(node));
            }
            let children = node.children();
            let build = Arc::clone(children[0]);
            let probe = Arc::new(CoalescePartitionsExec::new(Arc::clone(children[1])));
            Ok(Transformed::yes(
                node.with_new_children(vec![build, probe])?,
            ))
        })
        .data()
    }
}

/// Whether `node` is a nested-loop join that will lose rows unless its probe side is
/// collected into one partition — the shape the module documentation describes.
fn probe_side_must_be_one_partition(node: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() else {
        return false;
    };
    if !decided_on_the_build_side(*join.join_type()) {
        return false;
    }
    // A probe side that is already one partition seeds the counter with one, so the task
    // running it reaches zero on its own and there is nothing to repair.
    join.right().output_partitioning().partition_count() > 1
}

/// Whether a join type's output can only be known once *every* probe row has been seen —
/// DataFusion's `need_produce_result_in_final`, which is private to it.
///
/// Listed exhaustively rather than by a negation so that a join type added upstream is a
/// compile error here rather than a silently unrepaired plan.
fn decided_on_the_build_side(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Left
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::LeftMark => true,
        JoinType::Inner
        | JoinType::Right
        | JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::RightMark => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::JoinSide;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{Partitioning, collect, displayable};
    use datafusion::prelude::SessionConfig;

    /// `l.k = 10, 20, 30, NULL` — the build side of the cluster measurement's fixture.
    const L_KEYS: [Option<i32>; 4] = [Some(10), Some(20), Some(30), None];
    /// `r.k = 20, 99, 50` — the probe side.
    const R_KEYS: [Option<i32>; 3] = [Some(20), Some(99), Some(50)];

    /// The five join types whose output is only known once every probe row has been seen.
    const DECIDED_ON_THE_BUILD_SIDE: [JoinType; 5] = [
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::Left,
        JoinType::Full,
        JoinType::LeftMark,
    ];
    /// The five decided as the probe streams, which this rule must leave alone.
    const DECIDED_AS_THE_PROBE_STREAMS: [JoinType; 5] = [
        JoinType::Inner,
        JoinType::Right,
        JoinType::RightSemi,
        JoinType::RightAnti,
        JoinType::RightMark,
    ];

    fn key_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]))
    }

    /// A one-partition scan of `keys`.
    fn scan(keys: &[Option<i32>]) -> Arc<dyn ExecutionPlan> {
        let schema = key_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(keys.to_vec()))],
        )
        .expect("the test batch is well formed");
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("the test scan is well formed")
    }

    /// `r.k > l.k`, the correlation that has no equijoin key and so is carried as a
    /// nested-loop join's filter rather than as a hash key.
    fn greater_than_filter() -> JoinFilter {
        let intermediate = Arc::new(Schema::new(vec![
            Field::new("l_k", DataType::Int32, true),
            Field::new("r_k", DataType::Int32, true),
        ]));
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("r_k", 1)),
                datafusion::logical_expr::Operator::Gt,
                Arc::new(Column::new("l_k", 0)),
            )),
            vec![
                ColumnIndex {
                    index: 0,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 0,
                    side: JoinSide::Right,
                },
            ],
            intermediate,
        )
    }

    /// The plan shape the cluster measurement produced, built directly rather than through
    /// SQL: a `NestedLoopJoinExec` of `join_type` over a one-partition build side and a
    /// `probe_partitions`-way probe side.
    ///
    /// Built by hand because SQL cannot be made to produce it in one process. With local
    /// statistics in hand DataFusion's `JoinSelection` swaps a small build side to the
    /// probe — a `LeftSemi` becomes a `RightSemi` — and a `MemTable`'s single partition is
    /// never repartitioned. On a cluster neither happens: `RemoteDuckDbScanExec` reports no
    /// statistics, so there is nothing to swap on, and each shard is its own partition. So
    /// the shape has to be stated, or the rule would be tested against a plan that never
    /// reaches it.
    fn nested_loop_join(join_type: JoinType, probe_partitions: usize) -> Arc<dyn ExecutionPlan> {
        let probe = if probe_partitions > 1 {
            Arc::new(
                RepartitionExec::try_new(
                    scan(&R_KEYS),
                    Partitioning::RoundRobinBatch(probe_partitions),
                )
                .expect("the test repartition is well formed"),
            ) as Arc<dyn ExecutionPlan>
        } else {
            scan(&R_KEYS)
        };
        Arc::new(
            NestedLoopJoinExec::try_new(
                scan(&L_KEYS),
                probe,
                Some(greater_than_filter()),
                &join_type,
                None,
            )
            .expect("the test join is well formed"),
        )
    }

    fn optimized(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        RunTheNestedLoopJoinInOneTask
            .optimize(plan, &ConfigOptions::default())
            .expect("the rule must not fail")
    }

    fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(false).to_string()
    }

    /// Every value of `plan`'s first column, sorted, with a NULL rendered as `-1` so it is
    /// visible. Collected across all output partitions, the way a client sees the answer.
    async fn keys(plan: Arc<dyn ExecutionPlan>) -> Vec<i32> {
        let ctx = SessionContext::new();
        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("the plan must run");
        let mut keys = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("an int4 column");
            for row in 0..batch.num_rows() {
                keys.push(if column.is_null(row) {
                    -1
                } else {
                    column.value(row)
                });
            }
        }
        keys.sort_unstable();
        keys
    }

    // The premise of the whole module: without the rule, these joins are executed across
    // as many partitions as the probe side has, and each of those partitions is a separate
    // Ballista task with its own copy of the counter that decides when the build-side rows
    // are emitted. In one process the partitions share that counter and the answer is
    // still right, which is why the plan is what has to be asserted here — the wrong rows
    // only appear once the partitions are processes, and the e2e suite asserts those.
    #[tokio::test]
    async fn the_unrepaired_probe_side_is_partitioned() {
        for join_type in DECIDED_ON_THE_BUILD_SIDE {
            let plan = nested_loop_join(join_type, 3);
            assert_eq!(
                plan.children()[1].output_partitioning().partition_count(),
                3,
                "{join_type:?} must start out with a partitioned probe side"
            );
            assert!(
                plan.output_partitioning().partition_count() > 1,
                "{join_type:?} must start out running in more than one task: {}",
                plan_text(&plan)
            );
        }
    }

    // The repair, in plan form: one partition out of the join, which is one Ballista task,
    // which is the one place the probe counter can reach zero.
    #[tokio::test]
    async fn the_repaired_probe_side_is_one_partition() {
        for join_type in DECIDED_ON_THE_BUILD_SIDE {
            let plan = optimized(nested_loop_join(join_type, 3));
            let text = plan_text(&plan);
            assert_eq!(
                plan.output_partitioning().partition_count(),
                1,
                "{join_type:?} must run in one task after the repair: {text}"
            );
            assert!(
                text.starts_with("NestedLoopJoinExec:"),
                "{join_type:?} must still be a nested-loop join at the root: {text}"
            );
            assert!(
                plan.children()[1]
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_some(),
                "{join_type:?} must have gained the coalesce on its probe side: {text}"
            );
            // The build side is left exactly as it was — it is already collected whole by
            // every task, and wrapping it would add a stage boundary for nothing.
            assert!(
                plan.children()[0]
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_none(),
                "{join_type:?} must not have gained a coalesce on its build side: {text}"
            );
        }
    }

    // The repair must not change the answer in process, where the unrepaired plan is
    // already correct. This is what says the added coalesce neither loses nor duplicates a
    // row, independently of the distribution it exists to fix.
    //
    // The expected values are PostgreSQL's, for `l.k = 10, 20, 30, NULL`, `r.k = 20, 99,
    // 50` and the correlation `r.k > l.k`: every non-NULL left key has something above it,
    // and `r.k > NULL` is NULL for every candidate, so the NULL row is the only one with
    // no match.
    #[tokio::test]
    async fn the_repaired_plan_returns_the_same_rows() {
        for (join_type, expected) in [
            (JoinType::LeftSemi, vec![10, 20, 30]),
            (JoinType::LeftAnti, vec![-1]),
            // Three matches for 10, two each for 20 and 30, and the NULL row unmatched.
            (JoinType::Left, vec![-1, 10, 10, 10, 20, 20, 30, 30]),
            (JoinType::Full, vec![-1, 10, 10, 10, 20, 20, 30, 30]),
            // `LeftMark` returns every left row once, with a boolean second column.
            (JoinType::LeftMark, vec![-1, 10, 20, 30]),
        ] {
            // Built twice rather than shared: a `RepartitionExec` initializes its channels
            // on first execution and panics if a plan holding it is run again, so the two
            // measurements need their own copies.
            assert_eq!(
                keys(nested_loop_join(join_type, 3)).await,
                expected,
                "{join_type:?} was already right in one process"
            );
            assert_eq!(
                keys(optimized(nested_loop_join(join_type, 3))).await,
                expected,
                "{join_type:?} must answer the same rows after the repair"
            );
        }
    }

    // The join types decided as the probe streams keep every partition they had. Paying
    // the serialization for these would be a cost with nothing bought — an `INNER` and a
    // `RIGHT` nested-loop join were measured correct on the cluster before this rule
    // existed, over the identical plan shape.
    #[tokio::test]
    async fn a_probe_driven_nested_loop_join_is_untouched() {
        for join_type in DECIDED_AS_THE_PROBE_STREAMS {
            let plan = nested_loop_join(join_type, 3);
            assert_eq!(
                plan_text(&optimized(Arc::clone(&plan))),
                plan_text(&plan),
                "the rule changed a {join_type:?} join it had no business changing"
            );
        }
    }

    // A probe side that is already one partition seeds the counter with one, so the single
    // task reaches zero on its own. Nothing to repair, and nothing may be added.
    #[tokio::test]
    async fn a_single_partition_probe_side_is_untouched() {
        for join_type in DECIDED_ON_THE_BUILD_SIDE {
            let plan = nested_loop_join(join_type, 1);
            assert_eq!(
                plan_text(&optimized(Arc::clone(&plan))),
                plan_text(&plan),
                "the rule added a coalesce to a {join_type:?} join that did not need one"
            );
        }
    }

    // Running twice must not stack a second coalesce: the first pass leaves the probe side
    // at one partition, which is exactly the condition the rule tests.
    #[tokio::test]
    async fn the_rule_is_idempotent() {
        for join_type in DECIDED_ON_THE_BUILD_SIDE {
            let once = optimized(nested_loop_join(join_type, 3));
            assert_eq!(
                plan_text(&optimized(Arc::clone(&once))),
                plan_text(&once),
                "{join_type:?} gained a second coalesce on a second pass"
            );
        }
    }

    /// Two contexts over `l(k)` and `r(k)`: one plain, one with the rule appended after
    /// the default physical optimizer set, exactly as `start_scheduler` installs it.
    fn contexts() -> (SessionContext, SessionContext) {
        let register = |ctx: SessionContext| {
            for (name, keys) in [("l", L_KEYS.to_vec()), ("r", R_KEYS.to_vec())] {
                let batch =
                    RecordBatch::try_new(key_schema(), vec![Arc::new(Int32Array::from(keys))])
                        .expect("the test batch is well formed");
                ctx.register_batch(name, batch)
                    .expect("registering the test table");
            }
            ctx
        };

        let config = SessionConfig::new().with_target_partitions(4);
        let plain = register(SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config.clone())
                .build(),
        ));
        let repaired = register(SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config)
                .with_physical_optimizer_rule(Arc::new(RunTheNestedLoopJoinInOneTask))
                .build(),
        ));
        (plain, repaired)
    }

    // A join carrying an equijoin key is not a nested-loop join at all — it is hash- or
    // sort-merge-partitioned by that key, so each task holds the whole of its own key
    // range and needs no counter shared with anyone. Those plans must come through
    // untouched, or the rule would serialize the ordinary case. Asserted through SQL
    // because the point is which operator the planner chooses.
    #[tokio::test]
    async fn a_join_with_an_equijoin_key_is_untouched() {
        for sql in [
            "SELECT k FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
            "SELECT k FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
            "SELECT l.k FROM l LEFT JOIN r ON r.k = l.k",
            "SELECT l.k FROM l FULL JOIN r ON r.k = l.k",
            "SELECT l.k FROM l JOIN r ON r.k = l.k",
        ] {
            let (plain, repaired) = contexts();
            let plan_of = |ctx: SessionContext| async move {
                let plan = ctx
                    .sql(sql)
                    .await
                    .unwrap_or_else(|e| panic!("`{sql}` must plan: {e}"))
                    .create_physical_plan()
                    .await
                    .unwrap_or_else(|e| panic!("`{sql}` must have a physical plan: {e}"));
                plan_text(&plan)
            };
            let before = plan_of(plain).await;
            assert!(
                !before.contains("NestedLoopJoinExec"),
                "`{sql}` must plan on its key, not as a nested loop: {before}"
            );
            assert_eq!(
                before,
                plan_of(repaired).await,
                "the rule changed a plan it had no business changing: {sql}"
            );
        }
    }
}
