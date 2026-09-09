//! Re-sort a window's input on its `PARTITION BY` columns when the plan's reason for
//! not sorting them will not survive being distributed.
//!
//! ## The failure
//!
//! `SELECT row_number() OVER (PARTITION BY g ORDER BY x) FROM t WHERE g = 1` failed at
//! runtime with `Expects PARTITION BY expression to be ordered`, wrapped in an internal
//! error naming DataFusion. Dropping the `WHERE`, or making it `g > 0`, or filtering on
//! any column other than `g`, made the same query answer — the predicate had to be an
//! **equality on the partition column**.
//!
//! ## Why
//!
//! `FilterExec: g = 1` tells the plan above it that `g` is a constant, and a constant
//! column is trivially ordered. So `EnforceSorting` correctly concludes that
//! `PARTITION BY g` needs no sort of its own and emits `SortExec: expr=[x ASC]`, and
//! `BoundedWindowAggExec` is built in `Sorted` mode on the strength of that conclusion.
//!
//! Then Ballista cuts the plan into stages at the shuffle. The window and its sort land
//! in a stage whose input is a shuffle reader, which reports no equivalences at all —
//! the knowledge that `g` is constant stayed behind in the stage that had the filter.
//! The window re-derives its ordering against that input, finds nothing ordering `g`,
//! and fails. The plan was valid as one piece and invalid once split, which is a hazard
//! of distributing a plan rather than a mistake in either half.
//!
//! ## The repair
//!
//! Put the partition columns into the window's sort, so the requirement is met by an
//! ordering physically present inside its own stage rather than by an inference that
//! does not cross the boundary. Where the plan believed *every* ordering key was
//! constant there is no sort to widen at all — the window sits straight on the
//! repartition — so one is inserted instead.
//!
//! The rule fires only on that shape: a window in `Sorted` mode whose input does not
//! already carry every partition column in its ordering. A plan that was going to work
//! is left exactly as it was, and a sort is only ever added, so no result changes.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, InputOrderMode};

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct SortWindowPartitionsWithinTheStage;

impl PhysicalOptimizerRule for SortWindowPartitionsWithinTheStage {
    fn name(&self) -> &str {
        "sort_window_partitions_within_the_stage"
    }

    /// The rule only replaces a sort's key list, so no schema anywhere changes.
    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(sort) = window_input_needing_partition_keys(&node) else {
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(node.with_new_children(vec![sort])?))
        })
        .data()
    }
}

/// The replacement input for `node`, if `node` is a window whose sort has to be
/// widened. `None` leaves the node alone, which is the answer for every plan that is
/// not the shape described in the module documentation.
fn window_input_needing_partition_keys(
    node: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let window = node.downcast_ref::<BoundedWindowAggExec>()?;

    // Only `Sorted` mode asserts anything about the partition columns' order; the other
    // modes search for partitions rather than relying on them being contiguous, so they
    // cannot be broken by an equivalence that failed to cross the stage boundary.
    if !matches!(window.input_order_mode, InputOrderMode::Sorted) {
        return None;
    }

    let expr = window.window_expr().first()?;
    let partition_by = expr.partition_by();
    if partition_by.is_empty() {
        return None;
    }

    let input = window.input();

    // The ordering the input actually carries in the data, as opposed to the one it can
    // argue for. `output_ordering` reports sort keys and not constants, so a column the
    // plan believes is constant is absent here — which is precisely the case to repair.
    let carried: &[PhysicalSortExpr] = match input.downcast_ref::<SortExec>() {
        Some(sort) => sort.expr(),
        None => input.output_ordering().map(|o| o.as_ref()).unwrap_or(&[]),
    };
    if partition_by
        .iter()
        .all(|p| carried.iter().any(|s| s.expr.eq(p)))
    {
        return None;
    }

    // Partition columns first — the window needs its partitions contiguous before it
    // needs the rows within one ordered — then the window's own ordering, then whatever
    // the input was already sorted by, so a requirement above the window is not lost.
    // `LexOrdering` drops a repeated key, so a column named twice is listed once.
    let ordering = LexOrdering::new(
        partition_by
            .iter()
            .map(|p| PhysicalSortExpr::new_default(Arc::clone(p)))
            .chain(expr.order_by().iter().cloned())
            .chain(carried.iter().cloned()),
    )?;

    match input.downcast_ref::<SortExec>() {
        // Replacing the sort rather than stacking on top of it: a sort over a sort would
        // discard the inner ordering, and only the outer one reaches the window.
        // `preserve_partitioning` carries over unchanged — whether each stream may be
        // sorted on its own is a property of the input below, which this does not touch.
        Some(sort) => Some(Arc::new(
            SortExec::new(ordering, Arc::clone(sort.input()))
                .with_preserve_partitioning(sort.preserve_partitioning()),
        )),
        // No sort at all, which is what happens when the plan believes *every* ordering
        // key is constant. One has to be added rather than widened.
        //
        // Partitioning is preserved whenever there is more than one stream to preserve:
        // this rule runs after `EnforceDistribution`, so a sort that instead asked its
        // input to be a single partition would leave a distribution requirement that
        // nothing downstream is still going to satisfy.
        None => {
            let per_partition = input.output_partitioning().partition_count() > 1;
            Some(Arc::new(
                SortExec::new(ordering, Arc::clone(input))
                    .with_preserve_partitioning(per_partition),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::displayable;

    /// Two contexts over the same table: one plain, one with the rule appended after the
    /// default physical optimizer set, exactly as `start_scheduler` installs it.
    ///
    /// `g` is the partition column and `x` the ordering one, three groups of two rows.
    /// Multiple target partitions matter: without a repartition there is no shuffle for a
    /// distributed plan to be cut at, and the rule's shape never arises.
    fn contexts() -> (SessionContext, SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int32, false),
            Field::new("x", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();

        let register = |ctx: SessionContext| {
            ctx.register_batch("t", batch.clone()).unwrap();
            ctx
        };

        let plain = register(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let repaired = register(SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(SortWindowPartitionsWithinTheStage))
                .build(),
        ));
        (plain, repaired)
    }

    async fn plan_text(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        displayable(plan.as_ref()).indent(false).to_string()
    }

    /// The rows `sql` answers with, rendered as a table so a difference reads as one.
    async fn rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    /// The window's own sort, as one line of the plan.
    fn sort_line(plan: &str) -> String {
        plan.lines()
            .find(|l| l.trim_start().starts_with("SortExec:"))
            .unwrap_or("<no sort in the plan>")
            .trim()
            .to_string()
    }

    // The defect, in plan form. An equality on the partition column makes `g` a constant
    // to the coordinator's optimizer, which then sorts by `x` alone — a conclusion that
    // does not survive the plan being cut into stages, because the shuffle reader on the
    // other side reports no equivalences.
    #[tokio::test]
    async fn a_filter_on_the_partition_column_drops_it_from_the_sort() {
        let sql = "SELECT row_number() OVER (PARTITION BY g ORDER BY x) FROM t WHERE g = 1";
        let (plain, repaired) = contexts();

        let before = sort_line(&plan_text(&plain, sql).await);
        assert!(
            !before.contains("g@"),
            "the unrepaired sort should have dropped the partition column: {before}"
        );

        let after = sort_line(&plan_text(&repaired, sql).await);
        assert!(
            after.contains("g@") && after.contains("x@"),
            "the repaired sort should order by the partition column first: {after}"
        );
    }

    // A plan that already sorts its partition column is not this rule's business, and
    // must come through untouched — the sort it has is the one it keeps.
    #[tokio::test]
    async fn a_window_whose_sort_already_covers_the_partition_is_untouched() {
        for sql in [
            "SELECT row_number() OVER (PARTITION BY g ORDER BY x) FROM t",
            "SELECT row_number() OVER (PARTITION BY g ORDER BY x) FROM t WHERE g > 0",
            "SELECT row_number() OVER (ORDER BY x) FROM t",
            "SELECT sum(x) OVER () FROM t",
        ] {
            let (plain, repaired) = contexts();
            assert_eq!(
                plan_text(&plain, sql).await,
                plan_text(&repaired, sql).await,
                "the rule changed a plan it had no business changing: {sql}"
            );
        }
    }

    // The repair must not change the answer. Six rows, one group selected, numbered in
    // `x` order — that is what PostgreSQL returns for this query and what the rule has to
    // preserve while adding a key to the sort.
    #[tokio::test]
    async fn the_repaired_plan_returns_the_same_rows() {
        let sql = "SELECT x, row_number() OVER (PARTITION BY g ORDER BY x) AS rn \
                   FROM t WHERE g = 1 ORDER BY x";
        let (plain, repaired) = contexts();

        let expected = rows(&plain, sql).await;
        assert!(expected.contains("| 10 | 1  |"), "{expected}");
        assert!(expected.contains("| 20 | 2  |"), "{expected}");
        assert_eq!(rows(&repaired, sql).await, expected);
    }

    // Every ordering key constant: the plan has no sort at all, so the repair has to
    // insert one carrying both partition columns rather than widen an existing one. And
    // it has to do so without asking for a distribution nothing will now provide — a sort
    // that demanded a single input partition would make the plan unrunnable, so
    // collecting the rows is the assertion that matters as much as the plan shape.
    #[tokio::test]
    async fn a_sort_is_inserted_where_the_plan_had_none() {
        let sql = "SELECT x, row_number() OVER (PARTITION BY g, x ORDER BY x) AS rn \
                   FROM t WHERE g = 1 AND x = 10";
        let (plain, repaired) = contexts();

        assert_eq!(
            sort_line(&plan_text(&plain, sql).await),
            "<no sort in the plan>",
            "the premise of this test is that the unrepaired plan has no sort"
        );

        let line = sort_line(&plan_text(&repaired, sql).await);
        assert!(
            line.contains("g@") && line.contains("x@"),
            "both partition columns should be in the inserted sort: {line}"
        );
        assert_eq!(rows(&repaired, sql).await, rows(&plain, sql).await);
    }
}
