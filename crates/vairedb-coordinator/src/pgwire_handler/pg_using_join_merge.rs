//! Give a `USING` or `NATURAL` join key the *merged* value PostgreSQL gives it, on the
//! two join types where the two sides can disagree.
//!
//! `USING (c)` does not just say what to join on. It says the result has **one** column
//! `c`, and PostgreSQL defines its value as `COALESCE(left.c, right.c)`:
//!
//! ```text
//! l = (1,2,3,4)   r = (1,2,3,5)
//!
//! postgres=# SELECT id FROM l FULL JOIN r USING (id) ORDER BY id;
//!  1  2  3  4  5
//! ```
//!
//! The row that exists only on the right reports `5` — its own key — because the left
//! side has no `id` to report there. DataFusion's `Join` node keeps both sides' `c`
//! instead, and the merge is faked twice over, both times by *picking* one of them and
//! never by combining them:
//!
//! * `Column::normalize_with_schemas_and_ambiguity_check` resolves an unqualified `c`
//!   against the join's schema, finds two fields named `c`, checks they belong to one
//!   `USING` set — and then, in its own words, "simply pick[s] the qualifier from the
//!   first match", which is the **left** one.
//! * `expand_wildcard` drops the duplicate rather than merging it: `exclude_using_columns`
//!   sorts the set and keeps one column per name, so `SELECT *` reports whichever
//!   qualifier sorts first.
//!
//! For an inner or left join, picking the left column is right by construction — an inner
//! join's two keys are equal, and a left join's unmatched row has no right key to prefer.
//! For a **full** or **right** join it is wrong, and wrong in the quiet way: `SELECT id
//! FROM l FULL JOIN r USING (id)` answers `1, 2, 3, 4, NULL`, a NULL key on a row whose
//! key PostgreSQL reports as `5`, with nothing to say the answer is not the answer.
//!
//! `NATURAL JOIN` needs no separate handling: DataFusion's SQL planner routes it through
//! the same `JoinConstraint::Using`, over every column the two sides share.
//!
//! ## The repair
//!
//! One `Projection` over the join, replacing **both** key columns with
//! `COALESCE(left.c, right.c)` under their own qualified names, and otherwise passing the
//! join's columns through unchanged. The projection declares the join's own schema
//! (`Projection::try_new_with_schema`), so nothing above it sees a new shape — it is the
//! two columns' *values* that change and nothing else.
//!
//! Both sides and not just the left, because the two fakes above disagree about which one
//! they read: name resolution takes the field that comes first in the schema (the left),
//! wildcard expansion the one that sorts first by qualifier (either). Overwriting both is
//! what makes `SELECT id` and `SELECT *` agree with each other, whatever the two tables
//! happen to be called.
//!
//! ## What this does not fix
//!
//! A key column reached by an **explicit qualifier** — `SELECT l.id, r.id FROM l FULL
//! JOIN r USING (id)` — where PostgreSQL still reports the raw per-side values, NULL and
//! all. After this rewrite both report the merged value.
//!
//! It is not a corner that was skipped; it is one DataFusion's plan cannot represent.
//! PostgreSQL's join output has *three* addressable names here (`id`, `l.id`, `r.id`) and
//! the merged one belongs to the join rather than to either side. A `DFSchema` has two
//! fields to put them in, and the merged column has to live in whichever of the two every
//! consumer reads — which, per the paragraph above, is both. Recorded as its own row in
//! `docs/specs/gap-analysis-join.md`, with a test pinning it, rather than left to be
//! discovered.
//!
//! ## Where this runs
//!
//! On the **logical plan**, in [`crate::pgwire_handler::parser::plan_select`], for the
//! same reason [`super::pg_aggregate_widening`] runs there: a rewrite registered on the
//! session context would run inside `create_physical_plan`, after the plan the client is
//! told about has been settled, and Describe would then describe a different plan than
//! the one that executes. Here it also has to run **after** the SQL planner, not before —
//! `SELECT *` is expanded and unqualified names are resolved during planning, so a rewrite
//! that ran first would be rewriting columns nothing referred to yet.

use std::collections::HashMap;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result};
use datafusion::logical_expr::{Expr, JoinConstraint, JoinType, LogicalPlan, Projection};

/// Merge the `USING` key columns of every full and right join in `plan`.
///
/// A plan with no such join comes back untouched. `with_subqueries`, because a full join
/// inside an `EXISTS` or an `IN` merges its keys the same way.
///
/// Called once, from `plan_select`. A second application would add a second projection
/// rather than recognize the first — the join it wraps is unchanged, so there is nothing
/// on it to see — but it cannot change an answer: the outer projection computes
/// `COALESCE(m, m)` over the merged values `m` the inner one produced, which is `m`. So
/// the cost of running it twice is a redundant plan node and not a wrong number, and the
/// guard that would avoid it (recognizing the projection from the join below it) is not
/// worth the walk it would need.
pub(crate) fn merge_using_join_keys(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(merge_keys_of_one_join)
        .map(|t| t.data)
}

/// Wrap `plan` in the merging projection if it is a join that needs one.
fn merge_keys_of_one_join(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Join(join) = &plan else {
        return Ok(Transformed::no(plan));
    };
    // Only a `USING`/`NATURAL` join has a merged column at all — an `ON` join's two key
    // columns stay two columns in PostgreSQL too, which is why the `ON` spelling of this
    // query has always been correct here.
    if join.join_constraint != JoinConstraint::Using {
        return Ok(Transformed::no(plan));
    }
    // Inner and left keys need no merge (see the module doc), and a semi, anti or mark
    // join carries only one side's columns, so there is no second key to merge with.
    if !matches!(join.join_type, JoinType::Full | JoinType::Right) {
        return Ok(Transformed::no(plan));
    }

    let schema = std::sync::Arc::clone(&join.schema);
    let mut merged: HashMap<Column, Expr> = HashMap::new();
    for (left, right) in &join.on {
        let (Expr::Column(left_key), Expr::Column(right_key)) = (left, right) else {
            // `USING` keys are plain columns by construction; anything else is not a
            // shape this rewrite knows how to merge, so leave the join alone.
            return Ok(Transformed::no(plan));
        };
        let (Ok(left_field), Ok(right_field)) = (
            schema.index_of_column(left_key),
            schema.index_of_column(right_key),
        ) else {
            return Ok(Transformed::no(plan));
        };
        // A merge across two different types would need a cast to keep the declared
        // schema, and casting the *wider* side into the narrower one can fail on a value
        // that the join itself would have carried. Left alone instead: the answer is no
        // worse than today's and the narrowing is not introduced. `USING` over two
        // differently-typed columns is rare, and PostgreSQL's merged column takes the
        // common type of the two rather than either side's.
        if schema.field(left_field).data_type() != schema.field(right_field).data_type() {
            return Ok(Transformed::no(plan));
        }
        let coalesced = datafusion::functions::core::expr_fn::coalesce(vec![
            Expr::Column(left_key.clone()),
            Expr::Column(right_key.clone()),
        ]);
        merged.insert(left_key.clone(), coalesced.clone());
        merged.insert(right_key.clone(), coalesced);
    }
    if merged.is_empty() {
        return Ok(Transformed::no(plan));
    }

    // The alias is what keeps the projection's fields named as the join's were, so a
    // parent still finds `l.id` where it left it.
    let projected = schema
        .columns()
        .into_iter()
        .map(|column| match merged.get(&column).cloned() {
            Some(coalesced) => coalesced.alias_qualified(column.relation.clone(), column.name),
            None => Expr::Column(column),
        })
        .collect();

    let projection = Projection::try_new_with_schema(
        projected,
        std::sync::Arc::new(plan),
        std::sync::Arc::clone(&schema),
    )?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    /// Two tables shaped like the module doc's example: keys 1–3 in common, `4` only on
    /// the left and `5` only on the right, so a full join has an unmatched row on each
    /// side and a right join has one on the right.
    ///
    /// `k` is common to both by name as well, so `NATURAL JOIN` has two columns to merge
    /// and `USING (id)` leaves one of them alone — the pair that says the rewrite merges
    /// the join's keys and not merely the columns that share a name.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register(
            &ctx,
            "l",
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
            "v",
            &["a", "b", "c", "d"],
        );
        register(
            &ctx,
            "r",
            &[1, 2, 3, 5],
            &[10, 20, 30, 50],
            "w",
            &["x", "y", "z", "q"],
        );
        ctx
    }

    fn register(
        ctx: &SessionContext,
        name: &str,
        ids: &[i32],
        ks: &[i32],
        payload_name: &str,
        payload: &[&str],
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("k", DataType::Int32, true),
            Field::new(payload_name, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(ks.to_vec())),
                Arc::new(StringArray::from(payload.to_vec())),
            ],
        )
        .unwrap();
        ctx.register_batch(name, batch).unwrap();
    }

    /// The plan `plan_select` hands the rest of the read path: planned, then merged.
    async fn read_path_plan(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        merge_using_join_keys(plan).unwrap()
    }

    /// Every row of the answer, one string per row, columns joined by `|` and a NULL
    /// rendered as `NULL`. Sorted, because a join's row order is not defined.
    async fn collect(ctx: &SessionContext, plan: LogicalPlan) -> Vec<String> {
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let cells: Vec<String> = (0..batch.num_columns())
                    .map(|col| {
                        let column = batch.column(col);
                        if column.is_null(row) {
                            "NULL".to_string()
                        } else {
                            datafusion::arrow::util::display::array_value_to_string(column, row)
                                .unwrap()
                        }
                    })
                    .collect();
                out.push(cells.join("|"));
            }
        }
        out.sort();
        out
    }

    /// The answer to `sql` on the read path's own plan — planned, merged, executed.
    async fn answer(sql: &str) -> Vec<String> {
        let ctx = ctx();
        let plan = read_path_plan(&ctx, sql).await;
        collect(&ctx, plan).await
    }

    /// The names the client is told, in order — the shape `SELECT *` reports.
    async fn column_names(sql: &str) -> Vec<String> {
        let ctx = ctx();
        read_path_plan(&ctx, sql)
            .await
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    // The defect, and PostgreSQL's answer for it: `5` and not NULL on the row that only
    // the right side has.
    #[tokio::test]
    async fn a_full_join_using_reports_the_merged_key() {
        assert_eq!(
            answer("SELECT id FROM l FULL JOIN r USING (id)").await,
            ["1", "2", "3", "4", "5"]
        );
    }

    // The same query the other way round: a right join's unmatched row is on the side
    // whose key resolution does *not* pick, so it is wrong for the same reason.
    #[tokio::test]
    async fn a_right_join_using_reports_the_merged_key() {
        assert_eq!(
            answer("SELECT id FROM l RIGHT JOIN r USING (id)").await,
            ["1", "2", "3", "5"]
        );
    }

    // `SELECT *` reaches the key column through wildcard expansion rather than name
    // resolution, and the two read *different* fields — the reason both are overwritten.
    // PostgreSQL answers `5 | | | 50 | q` for the right-only row.
    #[tokio::test]
    async fn a_full_join_using_wildcard_reports_the_merged_key() {
        assert_eq!(
            answer("SELECT * FROM l FULL JOIN r USING (id)").await,
            [
                "1|10|a|10|x",
                "2|20|b|20|y",
                "3|30|c|30|z",
                "4|40|d|NULL|NULL",
                "5|NULL|NULL|50|q",
            ]
        );
    }

    // The shape must not move: PostgreSQL reports one `id` and both `k`s for this query,
    // which is what the un-merged plan already reported. A projection that declared its
    // own derived schema instead of the join's could quietly add a column here.
    #[tokio::test]
    async fn the_wildcard_shape_is_unchanged() {
        assert_eq!(
            column_names("SELECT * FROM l FULL JOIN r USING (id)").await,
            ["id", "k", "v", "k", "w"]
        );
    }

    // `NATURAL` is the same `JoinConstraint::Using` over every shared column, so both
    // `id` and `k` merge — and `SELECT *` reports each of them once.
    #[tokio::test]
    async fn a_natural_full_join_merges_every_shared_column() {
        assert_eq!(
            column_names("SELECT * FROM l NATURAL FULL JOIN r").await,
            ["id", "k", "v", "w"]
        );
        assert_eq!(
            answer("SELECT * FROM l NATURAL FULL JOIN r").await,
            [
                "1|10|a|x",
                "2|20|b|y",
                "3|30|c|z",
                "4|40|d|NULL",
                "5|50|NULL|q",
            ]
        );
    }

    // A multi-column `USING` merges each key independently.
    #[tokio::test]
    async fn a_full_join_using_two_columns_merges_both() {
        assert_eq!(
            answer("SELECT id, k FROM l FULL JOIN r USING (id, k)").await,
            ["1|10", "2|20", "3|30", "4|40", "5|50"]
        );
    }

    // `k` is shared by name but is not a key of this join, so it must stay two columns
    // with two values — the rewrite merges what `USING` named and nothing else.
    #[tokio::test]
    async fn a_column_that_is_not_a_key_is_not_merged() {
        assert_eq!(
            answer("SELECT l.k, r.k FROM l FULL JOIN r USING (id)").await,
            ["10|10", "20|20", "30|30", "40|NULL", "NULL|50",]
        );
    }

    // The merged column has to be merged everywhere it is referred to and not only in the
    // select list. Each of these resolves the same unqualified name through a different
    // part of the planner, and every one of them has to see the right-only key `5`.
    #[tokio::test]
    async fn the_merged_key_is_merged_in_every_clause() {
        for (sql, expected) in [
            (
                "SELECT id FROM l FULL JOIN r USING (id) ORDER BY id",
                vec!["1", "2", "3", "4", "5"],
            ),
            (
                "SELECT id FROM l FULL JOIN r USING (id) GROUP BY id HAVING id > 2",
                vec!["3", "4", "5"],
            ),
            ("SELECT max(id) FROM l FULL JOIN r USING (id)", vec!["5"]),
            (
                "SELECT id FROM l FULL JOIN r USING (id) UNION SELECT 6",
                vec!["1", "2", "3", "4", "5", "6"],
            ),
        ] {
            assert_eq!(answer(sql).await, expected, "for `{sql}`");
        }
    }

    /// `WHERE` is the one clause that cannot reach the merged column, and it is refused
    /// rather than answered wrongly — so it is outside what this rewrite is for, and
    /// pinned here because the rewrite is where somebody will come looking for it.
    ///
    /// DataFusion plans a `WHERE` predicate without the join's `USING` set in hand, so an
    /// unqualified key column is ambiguous to it: two fields are named `id` and nothing
    /// tells it they are one column. PostgreSQL accepts the predicate. That is a planning
    /// refusal on **every** `USING` and `NATURAL` join and not only the two this rewrite
    /// merges, which is what says it is upstream's and not this one's — and `l.id` is a
    /// spelling of the same predicate that works today, and now carries the merged value.
    #[tokio::test]
    async fn a_where_clause_on_the_key_is_refused_by_the_planner() {
        for sql in [
            "SELECT id FROM l FULL JOIN r USING (id) WHERE id > 2",
            "SELECT id FROM l JOIN r USING (id) WHERE id > 2",
            "SELECT id FROM l NATURAL FULL JOIN r WHERE id > 2",
        ] {
            let err = ctx()
                .state()
                .create_logical_plan(sql)
                .await
                .expect_err("upstream refuses an unqualified USING key in WHERE")
                .to_string();
            assert!(
                err.contains("Ambiguous reference to unqualified field id"),
                "`{sql}` failed for some other reason: {err}"
            );
        }
        // The qualified spelling plans, and answers PostgreSQL's rows because the rewrite
        // put the merged value in `l.id`.
        assert_eq!(
            answer("SELECT l.id FROM l FULL JOIN r USING (id) WHERE l.id > 2").await,
            ["3", "4", "5"]
        );
    }

    // Reached through a subquery rather than at the top of the plan, which is what
    // `with_subqueries` and the bottom-up walk are for.
    #[tokio::test]
    async fn a_full_join_nested_in_a_subquery_merges_too() {
        assert_eq!(
            answer("SELECT id FROM (SELECT id FROM l FULL JOIN r USING (id)) t WHERE id = 5").await,
            ["5"]
        );
    }

    /// The divergence the module doc records, pinned so it is a decision and not a
    /// surprise: PostgreSQL answers `4 | ` and ` | 5` for the two unmatched rows here,
    /// because a qualified `l.id` is the left side's own key and nothing merged. VaireDB
    /// answers the merged value in both, since the merged column has to live in the fields
    /// every other consumer reads and a `DFSchema` has nowhere else to put it.
    ///
    /// The `ON` spelling is the one that answers this question correctly today — see
    /// `an_inner_or_left_join_is_left_alone`, which plans it untouched.
    #[tokio::test]
    async fn a_qualified_key_reports_the_merged_value_too() {
        assert_eq!(
            answer("SELECT l.id, r.id FROM l FULL JOIN r USING (id)").await,
            ["1|1", "2|2", "3|3", "4|4", "5|5"]
        );
    }

    // Inner and left joins are already right, and merging them would buy nothing while
    // costing the qualified-access divergence the module doc records. So the rewrite has
    // to leave them alone — pinned by asserting the plan is untouched, since the *answers*
    // agree either way and would not catch it.
    #[tokio::test]
    async fn an_inner_or_left_join_is_left_alone() {
        for sql in [
            "SELECT id FROM l JOIN r USING (id)",
            "SELECT id FROM l LEFT JOIN r USING (id)",
            "SELECT l.id, r.id FROM l FULL JOIN r ON l.id = r.id",
        ] {
            let ctx = ctx();
            let planned = ctx.state().create_logical_plan(sql).await.unwrap();
            let merged = merge_using_join_keys(planned.clone()).unwrap();
            assert_eq!(
                format!("{}", merged.display_indent()),
                format!("{}", planned.display_indent()),
                "`{sql}` needs no merge and must come back unchanged"
            );
        }
    }

    // Applying the rewrite twice must answer what applying it once answers. It does add a
    // second projection — see the note on [`merge_using_join_keys`] — and this is the
    // property that says the extra node is redundant rather than harmful: the outer
    // `coalesce` sees the merged value in both of its arguments.
    #[tokio::test]
    async fn merging_twice_answers_what_merging_once_answers() {
        let ctx = ctx();
        let sql = "SELECT * FROM l FULL JOIN r USING (id)";
        let once = read_path_plan(&ctx, sql).await;
        let twice = merge_using_join_keys(once.clone()).unwrap();
        assert_eq!(collect(&ctx, twice).await, collect(&ctx, once).await);
    }

    // A `USING` key whose two sides are typed differently is declined rather than merged,
    // because keeping the declared schema would need a cast that can fail on a value the
    // join would have carried. The answer stays the un-merged one, which is what the gap
    // row records.
    #[tokio::test]
    async fn a_key_whose_sides_disagree_on_type_is_declined() {
        let ctx = SessionContext::new();
        register(
            &ctx,
            "l",
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
            "v",
            &["a", "b", "c", "d"],
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 5]))],
        )
        .unwrap();
        ctx.register_batch("wide", batch).unwrap();

        let sql = "SELECT id FROM l FULL JOIN wide USING (id)";
        let planned = ctx.state().create_logical_plan(sql).await.unwrap();
        let merged = merge_using_join_keys(planned.clone()).unwrap();
        assert_eq!(
            format!("{}", merged.display_indent()),
            format!("{}", planned.display_indent())
        );
    }
}
