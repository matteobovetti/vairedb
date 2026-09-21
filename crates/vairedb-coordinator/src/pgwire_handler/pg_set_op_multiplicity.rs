//! Answer `INTERSECT ALL` and `EXCEPT ALL` with the row counts PostgreSQL answers, by
//! numbering the duplicates inside each branch before the set operation's join sees them.
//!
//! ```text
//! l(k int4) = 1, 1, 1, 2
//! r(k int4) = 1, 1, 3
//!
//! SELECT k FROM l INTERSECT ALL SELECT k FROM r
//! PostgreSQL  1, 1        -- min(3, 2) copies of 1
//! DataFusion  1, 1, 1     -- the semi join keeps every left row that has a match
//! after       1, 1
//!
//! SELECT k FROM l EXCEPT ALL SELECT k FROM r
//! PostgreSQL  1, 2        -- max(3 - 2, 0) copies of 1, and the 2
//! DataFusion  2           -- the anti join drops every left row that has a match
//! after       1, 2
//! ```
//!
//! The `ALL` in a set operation is not a synonym for "no `DISTINCT`": it makes the operation
//! *count* duplicates. `INTERSECT ALL` keeps a row `min(m, n)` times and `EXCEPT ALL` keeps
//! it `max(m - n, 0)` times, where `m` and `n` are how often it appears on the left and on
//! the right. DataFusion lowers both quantifiers onto the same semi/anti join and so answers
//! `m` and `0` — a row count, which is exactly what an analytical client goes on to
//! aggregate. `UNION ALL` is untouched: concatenation is all its `ALL` asks for.
//!
//! ## The rewrite
//!
//! Number the rows *within each distinct value tuple* on both sides, then join on the value
//! tuple **and** that number:
//!
//! ```text
//! Projection: l.k                                         -- back to the original schema
//!   LeftSemi Join: l.k = r.k, lhs_rn = rhs_rn, NULLs equal
//!     Projection: l.k, row_number() PARTITION BY [l.k] AS lhs_rn
//!       WindowAggr: row_number() PARTITION BY [l.k]
//!         <left branch>
//!     Projection: r.k, row_number() PARTITION BY [r.k] AS rhs_rn
//!       WindowAggr: row_number() PARTITION BY [r.k]
//!         <right branch>
//! ```
//!
//! The left copies of a value are numbered `1..m` and the right copies `1..n`, so the
//! *i*-th left copy has a partner exactly when `i <= n`. A semi join therefore keeps
//! `min(m, n)` of them and an anti join keeps the `m - n` whose number no right row
//! reaches — which is PostgreSQL's answer for both, including when `n` is zero. `NULL` is
//! grouped with `NULL` throughout: the partitioning puts equal-and-null values in one
//! partition, and the join is the `NullEquality::NullEqualsNull` one DataFusion already
//! builds for a set operation, which is what PostgreSQL's `INTERSECT` does too.
//!
//! The gap analysis recorded this as a gap with no rewrite — "there is no rewrite that
//! preserves multiplicity". The window function is what makes one possible, and its cost is
//! real: `PARTITION BY` every column means a partition-wise sort of both branches where the
//! `DISTINCT` forms need only a hash join. That is the price of the right answer, and only
//! the statements that ask for `ALL` pay it.
//!
//! ## Why it takes two passes, and where the marker comes from
//!
//! The rewrite needs the branches' **columns**, to partition by all of them, so it has to
//! run on the plan. But on the plan the two quantifiers are no longer distinguishable with
//! certainty. `LogicalPlanBuilder::intersect_or_except` lowers the `DISTINCT` form to
//! `Join(Distinct(left), right, …)` and the `ALL` form to `Join(left, right, …)`, so
//! "the left child is not a `Distinct`" looks like the test for `ALL` — and it is not one:
//! `(A UNION B) INTERSECT ALL C` has a `Distinct` left child of its own, because that is
//! what a distinct `UNION` lowers to. Reading the shape would answer that query as
//! `INTERSECT` and quietly drop rows.
//!
//! So the quantifier is carried from the AST, where it is still written down, by
//! [`mark_multiplicity_set_operations`]: it wraps the **right** branch of every `ALL`
//! intersection or difference in a derived table named [`MARKER_RELATION`]. The right branch
//! is the one to wrap because a semi or anti join's output schema is its *left* input's, so
//! the marker cannot reach the client — no column name, qualifier or type changes.
//! [`preserve_set_operation_multiplicity`] then rewrites exactly the joins that carry it, and
//! removes the marker as it does, so nothing downstream ever sees one.
//!
//! ## The invariant, and its second half
//!
//! **No `INTERSECT ALL` or `EXCEPT ALL` can answer a row count PostgreSQL would not.** The
//! rewrite alone cannot hold that, because it only fires where the marker arrived. So the
//! same pass refuses two residues, both recognized from the join `intersect_or_except`
//! builds and nothing else does — a semi or anti join with no filter and `NullEqualsNull`
//! (a client's own `LEFT SEMI JOIN` has neither: an explicit join's predicate lands in
//! `filter`, and its `null_equality` is `NullEqualsNothing`):
//!
//! * a marker that reached such a join with keys that do not cover every column of both
//!   branches — the rewrite pairs the branches positionally, and without that it cannot.
//! * such a join carrying **no** marker and no `Distinct` on its left: a set operation that
//!   got here without passing the AST. That is a bug in the pass order rather than in the
//!   statement, but it is a bug that answers wrongly, so it is refused rather than trusted.
//!
//! Neither is reachable from today's planner, which is the point: the pair means "marked and
//! rewritten, or refused", never "answered as if the `ALL` had not been written".
//!
//! `INTERSECT ALL BY NAME` and `EXCEPT ALL BY NAME` are refused in the AST instead of
//! marked. They are DuckDB spellings with no PostgreSQL equivalent, and PostgreSQL is the
//! contract.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, JoinType, NullEquality, Result as DFResult, plan_err};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    Expr, ExprFunctionExt, Join, LogicalPlan, LogicalPlanBuilder, SubqueryAlias,
};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::sqlparser::ast::{SetExpr, SetOperator, Statement};

/// The derived table [`mark_multiplicity_set_operations`] wraps an `ALL` set operation's
/// right branch in, and the only thing that tells the plan pass a join counts duplicates.
///
/// Never reaches a client: it names the right input of a semi or anti join, whose output
/// schema is the left input's, and the plan pass removes it.
const MARKER_RELATION: &str = "__vaire_set_op_all";

/// The row number added to the left branch. Distinct from the right branch's, because a set
/// operation between two selects over the *same* table would otherwise offer the join one
/// name from both sides. Deliberately not a name starting with [`MARKER_RELATION`], so that
/// "no marker survived" stays a question a test can ask of a plan's text.
const LEFT_ROW_NUMBER: &str = "__vaire_duplicate_lhs";

/// The row number added to the right branch. See [`LEFT_ROW_NUMBER`].
const RIGHT_ROW_NUMBER: &str = "__vaire_duplicate_rhs";

/// Mark every `INTERSECT ALL` / `EXCEPT ALL` / `MINUS ALL` in `stmt` for the plan pass, and
/// refuse the `BY NAME` spellings.
///
/// Runs on the AST because the quantifier is only unambiguous there. See the module doc.
pub(super) fn mark_multiplicity_set_operations(stmt: &mut Statement) -> PgWireResult<()> {
    use crate::sqlparser::ast::{Query, VisitMut, VisitorMut};

    struct Marker;

    impl VisitorMut for Marker {
        type Break = PgWireError;

        // On the `Query` rather than on the `Select`, because a set operation *is* a query
        // body: its branches are not `Select`s that a select visitor would reach.
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<PgWireError> {
            match mark_set_expr(&mut query.body) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }

    match stmt.visit(&mut Marker) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// The per-body half of [`mark_multiplicity_set_operations`].
///
/// A nested `SetExpr::Query` — and so the branch this pass has just wrapped — is left to the
/// visitor, which reaches that `Query` itself.
fn mark_set_expr(body: &mut SetExpr) -> PgWireResult<()> {
    use crate::sqlparser::ast::SetQuantifier;

    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = body
    else {
        return Ok(());
    };
    // `MINUS` is `EXCEPT` under another name, so it counts duplicates the same way.
    let counts_duplicates = matches!(
        op,
        SetOperator::Intersect | SetOperator::Except | SetOperator::Minus
    );
    if counts_duplicates {
        match set_quantifier {
            // Marking an already-marked branch would nest one wrapper inside another, which
            // the plan pass would read as a marker over a plain projection and refuse. The
            // guard keeps this pass idempotent instead.
            SetQuantifier::All if !is_marked(right) => mark_branch(right)?,
            SetQuantifier::AllByName => return Err(by_name_unsupported(op)),
            _ => {}
        }
    }
    mark_set_expr(left)?;
    mark_set_expr(right)
}

/// Whether `body` is already the marker wrapper.
fn is_marked(body: &SetExpr) -> bool {
    use crate::sqlparser::ast::TableFactor;

    let SetExpr::Select(select) = body else {
        return false;
    };
    let Some(table) = select.from.first() else {
        return false;
    };
    matches!(
        &table.relation,
        TableFactor::Derived {
            alias: Some(alias),
            ..
        } if alias.name.value == MARKER_RELATION
    )
}

/// Replace `branch` with `SELECT * FROM (<branch>) AS __vaire_set_op_all`.
///
/// Built by parsing a template and swapping `branch` into it, rather than by rendering
/// `branch` back to SQL and reparsing the result: this is a whole query the client wrote, and
/// `Display` on an AST is lossy.
fn mark_branch(branch: &mut Box<SetExpr>) -> PgWireResult<()> {
    use crate::sqlparser::ast::{Statement as SqlStatement, TableFactor};
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    let template = format!("SELECT * FROM (SELECT 1) AS {MARKER_RELATION}");
    let mut wrapper = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&template)
        .ok()
        .and_then(|mut parser| parser.parse_statements().ok())
        .and_then(|mut statements| match statements.pop() {
            Some(SqlStatement::Query(query)) => Some(query.body),
            _ => None,
        })
        .ok_or_else(marker_template_failed)?;

    let SetExpr::Select(select) = wrapper.as_mut() else {
        return Err(marker_template_failed());
    };
    let Some(TableFactor::Derived { subquery, .. }) =
        select.from.first_mut().map(|table| &mut table.relation)
    else {
        return Err(marker_template_failed());
    };
    // Only the body is swapped in: the template's `Query` supplies the empty `WITH`,
    // `ORDER BY` and `LIMIT` a derived table needs, and a branch that had any of its own
    // carries them inside itself as a `SetExpr::Query`.
    std::mem::swap(&mut subquery.body, branch);
    *branch = wrapper;
    Ok(())
}

/// `XX000`. Unreachable — the template is a constant this module's own tests parse — and
/// reported rather than unwrapped so that a future edit to it cannot panic a session.
fn marker_template_failed() -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        "could not prepare INTERSECT ALL / EXCEPT ALL for planning".to_string(),
    )
}

/// `0A000` for the `BY NAME` quantifiers, which have no PostgreSQL spelling to be compatible
/// with.
fn by_name_unsupported(op: &SetOperator) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{op} ALL BY NAME is not supported: it is a DuckDB extension with no PostgreSQL \
             equivalent, and the branches of a PostgreSQL set operation are matched by \
             position; write the branches with their columns in the same order and use \
             {op} ALL"
        ),
    )
}

/// Rewrite every marked set operation in `plan` so that it counts duplicates, and refuse the
/// residue the pair of passes cannot answer.
///
/// Runs in [`super::parser::plan_select`] on the planned but not yet optimized plan: the
/// semi/anti join it rewrites is what the SQL planner produces, and the optimizer would
/// otherwise have reordered it out of recognition.
pub(super) fn preserve_set_operation_multiplicity(plan: LogicalPlan) -> PgWireResult<LogicalPlan> {
    let mut refusal = None;
    // Bottom-up, so `A INTERSECT ALL B INTERSECT ALL C` has its inner operation rewritten
    // into the projection the outer one then reads. `_with_subqueries`, so an operation
    // inside a subquery or a CTE is rewritten too.
    let rewritten = plan.transform_up_with_subqueries(|node| {
        if refusal.is_some() {
            return Ok(Transformed::no(node));
        }
        let LogicalPlan::Join(join) = node else {
            return Ok(Transformed::no(node));
        };
        match classify(&join) {
            Shape::NotASetOperation => Ok(Transformed::no(LogicalPlan::Join(join))),
            Shape::Marked => Ok(Transformed::yes(count_duplicates(join)?)),
            Shape::MarkedButUnrewritable => {
                refusal = Some(unrewritable(&join));
                Ok(Transformed::no(LogicalPlan::Join(join)))
            }
            Shape::Unmarked => {
                refusal = Some(unmarked_all(&join));
                Ok(Transformed::no(LogicalPlan::Join(join)))
            }
        }
    });
    if let Some(e) = refusal {
        return Err(e);
    }
    rewritten
        .map(|transformed| transformed.data)
        .map_err(|e| rewrite_failed(&e.to_string()))
}

/// What a `Join` node is, as far as this pass is concerned.
enum Shape {
    /// Not the join a set operation lowers to, or one whose `DISTINCT` answer is already
    /// right.
    NotASetOperation,
    /// A marked `ALL` set operation this pass can rewrite.
    Marked,
    /// A marked `ALL` set operation whose keys do not cover both branches.
    MarkedButUnrewritable,
    /// A set operation that reached the plan without the AST marker.
    Unmarked,
}

fn classify(join: &Join) -> Shape {
    // Everything below only holds of the join `intersect_or_except` builds, so a client's own
    // join never reaches the two refusals. See the module doc.
    if !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        || join.filter.is_some()
        || join.null_equality != NullEquality::NullEqualsNull
    {
        return Shape::NotASetOperation;
    }
    let keys_cover_both_branches = !join.on.is_empty()
        && join.on.len() == join.left.schema().fields().len()
        && join.on.len() == join.right.schema().fields().len();

    if unmark(&join.right).is_some() {
        return if keys_cover_both_branches {
            Shape::Marked
        } else {
            Shape::MarkedButUnrewritable
        };
    }
    // The `DISTINCT` forms are the ones the planner wraps in a `Distinct`, and they are
    // already correct. Anything else of this exact shape is a set operation that did not pass
    // the AST.
    if keys_cover_both_branches && !matches!(join.left.as_ref(), LogicalPlan::Distinct(_)) {
        return Shape::Unmarked;
    }
    Shape::NotASetOperation
}

/// The branch under the marker wrapper, or `None` if `plan` does not carry one.
///
/// The marker arrives under a variable number of layers that all only rename: the wrapper is
/// `SELECT * FROM (<branch>) AS __vaire_set_op_all`, which the planner turns into a projection
/// of the alias's columns over the alias itself, and `intersect_or_except` then wraps both
/// branches in another alias whenever their qualified names would collide in the join. So the
/// search descends through aliases and column-only projections until it finds the marker or
/// something that is not a renaming, and answers with the branch as the client wrote it.
///
/// Dropping those layers loses nothing: they qualify the right branch's columns, and a semi or
/// anti join does not output them — the rewrite reads the right branch's names off the plan it
/// builds rather than off the join it replaces.
///
/// A marker belonging to a *nested* `ALL` operation on the right cannot be found by mistake,
/// because [`preserve_set_operation_multiplicity`] works bottom-up: the inner operation has
/// already been rewritten, and its marker removed, before the outer one is classified.
fn unmark(plan: &LogicalPlan) -> Option<&Arc<LogicalPlan>> {
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                if alias.table() == MARKER_RELATION {
                    return Some(input);
                }
                node = input;
            }
            LogicalPlan::Projection(projection) if renames_only(&projection.expr) => {
                node = &projection.input;
            }
            _ => return None,
        }
    }
}

/// Whether `exprs` only pass columns through, under a new name or their own — the one kind of
/// projection [`unmark`] may descend past without changing what the branch computes.
fn renames_only(exprs: &[Expr]) -> bool {
    exprs.iter().all(|expr| match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Column(_)),
        _ => false,
    })
}

/// Rebuild `join` so that the *i*-th copy of a value on the left is matched against the
/// *i*-th copy on the right.
fn count_duplicates(join: Join) -> DFResult<LogicalPlan> {
    let Join {
        left,
        right,
        join_type,
        ..
    } = join;
    // The schema the set operation must keep: a semi or anti join's is its left input's, so
    // these are the columns the projection at the top restores, unchanged and in order.
    let output = left.schema().columns();

    // The marker comes off here, and nowhere else: it has said all it has to say, and leaving
    // it would put a relation named after this module in a plan a client can `EXPLAIN`.
    let right_branch = match unmark(&right) {
        Some(branch) => Arc::clone(branch),
        None => right,
    };
    let left_numbered = number_duplicates(Arc::unwrap_or_clone(left), LEFT_ROW_NUMBER)?;
    let right_numbered = number_duplicates(Arc::unwrap_or_clone(right_branch), RIGHT_ROW_NUMBER)?;

    // Paired by position, exactly as `intersect_or_except` paired them — which is sound
    // because `classify` established that the keys cover every column of both branches.
    let mut left_keys = output.clone();
    left_keys.push(Column::from_name(LEFT_ROW_NUMBER));
    let mut right_keys = right_numbered.schema().columns();
    // The row number is the column `number_duplicates` appended; naming it explicitly keeps
    // the value columns paired by position and the number paired by name.
    let last = right_keys.len() - 1;
    right_keys[last] = Column::from_name(RIGHT_ROW_NUMBER);

    LogicalPlanBuilder::from(left_numbered)
        .join_detailed(
            right_numbered,
            join_type,
            (left_keys, right_keys),
            None,
            NullEquality::NullEqualsNull,
        )?
        .project(output.into_iter().map(Expr::Column))?
        .build()
}

/// Add a column numbering the rows of `branch` within each distinct value tuple, named
/// `alias`.
fn number_duplicates(branch: LogicalPlan, alias: &str) -> DFResult<LogicalPlan> {
    let columns: Vec<Expr> = branch
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    // No `ORDER BY`: all the rewrite asks of the numbering is that it be a bijection onto
    // `1..n` within the partition, which `row_number` is however the rows arrive.
    let numbering = row_number().partition_by(columns).build()?;
    let numbered = LogicalPlanBuilder::from(branch)
        .window(vec![numbering])?
        .build()?;

    // Read the numbering back as the last column of the window node rather than by
    // reconstructing the name it was given, and alias it: the name a window function gets
    // spells out its partitioning, so two branches over the same table would otherwise offer
    // the join above one name from both sides.
    let mut projection = numbered.schema().columns();
    let Some(window_column) = projection.pop() else {
        return plan_err!("the window function added no column to {alias}");
    };
    let mut projection: Vec<Expr> = projection.into_iter().map(Expr::Column).collect();
    projection.push(Expr::Column(window_column).alias(alias));
    LogicalPlanBuilder::from(numbered)
        .project(projection)?
        .build()
}

/// The `ALL` operator a semi or anti join spells, and the `DISTINCT` one a message points at.
fn operator_of(join: &Join) -> (&'static str, &'static str) {
    match join.join_type {
        JoinType::LeftSemi => ("INTERSECT ALL", "INTERSECT"),
        _ => ("EXCEPT ALL", "EXCEPT"),
    }
}

/// `0A000` for a marked set operation whose join does not compare the branches on every
/// column.
fn unrewritable(join: &Join) -> PgWireError {
    let (all, distinct) = operator_of(join);
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{all} is not supported for this statement: counting duplicates needs the branches \
             compared on every column, and this one was planned as a join comparing only some \
             of them, so it would answer a row count PostgreSQL does not; use {distinct}, \
             which compares the rows as sets"
        ),
    )
}

/// `0A000` for a set operation that reached the plan without the AST marker — a bug in the
/// pass order rather than in the statement, refused because the alternative is a wrong row
/// count.
fn unmarked_all(join: &Join) -> PgWireError {
    let (all, distinct) = operator_of(join);
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{all} is not supported for this statement: it reached the planner without the \
             duplicate-counting rewrite, and the join it was planned as keeps every matching \
             row rather than one per match; use {distinct}, which compares the rows as sets"
        ),
    )
}

/// `XX000` for a rewrite that produced an invalid plan — unreachable, and reported rather
/// than swallowed so that a wrong answer is never the failure mode.
fn rewrite_failed(detail: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("could not plan INTERSECT ALL / EXCEPT ALL: {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Extension;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    };
    use datafusion_proto::logical_plan::LogicalExtensionCodec;

    /// `l(a int4, b int4)` and `r(a int4, b int4)` — the pair the e2e set-operation tests use.
    ///
    /// ```text
    /// l = (1, 1), (1, 1), (1, 1), (2, 2), (3, NULL), (3, NULL)
    /// r = (1, 1), (1, 1), (3, NULL), (4, 4)
    /// ```
    ///
    /// The counts are what make the three cases distinguishable: `(1, 1)` appears more often
    /// on the left than on the right, `(4, 4)` only on the right, `(2, 2)` only on the left,
    /// and `(3, NULL)` on both — so a rewrite that dropped the NULL-equals-NULL grouping
    /// would answer a different number of rows.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = || {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ]))
        };
        let l = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3, 3])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    None,
                ])),
            ],
        )
        .unwrap();
        let r = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(1), Some(1), None, Some(4)])),
            ],
        )
        .unwrap();
        ctx.register_batch("l", l).unwrap();
        ctx.register_batch("r", r).unwrap();
        ctx
    }

    fn parse(sql: &str) -> Statement {
        use crate::sqlparser::dialect::PostgreSqlDialect;
        use crate::sqlparser::parser::Parser;

        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .expect("lexes")
            .parse_statements()
            .expect("parses")
            .pop()
            .expect("one statement")
    }

    /// Mark `sql`, plan it, and rewrite the plan — the two passes in the order the read path
    /// runs them, with nothing else in between.
    async fn planned(ctx: &SessionContext, sql: &str) -> PgWireResult<LogicalPlan> {
        let mut stmt = parse(sql);
        mark_multiplicity_set_operations(&mut stmt)?;
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
            .await
            .expect("statement plans");
        preserve_set_operation_multiplicity(plan)
    }

    /// The rows `sql` answers, sorted, so that a multiset comparison is an equality.
    async fn rows(sql: &str) -> Vec<Vec<Option<i32>>> {
        let ctx = ctx();
        let plan = planned(&ctx, sql).await.expect("not refused");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("plans physically")
            .collect()
            .await
            .expect("executes");
        let mut answered: Vec<Vec<Option<i32>>> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| {
                        (0..batch.num_columns())
                            .map(|column| {
                                let values = batch
                                    .column(column)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .expect("every test column is int4");
                                (!values.is_null(row)).then(|| values.value(row))
                            })
                            .collect()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        answered.sort();
        answered
    }

    /// The rewritten plan of `sql`, as text.
    async fn plan_text(sql: &str) -> String {
        let ctx = ctx();
        planned(&ctx, sql)
            .await
            .expect("not refused")
            .display_indent()
            .to_string()
    }

    /// The message `sql` is refused with.
    async fn refusal(sql: &str) -> String {
        let ctx = ctx();
        match planned(&ctx, sql).await {
            Ok(plan) => panic!("expected a refusal, planned:\n{}", plan.display_indent()),
            Err(e) => e.to_string(),
        }
    }

    /// A codec for the in-memory tables these tests register, standing in for the
    /// [`crate::scheduler::logical_codec::VaireLogicalCodec`] the cluster ships plans with.
    #[derive(Debug)]
    struct MemTableCodec;

    impl LogicalExtensionCodec for MemTableCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> DFResult<Extension> {
            unimplemented!("these plans carry no extension node")
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> DFResult<()> {
            unimplemented!("these plans carry no extension node")
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _table_ref: &TableReference,
            schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> DFResult<Arc<dyn TableProvider>> {
            Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            _node: Arc<dyn TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> DFResult<()> {
            Ok(())
        }
    }

    /// Whether the optimized plan of `sql` survives the serialization Ballista puts it through
    /// on its way to the scheduler — which is what decides whether the rewrite works on a
    /// cluster and not only in process.
    async fn round_trips(sql: &str) -> Result<(), String> {
        let ctx = ctx();
        let plan = planned(&ctx, sql).await.expect("not refused");
        let optimized = ctx.state().optimize(&plan).map_err(|e| e.to_string())?;
        let bytes = logical_plan_to_bytes_with_extension_codec(&optimized, &MemTableCodec)
            .map_err(|e| e.to_string())?;
        logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &MemTableCodec)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    // ---- the answers ---------------------------------------------------------------------

    /// `(1, 1)` three times on the left and twice on the right, so `min(3, 2) = 2` copies;
    /// `(3, NULL)` twice and once, so one copy; nothing else is on both sides.
    #[tokio::test]
    async fn intersect_all_keeps_the_smaller_count() {
        assert_eq!(
            rows("SELECT a, b FROM l INTERSECT ALL SELECT a, b FROM r").await,
            vec![
                vec![Some(1), Some(1)],
                vec![Some(1), Some(1)],
                vec![Some(3), None],
            ]
        );
    }

    /// `(1, 1)` three times less twice, `(2, 2)` once less none, `(3, NULL)` twice less once,
    /// and `(4, 4)` is only on the right so it contributes nothing.
    #[tokio::test]
    async fn except_all_keeps_the_difference_of_the_counts() {
        assert_eq!(
            rows("SELECT a, b FROM l EXCEPT ALL SELECT a, b FROM r").await,
            vec![
                vec![Some(1), Some(1)],
                vec![Some(2), Some(2)],
                vec![Some(3), None],
            ]
        );
    }

    /// One column, which is the shape the gap analysis measured: `1, 1, 1, 2, 3, 3` against
    /// `1, 1, 3, 4`.
    #[tokio::test]
    async fn one_column_counts_the_same_way() {
        assert_eq!(
            rows("SELECT a FROM l INTERSECT ALL SELECT a FROM r").await,
            vec![vec![Some(1)], vec![Some(1)], vec![Some(3)]]
        );
        assert_eq!(
            rows("SELECT a FROM l EXCEPT ALL SELECT a FROM r").await,
            vec![vec![Some(1)], vec![Some(2)], vec![Some(3)]]
        );
    }

    /// `max(m - n, 0)`, not `m - n`: a right side with more copies than the left removes all
    /// of them and no more.
    #[tokio::test]
    async fn except_all_never_answers_a_negative_count() {
        assert_eq!(
            rows("SELECT a FROM r EXCEPT ALL SELECT a FROM l").await,
            vec![vec![Some(4)]]
        );
    }

    /// An empty right branch leaves both operations at their extremes: nothing intersects it,
    /// and nothing is subtracted by it.
    #[tokio::test]
    async fn an_empty_branch_is_min_zero_and_max_m() {
        assert!(
            rows("SELECT a FROM l INTERSECT ALL SELECT a FROM r WHERE false")
                .await
                .is_empty()
        );
        assert_eq!(
            rows("SELECT a FROM l EXCEPT ALL SELECT a FROM r WHERE false").await,
            vec![
                vec![Some(1)],
                vec![Some(1)],
                vec![Some(1)],
                vec![Some(2)],
                vec![Some(3)],
                vec![Some(3)],
            ]
        );
    }

    /// A row is its own partner as often as it appears, so a branch intersected with itself is
    /// itself and a branch minus itself is empty. The interesting half is that both sides
    /// carry the same qualifier, which is what the two distinct row-number names are for.
    #[tokio::test]
    async fn a_branch_against_itself_is_identity_and_empty() {
        assert_eq!(
            rows("SELECT a FROM l INTERSECT ALL SELECT a FROM l").await,
            vec![
                vec![Some(1)],
                vec![Some(1)],
                vec![Some(1)],
                vec![Some(2)],
                vec![Some(3)],
                vec![Some(3)],
            ]
        );
        assert!(
            rows("SELECT a FROM l EXCEPT ALL SELECT a FROM l")
                .await
                .is_empty()
        );
    }

    /// `NULL` is grouped with `NULL`, as PostgreSQL's set operations do and its `=` does not:
    /// the `(3, NULL)` rows have to find each other for the counts above to come out.
    #[tokio::test]
    async fn nulls_are_matched_with_nulls() {
        assert_eq!(
            rows("SELECT b FROM l WHERE a = 3 INTERSECT ALL SELECT b FROM r WHERE a = 3").await,
            vec![vec![None]]
        );
        assert_eq!(
            rows("SELECT b FROM l WHERE a = 3 EXCEPT ALL SELECT b FROM r WHERE a = 3").await,
            vec![vec![None]]
        );
    }

    /// Chained operations associate to the left, and each link is rewritten in turn: `l`
    /// against `r` leaves `(1, 1) x 2, (3, NULL)`, and intersecting that with `r` again leaves
    /// the same thing, because `r` still has two `(1, 1)` and one `(3, NULL)`.
    #[tokio::test]
    async fn a_chain_is_rewritten_link_by_link() {
        assert_eq!(
            rows(
                "SELECT a, b FROM l INTERSECT ALL SELECT a, b FROM r \
                 INTERSECT ALL SELECT a, b FROM r"
            )
            .await,
            vec![
                vec![Some(1), Some(1)],
                vec![Some(1), Some(1)],
                vec![Some(3), None],
            ]
        );
    }

    /// A distinct `UNION` on the left is the case that makes reading the plan's shape unsound:
    /// it puts a `Distinct` where the `DISTINCT` form of the set operation puts one, so only
    /// the marker can tell the two apart. `l UNION r` has one of each distinct row, so
    /// intersecting it with `r` counts one each of `r`'s three distinct rows.
    #[tokio::test]
    async fn a_union_on_the_left_is_still_counted_as_all() {
        assert_eq!(
            rows(
                "(SELECT a, b FROM l UNION SELECT a, b FROM r) \
                 INTERSECT ALL SELECT a, b FROM r"
            )
            .await,
            vec![
                vec![Some(1), Some(1)],
                vec![Some(3), None],
                vec![Some(4), Some(4)],
            ]
        );
    }

    /// `SELECT DISTINCT` on the left is the same trap from the other direction, and the same
    /// marker resolves it.
    #[tokio::test]
    async fn a_select_distinct_on_the_left_is_still_counted_as_all() {
        assert_eq!(
            rows("SELECT DISTINCT a FROM l EXCEPT ALL SELECT a FROM r").await,
            vec![vec![Some(2)]]
        );
    }

    /// Inside a derived table, so the rewrite has to reach a plan that is not the root.
    #[tokio::test]
    async fn a_set_operation_inside_a_subquery_is_rewritten() {
        assert_eq!(
            rows(
                "SELECT count(*)::int FROM \
                 (SELECT a FROM l INTERSECT ALL SELECT a FROM r) AS counted"
            )
            .await,
            vec![vec![Some(3)]]
        );
    }

    /// Inside a `WITH`, which is where an analytical client writes one.
    #[tokio::test]
    async fn a_set_operation_inside_a_cte_is_rewritten() {
        assert_eq!(
            rows(
                "WITH both AS (SELECT a FROM l INTERSECT ALL SELECT a FROM r) \
                 SELECT count(*)::int FROM both"
            )
            .await,
            vec![vec![Some(3)]]
        );
    }

    // ---- what the client sees of the rewrite ---------------------------------------------

    /// The marker names a relation, and a relation that reached a client would show up in
    /// `EXPLAIN` and in an error message. It comes off in the same pass that reads it.
    #[tokio::test]
    async fn no_marker_survives_in_the_plan() {
        for sql in [
            "SELECT a, b FROM l INTERSECT ALL SELECT a, b FROM r",
            "SELECT a, b FROM l EXCEPT ALL SELECT a, b FROM r",
        ] {
            let plan = plan_text(sql).await;
            assert!(!plan.contains(MARKER_RELATION), "{sql}\n{plan}");
        }
    }

    /// The column names and their order are the left branch's, before and after — an
    /// `ALL` set operation is not a place a client expects its labels to change.
    #[tokio::test]
    async fn the_result_columns_are_unchanged() {
        let ctx = ctx();
        for sql in [
            "SELECT a AS first, b AS second FROM l INTERSECT ALL SELECT a, b FROM r",
            "SELECT a AS first, b AS second FROM l EXCEPT ALL SELECT a, b FROM r",
        ] {
            let plan = planned(&ctx, sql).await.expect("not refused");
            let names: Vec<&str> = plan
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            assert_eq!(names, vec!["first", "second"], "{sql}");
        }
    }

    /// The rewrite is only worth anything if it reaches an executor, and a window function
    /// under a semi join is a plan shape nothing else in the read path produces.
    #[tokio::test]
    async fn the_rewritten_plan_reaches_an_executor() {
        for sql in [
            "SELECT a, b FROM l INTERSECT ALL SELECT a, b FROM r",
            "SELECT a, b FROM l EXCEPT ALL SELECT a, b FROM r",
            "SELECT a FROM l INTERSECT ALL SELECT a FROM l",
            "WITH both AS (SELECT a FROM l INTERSECT ALL SELECT a FROM r) SELECT * FROM both",
        ] {
            assert_eq!(round_trips(sql).await, Ok(()), "{sql}");
        }
    }

    // ---- what is left alone --------------------------------------------------------------

    /// The `DISTINCT` forms were already right, so they are not rewritten: no numbering, and
    /// the `Distinct` the planner put on the left is still there.
    #[tokio::test]
    async fn the_distinct_forms_are_untouched() {
        for sql in [
            "SELECT a, b FROM l INTERSECT SELECT a, b FROM r",
            "SELECT a, b FROM l EXCEPT SELECT a, b FROM r",
        ] {
            let plan = plan_text(sql).await;
            assert!(!plan.contains("row_number"), "{sql}\n{plan}");
            assert!(plan.contains("Distinct:"), "{sql}\n{plan}");
        }
        assert_eq!(
            rows("SELECT a, b FROM l INTERSECT SELECT a, b FROM r").await,
            vec![vec![Some(1), Some(1)], vec![Some(3), None]]
        );
        assert_eq!(
            rows("SELECT a, b FROM l EXCEPT SELECT a, b FROM r").await,
            vec![vec![Some(2), Some(2)]]
        );
    }

    /// `UNION ALL` concatenates, which is all its `ALL` asks for, so neither pass touches it.
    #[tokio::test]
    async fn union_all_is_untouched() {
        let sql = "SELECT a FROM l UNION ALL SELECT a FROM r";
        let mut stmt = parse(sql);
        let before = stmt.to_string();
        mark_multiplicity_set_operations(&mut stmt).expect("not refused");
        assert_eq!(stmt.to_string(), before);
        assert!(!plan_text(sql).await.contains("row_number"));
        assert_eq!(rows(sql).await.len(), 10);
    }

    /// A client's own semi join is not the join a set operation lowers to — its predicate is a
    /// filter and its NULLs are not equal — so the residue refusals cannot reach it.
    #[tokio::test]
    async fn an_explicit_semi_join_is_not_mistaken_for_a_set_operation() {
        let sql = "SELECT l.a FROM l LEFT SEMI JOIN r ON l.a = r.a";
        let ctx = ctx();
        planned(&ctx, sql).await.expect("not refused");
    }

    // ---- the marker itself ---------------------------------------------------------------

    /// Marking twice marks once: the pass runs once per statement today, but a second run has
    /// to be a no-op for the plan pass to keep recognizing what it produced.
    #[tokio::test]
    async fn marking_is_idempotent() {
        let mut stmt = parse("SELECT a FROM l INTERSECT ALL SELECT a FROM r");
        mark_multiplicity_set_operations(&mut stmt).expect("not refused");
        let once = stmt.to_string();
        assert!(once.contains(MARKER_RELATION), "{once}");
        mark_multiplicity_set_operations(&mut stmt).expect("not refused");
        assert_eq!(stmt.to_string(), once);
    }

    /// `MINUS` is `EXCEPT` under another name, so it counts duplicates the same way and gets
    /// the same marker. Only the marking is asserted: DataFusion's planner does not implement
    /// `MINUS` at all, and PostgreSQL does not spell it, so there is no answer to compare — but
    /// the marker has to be there for the day either changes.
    #[tokio::test]
    async fn minus_all_is_marked_like_except_all() {
        let mut stmt = parse("SELECT a FROM l MINUS ALL SELECT a FROM r");
        mark_multiplicity_set_operations(&mut stmt).expect("not refused");
        assert!(stmt.to_string().contains(MARKER_RELATION), "{stmt}");
    }

    /// A branch that carries its own `ORDER BY` and `LIMIT` keeps them: they travel inside the
    /// branch, not on the wrapper the marker adds.
    #[tokio::test]
    async fn a_branch_with_its_own_order_by_and_limit_is_marked_intact() {
        assert_eq!(
            rows(
                "SELECT a FROM l INTERSECT ALL \
                 (SELECT a FROM r ORDER BY a LIMIT 1)"
            )
            .await,
            vec![vec![Some(1)]]
        );
    }

    // ---- the refusals --------------------------------------------------------------------

    /// `BY NAME` is a DuckDB spelling, and PostgreSQL matches a set operation's branches by
    /// position. Refused in the AST, so it never reaches the plan pass.
    #[tokio::test]
    async fn by_name_is_refused() {
        for sql in [
            "SELECT a, b FROM l INTERSECT ALL BY NAME SELECT b, a FROM r",
            "SELECT a, b FROM l EXCEPT ALL BY NAME SELECT b, a FROM r",
        ] {
            let message = refusal(sql).await;
            assert!(
                message.contains("ALL BY NAME is not supported"),
                "{message}"
            );
            assert!(message.contains("matched by position"), "{message}");
        }
    }

    /// The second half of the invariant: a set operation that reaches the plan without the
    /// marker is refused rather than answered as its `DISTINCT` form. Reachable only by
    /// skipping the AST pass, which is what this test does.
    #[tokio::test]
    async fn an_unmarked_all_set_operation_is_refused() {
        let ctx = ctx();
        for (sql, expected) in [
            (
                "SELECT a FROM l INTERSECT ALL SELECT a FROM r",
                "INTERSECT ALL is not supported",
            ),
            (
                "SELECT a FROM l EXCEPT ALL SELECT a FROM r",
                "EXCEPT ALL is not supported",
            ),
        ] {
            let plan = ctx
                .state()
                .statement_to_plan(DFStatement::Statement(Box::new(parse(sql))))
                .await
                .expect("statement plans");
            let message = preserve_set_operation_multiplicity(plan)
                .expect_err("an unmarked ALL set operation is refused")
                .to_string();
            assert!(message.contains(expected), "{sql}\n{message}");
            assert!(
                message.contains("without the duplicate-counting rewrite"),
                "{sql}\n{message}"
            );
        }
    }

    /// The `DISTINCT` form of the same shape, which is where that refusal could over-reach:
    /// the planner aliases both branches to keep their names apart, and the marker the plan
    /// pass looks for is then two layers down. The `Distinct` that exempts it is not, because
    /// `intersect_or_except` adds it above the aliasing.
    #[tokio::test]
    async fn a_requalified_distinct_set_operation_is_not_refused() {
        assert_eq!(
            rows("(SELECT a, b FROM l UNION SELECT a, b FROM r) INTERSECT SELECT a, b FROM r")
                .await,
            vec![
                vec![Some(1), Some(1)],
                vec![Some(3), None],
                vec![Some(4), Some(4)],
            ]
        );
    }

    /// And the `DISTINCT` forms are not caught by that refusal, which is the over-reach the
    /// pair of passes would be worth nothing without.
    #[tokio::test]
    async fn an_unmarked_distinct_set_operation_is_not_refused() {
        let ctx = ctx();
        for sql in [
            "SELECT a FROM l INTERSECT SELECT a FROM r",
            "SELECT a FROM l EXCEPT SELECT a FROM r",
        ] {
            let plan = ctx
                .state()
                .statement_to_plan(DFStatement::Statement(Box::new(parse(sql))))
                .await
                .expect("statement plans");
            preserve_set_operation_multiplicity(plan).expect("not refused");
        }
    }
}
