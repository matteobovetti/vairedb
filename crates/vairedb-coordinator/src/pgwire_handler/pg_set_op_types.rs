//! Refuse a set operation whose branches have no common type in PostgreSQL, instead of
//! answering rows PostgreSQL never would — or, for `INTERSECT` and `EXCEPT`, failing halfway
//! through execution.
//!
//! ```text
//! l(id int4)  = 1, 2, 3, 4
//! r(w  text)  = 'x', 'y', 'z'
//!
//! SELECT id FROM l UNION ALL SELECT w FROM r
//!
//! PostgreSQL  ERROR:  UNION types integer and text cannot be matched   -- 42804, at plan time
//! DataFusion  1, 2, 3, 4, x, y, z                                     -- text, and answered
//!
//! SELECT id FROM l INTERSECT SELECT w FROM r
//!
//! PostgreSQL  ERROR:  INTERSECT types integer and text cannot be matched   -- 42804, at plan time
//! DataFusion  XX000, mid-execution: Cannot cast string 'x' to value of Int32 type
//! ```
//!
//! The two engines resolve a set operation's type by different rules, and the difference is
//! not a narrowing — it is wider. DataFusion asks for *a* common type and Arrow always has
//! one, because everything casts to a string. PostgreSQL asks for a common type reachable by
//! **implicit** coercion, and between two different type categories there is none: an integer
//! does not implicitly become text, so the query is rejected before a row is read.
//!
//! Answering it is the dangerous outcome rather than the generous one. The rows come back
//! rendered into a type the client did not ask for, `ORDER BY` over them sorts
//! lexicographically (`10` before `9`), and an aggregate over the result is computing on text.
//! Nothing in the answer says so.
//!
//! ## What PostgreSQL's rule is, and what of it this models
//!
//! PostgreSQL resolves one common type over *every* branch (`select_common_type`): it takes
//! the first branch with a known type as the candidate and, for each later one, keeps the
//! candidate if the new type coerces implicitly to it, adopts the new type if the candidate
//! coerces to *that*, and otherwise raises `42804` naming both. Implicit coercion between
//! scalar types follows the type category, so what the rule amounts to for the types VaireDB
//! carries is: all numerics meet, all strings meet, dates and timestamps meet, and nothing
//! else crosses. [`PgUnionGroup`] is that partition and is the whole of the decision.
//!
//! The one part of the rule that is not about types is **`UNKNOWN`**. A bare string literal
//! and a bare `NULL` have no type yet in PostgreSQL; they take the type the other branches
//! resolve to, which is why `SELECT id FROM l UNION SELECT '2'` is an integer union and not a
//! mismatch. Arrow has no `UNKNOWN` — the planner has already made that literal `Utf8` — so
//! the literal is recognized in the plan instead and excluded from the decision, and only the
//! branches whose type came from somewhere other than a bare literal are compared. Without
//! that, this would refuse a query PostgreSQL answers.
//!
//! `UNKNOWN` reaches less far than it looks, and the boundary was measured rather than
//! reasoned about. A bare literal is unknown only in a branch's own select list: PostgreSQL
//! resolves a `VALUES` clause's column types before the set operation sees them, and an
//! unknown literal with nothing to resolve against becomes `text` there. So
//! `… UNION ALL SELECT '2'` is an integer union while `… UNION ALL VALUES ('2')` and
//! `… UNION ALL SELECT * FROM (VALUES ('2')) t` are both mismatches — all three confirmed
//! against a PostgreSQL 16.15 oracle. A cast literal is not unknown either: `'2'::text` is
//! text in PostgreSQL as much as a text column is.
//!
//! One pair inside that partition carries a different SQLSTATE in PostgreSQL, and it is
//! recorded rather than matched: `date ∪ time` is refused `42846 cannot_coerce` there, because
//! PostgreSQL gets as far as choosing a candidate type and then fails to cast to it, while
//! every other crossing fails earlier at `42804 datatype_mismatch`. Both are class `42` and
//! both are refusals of the same query; the distinction would cost a new error code for one
//! exotic pair, so this reports `42804` for all of them.
//!
//! ## Finding `INTERSECT` and `EXCEPT`, which do not survive planning
//!
//! PostgreSQL refuses both with the same `42804` and by the same rule, differing only in that
//! the message names the operator: `INTERSECT types integer and text cannot be matched`. But
//! neither reaches this check as a set operation — DataFusion lowers `INTERSECT` to a
//! `LeftSemi` join and `EXCEPT` to a `LeftAnti` join, on an equality per column between the two
//! branches. So there is no `Union` node to look at, and the branches are the join's two
//! inputs.
//!
//! That plan shape has to be told apart from a semi/anti join a client asked for, and it can
//! be — measured on the unoptimized plan this check runs against:
//!
//! ```text
//! INTERSECT              LeftSemi Join: l.id = r.w            -- `on` carries the equality
//! LEFT SEMI JOIN … ON    LeftSemi Join:  Filter: l.id = r.w   -- `on` empty, `filter` carries it
//! IN (subquery)          Filter: l.id IN (<subquery>)         -- not a join at all
//! EXISTS (subquery)      Filter: EXISTS (<subquery>)          -- not a join at all
//! ```
//!
//! A client's `IN`/`NOT IN`/`EXISTS` subquery only becomes a join in the optimizer, which runs
//! later — here it is still an expression inside a `Filter`. That matters because PostgreSQL
//! refuses a mismatched `IN (subquery)` as a missing operator (`42883`), not as a set-operation
//! type mismatch, so a check that caught both would put the wrong code on one. It does not: the
//! shapes are distinct. The remaining overlap is an explicit `LEFT SEMI JOIN`, whose equality
//! sits in `filter` rather than `on` — and which is not PostgreSQL syntax at all, so it is
//! outside the contract either way. [`set_operation_branches`] is that shape test.
//!
//! ## What this does not cover
//!
//! `INTERSECT ALL` and `EXCEPT ALL`, which are refused earlier by
//! `pg_operators::reject_multiplicity_set_operations` for a different reason (DataFusion drops
//! the multiplicity) and so never get here.
//!
//! ## Where this runs
//!
//! From `plan_select`, on the planned logical plan and **before** `coerce_types`. That
//! ordering is the requirement, not a preference: type coercion is exactly the pass that
//! inserts the casts making the branches agree, so afterwards there is no disagreement left
//! to see.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::logical_expr::{Expr, Join, JoinType, LogicalPlan};
use pgwire::error::{PgWireError, PgWireResult};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// PostgreSQL's implicit-coercion classes, as far as a set operation is concerned: two
/// branch types can meet if and only if they land in the same group.
///
/// This is coarser than PostgreSQL's `TYPCATEGORY` in one place and finer in another, both
/// deliberately. Coarser: every numeric width is one group, because they all coerce to each
/// other. Finer: `time` and `interval` are split out of the datetime category, because
/// PostgreSQL refuses `date ∪ time` and `interval ∪ time` even though all three are category
/// `D` — within a category, coercibility still has to exist.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PgUnionGroup {
    /// `int2` … `int8`, `numeric`, `float4`, `float8`. Mutually coercible.
    Numeric,
    /// `text`, `varchar`, `char`.
    Text,
    /// `bytea`. Does *not* meet `Text`: PostgreSQL has no implicit `bytea` ↔ `text`.
    Binary,
    Boolean,
    /// `date`, `timestamp`, `timestamptz` — `date` coerces to either timestamp.
    DateTime,
    /// `time`, `timetz`. Its own group: `date ∪ time` is a mismatch in PostgreSQL.
    Time,
    Interval,
}

/// The group `dt` belongs to, or `None` for a type this does not model — a nested or
/// extension type, or `Null`. `None` means *do not judge*: a type whose PostgreSQL coercion
/// rules are not represented here must not produce a refusal, because a wrong refusal is a
/// query that used to work.
fn group_of(dt: &DataType) -> Option<PgUnionGroup> {
    use PgUnionGroup::*;
    Some(match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Numeric,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Text,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Binary,
        DataType::Boolean => Boolean,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => DateTime,
        DataType::Time32(_) | DataType::Time64(_) => Time,
        DataType::Interval(_) | DataType::Duration(_) => Interval,
        // A dictionary is an encoding, not a type — PostgreSQL sees only what it encodes.
        DataType::Dictionary(_, value) => return group_of(value),
        _ => return None,
    })
}

/// Refuse every set operation in `plan` whose branches PostgreSQL would not let meet.
///
/// Reads the plan and never rewrites it: the outcome is either the same plan or `42804`.
pub(crate) fn reject_incompatible_set_operation_types(plan: &LogicalPlan) -> PgWireResult<()> {
    let mut visitor = SetOpTypes { error: None };
    // `visit_with_subqueries`, so a set operation inside a subquery or a CTE is checked too —
    // it is the same wrong answer one level down.
    let _ = plan.visit_with_subqueries(&mut visitor);
    match visitor.error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

struct SetOpTypes {
    error: Option<PgWireError>,
}

impl<'n> TreeNodeVisitor<'n> for SetOpTypes {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n LogicalPlan) -> datafusion::common::Result<TreeNodeRecursion> {
        let checked = match node {
            LogicalPlan::Union(union) => {
                let branches: Vec<&LogicalPlan> =
                    union.inputs.iter().map(|input| input.as_ref()).collect();
                let names: Vec<&str> = union
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect();
                check_branches("UNION", &branches, &names)
            }
            LogicalPlan::Join(join) => match set_operation_branches(join) {
                Some((operator, branches, names)) => {
                    let names: Vec<&str> = names.iter().map(String::as_str).collect();
                    check_branches(operator, &branches, &names)
                }
                None => Ok(()),
            },
            _ => Ok(()),
        };
        if let Err(e) = checked {
            self.error = Some(e);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// The two branches of an `INTERSECT` or `EXCEPT` that DataFusion has lowered to a semi or
/// anti join, with the operator's PostgreSQL name and one output column name per position — or
/// `None` for any other join, including a semi/anti join a client wrote by hand.
///
/// The shape test is `on` non-empty and `filter` absent, which is what distinguishes the two
/// (see the module doc). `on` is one equality per output column, in column order, so its length
/// is the column count and its index is the position.
fn set_operation_branches(join: &Join) -> Option<(&'static str, Vec<&LogicalPlan>, Vec<String>)> {
    let operator = match join.join_type {
        JoinType::LeftSemi => "INTERSECT",
        JoinType::LeftAnti => "EXCEPT",
        _ => return None,
    };
    if join.on.is_empty() || join.filter.is_some() {
        return None;
    }
    // The name the client sees is the join's own output schema, which for these plans is the
    // left branch's. Falling back to the left key's rendering keeps the message useful if a
    // future lowering stops matching that.
    let names = join
        .on
        .iter()
        .enumerate()
        .map(|(position, (left_key, _))| {
            join.schema
                .fields()
                .get(position)
                .map(|field| field.name().clone())
                .unwrap_or_else(|| left_key.schema_name().to_string())
        })
        .collect();
    Some((operator, vec![&join.left, &join.right], names))
}

/// Compare a set operation's branches column by column, exactly as PostgreSQL folds them: the
/// first branch with a known type is the candidate, and the first later branch outside its
/// group is the error. `names` supplies the client-facing column name per position.
fn check_branches(operator: &str, branches: &[&LogicalPlan], names: &[&str]) -> PgWireResult<()> {
    for (position, name) in names.iter().enumerate() {
        let mut candidate: Option<(&DataType, PgUnionGroup)> = None;
        for branch in branches {
            // A branch narrower than the set operation's own width cannot happen — a
            // column-count mismatch is refused at parse time — but the plan does not promise
            // it, so the position is skipped rather than indexed blindly.
            let Some(branch_field) = branch.schema().fields().get(position) else {
                continue;
            };
            if column_is_unknown(branch, position) {
                continue;
            }
            let Some(group) = group_of(branch_field.data_type()) else {
                continue;
            };
            match candidate {
                None => candidate = Some((branch_field.data_type(), group)),
                Some((first, first_group)) if first_group != group => {
                    return Err(mismatch(operator, name, first, branch_field.data_type()));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

/// `42804`, naming the operator and both types the way PostgreSQL's own message does, and the
/// column whose branches disagree — which PostgreSQL's message leaves out and a wide set
/// operation needs.
fn mismatch(operator: &str, column: &str, left: &DataType, right: &DataType) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::TypeMismatch,
        format!(
            "{operator} types {} and {} cannot be matched in column \"{column}\": there is no \
             type both branches convert to implicitly, so the rows would come back rendered \
             into a type neither branch has; cast one branch explicitly to the type the result \
             should have",
            pg_type_name(left),
            pg_type_name(right),
        ),
    )
}

/// The PostgreSQL name for an Arrow type, for an error message only. Falls back to Arrow's
/// own spelling for a type `arrow-pg` has no OID for, which is better than saying nothing.
fn pg_type_name(dt: &DataType) -> String {
    arrow_pg::datatypes::into_pg_type(dt)
        .map(|t| t.name().to_string())
        .unwrap_or_else(|_| dt.to_string())
}

/// Whether `plan`'s output column `position` is PostgreSQL's `UNKNOWN` — a bare string
/// literal, a bare `NULL`, or a parameter — rather than a value with a type of its own.
///
/// Recognizing this in the plan is what stands in for a type Arrow does not have. It is
/// deliberately conservative in both directions: an unrecognized plan shape answers `false`,
/// so the column is compared on its type, and every shape that *can* put a literal in a
/// branch's output is recognized — a projection, a `VALUES` list, and the wrappers a
/// parenthesized branch adds around either.
fn column_is_unknown(plan: &LogicalPlan, position: usize) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .get(position)
            .is_some_and(is_unknown_literal),
        // A `VALUES` list is **not** unknown, which is not the obvious answer and was
        // measured: PostgreSQL resolves a `VALUES` clause's own column types before the set
        // operation sees them, and an unknown literal with nothing to resolve against becomes
        // `text`. So `SELECT id FROM l UNION ALL VALUES ('2')` is a mismatch in PostgreSQL
        // where `SELECT id FROM l UNION ALL SELECT '2'` is not. Named rather than left to the
        // fallback, because the two spellings look interchangeable and are not.
        LogicalPlan::Values(_) => false,
        // A `UNION` nested in a branch is unknown only if all of its own branches are.
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .all(|input| column_is_unknown(input, position)),
        // Wrappers a parenthesized or ordered branch adds. None of them changes which
        // expression produces the column.
        LogicalPlan::SubqueryAlias(alias) => column_is_unknown(&alias.input, position),
        LogicalPlan::Distinct(distinct) => column_is_unknown(distinct.input(), position),
        LogicalPlan::Sort(sort) => column_is_unknown(&sort.input, position),
        LogicalPlan::Limit(limit) => column_is_unknown(&limit.input, position),
        LogicalPlan::Filter(filter) => column_is_unknown(&filter.input, position),
        _ => false,
    }
}

/// A literal PostgreSQL would have typed `UNKNOWN`: a bare string, a bare `NULL`, or a
/// placeholder still waiting for its type. An aliased one counts — `SELECT '2' AS n` is the
/// same literal — but a cast one does not, because `'2'::text` *is* text in PostgreSQL too.
fn is_unknown_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_unknown_literal(&alias.expr),
        Expr::Placeholder(_) => true,
        Expr::Literal(value, _) => matches!(
            value,
            ScalarValue::Utf8(_)
                | ScalarValue::LargeUtf8(_)
                | ScalarValue::Utf8View(_)
                | ScalarValue::Null
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    /// `l(id int4, k int4, v text)` and `r(id int4, w text)`, plus `wide(big int8, d
    /// float8, b boolean)` — one table per group the check partitions by, so a mismatch
    /// can be built out of any two of them.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        ctx.register_batch("l", batch).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("w", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        ctx.register_batch("r", batch).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("big", DataType::Int64, true),
            Field::new("d", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![5_000_000_000_i64])),
                Arc::new(Float64Array::from(vec![2.5])),
            ],
        )
        .unwrap();
        ctx.register_batch("wide", batch).unwrap();

        ctx
    }

    /// The verdict on `sql` at the point `plan_select` asks for it: planned, not yet
    /// coerced. `Err` carries the client-facing message.
    async fn verdict(sql: &str) -> Result<(), String> {
        let ctx = ctx();
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        reject_incompatible_set_operation_types(&plan).map_err(|e| e.to_string())
    }

    async fn refusal(sql: &str) -> String {
        verdict(sql)
            .await
            .expect_err(&format!("`{sql}` must be refused"))
    }

    async fn accepted(sql: &str) {
        if let Err(e) = verdict(sql).await {
            panic!("`{sql}` must be accepted, got: {e}");
        }
    }

    // The defect: an integer branch and a text branch, in either order, which PostgreSQL
    // refuses `42804` before reading a row.
    #[tokio::test]
    async fn a_numeric_branch_and_a_text_branch_are_refused() {
        for sql in [
            "SELECT id FROM l UNION ALL SELECT w FROM r",
            "SELECT w FROM r UNION ALL SELECT id FROM l",
            "SELECT id FROM l UNION SELECT w FROM r",
        ] {
            let message = refusal(sql).await;
            assert!(
                message.contains("cannot be matched"),
                "`{sql}` should name the mismatch, got: {message}"
            );
        }
    }

    // The message has to name both types, because the client's fix is a cast and it needs
    // to know to what. PostgreSQL's own message names them and nothing else.
    #[tokio::test]
    async fn the_refusal_names_both_types_and_the_column() {
        let message = refusal("SELECT id FROM l UNION ALL SELECT w FROM r").await;
        assert!(message.contains("int4"), "should name int4: {message}");
        assert!(message.contains("text"), "should name text: {message}");
        assert!(
            message.contains("\"id\""),
            "should name the column: {message}"
        );
    }

    // Every width of number meets every other, which is the case the check must not
    // refuse: these are the two rows the type-resolution fix pinned.
    #[tokio::test]
    async fn numeric_widths_meet_each_other() {
        for sql in [
            "SELECT k FROM l UNION ALL SELECT big FROM wide",
            "SELECT big FROM wide UNION ALL SELECT k FROM l",
            "SELECT k FROM l UNION ALL SELECT d FROM wide",
            "SELECT d FROM wide UNION ALL SELECT big FROM wide",
        ] {
            accepted(sql).await;
        }
    }

    // Same type on both sides, and the ordinary case of a union over one table.
    #[tokio::test]
    async fn matching_types_are_accepted() {
        for sql in [
            "SELECT id FROM l UNION ALL SELECT id FROM r",
            "SELECT v FROM l UNION SELECT w FROM r",
            "SELECT id, v FROM l UNION ALL SELECT id, w FROM r",
        ] {
            accepted(sql).await;
        }
    }

    // A wide union is checked column by column, so a mismatch in the second column is
    // caught even though the first agrees — and the message says which column.
    #[tokio::test]
    async fn a_mismatch_in_a_later_column_is_found() {
        let message = refusal("SELECT id, v FROM l UNION ALL SELECT id, id AS v FROM r").await;
        assert!(
            message.contains("\"v\""),
            "should name the column that disagrees: {message}"
        );
    }

    // PostgreSQL's `UNKNOWN`: a bare string literal takes the other branch's type, so this
    // is an integer union and not a mismatch. Refusing it would break a query that works.
    #[tokio::test]
    async fn a_bare_string_literal_takes_the_other_branch_s_type() {
        for sql in [
            "SELECT id FROM l UNION ALL SELECT '2'",
            "SELECT '2' UNION ALL SELECT id FROM l",
            "SELECT id FROM l UNION ALL SELECT '2' AS id",
        ] {
            accepted(sql).await;
        }
    }

    // And where `UNKNOWN` stops, which is not where it looks like it should: a `VALUES`
    // branch's literal is already `text` by the time the set operation sees it, so these are
    // mismatches in PostgreSQL while the bare `SELECT '2'` above is not. Measured against a
    // PostgreSQL 16.15 oracle, both spellings.
    #[tokio::test]
    async fn a_values_branch_is_text_and_not_unknown() {
        for sql in [
            "SELECT id FROM l UNION ALL VALUES ('2')",
            "SELECT id FROM l UNION ALL SELECT * FROM (VALUES ('2')) AS t",
        ] {
            let message = refusal(sql).await;
            assert!(message.contains("cannot be matched"), "`{sql}`: {message}");
        }
    }

    // A bare `NULL` is unknown too — it is how a client pads a branch it has no value for.
    #[tokio::test]
    async fn a_bare_null_takes_the_other_branch_s_type() {
        for sql in [
            "SELECT id FROM l UNION ALL SELECT NULL",
            "SELECT NULL UNION ALL SELECT v FROM l",
            "SELECT id, v FROM l UNION ALL SELECT NULL, NULL",
        ] {
            accepted(sql).await;
        }
    }

    // A *cast* literal is not unknown: `'2'::text` is text in PostgreSQL as much as a text
    // column is, and `int ∪ text` is a mismatch however the text was spelled.
    #[tokio::test]
    async fn a_cast_literal_is_not_unknown() {
        let message = refusal("SELECT id FROM l UNION ALL SELECT '2'::text").await;
        assert!(message.contains("cannot be matched"), "got: {message}");
    }

    // Groups other than numeric and text, to pin that the partition is the decision and
    // not a special case for strings.
    #[tokio::test]
    async fn the_other_groups_do_not_cross() {
        for sql in [
            "SELECT id FROM l UNION ALL SELECT true",
            "SELECT v FROM l UNION ALL SELECT true",
            "SELECT id FROM l UNION ALL SELECT DATE '2026-01-01'",
            "SELECT v FROM l UNION ALL SELECT DATE '2026-01-01'",
        ] {
            let message = refusal(sql).await;
            assert!(message.contains("cannot be matched"), "`{sql}`: {message}");
        }
        // …and that a date meets a timestamp, which is the one cross-type pair inside a
        // group that PostgreSQL does allow.
        accepted("SELECT DATE '2026-01-01' UNION ALL SELECT TIMESTAMP '2026-01-01 00:00:00'").await;
    }

    // A set operation one level down is the same wrong answer, so the walk has to enter a
    // derived table, a CTE and a subquery.
    #[tokio::test]
    async fn a_nested_set_operation_is_checked_too() {
        for sql in [
            "SELECT * FROM (SELECT id FROM l UNION ALL SELECT w FROM r) AS u",
            "WITH u AS (SELECT id FROM l UNION ALL SELECT w FROM r) SELECT * FROM u",
            "SELECT id FROM l WHERE id IN (SELECT id FROM l UNION ALL SELECT w FROM r)",
        ] {
            let message = refusal(sql).await;
            assert!(message.contains("cannot be matched"), "`{sql}`: {message}");
        }
    }

    // A three-branch chain, because PostgreSQL folds the candidate left to right and the
    // mismatch can be against any earlier branch rather than the immediately previous one.
    #[tokio::test]
    async fn a_chain_is_folded_over_every_branch() {
        accepted("SELECT id FROM l UNION ALL SELECT k FROM l UNION ALL SELECT big FROM wide").await;
        let message =
            refusal("SELECT id FROM l UNION ALL SELECT k FROM l UNION ALL SELECT w FROM r").await;
        assert!(message.contains("cannot be matched"), "got: {message}");
    }

    // `INTERSECT` and `EXCEPT` reach the check as a semi and an anti join rather than as set
    // operations, and are refused the same way — with the operator PostgreSQL names, not
    // "UNION", because the client is told which of its own operators to fix.
    #[tokio::test]
    async fn intersect_and_except_are_refused_under_their_own_names() {
        for (sql, operator) in [
            ("SELECT id FROM l INTERSECT SELECT w FROM r", "INTERSECT"),
            ("SELECT id FROM l EXCEPT SELECT w FROM r", "EXCEPT"),
            ("SELECT w FROM r INTERSECT SELECT id FROM l", "INTERSECT"),
        ] {
            let message = refusal(sql).await;
            assert!(
                message.contains(operator) && message.contains("cannot be matched"),
                "`{sql}` should name {operator}, got: {message}"
            );
            assert!(message.contains("int4"), "should name int4: {message}");
            assert!(message.contains("text"), "should name text: {message}");
        }
    }

    // A mismatch in a later column of a wide `INTERSECT`, which is the case a per-column loop
    // over the join's key pairs exists for — PostgreSQL refuses this too, measured.
    #[tokio::test]
    async fn a_later_column_of_an_intersect_is_checked() {
        let message = refusal("SELECT id, k FROM l INTERSECT SELECT id, w FROM r").await;
        assert!(
            message.contains("INTERSECT") && message.contains("\"k\""),
            "should name the second column: {message}"
        );
        accepted("SELECT id, v FROM l INTERSECT SELECT id, w FROM r").await;
    }

    // The `UNKNOWN` rule applies to these two as well: `… INTERSECT SELECT '2'` is an integer
    // intersection in PostgreSQL and answers, measured against the 16.15 oracle.
    #[tokio::test]
    async fn an_unknown_literal_branch_of_an_intersect_is_accepted() {
        accepted("SELECT id FROM l INTERSECT SELECT '2'").await;
        accepted("SELECT id FROM l EXCEPT SELECT '2'").await;
        accepted("SELECT id FROM l EXCEPT SELECT NULL").await;
    }

    // The other side of the shape test: a semi/anti join a client asked for carries its
    // equality in `filter`, not `on`, and an `IN`/`EXISTS` subquery is not a join here at all.
    // None of them is a set operation, so none is refused as one — which is what keeps the
    // `42883` PostgreSQL gives `IN (subquery)` from being overwritten with `42804`.
    #[tokio::test]
    async fn a_hand_written_semi_join_and_a_subquery_are_not_set_operations() {
        for sql in [
            "SELECT l.id FROM l LEFT SEMI JOIN r ON l.id = r.w",
            "SELECT l.id FROM l LEFT ANTI JOIN r ON l.id = r.w",
            "SELECT id FROM l WHERE id IN (SELECT w FROM r)",
            "SELECT id FROM l WHERE id NOT IN (SELECT w FROM r)",
            "SELECT id FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.w = l.v)",
        ] {
            accepted(sql).await;
        }
    }

    // An ordinary join is never a set operation, whatever its key types — the check must not
    // reach the everyday case.
    #[tokio::test]
    async fn an_ordinary_join_is_untouched() {
        accepted("SELECT l.id FROM l JOIN r ON l.v = r.w").await;
        accepted("SELECT l.id FROM l LEFT JOIN r ON l.id = r.id").await;
        accepted("SELECT l.id FROM l FULL JOIN r ON l.id = r.id").await;
    }

    // The refusal is a read, not a rewrite: an accepted plan has to be the plan the caller
    // already holds, unchanged.
    #[tokio::test]
    async fn an_accepted_plan_is_untouched() {
        let ctx = ctx();
        let sql = "SELECT id FROM l UNION ALL SELECT id FROM r";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let before = plan.display_indent().to_string();
        reject_incompatible_set_operation_types(&plan).unwrap();
        assert_eq!(plan.display_indent().to_string(), before);
    }
}
