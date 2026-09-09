//! Give an untyped `$N` the type the query compares it against.
//!
//! A client that sends `Parse` without declaring parameter OIDs — which is what
//! `tokio-postgres`, and therefore a large share of drivers, does — leaves the server to
//! work the types out. DataFusion's SQL planner does that for most shapes: in
//! `WHERE n > $1` the placeholder comes back already typed from the column beside it. In
//! a few shapes it does not, and `HAVING sum(n) > $1` is the one that matters, because
//! the comparison is against an aggregate rather than a column.
//!
//! An untyped placeholder is decoded as text, so `HAVING sum(n) > $1` with `$1 = '60'`
//! became a **string** comparison: `'185' > '60'` is false, and the group whose total is
//! 185 vanished from the answer. Wrong rows, no error, and a plausible-looking result set
//! — the client cannot tell it was served a lexicographic compare.
//!
//! ## Where the types come from
//!
//! Not from a table of our own. [`Expr::infer_placeholder_types`] already does exactly
//! this — for a binary comparison, `BETWEEN`, `IN` and `LIKE` it takes the type of the
//! expression on the other side — and DataFusion calls it from
//! `replace_params_with_values`, on the way to substituting the values in. That is one
//! step too late: by then the value has already been decoded, as text, using the type the
//! plan reported at `Describe` time.
//!
//! So this runs the same inference **one step earlier**, over the cached plan, so that
//! `get_parameter_types` answers with the inferred type. Describe then advertises `int8`
//! for the placeholder, `arrow-pg` decodes the bound value as an `i64`, and the
//! comparison DataFusion plans is the arithmetic one. The two spellings agree because
//! they are the same inference; running it twice is a no-op, since the second pass finds
//! every placeholder already typed.
//!
//! A client that *does* declare an OID is unaffected: `deserialize_parameters` prefers the
//! declared type over the inferred one, which is PostgreSQL's rule too.
//!
//! ## `LIMIT $1`
//!
//! `LIMIT`/`OFFSET` have no comparand to infer from — the placeholder stands alone — so
//! inference cannot reach them and they are typed here directly, as `bigint`. That is not
//! a guess: PostgreSQL's grammar admits only a row count there, and declares the
//! parameter `bigint` for exactly that reason.
//!
//! ## What is left untyped
//!
//! A placeholder with no context at all — `SELECT $1` — stays untyped and is still decoded
//! as text. PostgreSQL refuses that shape outright (`42P18`, *could not determine data
//! type of parameter*) when no OID is declared, so text is a divergence; it is recorded in
//! the operator/literal analysis rather than fixed here, because a client that declares
//! the OID (which is the only way the shape is useful) already gets the right answer.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::{Expr, LogicalPlan};

/// The type PostgreSQL gives the parameter of a `LIMIT` or `OFFSET`.
const ROW_COUNT: DataType = DataType::Int64;

/// Fill in the type of every `$N` in `plan` that its context determines.
///
/// A plan whose placeholders are all typed already — the common case, and every plan
/// from the simple protocol, which has no placeholders at all — is returned untouched.
///
/// Infallible by construction: inference needs the type of the expression beside the
/// placeholder, and where that type cannot be derived the original plan is returned
/// rather than an error. Nothing is lost by that. `replace_params_with_values` runs the
/// same inference when the values arrive, so a shape that cannot be inferred fails there
/// exactly as it does today, and one that can is only ever improved here.
pub(crate) fn resolve_placeholder_types(plan: LogicalPlan) -> LogicalPlan {
    if !has_untyped_placeholder(&plan) {
        return plan;
    }
    resolve(plan.clone()).unwrap_or(plan)
}

/// Whether any `$N` in `plan` is still waiting for a type.
///
/// `get_parameter_types` collects placeholders across the whole plan, subqueries
/// included, and reports `None` for one the planner could not type.
fn has_untyped_placeholder(plan: &LogicalPlan) -> bool {
    plan.get_parameter_types()
        .is_ok_and(|types| types.values().any(Option::is_none))
}

/// The inference itself, over one node at a time.
fn resolve(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|plan| {
        // A row count, unlike a comparison, has nothing beside it to infer from.
        if matches!(plan, LogicalPlan::Limit(_)) {
            return plan.map_expressions(|expr| Ok(type_row_count(expr)));
        }
        // The node's own schema, matching `replace_params_with_values` exactly — for the
        // plans that carry a placeholder (`Filter`, `Projection`, `Join`) it is the schema
        // the placeholder's neighbour resolves against.
        let schema = Arc::clone(plan.schema());
        plan.map_expressions(|expr| {
            expr.infer_placeholder_types(&schema)
                .map(|(expr, _)| Transformed::yes(expr))
        })
    })
    .map(|transformed| transformed.data)
}

/// A bare `$N` used as a row count becomes a `bigint`.
///
/// Only a bare placeholder: `LIMIT $1 + 1` is a binary expression, which the ordinary
/// inference types from the literal beside it.
fn type_row_count(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Placeholder(Placeholder { id, field: None }) => {
            let field = Field::new(&id, ROW_COUNT, true);
            Transformed::yes(Expr::Placeholder(Placeholder::new_with_field(
                id,
                Some(Arc::new(field)),
            )))
        }
        expr => Transformed::no(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::context::SessionContext;

    /// One table with a group key, a `bigint` measure and a text column.
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int32, false),
            Field::new("n", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 40, 185])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    /// The type the plan reports for `$1` after this pass — which is the type Describe
    /// advertises and the type the bound value is decoded as.
    async fn param_type(sql: &str) -> Option<DataType> {
        let plan = ctx().state().create_logical_plan(sql).await.unwrap();
        resolve_placeholder_types(plan)
            .get_parameter_types()
            .unwrap()
            .remove("$1")
            .expect("the plan has a $1")
    }

    // The defect. `sum(n)` is a `bigint`, so `$1` beside it is one too; untyped it was
    // decoded as text and the comparison became lexicographic.
    #[tokio::test]
    async fn a_parameter_compared_against_an_aggregate_is_typed_from_it() {
        assert_eq!(
            param_type("SELECT g FROM t GROUP BY g HAVING sum(n) > $1").await,
            Some(ROW_COUNT)
        );
    }

    // The same shape over `count(*)`, which is the spelling reporting clients emit most.
    #[tokio::test]
    async fn a_parameter_compared_against_a_count_is_typed_from_it() {
        assert_eq!(
            param_type("SELECT g FROM t GROUP BY g HAVING count(*) > $1").await,
            Some(ROW_COUNT)
        );
    }

    // A row count has no neighbour to be inferred from, so it is typed outright.
    #[tokio::test]
    async fn a_row_count_parameter_is_a_bigint() {
        assert_eq!(
            param_type("SELECT g FROM t LIMIT $1").await,
            Some(ROW_COUNT)
        );
    }

    #[tokio::test]
    async fn an_offset_parameter_is_a_bigint() {
        assert_eq!(
            param_type("SELECT g FROM t OFFSET $1").await,
            Some(ROW_COUNT)
        );
    }

    // What the planner already types is the majority of cases, and this pass must agree
    // with it rather than override it — a text column's parameter stays text.
    #[tokio::test]
    async fn a_parameter_the_planner_already_typed_is_left_alone() {
        assert_eq!(
            param_type("SELECT g FROM t WHERE label = $1").await,
            Some(DataType::Utf8)
        );
        assert_eq!(
            param_type("SELECT g FROM t WHERE n > $1").await,
            Some(ROW_COUNT)
        );
    }

    // Idempotence: `replace_params_with_values` runs the same inference again when the
    // values arrive, so a second pass has to be a no-op rather than a drift.
    #[tokio::test]
    async fn resolving_twice_changes_nothing() {
        let plan = ctx()
            .state()
            .create_logical_plan("SELECT g FROM t GROUP BY g HAVING sum(n) > $1")
            .await
            .unwrap();
        let once = resolve_placeholder_types(plan);
        let twice = resolve_placeholder_types(once.clone());
        assert_eq!(format!("{once:?}"), format!("{twice:?}"));
    }

    // A placeholder with nothing to infer from is left untyped rather than guessed at,
    // and the pass must not fail over it — the plan still has to reach the client.
    #[tokio::test]
    async fn a_parameter_with_no_context_stays_untyped() {
        assert_eq!(param_type("SELECT $1").await, None);
    }

    // The typed parameter has to survive being bound, not merely be reported: this is
    // the whole point, and it is the assertion that fails if the type is right in
    // `Describe` and wrong in the comparison.
    #[tokio::test]
    async fn the_typed_parameter_selects_the_groups_postgres_selects() {
        use datafusion::common::ParamValues;
        use datafusion::scalar::ScalarValue;

        let ctx = ctx();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT g FROM t GROUP BY g HAVING sum(n) > $1 ORDER BY g")
            .await
            .unwrap();
        let bound = resolve_placeholder_types(plan)
            .replace_params_with_values(&ParamValues::List(vec![
                ScalarValue::Int64(Some(60)).into(),
            ]))
            .unwrap();
        let batches = ctx
            .execute_logical_plan(bound)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Group 1 totals 50 and group 2 totals 185. Only 185 clears 60 — and it is the
        // one a lexicographic compare dropped, because `'185' < '60'` as text.
        let rows: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<i32>>()
            })
            .collect();
        assert_eq!(rows, vec![2]);
    }
}
