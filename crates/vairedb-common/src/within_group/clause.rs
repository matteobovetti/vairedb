//! What a `WITHIN GROUP` call says, and what it is refused for saying.
//!
//! Every aggregate here is defined by an order it did not choose: the clause supplies the
//! values, and a direct argument is fixed for the whole group. Reading that call is not a
//! rule a client can see on its own — it is the part each of the seven aggregates had to get
//! *identically* right, including the refusal `DISTINCT` deserves, the sentence a missing
//! clause deserves, and the rule that a direct argument is a literal. The reason they are
//! here is that two copies of each is two chances of them differing. The distribution rules
//! that *are* client-visible stay in the file that owns the family: see
//! [`super::percentile`], [`super::mode`] and [`super::hypothetical`]; the group those rules
//! read is [`super::group`].
//!
//! Every refusal here names the function the *client* wrote, which for the hypothetical-set
//! family is not the name the aggregate is registered under — see [`super::hypothetical`] for
//! the rename — and every refusal that can only be reached on an executor carries a
//! [`tagged_message`] so its SQLSTATE survives the Ballista scheduler rendering the error to
//! text.

use std::any::Any;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, internal_err, not_impl_err, plan_err};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The ordered column's type, which is argument 0 whether or not there are direct
/// arguments — and the one place `DISTINCT` is refused.
///
/// PostgreSQL has no `DISTINCT` in an ordered-set aggregate, and de-duplicating the
/// *ordered* values would answer a different question than the one asked: it would change
/// the group, which every answer here describes.
pub(super) fn ordered_column(function: &str, args: &AccumulatorArgs) -> Result<DataType> {
    if args.is_distinct {
        return not_impl_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "DISTINCT is not supported for {function}: PostgreSQL has no DISTINCT in \
                     an ordered-set aggregate, and removing duplicates would change the group \
                     the answer describes"
                )
            )
        );
    }
    match args.expr_fields.first() {
        Some(ordered) => Ok(ordered.data_type().clone()),
        None => internal_err!("{function} was called without an ordered value"),
    }
}

/// The `WITHIN GROUP ORDER BY`'s sort options, refusing the spelling that has none.
///
/// Every answer here is defined by an order, so an aggregate call without the clause is not
/// a weaker version of one with it — it is a different function, which PostgreSQL does not
/// have. The coordinator refuses the spelling first, with PostgreSQL's own `42809`; this is
/// the same refusal for a call that reached a registry another way.
pub(super) fn sort_options(function: &str, args: &AccumulatorArgs) -> Result<SortOptions> {
    match args.order_bys.first() {
        Some(sort) => Ok(sort.options),
        None => plan_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "{function} is an ordered-set aggregate and requires a WITHIN GROUP \
                     clause: spell it {function}(…) WITHIN GROUP (ORDER BY expr)"
                )
            )
        ),
    }
}

/// Which end the clause counts from, for an aggregate that reads nothing else from it and
/// answers without it.
///
/// The percentiles do not go through [`sort_options`]: DataFusion spells the same function
/// as a plain two-argument aggregate (`percentile_cont(n, 0.5)`), and shadowing it must not
/// take that spelling away. The PostgreSQL spelling without the clause is refused by the
/// coordinator before it reaches here, so the leniency is not a second behaviour a client
/// can reach — it is the upstream one being kept.
pub(super) fn descending(args: &AccumulatorArgs) -> bool {
    args.order_bys
        .first()
        .map(|sort| sort.options.descending)
        .unwrap_or(false)
}

/// The literal value of the direct argument at `index`, named `what` in every refusal.
///
/// A direct argument has to be a literal: it is fixed for the whole group, so there is no
/// row to evaluate an expression against. It is read leniently by the caller, because
/// `parse_float_as_decimal` makes `0.9` arrive as `numeric` rather than `float8`.
///
/// The refusal a non-literal earns is tagged. This runs *on an executor*, so its
/// `DataFusionError` is rendered to text by the Ballista scheduler and the variant the
/// coordinator would have classified is gone by the time the failure arrives; a
/// [`tagged_message`] carries the code across instead. Without it the client is told
/// `XX000 internal_error` — that the server broke and the statement is worth retrying —
/// and no retry will ever make a non-literal into a literal.
///
/// A *missing* argument is the opposite case, and is deliberately left untagged. Every
/// signature that reaches here requires the argument, so DataFusion has already refused a
/// call without it during planning; arriving here means the physical plan holds fewer
/// expressions than the signature admits, which is the server broken and nothing the client
/// wrote. `XX000` is the honest answer to that, and tagging it would tell the client a
/// feature is unsupported when the statement was fine.
pub(super) fn literal_direct_argument(
    exprs: &[Arc<dyn PhysicalExpr>],
    index: usize,
    function: &str,
    what: &str,
) -> Result<ScalarValue> {
    let Some(argument) = exprs.get(index) else {
        return internal_err!(
            "{function} was planned without its {what}: the signature requires \
             {} arguments",
            index + 1
        );
    };
    // Upcast rather than call `as_any()`: Arrow's `Array` is in scope here and owns that
    // method name too.
    let argument: &dyn Any = argument.as_ref();
    match argument.downcast_ref::<Literal>() {
        Some(literal) => Ok(literal.value().clone()),
        None => plan_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "the {what} for {function} must be a literal: a direct argument is \
                     evaluated once for the whole group, not once per row"
                )
            )
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{Field, FieldRef, Schema};
    use datafusion::common::DataFusionError;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::{Column, lit};

    use crate::error::code_of_tagged_message;

    /// An `AccumulatorArgs` shaped like the one a planned `f(direct) WITHIN GROUP (ORDER BY
    /// ordered)` hands to `accumulator`, so the refusals can be read without a session.
    fn call<'a>(
        ordered: &'a [FieldRef],
        order_bys: &'a [PhysicalSortExpr],
        is_distinct: bool,
        schema: &'a Schema,
    ) -> AccumulatorArgs<'a> {
        AccumulatorArgs {
            return_field: Arc::new(Field::new("f", DataType::Int64, true)),
            schema,
            ignore_nulls: false,
            order_bys,
            is_reversed: false,
            name: "f",
            is_distinct,
            exprs: &[],
            expr_fields: ordered,
        }
    }

    fn ordered_field() -> Vec<FieldRef> {
        vec![Arc::new(Field::new("n", DataType::Int64, true))]
    }

    fn ascending() -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr::new(
            Arc::new(Column::new("n", 0)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )]
    }

    #[test]
    fn the_ordered_column_is_argument_zero() {
        let schema = Schema::empty();
        let fields = ordered_field();
        let args = call(&fields, &[], false, &schema);
        assert_eq!(ordered_column("mode", &args).unwrap(), DataType::Int64);
    }

    /// `DISTINCT` is refused in PostgreSQL's terms and with a SQLSTATE that survives the
    /// Ballista boundary: without the tag the client is told the server broke and a retry
    /// might help, and no retry turns `DISTINCT` into something these aggregates have.
    #[test]
    fn distinct_is_refused_with_a_tagged_sqlstate() {
        let schema = Schema::empty();
        let fields = ordered_field();
        let args = call(&fields, &[], true, &schema);
        let err = ordered_column("mode", &args).expect_err("DISTINCT has no meaning here");
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::FeatureNotSupported)
        );
        assert!(
            err.to_string()
                .contains("DISTINCT is not supported for mode"),
            "got: {err}"
        );
    }

    /// The sentence a client gets for `mode(x)` that reached a registry without the clause,
    /// which names the spelling that works rather than an arity nobody wrote.
    #[test]
    fn a_missing_within_group_clause_is_refused_naming_the_spelling() {
        let schema = Schema::empty();
        let fields = ordered_field();
        let args = call(&fields, &[], false, &schema);
        let err = sort_options("mode", &args).expect_err("there is no order to answer under");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::FeatureNotSupported)
        );
        assert!(
            err.to_string()
                .contains("spell it mode(…) WITHIN GROUP (ORDER BY expr)"),
            "got: {err}"
        );
    }

    /// The percentiles keep DataFusion's clause-less spelling, so the missing clause is a
    /// default rather than a refusal for them: ascending, which is what `ORDER BY` means.
    #[test]
    fn a_missing_clause_reads_as_ascending_for_the_aggregates_that_allow_it() {
        let schema = Schema::empty();
        let fields = ordered_field();
        let order_bys = ascending();
        assert!(!descending(&call(&fields, &[], false, &schema)));
        assert!(!descending(&call(&fields, &order_bys, false, &schema)));
    }

    #[test]
    fn a_literal_direct_argument_is_read_off_the_plan() {
        let exprs = vec![lit(1_i64), lit(0.5_f64)];
        assert_eq!(
            literal_direct_argument(&exprs, 1, "percentile_cont", "percentile fraction").unwrap(),
            ScalarValue::Float64(Some(0.5))
        );
    }

    /// A column where a fraction belongs is the client's mistake, so it is tagged: the
    /// refusal has to arrive as `feature_not_supported` and not as the `XX000` that says the
    /// statement is worth retrying.
    #[test]
    fn a_non_literal_direct_argument_is_refused_with_a_tagged_sqlstate() {
        let exprs: Vec<Arc<dyn PhysicalExpr>> =
            vec![lit(1_i64), Arc::new(Column::new("fraction", 0))];
        let err = literal_direct_argument(&exprs, 1, "percentile_cont", "percentile fraction")
            .expect_err("a column is not fixed for the group");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::FeatureNotSupported)
        );
        assert!(
            err.to_string()
                .contains("the percentile fraction for percentile_cont must be a literal"),
            "got: {err}"
        );
    }

    /// The opposite case, and the reason the two are not one message: a plan holding fewer
    /// expressions than the signature admits is the server broken, so it stays `XX000` and
    /// untagged rather than telling the client a feature is missing.
    #[test]
    fn a_missing_direct_argument_is_an_untagged_internal_error() {
        let exprs = vec![lit(1_i64)];
        let err = literal_direct_argument(&exprs, 1, "rank", "hypothetical value")
            .expect_err("the signature requires it");
        assert!(matches!(err, DataFusionError::Internal(_)), "{err}");
        assert_eq!(code_of_tagged_message(&err.to_string()), None);
    }
}
