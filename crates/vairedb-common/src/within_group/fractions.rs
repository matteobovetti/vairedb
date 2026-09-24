//! The percentiles' direct argument: one fraction, or PostgreSQL's array of them.
//!
//! PostgreSQL has a second signature for each percentile: `percentile_cont(float8[])` answers
//! `float8[]`, and `percentile_disc(float8[])` answers `anyarray` — one element per fraction,
//! in the order given. It exists because the group is read once for all of them, which is
//! exactly the saving a client loses by calling the aggregate once per fraction.
//!
//! Nothing about the distribution changes, which is why no distribution rule is in this file:
//! the same accumulator holds the same values and [`super::percentile`] answers every
//! fraction against the same sorted array. What is here is the *argument* — the two Arrow
//! shapes it can arrive in, the shape the answer takes from it, and the range check. That
//! check is worded and coded exactly as the coordinator's pre-flight check words it, so the
//! two places a bad fraction can be caught cannot be told apart by a client.
//!
//! ## A divergence this file does not fix
//!
//! `percentile_cont(ARRAY[]::float8[])` answers the empty array in PostgreSQL. Here the
//! answer's shape is read off the *first* fraction rather than off the argument's own type,
//! so an argument with no fractions in it reads as a scalar and `evaluate` faults with
//! `a percentile needs a fraction to answer` — an `XX000` for a statement that is fine.
//! Reading the shape off the argument's type is the fix and is a change to what a client
//! sees, so it is left to the change that owns it rather than smuggled in with a refactor.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion::physical_expr::PhysicalExpr;

use super::clause;
use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// How the direct argument is named in a refusal, and so what a client is told to fix.
const FRACTION: &str = "percentile fraction";

/// The fractions one call asks for, and whether it asked for them as an array.
#[derive(Debug)]
pub(super) struct Fractions {
    values: Vec<f64>,
    /// Whether the answer is an array, which is the direct argument's shape and not the number
    /// of fractions: `ARRAY[0.5]` answers a one-element array, `0.5` a scalar.
    array: bool,
}

impl Fractions {
    /// The fractions the direct argument of `function` spells, checked.
    ///
    /// The argument itself is read by [`clause::literal_direct_argument`], which owns the rule
    /// that a direct argument is a literal and the tagging every refusal on an executor needs.
    pub(super) fn of(exprs: &[Arc<dyn PhysicalExpr>], function: &str) -> Result<Self> {
        let argument = clause::literal_direct_argument(exprs, 1, function, FRACTION)?;
        let (given, array) = match &argument {
            list @ (ScalarValue::List(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::FixedSizeList(_)) => (elements_of(list, function)?, true),
            scalar => (vec![scalar.clone()], false),
        };
        let mut values = Vec::with_capacity(given.len());
        for value in given {
            values.push(fraction_of(value, function)?);
        }
        Ok(Self {
            // The shape follows the first fraction, not the argument's type — which is the
            // divergence the module doc records, for an argument that has no first fraction.
            array: array && !values.is_empty(),
            values,
        })
    }

    pub(super) fn values(&self) -> &[f64] {
        &self.values
    }

    pub(super) fn is_array(&self) -> bool {
        self.array
    }
}

/// Whether `data_type` is one of Arrow's list shapes, i.e. PostgreSQL's `float8[]` direct
/// argument rather than a single fraction.
pub(super) fn is_list(data_type: Option<&DataType>) -> bool {
    matches!(
        data_type,
        Some(DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _))
    )
}

/// `element[]`, as the nullable-element list PostgreSQL's array-of-fractions overload
/// answers.
pub(super) fn list_of(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(element, true)))
}

/// Every element of a list literal, in the order it was written.
///
/// `ScalarValue::List` and its wider siblings all hold a one-row array whose single element is
/// the list, so the elements are that row's, read exactly as a scalar fraction is read.
fn elements_of(list: &ScalarValue, function: &str) -> Result<Vec<ScalarValue>> {
    let array = list.to_array()?;
    let Some(elements) = list_elements(&array) else {
        return internal_err!("{function} was given a list literal with no list in it");
    };
    (0..elements.len())
        .map(|index| ScalarValue::try_from_array(&elements, index))
        .collect()
}

/// The single list element of a one-row list array, whatever its list flavour.
fn list_elements(array: &ArrayRef) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => array.as_list_opt::<i32>()?.iter().next().flatten(),
        DataType::LargeList(_) => array.as_list_opt::<i64>()?.iter().next().flatten(),
        DataType::FixedSizeList(_, _) => array.as_fixed_size_list_opt()?.iter().next().flatten(),
        _ => None,
    }
}

/// One fraction, read leniently and then held to PostgreSQL's range.
///
/// Leniently because `parse_float_as_decimal` makes `0.9` arrive as `numeric` rather than
/// `float8`; to PostgreSQL's range because `percentile value 1.5 is not between 0 and 1` is
/// the sentence and the `22023` a client gets for it, whether the coordinator caught the
/// literal first or the aggregate caught the evaluated value here.
fn fraction_of(value: ScalarValue, function: &str) -> Result<f64> {
    let fraction = match value.cast_to(&DataType::Float64) {
        Ok(ScalarValue::Float64(Some(fraction))) => fraction,
        _ => {
            return plan_err!(
                "{}",
                tagged_message(
                    VdbErrorCode::InvalidParameterValue,
                    format!(
                        "the {FRACTION} for {function} must be a number between \
                         0 and 1, not {value}"
                    )
                )
            );
        }
    };
    if !(0.0..=1.0).contains(&fraction) {
        return exec_err!(
            "{}",
            tagged_message(
                VdbErrorCode::InvalidParameterValue,
                format!("percentile value {fraction} is not between 0 and 1")
            )
        );
    }
    Ok(fraction)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::physical_expr::expressions::{Column, Literal, lit};

    use crate::error::code_of_tagged_message;

    /// The two physical arguments a planned `percentile_cont(direct) WITHIN GROUP (ORDER BY n)`
    /// carries: the ordered value, and the direct argument this file reads.
    fn call(direct: ScalarValue) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![lit(1_i64), Arc::new(Literal::new(direct))]
    }

    fn fractions(direct: ScalarValue) -> Result<Fractions> {
        Fractions::of(&call(direct), "percentile_cont")
    }

    #[test]
    fn a_scalar_fraction_answers_a_scalar() {
        let read = fractions(ScalarValue::Float64(Some(0.9))).unwrap();
        assert_eq!(read.values(), &[0.9]);
        assert!(!read.is_array(), "`0.9` is not `ARRAY[0.9]`");
    }

    /// `parse_float_as_decimal` is on, so `0.5` reaches the aggregate as `numeric` — which is
    /// why the fraction is read leniently rather than required to already be `float8`.
    #[test]
    fn a_numeric_fraction_is_read_as_the_float_postgresql_declares() {
        let read = fractions(ScalarValue::Decimal128(Some(5), 2, 1)).unwrap();
        assert_eq!(read.values(), &[0.5]);
    }

    /// PostgreSQL's overload, and the whole reason it exists: every fraction in the order it
    /// was written, answered against one reading of the group.
    #[test]
    fn an_array_of_fractions_keeps_the_order_it_was_written_in() {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Float64(Some(0.9)),
                ScalarValue::Float64(Some(0.25)),
                ScalarValue::Float64(Some(0.5)),
            ],
            &DataType::Float64,
            true,
        ));
        let read = fractions(list).unwrap();
        assert_eq!(read.values(), &[0.9, 0.25, 0.5]);
        assert!(read.is_array(), "an array argument answers an array");
    }

    /// A one-element array is still an array: the answer's shape is the argument's shape and
    /// not the number of fractions in it.
    #[test]
    fn a_one_element_array_still_answers_an_array() {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Float64(Some(0.5))],
            &DataType::Float64,
            true,
        ));
        let read = fractions(list).unwrap();
        assert_eq!(read.values(), &[0.5]);
        assert!(read.is_array());
    }

    /// PostgreSQL's own sentence and its `22023`, which the coordinator's pre-flight check
    /// spells identically: a client must not be able to tell which of the two caught it.
    #[test]
    fn a_fraction_outside_zero_to_one_is_refused_in_postgresqls_words() {
        let err = fractions(ScalarValue::Float64(Some(1.5)))
            .expect_err("1.5 is not a percentile fraction");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::InvalidParameterValue)
        );
        assert!(
            err.to_string()
                .contains("percentile value 1.5 is not between 0 and 1"),
            "got: {err}"
        );
    }

    /// One bad fraction refuses the whole call, naming the offending value the way a single
    /// fraction is named — there is no partial answer for an array.
    #[test]
    fn one_bad_fraction_in_an_array_refuses_the_call() {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Float64(Some(0.5)),
                ScalarValue::Float64(Some(-0.25)),
            ],
            &DataType::Float64,
            true,
        ));
        let err = fractions(list).expect_err("-0.25 is not a percentile fraction");
        assert!(
            err.to_string()
                .contains("percentile value -0.25 is not between 0 and 1"),
            "got: {err}"
        );
    }

    /// A fraction that is not a number at all — `percentile_cont('half')` — is told what a
    /// fraction is, rather than being cast to something arbitrary.
    #[test]
    fn a_fraction_that_is_not_a_number_is_refused_with_its_own_sentence() {
        let err =
            fractions(ScalarValue::from("half")).expect_err("'half' is not a percentile fraction");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::InvalidParameterValue)
        );
        assert!(
            err.to_string().contains(
                "the percentile fraction for percentile_cont must be a number between 0 and 1, \
                 not half"
            ),
            "got: {err}"
        );
    }

    /// The rule [`clause::literal_direct_argument`] owns, reached through the fraction: a
    /// column cannot be a fraction, and the refusal has to carry its own SQLSTATE across the
    /// Ballista boundary rather than arrive as a retryable `XX000`.
    #[test]
    fn a_column_where_a_fraction_belongs_is_refused_and_tagged() {
        let exprs: Vec<Arc<dyn PhysicalExpr>> =
            vec![lit(1_i64), Arc::new(Column::new("fraction", 0))];
        let err = Fractions::of(&exprs, "percentile_cont").expect_err("a column is not a literal");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::FeatureNotSupported)
        );
    }

    /// The return type the overload declares, which is what tells `evaluate` to build a list:
    /// the fraction argument's own Arrow shape is the whole of the distinction.
    #[test]
    fn a_list_argument_is_recognised_at_every_offset_width() {
        let element = Arc::new(Field::new_list_field(DataType::Float64, true));
        assert!(is_list(Some(&DataType::List(Arc::clone(&element)))));
        assert!(is_list(Some(&DataType::LargeList(Arc::clone(&element)))));
        assert!(is_list(Some(&DataType::FixedSizeList(element, 2))));
        assert!(!is_list(Some(&DataType::Float64)));
        assert!(!is_list(None), "no direct argument is not an array");
        assert_eq!(
            list_of(DataType::Float64),
            DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
        );
    }
}
