//! The `numeric` an exact aggregate answers in, and the plumbing that shape brings with it.
//!
//! [`crate::avg_udaf`] and [`crate::stats_udaf`] compute different things — an average, and
//! the variance family — but they answer in the **same** type, and that is a promise to a
//! client rather than an implementation detail: `avg(n)`, `var_samp(n)` and `stddev(n)` over
//! the same integer column all arrive as `numeric(38, 16)`, so a client that reflects on one
//! of them, or binds one receive buffer, has the others too. The precision and the scale used
//! to be written out in both modules, where changing one and forgetting the other would have
//! shipped two types under one promise; here they are written once.
//!
//! What else is here follows from that shared shape rather than from either aggregate's
//! arithmetic: the name a partial aggregate's state column carries, the single-element list a
//! `DISTINCT` accumulator ships its values in, and the refusal an answer that does not fit is
//! reported with. None of it decides anything a client can observe beyond the text of that
//! refusal, which is why two aggregates that share no arithmetic at all can share all of it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, ScalarValue};

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// The precision every exact answer is reported at: Arrow's widest 128-bit decimal, which is
/// what [`crate`]'s `sum` widening reports too.
pub(crate) const RESULT_PRECISION: u8 = 38;

/// The decimal places of every exact answer — PostgreSQL's own sixteen.
///
/// Why a fixed sixteen rather than the scale PostgreSQL chooses per value is argued in
/// [`crate::avg_udaf`]'s module doc, and [`crate::stats_udaf`] follows it: one scale across
/// `avg`, `var` and `stddev` is what lets a client read the same `numeric(38, 16)` from all
/// three.
pub(crate) const RESULT_SCALE: i8 = 16;

/// `10³⁸`, one past the widest magnitude a decimal of [`RESULT_PRECISION`] digits holds.
///
/// The bound at both ends of an exact aggregate: a running total that crosses the wire as a
/// `Decimal128(38, 0)`, and the unscaled integer of the answer itself. A value past it is
/// refused with [`out_of_range`] rather than reported at a lower precision, which is the one
/// thing that keeps a plausible wrong number off the wire.
pub(crate) const DECIMAL128_CEILING: i128 = 10i128.pow(RESULT_PRECISION as u32);

/// The out-of-range refusal for `aggregate`, tagged so it survives the trip from an executor.
///
/// This runs on the node that evaluates the aggregate, and a [`DataFusionError`] raised there
/// reaches the coordinator as text with its variant gone (§ 1.3 of the gap analysis). Without
/// the tag the client would be told `XX000 internal_error` — that the server broke and the
/// statement is worth retrying — where the truth is `22003`: the answer does not fit, and it
/// will not fit on a retry either.
pub(crate) fn out_of_range(aggregate: &str) -> DataFusionError {
    DataFusionError::Execution(tagged_message(
        VdbErrorCode::NumericValueOutOfRange,
        format!(
            "the exact {aggregate} of these values does not fit \
             numeric({RESULT_PRECISION}, {RESULT_SCALE})"
        ),
    ))
}

/// The marker an exact calculation refuses with.
///
/// Kept as a unit type rather than a [`DataFusionError`] so that the arithmetic has no opinion
/// about how a refusal is reported: the accumulator, which is the thing that knows the name of
/// the function being computed, turns it into [`out_of_range`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OutOfRange;

/// One column of `aggregate`'s partial state, named the way DataFusion names them.
pub(crate) fn state_field(aggregate: &str, suffix: &str, data_type: DataType) -> FieldRef {
    Arc::new(Field::new(
        format!("{aggregate}[{suffix}]"),
        data_type,
        true,
    ))
}

/// The single state column a `DISTINCT` accumulator carries: the values themselves, as a list.
///
/// De-duplicating is not something a running total can do — whether a value counts towards one
/// depends on every value before it — so the values are what crosses the wire, and this is the
/// type [`distinct_state`] builds a value of.
pub(crate) fn distinct_state_field(aggregate: &str, element: DataType) -> FieldRef {
    state_field(
        aggregate,
        "distinct",
        DataType::List(Arc::new(Field::new_list_field(element, true))),
    )
}

/// `values` as the one-row list [`distinct_state_field`] declares.
pub(crate) fn distinct_state(values: ArrayRef) -> ScalarValue {
    let element = Field::new_list_field(values.data_type().clone(), true);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
    ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(element),
        offsets,
        values,
        None,
    )))
}
