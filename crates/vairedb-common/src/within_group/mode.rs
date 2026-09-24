//! `mode() WITHIN GROUP (ORDER BY expr)` — the most frequent non-null value of `expr`.
//!
//! PostgreSQL documents the tie as arbitrary and resolves it in practice by taking the first
//! of the tied values in the `WITHIN GROUP` sort order, which is what [`mode_of`] does — so
//! `ORDER BY x` and `ORDER BY x DESC` can legitimately disagree on a tie, and each agrees
//! with PostgreSQL. That is the whole reason `mode` is an ordered-set aggregate at all: the
//! clause is not decoration, it is the tie-break.
//!
//! `mode` needs none of the renaming the hypothetical-set family needs — no window function
//! has that name — so it is registered as PostgreSQL spells it.

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

use super::clause;
use super::group::{self, Grouped, SortedRows};

/// What the client writes, which is what every message names — and what it is registered as,
/// there being no window function of the name to protect.
const MODE: &str = "mode";

/// The aggregate this file owns, for [`super::register_within_group_aggregates`].
pub(super) fn udafs() -> Vec<Arc<AggregateUDF>> {
    vec![Arc::new(AggregateUDF::from(Mode::new()))]
}

/// `mode() WITHIN GROUP (ORDER BY expr)` — the most frequent value of `expr`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Mode {
    signature: Signature,
}

impl Mode {
    fn new() -> Self {
        Self {
            // No direct arguments: the one argument is the ordered value the `WITHIN GROUP`
            // clause supplies, of any type the row format can sort.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Mode {
    fn name(&self) -> &str {
        MODE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // A value of the input, so the input's type — PostgreSQL's `anyelement`.
        match arg_types.first() {
            Some(ordered) => Ok(ordered.clone()),
            None => internal_err!("{MODE} was called without an ordered value"),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        group::state_fields(MODE, &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordered = clause::ordered_column(MODE, &args)?;
        Ok(Box::new(ModeAccumulator {
            // Only which end the order counts from is read: nulls are never candidates, so
            // where the clause would put them cannot change the answer.
            descending: clause::sort_options(MODE, &args)?.descending,
            group: Grouped::new(ordered, MODE),
        }))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// Accumulates the group's values and answers the most frequent one once it has them all.
#[derive(Debug)]
struct ModeAccumulator {
    descending: bool,
    group: Grouped,
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.group.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.group.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.group.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        mode_of(&self.group.accumulated()?, self.descending)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.group.size()
    }
}

/// The most frequent value of `values`, ignoring nulls the way every aggregate does.
///
/// Kept separate from the accumulator so the rule that has to match PostgreSQL — including
/// which of two equally frequent values wins — can be tested on an array alone.
fn mode_of(values: &ArrayRef, descending: bool) -> Result<ScalarValue> {
    // Nulls are not candidates, which is the rule `SortedRows` owns: they are parked past the
    // end and never reach a rank. An empty group is a null, not an error — PostgreSQL's
    // `mode()` over no rows.
    let Some(sorted) = SortedRows::of(values, descending)? else {
        return ScalarValue::try_from(values.data_type());
    };

    // Sorted, equal values are adjacent, so the frequencies are run lengths.
    let mut best: Option<(usize, ScalarValue)> = None;
    let mut run: Option<(usize, ScalarValue)> = None;
    for rank in 0..sorted.rows() {
        let value = ScalarValue::try_from_array(values, sorted.row(rank))?;
        let length = match &run {
            Some((length, previous)) if *previous == value => length + 1,
            _ => 1,
        };
        // `>` and not `>=` is the tie rule: the first run to reach a given length keeps the
        // answer, and the first run is the first value in the clause's own sort order — which
        // is how PostgreSQL resolves the tie it documents as arbitrary.
        if best.as_ref().is_none_or(|(longest, _)| length > *longest) {
            best = Some((length, value.clone()));
        }
        run = Some((length, value));
    }
    match best {
        Some((_, value)) => Ok(value),
        None => internal_err!(
            "{MODE} found no value in a group that has {} of them",
            sorted.rows()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringArray};

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn text(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// The mode is the most frequent value, not the largest or the first.
    #[test]
    fn the_mode_is_the_most_frequent_value() {
        let values = ints(&[Some(9), Some(1), Some(2), Some(2), Some(2), Some(3)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(2))
        );
    }

    /// Nulls are not candidates, however many of them there are — PostgreSQL's `mode()`
    /// ignores them like any other aggregate.
    #[test]
    fn nulls_are_never_the_mode() {
        let values = ints(&[None, None, None, Some(7)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(7))
        );
    }

    /// A group with no non-null value is a null, not an error.
    #[test]
    fn a_group_with_no_value_has_no_mode() {
        assert_eq!(
            mode_of(&ints(&[None, None]), false).unwrap(),
            ScalarValue::Int64(None)
        );
        assert_eq!(
            mode_of(&ints(&[]), false).unwrap(),
            ScalarValue::Int64(None)
        );
    }

    /// The tie is broken by the clause's own order, which is why `mode()` is a `WITHIN GROUP`
    /// aggregate at all: `ORDER BY x` and `ORDER BY x DESC` pick opposite ends of the tie,
    /// and PostgreSQL agrees with each.
    #[test]
    fn a_tie_is_broken_by_the_sort_order() {
        let values = ints(&[Some(1), Some(1), Some(5), Some(5)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(mode_of(&values, true).unwrap(), ScalarValue::Int64(Some(5)));
    }

    /// The input order does not change the answer: it is a property of the group, and the
    /// accumulator sorts before reading it.
    #[test]
    fn the_input_order_does_not_change_the_mode() {
        let one = ints(&[Some(3), Some(1), Some(3), Some(2)]);
        let other = ints(&[Some(2), Some(3), Some(1), Some(3)]);
        assert_eq!(mode_of(&one, false).unwrap(), ScalarValue::Int64(Some(3)));
        assert_eq!(mode_of(&other, false).unwrap(), ScalarValue::Int64(Some(3)));
    }

    /// Text has a mode too, which is the reason the group is kept as an Arrow array rather
    /// than as numbers: `mode()` is `anyelement` in PostgreSQL.
    #[test]
    fn the_mode_of_text_is_text() {
        let values = text(&["b", "a", "b"]);
        assert_eq!(mode_of(&values, false).unwrap(), ScalarValue::from("b"));
    }
}
