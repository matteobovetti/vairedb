//! The group itself: the values every aggregate here accumulates, the shape they cross the
//! Ballista wire in, and the order the clause puts them in.
//!
//! None of this is a rule a client can see on its own — it is the state a partial and a final
//! aggregate have to agree on, and the null rule two of the three families have to answer
//! identically. Two copies of either is two chances of them differing, and a group ordered
//! two ways or merged two ways is a *wrong answer* rather than an error: the failure mode
//! these aggregates have that a client cannot detect. The distribution rules that *are*
//! client-visible stay in the file that owns the family: see [`super::percentile`],
//! [`super::mode`] and [`super::hypothetical`]; the call those families read is
//! [`super::clause`].

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, UInt32Array, new_empty_array};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, concat, sort_to_indices};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::function::StateFieldsArgs;

/// The intermediate state of every aggregate here: every value seen, as a list.
///
/// None of them can be summarised into anything smaller — an exact percentile, the most
/// frequent value and the position of a hypothetical row are all properties of the whole
/// distribution — so the state is the values themselves, which is what lets the final
/// aggregate merge the partials one per shard.
pub(super) fn state_fields(function: &str, args: &StateFieldsArgs) -> Result<Vec<FieldRef>> {
    let Some(ordered) = args.input_fields.first() else {
        return internal_err!("{function} was called without an ordered value");
    };
    let element = Field::new_list_field(ordered.data_type().clone(), true);
    Ok(vec![
        Field::new(
            format!("{}[{function}]", args.name),
            DataType::List(Arc::new(element)),
            true,
        )
        .into(),
    ])
}

/// The group's ordered values, gathered batch by batch and concatenated once.
#[derive(Debug)]
pub(super) struct Grouped {
    /// The ordered column's type, needed to describe an empty group's state.
    ordered_type: DataType,
    /// The name the *client* wrote, for the errors below. It is fixed for the aggregate's
    /// whole life, so it is held once here rather than passed to every call — one aggregate
    /// naming another in a message is then not a thing a caller can get wrong.
    function: &'static str,
    /// One entry per batch seen, concatenated only when the answer is needed.
    values: Vec<ArrayRef>,
}

impl Grouped {
    pub(super) fn new(ordered_type: DataType, function: &'static str) -> Self {
        Self {
            ordered_type,
            function,
            values: Vec::new(),
        }
    }

    pub(super) fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(ordered) = values.first() else {
            return internal_err!("{} needs an ordered value to accumulate", self.function);
        };
        if !ordered.is_empty() {
            self.values.push(Arc::clone(ordered));
        }
        Ok(())
    }

    pub(super) fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(state) = states.first() else {
            return internal_err!("{} needs its own state to merge", self.function);
        };
        for partial in state.as_list::<i32>().iter().flatten() {
            if !partial.is_empty() {
                self.values.push(partial);
            }
        }
        Ok(())
    }

    /// The one state column [`state_fields`] declares: every value seen, as a one-row list.
    pub(super) fn state(&self) -> Result<Vec<ScalarValue>> {
        let values = self.accumulated()?;
        let element = Field::new_list_field(values.data_type().clone(), true);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
        let list = ListArray::new(Arc::new(element), offsets, values, None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    /// Every value seen so far, in one array.
    pub(super) fn accumulated(&self) -> Result<ArrayRef> {
        match self.values.len() {
            0 => Ok(new_empty_array(&self.ordered_type)),
            1 => Ok(Arc::clone(&self.values[0])),
            _ => {
                let parts: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();
                Ok(concat(&parts)?)
            }
        }
    }

    pub(super) fn size(&self) -> usize {
        self.values
            .iter()
            .map(|values| values.get_array_memory_size())
            .sum()
    }
}

/// The group's non-null values by rank, in the order the clause asked for.
///
/// [`super::mode`] and [`super::percentile`] both answer by *position* — the k-th value, or
/// the two the answer falls between — and both have to ignore nulls the way every aggregate
/// does. Arrow gives both at once: `sort_to_indices` asked for `nulls_first: false` parks the
/// nulls past the end, so the first [`rows`](Self::rows) ranks are exactly the non-null
/// values in the clause's own order. Held together because two copies of that sentence is
/// two chances of the two families disagreeing about where a null goes, which a client sees
/// as one of them answering a value where the other answers NULL.
///
/// [`super::hypothetical`] deliberately does not use this: its nulls are *ordered* rather
/// than ignored, because the window functions it mirrors would see them.
#[derive(Debug)]
pub(super) struct SortedRows {
    /// The rows of the ordered column, ranked; nulls past the end and not counted.
    order: UInt32Array,
    rows: usize,
}

impl SortedRows {
    /// The non-null values of `values` by rank, or `None` when there are none — which every
    /// aggregate here answers as a null rather than as an error, the way PostgreSQL does for
    /// a group with no rows.
    pub(super) fn of(values: &ArrayRef, descending: bool) -> Result<Option<Self>> {
        let rows = values.len() - values.null_count();
        if rows == 0 {
            return Ok(None);
        }
        let order = sort_to_indices(
            values,
            Some(SortOptions {
                descending,
                nulls_first: false,
            }),
            None,
        )?;
        Ok(Some(Self { order, rows }))
    }

    /// How many values there are, which is every distribution's denominator.
    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    /// Which row of the ordered column holds the value at `rank`.
    pub(super) fn row(&self, rank: usize) -> usize {
        self.order.value(rank) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn grouped() -> Grouped {
        Grouped::new(DataType::Int64, "mode")
    }

    fn read(values: &ArrayRef) -> Vec<Option<i64>> {
        values
            .as_primitive::<arrow::datatypes::Int64Type>()
            .iter()
            .collect()
    }

    /// A group that saw nothing still has to describe itself, in the ordered column's type:
    /// an aggregate over zero rows answers NULL, and its state has to be mergeable by a final
    /// aggregate that saw rows.
    #[test]
    fn an_empty_group_accumulates_to_an_empty_array_of_the_ordered_type() {
        let group = grouped();
        let values = group.accumulated().unwrap();
        assert_eq!(values.len(), 0);
        assert_eq!(values.data_type(), &DataType::Int64);
    }

    /// The batches a shard produces are one group: the values are kept in the order they
    /// arrived, and every family sorts them itself.
    #[test]
    fn the_batches_of_a_group_concatenate_into_one_array() {
        let mut group = grouped();
        group.update_batch(&[ints(&[Some(1), Some(2)])]).unwrap();
        group.update_batch(&[ints(&[])]).unwrap();
        group.update_batch(&[ints(&[None, Some(3)])]).unwrap();
        assert_eq!(
            read(&group.accumulated().unwrap()),
            vec![Some(1), Some(2), None, Some(3)]
        );
    }

    /// The wire contract, and the only thing in this module that two *processes* have to
    /// agree on: what a partial aggregate emits as its state is exactly what a final
    /// aggregate merges, values, nulls and all. A partial that dropped or reordered a value
    /// here would answer a different distribution than a single-node run, which is the one
    /// failure these aggregates have that a client cannot see.
    #[test]
    fn a_partials_state_merges_back_into_the_same_values() {
        let mut shard = grouped();
        shard
            .update_batch(&[ints(&[Some(1), None, Some(2)])])
            .unwrap();
        let state = shard.state().unwrap();

        let mut final_aggregate = grouped();
        let states: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        final_aggregate.merge_batch(&states).unwrap();
        final_aggregate.merge_batch(&states).unwrap();
        assert_eq!(
            read(&final_aggregate.accumulated().unwrap()),
            vec![Some(1), None, Some(2), Some(1), None, Some(2)],
            "two shards' partials are both merged, neither replacing the other"
        );
    }

    /// An empty shard's state is a list with no elements rather than a NULL list, so a final
    /// aggregate merging it adds nothing instead of faulting on it.
    #[test]
    fn an_empty_partials_state_merges_as_nothing() {
        let state = grouped().state().unwrap();
        let states: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        let mut final_aggregate = grouped();
        final_aggregate.merge_batch(&states).unwrap();
        assert_eq!(final_aggregate.accumulated().unwrap().len(), 0);
    }

    /// The state column names the aggregate and holds a list of the ordered column's type —
    /// which is what makes a partial's schema readable by a final aggregate on another node.
    #[test]
    fn the_state_field_is_a_list_of_the_ordered_type() {
        let input: Vec<FieldRef> = vec![Arc::new(Field::new("n", DataType::Utf8, true))];
        let args = StateFieldsArgs {
            name: "mode(n)",
            input_fields: &input,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            ordering_fields: &[],
            is_distinct: false,
        };
        let fields = state_fields("mode", &args).unwrap();
        let [field] = fields.as_slice() else {
            panic!("one state column, got {fields:?}");
        };
        assert_eq!(field.name(), "mode(n)[mode]");
        assert_eq!(
            field.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
        );
    }

    /// Nulls are parked past the end and not counted, which is the rule `mode` and the
    /// percentiles share: their ranks only ever reach real values.
    #[test]
    fn the_ranks_cover_the_non_null_values_only() {
        let values = ints(&[Some(3), None, Some(1), None, Some(2)]);
        let sorted = SortedRows::of(&values, false)
            .unwrap()
            .expect("three values");
        assert_eq!(sorted.rows(), 3);
        let ranked: Vec<usize> = (0..sorted.rows()).map(|rank| sorted.row(rank)).collect();
        assert_eq!(ranked, vec![2, 4, 0], "the rows holding 1, 2 and 3");
    }

    /// `DESC` ranks from the other end, and the nulls stay past the end either way: where the
    /// clause would put them cannot change an answer that never reaches them.
    #[test]
    fn a_descending_clause_ranks_from_the_other_end() {
        let values = ints(&[Some(3), None, Some(1), Some(2)]);
        let sorted = SortedRows::of(&values, true)
            .unwrap()
            .expect("three values");
        let ranked: Vec<usize> = (0..sorted.rows()).map(|rank| sorted.row(rank)).collect();
        assert_eq!(ranked, vec![0, 3, 2], "the rows holding 3, 2 and 1");
    }

    /// A group with no non-null value has no ranks at all, which is how every family here
    /// reaches its null answer.
    #[test]
    fn a_group_of_nulls_has_no_ranks() {
        assert!(
            SortedRows::of(&ints(&[None, None]), false)
                .unwrap()
                .is_none()
        );
        assert!(SortedRows::of(&ints(&[]), false).unwrap().is_none());
        let text: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        assert!(SortedRows::of(&text, false).unwrap().is_none());
    }
}
