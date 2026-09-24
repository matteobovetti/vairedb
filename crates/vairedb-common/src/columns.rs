//! Reading one argument's column in the Arrow layout the rule behind it is written
//! against.
//!
//! A scalar function receives a [`ColumnarValue`](datafusion::logical_expr::ColumnarValue)
//! whose layout is whatever survived coercion and whatever the shard produced — `Utf8`
//! usually, a view or large variant sometimes, a `Dictionary` when a scan encoded one.
//! None of that is the question a PostgreSQL rule asks, so each function narrowed the
//! column before reading it, and each function grew its own copy of the narrowing.
//!
//! The copies were identical — three of [`strings`], two of [`list_row`] — so they are one
//! copy here. This module holds no rule of its own: nothing in it decides anything a client
//! could observe beyond the error text it is handed, which is why it can be shared by
//! functions that agree on nothing else.

use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err};

/// One argument as a `Utf8` array, casting any other text layout into it.
///
/// `Utf8` is passed through untouched; a view or large variant, or a dictionary a scan
/// encoded, is cast. [`ScalarUDFImpl::coerce_types`](datafusion::logical_expr::ScalarUDFImpl::coerce_types)
/// asking for `Utf8` is not enough on its own to make the cast unreachable, which is why it
/// is here rather than asserted away.
pub(crate) fn strings(array: &ArrayRef) -> Result<StringArray> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().clone()),
        _ => Ok(cast(array, &DataType::Utf8)?.as_string::<i32>().clone()),
    }
}

/// The list held at `row` of a list column, whichever offset width it uses.
///
/// `what` names the caller's own expectation, so the refusal reads as the rule the client
/// broke rather than as an Arrow layout complaint: `"a #> path must be a text array, got
/// Int64"`.
pub(crate) fn list_row(list: &ArrayRef, row: usize, what: &str) -> Result<ArrayRef> {
    match list.data_type() {
        DataType::List(_) => Ok(list.as_list::<i32>().value(row)),
        DataType::LargeList(_) => Ok(list.as_list::<i64>().value(row)),
        other => exec_err!("{what}, got {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, ListArray, StringViewArray};
    use std::sync::Arc;

    #[test]
    fn a_utf8_column_is_read_without_a_cast() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let read = strings(&array).expect("utf8 reads");
        assert_eq!(read.value(0), "a");
        assert!(read.is_null(1));
    }

    /// The reason the cast arm exists: a view layout is a shape, not a different type.
    #[test]
    fn a_view_column_is_cast_rather_than_refused() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec![Some("a"), None]));
        let read = strings(&array).expect("a view layout reads");
        assert_eq!(read.value(0), "a");
        assert!(read.is_null(1));
    }

    #[test]
    fn a_list_row_is_read_at_either_offset_width() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(arrow::datatypes::Field::new_list_field(
                DataType::Utf8,
                true,
            )),
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(vec![0, 2, 3])),
            values,
            None,
        ));
        assert_eq!(list_row(&list, 0, "x").expect("row 0").len(), 2);
        assert_eq!(list_row(&list, 1, "x").expect("row 1").len(), 1);
    }

    /// The caller's wording, not Arrow's, is what a client reads.
    #[test]
    fn a_non_list_column_is_refused_in_the_callers_words() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let err = list_row(&array, 0, "a #> path must be a text array")
            .expect_err("an int64 column is not a list");
        assert!(
            err.to_string()
                .contains("a #> path must be a text array, got Int64"),
            "got: {err}"
        );
    }
}
