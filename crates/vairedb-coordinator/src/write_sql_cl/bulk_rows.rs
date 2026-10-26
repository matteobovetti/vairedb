//! Materialized rows as bulk-insert payloads: the counterpart of [`super::rows`] that
//! ships a batch to the shards as data instead of spelling it into `INSERT ...
//! VALUES` text.
//!
//! The literal lane is the definition this one has to meet, value for value. A row
//! written either way must land on the same shard and store the same value, or a table
//! loaded in bulk would answer differently from the same table loaded by INSERT — a
//! silently wrong answer, which is the class a refusal is preferred to. So nothing here
//! decides what the literal lane already decides:
//!
//! * **Which shard.** A row's routing value is computed from the very literal the
//!   literal lane writes for its shard-key cell, by the function that lane routes with
//!   ([`super::routing_value`]). A row whose key that function cannot route — NULL, or
//!   a value with no routable spelling — makes the whole batch the literal lane's,
//!   which refuses it in its own words.
//! * **Which value.** A cell the literal lane writes as a bare number, `true`/`false`
//!   or a quoted copy of its own string travels as that integer, boolean or string.
//!   Every other type travels as the exact text of its literal, so the shard casts it
//!   to the column's type exactly as it would have cast the literal. Sending a double
//!   or a timestamp as itself would be cheaper and would not be the same statement: a
//!   double cast to `NUMERIC` rounds differently from its literal's text, and the
//!   shard's engine spells a double stored in a text column differently from the
//!   renderer the literal lane uses.
//! * **Which types at all.** The literal lane's refusal of a type it cannot spell — a
//!   binary, a list, an interval — is this lane's too, in the same words.
//!
//! What it does decide is how large one payload is, since that has nothing to do with
//! what the rows mean: small enough for a node's request limit, large enough that a
//! batch is a few requests per shard rather than one per handful of rows.

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;

use crate::pgwire_handler::encoding::{arrow_array_value_to_string, is_null_on_the_wire};

use super::routing_value::{RoutedValue, expr_routing_value};
use super::rows::{cell_literal, literal_kind};

/// One request's worth of bulk rows: an Arrow IPC stream and the rows it holds.
pub struct BulkPayload {
    pub rows: usize,
    pub arrow_ipc: Bytes,
}

/// `batch` with every column in the form a shard receives it in: as itself where the
/// literal lane writes the value as a bare number, a boolean or a quoted copy of the
/// string, and as the text of its literal otherwise.
///
/// `Err(reason)` names the first column whose type the literal lane refuses, in that
/// lane's words.
pub fn bulk_wire_rows(batch: &RecordBatch) -> std::result::Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        literal_kind(field.data_type())
            .map_err(|reason| format!("column \"{}\": {reason}", field.name()))?;
        let wire = wire_column(column)
            .map_err(|e| format!("column \"{}\" could not be prepared: {e}", field.name()))?;
        fields.push(Field::new(field.name(), wire.data_type().clone(), true));
        columns.push(wire);
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| e.to_string())
}

/// The routing value of every row's shard key, in row order — or `None` if any row's
/// key is one the literal lane would refuse to route, which leaves the batch to that
/// lane and its refusal.
pub fn bulk_routing_values(
    batch: &RecordBatch,
    key_column: usize,
) -> std::result::Result<Option<Vec<String>>, String> {
    let schema = batch.schema();
    let field = schema.field(key_column);
    let kind = literal_kind(field.data_type())
        .map_err(|reason| format!("column \"{}\": {reason}", field.name()))?;
    let column = batch.column(key_column);

    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        match expr_routing_value(&cell_literal(column.as_ref(), row, kind), &[]) {
            RoutedValue::Value(value) => values.push(value),
            RoutedValue::Null | RoutedValue::Unroutable(_) => return Ok(None),
        }
    }
    Ok(Some(values))
}

/// `rows` as Arrow IPC streams of at most `max_bytes` each, in row order — except a
/// single row larger than that, which goes alone and is the node's to refuse.
pub fn bulk_payloads(
    rows: &RecordBatch,
    max_bytes: usize,
) -> std::result::Result<Vec<BulkPayload>, String> {
    let mut payloads = Vec::new();
    let total = rows.num_rows();
    if total == 0 {
        return Ok(payloads);
    }

    // A first cut from the size the rows occupy in memory, so a large batch is not
    // encoded whole only to be found too large; the encoded size has the last word.
    let per_row = (rows.get_array_memory_size() / total).max(1);
    let rows_per_piece = (max_bytes / per_row).clamp(1, total);
    let mut offset = 0;
    while offset < total {
        let len = rows_per_piece.min(total - offset);
        encode_within(&rows.slice(offset, len), max_bytes, &mut payloads)?;
        offset += len;
    }
    Ok(payloads)
}

/// Encode `rows`, halving them until each half fits in `max_bytes`.
fn encode_within(
    rows: &RecordBatch,
    max_bytes: usize,
    out: &mut Vec<BulkPayload>,
) -> std::result::Result<(), String> {
    let encoded = encode(rows)?;
    if encoded.len() <= max_bytes || rows.num_rows() == 1 {
        out.push(BulkPayload {
            rows: rows.num_rows(),
            arrow_ipc: Bytes::from(encoded),
        });
        return Ok(());
    }
    let half = rows.num_rows() / 2;
    encode_within(&rows.slice(0, half), max_bytes, out)?;
    encode_within(&rows.slice(half, rows.num_rows() - half), max_bytes, out)
}

fn encode(rows: &RecordBatch) -> std::result::Result<Vec<u8>, String> {
    let ipc_error = |e: datafusion::arrow::error::ArrowError| {
        format!("could not encode the rows as Arrow IPC: {e}")
    };
    let mut writer = StreamWriter::try_new(Vec::new(), &rows.schema()).map_err(ipc_error)?;
    writer.write(rows).map_err(ipc_error)?;
    writer.finish().map_err(ipc_error)?;
    writer.into_inner().map_err(ipc_error)
}

/// The kinds the literal lane writes as a bare number, a boolean, or a quoted copy of
/// the string itself — so sending the value as itself is sending its literal.
fn travels_as_itself(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
    )
}

/// One column in the form a shard receives it.
fn wire_column(
    column: &ArrayRef,
) -> std::result::Result<ArrayRef, datafusion::arrow::error::ArrowError> {
    match column.data_type() {
        data_type if travels_as_itself(data_type) => Ok(Arc::clone(column)),
        // The same string in a wider offset type.
        DataType::LargeUtf8 => cast(column, &DataType::Utf8),
        // A dictionary is an encoding, not a type. Of values that travel as
        // themselves it travels as those values; of anything else it is spelled cell
        // by cell below, by the renderer the literal lane calls on the dictionary.
        DataType::Dictionary(_, value_type) if travels_as_itself(value_type) => {
            cast(column, value_type)
        }
        _ => Ok(Arc::new(literal_text(column.as_ref()))),
    }
}

/// Every cell of `column` as the text its literal carries: the literal lane's
/// renderer, and its rule for what is NULL.
fn literal_text(column: &dyn Array) -> StringArray {
    (0..column.len())
        .map(|row| {
            (!is_null_on_the_wire(column, row)).then(|| arrow_array_value_to_string(column, row))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
        Float32Array, Float64Array, Int8Array, Int32Array, Int64Array, LargeStringArray, NullArray,
        StringViewArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Int8Type, Int32Type};
    use datafusion::arrow::ipc::reader::StreamReader;

    use super::super::{
        ROWS_PER_STATEMENT, extract_insert_row_shard_keys, insert_statements_from_batches,
        insert_template,
    };
    use super::*;
    use crate::sqlparser::ast::{Expr, SetExpr, Statement, Value};

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    /// The single literal `INSERT ... VALUES` the literal lane spells `batch` into, as
    /// rows of literal expressions.
    fn literal_rows(batch: &RecordBatch) -> Vec<Vec<Expr>> {
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let template = insert_template("t", &refs).unwrap();
        let statements =
            insert_statements_from_batches(&template, std::slice::from_ref(batch), usize::MAX)
                .unwrap();
        assert_eq!(statements.len(), 1);
        let Statement::Insert(insert) = &statements[0] else {
            panic!("expected an INSERT");
        };
        let SetExpr::Values(values) = insert.source.as_ref().unwrap().body.as_ref() else {
            panic!("expected VALUES");
        };
        values.rows.iter().map(|row| row.to_vec()).collect()
    }

    // The value half of the contract: every cell reaches the shard as exactly what the
    // literal lane would have written — the same integer, boolean or string, or the
    // same literal text for a type that travels as text — across every type the
    // literal lane accepts.
    #[test]
    fn every_cell_travels_as_the_value_its_literal_writes() {
        let source = batch(vec![
            (
                "i64",
                Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(7)])) as _,
            ),
            (
                "i32",
                Arc::new(Int32Array::from(vec![Some(-1), Some(0), None])) as _,
            ),
            (
                "u64",
                Arc::new(UInt64Array::from(vec![u64::MAX, 0, 1])) as _,
            ),
            (
                "b",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as _,
            ),
            (
                "s",
                Arc::new(StringArray::from(vec![Some("it's"), Some(""), None])) as _,
            ),
            (
                "ls",
                Arc::new(LargeStringArray::from(vec![Some("x"), None, Some("y\nz")])) as _,
            ),
            (
                "sv",
                Arc::new(StringViewArray::from(vec![Some("view"), None, Some("v")])) as _,
            ),
            (
                "f64",
                Arc::new(Float64Array::from(vec![
                    Some(0.1),
                    Some(f64::NAN),
                    Some(-0.0),
                ])) as _,
            ),
            (
                "f32",
                Arc::new(Float32Array::from(vec![
                    Some(0.1_f32),
                    None,
                    Some(f32::INFINITY),
                ])) as _,
            ),
            (
                "dec",
                Arc::new(
                    Decimal128Array::from(vec![Some(267_500), Some(-1), None])
                        .with_precision_and_scale(10, 5)
                        .unwrap(),
                ) as _,
            ),
            (
                "d",
                Arc::new(Date32Array::from(vec![Some(0), Some(19_782), None])) as _,
            ),
            (
                "t",
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(1),
                    None,
                    Some(86_399_999_999),
                ])) as _,
            ),
            (
                "ts",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_546_329_840_000_000),
                    None,
                    Some(1),
                ])) as _,
            ),
            (
                "tsz",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(1_546_329_840_000_000),
                        Some(0),
                        None,
                    ])
                    .with_timezone("+01:00"),
                ) as _,
            ),
            (
                "tsns",
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_546_329_840_123_456_789),
                    None,
                    Some(0),
                ])) as _,
            ),
            (
                "dict_s",
                Arc::new(DictionaryArray::<Int32Type>::from_iter([
                    Some("a"),
                    None,
                    Some("a"),
                ])) as _,
            ),
            (
                "dict_i",
                Arc::new(
                    DictionaryArray::<Int8Type>::try_new(
                        Int8Array::from(vec![Some(0), Some(1), None]),
                        Arc::new(Int64Array::from(vec![10, -20])),
                    )
                    .unwrap(),
                ) as _,
            ),
            (
                "dict_f",
                Arc::new(
                    DictionaryArray::<Int8Type>::try_new(
                        Int8Array::from(vec![Some(0), None, Some(0)]),
                        Arc::new(Float64Array::from(vec![1.0])),
                    )
                    .unwrap(),
                ) as _,
            ),
            ("null", Arc::new(NullArray::new(3)) as _),
        ]);

        let literal = literal_rows(&source);
        let wire = bulk_wire_rows(&source).unwrap();
        assert_eq!(wire.num_rows(), 3);

        for (c, field) in wire.schema().fields().iter().enumerate() {
            let column = wire.column(c);
            for (r, row) in literal.iter().enumerate() {
                let Expr::Value(value) = &row[c] else {
                    panic!("{}: row {r} is not a literal: {}", field.name(), row[c]);
                };
                let at = format!("column {} row {r}", field.name());
                match &value.value {
                    Value::Null => assert!(column.is_null(r), "{at}: expected NULL"),
                    Value::Number(text, _) => {
                        assert!(
                            column.data_type().is_integer(),
                            "{at}: a number must travel as an integer"
                        );
                        assert_eq!(
                            &arrow_array_value_to_string(column.as_ref(), r),
                            text,
                            "{at}"
                        );
                    }
                    Value::Boolean(b) => {
                        assert_eq!(column.as_boolean().value(r), *b, "{at}");
                    }
                    Value::SingleQuotedString(text) => {
                        assert_eq!(column.as_string::<i32>().value(r), text, "{at}");
                    }
                    other => panic!("{at}: unexpected literal {other}"),
                }
            }
        }
    }

    // The refusal half: a type the literal lane cannot spell is refused here too, and
    // in the same words, so which lane a batch took never shows in the error.
    #[test]
    fn a_type_the_literal_lane_refuses_is_refused_in_its_words() {
        let source = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as _),
            (
                "blob",
                Arc::new(BinaryArray::from(vec![b"\x01".as_ref()])) as _,
            ),
        ]);
        let template = insert_template("t", &["id", "blob"]).unwrap();
        let literal =
            insert_statements_from_batches(&template, &[source.clone()], ROWS_PER_STATEMENT)
                .err()
                .expect("the literal lane refuses a binary column");
        assert_eq!(bulk_wire_rows(&source).err(), Some(literal));
    }

    // The shard half: each row's routing value is the one the literal lane computes
    // for the same row, whatever the key's type — so every row lands where a point
    // lookup of its key will look.
    #[test]
    fn rows_route_where_their_literals_route() {
        let source = batch(vec![
            ("i", Arc::new(Int64Array::from(vec![1, -2, 300])) as _),
            ("s", Arc::new(StringArray::from(vec!["a", "it's", ""])) as _),
            ("f", Arc::new(Float64Array::from(vec![1.0, 2.5, -0.0])) as _),
            (
                "ts",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![0, 1_546_329_840_000_000, 1])
                        .with_timezone("UTC"),
                ) as _,
            ),
            (
                "dec",
                Arc::new(
                    Decimal128Array::from(vec![100, 250, -5])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as _,
            ),
        ]);
        let template = insert_template("t", &["i", "s", "f", "ts", "dec"]).unwrap();
        let statement =
            insert_statements_from_batches(&template, &[source.clone()], ROWS_PER_STATEMENT)
                .unwrap()
                .remove(0);

        for (key_column, key) in ["i", "s", "f", "ts", "dec"].iter().enumerate() {
            let literal: Vec<String> = extract_insert_row_shard_keys(&statement, key, &[])
                .expect("every row is routable")
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            assert_eq!(
                bulk_routing_values(&source, key_column).unwrap(),
                Some(literal),
                "shard key {key}"
            );
        }
    }

    #[test]
    fn a_key_the_literal_lane_cannot_route_leaves_the_batch_to_it() {
        let source = batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![Some(1), None])) as _,
        )]);
        assert_eq!(bulk_routing_values(&source, 0).unwrap(), None);
    }

    fn decoded(payloads: &[BulkPayload]) -> Vec<RecordBatch> {
        payloads
            .iter()
            .flat_map(|p| {
                StreamReader::try_new(std::io::Cursor::new(p.arrow_ipc.to_vec()), None)
                    .unwrap()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn payloads_hold_every_row_in_order_within_the_limit() {
        let ids: Vec<i64> = (0..5_000).collect();
        let text: Vec<String> = ids.iter().map(|i| format!("{i:0>200}")).collect();
        let source = batch(vec![
            ("id", Arc::new(Int64Array::from(ids.clone())) as _),
            ("v", Arc::new(StringArray::from(text)) as _),
        ]);

        let max = 64 * 1024;
        let payloads = bulk_payloads(&source, max).unwrap();
        assert!(
            payloads.len() > 1,
            "a 1 MB batch must not fit one 64 KiB payload"
        );
        for payload in &payloads {
            assert!(
                payload.arrow_ipc.len() <= max,
                "payload of {} bytes",
                payload.arrow_ipc.len()
            );
        }
        assert_eq!(payloads.iter().map(|p| p.rows).sum::<usize>(), 5_000);

        let back =
            datafusion::arrow::compute::concat_batches(&source.schema(), &decoded(&payloads))
                .unwrap();
        assert_eq!(
            back, source,
            "the payloads must carry the rows unchanged and in order"
        );
    }

    #[test]
    fn a_row_larger_than_the_limit_goes_alone() {
        let source = batch(vec![(
            "v",
            Arc::new(StringArray::from(vec!["x".repeat(4096), "y".to_string()])) as _,
        )]);
        let payloads = bulk_payloads(&source, 1024).unwrap();
        assert_eq!(payloads.iter().map(|p| p.rows).collect::<Vec<_>>(), [1, 1]);
    }

    #[test]
    fn an_empty_batch_has_no_payload() {
        let source = batch(vec![(
            "id",
            Arc::new(Int64Array::from(Vec::<i64>::new())) as _,
        )]);
        assert!(bulk_payloads(&source, 1024).unwrap().is_empty());
    }
}
