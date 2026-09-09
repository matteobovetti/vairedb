//! Encodes DataFusion query results into pgwire wire-format rows. Bridges Arrow
//! arrays to PostgreSQL field types and per-column text/binary value encoding,
//! taking care to render values (notably temporal types) in a form that libpq
//! and JDBC clients accept.

use std::sync::Arc;

use arrow_pg::datatypes::arrow_schema_to_pg_fields;
use arrow_pg::encoder::encode_value;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrame;
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, QueryResponse, Response};
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::{
    ErrorContext, enrich_datafusion_error, make_vdb_error,
};

/// Map any result-encoding failure to a uniform internal error. Both the text
/// and binary cell paths plus the row finalizer funnel through here.
fn encode_error(e: impl std::fmt::Display) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InternalError,
        format!("failed to encode query result: {}", e),
    )
}

/// Collect a `DataFrame` and encode its rows as a pgwire query response,
/// honoring the client's requested per-column result format (`format`).
/// Shared by the simple-protocol (always text) and extended-protocol
/// (text or binary, per the Bind message) read paths. Result-column type
/// OIDs and value encoding both go through arrow-pg, the same mapping
/// `get_result_schema` uses for Describe, so Describe and Execute agree.
pub(super) async fn encode_dataframe_response(
    df: DataFrame,
    format: &Format,
    select_ctx: &ErrorContext,
) -> PgWireResult<Response> {
    let arrow_schema = df.schema().as_arrow().clone();
    let batches = df
        .collect()
        .await
        .map_err(|e| enrich_datafusion_error(&e, select_ctx))?;

    encode_batches_response(&arrow_schema, &batches, format)
}

/// The Arrow schema VaireDB puts on the wire, which is not always the plan's own.
///
/// Two kinds of column are not sent as themselves.
///
/// **`UInt64`.** PostgreSQL has no unsigned integers, and arrow-pg already widens most
/// Arrow ones to the signed type that holds them — `UInt8` to `int2`, `UInt16` to `int4`,
/// `UInt32` to `int8`. `UInt64` is the exception: it is advertised as `numeric`. That
/// single gap is what a client meets on `row_number()`, `rank()` and `dense_rank()`,
/// which DataFusion types `UInt64` where PostgreSQL promises `bigint` — so a driver
/// is handed OID 1700 and a conforming one fails on the type before it ever reads a
/// value.
///
/// **The list types that are not `List`.** arrow-pg encodes `List`; a `LargeList` panics
/// inside its encoder in either format and a `FixedSizeList` in binary. Both hold exactly
/// what a `List` holds — the same elements, the same nullability — so they are converted
/// rather than refused, and the conversion happens here, before any OID is derived from
/// the type.
///
/// A column is *converted*, not merely relabelled, because the label and the payload come
/// from different places: relabelling alone would leave the binary encoder writing a
/// `numeric` body under an `int8` header. Only top-level columns are converted; a `UInt64`
/// nested inside a list keeps arrow-pg's mapping, since its element OID is derived
/// separately.
pub(crate) fn wire_schema(schema: &Schema) -> Schema {
    if !schema
        .fields()
        .iter()
        .any(|f| wire_type(f.data_type()).is_some())
    {
        return schema.clone();
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match wire_type(f.data_type()) {
            Some(wire) => Field::new(f.name(), wire, f.is_nullable()),
            None => f.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// The type a column has to be sent as when it cannot be sent as its own, or `None` when
/// it can. See [`wire_schema`] for why each of these is here.
fn wire_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::UInt64 => Some(DataType::Int64),
        DataType::LargeList(element) | DataType::FixedSizeList(element, _) => {
            Some(DataType::List(Arc::clone(element)))
        }
        _ => None,
    }
}

/// Cast `batch` into the types [`wire_schema`] advertises.
///
/// The cast is **checked**: a `UInt64` above `i64::MAX` has no PostgreSQL
/// representation at all, and saying so is the honest answer where a safe cast would
/// hand the client a silent `NULL` in place of the number it stored.
fn coerce_batch_for_wire(batch: &RecordBatch, wire: &Schema) -> PgWireResult<RecordBatch> {
    use datafusion::arrow::compute::{CastOptions, cast_with_options};

    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = batch
        .columns()
        .iter()
        .zip(wire.fields())
        .map(|(col, field)| {
            if col.data_type() == field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast_with_options(col.as_ref(), field.data_type(), &options).map_err(|e| {
                    make_vdb_error(
                        VdbErrorCode::NumericValueOutOfRange,
                        format!(
                            "column \"{}\" holds a value PostgreSQL's {} cannot represent: {}",
                            field.name(),
                            field.data_type(),
                            e
                        ),
                    )
                })
            }
        })
        .collect::<PgWireResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(wire.clone()), columns).map_err(encode_error)
}

/// Encode already-collected batches as a pgwire query response.
///
/// The half of [`encode_dataframe_response`] that touches the wire, split out for
/// the callers that have rows but no `DataFrame`: a statement the coordinator
/// answers itself rather than by executing a plan on a context — see
/// [`crate::pgwire_handler::introspection`], which builds its own single-column
/// batch. Sharing the encoder is what keeps those rows typed and rendered exactly
/// like a SELECT's.
///
/// `schema` is the batches' own Arrow schema and supplies both the column labels
/// and, through arrow-pg, their type OIDs.
pub(super) fn encode_batches_response(
    schema: &datafusion::arrow::datatypes::Schema,
    batches: &[RecordBatch],
    format: &Format,
) -> PgWireResult<Response> {
    let wire = wire_schema(schema);
    let arrow_schema = &wire;
    let field_info = Arc::new(arrow_schema_to_pg_fields(arrow_schema, format, None)?);

    let mut rows = Vec::new();
    for batch in batches {
        let batch = &coerce_batch_for_wire(batch, arrow_schema)?;
        for row_idx in 0..batch.num_rows() {
            let mut encoder = DataRowEncoder::new(Arc::clone(&field_info));
            for (col_idx, field) in field_info.iter().enumerate() {
                let col = batch.column(col_idx);
                let is_list = matches!(
                    col.data_type(),
                    datafusion::arrow::datatypes::DataType::List(_)
                        | datafusion::arrow::datatypes::DataType::LargeList(_)
                );
                if field.format() == FieldFormat::Text && !is_list {
                    // Postgres trims trailing fractional zeros on temporal
                    // values (e.g. `00:00:00`, not `00:00:00.000000`), but
                    // arrow-pg's encoder always emits `%.6f`. Render text
                    // cells ourselves to stay wire-faithful to libpq/JDBC.
                    let result = if is_null_on_the_wire(col.as_ref(), row_idx) {
                        encoder.encode_field(&None::<&str>)
                    } else {
                        let val = wire_text_value(col.as_ref(), row_idx);
                        encoder.encode_field(&val)
                    };
                    result.map_err(encode_error)?;
                } else {
                    // Binary cells, and array cells in either format, go through
                    // arrow-pg: it owns the correct binary codec and renders text
                    // arrays as PostgreSQL array literals (`{1,2,3}`). Hand-rolling
                    // the text here would re-quote the braces into `"{1,2,3}"`.
                    encode_value(
                        &mut encoder,
                        col,
                        row_idx,
                        arrow_schema.field(col_idx),
                        field,
                    )
                    .map_err(encode_error)?;
                }
            }
            #[allow(deprecated)]
            let row = encoder.finish().map_err(encode_error)?;
            rows.push(Ok(row));
        }
    }

    let row_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(field_info, row_stream)))
}

/// Whether the cell at `row` of `col` is NULL as far as a client is concerned.
///
/// Not `Array::is_null`, which reports the *physical* null buffer and so answers
/// `false` for every array type that has no buffer to consult — including
/// `NullArray`, which is all nulls and nothing else. `SELECT NULL` plans to exactly
/// that array, so asking `is_null` sent the client an empty string where PostgreSQL
/// sends a NULL: a value where there was none, and the wrong one. `logical_nulls`
/// is the question actually being asked, and it also covers the dictionary and
/// run-end encodings whose nulls live one level down.
pub(crate) fn is_null_on_the_wire(col: &dyn datafusion::arrow::array::Array, row: usize) -> bool {
    col.logical_nulls()
        .map(|nulls| nulls.is_null(row))
        .unwrap_or(false)
}

/// Render the cell at `row` of `col` the way PostgreSQL's **text wire format** spells
/// it, which is not always the way a SQL literal of the same value is spelled.
///
/// Two types differ that way.
///
/// `boolean`: PostgreSQL sends `t` and `f` on the wire, and a client reading a text-format
/// boolean — every column of a simple query, and any extended-protocol column a driver did
/// not ask for in binary — compares against those. A literal, meanwhile, has to say `true`
/// or `false`, because `t` is an identifier to a SQL parser.
///
/// `interval`: PostgreSQL prints `1 day`, and a client that renders an interval back to a
/// user shows whatever it was sent. `1 day` is not a form the shards' engine parses in
/// every position, so the literal stays in explicit units.
///
/// Everything else keeps the one renderer, [`arrow_array_value_to_string`], which the write
/// path shares.
fn wire_text_value(col: &dyn datafusion::arrow::array::Array, row: usize) -> String {
    use datafusion::arrow::array::BooleanArray;

    match col.data_type() {
        DataType::Boolean => {
            let values = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("a Boolean column is a BooleanArray");
            if values.value(row) { "t" } else { "f" }.to_string()
        }
        DataType::Interval(_) => {
            let (months, days, nanos) = interval_parts(col, row);
            postgres_interval_text(months, days, nanos)
        }
        _ => arrow_array_value_to_string(col, row),
    }
}

/// Render an Arrow cell to its PostgreSQL text representation. arrow-pg's own
/// encoder is used for binary result columns, but its text path always emits
/// `%.6f` fractional seconds, whereas Postgres trims trailing zeros; this keeps
/// the text wire form faithful for libpq/JDBC clients.
///
/// The write path renders cells with this too ([`crate::write_sql_cl`]'s row
/// materialization), deliberately sharing the one renderer: a value re-emitted as
/// a SQL literal then spells itself the same way a client reading it back sees it,
/// which is what makes `INSERT ... SELECT` route a row to the shard a literal of
/// the same value would.
///
/// `boolean` is the exception the sharing cannot cover, since PostgreSQL's wire text
/// (`t`) is not a SQL literal (`true`) — [`wire_text_value`] owns that one.
pub(crate) fn arrow_array_value_to_string(
    col: &dyn datafusion::arrow::array::Array,
    row: usize,
) -> String {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType as ArrowDT;

    /// Downcast `col` to the given Arrow array type and stringify the cell at
    /// `row`. The downcast is infallible here: the arm is selected by matching
    /// `col.data_type()`, so the concrete array type is guaranteed.
    macro_rules! cell_to_string {
        ($ty:ty) => {
            col.as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .value(row)
                .to_string()
        };
    }

    match col.data_type() {
        ArrowDT::Boolean => cell_to_string!(BooleanArray),
        ArrowDT::Int8 => cell_to_string!(Int8Array),
        ArrowDT::Int16 => cell_to_string!(Int16Array),
        ArrowDT::Int32 => cell_to_string!(Int32Array),
        ArrowDT::Int64 => cell_to_string!(Int64Array),
        ArrowDT::UInt8 => cell_to_string!(UInt8Array),
        ArrowDT::UInt16 => cell_to_string!(UInt16Array),
        ArrowDT::UInt32 => cell_to_string!(UInt32Array),
        ArrowDT::UInt64 => cell_to_string!(UInt64Array),
        ArrowDT::Float32 => cell_to_string!(Float32Array),
        ArrowDT::Float64 => cell_to_string!(Float64Array),
        ArrowDT::Utf8 => cell_to_string!(StringArray),
        ArrowDT::LargeUtf8 => cell_to_string!(LargeStringArray),
        // An interval is spelled in explicit units here, not in PostgreSQL's own form:
        // this is the literal renderer, and `1 year 2 mons` is not something the shards'
        // engine parses back. [`wire_text_value`] spells the same value PostgreSQL's way
        // for the client. The parts are the interval's own — nothing is normalized,
        // because a month is not a fixed number of days in either engine.
        ArrowDT::Interval(_) => {
            let (months, days, nanos) = interval_parts(col, row);
            interval_sql_literal(months, days, nanos)
        }
        _ => {
            // PostgreSQL's text format separates date and time with a space, whereas Arrow's
            // default formatter uses an ISO-8601 `T`. JDBC/libpq clients reject the `T` form
            // when parsing TIMESTAMP values, so override the timestamp formats accordingly.
            let format_options = datafusion::arrow::util::display::FormatOptions::default()
                .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
                .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"));
            let formatter =
                datafusion::arrow::util::display::ArrayFormatter::try_new(col, &format_options);
            let rendered = match formatter {
                Ok(f) => f.value(row).to_string(),
                Err(_) => "?".to_string(),
            };
            if matches!(col.data_type(), ArrowDT::Timestamp(_, Some(_))) {
                return trim_whole_hour_offset(rendered);
            }
            rendered
        }
    }
}

/// The `(months, days, nanoseconds)` an interval cell carries, whichever of Arrow's three
/// interval layouts it is stored in.
///
/// The three are not interchangeable — `YearMonth` has no day or time part, `DayTime` has
/// no month part — so each is read as itself and widened to the union, which is what
/// `MonthDayNano` already is. A non-interval column cannot reach here: every caller
/// matches on `Interval(_)` first.
fn interval_parts(col: &dyn datafusion::arrow::array::Array, row: usize) -> (i32, i32, i64) {
    use datafusion::arrow::array::{
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    };
    use datafusion::arrow::datatypes::IntervalUnit;

    /// Nanoseconds in a millisecond, the unit a `DayTime` interval's time part is in.
    const NANOS_PER_MILLI: i64 = 1_000_000;

    match col.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            let months = col
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .expect("a YearMonth interval column is an IntervalYearMonthArray")
                .value(row);
            (months, 0, 0)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let value = col
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .expect("a DayTime interval column is an IntervalDayTimeArray")
                .value(row);
            (0, value.days, value.milliseconds as i64 * NANOS_PER_MILLI)
        }
        _ => {
            let value = col
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .expect("a MonthDayNano interval column is an IntervalMonthDayNanoArray")
                .value(row);
            (value.months, value.days, value.nanoseconds)
        }
    }
}

/// An interval as a SQL literal, in units both PostgreSQL and DuckDB spell the same way.
///
/// Shared with the bind-parameter path ([`crate::write_router::write_params`]), which needs
/// exactly this: a string the shards' engine casts back to the same interval. All three
/// components are always written, including zeroes, because an empty string is not a
/// literal. Sub-day time is rendered in microseconds, DuckDB's interval resolution — a
/// nanosecond remainder cannot be stored and is dropped here rather than silently inside
/// the engine.
pub(crate) fn interval_sql_literal(months: i32, days: i32, nanos: i64) -> String {
    let micros = nanos / 1_000;
    format!("{months} months {days} days {micros} microseconds")
}

/// An interval the way PostgreSQL's default `IntervalStyle` prints one: `1 day`,
/// `1 year 2 mons`, `-04:05:06`, `1 mon -1 days +01:00:00`.
///
/// Nothing is normalized across units — PostgreSQL does not either, because a month is not
/// a fixed number of days and 30 days is not a month. Three details of its spelling are
/// easy to get subtly wrong and are copied deliberately from `EncodeInterval`: a field is
/// omitted when zero; it is pluralized on `value != 1`, so `-1` prints as `-1 days`; and a
/// positive field that *follows* a negative one is written with an explicit `+`, which is
/// what makes `1 mon -1 days +01:00:00` unambiguous. The all-zero interval prints as
/// `00:00:00`, since something has to be printed.
fn postgres_interval_text(months: i32, days: i32, nanos: i64) -> String {
    /// One `N unit` field, appended only if it carries anything.
    fn append_part(parts: &mut Vec<String>, after_negative: &mut bool, value: i32, unit: &str) {
        if value == 0 {
            return;
        }
        let sign = if *after_negative && value > 0 {
            "+"
        } else {
            ""
        };
        let plural = if value != 1 { "s" } else { "" };
        parts.push(format!("{sign}{value} {unit}{plural}"));
        *after_negative = value < 0;
    }

    let mut parts: Vec<String> = Vec::new();
    let mut after_negative = false;
    append_part(&mut parts, &mut after_negative, months / 12, "year");
    append_part(&mut parts, &mut after_negative, months % 12, "mon");
    append_part(&mut parts, &mut after_negative, days, "day");

    if nanos != 0 {
        let magnitude = nanos.unsigned_abs();
        let seconds = magnitude / 1_000_000_000;
        let sign = if nanos < 0 {
            "-"
        } else if after_negative {
            "+"
        } else {
            ""
        };
        let mut time = format!(
            "{sign}{:02}:{:02}:{:02}",
            seconds / 3600,
            (seconds % 3600) / 60,
            seconds % 60
        );
        // Microseconds, with trailing zeros trimmed the way PostgreSQL trims them.
        let micros = (magnitude % 1_000_000_000) / 1_000;
        if micros != 0 {
            time.push('.');
            time.push_str(format!("{micros:06}").trim_end_matches('0'));
        }
        parts.push(time);
    } else if parts.is_empty() {
        parts.push("00:00:00".to_string());
    }
    parts.join(" ")
}

/// Drop the minutes from a zone offset that has none, the way PostgreSQL prints one.
///
/// PostgreSQL renders a `timestamptz` as `2026-06-03 12:30:00+00`, and `+05:30` only
/// where the offset really has minutes; chrono's `%:z`, which is the closest format
/// Arrow can be given, always writes them. Both spellings parse everywhere, so this is
/// about what a client *reading* the column sees — a text comparison against a value
/// PostgreSQL produced, or a log line a person reads.
fn trim_whole_hour_offset(rendered: String) -> String {
    let Some(head) = rendered.strip_suffix(":00") else {
        return rendered;
    };
    // The offset's `±HH` is the last three characters of what remains. A timestamp's own
    // `:00` seconds are preceded by digits, never by a sign, so this cannot eat them.
    let ends_with_whole_hour_offset = head.len().checked_sub(3).is_some_and(|sign_at| {
        head.is_char_boundary(sign_at) && head[sign_at..].starts_with(['+', '-'])
    });
    if ends_with_whole_hour_offset {
        head.to_string()
    } else {
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_pg::datatypes::into_pg_type;
    use datafusion::arrow::array::{Int64Array, UInt64Array};
    use pgwire::api::Type;

    /// The premise of [`wire_schema`]: arrow-pg maps `UInt64` to `numeric`, which is
    /// not the type PostgreSQL gives a row number. If this ever fails because
    /// upstream changed the mapping, the widening below is no longer needed.
    #[test]
    fn arrow_pg_still_advertises_uint64_as_numeric() {
        assert_eq!(into_pg_type(&DataType::UInt64).unwrap(), Type::NUMERIC);
        // The other unsigned widths are already widened to a signed PostgreSQL type
        // by arrow-pg itself, which is why only `UInt64` is touched here.
        assert_eq!(into_pg_type(&DataType::UInt8).unwrap(), Type::INT2);
        assert_eq!(into_pg_type(&DataType::UInt16).unwrap(), Type::INT4);
        assert_eq!(into_pg_type(&DataType::UInt32).unwrap(), Type::INT8);
    }

    #[test]
    fn widens_only_top_level_uint64_columns() {
        let inner = Arc::new(Field::new("item", DataType::UInt64, true));
        let schema = Schema::new(vec![
            Field::new("rn", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ids", DataType::List(inner), true),
        ]);

        let wire = wire_schema(&schema);

        assert_eq!(wire.field(0).data_type(), &DataType::Int64);
        assert!(
            !wire.field(0).is_nullable(),
            "widening a column must not make it nullable: a driver reading the \
             NoData/DataRow pair would disagree with the header"
        );
        assert_eq!(wire.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            wire.field(2).data_type(),
            schema.field(2).data_type(),
            "a UInt64 inside a list keeps arrow-pg's own element mapping"
        );
    }

    #[test]
    fn leaves_a_schema_without_uint64_identical() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        assert_eq!(wire_schema(&schema), schema);
    }

    /// arrow-pg's encoder writes a `List`; the two other list layouts hold the same
    /// elements but panic inside it rather than returning an error, which takes the
    /// connection with them. They are converted to `List` before an OID is derived.
    #[test]
    fn sends_the_other_list_layouts_as_a_plain_list() {
        let element = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Schema::new(vec![
            Field::new("large", DataType::LargeList(Arc::clone(&element)), true),
            Field::new(
                "fixed",
                DataType::FixedSizeList(Arc::clone(&element), 3),
                true,
            ),
            Field::new("plain", DataType::List(Arc::clone(&element)), true),
        ]);

        let wire = wire_schema(&schema);

        let expected = DataType::List(element);
        assert_eq!(wire.field(0).data_type(), &expected);
        assert_eq!(wire.field(1).data_type(), &expected);
        assert_eq!(wire.field(2).data_type(), &expected);
    }

    /// And the values follow the label: a fixed-size list is rebuilt as a variable one,
    /// with its elements intact.
    #[test]
    fn coerces_a_fixed_size_list_to_the_list_it_advertises() {
        use datafusion::arrow::array::{Array, FixedSizeListArray, Int32Array, ListArray};

        let element = Arc::new(Field::new("item", DataType::Int32, true));
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let column =
            FixedSizeListArray::try_new(Arc::clone(&element), 2, Arc::new(values), None).unwrap();
        let schema = Schema::new(vec![Field::new("pair", column.data_type().clone(), true)]);
        let wire = wire_schema(&schema);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(column)]).unwrap();

        let coerced = coerce_batch_for_wire(&batch, &wire).unwrap();

        let lists = coerced
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("the column is a List once coerced");
        assert_eq!(lists.len(), 2);
        assert_eq!(lists.value_length(0), 2);
    }

    #[test]
    fn coerces_a_uint64_column_to_the_type_it_advertises() {
        let schema = Schema::new(vec![Field::new("rn", DataType::UInt64, false)]);
        let wire = wire_schema(&schema);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![1, 2, i64::MAX as u64]))],
        )
        .unwrap();

        let coerced = coerce_batch_for_wire(&batch, &wire).unwrap();

        let values = coerced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("the column is Int64 once coerced");
        assert_eq!(values.values(), &[1, 2, i64::MAX]);
    }

    // PostgreSQL's text format for `boolean` is `t`/`f`, and a client comparing against
    // those reads `true` as neither. The SQL literal form has to stay `true`/`false`,
    // which is why the two renderers are separate rather than one.
    #[test]
    fn renders_a_boolean_the_way_the_text_wire_format_does() {
        use datafusion::arrow::array::BooleanArray;

        let col = BooleanArray::from(vec![true, false]);

        assert_eq!(wire_text_value(&col, 0), "t");
        assert_eq!(wire_text_value(&col, 1), "f");
        assert_eq!(arrow_array_value_to_string(&col, 0), "true");
        assert_eq!(arrow_array_value_to_string(&col, 1), "false");
    }

    // Every other type is rendered by the one renderer, so the wire and literal forms
    // cannot drift apart for it.
    #[test]
    fn defers_every_other_type_to_the_shared_renderer() {
        let col = Int64Array::from(vec![42]);
        assert_eq!(
            wire_text_value(&col, 0),
            arrow_array_value_to_string(&col, 0)
        );
    }

    // The honest half: a value above `i64::MAX` has no `bigint` to be reported as, and
    // a safe cast would hand the client a NULL where it stored a number.
    #[test]
    fn refuses_a_uint64_value_bigint_cannot_hold() {
        let schema = Schema::new(vec![Field::new("n", DataType::UInt64, false)]);
        let wire = wire_schema(&schema);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![u64::MAX]))],
        )
        .unwrap();

        match coerce_batch_for_wire(&batch, &wire).expect_err("u64::MAX does not fit in bigint") {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "22003", "numeric_value_out_of_range");
                assert!(
                    info.message.contains("\"n\""),
                    "the message names the column: {}",
                    info.message
                );
            }
            other => panic!("expected a client-facing error, got {other}"),
        }
    }

    // `SELECT NULL` plans to a `NullArray`, which holds no null buffer because it holds
    // nothing else either. `Array::is_null` reads that buffer, so it answered `false`
    // for a value that is nothing but null, and the client was sent an empty string.
    #[test]
    fn sees_the_null_in_an_array_that_is_only_nulls() {
        use datafusion::arrow::array::{Array, NullArray};

        let col = NullArray::new(1);

        assert!(
            !col.is_null(0),
            "the premise: the physical answer is `false`, which is why it is not the \
             one asked"
        );
        assert!(is_null_on_the_wire(&col, 0));
    }

    // PostgreSQL prints a whole-hour zone offset as `+00`, and `%:z` — the closest format
    // Arrow takes — always writes the minutes.
    #[test]
    fn renders_a_zoned_timestamp_the_way_postgresql_prints_one() {
        use datafusion::arrow::array::TimestampMicrosecondArray;

        // 2026-06-07 12:30:00 UTC.
        let micros = 1_780_835_400_000_000;
        let utc = TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC");
        assert_eq!(
            arrow_array_value_to_string(&utc, 0),
            "2026-06-07 12:30:00+00"
        );

        // An offset that really has minutes keeps them.
        let kolkata = TimestampMicrosecondArray::from(vec![micros]).with_timezone("+05:30");
        assert_eq!(
            arrow_array_value_to_string(&kolkata, 0),
            "2026-06-07 18:00:00+05:30"
        );

        // And a timestamp without a zone is untouched, seconds included.
        let naive = TimestampMicrosecondArray::from(vec![micros]);
        assert_eq!(
            arrow_array_value_to_string(&naive, 0),
            "2026-06-07 12:30:00"
        );
    }

    // PostgreSQL's own interval spelling, which is what a client displays to a person.
    #[test]
    fn renders_an_interval_the_way_postgresql_prints_one() {
        const DAY: i64 = 0;
        for (months, days, nanos, expected) in [
            (0, 1, DAY, "1 day"),
            (0, 2, 0, "2 days"),
            (1, 0, 0, "1 mon"),
            (14, 0, 0, "1 year 2 mons"),
            // Pluralized on `value != 1`, so a negative one is plural.
            (-1, 0, 0, "-1 mons"),
            // The time part is always `HH:MM:SS`, with its own sign.
            (0, 0, 14_706_000_000_000, "04:05:06"),
            (0, 0, -14_706_000_000_000, "-04:05:06"),
            // A positive field after a negative one is written with an explicit `+`.
            (1, -1, 3_600_000_000_000, "1 mon -1 days +01:00:00"),
            // Fractional seconds keep only the digits they need.
            (0, 0, 500_000_000, "00:00:00.5"),
            // Something has to be printed, even for the identity interval.
            (0, 0, 0, "00:00:00"),
        ] {
            assert_eq!(
                postgres_interval_text(months, days, nanos),
                expected,
                "({months}, {days}, {nanos})"
            );
        }
    }

    // The literal form is a different string on purpose: the shards' engine parses it, and
    // `1 year 2 mons` is not something it parses.
    #[test]
    fn renders_an_interval_cell_as_a_literal_the_shards_parse() {
        use datafusion::arrow::array::IntervalMonthDayNanoArray;
        use datafusion::arrow::datatypes::IntervalMonthDayNano;

        let col = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(14, 1, 3_000)]);

        assert_eq!(
            arrow_array_value_to_string(&col, 0),
            "14 months 1 days 3 microseconds"
        );
        assert_eq!(
            wire_text_value(&col, 0),
            "1 year 2 mons 1 day 00:00:00.000003"
        );
    }

    // The ordinary case still has to work, in both directions.
    #[test]
    fn reads_the_nulls_of_an_ordinary_column() {
        let col = Int64Array::from(vec![Some(1), None]);

        assert!(!is_null_on_the_wire(&col, 0));
        assert!(is_null_on_the_wire(&col, 1));
    }
}
