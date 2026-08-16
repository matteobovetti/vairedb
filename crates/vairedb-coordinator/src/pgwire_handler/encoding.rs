//! Encodes DataFusion query results into pgwire wire-format rows. Bridges Arrow
//! arrays to PostgreSQL field types and per-column text/binary value encoding,
//! taking care to render values (notably temporal types) in a form that libpq
//! and JDBC clients accept.

use std::sync::Arc;

use arrow_pg::datatypes::arrow_schema_to_pg_fields;
use arrow_pg::encoder::encode_value;
use datafusion::dataframe::DataFrame;
use futures::stream;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, QueryResponse, Response};
use pgwire::error::{PgWireError, PgWireResult};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::{ErrorContext, enrich_generic_error, make_vdb_error};

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
    let field_info = Arc::new(arrow_schema_to_pg_fields(&arrow_schema, format, None)?);

    let batches = df
        .collect()
        .await
        .map_err(|e| enrich_generic_error(&e, select_ctx))?;

    let mut rows = Vec::new();
    for batch in &batches {
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
                    let result = if col.is_null(row_idx) {
                        encoder.encode_field(&None::<&str>)
                    } else {
                        let val = arrow_array_value_to_string(col.as_ref(), row_idx);
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

/// Render an Arrow cell to its PostgreSQL text representation. arrow-pg's own
/// encoder is used for binary result columns, but its text path always emits
/// `%.6f` fractional seconds, whereas Postgres trims trailing zeros; this keeps
/// the text wire form faithful for libpq/JDBC clients.
fn arrow_array_value_to_string(col: &dyn datafusion::arrow::array::Array, row: usize) -> String {
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
        _ => {
            // PostgreSQL's text format separates date and time with a space, whereas Arrow's
            // default formatter uses an ISO-8601 `T`. JDBC/libpq clients reject the `T` form
            // when parsing TIMESTAMP values, so override the timestamp formats accordingly.
            let format_options = datafusion::arrow::util::display::FormatOptions::default()
                .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
                .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"));
            let formatter =
                datafusion::arrow::util::display::ArrayFormatter::try_new(col, &format_options);
            match formatter {
                Ok(f) => f.value(row).to_string(),
                Err(_) => "?".to_string(),
            }
        }
    }
}
