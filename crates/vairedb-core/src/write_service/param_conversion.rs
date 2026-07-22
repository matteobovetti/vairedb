use duckdb::types::Value;

use vairedb_common::proto::vairedb::v1::{WriteParam, write_param};

/// Convert a protobuf `WriteParam` into a DuckDB bind value. An unset oneof or
/// the `is_null` variant maps to SQL NULL; numeric/date/timestamp values arrive
/// as strings and are cast by DuckDB on bind.
pub(crate) fn write_param_to_duckdb_value(param: &WriteParam) -> Value {
    match &param.value {
        None => Value::Null,
        Some(write_param::Value::IsNull(_)) => Value::Null,
        Some(write_param::Value::BoolVal(b)) => Value::Boolean(*b),
        Some(write_param::Value::IntVal(i)) => Value::BigInt(*i),
        Some(write_param::Value::DoubleVal(d)) => Value::Double(*d),
        Some(write_param::Value::StringVal(s)) => Value::Text(s.clone()),
        Some(write_param::Value::BytesVal(b)) => Value::Blob(b.clone()),
    }
}
