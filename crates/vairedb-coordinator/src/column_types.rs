//! Map a declared column type to the Arrow type the coordinator advertises for it.
//!
//! This is the read path's counterpart to the write path's
//! [`transform_data_type`](crate::write_sql_cl): the catalog stores the type *as the client
//! declared it*, and every read has to turn that string back into an Arrow type. That type
//! is not a detail — it is the schema DataFusion plans against, the schema
//! `coerce_batch_to_schema` rebuilds shard batches into, and the schema
//! `arrow_pg::into_pg_type` turns into the OIDs a `RowDescription` carries. A type that is
//! merely *close* is therefore not close at all:
//!
//! - **Too narrow, and values disappear.** The rebuild is a *safe* cast, so anything that
//!   does not fit becomes `NULL` rather than an error.
//! - **`Utf8` when the column is not text, and typed drivers break.** JDBC's `getInt`, a
//!   Rust client's `try_get::<i32>`, `psql`'s right-alignment and every comparison the
//!   planner pushes down are all decided by the advertised type, not by the value.
//!
//! So the mapping aims at DuckDB's *actual* Arrow return type for each declared type,
//! which makes the rebuild take its "fields already match" fast path and leaves nothing to
//! cast. Microseconds for `TIMESTAMP` and `TIME` are that choice, not DataFusion's
//! SQL-parser default of nanoseconds.
//!
//! ## Declared parameters are part of the type
//!
//! `DECIMAL(10,2)` and `DECIMAL(38,0)` are different Arrow types, and reading both as
//! `Decimal128(38,10)` — as this function once did — is wrong twice over: `1.5` renders as
//! `1.5000000000`, and a 38-digit value overflows the rescale and comes back `NULL` (on a
//! `NOT NULL` column, the rebuild then fails the read outright). The declared parentheses
//! are parsed for exactly that reason. Length parameters that Arrow does not model —
//! `VARCHAR(64)`, `TIMESTAMP(6)` — are stripped and ignored, which is what PostgreSQL does
//! to them on the wire too.
//!
//! ## What is deliberately left as text
//!
//! `_ => Utf8` remains the fallback, and after DDL-time restriction of the types that
//! cannot be served it is reached only by types whose *values* are faithful as text:
//! `UUID`, `CHAR`/`BPCHAR`, `JSON`, `ENUM`, `STRUCT`. Their OIDs are cosmetically wrong
//! (`text` rather than `uuid`/`json`), which is a smaller defect than the alternative and
//! is tracked separately. `ENUM` is not mapped to `Dictionary(UInt8, Utf8)` even though
//! arrow-pg accepts it: the dictionary would still sort by decoded string, so the one
//! behavior that would justify the change — PostgreSQL's ordering by declaration order —
//! would not follow.
//!
//! ## And what is refused outright
//!
//! [`unserviceable_type_reason`] is the other half of the same table. A handful of types
//! *cannot* be read back correctly no matter what this module advertises, because the loss
//! happens below it — inside DuckDB's own Arrow bridge, or in a cast the rebuild has no
//! choice about. Those are refused at `CREATE TABLE`, which is the difference between a
//! statement that fails and a table that accepts rows and then cannot return them.

use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Field, IntervalUnit, TimeUnit,
};

/// DuckDB's `DECIMAL` when it is declared with no parentheses at all. PostgreSQL's bare
/// `NUMERIC` is unconstrained, but the shard is what actually stores the value, so its
/// default is the truth about what comes back.
const DEFAULT_DECIMAL: (u8, i8) = (18, 3);

/// Map a SQL/DuckDB column type name to its Arrow [`DataType`].
///
/// Matching is case-insensitive and ignores declared lengths Arrow does not model.
/// Unrecognized types fall back to `Utf8` — see the module docs for why that is still the
/// right fallback.
///
/// A trailing `[]` (optionally sized, e.g. `INTEGER[3]`) marks a PostgreSQL/DuckDB array
/// column; it is modeled as an Arrow `List` of the element type (recursing for nested
/// arrays like `INTEGER[][]`), so the advertised schema — and thus the pgwire array type
/// OID — reflects the column's real shape rather than `Utf8`.
pub fn parse_data_type(type_str: &str) -> DataType {
    let upper = type_str.trim().to_uppercase();

    if let Some(element) = array_element(&upper) {
        let element = parse_data_type(element);
        return DataType::List(Arc::new(Field::new("item", element, true)));
    }

    let (base, parameters) = split_parameters(&upper);

    match base.as_str() {
        "INTEGER" | "INT" | "INT4" | "SIGNED" => DataType::Int32,
        "BIGINT" | "INT8" | "LONG" => DataType::Int64,
        "SMALLINT" | "INT2" | "SHORT" => DataType::Int16,
        "TINYINT" | "INT1" => DataType::Int8,
        "UTINYINT" => DataType::UInt8,
        "USMALLINT" => DataType::UInt16,
        "UINTEGER" => DataType::UInt32,
        // A decimal, not `UInt64`, even though `UInt64` is what the shard returns. No
        // PostgreSQL integer type is unsigned, so a value above `i64::MAX` has only
        // `numeric` to go to — and a top-level `UInt64` column is widened to `Int64` on the
        // wire (see `encoding::wire_schema`, which does that so `row_number()` is advertised
        // as `bigint` the way PostgreSQL promises), which would refuse exactly those values.
        // `DECIMAL(20,0)` holds every `u64`, is advertised as `numeric`, renders as digits,
        // and compares numerically. The cost is one cast per batch on such a column.
        "UBIGINT" => DataType::Decimal128(20, 0),
        "BOOLEAN" | "BOOL" | "LOGICAL" => DataType::Boolean,
        "FLOAT" | "REAL" | "FLOAT4" => DataType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
        "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
        // `BINARY`/`VARBINARY` are DuckDB aliases of `BLOB`. Missing them was silent data
        // loss, not a cosmetic OID: the `Binary` -> `Utf8` rebuild is a safe cast, so
        // every value that was not valid UTF-8 read back as `NULL`.
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP WITHOUT TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        // The zone is the point of the type. DuckDB hands back microseconds labelled
        // `UTC`, and the instant is stored in UTC whatever the session zone renders it as.
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "TIMESTAMP_S" => DataType::Timestamp(TimeUnit::Second, None),
        "TIMESTAMP_MS" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "TIMESTAMP_NS" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "DATE" => DataType::Date32,
        "TIME" | "TIME WITHOUT TIME ZONE" => DataType::Time64(TimeUnit::Microsecond),
        // Months, days and nanoseconds separately: an interval is not a duration, because
        // a month is not a fixed number of days and a day is not always 24 hours.
        "INTERVAL" => DataType::Interval(IntervalUnit::MonthDayNano),
        "JSON" | "JSONB" => DataType::Utf8,
        "DECIMAL" | "NUMERIC" => decimal_type(parameters),
        _ => DataType::Utf8,
    }
}

/// Whether a column declared as `type_str` is *really* text, rather than a type that only
/// [`parse_data_type`] renders as text.
///
/// The distinction changes nothing a client is told — both answer `text` — but it decides
/// whether a predicate on the column may be handed to a shard. DuckDB re-parses a pushed
/// predicate against the type *it* stored, and a literal it cannot convert to that type is a
/// query **error** rather than an empty result: `u = 'notauuid'` on a `UUID` column fails
/// with `Could not convert string 'notauuid' to INT128`. See
/// [`OpaqueTextColumns`](crate::scheduler::OpaqueTextColumns).
///
/// `CHAR`/`BPCHAR` count as text: DuckDB stores both as `VARCHAR`, so a comparison against
/// one behaves identically in the two engines. `JSON` does not count, even though it is
/// mapped by an explicit arm rather than by the fallback, because DuckDB parses the literal
/// it is compared against as JSON.
pub fn is_declared_text(type_str: &str) -> bool {
    let upper = type_str.trim().to_uppercase();

    // An array of text is a list, not text.
    if array_element(&upper).is_some() {
        return false;
    }

    let (base, _) = split_parameters(&upper);
    matches!(
        base.as_str(),
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" | "CHARACTER" | "CHARACTER VARYING"
    )
}

/// Why a column declared as `type_str` cannot be served, or `None` if it can be.
///
/// The reason is client-facing and names a way to store the value instead, because every
/// type here has one. What it deliberately does not do is let the `CREATE TABLE` succeed:
/// each of these loses data *below* the coordinator — inside DuckDB's Arrow bridge, or in
/// the safe cast the schema rebuild performs — so the loss cannot be repaired by
/// advertising a different type, and the only place left to report it is the DDL. Today,
/// without this, the table is created, the `INSERT`s are accepted, and the first `SELECT`
/// is what fails: after the data was written, and with an error about the read.
///
/// An array is refused for the same reason its element is.
pub fn unserviceable_type_reason(type_str: &str) -> Option<&'static str> {
    let upper = type_str.trim().to_uppercase();

    if let Some(element) = array_element(&upper) {
        return unserviceable_type_reason(element);
    }

    let (base, _) = split_parameters(&upper);

    match base.as_str() {
        "HUGEINT" | "INT128" => Some(
            "the shards' engine narrows a 128-bit integer to a 38-digit decimal before the \
             coordinator ever sees it, so the widest values would read back silently \
             truncated. Use BIGINT, or DECIMAL(38,0) when 38 digits are enough",
        ),
        "UHUGEINT" | "UINT128" => Some(
            "the shards' engine narrows a 128-bit integer to a 38-digit decimal before the \
             coordinator ever sees it, so the widest values would read back silently wrong — \
             the unsigned maximum comes back as -1. Use UBIGINT, or DECIMAL(38,0) when 38 \
             digits are enough",
        ),
        "BIT" | "BITSTRING" | "VARBIT" | "BIT VARYING" => Some(
            "the shards return a bit string as raw bytes that are not valid text, and the \
             conversion turns every one of them into NULL. Use BOOLEAN for a single flag, or \
             VARCHAR to keep the '0'/'1' spelling",
        ),
        "BIGNUM" | "VARINT" => Some(
            "the shards return an arbitrary-precision number as raw bytes that are not valid \
             text, and the conversion turns every one of them into NULL. Use DECIMAL(38,s), \
             which is the widest exact number the shards can store",
        ),
        "UNION" => Some(
            "a tagged union has no PostgreSQL wire type, so there is nothing to describe the \
             column as to a client. Use one nullable column per member, or JSON",
        ),
        "VARIANT" => Some(
            "the shards' storage format cannot hold a VARIANT column, and their driver cannot \
             decode one. Use JSON",
        ),
        "MAP" => Some(
            "a map has no PostgreSQL wire type, so there is nothing to describe the column as \
             to a client. Use JSON, or a pair of array columns",
        ),
        "TIMETZ" | "TIME WITH TIME ZONE" => Some(
            "the shards' engine drops the UTC offset when it returns a time with time zone, so \
             the value would come back as a different time than the one stored. Use \
             TIMESTAMPTZ, which keeps its offset, or TIME with the offset in its own column",
        ),
        _ => None,
    }
}

/// The element type of an array declaration, or `None` if `upper` is not one.
///
/// Only a length or nothing may sit between the brackets, so a `[` belonging to a nested
/// type's own text cannot be mistaken for an array marker.
fn array_element(upper: &str) -> Option<&str> {
    let inner = upper.strip_suffix(']')?;
    let open = inner.rfind('[')?;
    inner[open + 1..]
        .chars()
        .all(|c| c.is_ascii_digit())
        .then(|| &inner[..open])
}

/// Split a declared type into its base name and the text inside its parentheses.
///
/// Anything trailing the closing parenthesis stays part of the base name, so
/// `TIMESTAMP(6) WITH TIME ZONE` reduces to `TIMESTAMP WITH TIME ZONE` rather than to
/// something unrecognized.
fn split_parameters(upper: &str) -> (String, Option<&str>) {
    let (Some(open), Some(close)) = (upper.find('('), upper.rfind(')')) else {
        return (upper.to_string(), None);
    };
    if close < open {
        return (upper.to_string(), None);
    }

    let mut base = upper[..open].trim_end().to_string();
    let trailing = upper[close + 1..].trim();
    if !trailing.is_empty() {
        base.push(' ');
        base.push_str(trailing);
    }
    (base, Some(&upper[open + 1..close]))
}

/// `DECIMAL`/`NUMERIC` at its declared precision and scale.
///
/// A declaration Arrow cannot represent falls back to the shard's default rather than to a
/// clamped guess: DuckDB rejects `DECIMAL(p)` with p > 38 at `CREATE TABLE`, so such a
/// column cannot exist to be read, and inventing a precision for it would only hide that.
fn decimal_type(parameters: Option<&str>) -> DataType {
    let default = DataType::Decimal128(DEFAULT_DECIMAL.0, DEFAULT_DECIMAL.1);
    let Some(parameters) = parameters else {
        return default;
    };

    let mut parts = parameters.split(',');
    let precision = parts.next().and_then(|p| p.trim().parse::<u8>().ok());
    // `DECIMAL(10)` means scale 0, the same as it does in PostgreSQL.
    let scale = match parts.next() {
        Some(s) => s.trim().parse::<i8>().ok(),
        None => Some(0),
    };

    match (precision, scale) {
        (Some(precision), Some(scale))
            if (1..=DECIMAL128_MAX_PRECISION).contains(&precision)
                && (0..=precision as i8).contains(&scale) =>
        {
            DataType::Decimal128(precision, scale)
        }
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn micros_utc() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    #[test]
    fn unsigned_integers_keep_their_range() {
        assert_eq!(parse_data_type("UTINYINT"), DataType::UInt8);
        assert_eq!(parse_data_type("USMALLINT"), DataType::UInt16);
        assert_eq!(parse_data_type("UINTEGER"), DataType::UInt32);
        // The one width with no unsigned room left below it: `u64::MAX` is not an `i64`,
        // and a `UInt64` column is put on the wire as `bigint`, which would refuse it.
        assert_eq!(parse_data_type("UBIGINT"), DataType::Decimal128(20, 0));
    }

    #[test]
    fn the_temporal_types_are_not_text() {
        assert_eq!(parse_data_type("TIMESTAMPTZ"), micros_utc());
        assert_eq!(parse_data_type("TIMESTAMP WITH TIME ZONE"), micros_utc());
        assert_eq!(
            parse_data_type("TIME"),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            parse_data_type("INTERVAL"),
            DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn timestamp_units_are_honored() {
        assert_eq!(
            parse_data_type("TIMESTAMP_S"),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        assert_eq!(
            parse_data_type("TIMESTAMP_MS"),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            parse_data_type("TIMESTAMP_NS"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        // The unqualified spelling stays microseconds, matching what DuckDB returns.
        assert_eq!(
            parse_data_type("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn the_missing_aliases_resolve_to_their_canonical_types() {
        assert_eq!(parse_data_type("LONG"), DataType::Int64);
        assert_eq!(parse_data_type("SIGNED"), DataType::Int32);
        assert_eq!(parse_data_type("SHORT"), DataType::Int16);
        assert_eq!(parse_data_type("INT1"), DataType::Int8);
        assert_eq!(parse_data_type("LOGICAL"), DataType::Boolean);
        assert_eq!(
            parse_data_type("DATETIME"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    // The alias whose absence lost data rather than an OID.
    #[test]
    fn the_blob_aliases_stay_binary() {
        assert_eq!(parse_data_type("BINARY"), DataType::Binary);
        assert_eq!(parse_data_type("VARBINARY"), DataType::Binary);
        assert_eq!(parse_data_type("BLOB"), DataType::Binary);
        assert_eq!(parse_data_type("BYTEA"), DataType::Binary);
    }

    #[test]
    fn a_decimal_keeps_its_declared_precision_and_scale() {
        assert_eq!(
            parse_data_type("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(parse_data_type("NUMERIC(5, 3)"), DataType::Decimal128(5, 3));
        // The widest DuckDB decimal, which the old fixed (38,10) target NULLified.
        assert_eq!(
            parse_data_type("NUMERIC(38,0)"),
            DataType::Decimal128(38, 0)
        );
        // One parameter is a precision with scale 0.
        assert_eq!(parse_data_type("DECIMAL(9)"), DataType::Decimal128(9, 0));
    }

    #[test]
    fn a_decimal_with_no_parentheses_takes_the_shards_default() {
        let default = DataType::Decimal128(DEFAULT_DECIMAL.0, DEFAULT_DECIMAL.1);
        assert_eq!(parse_data_type("DECIMAL"), default);
        assert_eq!(parse_data_type("NUMERIC"), default);
        // As does a declaration Arrow cannot hold, or one that is not a declaration.
        assert_eq!(parse_data_type("DECIMAL(40,2)"), default);
        assert_eq!(parse_data_type("DECIMAL(10,20)"), default);
        assert_eq!(parse_data_type("DECIMAL(x,y)"), default);
        assert_eq!(parse_data_type("DECIMAL()"), default);
    }

    #[test]
    fn lengths_arrow_does_not_model_are_ignored() {
        assert_eq!(parse_data_type("VARCHAR(64)"), DataType::Utf8);
        assert_eq!(
            parse_data_type("TIMESTAMP(6)"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Including when the type's own words continue after them.
        assert_eq!(parse_data_type("TIMESTAMP(6) WITH TIME ZONE"), micros_utc());
        assert_eq!(
            parse_data_type("TIME(3) WITHOUT TIME ZONE"),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn matching_is_case_and_space_insensitive() {
        assert_eq!(parse_data_type("Integer"), DataType::Int32);
        assert_eq!(parse_data_type("  bigint  "), DataType::Int64);
        assert_eq!(parse_data_type("Double Precision"), DataType::Float64);
        assert_eq!(parse_data_type("timestamptz"), micros_utc());
        assert_eq!(
            parse_data_type("numeric(10,2)"),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn an_unknown_type_still_degrades_to_text() {
        assert_eq!(parse_data_type("GEOMETRY"), DataType::Utf8);
        assert_eq!(parse_data_type("UUID"), DataType::Utf8);
        assert_eq!(parse_data_type("ENUM('a', 'b')"), DataType::Utf8);
        assert_eq!(parse_data_type("STRUCT(a INTEGER)"), DataType::Utf8);
    }

    // Both halves of the `Utf8` answer, told apart. The predicate push-down needs the
    // difference: only a column the shard also stores as text can carry a comparison.
    #[test]
    fn real_text_is_told_apart_from_text_the_fallback_invented() {
        for declared in [
            "VARCHAR",
            "VARCHAR(64)",
            "text",
            "STRING",
            "CHAR",
            "CHAR(3)",
            "BPCHAR",
            "CHARACTER VARYING(10)",
        ] {
            assert!(
                is_declared_text(declared),
                "{declared} is a text type and must be recognized as one"
            );
        }

        // Advertised as text, stored as something else — the columns a predicate must not
        // be pushed onto.
        for declared in [
            "UUID",
            "JSON",
            "JSONB",
            "ENUM('a', 'b')",
            "STRUCT(a INTEGER)",
            "GEOMETRY",
            "VARCHAR[]",
        ] {
            assert!(
                !is_declared_text(declared),
                "{declared} is not text, whatever it is advertised as"
            );
        }

        // A type that is not text at all is trivially not text.
        assert!(!is_declared_text("INTEGER"));
    }

    #[test]
    fn arrays_carry_their_element_type() {
        let list_of = |dt| DataType::List(Arc::new(Field::new("item", dt, true)));
        assert_eq!(parse_data_type("INTEGER[]"), list_of(DataType::Int32));
        assert_eq!(parse_data_type("integer[3]"), list_of(DataType::Int32));
        assert_eq!(
            parse_data_type("NUMERIC(10,2)[]"),
            list_of(DataType::Decimal128(10, 2))
        );
        assert_eq!(
            parse_data_type("INTEGER[][]"),
            list_of(list_of(DataType::Int32))
        );
        assert_eq!(parse_data_type("GEOMETRY[]"), list_of(DataType::Utf8));
    }

    // A `[` inside a nested declaration is not an array marker.
    #[test]
    fn a_bracket_inside_a_declaration_is_not_an_array_suffix() {
        assert_eq!(parse_data_type("STRUCT(a INTEGER[])"), DataType::Utf8);
    }

    #[test]
    fn the_types_that_cannot_be_read_back_are_refused() {
        for declared in [
            "HUGEINT",
            "UHUGEINT",
            "BIT",
            "BIT(8)",
            "BITSTRING",
            "VARBIT",
            "BIGNUM",
            "UNION(num INTEGER, str VARCHAR)",
            "VARIANT",
            "MAP(VARCHAR, INTEGER)",
            "TIMETZ",
            "TIME WITH TIME ZONE",
            "TIME(3) WITH TIME ZONE",
        ] {
            let reason = unserviceable_type_reason(declared)
                .unwrap_or_else(|| panic!("{declared} must be refused at DDL"));
            // The refusal has to leave the client somewhere to go.
            assert!(
                reason.contains("Use "),
                "{declared} is refused without naming an alternative: {reason}"
            );
        }
    }

    #[test]
    fn an_array_is_refused_for_the_same_reason_its_element_is() {
        assert_eq!(
            unserviceable_type_reason("HUGEINT[]"),
            unserviceable_type_reason("HUGEINT")
        );
        assert!(unserviceable_type_reason("BIT[3]").is_some());
    }

    #[test]
    fn the_types_that_can_be_served_are_not_refused() {
        for declared in [
            "INTEGER",
            "UBIGINT",
            "BIGINT",
            "DECIMAL(38,0)",
            "VARCHAR(64)",
            "BLOB",
            "TIMESTAMPTZ",
            "TIMESTAMP WITH TIME ZONE",
            "TIME",
            "INTERVAL",
            "TIMESTAMP_NS",
            "UUID",
            "JSON",
            "INTEGER[]",
            // Unknown to the mapping, but text is a faithful rendering of whatever it is,
            // and refusing types this layer has simply not heard of would be worse.
            "GEOMETRY",
        ] {
            assert_eq!(
                unserviceable_type_reason(declared),
                None,
                "{declared} must not be refused"
            );
        }
    }
}
