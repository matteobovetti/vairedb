//! What VaireDB tells a client a result column **is**, and the cells it writes itself.
//!
//! arrow-pg derives a column's PostgreSQL type from its Arrow type and writes the bytes to
//! match. That answer is right for almost every column and this module delegates to it. The
//! exceptions are the columns where the Arrow type is not the whole story, and they divide
//! into two kinds.
//!
//! ## A type arrow-pg has no arm for
//!
//! `Decimal256` — an Arrow `NUMERIC` of precision above 38 — is refused outright by
//! `into_pg_type`, so a `numeric(50,2)` column came back as `XX000 Unsupported Datatype`
//! rather than as a number. There is nothing wrong with the value; the mapping simply stops
//! short. [`pg_type`] carries it to `numeric`, which is what it is.
//!
//! ## A type the Arrow type cannot say
//!
//! `JSON`, `UUID`, `CHAR(n)` and `VARCHAR(n)` are all Arrow `Utf8`, and `Utf8` is `text`. The
//! declared string is the only place the difference survives, and
//! [`crate::column_types::column_field`] keeps it in the field's metadata for exactly this
//! moment. [`pg_type`] reads it back and advertises `json`, `uuid`, `bpchar` or `varchar`.
//!
//! ## Why advertising is not the whole job
//!
//! An OID is a promise about the **bytes**, and pgwire does not check the two agree —
//! `DataRowEncoder::encode_field` calls `to_sql` without consulting `accepts`, so a column
//! advertised as one type and written as another is a wrong answer the client cannot detect.
//! Three of the four new OIDs need no new bytes: PostgreSQL's binary `json`, `bpchar` and
//! `varchar` are each just the text, which is what arrow-pg already writes for a `Utf8`. The
//! two that do are:
//!
//! - **`uuid`**, whose binary form is the 16 raw bytes, not the 36-character spelling.
//! - **`numeric`**, whose binary form arrow-pg builds through `rust_decimal`, a 96-bit
//!   mantissa that raises `22003` above roughly 29 digits — and which it cannot build at all
//!   for a `Decimal256`.
//!
//! [`PgValue`] is those bytes. Every binary decimal cell goes through it, not only the ones
//! `rust_decimal` would have refused, so that one piece of code owns the format and is
//! exercised by every decimal test rather than only the extreme ones.
//!
//! The **text** format needs nothing here: [`super::encoding::wire_text_value`] renders
//! decimals through Arrow's own formatter, which is exact at any precision, and a `uuid` is
//! its own spelling.
//!
//! ## What is deliberately still arrow-pg's
//!
//! A decimal **inside a list** keeps arrow-pg's array encoder, which still goes through
//! `rust_decimal`: the element bytes are written by code this module does not reach, so
//! advertising a wider element type than that encoder can produce would trade an honest
//! refusal for a wrong answer. `numeric[]` above 29 digits, and `Decimal256[]` at all,
//! therefore stay as they are.

use std::error::Error;

use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, Type, to_sql_checked};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::column_types::{PgDeclaredType, field_declared_type};
use crate::pgwire_handler::error_enrichment::make_vdb_error;

/// The row description for a wire schema: one [`FieldInfo`] per column, named as the schema
/// names it and typed as [`pg_type`] answers.
///
/// Replaces `arrow_pg::datatypes::arrow_schema_to_pg_fields`, whose shape this keeps — the
/// same `FieldInfo::new(name, None, None, type, format_for(idx))` — so that Describe and
/// Execute, which both call this, keep agreeing.
pub(super) fn pg_fields(wire: &Schema, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    wire.fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            Ok(FieldInfo::new(
                field.name().into(),
                None,
                None,
                pg_type(field)?,
                format.format_for(idx),
            ))
        })
        .collect()
}

/// The PostgreSQL type a column is advertised as.
///
/// VaireDB's answer where it has one and arrow-pg's everywhere else. See the module docs for
/// what each arm is for.
fn pg_type(field: &FieldRef) -> PgWireResult<Type> {
    if let DataType::Decimal256(_, _) = field.data_type() {
        return Ok(Type::NUMERIC);
    }
    if let Some(declared) = declared_pg_type(field) {
        return Ok(declared);
    }
    arrow_pg::datatypes::field_into_pg_type(field)
}

/// The type a column's **declared** string names, where that is something its Arrow type
/// cannot say, or `None` where the Arrow type is the whole answer.
///
/// Guarded on the Arrow type as well as the declaration, because the OID advertised and the
/// bytes written must be decided by the same question: a field whose metadata says `UUID`
/// but whose array is not a string is a contradiction, and the honest reading of it is the
/// array.
fn declared_pg_type(field: &Field) -> Option<Type> {
    if !matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }
    match field_declared_type(field)? {
        (PgDeclaredType::Json, _) => Some(Type::JSON),
        (PgDeclaredType::Uuid, _) => Some(Type::UUID),
        (PgDeclaredType::BpChar, _) => Some(Type::BPCHAR),
        (PgDeclaredType::VarChar, _) => Some(Type::VARCHAR),
    }
}

/// The value VaireDB writes for the cell at `row` of `col`, or `None` where arrow-pg's
/// encoder is the one that writes it.
///
/// Binary only. `pg_field` is consulted rather than assumed so that a text cell reaching
/// here — an array, which the caller sends this way in both formats — is left alone.
pub(super) fn own_binary_value(
    col: &ArrayRef,
    row: usize,
    field: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<Option<PgValue>> {
    if pg_field.format() != FieldFormat::Binary {
        return Ok(None);
    }
    let is_null = super::encoding::is_null_on_the_wire(col.as_ref(), row);
    match field.data_type() {
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            if is_null {
                return Ok(Some(PgValue::Null));
            }
            let unscaled = unscaled_decimal(col, row, field)?;
            Ok(Some(PgValue::Numeric(PgNumeric::from_unscaled(
                &unscaled, *scale,
            ))))
        }
        _ if declared_pg_type(field) == Some(Type::UUID) => {
            if is_null {
                return Ok(Some(PgValue::Null));
            }
            let text = super::encoding::wire_text_value(col.as_ref(), row);
            let parsed = uuid::Uuid::parse_str(text.trim()).map_err(|_| {
                make_vdb_error(
                    VdbErrorCode::InternalError,
                    format!(
                        "column \"{}\" is declared uuid but holds a value that is not one",
                        field.name()
                    ),
                )
            })?;
            Ok(Some(PgValue::Uuid(parsed.into_bytes())))
        }
        _ => Ok(None),
    }
}

/// A decimal cell as its unscaled integer, spelled in base ten.
///
/// A string and not an integer because `Decimal256` has no primitive to be: 78 digits do not
/// fit an `i128`, and the base-10000 regrouping [`PgNumeric`] does reads decimal digits
/// anyway, so the string is both the widest common form and the one actually wanted.
fn unscaled_decimal(col: &ArrayRef, row: usize, field: &Field) -> PgWireResult<String> {
    let missing = || {
        make_vdb_error(
            VdbErrorCode::InternalError,
            format!(
                "column \"{}\" is {} but is not backed by a decimal array",
                field.name(),
                field.data_type()
            ),
        )
    };
    match field.data_type() {
        DataType::Decimal128(_, _) => Ok(col
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(missing)?
            .value(row)
            .to_string()),
        DataType::Decimal256(_, _) => Ok(col
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .ok_or_else(missing)?
            .value(row)
            .to_string()),
        _ => Err(missing()),
    }
}

/// A cell VaireDB writes to the wire itself. See the module docs for why each is here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PgValue {
    /// PostgreSQL's `numeric`, in the base-10000 form it is on the wire.
    Numeric(PgNumeric),
    /// PostgreSQL's `uuid`, as the sixteen bytes it is on the wire.
    Uuid([u8; 16]),
    /// No value at all. Carried as a variant rather than an `Option` around one, so the
    /// caller has a single thing to encode however the cell turned out.
    Null,
}

impl ToSql for PgValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            PgValue::Numeric(numeric) => numeric.put_binary(out),
            PgValue::Uuid(bytes) => out.put_slice(bytes),
            PgValue::Null => return Ok(IsNull::Yes),
        }
        Ok(IsNull::No)
    }

    /// Declared for the trait's sake and for honesty about what these bytes are. pgwire
    /// never asks: `encode_field` goes straight to `to_sql`, which is why the OID and the
    /// bytes are decided together in [`own_binary_value`] instead.
    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC | Type::UUID)
    }

    to_sql_checked!();
}

impl ToSqlText for PgValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            PgValue::Numeric(numeric) => out.put_slice(numeric.to_text().as_bytes()),
            PgValue::Uuid(bytes) => {
                out.put_slice(uuid::Uuid::from_bytes(*bytes).to_string().as_bytes())
            }
            PgValue::Null => return Ok(IsNull::Yes),
        }
        Ok(IsNull::No)
    }
}

/// PostgreSQL's `numeric`, as the wire holds it.
///
/// Not a scaled binary integer like Arrow's decimals and not a float: a sign, a count of
/// digits **base 10000**, the power of 10000 the first of them stands for, and the number of
/// decimal digits to display. That shape is why it has no precision limit to run into — the
/// digits are a list, and a list can be as long as the number is.
///
/// `dscale` is the display scale, which is the column's own declared scale and not a
/// property of the value: `1.50` and `1.5` are the same number and differ only here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgNumeric {
    /// Base-10000 digits, most significant first, with no leading or trailing zero group.
    digits: Vec<i16>,
    /// The power of 10000 that `digits[0]` stands for. `digits[i]` stands for
    /// `10000^(weight - i)`, so the first digit past the decimal point sits at `weight + 1`.
    weight: i16,
    /// `0x0000` positive, `0x4000` negative. `0xC000`, NaN, cannot arise: an Arrow decimal
    /// has no NaN to carry.
    sign: u16,
    /// Decimal digits to display after the point.
    dscale: u16,
}

/// PostgreSQL's `NUMERIC_POS`.
const SIGN_POSITIVE: u16 = 0x0000;
/// PostgreSQL's `NUMERIC_NEG`.
const SIGN_NEGATIVE: u16 = 0x4000;
/// Digits per base-10000 group.
const GROUP: usize = 4;

impl PgNumeric {
    /// Build from a decimal's unscaled integer, spelled in base ten, and its scale — which
    /// is exactly what an Arrow `Decimal128` or `Decimal256` column is.
    ///
    /// A negative scale is a whole number with trailing zeros the digits do not spell, so it
    /// is written out and the display scale is none; Arrow permits it, PostgreSQL's wire
    /// format has no room for it, and multiplying it out loses nothing.
    pub(super) fn from_unscaled(unscaled: &str, scale: i8) -> Self {
        let sign = if unscaled.starts_with('-') {
            SIGN_NEGATIVE
        } else {
            SIGN_POSITIVE
        };
        let spelled = unscaled.trim_start_matches(['-', '+']);

        let (integral, fractional) = if scale <= 0 {
            let mut integral = spelled.to_string();
            integral.push_str(&"0".repeat(scale.unsigned_abs() as usize));
            (integral, String::new())
        } else {
            let scale = scale as usize;
            match spelled.len().checked_sub(scale) {
                // More digits than the scale spends: the point falls inside them.
                Some(at) if at > 0 => (spelled[..at].to_string(), spelled[at..].to_string()),
                // The scale swallows every digit, and then some: a leading zero and as many
                // more as it takes to push the digits down to where the scale puts them.
                _ => ("0".to_string(), format!("{spelled:0>scale$}")),
            }
        };

        // The groups are counted from the decimal point outwards in both directions, so each
        // side is padded on its far end to a multiple of four.
        let integral_groups = grouped(&pad_start(&integral));
        let fractional_groups = grouped(&pad_end(&fractional));

        let mut weight = integral_groups.len() as i16 - 1;
        let mut digits: Vec<i16> = integral_groups
            .into_iter()
            .chain(fractional_groups)
            .collect();

        // A leading zero group is one the weight already accounts for, and a trailing one
        // says nothing the display scale does not. PostgreSQL sends neither.
        let leading = digits.iter().take_while(|digit| **digit == 0).count();
        digits.drain(..leading);
        weight -= leading as i16;
        while digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() {
            weight = 0;
        }

        PgNumeric {
            digits,
            weight,
            sign,
            dscale: scale.max(0) as u16,
        }
    }

    /// Append the value in PostgreSQL's binary `numeric` format.
    fn put_binary(&self, out: &mut BytesMut) {
        out.put_i16(self.digits.len() as i16);
        out.put_i16(self.weight);
        out.put_u16(self.sign);
        out.put_u16(self.dscale);
        for digit in &self.digits {
            out.put_i16(*digit);
        }
    }

    /// The value in PostgreSQL's text `numeric` format.
    ///
    /// Only a `numeric` VaireDB built itself is ever rendered from here — a decimal column
    /// takes Arrow's own formatter instead ([`super::encoding::wire_text_value`]). It exists
    /// so that the two formats of one value are written from one place and can be tested
    /// against each other.
    fn to_text(&self) -> String {
        let mut spelled = String::new();
        if self.sign == SIGN_NEGATIVE {
            spelled.push('-');
        }

        // Everything at or above 10000^0. A group's exponent is not its index: `digits[i]`
        // stands for `10000^(weight - i)`, so the exponent is what to count down and the
        // index is derived from it — which is also how a group the number does not carry
        // reads as the zero it is.
        let mut integral = String::new();
        let mut exponent = self.weight.max(0);
        loop {
            let group = self.group_at(exponent);
            if integral.is_empty() {
                // The most significant group is the only one not zero-padded.
                integral.push_str(&group.to_string());
            } else {
                integral.push_str(&format!("{group:04}"));
            }
            if exponent == 0 {
                break;
            }
            exponent -= 1;
        }
        spelled.push_str(integral.trim_start_matches('0'));
        if spelled.is_empty() || spelled == "-" {
            spelled.push('0');
        }

        if self.dscale == 0 {
            return spelled;
        }

        // The fractional digits are the groups below 10000^0, taken to as many decimal
        // places as the display scale asks for and no fewer.
        let mut fractional = String::new();
        let mut exponent: i16 = -1;
        while fractional.len() < self.dscale as usize {
            fractional.push_str(&format!("{:04}", self.group_at(exponent)));
            exponent -= 1;
        }
        fractional.truncate(self.dscale as usize);

        spelled.push('.');
        spelled.push_str(&fractional);
        spelled
    }

    /// The base-10000 group standing for `10000^exponent`, zero where the number does not
    /// carry one.
    fn group_at(&self, exponent: i16) -> i16 {
        let index = i32::from(self.weight) - i32::from(exponent);
        usize::try_from(index)
            .ok()
            .and_then(|index| self.digits.get(index))
            .copied()
            .unwrap_or(0)
    }
}

/// Left-pad to a whole number of base-10000 groups.
fn pad_start(digits: &str) -> String {
    format!("{}{digits}", "0".repeat(padding(digits.len())))
}

/// Right-pad to a whole number of base-10000 groups.
fn pad_end(digits: &str) -> String {
    format!("{digits}{}", "0".repeat(padding(digits.len())))
}

/// Zeros needed to round `len` up to a multiple of [`GROUP`].
fn padding(len: usize) -> usize {
    (GROUP - len % GROUP) % GROUP
}

/// Read a padded digit string as base-10000 groups.
///
/// Infallible by construction: every caller passes a string of ASCII digits whose length is
/// a multiple of four, and four digits cannot exceed 9999.
fn grouped(padded: &str) -> Vec<i16> {
    padded
        .as_bytes()
        .chunks(GROUP)
        .map(|chunk| {
            chunk
                .iter()
                .fold(0i16, |acc, byte| acc * 10 + i16::from(byte - b'0'))
        })
        .collect()
}

/// Whether a wire schema has any column whose bytes VaireDB writes itself, asked once per
/// result rather than once per cell.
pub(super) fn writes_any_own_value(wire: &Schema) -> bool {
    wire.fields().iter().any(|field| {
        matches!(
            field.data_type(),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ) || declared_pg_type(field) == Some(Type::UUID)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::column_types::column_field;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::i256;

    /// The four fields PostgreSQL's header carries, decoded back out of the bytes.
    fn header(value: &PgValue) -> (i16, i16, u16, u16, Vec<i16>) {
        let mut out = BytesMut::new();
        let written = value.to_sql(&Type::NUMERIC, &mut out).unwrap();
        assert!(matches!(written, IsNull::No), "a numeric is not a null");
        let read = |at: usize| i16::from_be_bytes([out[at], out[at + 1]]);
        let ndigits = read(0);
        let digits = (0..ndigits as usize).map(|i| read(8 + i * 2)).collect();
        (ndigits, read(2), read(4) as u16, read(6) as u16, digits)
    }

    fn numeric(unscaled: &str, scale: i8) -> PgValue {
        PgValue::Numeric(PgNumeric::from_unscaled(unscaled, scale))
    }

    fn text_of(value: &PgValue) -> String {
        let mut out = BytesMut::new();
        value
            .to_sql_text(&Type::NUMERIC, &mut out, &FormatOptions::default())
            .unwrap();
        String::from_utf8(out.to_vec()).unwrap()
    }

    #[test]
    fn a_number_is_grouped_from_the_decimal_point_outwards() {
        // 12345.678 — the integral side pads on its left, the fractional on its right, so
        // `1 | 2345 . 6780`.
        let (ndigits, weight, sign, dscale, digits) = header(&numeric("12345678", 3));
        assert_eq!((ndigits, weight, sign, dscale), (3, 1, SIGN_POSITIVE, 3));
        assert_eq!(digits, vec![1, 2345, 6780]);
    }

    #[test]
    fn a_whole_number_that_is_a_power_of_the_base_is_one_digit() {
        // 10000 is 1 * 10000^1, and the trailing zero group says nothing.
        let (ndigits, weight, _, dscale, digits) = header(&numeric("10000", 0));
        assert_eq!((ndigits, weight, dscale), (1, 1, 0));
        assert_eq!(digits, vec![1]);
    }

    #[test]
    fn a_number_below_one_has_a_negative_weight() {
        // 0.0001 is 1 * 10000^-1.
        let (ndigits, weight, _, dscale, digits) = header(&numeric("1", 4));
        assert_eq!((ndigits, weight, dscale), (1, -1, 4));
        assert_eq!(digits, vec![1]);
    }

    #[test]
    fn zero_has_no_digits_at_all_and_still_has_a_scale() {
        let (ndigits, weight, sign, dscale, digits) = header(&numeric("0", 2));
        assert_eq!((ndigits, weight, sign, dscale), (0, 0, SIGN_POSITIVE, 2));
        assert!(digits.is_empty());
        assert_eq!(text_of(&numeric("0", 2)), "0.00");
    }

    #[test]
    fn the_sign_is_a_flag_and_not_a_digit() {
        let (_, weight, sign, _, digits) = header(&numeric("-12345678", 3));
        assert_eq!((weight, sign), (1, SIGN_NEGATIVE));
        assert_eq!(digits, vec![1, 2345, 6780]);
        assert_eq!(text_of(&numeric("-12345678", 3)), "-12345.678");
    }

    #[test]
    fn a_negative_scale_is_multiplied_out_rather_than_carried() {
        // Arrow allows `Decimal128(3, -2)` — 12 meaning 1200. The wire format has no
        // negative dscale, so the zeros are spelled.
        let (_, weight, _, dscale, digits) = header(&numeric("12", -2));
        assert_eq!((weight, dscale), (0, 0));
        assert_eq!(digits, vec![1200]);
        assert_eq!(text_of(&numeric("12", -2)), "1200");
    }

    #[test]
    fn a_number_wider_than_rust_decimal_is_exact() {
        // 31 digits. arrow-pg's path raises `22003` here, because `rust_decimal` has a
        // 96-bit mantissa; a list of base-10000 digits has no width to exceed.
        let unscaled = "1234567890123456789012345678901";
        let value = numeric(unscaled, 2);
        let (_, weight, _, dscale, digits) = header(&value);
        assert_eq!((weight, dscale), (7, 2));
        assert_eq!(
            digits,
            vec![1, 2345, 6789, 123, 4567, 8901, 2345, 6789, 100]
        );
        assert_eq!(text_of(&value), "12345678901234567890123456789.01");
    }

    #[test]
    fn a_number_wider_than_decimal128_is_exact() {
        // 40 digits: a `Decimal256` value, which arrow-pg refuses outright.
        let unscaled = "1234567890123456789012345678901234567890";
        assert_eq!(
            text_of(&numeric(unscaled, 10)),
            "123456789012345678901234567890.1234567890"
        );
    }

    #[test]
    fn the_text_of_a_number_is_the_binary_read_back() {
        // Every case above spelled again through the other format, so the two cannot drift.
        for (unscaled, scale, spelled) in [
            ("12345678", 3, "12345.678"),
            ("10000", 0, "10000"),
            ("1", 4, "0.0001"),
            ("-1", 4, "-0.0001"),
            ("100", 2, "1.00"),
            ("999999999999", 0, "999999999999"),
            ("1", 0, "1"),
            ("-500", 1, "-50.0"),
            ("100000001", 4, "10000.0001"),
            ("10000000000000001", 0, "10000000000000001"),
        ] {
            assert_eq!(
                text_of(&numeric(unscaled, scale)),
                spelled,
                "{unscaled}e-{scale}"
            );
        }
    }

    #[test]
    fn a_uuid_goes_out_as_sixteen_bytes_and_not_as_its_spelling() {
        let spelled = "550e8400-e29b-41d4-a716-446655440000";
        let value = PgValue::Uuid(uuid::Uuid::parse_str(spelled).unwrap().into_bytes());
        let mut out = BytesMut::new();
        let written = value.to_sql(&Type::UUID, &mut out).unwrap();
        assert!(matches!(written, IsNull::No));
        assert_eq!(out.len(), 16, "the binary form is the bytes, not the text");
        assert_eq!(out[0], 0x55);
        assert_eq!(text_of(&value), spelled, "the text form is the spelling");
    }

    #[test]
    fn a_null_is_a_null_in_either_format() {
        let mut out = BytesMut::new();
        let binary = PgValue::Null.to_sql(&Type::NUMERIC, &mut out).unwrap();
        assert!(matches!(binary, IsNull::Yes));
        assert!(out.is_empty());

        let mut out = BytesMut::new();
        let text = PgValue::Null
            .to_sql_text(&Type::NUMERIC, &mut out, &FormatOptions::default())
            .unwrap();
        assert!(matches!(text, IsNull::Yes));
        assert!(out.is_empty());
    }

    #[test]
    fn a_wide_decimal_is_advertised_as_numeric_rather_than_refused() {
        let field = Arc::new(Field::new("d", DataType::Decimal256(50, 2), true));
        assert_eq!(pg_type(&field).unwrap(), Type::NUMERIC);
        // The narrow one was never in doubt, and still answers the same.
        let narrow = Arc::new(Field::new("d", DataType::Decimal128(10, 2), true));
        assert_eq!(pg_type(&narrow).unwrap(), Type::NUMERIC);
    }

    #[test]
    fn a_declared_type_is_advertised_as_itself_and_not_as_text() {
        for (declared, expected) in [
            ("JSON", Type::JSON),
            ("JSONB", Type::JSON),
            ("UUID", Type::UUID),
            ("CHAR(3)", Type::BPCHAR),
            ("VARCHAR(64)", Type::VARCHAR),
        ] {
            let field = Arc::new(column_field("c", declared, true));
            assert_eq!(pg_type(&field).unwrap(), expected, "{declared}");
        }
    }

    #[test]
    fn a_column_with_nothing_extra_to_say_keeps_arrow_pgs_answer() {
        for (declared, expected) in [
            ("TEXT", Type::TEXT),
            ("INTEGER", Type::INT4),
            ("BOOLEAN", Type::BOOL),
            ("DATE", Type::DATE),
        ] {
            let field = Arc::new(column_field("c", declared, true));
            assert_eq!(pg_type(&field).unwrap(), expected, "{declared}");
        }
    }

    #[test]
    fn a_declaration_the_array_contradicts_is_read_as_the_array() {
        // Metadata says `UUID`, the column is an integer. The bytes are the integer's, so
        // the advertised type has to be too.
        let field = Arc::new(
            Field::new("c", DataType::Int32, true).with_metadata(
                [(
                    crate::column_types::DECLARED_TYPE_KEY.to_string(),
                    "UUID".to_string(),
                )]
                .into(),
            ),
        );
        assert_eq!(pg_type(&field).unwrap(), Type::INT4);
    }

    #[test]
    fn the_row_description_names_and_types_each_column() {
        let schema = Schema::new(vec![
            column_field("u", "UUID", true),
            Field::new("d", DataType::Decimal256(50, 2), true),
            column_field("t", "TEXT", true),
        ]);
        let fields = pg_fields(&schema, &Format::UnifiedBinary).unwrap();
        assert_eq!(
            fields
                .iter()
                .map(|f| (f.name().to_string(), f.datatype().clone(), f.format()))
                .collect::<Vec<_>>(),
            vec![
                ("u".to_string(), Type::UUID, FieldFormat::Binary),
                ("d".to_string(), Type::NUMERIC, FieldFormat::Binary),
                ("t".to_string(), Type::TEXT, FieldFormat::Binary),
            ]
        );
    }

    #[test]
    fn only_a_column_vairedb_writes_itself_is_taken_off_arrow_pgs_path() {
        let ours = Schema::new(vec![column_field("u", "UUID", true)]);
        assert!(writes_any_own_value(&ours));
        let decimals = Schema::new(vec![Field::new("d", DataType::Decimal128(10, 2), true)]);
        assert!(writes_any_own_value(&decimals));
        // `json`, `bpchar` and `varchar` are advertised anew but written as they always were:
        // PostgreSQL's binary form for each of them is the text.
        let text_shaped = Schema::new(vec![
            column_field("j", "JSON", true),
            column_field("c", "CHAR(3)", true),
            column_field("v", "VARCHAR(8)", true),
            column_field("t", "TEXT", true),
        ]);
        assert!(!writes_any_own_value(&text_shaped));
    }

    #[test]
    fn a_decimal_cell_is_read_out_of_either_width_of_array() {
        let field = Field::new("d", DataType::Decimal128(12, 2), true);
        let col: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(-1234i128), None])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        );
        let pg_field = FieldInfo::new("d".into(), None, None, Type::NUMERIC, FieldFormat::Binary);
        assert_eq!(
            own_binary_value(&col, 0, &field, &pg_field).unwrap(),
            Some(numeric("-1234", 2))
        );
        assert_eq!(
            own_binary_value(&col, 1, &field, &pg_field).unwrap(),
            Some(PgValue::Null),
            "a null decimal is still VaireDB's to write"
        );

        let wide = Field::new("d", DataType::Decimal256(50, 2), true);
        let col: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(9999i128))])
                .with_precision_and_scale(50, 2)
                .unwrap(),
        );
        assert_eq!(
            own_binary_value(&col, 0, &wide, &pg_field).unwrap(),
            Some(numeric("9999", 2))
        );
    }

    #[test]
    fn a_uuid_cell_is_parsed_out_of_the_string_it_is_stored_as() {
        let field = column_field("u", "UUID", true);
        let spelled = "550e8400-e29b-41d4-a716-446655440000";
        let col: ArrayRef = Arc::new(StringArray::from(vec![Some(spelled), None]));
        let pg_field = FieldInfo::new("u".into(), None, None, Type::UUID, FieldFormat::Binary);
        assert_eq!(
            own_binary_value(&col, 0, &field, &pg_field).unwrap(),
            Some(PgValue::Uuid(
                uuid::Uuid::parse_str(spelled).unwrap().into_bytes()
            ))
        );
        assert_eq!(
            own_binary_value(&col, 1, &field, &pg_field).unwrap(),
            Some(PgValue::Null)
        );
    }

    #[test]
    fn a_text_cell_is_left_to_arrow_pg_whatever_it_is_declared() {
        let field = column_field("u", "UUID", true);
        let col: ArrayRef = Arc::new(StringArray::from(vec![Some(
            "550e8400-e29b-41d4-a716-446655440000",
        )]));
        let pg_field = FieldInfo::new("u".into(), None, None, Type::UUID, FieldFormat::Text);
        assert_eq!(own_binary_value(&col, 0, &field, &pg_field).unwrap(), None);
    }

    #[test]
    fn a_column_that_is_not_ours_is_left_to_arrow_pg() {
        let field = column_field("t", "TEXT", true);
        let col: ArrayRef = Arc::new(StringArray::from(vec![Some("hello")]));
        let pg_field = FieldInfo::new("t".into(), None, None, Type::TEXT, FieldFormat::Binary);
        assert_eq!(own_binary_value(&col, 0, &field, &pg_field).unwrap(), None);
    }
}
