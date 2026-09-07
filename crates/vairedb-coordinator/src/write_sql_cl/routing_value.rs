//! Resolves a shard-key expression to a stable, canonical routing string so that
//! a parameterized write and the equivalent literal write hash to the same
//! shard. Numeric forms are normalized (`10`, `10.0`, `1e1` all → `10`) using
//! exact string/integer arithmetic — never a float round-trip — so large
//! integers and high-precision decimals route exactly.
//!
//! Only forms whose value the coordinator can determine from the statement alone
//! are routable. Anything else — a computed expression, a function call, a value
//! of a type whose bound and literal spellings do not agree — yields
//! [`RoutedValue::Unroutable`] so the caller rejects the write. Hashing such an
//! expression's *source text* would place the row on a shard that no equivalent
//! lookup ever visits, and nothing would fail.

use crate::sqlparser::ast::{Expr, UnaryOperator, Value};
use datafusion::scalar::ScalarValue;

/// The routing form of a single shard-key expression.
pub(super) enum RoutedValue {
    /// A canonicalized routing string ready to hash.
    Value(String),
    /// The expression resolves to SQL NULL (literal `NULL`, or a `$N` bound to a
    /// NULL parameter).
    Null,
    /// The expression's value is not determinable here, so it cannot be hashed.
    /// Carries a client-facing reason; the caller decides whether that means
    /// rejecting the statement (INSERT — routing on the wrong shard would lose
    /// the row) or broadcasting it (UPDATE/DELETE — every shard re-evaluates the
    /// predicate itself, so a broadcast is correct, just unoptimized).
    Unroutable(String),
}

/// Resolve the routing string for a shard-key expression.
///
/// Routable forms are those whose value is fixed by the statement plus its bind
/// parameters: a literal (rendered the way it would be re-serialized into SQL),
/// a parenthesized such literal, a signed numeric literal, a typed string
/// constant (`DATE '2022-01-08'`), and a `$N` placeholder resolved against the
/// decoded parameters so a parameterized write hashes to the same shard as the
/// equivalent literal. The result is canonicalized so different textual forms of
/// one value (`10`, `10.0`, `10.00`) route together.
///
/// Every other expression is [`RoutedValue::Unroutable`]: `VALUES (1 + 1, …)`
/// hashed as the text `1 + 1` lands on a different shard from the `2` the row is
/// stored under, and `nextval('s')` or a bare column reference has no value here
/// at all.
pub(super) fn expr_routing_value(expr: &Expr, params: &[ScalarValue]) -> RoutedValue {
    match expr {
        // Parentheses do not change the value.
        Expr::Nested(inner) => expr_routing_value(inner, params),
        Expr::Value(v) => value_routing_value(&v.value, expr, params),
        // `-1` parses as unary minus applied to the literal `1`. sqlparser
        // re-renders it without a space, so the whole expression's text is a
        // signed numeric token the canonicalizer understands.
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr: operand,
        } if matches!(operand.as_ref(), Expr::Value(v) if matches!(v.value, Value::Number(..))) => {
            RoutedValue::Value(canonicalize_routing_value(expr.to_string()))
        }
        // A typed string constant (`DATE '2022-01-08'`, `TIMESTAMP '...'`) routes
        // on its string body, so the same value spelled bare — `'2022-01-08'`
        // into a DATE column, which stores the identical value — hashes to the
        // same shard. Date parameters are rendered to match (see
        // [`scalar_routing_value`]).
        Expr::TypedString(typed) => match typed.value.value.clone().into_string() {
            Some(s) => RoutedValue::Value(canonicalize_routing_value(quote_literal(&s))),
            None => unroutable_expr(expr),
        },
        _ => unroutable_expr(expr),
    }
}

/// Routing form of a literal or placeholder in the shard-key position.
///
/// `expr` is the enclosing expression, used for its `Display` so a literal
/// renders exactly as sqlparser would re-serialize it.
fn value_routing_value(value: &Value, expr: &Expr, params: &[ScalarValue]) -> RoutedValue {
    match value {
        Value::Null => RoutedValue::Null,
        Value::Placeholder(name) => match name
            .strip_prefix('$')
            .and_then(|d| d.parse::<usize>().ok())
            .and_then(|n| n.checked_sub(1))
        {
            Some(idx) => match params.get(idx) {
                Some(scalar) => scalar_routing_value(scalar),
                // The client bound fewer parameters than the statement uses.
                // Guessing a shard here would be worse than saying so.
                None => RoutedValue::Unroutable(format!(
                    "no value was bound for shard-key parameter {name}"
                )),
            },
            None => RoutedValue::Unroutable(format!(
                "shard-key placeholder `{name}` is not a positional `$N` parameter"
            )),
        },
        // Literal spellings whose `Display` is the value itself.
        Value::Number(..)
        | Value::Boolean(_)
        | Value::SingleQuotedString(_)
        | Value::DollarQuotedString(_)
        | Value::EscapedStringLiteral(_)
        | Value::UnicodeStringLiteral(_)
        | Value::NationalStringLiteral(_) => {
            RoutedValue::Value(canonicalize_routing_value(expr.to_string()))
        }
        _ => unroutable_expr(expr),
    }
}

/// Reject a shard-key expression the coordinator cannot reduce to a value,
/// naming it so the client can see what to rewrite.
fn unroutable_expr(expr: &Expr) -> RoutedValue {
    RoutedValue::Unroutable(format!(
        "shard-key expression `{expr}` is not a constant; the coordinator hashes \
         the shard key before the write reaches a shard, so it must be a literal \
         or a bind parameter"
    ))
}

/// Wrap `s` as a single-quoted SQL string literal, doubling embedded quotes —
/// the form sqlparser re-serializes a string literal in.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Expand a scientific-notation numeric token (e.g. `"1e20"`, `"1.5e3"`,
/// `"15e-1"`) into its plain-decimal string form (`"100000000000000000000"`,
/// `"1500"`, `"1.5"`) by shifting the decimal point with exact string ops — no
/// float round-trip, so large integers and high-precision decimals stay exact.
/// Returns `None` for tokens without an exponent marker, ones that are not a
/// well-formed `[-+]<digits>[.<digits>]e[-+]<digits>` form, or whose exponent
/// magnitude is implausibly large.
fn expand_scientific_notation(t: &str) -> Option<String> {
    let e_pos = t.bytes().position(|b| b == b'e' || b == b'E')?;
    let mantissa = &t[..e_pos];
    let exp: i32 = t[e_pos + 1..].parse().ok()?;
    if exp.abs() > 1000 {
        return None;
    }

    let (neg, mant) = match mantissa.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, mantissa.strip_prefix('+').unwrap_or(mantissa)),
    };

    // Mantissa must be plain decimal: digits and at most one '.'.
    if mant.matches('.').count() > 1 {
        return None;
    }
    let mut mant_parts = mant.splitn(2, '.');
    let int_digits = mant_parts.next().unwrap_or("");
    let frac_digits = mant_parts.next().unwrap_or("");
    let digits = format!("{int_digits}{frac_digits}");
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    // Position of the decimal point (digits to its left) after shifting by exp.
    let point = int_digits.len() as i64 + exp as i64;
    let ndigits = digits.len() as i64;

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if point <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-point) as usize));
        out.push_str(&digits);
    } else if point >= ndigits {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', (point - ndigits) as usize));
    } else {
        let p = point as usize;
        out.push_str(&digits[..p]);
        out.push('.');
        out.push_str(&digits[p..]);
    }
    Some(out)
}

/// Canonicalize a routing token so that all textual forms of the same value hash
/// identically: `"10.0"`, `"10.00"`, `"10"` → `"10"`; `"-0"` → `"0"`. A
/// single-quoted string literal whose content is numeric is unwrapped and
/// canonicalized as that number (`"'2'"` → `"2"`), so a value written to a
/// numeric column as a quoted string (`VALUES ('2')`) routes to the same shard
/// as the bare-number form used in a later `WHERE id = 2` — they denote the same
/// stored value. Genuine (non-numeric) string keys (`'abc'`) and booleans
/// (`true`/`false`) are returned unchanged.
fn canonicalize_routing_value(s: String) -> String {
    let t = s.trim();

    // A quoted string literal whose content is numeric must route identically to
    // the equivalent bare number: a numeric column stores `'2'` and `2` as the
    // same value, so a point lookup written either way must hash to one shard.
    // Unwrap the quotes (un-escaping doubled quotes) and canonicalize the inner
    // token; a non-numeric string key falls through and is returned unchanged.
    if let Some(inner) = t.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')) {
        return canonicalize_numeric(&inner.replace("''", "'")).unwrap_or(s);
    }

    canonicalize_numeric(t).unwrap_or(s)
}

/// Canonicalize a bare numeric token so all textual forms of the same value map
/// to one string (`"10.0"`/`"10.00"`/`"1e1"` → `"10"`, `"-0"` → `"0"`), using
/// exact string ops — never a float round-trip. Returns `None` for anything that
/// is not a plain-or-scientific numeric token (quoted strings, booleans, hex,
/// `inf`/`nan`, malformed exponents), so the caller can leave it unchanged.
fn canonicalize_numeric(t: &str) -> Option<String> {
    let t = t.trim();

    // Normalize scientific notation to plain decimal first, so a `$N` param
    // (rendered by `ScalarValue` Display as plain decimal) and an equivalent
    // SQL literal (which sqlparser may keep in exponent form) hash identically.
    let expanded;
    let t = if t.contains(['e', 'E']) {
        expanded = expand_scientific_notation(t)?;
        expanded.as_str()
    } else {
        t
    };

    let (neg, digits) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t),
    };

    // Plain decimal only: digits and at most one '.'. Reject empty, exponent
    // forms, hex, "inf"/"nan", and anything non-numeric.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return None;
    }
    let mut parts = digits.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    if digits.matches('.').count() > 1 {
        return None;
    }

    let int_trimmed = int_part.trim_start_matches('0');
    let int_norm = if int_trimmed.is_empty() {
        "0"
    } else {
        int_trimmed
    };
    let frac_norm = frac_part.map(|f| f.trim_end_matches('0')).unwrap_or("");

    let mut out = String::new();
    let value_is_zero = int_norm == "0" && frac_norm.is_empty();
    if neg && !value_is_zero {
        out.push('-');
    }
    out.push_str(int_norm);
    if !frac_norm.is_empty() {
        out.push('.');
        out.push_str(frac_norm);
    }
    Some(out)
}

/// Routing form of a decoded bind parameter.
///
/// Only types whose bound form and literal form agree are routable: a parameter
/// and a literal of the same value must hash to one shard, or a parameterized
/// INSERT and a literal point lookup of the same row disagree about which shard
/// holds it. `ScalarValue`'s `Display` is a *debug-ish* rendering, not a SQL
/// literal, so each accepted type is spelled out here rather than falling back to
/// it: a `Timestamp` displays as its raw epoch count (and a different count per
/// `TimeUnit`), `Interval` as a Rust struct, `Binary` as hex — none of which any
/// literal of the same value can match. Those are rejected.
fn scalar_routing_value(scalar: &ScalarValue) -> RoutedValue {
    if scalar.is_null() {
        return RoutedValue::Null;
    }
    let s = match scalar {
        // Text: single-quoted, as sqlparser re-serializes a string literal.
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => quote_literal(s),
        ScalarValue::Boolean(Some(b)) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        // Integers and floats already display as a bare numeric token, which the
        // canonicalizer folds to the same form as the equivalent literal.
        ScalarValue::Int8(Some(_))
        | ScalarValue::Int16(Some(_))
        | ScalarValue::Int32(Some(_))
        | ScalarValue::Int64(Some(_))
        | ScalarValue::UInt8(Some(_))
        | ScalarValue::UInt16(Some(_))
        | ScalarValue::UInt32(Some(_))
        | ScalarValue::UInt64(Some(_))
        | ScalarValue::Float16(Some(_))
        | ScalarValue::Float32(Some(_))
        | ScalarValue::Float64(Some(_)) => scalar.to_string(),
        // `Decimal*` Display renders the raw `(mantissa, precision, scale)` debug
        // form, not a numeric literal, so format it as a plain decimal to match
        // how an equivalent SQL literal is re-serialized.
        ScalarValue::Decimal128(Some(v), _, scale) => {
            scaled_plain_string(*v < 0, &v.unsigned_abs().to_string(), *scale)
        }
        ScalarValue::Decimal256(Some(v), _, scale) => {
            let text = v.to_string();
            match text.strip_prefix('-') {
                Some(digits) => scaled_plain_string(true, digits, *scale),
                None => scaled_plain_string(false, &text, *scale),
            }
        }
        // A date displays as `YYYY-MM-DD`; quoting it matches the `DATE '...'`
        // literal form (see [`expr_routing_value`]), so both spellings of one date
        // route together.
        ScalarValue::Date32(Some(_)) | ScalarValue::Date64(Some(_)) => {
            quote_literal(&scalar.to_string())
        }
        other => {
            return RoutedValue::Unroutable(format!(
                "a shard-key bind parameter of type {} cannot be routed: its bound \
                 form and its SQL literal form do not agree, so the row would be \
                 placed on a shard no lookup of the same value would search; send \
                 the shard key as a literal instead",
                other.data_type()
            ));
        }
    };
    RoutedValue::Value(canonicalize_routing_value(s))
}

/// Render `digits` (the unsigned decimal digits of a mantissa) scaled by
/// `10^-scale` as a plain decimal string (e.g. `"123456"`, scale `3` ->
/// `"123.456"`) using exact string ops — never a float round-trip.
fn scaled_plain_string(neg: bool, digits: &str, scale: i8) -> String {
    let body = if scale <= 0 {
        // Zero/negative scale multiplies by 10^-scale; append trailing zeros.
        let mut s = digits.to_string();
        if digits.bytes().any(|b| b != b'0') {
            s.extend(std::iter::repeat_n('0', (-scale) as usize));
        }
        s
    } else {
        let scale = scale as usize;
        if digits.len() > scale {
            let point = digits.len() - scale;
            format!("{}.{}", &digits[..point], &digits[point..])
        } else {
            format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
        }
    };
    if neg { format!("-{body}") } else { body }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_routing_value_cases() {
        assert_eq!(canonicalize_routing_value("10.0".into()), "10");
        assert_eq!(canonicalize_routing_value("10.00".into()), "10");
        assert_eq!(canonicalize_routing_value("10".into()), "10");
        assert_eq!(canonicalize_routing_value("-10.0".into()), "-10");
        assert_eq!(canonicalize_routing_value("-0".into()), "0");
        assert_eq!(canonicalize_routing_value("-0.0".into()), "0");
        assert_eq!(canonicalize_routing_value("1.50".into()), "1.5");
        // Scientific notation is expanded to plain decimal so it matches the
        // ScalarValue Display form of the same numeric param.
        assert_eq!(canonicalize_routing_value("1e2".into()), "100");
        assert_eq!(canonicalize_routing_value("1.5e3".into()), "1500");
        assert_eq!(canonicalize_routing_value("1.50e3".into()), "1500");
        assert_eq!(canonicalize_routing_value("15e-1".into()), "1.5");
        assert_eq!(canonicalize_routing_value("-1e2".into()), "-100");
        assert_eq!(canonicalize_routing_value("-0e5".into()), "0");
        assert_eq!(
            canonicalize_routing_value("1e20".into()),
            "100000000000000000000"
        );
        // Malformed / non-finite exponent forms are left untouched.
        assert_eq!(canonicalize_routing_value("inf".into()), "inf");
        assert_eq!(canonicalize_routing_value("nan".into()), "nan");
        assert_eq!(canonicalize_routing_value("1e".into()), "1e");
        assert_eq!(canonicalize_routing_value("e5".into()), "e5");
        assert_eq!(canonicalize_routing_value("1.2.3e4".into()), "1.2.3e4");
        // A quoted string with numeric content routes as the bare number, so a
        // value written to a numeric column as `'2'` and a later `WHERE id = 2`
        // hash to the same shard.
        assert_eq!(canonicalize_routing_value("'2'".into()), "2");
        assert_eq!(canonicalize_routing_value("'10.0'".into()), "10");
        assert_eq!(canonicalize_routing_value("'-0'".into()), "0");
        // A genuine (non-numeric) string key and booleans are returned unchanged.
        assert_eq!(canonicalize_routing_value("'abc'".into()), "'abc'");
        assert_eq!(canonicalize_routing_value("'a''b'".into()), "'a''b'");
        assert_eq!(canonicalize_routing_value("true".into()), "true");
    }
}
