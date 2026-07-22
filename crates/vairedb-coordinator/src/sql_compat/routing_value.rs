//! Resolves a shard-key expression to a stable, canonical routing string so that
//! a parameterized write and the equivalent literal write hash to the same
//! shard. Numeric forms are normalized (`10`, `10.0`, `1e1` all → `10`) using
//! exact string/integer arithmetic — never a float round-trip — so large
//! integers and high-precision decimals route exactly.

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr, Value};

/// The routing form of a single shard-key expression.
pub(super) enum RoutedValue {
    /// A canonicalized routing string ready to hash.
    Value(String),
    /// The expression resolves to SQL NULL (literal `NULL`, a `$N` bound to a
    /// NULL parameter, or an unresolvable/out-of-range placeholder).
    Null,
}

/// Resolve the routing string for a shard-key expression. A literal renders the
/// same way it would be re-serialized into SQL (`expr.to_string()`); a `$N`
/// placeholder is resolved against the decoded bind parameters so that a
/// parameterized write hashes to the same shard as the equivalent literal. The
/// result is canonicalized so that different textual forms of the same numeric
/// value (`10`, `10.0`, `10.00`) route to the same shard.
pub(super) fn expr_routing_value(expr: &Expr, params: &[ScalarValue]) -> RoutedValue {
    if let Expr::Nested(inner) = expr {
        return expr_routing_value(inner, params);
    }
    let raw = if let Some(idx) = placeholder_index(expr) {
        match params.get(idx).and_then(scalar_to_shard_key_string) {
            Some(s) => s,
            None => return RoutedValue::Null,
        }
    } else if matches!(expr, Expr::Value(v) if matches!(v.value, Value::Null)) {
        return RoutedValue::Null;
    } else {
        expr.to_string()
    };
    RoutedValue::Value(canonicalize_routing_value(raw))
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

/// Zero-based parameter index for a `$N` placeholder expression, or `None`.
fn placeholder_index(expr: &Expr) -> Option<usize> {
    let Expr::Value(v) = expr else {
        return None;
    };
    let Value::Placeholder(name) = &v.value else {
        return None;
    };
    let n: usize = name.strip_prefix('$')?.parse().ok()?;
    n.checked_sub(1)
}

/// Stringify a decoded parameter for shard routing so that it matches the
/// textual form a literal of the same value would take when re-serialized by
/// sqlparser: numbers/booleans bare, strings single-quoted. Routing only needs
/// a stable, collision-resistant key, not a perfectly faithful SQL literal.
fn scalar_to_shard_key_string(scalar: &ScalarValue) -> Option<String> {
    if scalar.is_null() {
        return None;
    }
    let s = match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Boolean(Some(b)) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        // `Decimal128` Display renders the raw `(mantissa, precision, scale)`
        // debug form, not a numeric literal, so format it as a plain decimal to
        // match how an equivalent SQL literal is re-serialized.
        ScalarValue::Decimal128(Some(v), _, scale) => decimal_to_plain_string(*v, *scale),
        other => other.to_string(),
    };
    Some(s)
}

/// Render a decimal mantissa scaled by `10^-scale` as a plain decimal string
/// (e.g. mantissa `123456`, scale `3` -> `"123.456"`) using exact integer ops.
fn decimal_to_plain_string(mantissa: i128, scale: i8) -> String {
    if scale <= 0 {
        // Zero/negative scale multiplies by 10^-scale; append trailing zeros.
        let mut s = mantissa.to_string();
        if mantissa != 0 {
            s.extend(std::iter::repeat_n('0', (-scale) as usize));
        }
        return s;
    }
    let neg = mantissa < 0;
    let digits = mantissa.unsigned_abs().to_string();
    let scale = scale as usize;
    let s = if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if neg { format!("-{s}") } else { s }
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
