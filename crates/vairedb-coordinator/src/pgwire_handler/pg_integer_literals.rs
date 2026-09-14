//! PostgreSQL's non-decimal integer literals — `0b101`, `0o17`, `0x1F` — respelled as
//! decimal before either path parses the statement.
//!
//! PostgreSQL 16 added binary, octal and hexadecimal integer literals: `0b101` is `5`,
//! `0o17` is `15`, `0x1F` is `31`, and `_` may separate their digits (`0b1_0000_0000` is
//! `256`). sqlparser has neither form, and what it does instead is the reason this module
//! exists — it does not fail, it produces something else that parses:
//!
//! | text | sqlparser tokens | what the statement then means |
//! |---|---|---|
//! | `0b101` | `Number("0")`, `Word("b101")` | `SELECT 0 AS b101` — the answer is **0** |
//! | `0o17` | `Number("0")`, `Word("o17")` | `SELECT 0 AS o17` — the answer is **0** |
//! | `0X1f` | `Number("0")`, `Word("X1f")` | `SELECT 0 AS x1f` — the answer is **0** |
//! | `0x1F` | `HexStringLiteral("1F")` | the *bytes* `X'1F'`, not the number 31 |
//!
//! Every row is a wrong answer with nothing in it for a client to detect: three of them
//! answer `0` under a column label that looks like an alias the query never wrote, and
//! the fourth answers a byte string where a number was asked for. It is wrong on both
//! paths and wrong in the same way, because both start from the same tokenizer.
//!
//! ## Why the text and not the AST
//!
//! There is no AST to fix. By the time a statement is parsed the digits are gone: `0b101`
//! has become two unrelated nodes, and no rewrite can tell that `Number("0")` beside
//! `Word("b101")` was one literal rather than a value and its alias. The information
//! survives only in the source text, so the correction has to happen there — which is
//! also why one pass serves both paths. [`super::parser::parse_sql`] normalizes the text
//! once, and the read path's compatibility parser and the write path's verbatim parse
//! both see decimal digits.
//!
//! Splicing text is the narrow operation it looks like: the replacement is the same
//! integer written in base 10, so every statement that parsed before parses the same way,
//! and a literal that was already decimal is not touched at all.
//!
//! ## Why arbitrary precision
//!
//! PostgreSQL types a non-decimal literal exactly as it types the decimal spelling:
//! `0x7fffffffffffffff` is a `bigint`, and `0x8000000000000000` is a `numeric` rather
//! than an overflow. Converting through `u64` would refuse the second and converting
//! through `i128` would move the wall rather than remove it, so the digits are converted
//! by long multiplication into a decimal string of whatever length they need. What the
//! literal then *means* is decided downstream by the same code that reads
//! `340282366920938463463374607431768211455` when a client writes it in base 10 — which
//! is the point: after this pass the two spellings are the same statement.
//!
//! ## Malformed forms
//!
//! PostgreSQL's scanner has three verdicts and this reproduces all of them, because the
//! alternative is inventing a fourth. A prefix with no digits (`0x`) is *"invalid
//! hexadecimal integer"*; digits that run into something else (`0b102`, `0x1f_`) are
//! *"trailing junk after numeric literal"*; a literal followed by `.5` is a plain
//! *"syntax error"* at the `.5`. All three are `42601`, which is what a client needs to
//! know: the statement was read and refused, and no retry will help.

use std::borrow::Cow;

use crate::error::Result;
use crate::sqlparser::dialect::PostgreSqlDialect;
use crate::sqlparser::parser::ParserError;
use crate::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Word};

/// Rewrite every binary, octal and hexadecimal integer literal in `sql` to its decimal
/// spelling, leaving the rest of the text byte-identical.
///
/// Borrows `sql` unchanged when there is nothing to rewrite, which is every statement a
/// client normally sends: the guard is a two-byte scan and the tokenizer only runs when
/// it matches.
///
/// Fails with `42601` for a malformed literal, in PostgreSQL's own wording. A statement
/// that does not tokenize at all is returned untouched instead of being refused here —
/// the real parse is one call away and its error is the better one, reported against the
/// text the client actually sent.
pub(super) fn normalize_non_decimal_integers(sql: &str) -> Result<Cow<'_, str>> {
    if !might_hold_a_non_decimal_literal(sql) {
        return Ok(Cow::Borrowed(sql));
    }
    let Ok(tokens) = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize_with_location() else {
        return Ok(Cow::Borrowed(sql));
    };
    // Consecutive tokens tile the input — whitespace is a token too — so the byte where
    // one token ends is the byte where the next begins, and two tokens are adjacent (no
    // whitespace between them) exactly when they are consecutive in this list. That makes
    // the start offsets the only positions needed, and it avoids reading a span's end.
    let starts = token_start_offsets(sql, &tokens);

    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < tokens.len() {
        let Some(found) = literal_at(sql, &tokens, &starts, i) else {
            i += 1;
            continue;
        };
        let text = &sql[found.start..found.end];
        let digits = validate(found.digits, found.radix, text)?;
        reject_a_trailing_fraction(sql, &tokens, &starts, found.token_end)?;

        let buf = out.get_or_insert_with(|| String::with_capacity(sql.len()));
        buf.push_str(&sql[copied..found.start]);
        buf.push_str(&to_decimal(digits, found.radix));
        copied = found.end;
        i = found.token_end;
    }

    Ok(match out {
        Some(mut buf) => {
            buf.push_str(&sql[copied..]);
            Cow::Owned(buf)
        }
        None => Cow::Borrowed(sql),
    })
}

/// Whether `sql` contains a `0` followed by a radix letter anywhere at all.
///
/// Deliberately approximate and one-sided: it matches `'0x'` inside a string literal and
/// the middle of the identifier `a0x1f`, and all a match costs is one tokenizer pass that
/// then finds nothing to change. What it must never do is miss a real literal, so it looks
/// at the two bytes alone and asks nothing about what precedes them.
fn might_hold_a_non_decimal_literal(sql: &str) -> bool {
    sql.as_bytes()
        .windows(2)
        .any(|w| w[0] == b'0' && radix_of(w[1] as char).is_some())
}

/// The radix a prefix letter selects, or `None` if the character is not one.
fn radix_of(c: char) -> Option<u32> {
    match c {
        'b' | 'B' => Some(2),
        'o' | 'O' => Some(8),
        'x' | 'X' => Some(16),
        _ => None,
    }
}

/// The name PostgreSQL uses for a radix when it reports a prefix with no digits.
fn radix_name(radix: u32) -> &'static str {
    match radix {
        2 => "binary",
        8 => "octal",
        _ => "hexadecimal",
    }
}

/// One non-decimal literal located in the token stream.
struct Located<'a> {
    /// Byte range of the whole literal in the source, prefix included.
    start: usize,
    end: usize,
    /// Index one past the last token the literal spans.
    token_end: usize,
    /// The digit text after the prefix, `_` separators and any junk included.
    digits: &'a str,
    radix: u32,
}

/// Recognize a non-decimal integer literal starting at token `i`, in either of the two
/// shapes sqlparser leaves.
///
/// `Number("0")` followed by an adjacent word is `0b…`, `0o…` and the uppercase `0X…`
/// the tokenizer does not know: the digits are inside the word, separators and all.
/// `HexStringLiteral` is lowercase `0x…` — which the tokenizer reads as SQL's `x'…'`
/// byte-string syntax and therefore also produces for a real `x'1F'`, so the source text
/// is what tells the two apart. Its digits stop at the first `_`, so an adjacent word
/// after it is part of the same literal and is joined back on.
fn literal_at<'a>(
    sql: &'a str,
    tokens: &[TokenWithSpan],
    starts: &[usize],
    i: usize,
) -> Option<Located<'a>> {
    let start = starts[i];
    match &tokens[i].token {
        Token::Number(n, false) if n == "0" => {
            let word = adjacent_word(tokens, i + 1)?;
            let radix = radix_of(word.value.chars().next()?)?;
            Some(Located {
                start,
                end: starts[i + 2],
                token_end: i + 2,
                digits: &sql[start + 2..starts[i + 2]],
                radix,
            })
        }
        Token::HexStringLiteral(_)
            if sql[start..].starts_with("0x") || sql[start..].starts_with("0X") =>
        {
            // A following word continues the digits (`0x1_f`) or is the junk that makes
            // the literal invalid (`0x1f_`); either way it belongs to this literal.
            let token_end = if adjacent_word(tokens, i + 1).is_some() {
                i + 2
            } else {
                i + 1
            };
            let end = starts[token_end];
            Some(Located {
                start,
                end,
                token_end,
                digits: &sql[start + 2..end],
                radix: 16,
            })
        }
        _ => None,
    }
}

/// The unquoted word at token index `i`, or `None` if there is no such token there.
///
/// Adjacency needs no test: whitespace is its own token, so a word *at* `i` is a word
/// touching whatever ended at `i`. A quoted identifier is not a word for this purpose —
/// `0"x1f"` is a value and a quoted alias, not a literal.
fn adjacent_word(tokens: &[TokenWithSpan], i: usize) -> Option<&Word> {
    match tokens.get(i).map(|t| &t.token) {
        Some(Token::Word(word)) if word.quote_style.is_none() => Some(word),
        _ => None,
    }
}

/// Check `digits` against PostgreSQL's rule for the digits of a non-decimal literal and
/// return them, or fail with the message PostgreSQL's scanner would give.
///
/// The rule is `_?D(_?D)*`: at least one digit, at most one `_` between any two of them,
/// one optional leading `_` right after the prefix, and none at the end. `0b_1` is
/// therefore `1` and `0b__1`, `0b1_` and `0x1__f` are all trailing junk — measured
/// against PostgreSQL 17, which is where the asymmetry comes from.
fn validate<'a>(digits: &'a str, radix: u32, text: &str) -> Result<&'a str> {
    if digits.is_empty() {
        return Err(syntax_error(format!(
            "invalid {} integer at or near \"{text}\"",
            radix_name(radix)
        )));
    }
    let body = digits.strip_prefix('_').unwrap_or(digits);
    let well_formed = !body.is_empty()
        && body
            .split('_')
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_digit(radix)));
    if !well_formed {
        return Err(syntax_error(format!(
            "trailing junk after numeric literal at or near \"{text}\""
        )));
    }
    Ok(body)
}

/// Refuse a literal immediately followed by `.`, which PostgreSQL reads as two tokens and
/// rejects.
///
/// Without this the respelling would *create* a valid statement out of an invalid one:
/// `0x1f.5` is a syntax error at `.5` in PostgreSQL, and `31.5` is the number 31.5. The
/// only case where correcting the digits could change a refusal into an answer, so it is
/// the only lookahead here.
fn reject_a_trailing_fraction(
    sql: &str,
    tokens: &[TokenWithSpan],
    starts: &[usize],
    after: usize,
) -> Result<()> {
    // `.5` is one `Number` token to the tokenizer and one snippet in PostgreSQL's message;
    // a `.` followed by anything else is a bare `Period` and quotes as just the dot.
    let end = match tokens.get(after).map(|t| &t.token) {
        Some(Token::Number(n, _)) if n.starts_with('.') => starts[after + 1],
        Some(Token::Period) => match tokens.get(after + 1).map(|t| &t.token) {
            Some(Token::Number(..)) => starts[after + 2],
            _ => starts[after + 1],
        },
        _ => return Ok(()),
    };
    Err(syntax_error(format!(
        "syntax error at or near \"{}\"",
        &sql[starts[after]..end]
    )))
}

/// A `42601` carrying `message` verbatim, the class PostgreSQL gives every malformed
/// literal here.
fn syntax_error(message: String) -> crate::error::CoordinatorError {
    ParserError::TokenizerError(message).into()
}

/// The byte offset in `sql` at which each token starts, with `sql.len()` appended twice
/// so the two lookaheads above can index one and two tokens past the last one.
fn token_start_offsets(sql: &str, tokens: &[TokenWithSpan]) -> Vec<usize> {
    // A `Location` is a line and a *character* column, both 1-based, so the offset is the
    // line's start plus the byte length of the characters before that column. Walking the
    // lines once keeps it linear rather than rescanning the text per token.
    let mut line_starts = vec![0usize];
    line_starts.extend(
        sql.char_indices()
            .filter_map(|(i, c)| (c == '\n').then_some(i + 1)),
    );

    let mut offsets = Vec::with_capacity(tokens.len() + 2);
    for token in tokens {
        let line = token.span.start.line.max(1) as usize - 1;
        let column = token.span.start.column.max(1) as usize - 1;
        let line_start = line_starts.get(line).copied().unwrap_or(sql.len());
        let offset = sql[line_start..]
            .char_indices()
            .nth(column)
            .map(|(i, _)| line_start + i)
            .unwrap_or(sql.len());
        offsets.push(offset);
    }
    offsets.push(sql.len());
    offsets.push(sql.len());
    offsets
}

/// `digits` read in base `radix` and written back in base 10, at whatever length that
/// takes.
///
/// Long multiplication over a little-endian vector of decimal digits: `value = value *
/// radix + digit`, one source digit at a time. `_` separators are skipped and the digits
/// have already been checked, so there is nothing here that can fail.
fn to_decimal(digits: &str, radix: u32) -> String {
    let mut decimal: Vec<u8> = vec![0];
    for digit in digits.chars().filter_map(|c| c.to_digit(radix)) {
        let mut carry = digit;
        for slot in decimal.iter_mut() {
            let scaled = u32::from(*slot) * radix + carry;
            *slot = (scaled % 10) as u8;
            carry = scaled / 10;
        }
        while carry > 0 {
            decimal.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    while decimal.len() > 1 && *decimal.last().unwrap() == 0 {
        decimal.pop();
    }
    decimal.iter().rev().map(|d| char::from(b'0' + d)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(sql: &str) -> String {
        normalize_non_decimal_integers(sql).unwrap().into_owned()
    }

    fn refused(sql: &str) -> String {
        normalize_non_decimal_integers(sql)
            .expect_err(&format!("`{sql}` must be refused"))
            .to_string()
    }

    // The four rows of the table in the module docs, each measured against PostgreSQL 17.
    #[test]
    fn respells_each_radix_as_decimal() {
        assert_eq!(normalized("SELECT 0b101"), "SELECT 5");
        assert_eq!(normalized("SELECT 0o17"), "SELECT 15");
        assert_eq!(normalized("SELECT 0x1F"), "SELECT 31");
        assert_eq!(normalized("SELECT 0X1f"), "SELECT 31");
    }

    // Both letter cases of every prefix, because only lowercase `0x` is a form the
    // tokenizer has an opinion about and the other five arrive by the other shape.
    #[test]
    fn accepts_either_case_of_the_prefix() {
        for (sql, want) in [
            ("SELECT 0b11", "SELECT 3"),
            ("SELECT 0B11", "SELECT 3"),
            ("SELECT 0o10", "SELECT 8"),
            ("SELECT 0O10", "SELECT 8"),
            ("SELECT 0xff", "SELECT 255"),
            ("SELECT 0XFF", "SELECT 255"),
        ] {
            assert_eq!(normalized(sql), want, "`{sql}`");
        }
    }

    // `_` groups digits. It may lead, once, and may not double or trail — PostgreSQL's own
    // asymmetry, reproduced rather than smoothed over.
    #[test]
    fn digit_separators_group_without_changing_the_value() {
        assert_eq!(normalized("SELECT 0b1_0000_0000"), "SELECT 256");
        assert_eq!(normalized("SELECT 0x1_f"), "SELECT 31");
        assert_eq!(normalized("SELECT 0b_1"), "SELECT 1");
        assert_eq!(normalized("SELECT 0x_1f"), "SELECT 31");
        assert_eq!(normalized("SELECT 0o_17"), "SELECT 15");
    }

    // The literal is typed downstream exactly as the decimal spelling is, so the
    // conversion cannot be the thing that decides a width: `bigint`'s largest value comes
    // through as its digits, and so does a value ten times past it.
    #[test]
    fn converts_beyond_every_fixed_width() {
        assert_eq!(
            normalized("SELECT 0x7fffffffffffffff"),
            "SELECT 9223372036854775807"
        );
        assert_eq!(
            normalized("SELECT 0x8000000000000000"),
            "SELECT 9223372036854775808"
        );
        assert_eq!(
            normalized("SELECT 0xffffffffffffffffffffffffffffffff"),
            "SELECT 340282366920938463463374607431768211455"
        );
    }

    #[test]
    fn converts_zero_in_every_radix() {
        assert_eq!(normalized("SELECT 0b0"), "SELECT 0");
        assert_eq!(normalized("SELECT 0o000"), "SELECT 0");
        assert_eq!(normalized("SELECT 0x00"), "SELECT 0");
    }

    // A prefix with no digits at all is PostgreSQL's `invalid … integer`, named per radix.
    #[test]
    fn refuses_a_prefix_with_no_digits() {
        assert!(refused("SELECT 0b").contains("invalid binary integer at or near \"0b\""));
        assert!(refused("SELECT 0o").contains("invalid octal integer at or near \"0o\""));
        assert!(refused("SELECT 0x").contains("invalid hexadecimal integer at or near \"0x\""));
    }

    // Digits that run into something else are trailing junk, quoting the whole of it.
    #[test]
    fn refuses_digits_that_run_into_junk() {
        for (sql, text) in [
            ("SELECT 0b102", "0b102"),
            ("SELECT 0o18", "0o18"),
            ("SELECT 0xG", "0xG"),
            ("SELECT 0x1f_", "0x1f_"),
            ("SELECT 0b1_", "0b1_"),
            ("SELECT 0b__1", "0b__1"),
            ("SELECT 0x1__f", "0x1__f"),
            ("SELECT 0bar", "0bar"),
        ] {
            let msg = refused(sql);
            assert!(
                msg.contains(&format!(
                    "trailing junk after numeric literal at or near \"{text}\""
                )),
                "`{sql}`: {msg}"
            );
        }
    }

    // The one lookahead: without it `0x1f.5` would stop being a syntax error and start
    // being 31.5.
    #[test]
    fn refuses_a_literal_followed_by_a_fraction() {
        assert!(refused("SELECT 0x1f.5").contains("syntax error at or near \".5\""));
        assert!(refused("SELECT 0b1.5").contains("syntax error at or near \".5\""));
        assert!(refused("SELECT 0x1f.a").contains("syntax error at or near \".\""));
    }

    // Every position a literal can occupy, and more than one of them at once.
    #[test]
    fn rewrites_wherever_a_literal_appears() {
        assert_eq!(
            normalized("SELECT 0x10 + 0b1 FROM t WHERE id = 0o7"),
            "SELECT 16 + 1 FROM t WHERE id = 7"
        );
        assert_eq!(
            normalized("INSERT INTO t (a, b) VALUES (0b101, 0x1F)"),
            "INSERT INTO t (a, b) VALUES (5, 31)"
        );
        assert_eq!(normalized("SELECT -0x10"), "SELECT -16");
        assert_eq!(normalized("SELECT 0x1f::text"), "SELECT 31::text");
    }

    // The text a client sent is not the coordinator's to reformat, so anything that is not
    // one of these literals comes back byte-identical — including text that only looks
    // like one.
    #[test]
    fn leaves_everything_else_byte_identical() {
        for sql in [
            "SELECT 1",
            "SELECT 0",
            "SELECT 0.5",
            "SELECT 1_000",
            // A string literal, a comment and an identifier are not numbers.
            "SELECT '0x1f'",
            "SELECT $$0b101$$",
            "SELECT 1 -- 0x1f",
            "SELECT 1 /* 0b101 */",
            "SELECT a0x1f FROM t",
            "SELECT \"0x1f\" FROM t",
            // SQL's own byte-string syntax, which shares a token with `0x…` and must keep
            // meaning bytes.
            "SELECT x'1F'",
            "SELECT X'1F'",
            // A quoted alias touching a zero is a value and an alias, not a literal.
            "SELECT 0\"x1f\"",
            "SELECT 0 AS b101",
        ] {
            assert_eq!(normalized(sql), sql, "`{sql}` must not be rewritten");
        }
    }

    // The guard is what keeps the tokenizer off every statement, so it has to answer yes
    // for each form that needs rewriting and it is allowed to answer yes too often.
    #[test]
    fn the_guard_admits_every_literal_it_must() {
        for sql in ["SELECT 0b1", "SELECT 0O7", "SELECT 0xF", "SELECT 0X1"] {
            assert!(might_hold_a_non_decimal_literal(sql), "`{sql}`");
        }
        assert!(!might_hold_a_non_decimal_literal(
            "SELECT a FROM t WHERE b = 10"
        ));
    }

    // Offsets come from line/column pairs, so a literal after a newline or a multi-byte
    // character has to land on the right byte.
    #[test]
    fn splices_correctly_after_newlines_and_multibyte_text() {
        assert_eq!(
            normalized("SELECT\n  0x1F,\n  0b10\nFROM t"),
            "SELECT\n  31,\n  2\nFROM t"
        );
        assert_eq!(normalized("SELECT 'héllo→', 0x1F"), "SELECT 'héllo→', 31");
        assert_eq!(normalized("SELECT 0x1F, 'héllo→'"), "SELECT 31, 'héllo→'");
    }

    // A statement that does not tokenize is not this pass's to refuse: the real parse says
    // so, against the text the client sent.
    #[test]
    fn leaves_untokenizable_text_to_the_parser() {
        let unterminated = "SELECT 0x1F, 'oops";
        assert_eq!(normalized(unterminated), unterminated);
    }
}
