//! Reading a Rust `Debug` rendering back out of the text a transported failure arrives as.
//!
//! Both halves of this module's work — recovering the variant an error was raised as
//! ([`super::transported`]) and scrubbing the text before a client sees it
//! ([`super::sanitize`]) — come down to the same two questions about a rendering that has
//! become plain text: where does this nested structure end, and what did the escaping hide?
//! They are answered here once so the two sides cannot drift apart, and so neither has to
//! depend on the other for them.

/// How many layers of transport rendering either side will peel off.
///
/// Bounded rather than run to a fixed point, so no input can make a peeling loop spin: a
/// pass that does not shorten what it was given still costs one of the eight.
///
/// Eight is twice the deepest shape measured on either side, and the two sides count
/// different layers. [`super::sanitize::sanitize_message`] measured four — a `Status { … }`
/// dump, known engine prefixes, `Plan("…")`, prefixes again, `NotImplemented("…")`. And
/// [`super::transported::recover_transported_error`] measured four of its own — a job
/// wrapper, a task wrapper, `Execution("…")`, and an `ArrowError(…)` inside it.
pub(super) const MAX_TRANSPORT_LAYERS: usize = 8;

/// Whether a nesting scan reads a quoted string as opaque text or as more of the rendering.
///
/// This is the one axis on which the scans genuinely differ, and both settings are load
/// bearing — see [`end_of_nesting`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Quoting {
    /// Quotes carry no meaning: every delimiter counts, wherever it sits.
    Ignored,
    /// A delimiter inside a string literal is that string's own text, not structure.
    Respected,
}

/// A matched pair of delimiters a `Debug` rendering nests its output inside.
#[derive(Debug, Clone, Copy)]
pub(super) struct DelimiterPair {
    open: char,
    close: char,
}

/// The braces of a struct-shaped `Debug` rendering — `Signature { … }`, `Status { … }`.
pub(super) const BRACES: DelimiterPair = DelimiterPair {
    open: '{',
    close: '}',
};

/// The parentheses of a tuple-variant `Debug` rendering — `ArrowError(…)`, `Plan(…)`.
pub(super) const PARENS: DelimiterPair = DelimiterPair {
    open: '(',
    close: ')',
};

/// Byte offset, within `after_open`, of the delimiter closing an already-consumed opening
/// one — `None` if it never closes.
///
/// `after_open` starts *past* the opening delimiter, so the scan begins one level deep and
/// the offset it returns is relative to that point.
///
/// [`Quoting`] decides whether a delimiter inside a string literal counts, and each caller
/// needs a different answer:
///
/// - A tonic `Status { … }` dump and a `Debug` tuple variant both carry arbitrary text in a
///   quoted field, so a brace or parenthesis in a client's own SQL would close the
///   structure early and splice the transport's `metadata` — or the rest of the rendering —
///   back into the reply. Those scans must pass [`Quoting::Respected`].
/// - A `Signature { … }` dump carries type and volatility names and nothing else, so it has
///   no quoted text of its own to protect. It must pass [`Quoting::Ignored`], and not
///   merely may: a signature dump is often reached *through* another rendering, so the few
///   quotes it can contain (a `Timestamp`'s timezone, a struct field's name) arrive escaped
///   as `\"`. A quoting-aware scan would read one of those as an unterminated string, find
///   no closing brace, and leave the whole kilobyte-long dump in the message — which is the
///   failure it was elided to prevent.
pub(super) fn end_of_nesting(
    after_open: &str,
    pair: DelimiterPair,
    quoting: Quoting,
) -> Option<usize> {
    let mut depth = 1usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, c) in after_open.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_string => escaped = true,
            '"' if quoting == Quoting::Respected => in_string = !in_string,
            _ if in_string => {}
            _ if c == pair.open => depth += 1,
            _ if c == pair.close => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Byte offset, within `s`, of the `"` that closes a string literal starting at `s[0]` —
/// `None` if it never closes. Counts backslash escapes, since the literal being scanned
/// is a `Debug` rendering and its own quotes arrive as `\"`.
pub(super) fn string_literal_end(s: &str) -> Option<usize> {
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '"' => return Some(i),
            _ => {}
        }
    }
    None
}

/// Undo one level of Rust `Debug` string escaping.
///
/// A transported error can be nested several `Debug` renderings deep, so its innermost
/// text arrives with `\\\"`-style escaping. One level per pass is right: the caller loops.
pub(super) fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            // `\\`, `\"` and anything else stand for the character itself.
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nesting is counted, not just the first close, and the offset is relative to the
    /// text handed in — which starts past the opening delimiter.
    #[test]
    fn a_nested_structure_ends_at_its_own_closing_delimiter() {
        let after_open = " a: { b: 1 }, c: 2 } and the rest";
        let end = end_of_nesting(after_open, BRACES, Quoting::Ignored).expect("a closing brace");
        assert_eq!(&after_open[..end], " a: { b: 1 }, c: 2 ");
        assert_eq!(&after_open[end + 1..], " and the rest");
    }

    /// A structure truncated upstream never closes, and the caller is told so rather than
    /// handed an offset that happens to parse.
    #[test]
    fn a_structure_that_never_closes_has_no_end() {
        assert_eq!(end_of_nesting("a: { b: 1", BRACES, Quoting::Ignored), None);
        assert_eq!(
            end_of_nesting("Plan(\"x\"", PARENS, Quoting::Respected),
            None
        );
    }

    /// The axis the two settings exist for: the same input ends in two different places
    /// depending on whether a quoted brace is text or structure.
    #[test]
    fn quoting_decides_whether_a_delimiter_in_a_string_counts() {
        let after_open = " message: \"bad json {a\", source: None } tail";
        let respected =
            end_of_nesting(after_open, BRACES, Quoting::Respected).expect("the real close");
        assert_eq!(&after_open[respected..], "} tail");

        // Ignoring quotes, the `{` inside the message opens a level that the dump's own
        // closing brace then merely closes — so the scan runs past it.
        let ignored = end_of_nesting(after_open, BRACES, Quoting::Ignored);
        assert_ne!(ignored, Some(respected));
    }

    /// Why [`Quoting::Ignored`] is a requirement and not a shortcut: reached through
    /// another rendering, a dump's own quotes arrive escaped, and a quoting-aware scan
    /// would read the first of them as a string that never ends.
    #[test]
    fn an_escaped_quote_does_not_hide_the_end_of_a_signature_dump() {
        let after_open =
            " type_signature: Exact([Timestamp(Nanosecond, Some(\\\"UTC\\\"))]) } tail";
        let end = end_of_nesting(after_open, BRACES, Quoting::Ignored).expect("a closing brace");
        assert_eq!(&after_open[end..], "} tail");
        assert_eq!(end_of_nesting(after_open, BRACES, Quoting::Respected), None);
    }

    /// A literal ends at its first unescaped quote — the escaped ones are its own text.
    #[test]
    fn a_string_literal_ends_at_the_first_quote_that_is_not_escaped() {
        let literal = "invalid hexadecimal digit: \\\"z\\\"\", metadata: …";
        let end = string_literal_end(literal).expect("a closing quote");
        assert_eq!(&literal[..end], "invalid hexadecimal digit: \\\"z\\\"");
        assert_eq!(string_literal_end("unterminated"), None);
    }

    /// One level per call, because the caller loops: a doubly-rendered payload needs two.
    #[test]
    fn unescaping_undoes_exactly_one_level_of_debug_escaping() {
        assert_eq!(unescape("a \\\"quoted\\\" word"), "a \"quoted\" word");
        assert_eq!(unescape("line\\nbreak\\tand tab"), "line\nbreak\tand tab");
        // A trailing backslash has nothing to escape and stands for itself.
        assert_eq!(unescape("ends with \\"), "ends with \\");
    }
}
