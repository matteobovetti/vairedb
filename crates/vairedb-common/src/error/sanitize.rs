/// Engine-specific error message prefixes stripped before surfacing to clients.
///
/// The DataFusion block is transcribed from `DataFusionError::error_prefix` in
/// `datafusion-common`, which is the only thing that decides what a
/// `DataFusionError`'s `Display` starts with. It is spelled out in full — including
/// the variants behind cargo features and the two that carry no prefix at all —
/// because a prefix that is *nearly* right silently does nothing: the earlier version
/// of this list stripped `"Plan error: "`, `"Not Implemented: "` and
/// `"Configuration error: "`, none of which DataFusion has emitted since 53, so
/// `"Error during planning: "` reached clients verbatim.
///
/// Note the two spellings of the IO prefix. DataFusion writes `"IO error: "` and
/// DuckDB writes `"IO Error: "`; they differ only in one letter's case and both are
/// live, so neither can be dropped in favour of the other.
const PREFIXES_TO_STRIP: &[&str] = &[
    // DataFusion — see `DataFusionError::error_prefix`
    "Arrow error: ",
    "Parquet error: ",
    "Object Store error: ",
    "IO error: ",
    "SQL error: ",
    "This feature is not implemented: ",
    "Internal error: ",
    "Error during planning: ",
    "Invalid or Unsupported Configuration: ",
    "Schema error: ",
    "Execution error: ",
    "ExecutionJoin error: ",
    "Resources exhausted: ",
    "External error: ",
    "Substrait error: ",
    "FFI error: ",
    // Ballista — see `BallistaError`'s `Display` in `ballista-core`. Every error raised
    // past the scheduler wears one of these, and `"DataFusion error: "` is the one that
    // matters most: Ballista re-wraps a `DataFusionError` under its own name, so a
    // transported error carries the phrase twice with DataFusion's own prefix between.
    // `"Configuration error: "` is here on purpose after being deleted from the
    // DataFusion block as dead — DataFusion has not emitted it since 53, and Ballista
    // still does.
    "Not implemented: ",
    "General error: ",
    "DataFusion error: ",
    "Tonic error: ",
    "Grpc error: ",
    "Grpc connection error: ",
    "Grpc Execute Action error: ",
    "Internal Ballista error: ",
    "Tokio join error: ",
    "Configuration error: ",
    // The Ballista scheduler's own wrapper around a plan it could not deserialize. It
    // describes the transport rather than the statement, and reads to a client as if
    // their SQL failed to parse, which it did not.
    "Could not parse plan: ",
    // DuckDB
    "DuckDB error: ",
    "Catalog Error: ",
    "Parser Error: ",
    "Binder Error: ",
    "Conversion Error: ",
    "IO Error: ",
    "Runtime Error: ",
    "Invalid Input Error: ",
    "Constraint Error: ",
    "Out of Range Error: ",
    // CoreError thiserror prefixes
    "engine error: ",
    "shard not found: ",
    "write conflict: ",
    "type mismatch: ",
    "write queue error: ",
    // Core write queue
    "write execution failed: ",
];

/// The placeholder a `Signature { … }` debug dump collapses to.
const ELIDED_SIGNATURE: &str = "Signature { … }";

/// What a `Signature` debug dump opens with, and the shortest thing worth eliding.
const SIGNATURE_MARKER: &str = "Signature {";

/// Replace every `Signature { … }` debug dump in `msg` with [`ELIDED_SIGNATURE`].
///
/// When argument coercion fails, DataFusion formats the whole candidate signature
/// with `{:?}` into the message — around 1.5 KB of `TypeSignature::OneOf([Exact([…]),
/// …])` for an overloaded function. It reaches the client on *honest* rejections, so
/// the error is the right one and only its presentation is wrong: nobody reads a
/// kilobyte of Rust debug output to learn that a function does not accept `text`.
///
/// Elided by matching braces from the marker rather than by a byte budget, so the
/// message keeps whatever follows the dump — which is often the part that names the
/// function. An unbalanced dump (one truncated upstream before its closing brace)
/// leaves the text alone: there is no end to splice to, and mangling it further would
/// lose the little information it still carries.
fn elide_signature_dumps(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;

    while let Some(start) = rest.find(SIGNATURE_MARKER) {
        // The brace that opens the dump is the last byte of the marker.
        let brace = start + SIGNATURE_MARKER.len() - 1;
        let Some(end) = matching_brace(&rest[brace..]) else {
            break;
        };
        out.push_str(&rest[..start]);
        out.push_str(ELIDED_SIGNATURE);
        rest = &rest[brace + end + 1..];
    }

    out.push_str(rest);
    out
}

/// Byte offset, within `s`, of the `}` closing the `{` at `s[0]` — `None` if unclosed.
///
/// Counts nesting only, and deliberately does not try to respect braces inside string
/// literals: a `Signature` debug dump contains type and volatility names, never
/// arbitrary user text, so there is nothing for a quoted brace to come from.
fn matching_brace(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
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

/// What a tonic `Status` debug dump opens with.
const STATUS_MARKER: &str = "Status {";

/// The one field of a `Status` dump a client could act on. Everything else in it —
/// `code`, `metadata: MetadataMap { headers: { … } }`, `source` — describes the
/// transport.
const STATUS_MESSAGE_FIELD: &str = "message: \"";

/// Replace a tonic `Status { … }` debug dump in `msg` with the text it carries.
///
/// An error raised past the Ballista scheduler arrives as a gRPC status, and the
/// coordinator's classifier sees it as `DataFusionError::External` — whose prefix strips
/// cleanly, leaving the whole `Debug` rendering of the status for the client to read:
///
/// ```text
/// Status { code: InvalidArgument, message: "Could not parse plan: …", metadata:
/// MetadataMap { headers: {"content-type": "application/grpc", "date": "…"} }, source: None }
/// ```
///
/// Nothing in that is for a client. The `message` field is extracted and unescaped, and
/// the rest of the dump dropped; the surrounding text, if any, is kept.
///
/// Left alone rather than half-cleaned when the shape is not the expected one — no
/// `message` field, or a message literal or brace that does not close. A dump truncated
/// upstream still carries something, and mangling it further would lose that too.
fn unwrap_grpc_status(msg: &str) -> String {
    let Some(start) = msg.find(STATUS_MARKER) else {
        return msg.to_string();
    };
    // The brace that opens the dump is the last byte of the marker.
    let brace = start + STATUS_MARKER.len() - 1;
    let Some(field) = msg[brace..].find(STATUS_MESSAGE_FIELD) else {
        return msg.to_string();
    };
    let literal = brace + field + STATUS_MESSAGE_FIELD.len();
    let Some(len) = string_literal_end(&msg[literal..]) else {
        return msg.to_string();
    };
    let Some(end) = matching_brace_outside_strings(&msg[brace..]) else {
        return msg.to_string();
    };
    format!(
        "{}{}{}",
        &msg[..start],
        unescape(&msg[literal..literal + len]),
        &msg[brace + end + 1..]
    )
}

/// Byte offset, within `s`, of the `}` closing the `{` at `s[0]`, ignoring braces that
/// fall inside a string literal — `None` if unclosed.
///
/// Unlike [`matching_brace`], this one has to respect quoting: a `Status` dump's `message`
/// field carries arbitrary text, so a brace in a client's own SQL would otherwise close
/// the dump early and splice the transport's `metadata` back into the reply.
fn matching_brace_outside_strings(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '{' if !in_string => depth += 1,
            '}' if !in_string => {
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
fn string_literal_end(s: &str) -> Option<usize> {
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
fn unescape(s: &str) -> String {
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

/// Unwrap a whole-message Rust `Debug` tuple-variant dump — `Plan("…")`,
/// `NotImplemented("…")` — into the string it wraps.
///
/// This is what an error that crossed the scheduler's gRPC surface looks like once its
/// prefixes are gone: the variant name is *text* by then, because the error was rendered
/// with `{:?}` somewhere on the way, so nothing typed is being discarded here. Only a
/// single-field form is unwrapped, which is deliberately conservative — a multi-field dump
/// such as `SchemaError(FieldNotFound { … }, Some(""))` is not a message, and its first
/// field alone would read as if it were the whole error.
fn unwrap_debug_wrapper(msg: &str) -> Option<String> {
    let open = msg.find('(')?;
    let name = &msg[..open];
    if !name.starts_with(|c: char| c.is_ascii_uppercase())
        || !name.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return None;
    }
    let inner = msg[open + 1..].strip_suffix(')')?.strip_prefix('"')?;
    let len = string_literal_end(inner)?;
    // The literal has to be the whole payload: `len` addresses its closing quote, which
    // must be the last byte left.
    if len + 1 != inner.len() {
        return None;
    }
    Some(unescape(&inner[..len]))
}

/// Strip every known engine prefix from the front of `msg`, repeatedly.
fn strip_known_prefixes(msg: &str) -> String {
    let mut msg = msg.to_string();
    loop {
        let before = msg.len();
        for prefix in PREFIXES_TO_STRIP {
            if let Some(stripped) = msg.strip_prefix(prefix) {
                msg = stripped.to_string();
            }
        }
        if msg.len() == before {
            return msg;
        }
    }
}

/// How many times [`sanitize_message`] will peel a layer off a transported error.
///
/// Bounded rather than run to a fixed point: the deepest shape measured is four layers
/// (`Status` → prefixes → `Plan("…")` → prefixes → `NotImplemented("…")`), and a bound
/// means a pass that does not shorten the message cannot loop.
const MAX_UNWRAP_PASSES: usize = 8;

/// Scrub an internal error message into a client-safe string.
///
/// Repeatedly unwraps the layers a transported error arrives in — a gRPC
/// `Status { … }` dump, known engine prefixes (DataFusion, Ballista, DuckDB, `CoreError`),
/// and whole-message `Debug` variant dumps — collapses `Signature { … }` debug dumps,
/// unwraps Ballista "failed on executor" wrappers, and replaces any `http://` URLs with
/// `[node]` so node addresses never leak to clients.
///
/// The layers alternate rather than nest neatly, which is why this is a loop and not a
/// sequence: unwrapping a `Status` exposes a prefix, stripping the prefix exposes a
/// `Debug` dump, and unwrapping that exposes another prefix.
pub fn sanitize_message(raw: &str) -> String {
    let mut msg = raw.to_string();

    for _ in 0..MAX_UNWRAP_PASSES {
        let before = msg.clone();
        msg = unwrap_grpc_status(&msg);
        msg = elide_signature_dumps(&msg);
        msg = strip_known_prefixes(&msg);
        if let Some(inner) = unwrap_debug_wrapper(&msg) {
            msg = inner;
        }
        if msg == before {
            break;
        }
    }

    // Strip Ballista executor task failure patterns containing URLs
    if let Some(idx) = msg.find("failed on executor")
        && let Some(colon_idx) = msg[idx..].find(": ")
    {
        msg = msg[idx + colon_idx + 2..].to_string();
    }

    // Scrub any remaining http:// URLs (executor addresses, node endpoints)
    while let Some(start) = msg.find("http://") {
        let end = msg[start..]
            .find(|c: char| c.is_whitespace())
            .map(|i| start + i)
            .unwrap_or(msg.len());
        msg = format!("{}[node]{}", &msg[..start], &msg[end..]);
    }

    msg
}

#[cfg(test)]
mod tests {
    use super::sanitize_message;

    #[test]
    fn test_sanitize_strips_core_error_prefixes() {
        let msg =
            "engine error: write execution failed: IO Error: /data/core.duckdb: Permission denied";
        let result = sanitize_message(msg);
        assert!(!result.contains("engine error"));
        assert!(!result.contains("write execution failed"));
        assert!(!result.contains("IO Error"));
    }

    #[test]
    fn test_sanitize_strips_ballista_executor_url() {
        let msg = "Task 3 failed on executor http://192.168.1.5:50051: query failed on shard 'orders_shard0'";
        let result = sanitize_message(msg);
        assert!(!result.contains("192.168.1.5"));
        assert!(!result.contains("http://"));
        assert!(result.contains("query failed on shard"));
    }

    #[test]
    fn test_sanitize_strips_http_urls() {
        let msg = "connection to http://10.0.0.1:50041 failed";
        let result = sanitize_message(msg);
        assert!(!result.contains("10.0.0.1"));
        assert!(!result.contains("http://"));
    }

    /// One case per DataFusion prefix, because the defect this replaced was a list of
    /// prefixes that all looked plausible and several of which matched nothing.
    #[test]
    fn every_datafusion_prefix_is_the_one_datafusion_emits() {
        for prefix in [
            "Arrow error: ",
            "Parquet error: ",
            "Object Store error: ",
            "IO error: ",
            "SQL error: ",
            "This feature is not implemented: ",
            "Internal error: ",
            "Error during planning: ",
            "Invalid or Unsupported Configuration: ",
            "Schema error: ",
            "Execution error: ",
            "ExecutionJoin error: ",
            "Resources exhausted: ",
            "External error: ",
            "Substrait error: ",
            "FFI error: ",
        ] {
            let result = sanitize_message(&format!("{prefix}the part a client should read"));
            assert_eq!(
                result, "the part a client should read",
                "prefix {prefix:?} was not stripped"
            );
        }
    }

    /// The two IO spellings differ only in one letter's case, and both are live.
    #[test]
    fn both_io_prefix_spellings_are_stripped() {
        assert_eq!(
            sanitize_message("IO error: datafusion side"),
            "datafusion side"
        );
        assert_eq!(sanitize_message("IO Error: duckdb side"), "duckdb side");
    }

    #[test]
    fn a_signature_dump_is_elided_and_the_rest_of_the_message_kept() {
        let msg = "Failed to coerce arguments to satisfy a call to 'lpad': Signature { \
                   type_signature: OneOf([Exact([Utf8, Int64]), Exact([Utf8, Int64, Utf8])]), \
                   volatility: Immutable } failed";
        let result = sanitize_message(msg);
        assert!(!result.contains("type_signature"), "{result}");
        assert!(!result.contains("Immutable"), "{result}");
        assert!(result.contains("Signature { … }"), "{result}");
        assert!(result.contains("a call to 'lpad'"), "{result}");
        assert!(result.ends_with(" failed"), "{result}");
    }

    #[test]
    fn two_signature_dumps_are_both_elided() {
        let msg = "Signature { volatility: Immutable } and Signature { volatility: Stable } differ";
        assert_eq!(
            sanitize_message(msg),
            "Signature { … } and Signature { … } differ"
        );
    }

    /// A dump truncated before its closing brace is left alone rather than mangled.
    #[test]
    fn an_unbalanced_signature_dump_is_left_alone() {
        let msg = "coercion failed: Signature { type_signature: OneOf([Exact([Utf8";
        assert_eq!(sanitize_message(msg), msg);
    }

    #[test]
    fn a_message_with_no_signature_dump_is_unchanged() {
        assert_eq!(
            sanitize_message("column \"nope\" does not exist"),
            "column \"nope\" does not exist"
        );
    }

    /// The measured shape, copied from what `psql` printed for
    /// `SELECT format_type(oid, NULL) FROM (VALUES (23),(25)) t(oid)` before the
    /// `pg_catalog` scalar functions were registered on the scheduler.
    #[test]
    fn a_grpc_status_dump_is_reduced_to_the_message_it_carries() {
        let msg = "Status { code: InvalidArgument, message: \"Could not parse plan: \
                   DataFusion error: Error during planning: DataFusion error: \
                   Plan(\\\"DataFusion error: NotImplemented(\\\\\\\"LogicalExtensionCodec is \
                   not provided for scalar function format_type\\\\\\\")\\\")\", metadata: \
                   MetadataMap { headers: {\"content-type\": \"application/grpc\", \"date\": \
                   \"Wed, 09 Sep 2026 11:42:51 GMT\"} }, source: None }";
        assert_eq!(
            sanitize_message(msg),
            "LogicalExtensionCodec is not provided for scalar function format_type"
        );
    }

    /// Each field of the dump names something about the transport, and none of it is a
    /// client's business — the point of the test is the absence, not the wording.
    #[test]
    fn a_grpc_status_dump_leaks_none_of_its_transport_fields() {
        let msg = "External error: Status { code: Internal, message: \"stage 2 failed\", \
                   metadata: MetadataMap { headers: {\"content-type\": \"application/grpc\"} }, \
                   source: None }";
        let result = sanitize_message(msg);
        assert_eq!(result, "stage 2 failed", "{result}");
        for leaked in ["Status {", "code:", "MetadataMap", "grpc", "source:"] {
            assert!(!result.contains(leaked), "{leaked} survived: {result}");
        }
    }

    /// A brace in the client's own statement must not end the dump early, or the
    /// transport's metadata comes back spliced onto the message.
    #[test]
    fn a_brace_inside_the_status_message_does_not_end_the_dump() {
        let msg = "Status { code: Internal, message: \"bad json {a\", metadata: \
                   MetadataMap { headers: {} }, source: None }";
        assert_eq!(sanitize_message(msg), "bad json {a");
    }

    /// Text on either side of the dump is the caller's own framing and is kept.
    #[test]
    fn text_around_a_status_dump_is_kept() {
        let msg = "node execution failed: Status { code: Internal, message: \"shard 0 is \
                   gone\", metadata: MetadataMap { headers: {} }, source: None } (retryable)";
        assert_eq!(
            sanitize_message(msg),
            "node execution failed: shard 0 is gone (retryable)"
        );
    }

    /// A dump with no `message` field, or one truncated before the field closes, still
    /// carries something; half-cleaning it would lose that too.
    #[test]
    fn a_status_dump_that_is_not_the_expected_shape_is_left_alone() {
        for msg in [
            "Status { code: Internal, source: None }",
            "Status { code: Internal, message: \"unterminated",
            "Status { code: Internal, message: \"fine\", metadata: MetadataMap { headers: {}",
        ] {
            assert_eq!(sanitize_message(msg), msg, "{msg}");
        }
    }

    /// One case per Ballista prefix. `"Configuration error: "` is the interesting one: it
    /// was deleted from the DataFusion block as dead and is live here.
    #[test]
    fn every_ballista_prefix_is_stripped() {
        for prefix in [
            "Not implemented: ",
            "General error: ",
            "DataFusion error: ",
            "Tonic error: ",
            "Grpc error: ",
            "Grpc connection error: ",
            "Grpc Execute Action error: ",
            "Internal Ballista error: ",
            "Tokio join error: ",
            "Configuration error: ",
            "Could not parse plan: ",
        ] {
            let result = sanitize_message(&format!("{prefix}the part a client should read"));
            assert_eq!(
                result, "the part a client should read",
                "prefix {prefix:?} was not stripped"
            );
        }
    }

    /// Ballista re-wraps a `DataFusionError` under its own name, so the phrase arrives
    /// twice with DataFusion's own prefix between the two.
    #[test]
    fn the_doubled_ballista_and_datafusion_prefixes_both_go() {
        assert_eq!(
            sanitize_message(
                "DataFusion error: Error during planning: DataFusion error: \
                 Plan(\"table 'nope' not found\")"
            ),
            "table 'nope' not found"
        );
    }

    /// A `Debug` variant dump with more than one field is not a message: unwrapping it
    /// would present its first field as the whole error.
    #[test]
    fn a_multi_field_debug_dump_is_not_unwrapped() {
        let msg = "SchemaError(FieldNotFound { field: \"x\" }, Some(\"\"))";
        assert_eq!(sanitize_message(msg), msg);
    }

    /// Nothing about an ordinary message should survive a trip through the unwrapper
    /// differently — parentheses in a client's own text are not a `Debug` dump.
    #[test]
    fn a_message_with_ordinary_parentheses_is_unchanged() {
        for msg in [
            "function count(bigint) does not exist",
            "COPY (SELECT 1) TO STDOUT is not supported here",
        ] {
            assert_eq!(sanitize_message(msg), msg, "{msg}");
        }
    }

    #[test]
    fn test_sanitize_strips_constraint_error_prefix() {
        let msg = "Constraint Error: NOT NULL constraint failed for column 'id'";
        let result = sanitize_message(msg);
        assert!(!result.contains("Constraint Error"));
        assert!(result.contains("NOT NULL constraint failed"));
    }
}
