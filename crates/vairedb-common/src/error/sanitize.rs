//! Scrubbing an internal error message into something a client may see.
//!
//! Everything a failure picked up on its way here — the engine that raised it, the
//! transport that carried it, the addresses of the nodes it passed through — is detail the
//! client did not ask for and must not be told. What is left after
//! [`sanitize_message`] should read as the sentence PostgreSQL would have written.

use super::scan::{
    BRACES, MAX_TRANSPORT_LAYERS, Quoting, end_of_nesting, string_literal_end, unescape,
};
use super::tag::strip_code_tags;

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
///
/// The scan ignores quoting, which for this dump is a requirement rather than a
/// simplification — see [`end_of_nesting`].
fn elide_signature_dumps(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;

    while let Some(start) = rest.find(SIGNATURE_MARKER) {
        // The brace that opens the dump is the last byte of the marker.
        let brace = start + SIGNATURE_MARKER.len() - 1;
        let Some(end) = end_of_nesting(&rest[brace + 1..], BRACES, Quoting::Ignored) else {
            break;
        };
        let close = brace + 1 + end;
        out.push_str(&rest[..start]);
        out.push_str(ELIDED_SIGNATURE);
        rest = &rest[close + 1..];
    }

    out.push_str(rest);
    out
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
    grpc_status_message(msg).unwrap_or_else(|| msg.to_string())
}

/// `msg` with the `Status { … }` dump it contains replaced by that dump's `message`
/// field, or `None` if `msg` does not hold a complete one.
///
/// Every step is a shape the dump has to have, so a missing one answers `None` and
/// [`unwrap_grpc_status`] keeps the text it was given.
fn grpc_status_message(msg: &str) -> Option<String> {
    let start = msg.find(STATUS_MARKER)?;
    // The brace that opens the dump is the last byte of the marker.
    let brace = start + STATUS_MARKER.len() - 1;
    let field = brace + msg[brace..].find(STATUS_MESSAGE_FIELD)? + STATUS_MESSAGE_FIELD.len();
    let len = string_literal_end(&msg[field..])?;
    // Quoting has to be respected here: the `message` field carries arbitrary text, so a
    // brace in a client's own SQL would otherwise close the dump early and splice the
    // transport's `metadata` back into the reply.
    let close = brace + 1 + end_of_nesting(&msg[brace + 1..], BRACES, Quoting::Respected)?;
    Some(format!(
        "{}{}{}",
        &msg[..start],
        unescape(&msg[field..field + len]),
        &msg[close + 1..]
    ))
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
    let mut rest = msg;
    loop {
        let before = rest.len();
        for prefix in PREFIXES_TO_STRIP {
            rest = rest.strip_prefix(prefix).unwrap_or(rest);
        }
        if rest.len() == before {
            return rest.to_string();
        }
    }
}

/// Scrub an internal error message into a client-safe string.
///
/// Repeatedly unwraps the layers a transported error arrives in — a gRPC
/// `Status { … }` dump, known engine prefixes (DataFusion, Ballista, DuckDB, `CoreError`),
/// `[VDB-…]` transport tags, and whole-message `Debug` variant dumps — collapses
/// `Signature { … }` debug dumps, unwraps Ballista "failed on executor" wrappers, and
/// replaces any `http://` URLs with `[node]` so node addresses never leak to clients.
///
/// The layers alternate rather than nest neatly, which is why this is a loop and not a
/// sequence: unwrapping a `Status` exposes a prefix, stripping the prefix exposes a
/// `Debug` dump, and unwrapping that exposes another prefix.
pub fn sanitize_message(raw: &str) -> String {
    let mut msg = raw.to_string();

    for _ in 0..MAX_TRANSPORT_LAYERS {
        let unwrapped = unwrap_transport_layer(&msg);
        if unwrapped == msg {
            break;
        }
        msg = unwrapped;
    }

    scrub_node_addresses(&strip_executor_wrapper(&msg))
}

/// Take one layer of transport rendering off `msg`.
///
/// The order within a pass is the order the layers were applied in, outermost first: a
/// `Status` dump wraps a prefixed message, which wraps a `Debug` variant dump.
fn unwrap_transport_layer(msg: &str) -> String {
    let msg = unwrap_grpc_status(msg);
    let msg = elide_signature_dumps(&msg);
    let msg = strip_known_prefixes(&msg);
    // A `[VDB-…]` tag is how an error raised on an executor carries its code across the
    // scheduler (see `super::tag`). The code is read off the raw text before sanitizing;
    // the tag itself is transport, and the coordinator attaches exactly one of its own
    // when it formats the reply.
    let msg = strip_code_tags(&msg);
    match unwrap_debug_wrapper(&msg) {
        Some(inner) => inner,
        None => msg,
    }
}

/// Drop Ballista's "failed on executor <address>" framing, keeping the failure it frames.
///
/// The wrapper names the node the task ran on, which is cluster topology rather than
/// anything a client can act on, and the statement failed for the reason that follows it.
fn strip_executor_wrapper(msg: &str) -> String {
    let Some(idx) = msg.find("failed on executor") else {
        return msg.to_string();
    };
    let Some(colon_idx) = msg[idx..].find(": ") else {
        return msg.to_string();
    };
    msg[idx + colon_idx + 2..].to_string()
}

/// Replace every `http://…` URL in `msg` with `[node]`.
///
/// The last line of defence for the one detail that is never a client's business however
/// it got into the message: the address of an executor, a node endpoint, or the scheduler.
fn scrub_node_addresses(msg: &str) -> String {
    let mut msg = msg.to_string();
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
    use super::*;

    /// The DataFusion block of [`PREFIXES_TO_STRIP`], transcribed a second time from
    /// `DataFusionError::error_prefix` so the test is an independent statement of what
    /// DataFusion emits rather than a reading of the list under test. The defect this
    /// replaced was a list of prefixes that all looked plausible and several of which
    /// matched nothing.
    const DATAFUSION_PREFIXES: &[&str] = &[
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
    ];

    /// The Ballista block. `"Configuration error: "` is the interesting one: it was
    /// deleted from the DataFusion block as dead and is live here.
    const BALLISTA_PREFIXES: &[&str] = &[
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
    ];

    /// The DuckDB block, from the exception names DuckDB renders. `"IO Error: "` is the
    /// one to read twice: DataFusion's spelling of the same prefix differs only in one
    /// letter's case, both are live, and each is tested against its own engine's list.
    const DUCKDB_PREFIXES: &[&str] = &[
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
    ];

    /// VaireDB's own core-node prefixes, which `thiserror` writes from `CoreError`'s
    /// `#[error("…")]` attributes and the write queue adds one more to.
    const CORE_PREFIXES: &[&str] = &[
        "engine error: ",
        "shard not found: ",
        "write conflict: ",
        "type mismatch: ",
        "write queue error: ",
        "write execution failed: ",
    ];

    /// Every prefix, from every engine, leaves nothing of itself behind.
    #[test]
    fn every_prefix_an_engine_emits_is_stripped() {
        for prefix in DATAFUSION_PREFIXES
            .iter()
            .chain(BALLISTA_PREFIXES)
            .chain(DUCKDB_PREFIXES)
            .chain(CORE_PREFIXES)
        {
            let result = sanitize_message(&format!("{prefix}the part a client should read"));
            assert_eq!(
                result, "the part a client should read",
                "prefix {prefix:?} was not stripped"
            );
        }
    }

    /// The list under test and the transcriptions above must be the same set. A prefix in
    /// production and not in a transcription is one nobody has checked against the engine
    /// that supposedly emits it — which is exactly how the dead `"Plan error: "` survived.
    /// A prefix in a transcription and not in production is one that reaches clients.
    #[test]
    fn the_transcribed_prefixes_and_the_stripped_prefixes_are_the_same_set() {
        let transcribed: Vec<&&str> = DATAFUSION_PREFIXES
            .iter()
            .chain(BALLISTA_PREFIXES)
            .chain(DUCKDB_PREFIXES)
            .chain(CORE_PREFIXES)
            .collect();
        for prefix in PREFIXES_TO_STRIP {
            assert!(
                transcribed.contains(&prefix),
                "{prefix:?} is stripped in production but no test transcribes it"
            );
        }
        for prefix in transcribed {
            assert!(
                PREFIXES_TO_STRIP.contains(prefix),
                "{prefix:?} is transcribed as live but production does not strip it"
            );
        }
    }

    /// The prefixes arrive stacked, from three different layers, in one message.
    #[test]
    fn a_stack_of_core_and_duckdb_prefixes_is_stripped_down_to_the_message() {
        assert_eq!(
            sanitize_message(
                "engine error: write execution failed: IO Error: /data/core.duckdb: \
                 Permission denied"
            ),
            "/data/core.duckdb: Permission denied"
        );
    }

    /// Ballista names the executor a task failed on, address and all. The client gets the
    /// failure and none of the topology.
    #[test]
    fn a_ballista_executor_address_never_reaches_the_client() {
        let result = sanitize_message(
            "Task 3 failed on executor http://192.168.1.5:50051: query failed on shard \
             'orders_shard0'",
        );
        assert_eq!(result, "query failed on shard 'orders_shard0'");
        assert!(!result.contains("192.168.1.5"), "{result}");
        assert!(!result.contains("http://"), "{result}");
    }

    /// A node address in a message that wears no wrapper at all is still an address.
    #[test]
    fn a_node_url_anywhere_in_a_message_is_replaced_by_a_placeholder() {
        assert_eq!(
            sanitize_message("connection to http://10.0.0.1:50041 failed"),
            "connection to [node] failed"
        );
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

    /// A signature dump reached through another `Debug` rendering carries its own quotes
    /// escaped — a timezone, a struct field's name — and the dump still has to go. This is
    /// the case that decides the quoting rule in [`elide_signature_dumps`]: read as a
    /// string literal, the `\"` below would swallow the rest of the dump and the whole
    /// kilobyte would reach the client.
    #[test]
    fn a_signature_dump_containing_escaped_quotes_is_still_elided() {
        let msg = "Failed to coerce arguments to satisfy a call to 'date_trunc': Signature { \
                   type_signature: Exact([Utf8, Timestamp(Nanosecond, Some(\\\"UTC\\\"))]), \
                   volatility: Immutable } failed";
        let result = sanitize_message(msg);
        assert_eq!(
            result,
            "Failed to coerce arguments to satisfy a call to 'date_trunc': Signature { … } failed"
        );
        for leaked in ["type_signature", "Nanosecond", "Immutable"] {
            assert!(!result.contains(leaked), "{leaked} survived: {result}");
        }
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

    /// The other half of the contract: a message that is already the sentence PostgreSQL
    /// would have written comes back byte for byte. Parentheses in a client's own text are
    /// not a `Debug` dump, and a brace is not a debug dump either.
    #[test]
    fn a_message_a_client_should_read_is_returned_unchanged() {
        for msg in [
            "column \"nope\" does not exist",
            "function count(bigint) does not exist",
            "COPY (SELECT 1) TO STDOUT is not supported here",
        ] {
            assert_eq!(sanitize_message(msg), msg, "{msg}");
        }
    }

    /// A `[VDB-…]` tag is how an error raised on an executor carries its code home. It is
    /// transport, so it must not reach the client — the coordinator attaches exactly one
    /// tag of its own when it formats the reply, and a surviving one would read as a
    /// second code in the middle of the sentence.
    #[test]
    fn a_transport_code_tag_never_reaches_the_client() {
        assert_eq!(
            sanitize_message(
                "DataFusion error: Error during planning: DataFusion error: \
                 Plan(\"[VDB-1004] percentile_disc with an array of fractions is not supported\")"
            ),
            "percentile_disc with an array of fractions is not supported"
        );
    }

    /// `[VDB-…]` that is not a tag belongs to whoever wrote it.
    #[test]
    fn something_shaped_like_a_tag_but_not_one_is_kept() {
        assert_eq!(
            sanitize_message("column \"[VDB-x]\" does not exist"),
            "column \"[VDB-x]\" does not exist"
        );
    }
}
