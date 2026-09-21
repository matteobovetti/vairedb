//! Carrying a structured error code across the Ballista scheduler boundary, and reading
//! one back out of the text a transported failure arrives as.
//!
//! An error raised inside a Ballista executor never reaches the coordinator as a value.
//! The executor hands its failure to the scheduler as a `FailedTask`, whose only payload
//! is a `String`, and the scheduler formats the whole thing again on the way out. By the
//! time the coordinator sees it there is no `DataFusionError` left to match on — which is
//! why every failure raised on an executor used to land `XX000 internal_error`, the one
//! class that tells a client the *server* broke and the statement is worth retrying.
//!
//! Two things survive that trip, and this module is built on both.
//!
//! ## 1. A tag VaireDB puts in the message itself
//!
//! [`tagged_message`] writes a `[VDB-<code>]` prefix into an error VaireDB raises on an
//! executor, and [`code_of_tagged_message`] reads it back. That is the *structured* half:
//! the code is chosen where the error is raised, by the code that knows what went wrong,
//! rather than guessed at the other end from wording. A guard added to a UDF or UDAF gets
//! the right SQLSTATE by tagging its message and nothing else.
//!
//! The tag is a transport artifact and never reaches a client:
//! [`crate::error::sanitize_message`] strips it, and the coordinator re-attaches exactly
//! one `[VDB-…]` of its own when it formats the reply.
//!
//! ## 2. The variant name, still spelled out in the rendered text
//!
//! DataFusion's own errors cannot be tagged, but nothing is actually *lost* when one is
//! rendered — the variant is still there, as text. Ballista renders a failed task with
//! `{:?}` and a failed job with `{}`, so a transported `DataFusionError` arrives in one of
//! two spellings, and both name the variant:
//!
//! ```text
//! Job <id> failed: Job failed due to stage 1 failed: Task failed due to runtime
//!   execution error: DataFusionError(Plan("…"))          ← Debug: the constructor
//! Job <id> failed: DataFusion error: This feature is not implemented: …
//!                                                        ← Display: `error_prefix`
//! ```
//!
//! [`recover_transported_error`] reads either one back into a [`TransportedVariant`] and
//! the innermost message, so the coordinator can classify it by variant exactly as it
//! would have if the error had never crossed a process. Wrapper variants that carry no
//! class of their own — `Context`, `Diagnostic`, `Shared`, `Collection`, `External`, and
//! Ballista's own `DataFusionError(…)` — are deliberately absent from the marker table,
//! so a scan walks straight past them to the variant that means something.
//!
//! ## Why peeling is a loop, and why a marker must sit at a structural position
//!
//! The layers alternate rather than nest neatly, and each `Debug` rendering escapes the
//! one inside it, so the inner `Execution("…")` of a doubly-rendered error is spelled
//! `Execution(\"…\")` and only becomes findable once the layer above it is unescaped.
//! Peeling one layer per pass, outermost first, is what makes the innermost payload
//! reachable.
//!
//! The structural-position rule is what keeps the peeling honest. A marker is accepted
//! only where a renderer would have put it — at the start, or after `(`, `[`, `,`, a
//! newline, or `": "` / `", "` — so a variant name that merely *appears inside* a
//! message (`No function matches 'Execution(x)'`) is not mistaken for the error's own
//! variant and does not truncate the message to its argument.

use std::fmt::Display;

use super::sanitize::{string_literal_end, unescape};
use crate::proto::vairedb::v1::VdbErrorCode;

/// What a [`VdbErrorCode`] tag opens with.
const TAG_OPEN: &str = "[VDB-";

/// Write `message` with a `[VDB-<code>]` tag, so `code` survives being rendered to text.
///
/// Use this for any error raised where the typed value cannot reach the coordinator —
/// inside a UDF, a UDAF or a window evaluator, all of which run on an executor. The tag
/// is read back by [`code_of_tagged_message`] and removed before the client sees the
/// message, so the wording stays whatever PostgreSQL's is.
pub fn tagged_message(code: VdbErrorCode, message: impl Display) -> String {
    format!("{TAG_OPEN}{}] {message}", code as i32)
}

/// The [`VdbErrorCode`] tagged into `msg`, or `None` if it carries no tag.
///
/// Scans rather than matching a prefix: the tag is written where the error is raised and
/// arrives wrapped in whatever the scheduler put around it. An `Unspecified` tag reads as
/// no tag at all — it carries no more information than the absence of one.
pub fn code_of_tagged_message(msg: &str) -> Option<VdbErrorCode> {
    let digits = tag_digits(msg)?.1;
    let code = VdbErrorCode::try_from(digits.parse::<i32>().ok()?).ok()?;
    (code != VdbErrorCode::Unspecified).then_some(code)
}

/// Remove every `[VDB-<code>]` tag from `msg`, along with the single space after it.
///
/// Called by [`crate::error::sanitize_message`], because the coordinator formats its
/// reply with a tag of its own: left in, a transported tag would reach the client as a
/// second `[VDB-…]` in the middle of the sentence.
pub fn strip_code_tags(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;
    while let Some((at, digits)) = tag_digits(rest) {
        out.push_str(&rest[..at]);
        let after = at + TAG_OPEN.len() + digits.len() + 1;
        rest = rest[after..].strip_prefix(' ').unwrap_or(&rest[after..]);
    }
    out.push_str(rest);
    out
}

/// The offset of the first `[VDB-<digits>]` tag in `msg` and the digits it spells.
///
/// Digits only, so a `[VDB-…]` that is not a tag — one carrying a name, or an unclosed
/// bracket — is left alone rather than half-consumed.
fn tag_digits(msg: &str) -> Option<(usize, &str)> {
    let mut from = 0;
    while let Some(found) = msg[from..].find(TAG_OPEN) {
        let at = from + found;
        let rest = &msg[at + TAG_OPEN.len()..];
        from = at + TAG_OPEN.len();
        let end = rest.find(']')?;
        let digits = &rest[..end];
        if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
            return Some((at, digits));
        }
    }
    None
}

/// A `DataFusionError` variant, recovered from the text a transported failure arrives as.
///
/// One per variant that carries a PostgreSQL class of its own. The wrapper variants are
/// absent on purpose — see the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportedVariant {
    /// `DataFusionError::ArrowError` — the message is an [`arrow::error::ArrowError`],
    /// in either its `Debug` variant name or its `Display` wording.
    Arrow,
    /// `DataFusionError::Configuration`.
    Configuration,
    /// `DataFusionError::Execution`.
    Execution,
    /// `DataFusionError::ExecutionJoin`.
    ExecutionJoin,
    /// `DataFusionError::Internal`.
    Internal,
    /// `DataFusionError::IoError`.
    Io,
    /// `DataFusionError::NotImplemented`.
    NotImplemented,
    /// `DataFusionError::Plan`.
    Plan,
    /// `DataFusionError::ResourcesExhausted`.
    ResourcesExhausted,
    /// `DataFusionError::SchemaError`.
    Schema,
    /// `DataFusionError::SQL`.
    Sql,
    /// `DataFusionError::Substrait`.
    Substrait,
}

/// A transported error read back into the variant it was raised as and its own message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportedError {
    /// The variant the error was raised as.
    pub variant: TransportedVariant,
    /// The innermost message, with every layer of transport rendering peeled off.
    pub message: String,
}

/// One variant and the two spellings it can arrive in.
struct Marker {
    variant: TransportedVariant,
    /// The `Debug` tuple-variant constructor, including its opening parenthesis.
    debug: &'static str,
    /// The `Display` prefix — transcribed from `DataFusionError::error_prefix`, which is
    /// the only thing that decides what a rendered `DataFusionError` starts with.
    display: &'static str,
}

/// Every variant worth recovering, with both of its spellings.
///
/// The feature-gated variants (`ParquetError`, `ObjectStore`, `Ffi`) are left out for the
/// same reason `classify_datafusion_error_code` does not name them: all three are engine
/// failures, and naming them would tie this table to DataFusion's feature resolution.
const MARKERS: &[Marker] = &[
    Marker {
        variant: TransportedVariant::Arrow,
        debug: "ArrowError(",
        display: "Arrow error: ",
    },
    Marker {
        variant: TransportedVariant::Configuration,
        debug: "Configuration(",
        display: "Invalid or Unsupported Configuration: ",
    },
    Marker {
        variant: TransportedVariant::Execution,
        debug: "Execution(",
        display: "Execution error: ",
    },
    Marker {
        variant: TransportedVariant::ExecutionJoin,
        debug: "ExecutionJoin(",
        display: "ExecutionJoin error: ",
    },
    Marker {
        variant: TransportedVariant::Internal,
        debug: "Internal(",
        display: "Internal error: ",
    },
    Marker {
        variant: TransportedVariant::Io,
        debug: "IoError(",
        display: "IO error: ",
    },
    Marker {
        variant: TransportedVariant::NotImplemented,
        debug: "NotImplemented(",
        display: "This feature is not implemented: ",
    },
    Marker {
        variant: TransportedVariant::Plan,
        debug: "Plan(",
        display: "Error during planning: ",
    },
    Marker {
        variant: TransportedVariant::ResourcesExhausted,
        debug: "ResourcesExhausted(",
        display: "Resources exhausted: ",
    },
    Marker {
        variant: TransportedVariant::Schema,
        debug: "SchemaError(",
        display: "Schema error: ",
    },
    Marker {
        variant: TransportedVariant::Sql,
        debug: "SQL(",
        display: "SQL error: ",
    },
    Marker {
        variant: TransportedVariant::Substrait,
        debug: "Substrait(",
        display: "Substrait error: ",
    },
];

/// How many layers [`recover_transported_error`] will peel.
///
/// Bounded rather than run to a fixed point. The deepest shape measured is four — a job
/// wrapper, a task wrapper, `Execution("…")`, and an `ArrowError(…)` inside it — and a
/// bound means no input can make the loop spin.
const MAX_PEEL_PASSES: usize = 8;

/// Read `text` back into the innermost `DataFusionError` variant it was rendered from.
///
/// Returns `None` when the text names no variant at all, which is the honest answer for a
/// failure that never was a `DataFusionError` — a gRPC transport error, or a message a
/// node wrote itself.
pub fn recover_transported_error(text: &str) -> Option<TransportedError> {
    let mut current = text.to_string();
    let mut recovered = None;
    for _ in 0..MAX_PEEL_PASSES {
        let Some(peeled) = peel(&current) else {
            break;
        };
        current = peeled.message.clone();
        recovered = Some(peeled);
    }
    recovered
}

/// Peel one layer: find the outermost marker at a structural position and take its
/// payload.
///
/// Outermost and not innermost, because the inner layers are still escaped — one pass
/// unescapes what it takes, which is what makes the next marker findable.
fn peel(text: &str) -> Option<TransportedError> {
    let mut best: Option<(usize, &Marker, bool)> = None;
    for marker in MARKERS {
        for (needle, is_debug) in [(marker.debug, true), (marker.display, false)] {
            if let Some(at) = first_structural(text, needle)
                && best.is_none_or(|(found, _, _)| at < found)
            {
                best = Some((at, marker, is_debug));
            }
        }
    }

    let (at, marker, is_debug) = best?;
    let needle = if is_debug {
        marker.debug
    } else {
        marker.display
    };
    let rest = &text[at + needle.len()..];
    let message = if is_debug {
        debug_payload(rest)?
    } else {
        rest.trim().to_string()
    };
    (!message.is_empty()).then_some(TransportedError {
        variant: marker.variant,
        message,
    })
}

/// The offset of the first occurrence of `needle` in `text` that sits where a renderer
/// would have put it, per [`is_structural`].
fn first_structural(text: &str, needle: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(found) = text[from..].find(needle) {
        let at = from + found;
        if is_structural(text, at) {
            return Some(at);
        }
        from = at + needle.len();
    }
    None
}

/// Whether a marker starting at `at` sits where a `Debug` or `Display` rendering would
/// have put it.
///
/// The accepted predecessors are the punctuation a renderer emits around a nested error:
/// a tuple-variant's `(`, a slice's `[`, a field separator, the newline
/// `DataFusionError::Context` writes before the error it wraps, and the `": "` an engine
/// prefix follows. Anything else — a letter, a quote, an apostrophe — means the variant
/// name is part of some message's prose rather than the rendering of an error.
fn is_structural(text: &str, at: usize) -> bool {
    if at == 0 {
        return true;
    }
    let mut before = text[..at].chars().rev();
    match before.next() {
        Some('(') | Some('[') | Some('\n') | Some(',') => true,
        Some(' ') => matches!(before.next(), Some(':') | Some(',')),
        _ => false,
    }
}

/// The payload of a `Debug` tuple variant whose opening parenthesis has been consumed.
///
/// A quoted payload is the common case and is unescaped by one level, which is the level
/// the rendering that produced it added. An unquoted one — `ArrowError(DivideByZero)`, a
/// variant with no message of its own — is taken up to its closing parenthesis instead,
/// because the variant *name* is the whole of what it says.
fn debug_payload(rest: &str) -> Option<String> {
    if let Some(literal) = rest.strip_prefix('"') {
        let end = string_literal_end(literal)?;
        return Some(unescape(&literal[..end]));
    }
    Some(rest[..matching_paren(rest)?].to_string())
}

/// Byte offset, within `s`, of the `)` closing an already-consumed `(` — `None` if it
/// never closes.
///
/// Quoting is respected: a parenthesis inside a message the rendering quoted must not
/// close the variant that carries it.
fn matching_paren(s: &str) -> Option<usize> {
    let mut depth = 1usize;
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
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A tag round-trips, and the code it carries is the one that was written.
    #[test]
    fn a_tag_round_trips_through_its_own_message() {
        for code in [
            VdbErrorCode::FeatureNotSupported,
            VdbErrorCode::DivisionByZero,
            VdbErrorCode::GroupingError,
            VdbErrorCode::InvalidArgumentForNthValue,
        ] {
            let msg = tagged_message(code, "something the client reads");
            assert_eq!(code_of_tagged_message(&msg), Some(code));
            assert_eq!(strip_code_tags(&msg), "something the client reads");
        }
    }

    /// The point of the tag: it is still readable after the scheduler has rendered the
    /// error it was written into, twice.
    #[test]
    fn a_tag_survives_being_rendered_into_a_job_failure() {
        let raised = tagged_message(VdbErrorCode::FeatureNotSupported, "no array of these");
        let transported = format!(
            "Job 3QdcFzH failed: Job failed due to stage 1 failed: Task failed due to \
             runtime execution error: DataFusionError(Plan({raised:?}))"
        );
        assert_eq!(
            code_of_tagged_message(&transported),
            Some(VdbErrorCode::FeatureNotSupported)
        );
        let recovered = recover_transported_error(&transported).expect("a Plan error");
        assert_eq!(recovered.variant, TransportedVariant::Plan);
        assert_eq!(strip_code_tags(&recovered.message), "no array of these");
    }

    /// Text that is not a tag is left alone rather than half-consumed.
    #[test]
    fn something_that_is_not_a_tag_is_not_read_as_one() {
        for msg in [
            "no tag at all",
            "[VDB-] empty",
            "[VDB-abc] not digits",
            "[VDB-1004 unclosed",
            // Code 0 is `Unspecified`, which says no more than the absence of a tag.
            "[VDB-0] unspecified",
        ] {
            assert_eq!(code_of_tagged_message(msg), None, "for {msg:?}");
        }
        assert_eq!(
            strip_code_tags("[VDB-abc] not digits"),
            "[VDB-abc] not digits"
        );
    }

    /// Both spellings of a transported error, as measured on a five-node cluster.
    #[test]
    fn both_of_ballistas_renderings_name_the_variant() {
        // A failed *task*, rendered with `{:?}` by `FailedTask::from`.
        let debug_spelling = "Job rU2cwfN failed: Job failed due to stage 1 failed: Task \
                              failed due to runtime execution error: \
                              DataFusionError(Plan(\"the percentile fraction must be a \
                              number\"))";
        assert_eq!(
            recover_transported_error(debug_spelling),
            Some(TransportedError {
                variant: TransportedVariant::Plan,
                message: "the percentile fraction must be a number".to_string(),
            })
        );

        // A failed *job*, rendered with `{}` — so DataFusion's `error_prefix` is what
        // names the variant.
        let display_spelling = "Job WV0k16o failed: DataFusion error: This feature is not \
                                implemented: Physical plan does not support logical \
                                expression AggregateFunction(AggregateFunction { func: \
                                AggregateUDF { inner: Count { signature: Signature { … } } } })";
        let recovered = recover_transported_error(display_spelling).expect("a NotImplemented");
        assert_eq!(recovered.variant, TransportedVariant::NotImplemented);
        assert!(
            recovered
                .message
                .starts_with("Physical plan does not support logical expression AggregateFunction"),
            "{}",
            recovered.message
        );
    }

    /// The reason peeling is a loop: each rendering escapes the one inside it, so the
    /// innermost variant is only findable once the layer above it is unescaped.
    #[test]
    fn peeling_reaches_the_innermost_variant_through_two_renderings() {
        // A divide by zero, exactly as it arrives: an `ArrowError` rendered into an
        // `Execution` message.
        assert_eq!(
            recover_transported_error(
                "Job 4jmW6pr failed: Job failed due to stage 1 failed: Task failed due to \
                 runtime execution error: DataFusionError(Execution(\"ArrowError(DivideByZero)\"))"
            ),
            Some(TransportedError {
                variant: TransportedVariant::Arrow,
                message: "DivideByZero".to_string(),
            })
        );

        // A `bytea` literal the read path's UDF could not decode, doubly rendered — the
        // inner `Execution(\"…\")` is unreachable until the outer layer is unescaped.
        assert_eq!(
            recover_transported_error(
                "Job kSBslNo failed: Job failed due to stage 1 failed: Task failed due to \
                 runtime execution error: \
                 DataFusionError(Execution(\"DataFusionError(Execution(\\\"invalid \
                 hexadecimal digit: \\\\\\\"z\\\\\\\"\\\"))\"))"
            ),
            Some(TransportedError {
                variant: TransportedVariant::Execution,
                message: "invalid hexadecimal digit: \"z\"".to_string(),
            })
        );
    }

    /// `DataFusionError::Context` writes a newline before the error it wraps and has no
    /// prefix of its own, so the variant that matters is the one after the newline.
    #[test]
    fn a_context_wrapper_is_walked_past_to_what_it_wraps() {
        let recovered = recover_transported_error(
            "collect\ncaused by\nExecution error: Job 3QdcFzH failed: Task failed due to \
             runtime execution error: DataFusionError(Execution(\"ArrowError(DivideByZero)\"))",
        )
        .expect("the wrapped error");
        assert_eq!(recovered.variant, TransportedVariant::Arrow);
        assert_eq!(recovered.message, "DivideByZero");
    }

    /// The structural-position rule, which is what keeps peeling from following a variant
    /// name that is only part of somebody's prose. Without it the message below would be
    /// truncated to `x` and reclassified as an execution failure.
    #[test]
    fn a_variant_name_inside_a_message_is_not_read_as_the_variant() {
        assert_eq!(
            recover_transported_error(
                "Job abc failed: Task failed due to runtime execution error: \
                 DataFusionError(Plan(\"No function matches 'Execution(x)'\"))"
            ),
            Some(TransportedError {
                variant: TransportedVariant::Plan,
                message: "No function matches 'Execution(x)'".to_string(),
            })
        );
    }

    /// A failure that never was a `DataFusionError` recovers nothing, rather than being
    /// forced into a variant it did not have.
    #[test]
    fn text_that_names_no_variant_recovers_nothing() {
        for text in [
            "connection refused",
            "Job abc failed: stage 1 failed",
            "write quorum not reached: 1/2 nodes acknowledged",
        ] {
            assert_eq!(recover_transported_error(text), None, "for {text:?}");
        }
    }

    /// A quoted payload's own parentheses must not close the variant carrying it, and a
    /// truncated rendering is left alone rather than mangled further.
    #[test]
    fn a_malformed_or_parenthesised_payload_is_handled_without_panicking() {
        let recovered = recover_transported_error(
            "Task failed due to runtime execution error: \
             DataFusionError(Execution(\"cast error: sum(a) is not a number\"))",
        )
        .expect("an Execution error");
        assert_eq!(recovered.message, "cast error: sum(a) is not a number");

        // Truncated upstream before its closing quote: nothing is recovered, and the
        // caller keeps the text it already had.
        assert_eq!(
            recover_transported_error(
                "Task failed due to runtime execution error: DataFusionError(Plan(\"unclosed"
            ),
            None
        );
    }
}
