//! The `[VDB-<code>]` tag VaireDB writes into an error message so a [`VdbErrorCode`]
//! survives being rendered to text.
//!
//! An error raised inside a Ballista executor never reaches the coordinator as a value —
//! see [`super::transported`] for what that trip does to it. The short version is that the
//! only payload which crosses the scheduler is a `String`, so a code chosen where the error
//! was raised has to travel *in the message* or not at all.
//!
//! [`tagged_message`] writes that code in and [`code_of_tagged_message`] reads it back.
//! This is the *structured* half of the recovery: the code is chosen by the code that knows
//! what went wrong, rather than guessed at the other end from wording. A guard added to a
//! UDF or UDAF gets the right SQLSTATE by tagging its message and nothing else.
//!
//! The tag is a transport artifact and never reaches a client:
//! [`crate::error::sanitize_message`] strips it, and the coordinator re-attaches exactly
//! one `[VDB-…]` of its own when it formats the reply.

use std::fmt::Display;

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
}
