//! Cross-node error handling: a portable error type, PostgreSQL SQLSTATE
//! mapping, and message sanitization for client-facing output.
//!
//! The submodules divide the work by what changes them. `sqlstate` holds the
//! code-to-SQLSTATE contract; `tag` the `[VDB-<code>]` tag VaireDB writes so a code
//! survives being rendered to text; `transported` the recovery of a `DataFusionError`
//! from the text a failed Ballista task arrives as; `sanitize` the scrubbing that decides
//! what a client is allowed to read. The last two share their text-scanning primitives —
//! and the bound on how many layers of transport rendering either will peel — with `scan`,
//! so neither depends on the other for them.

mod sanitize;
mod scan;
mod sqlstate;
mod tag;
mod transported;
mod types;

pub use sanitize::sanitize_message;
pub use sqlstate::sqlstate_for_code;
pub use tag::{code_of_tagged_message, strip_code_tags, tagged_message};
pub use transported::{TransportedError, TransportedVariant, recover_transported_error};
pub use types::VaireDbError;
