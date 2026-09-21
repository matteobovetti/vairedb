//! Cross-node error handling: a portable error type, PostgreSQL SQLSTATE
//! mapping, and message sanitization for client-facing output.

mod sanitize;
mod sqlstate;
mod transported;
mod types;

pub use sanitize::sanitize_message;
pub use sqlstate::sqlstate_for_code;
pub use transported::{
    TransportedError, TransportedVariant, code_of_tagged_message, recover_transported_error,
    strip_code_tags, tagged_message,
};
pub use types::VaireDbError;
