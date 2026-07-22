//! Cross-node error handling: a portable error type, PostgreSQL SQLSTATE
//! mapping, and message sanitization for client-facing output.

mod sanitize;
mod sqlstate;
mod types;

pub use sanitize::sanitize_message;
pub use sqlstate::sqlstate_for_code;
pub use types::VaireDbError;
