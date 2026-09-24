use crate::proto::vairedb::v1::VdbErrorCode;

/// A portable error pairing a [`VdbErrorCode`] with a human-readable message,
/// usable on either node and convertible to the gRPC error representation.
#[derive(Debug, Clone)]
pub struct VaireDbError {
    /// Machine-readable classification of the error.
    pub code: VdbErrorCode,
    /// Human-readable description.
    pub message: String,
}

impl VaireDbError {
    /// Construct an error from a code and message.
    pub fn new(code: VdbErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The message prefixed with its `[VDB-<code>]` tag, as shown to clients.
    ///
    /// The numeric value of the code is what goes in, not its name: this is the one
    /// `[VDB-…]` a client is meant to see, and it has to be the same spelling a support
    /// question can be asked about and [`crate::error::code_of_tagged_message`] can read.
    pub fn formatted_message(&self) -> String {
        format!("[VDB-{}] {}", self.code as i32, self.message)
    }
}

impl std::fmt::Display for VaireDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.formatted_message())
    }
}

impl std::error::Error for VaireDbError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact text a client reads, through both ways of asking for it: the coordinator
    /// formats its reply with [`VaireDbError::formatted_message`], and anything that logs
    /// or re-wraps the error gets there through `Display`. Both have to spell the tag the
    /// same way, since the numeric code is what a client quotes back.
    #[test]
    fn both_renderings_prefix_the_message_with_the_numeric_code() {
        let err = VaireDbError::new(VdbErrorCode::TableNotFound, "no such table");
        assert_eq!(err.formatted_message(), "[VDB-1000] no such table");
        assert_eq!(err.to_string(), "[VDB-1000] no such table");
    }
}
