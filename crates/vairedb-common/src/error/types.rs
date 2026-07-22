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

    /// The error code's underlying numeric value.
    fn numeric_code(&self) -> i32 {
        self.code as i32
    }

    /// The message prefixed with its `[VDB-<code>]` tag, as shown to clients.
    pub fn formatted_message(&self) -> String {
        format!("[VDB-{}] {}", self.numeric_code(), self.message)
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

    #[test]
    fn formatted_message_includes_numeric_code() {
        let err = VaireDbError::new(VdbErrorCode::TableNotFound, "no such table");
        assert_eq!(err.formatted_message(), "[VDB-1000] no such table");
    }

    #[test]
    fn display_matches_formatted_message() {
        let err = VaireDbError::new(VdbErrorCode::WriteConflict, "conflict");
        assert_eq!(err.to_string(), err.formatted_message());
    }
}
