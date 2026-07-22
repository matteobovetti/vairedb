use crate::proto::vairedb::v1::VdbErrorCode;

/// Map an internal [`VdbErrorCode`] to the five-character PostgreSQL SQLSTATE
/// the pgwire layer reports to clients.
///
/// Codes with no precise SQLSTATE equivalent fall back to `XX000`
/// (internal error).
pub fn sqlstate_for_code(code: VdbErrorCode) -> &'static str {
    match code {
        VdbErrorCode::TableNotFound => "42P01",
        VdbErrorCode::ColumnNotFound => "42703",
        VdbErrorCode::TypeMismatch => "42804",
        VdbErrorCode::SqlSyntaxError => "42601",
        VdbErrorCode::FeatureNotSupported => "0A000",
        VdbErrorCode::TableAlreadyExists => "42P07",
        VdbErrorCode::ColumnAlreadyExists => "42701",
        VdbErrorCode::ShardNotFound => "42P01",
        VdbErrorCode::WriteConflict => "40001",
        VdbErrorCode::EngineError => "XX000",
        VdbErrorCode::WriteQueueFull => "53000",
        VdbErrorCode::NodeNotFound => "58000",
        VdbErrorCode::NodeUnavailable => "08001",
        VdbErrorCode::NodeShuttingDown => "57P01",
        VdbErrorCode::QuorumNotReached => "53000",
        VdbErrorCode::NoAliveNodes => "53000",
        VdbErrorCode::ShardNotAssigned => "55000",
        VdbErrorCode::ShardUnavailable => "08001",
        VdbErrorCode::NodeCommunicationError => "08006",
        VdbErrorCode::CatalogStorageError => "58030",
        VdbErrorCode::CatalogTransactionError => "53000",
        VdbErrorCode::CatalogCommitError => "40000",
        VdbErrorCode::CatalogAccessError => "XX000",
        VdbErrorCode::SerializationError => "XX000",
        VdbErrorCode::InternalError => "XX000",
        VdbErrorCode::Unspecified => "XX000",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_codes_to_expected_sqlstates() {
        assert_eq!(sqlstate_for_code(VdbErrorCode::TableNotFound), "42P01");
        assert_eq!(sqlstate_for_code(VdbErrorCode::WriteConflict), "40001");
        assert_eq!(sqlstate_for_code(VdbErrorCode::SqlSyntaxError), "42601");
    }

    #[test]
    fn unspecified_falls_back_to_internal_sqlstate() {
        assert_eq!(sqlstate_for_code(VdbErrorCode::Unspecified), "XX000");
    }
}
