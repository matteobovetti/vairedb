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
        VdbErrorCode::WrongObjectType => "42809",
        VdbErrorCode::InFailedTransaction => "25P02",
        VdbErrorCode::NoActiveTransaction => "25P01",
        VdbErrorCode::InvalidSavepoint => "3B001",
        VdbErrorCode::ReadOnlyTransaction => "25006",
        VdbErrorCode::PartialCommit => "40003",
        VdbErrorCode::SchemaNotFound => "3F000",
        VdbErrorCode::SchemaAlreadyExists => "42P06",
        VdbErrorCode::DependentObjectsExist => "2BP01",
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

    // Drivers and ORMs branch on these five: `25P02` is what tells a client its
    // transaction is aborted and only a rollback will do, `3B001` what tells it a
    // savepoint is gone, and `40003` that a commit's outcome is genuinely unknown.
    // A wrong SQLSTATE here turns a recoverable state into a hung session.
    #[test]
    fn maps_transaction_states_to_the_sqlstates_clients_branch_on() {
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::InFailedTransaction),
            "25P02"
        );
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::NoActiveTransaction),
            "25P01"
        );
        assert_eq!(sqlstate_for_code(VdbErrorCode::InvalidSavepoint), "3B001");
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::ReadOnlyTransaction),
            "25006"
        );
        assert_eq!(sqlstate_for_code(VdbErrorCode::PartialCommit), "40003");
    }

    // A schema that is missing and a schema that is already there are the two
    // states `CREATE SCHEMA` / `DROP SCHEMA` and every schema-qualified relation
    // report, and PostgreSQL clients distinguish them from the table-level codes.
    #[test]
    fn maps_schema_states_to_their_own_sqlstates() {
        assert_eq!(sqlstate_for_code(VdbErrorCode::SchemaNotFound), "3F000");
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::SchemaAlreadyExists),
            "42P06"
        );
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::DependentObjectsExist),
            "2BP01"
        );
    }
}
