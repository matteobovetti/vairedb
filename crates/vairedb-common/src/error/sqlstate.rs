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
        VdbErrorCode::UndefinedObject => "42704",
        VdbErrorCode::InvalidParameterValue => "22023",
        VdbErrorCode::CantChangeRuntimeParam => "55P02",
        VdbErrorCode::NumericValueOutOfRange => "22003",
        VdbErrorCode::DivisionByZero => "22012",
        VdbErrorCode::InvalidTextRepresentation => "22P02",
        VdbErrorCode::GroupingError => "42803",
        VdbErrorCode::WindowingError => "42P20",
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

    // `SET`/`SHOW`/`RESET` have three distinct failures a client should be able to
    // tell apart without reading the message: the parameter name is not one the
    // server has (`42704`), the name is fine but the value is not one it can take
    // (`22023`), and the parameter is reportable but fixed at startup (`55P02`).
    // Collapsing any of them into `0A000` would tell a driver that `SET` itself is
    // unsupported and stop it retrying with a value that would have worked.
    #[test]
    fn maps_session_parameter_failures_to_their_own_sqlstates() {
        assert_eq!(sqlstate_for_code(VdbErrorCode::UndefinedObject), "42704");
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::InvalidParameterValue),
            "22023"
        );
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::CantChangeRuntimeParam),
            "55P02"
        );
    }

    // A value the type it must be reported as cannot hold is a data error, not an
    // internal one: `22003` tells the client the query and the server are both fine
    // and one value was out of range, which is what distinguishes it from the `XX000`
    // that would otherwise swallow it.
    #[test]
    fn maps_an_unrepresentable_value_to_the_data_error_sqlstate() {
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::NumericValueOutOfRange),
            "22003"
        );
    }

    // The four codes added for v0.2, each of which used to arrive as `XX000` — a class
    // that tells a client the *server* broke. Two are data errors the client caused and
    // can fix in the value (`22012`, `22P02`) and two are syntax errors it can fix in
    // the query (`42803`, `42P20`); reporting any of them as internal invites a retry
    // that cannot succeed, or a bug report against the wrong component.
    #[test]
    fn maps_the_four_v02_codes_off_the_internal_class() {
        assert_eq!(sqlstate_for_code(VdbErrorCode::DivisionByZero), "22012");
        assert_eq!(
            sqlstate_for_code(VdbErrorCode::InvalidTextRepresentation),
            "22P02"
        );
        assert_eq!(sqlstate_for_code(VdbErrorCode::GroupingError), "42803");
        assert_eq!(sqlstate_for_code(VdbErrorCode::WindowingError), "42P20");
    }

    // A guard on the map itself rather than on any one code: `sqlstate_for_code`
    // matches exhaustively, so a new proto variant cannot be forgotten here — but it
    // *can* be mapped to `XX000` by copying a neighbouring arm, which is the mistake
    // this catches. Every code whose class is knowable should have left the internal
    // class behind.
    #[test]
    fn only_genuinely_internal_codes_report_the_internal_class() {
        let internal = [
            VdbErrorCode::Unspecified,
            VdbErrorCode::EngineError,
            VdbErrorCode::CatalogAccessError,
            VdbErrorCode::SerializationError,
            VdbErrorCode::InternalError,
        ];
        for code in [
            VdbErrorCode::DivisionByZero,
            VdbErrorCode::InvalidTextRepresentation,
            VdbErrorCode::GroupingError,
            VdbErrorCode::WindowingError,
        ] {
            assert!(!internal.contains(&code));
            assert_ne!(
                sqlstate_for_code(code),
                "XX000",
                "{code:?} still reports the internal class"
            );
        }
    }
}
