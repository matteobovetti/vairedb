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
        VdbErrorCode::InvalidArgumentForNthValue => "22016",
        VdbErrorCode::UndefinedFunction => "42883",
        VdbErrorCode::NullValueNotAllowed => "22004",
        VdbErrorCode::DatetimeFieldOverflow => "22008",
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

    /// The SQLSTATE a client receives for every code VaireDB can raise, written out once.
    ///
    /// This table is the contract, not a sample of it: a driver branches on these five
    /// characters and never on the wording, so a change to any row is a change a client
    /// sees. It is transcribed independently of [`sqlstate_for_code`]'s `match` — the point
    /// is to state what PostgreSQL reports, so that a row disagreeing with the map is a
    /// question about which of the two is wrong, and the comments record the answers that
    /// were expensive to work out.
    const CONTRACT: &[(VdbErrorCode, &str)] = &[
        (VdbErrorCode::TableNotFound, "42P01"),
        (VdbErrorCode::ColumnNotFound, "42703"),
        (VdbErrorCode::TypeMismatch, "42804"),
        (VdbErrorCode::SqlSyntaxError, "42601"),
        (VdbErrorCode::FeatureNotSupported, "0A000"),
        (VdbErrorCode::TableAlreadyExists, "42P07"),
        (VdbErrorCode::ColumnAlreadyExists, "42701"),
        (VdbErrorCode::WrongObjectType, "42809"),
        // Drivers and ORMs branch on these five: `25P02` is what tells a client its
        // transaction is aborted and only a rollback will do, `3B001` what tells it a
        // savepoint is gone, and `40003` that a commit's outcome is genuinely unknown.
        // A wrong SQLSTATE here turns a recoverable state into a hung session.
        (VdbErrorCode::InFailedTransaction, "25P02"),
        (VdbErrorCode::NoActiveTransaction, "25P01"),
        (VdbErrorCode::InvalidSavepoint, "3B001"),
        (VdbErrorCode::ReadOnlyTransaction, "25006"),
        (VdbErrorCode::PartialCommit, "40003"),
        // A schema that is missing and a schema that is already there are the two states
        // `CREATE SCHEMA` / `DROP SCHEMA` and every schema-qualified relation report, and
        // PostgreSQL clients distinguish them from the table-level codes.
        (VdbErrorCode::SchemaNotFound, "3F000"),
        (VdbErrorCode::SchemaAlreadyExists, "42P06"),
        (VdbErrorCode::DependentObjectsExist, "2BP01"),
        // `SET`/`SHOW`/`RESET` have three distinct failures a client should be able to tell
        // apart without reading the message: the parameter name is not one the server has
        // (`42704`), the name is fine but the value is not one it can take (`22023`), and
        // the parameter is reportable but fixed at startup (`55P02`). Collapsing any of
        // them into `0A000` would tell a driver that `SET` itself is unsupported and stop
        // it retrying with a value that would have worked.
        (VdbErrorCode::UndefinedObject, "42704"),
        (VdbErrorCode::InvalidParameterValue, "22023"),
        (VdbErrorCode::CantChangeRuntimeParam, "55P02"),
        // A value the type it must be reported as cannot hold is a data error, not an
        // internal one: `22003` tells the client the query and the server are both fine and
        // one value was out of range, which is what distinguishes it from the `XX000` that
        // would otherwise swallow it.
        (VdbErrorCode::NumericValueOutOfRange, "22003"),
        // The four codes added for v0.2, each of which used to arrive as `XX000` — a class
        // that tells a client the *server* broke. Two are data errors the client caused and
        // can fix in the value (`22012`, `22P02`) and two are syntax errors it can fix in
        // the query (`42803`, `42P20`); reporting any of them as internal invites a retry
        // that cannot succeed, or a bug report against the wrong component.
        (VdbErrorCode::DivisionByZero, "22012"),
        (VdbErrorCode::InvalidTextRepresentation, "22P02"),
        (VdbErrorCode::GroupingError, "42803"),
        (VdbErrorCode::WindowingError, "42P20"),
        // PostgreSQL spends a whole SQLSTATE on one argument of one function, and the
        // reason is worth keeping: without it, `nth_value(x, 0)` answers a column of NULLs
        // that a client reads as "the window had no such row" for every row. The code says
        // the argument was wrong, not that the data was absent.
        (VdbErrorCode::InvalidArgumentForNthValue, "22016"),
        // The distinction a client acts on differently: `0A000` means "PostgreSQL has this
        // form and VaireDB does not yet", so waiting for a release is rational; `42883`
        // means "PostgreSQL does not have it either", so only editing the call helps.
        // `count(a, b)` is the second, and reporting it as the first sends the client to
        // the wrong place.
        (VdbErrorCode::UndefinedFunction, "42883"),
        // A NULL where the value has to be rendered as an identifier has no spelling at
        // all, so PostgreSQL refuses rather than emitting something. `22004` says one row's
        // data was null; `22023` would say the argument was the wrong *kind* of value,
        // which is a different fix.
        (VdbErrorCode::NullValueNotAllowed, "22004"),
        // The 30th of February is a caller's arithmetic bug, and `22008` is what tells them
        // so. `22003` would say the number was too large for its type, which is a different
        // investigation: every field of `make_timestamp(2024, 2, 30, …)` fits in an
        // `integer`.
        (VdbErrorCode::DatetimeFieldOverflow, "22008"),
        // Shares `42P01` with `TableNotFound` on purpose rather than by copy-paste: a shard
        // is VaireDB's own unit and never appears in the statement, so the only thing a
        // client can be told is that the relation it did name is not there.
        (VdbErrorCode::ShardNotFound, "42P01"),
        (VdbErrorCode::WriteConflict, "40001"),
        (VdbErrorCode::EngineError, "XX000"),
        (VdbErrorCode::WriteQueueFull, "53000"),
        (VdbErrorCode::NodeNotFound, "58000"),
        (VdbErrorCode::NodeUnavailable, "08001"),
        (VdbErrorCode::NodeShuttingDown, "57P01"),
        (VdbErrorCode::QuorumNotReached, "53000"),
        (VdbErrorCode::NoAliveNodes, "53000"),
        (VdbErrorCode::ShardNotAssigned, "55000"),
        (VdbErrorCode::ShardUnavailable, "08001"),
        (VdbErrorCode::NodeCommunicationError, "08006"),
        (VdbErrorCode::CatalogStorageError, "58030"),
        (VdbErrorCode::CatalogTransactionError, "53000"),
        (VdbErrorCode::CatalogCommitError, "40000"),
        (VdbErrorCode::CatalogAccessError, "XX000"),
        (VdbErrorCode::SerializationError, "XX000"),
        (VdbErrorCode::InternalError, "XX000"),
        (VdbErrorCode::Unspecified, "XX000"),
    ];

    /// The codes for which `XX000` is the honest answer: the server did break, or nothing
    /// is known about what did.
    const GENUINELY_INTERNAL: &[VdbErrorCode] = &[
        VdbErrorCode::Unspecified,
        VdbErrorCode::EngineError,
        VdbErrorCode::CatalogAccessError,
        VdbErrorCode::SerializationError,
        VdbErrorCode::InternalError,
    ];

    #[test]
    fn every_code_maps_to_the_sqlstate_its_contract_row_pins() {
        for (code, sqlstate) in CONTRACT {
            assert_eq!(sqlstate_for_code(*code), *sqlstate, "for {code:?}");
        }
    }

    /// The guarantee the table on its own cannot give: that it is the *whole* contract.
    ///
    /// `sqlstate_for_code` matches exhaustively, so a new proto variant cannot be forgotten
    /// there — but it can be added to the map and never checked against PostgreSQL, which
    /// is what this catches. `VdbErrorCode` is a prost enum and offers no list of its
    /// variants, so the declared codes are enumerated the only way they can be: `try_from`
    /// accepts exactly the numbers `error.proto` declares, and the range below spans every
    /// block it allocates with room to spare for the next one.
    #[test]
    fn the_contract_covers_every_code_the_proto_declares() {
        let declared: Vec<VdbErrorCode> = (0..10_000)
            .filter_map(|n| VdbErrorCode::try_from(n).ok())
            .collect();
        for code in &declared {
            assert!(
                CONTRACT.iter().any(|(pinned, _)| pinned == code),
                "{code:?} is declared in error.proto but no contract row pins its SQLSTATE"
            );
        }
        assert_eq!(
            declared.len(),
            CONTRACT.len(),
            "the contract has a row for a code that is not declared, or a duplicate row"
        );
    }

    /// A guard on the map itself rather than on any one code: a new code *can* be mapped to
    /// `XX000` by copying a neighbouring arm, which is the mistake this catches. Every code
    /// whose class is knowable should have left the internal class behind.
    #[test]
    fn only_genuinely_internal_codes_report_the_internal_class() {
        for (code, _) in CONTRACT {
            if GENUINELY_INTERNAL.contains(code) {
                continue;
            }
            assert_ne!(
                sqlstate_for_code(*code),
                "XX000",
                "{code:?} still reports the internal class"
            );
        }
    }

    /// A SQLSTATE is five characters of `[0-9A-Z]` and a client parses it positionally —
    /// the first two are the class it branches on. One transposed or missing character
    /// turns a code a driver handles into one it cannot read.
    #[test]
    fn every_sqlstate_is_five_characters_a_client_can_parse() {
        for (code, sqlstate) in CONTRACT {
            assert_eq!(sqlstate.len(), 5, "{code:?} maps to {sqlstate:?}");
            assert!(
                sqlstate
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()),
                "{code:?} maps to {sqlstate:?}"
            );
        }
    }
}
