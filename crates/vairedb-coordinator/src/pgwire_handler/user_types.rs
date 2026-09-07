//! Refusal of user-defined types — `CREATE TYPE`, `CREATE DOMAIN`, `ALTER TYPE`.
//!
//! VaireDB has no user-defined types, and this is a decided limitation rather than
//! an unimplemented feature (`docs/specs/gap-analysis-command.md`, *Decided
//! limitations → No user-defined types*). Three things stand in the way, and none of
//! them is work waiting for a turn:
//!
//! * **The catalog models tables.** A type is cluster-wide state that every
//!   replica's DuckDB must already hold before any statement mentioning it can run,
//!   and there is no replay path for non-table DDL: a node that joins or is rebuilt
//!   would come back without the type and reject every write naming it.
//! * **The type does not survive the wire boundary.** The coordinator's emulated
//!   `pg_catalog` has no `pg_type` row to give a user type an OID, and a shard's
//!   enum column already reads back as text (`gap-analysis-data-type.md`), so a
//!   client could neither resolve the type nor receive a value tagged with it.
//! * **What an enum or a domain buys is a value check**, and a value check is
//!   shard-local — `VARCHAR` plus validation gives the same guarantee today, and a
//!   `CHECK` constraint gives the in-database version once constraints land.
//!
//! None of these statements classifies, so all three are refused by
//! `unsupported_statement_error`; this module is what makes that refusal say the
//! reason instead of a bare "not supported" a client would read as "not yet".
//! `DROP TYPE` is deliberately *not* here: it reports that the type does not exist,
//! which is exactly true and is what PostgreSQL says too.

use pgwire::error::PgWireError;
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::sqlparser::ast::Statement;

use super::error_enrichment::make_vdb_error;

/// The command name if `stmt` declares or alters a user-defined type, else `None`.
///
/// One place decides which statements this rule covers, so the label in the error
/// message and the decision to explain the reason cannot drift apart.
pub(super) fn refused_user_type(stmt: &Statement) -> Option<&'static str> {
    match stmt {
        // Enum, range and composite forms are all `CreateType`; a composite one is
        // the same answer, with a different way out (one column per field).
        Statement::CreateType { .. } => Some("CREATE TYPE"),
        // A domain is a named type plus a constraint, so it is the same request.
        Statement::CreateDomain(_) => Some("CREATE DOMAIN"),
        Statement::AlterType(_) => Some("ALTER TYPE"),
        _ => None,
    }
}

/// The one refusal message, so every spelling gets the same explanation and the
/// same way out. `what` names the command refused, as the client wrote it.
pub(super) fn user_type_error(what: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{what} is not supported by VaireDB: there are no user-defined types. A type is \
             cluster-wide state every replica must hold before a statement can name it, and the \
             catalog models tables — a node that joins or is rebuilt would come back without the \
             type and refuse every write using it. It would also not reach the client as a type: \
             the emulated pg_catalog has no OID to hand out for it, and a shard's enum column is \
             already read back as text. Declare the column as VARCHAR (or the closest built-in \
             type) and validate the values in the application; for a composite type, use one \
             column per field"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql)
            .unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"))
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn every_spelling_of_a_user_type_is_named() {
        for (sql, label) in [
            ("CREATE TYPE mood AS ENUM ('sad', 'happy')", "CREATE TYPE"),
            ("CREATE TYPE point AS (x INTEGER, y INTEGER)", "CREATE TYPE"),
            (
                "CREATE DOMAIN positive AS INTEGER CHECK (VALUE > 0)",
                "CREATE DOMAIN",
            ),
            ("ALTER TYPE mood ADD VALUE 'ok'", "ALTER TYPE"),
            ("ALTER TYPE mood RENAME TO feeling", "ALTER TYPE"),
        ] {
            assert_eq!(refused_user_type(&parse_one(sql)), Some(label), "`{sql}`");
        }
    }

    // The guard must not widen: a table whose *column* happens to use a type name
    // is ordinary DDL, and an `ALTER TABLE` is not an `ALTER TYPE`.
    #[test]
    fn ordinary_statements_are_untouched() {
        for sql in [
            "CREATE TABLE t (id INTEGER, mood VARCHAR)",
            "ALTER TABLE t ALTER COLUMN mood SET DATA TYPE TEXT",
            "DROP TYPE mood",
            "INSERT INTO t (id, mood) VALUES (1, 'sad')",
        ] {
            assert_eq!(refused_user_type(&parse_one(sql)), None, "`{sql}`");
        }
    }

    // The wording is the contract for a permanent limitation: the reason and the
    // way out both have to be in the message.
    #[test]
    fn the_message_carries_the_reason_and_the_alternative() {
        let msg = user_type_error("CREATE TYPE").to_string();
        assert!(msg.contains("CREATE TYPE"), "got: {msg}");
        assert!(msg.contains("no user-defined types"), "got: {msg}");
        assert!(msg.contains("VARCHAR"), "got: {msg}");
    }
}
