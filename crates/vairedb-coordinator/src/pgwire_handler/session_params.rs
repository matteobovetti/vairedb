//! Session runtime parameters: `SET`, `SHOW` and `RESET`.
//!
//! Drivers issue `SET` before their first query — `client_encoding`,
//! `application_name`, `extra_float_digits`, `DateStyle` — so refusing every one
//! of them breaks a client before it can run anything. That is the compatibility
//! risk this module answers.
//!
//! It answers it without the usual shortcut. "Accept and ignore" is only safe for
//! a parameter no answer depends on; for the rest, remembering a value the
//! coordinator does not act on would promise a rendering or a resolution rule that
//! nothing in the read path applies, and the client would never learn otherwise.
//! So every parameter is declared with a [`Disposition`] saying which values
//! VaireDB can honestly accept, and a value outside that set is refused rather
//! than recorded:
//!
//! * [`Disposition::Free`] — nothing VaireDB answers depends on it
//!   (`application_name`).
//! * [`Disposition::Matching`] — only values the coordinator's fixed behaviour
//!   already matches (`client_encoding` must be UTF-8, because the encoder only
//!   emits UTF-8).
//! * [`Disposition::Refused`] — refused whatever the value (`search_path`: there
//!   is no search path, an unqualified name always means the default schema).
//! * [`Disposition::ReadOnly`] — reportable but not settable, as in PostgreSQL.
//!
//! Nothing here reaches a shard: a runtime parameter is a property of one client's
//! connection, so there is nothing to broadcast and nothing a shard could answer
//! differently.
//!
//! The defaults are not written down twice. A session is seeded from the
//! `ParameterStatus` messages pgwire already sent at startup, so `SHOW` agrees
//! with the value the client was told on connect and `RESET` restores it —
//! PostgreSQL's `reset_val`.

use std::collections::HashMap;
use std::sync::Arc;

use pgwire::api::auth::{DefaultServerParameterProvider, ServerParameterProvider};
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::pgwire_handler::session::SessionState;
use crate::sqlparser::ast::{
    ContextModifier, Expr, Ident, ObjectName, Set, Statement, UnaryOperator, Value,
};

/// What VaireDB does with an assignment to a parameter.
enum Disposition {
    /// Any value is accepted and remembered. Only for parameters no answer VaireDB
    /// gives depends on — a label the client sets so it can read it back.
    Free,
    /// Only values the coordinator's own behaviour already matches. `accepts`
    /// decides; any other value is refused rather than remembered, because
    /// remembering it would promise behaviour the read path does not implement.
    Matching {
        accepts: fn(&str) -> bool,
        /// What the coordinator actually does, named in the refusal so the client
        /// learns why its value was not taken.
        behaviour: &'static str,
    },
    /// Refused whatever the value, with the reason.
    Refused(&'static str),
    /// Reported by `SHOW` but not settable, as in PostgreSQL (`pg_settings.context`
    /// of `internal`).
    ReadOnly,
}

/// One runtime parameter: how PostgreSQL spells and describes it, and what
/// VaireDB will accept for it.
struct ParamSpec {
    /// Canonical spelling, as PostgreSQL reports it from `SHOW` and
    /// `pg_settings.name` — `DateStyle`, not `datestyle`. Lookup is
    /// case-insensitive; this is what is reported back.
    name: &'static str,
    /// The value used when startup announced none. Startup takes precedence, so
    /// this is only reached for parameters pgwire does not announce.
    default: &'static str,
    /// `pg_settings.short_desc`, and the `description` column of `SHOW ALL`.
    short_desc: &'static str,
    disposition: Disposition,
}

/// True for the PostgreSQL spellings of boolean `on`.
fn is_on(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1")
}

/// True for the PostgreSQL spellings of boolean `off`.
fn is_off(v: &str) -> bool {
    matches!(
        v.to_ascii_lowercase().as_str(),
        "off" | "false" | "no" | "0"
    )
}

/// Every parameter VaireDB models. A name absent here is an unrecognized
/// configuration parameter (`42704`), which is what PostgreSQL reports too — so a
/// client that sets something nobody has considered learns that, rather than
/// having it silently swallowed.
const PARAMS: &[ParamSpec] = &[
    ParamSpec {
        name: "application_name",
        default: "",
        short_desc: "Sets the application name to be reported in statistics and logs.",
        // Purely a label: VaireDB reports it back and nothing else reads it.
        disposition: Disposition::Free,
    },
    ParamSpec {
        name: "client_encoding",
        default: "UTF8",
        short_desc: "Sets the client's character set encoding.",
        disposition: Disposition::Matching {
            accepts: |v| {
                matches!(
                    v.to_ascii_uppercase().replace('-', "").as_str(),
                    "UTF8" | "UNICODE"
                )
            },
            behaviour: "the result encoder emits UTF-8 only",
        },
    },
    ParamSpec {
        name: "client_min_messages",
        default: "notice",
        short_desc: "Sets the message levels that are sent to the client.",
        // VaireDB emits no notices, warnings or debug messages at all, so every
        // threshold filters the same empty set.
        disposition: Disposition::Free,
    },
    ParamSpec {
        name: "DateStyle",
        default: "ISO, YMD",
        short_desc: "Sets the display format for date and time values.",
        disposition: Disposition::Matching {
            // Only the output half is constrained: the encoder renders dates ISO
            // (`2024-01-31`). The input half (MDY/DMY/YMD) orders ambiguous date
            // *literals*, which DuckDB and DataFusion both read as ISO — so any of
            // the three orderings leaves an unambiguous ISO literal alone, and none
            // of them would make `01/31/2024` parse.
            accepts: |v| {
                let mut parts = v.split(',').map(str::trim);
                parts.next().is_some_and(|s| s.eq_ignore_ascii_case("ISO"))
                    && parts.all(|order| {
                        matches!(order.to_ascii_uppercase().as_str(), "MDY" | "YMD" | "DMY")
                    })
            },
            behaviour: "date and time values are always rendered ISO",
        },
    },
    ParamSpec {
        name: "default_transaction_read_only",
        default: "off",
        short_desc: "Sets the default read-only status of new transactions.",
        disposition: Disposition::Matching {
            // A read-only block is opened with `BEGIN READ ONLY`; nothing consults a
            // session default when a block opens, so accepting `on` would leave
            // every new transaction writable while the session claimed otherwise.
            accepts: is_off,
            behaviour: "a transaction is read-only only when BEGIN says so",
        },
    },
    ParamSpec {
        name: "extra_float_digits",
        default: "1",
        short_desc: "Sets the number of digits displayed for floating-point values.",
        disposition: Disposition::Matching {
            // The encoder emits the shortest representation that round-trips, which
            // is what PostgreSQL does for any value above 0. A value at or below 0
            // asks for *fewer* digits — a truncation VaireDB does not do — and
            // anything above 3 is outside PostgreSQL's own range.
            accepts: |v| matches!(v.trim().parse::<i32>(), Ok(1..=3)),
            behaviour: "floating-point values are always rendered at full precision",
        },
    },
    ParamSpec {
        name: "in_hot_standby",
        default: "off",
        short_desc: "Shows whether hot standby is currently active.",
        disposition: Disposition::ReadOnly,
    },
    ParamSpec {
        name: "integer_datetimes",
        default: "on",
        short_desc: "Shows whether datetimes are integer based.",
        disposition: Disposition::ReadOnly,
    },
    ParamSpec {
        name: "IntervalStyle",
        default: "postgres",
        short_desc: "Sets the display format for interval values.",
        disposition: Disposition::Matching {
            accepts: |v| v.eq_ignore_ascii_case("postgres"),
            behaviour: "interval values are always rendered in the postgres style",
        },
    },
    ParamSpec {
        name: "is_superuser",
        default: "on",
        short_desc: "Shows whether the current user is a superuser.",
        disposition: Disposition::ReadOnly,
    },
    ParamSpec {
        name: "role",
        // PostgreSQL's own value for "no role has been assumed".
        default: "none",
        short_desc: "Sets the current role.",
        // Refused for the reason `session_authorization` is, and declared here so
        // `RESET ROLE` — which a pooler issues on checkout — succeeds: it asks for
        // the default, and no role assumed *is* VaireDB's state. `SET ROLE` has its
        // own parse shape and is refused there, by name.
        disposition: Disposition::Refused(
            "VaireDB runs every statement as the connecting user; there are no roles to assume",
        ),
    },
    ParamSpec {
        name: "search_path",
        default: "public",
        short_desc: "Sets the schema search order for names that are not schema-qualified.",
        // The one parameter on this list refused rather than constrained. VaireDB
        // resolves an unqualified relation to the default schema and nothing else:
        // the catalog key *is* the qualified name, and a relation elsewhere is
        // reached by naming its schema. Recording a search path would leave every
        // unqualified name resolving against `public` while the session reported
        // otherwise — a wrong answer, not a missing feature.
        //
        // `RESET search_path` still succeeds, because it asks for the default and
        // the default is the behaviour VaireDB has.
        disposition: Disposition::Refused(
            "VaireDB resolves an unqualified relation to the default schema only; \
             name the schema instead (schema.relation)",
        ),
    },
    ParamSpec {
        name: "server_encoding",
        default: "UTF8",
        short_desc: "Shows the server-side character set encoding.",
        disposition: Disposition::ReadOnly,
    },
    ParamSpec {
        name: "server_version",
        default: "16.6",
        short_desc: "Shows the server version.",
        disposition: Disposition::ReadOnly,
    },
    ParamSpec {
        name: "session_authorization",
        default: "",
        short_desc: "Sets the session user name.",
        // Refused rather than read-only: PostgreSQL lets a superuser change it, so a
        // client told `OK` would be one acting under an identity the coordinator
        // never switched to.
        disposition: Disposition::Refused(
            "VaireDB runs every statement as the connecting user; there is no role to switch to",
        ),
    },
    ParamSpec {
        name: "standard_conforming_strings",
        default: "on",
        short_desc: "Causes '...' strings to treat backslashes literally.",
        disposition: Disposition::Matching {
            // Both parsers read `\` in a single-quoted string literally, so `off`
            // would change what every escape in every literal means.
            accepts: is_on,
            behaviour: "backslashes in '...' strings are always literal",
        },
    },
    ParamSpec {
        name: "statement_timeout",
        default: "0",
        short_desc: "Sets the maximum allowed duration of any statement.",
        disposition: Disposition::Matching {
            // Nothing cancels a running statement. A client that set a timeout and
            // got an OK would wait indefinitely on the query it expected to be
            // aborted, so only "no timeout" is accepted.
            accepts: |v| v.trim().trim_end_matches("ms").trim().parse::<i64>() == Ok(0),
            behaviour: "VaireDB does not cancel a running statement",
        },
    },
    ParamSpec {
        name: "TimeZone",
        default: "Etc/UTC",
        short_desc: "Sets the time zone for displaying and interpreting time stamps.",
        disposition: Disposition::Matching {
            // Timestamps are stored and rendered in UTC throughout; another zone
            // would shift every `timestamptz` the client reads.
            accepts: |v| {
                matches!(
                    v.to_ascii_uppercase().as_str(),
                    "UTC" | "ETC/UTC" | "GMT" | "ETC/GMT" | "UNIVERSAL" | "Z"
                )
            },
            behaviour: "timestamps are stored and rendered in UTC",
        },
    },
    ParamSpec {
        name: "transaction_isolation",
        // The weakest level, because it is the one VaireDB can stand behind: a
        // multi-shard commit is applied one node-set transaction at a time, so a
        // concurrent reader can see it half-applied. Reporting `read committed`
        // would make the claim PostgreSQL's default makes and VaireDB cannot.
        //
        // Reported rather than refused because JDBC calls
        // `SHOW TRANSACTION ISOLATION LEVEL` from `getTransactionIsolation()`, and
        // an honest weakest answer serves a client better than an exception.
        // `SET TRANSACTION ISOLATION LEVEL` stays refused — see [`other_set_form`].
        default: "read uncommitted",
        short_desc: "Shows the isolation level of the current transaction.",
        disposition: Disposition::ReadOnly,
    },
];

/// The spec for `name`, matched case-insensitively as PostgreSQL matches a
/// parameter name.
fn spec_for(name: &str) -> Option<&'static ParamSpec> {
    PARAMS.iter().find(|p| p.name.eq_ignore_ascii_case(name))
}

/// One connection's runtime parameters.
///
/// Values are keyed by [`ParamSpec::name`] — the canonical spelling — so a
/// parameter set as `datestyle` and read as `DateStyle` is one entry.
#[derive(Debug, Default)]
pub(crate) struct SessionParams {
    /// The value each parameter had when the session opened: what
    /// `ParameterStatus` announced at startup. `SHOW` falls back to it and `RESET`
    /// restores it, which is what PostgreSQL's `reset_val` is.
    initial: HashMap<&'static str, String>,
    /// Values `SET` since the session opened, shadowing [`Self::initial`].
    overrides: HashMap<&'static str, String>,
}

impl SessionParams {
    /// Seed from the `ParameterStatus` values pgwire announced to `client` at
    /// startup, so `SHOW` reports what the client was already told rather than a
    /// second, independently-maintained copy of the same defaults.
    ///
    /// Announced names VaireDB does not model are dropped: they are pgwire's own
    /// (`scram_iterations`), not PostgreSQL settings a client can `SHOW`.
    pub(crate) fn for_client<C: ClientInfo>(client: &C) -> Self {
        let announced = DefaultServerParameterProvider::default()
            .server_parameters(client)
            .unwrap_or_default();

        Self {
            initial: announced
                .into_iter()
                .filter_map(|(name, value)| Some((spec_for(&name)?.name, value)))
                .collect(),
            overrides: HashMap::new(),
        }
    }

    /// The current value of `spec`: what was `SET`, else what startup announced,
    /// else the spec's own default.
    fn value_of(&self, spec: &'static ParamSpec) -> &str {
        self.overrides
            .get(spec.name)
            .or_else(|| self.initial.get(spec.name))
            .map_or(spec.default, String::as_str)
    }

    /// Record `value` for `name`, or refuse it. The refusal is the point of the
    /// method: see [`Disposition`].
    fn set(&mut self, name: &str, value: &str) -> PgWireResult<()> {
        let spec = spec_for(name).ok_or_else(|| unrecognized_parameter(name))?;
        match spec.disposition {
            Disposition::Free => {}
            Disposition::Matching { accepts, behaviour } => {
                if !accepts(value) {
                    return Err(make_vdb_error(
                        VdbErrorCode::InvalidParameterValue,
                        format!(
                            "{} cannot be set to \"{value}\" on VaireDB: {behaviour}",
                            spec.name
                        ),
                    ));
                }
            }
            Disposition::Refused(reason) => {
                return Err(make_vdb_error(
                    VdbErrorCode::FeatureNotSupported,
                    format!("SET {} is not supported by VaireDB: {reason}", spec.name),
                ));
            }
            Disposition::ReadOnly => {
                return Err(make_vdb_error(
                    VdbErrorCode::CantChangeRuntimeParam,
                    format!("parameter \"{}\" cannot be changed", spec.name),
                ));
            }
        }
        // Stored as the client spelled it rather than folded to the spec's default:
        // an accepted value is one VaireDB's behaviour already matches, so echoing
        // the client's own spelling back is both true and the least surprising thing
        // a `SHOW` could report.
        self.overrides.insert(spec.name, value.to_string());
        Ok(())
    }

    /// Discard the session value of `name`, restoring what startup announced.
    /// Refuses an unrecognized name, as `SET` does — `RESET x` *is*
    /// `SET x TO DEFAULT`.
    ///
    /// Succeeds for a read-only or refused parameter: neither can have an override
    /// to discard, so asking for the default is asking for the state the session is
    /// already in.
    fn reset(&mut self, name: &str) -> PgWireResult<()> {
        let spec = spec_for(name).ok_or_else(|| unrecognized_parameter(name))?;
        self.overrides.remove(spec.name);
        Ok(())
    }

    /// Discard every session value (`RESET ALL`).
    fn reset_all(&mut self) {
        self.overrides.clear();
    }

    /// `(name, setting, short_desc)` for every modelled parameter, ordered by name
    /// as PostgreSQL's `SHOW ALL` orders it.
    fn show_all(&self) -> Vec<(&'static str, String, &'static str)> {
        let mut rows: Vec<_> = PARAMS
            .iter()
            .map(|spec| (spec.name, self.value_of(spec).to_string(), spec.short_desc))
            .collect();
        rows.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());
        rows
    }
}

/// `42704` — the name is not a configuration parameter VaireDB models. What
/// PostgreSQL reports for the same statement, so a client can tell a typo from a
/// refusal.
fn unrecognized_parameter(name: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::UndefinedObject,
        format!("unrecognized configuration parameter \"{name}\""),
    )
}

/// `0A000`, naming the `SET` form rather than just "SET", so a client that sent
/// `SET ROLE` is not told that `SET` in general is unsupported.
fn unsupported_set_form(form: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!("{form} is not supported by VaireDB"),
    )
}

/// `0A000`: `SET LOCAL` scopes a value to the enclosing transaction, so it has to
/// be undone when the block ends. The coordinator's block buffers writes and holds
/// no parameter snapshot to restore, so the value would outlive its transaction —
/// refused rather than leaked into the session.
fn set_local_error() -> PgWireError {
    make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        "SET LOCAL is not supported by VaireDB: a parameter set inside a transaction block \
         is not rolled back with it; use SET",
    )
}

/// `22023`: the value is an expression, not something a setting can hold.
fn non_literal_value(name: &str) -> PgWireError {
    make_vdb_error(
        VdbErrorCode::InvalidParameterValue,
        format!("the value for \"{name}\" must be a literal or a bare word, not an expression"),
    )
}

/// The parameter name a `SET` target spells.
///
/// A multi-part name (`SET plpgsql.extra_errors = 'all'`) is an extension GUC, and
/// VaireDB models no extensions, so it joins back to a name no spec matches and is
/// reported as unrecognized — which is what PostgreSQL does for an extension it has
/// not loaded.
fn param_name(variable: &ObjectName) -> String {
    variable
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

/// The string a `SET` value list carries, as PostgreSQL's `SHOW` would report it:
/// one value plain, several joined with `, ` (which is how `DateStyle` takes a
/// list).
///
/// `None` for an expression that is not a literal or a bare word, so the caller
/// refuses it rather than stringifying an arbitrary expression into a setting.
fn value_text(values: &[Expr]) -> Option<String> {
    let parts: Option<Vec<String>> = values.iter().map(single_value_text).collect();
    Some(parts?.join(", "))
}

/// One `SET` value as text, or `None` if it is not something a setting can be.
fn single_value_text(value: &Expr) -> Option<String> {
    match value {
        // `SET DateStyle TO ISO`, `SET x TO on`, `SET x TO DEFAULT`.
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
            Value::Number(n, _) => Some(n.clone()),
            // PostgreSQL reports a boolean setting as on/off, whatever set it.
            Value::Boolean(true) => Some("on".to_string()),
            Value::Boolean(false) => Some("off".to_string()),
            _ => None,
        },
        // `SET extra_float_digits = -1`: the sign is a unary operator, not part of
        // the number token, and the value has to carry it or the refusal would read
        // as though `1` had been rejected.
        Expr::UnaryOp { op, expr } => {
            let inner = single_value_text(expr)?;
            match op {
                UnaryOperator::Minus => Some(format!("-{inner}")),
                UnaryOperator::Plus => Some(inner),
                _ => None,
            }
        }
        _ => None,
    }
}

/// True when a `SET` value list is the single word `DEFAULT`, which PostgreSQL
/// defines as equivalent to `RESET`.
fn is_default_keyword(values: &[Expr]) -> bool {
    matches!(
        values,
        [Expr::Identifier(Ident { value, .. })] if value.eq_ignore_ascii_case("DEFAULT")
    )
}

/// The name `RESET ALL` is rewritten to target — see [`parse_reset`].
const RESET_ALL: &str = "all";

/// Recognize a lone `RESET` statement and return the `SET … TO DEFAULT` AST
/// PostgreSQL defines it to be equivalent to.
///
/// sqlparser's `PostgreSqlDialect` has no `RESET`, so without this the statement
/// fails at parse with `42601` — one step before the routing that could answer it.
/// Rewriting is sound rather than a shim: PostgreSQL documents `RESET x` as
/// `SET x TO DEFAULT`, so the two really are the same statement. (The compat
/// parser's own token rewrites, which turn `ABORT` into `ROLLBACK`, are private to
/// `datafusion_pg_catalog`, so this cannot hook into them.)
///
/// Deliberately narrow. Only a whole input that is one `RESET` statement (with at
/// most a trailing `;`) is recognized; a batch such as `SET a = 1; RESET a` falls
/// through to the parser and its `42601`, because splitting SQL on `;` here would
/// have to re-tokenize string literals to be correct. Drivers send `RESET` alone.
///
/// `RESET ALL` becomes an assignment to the name [`RESET_ALL`], which is not a
/// parameter any spec declares. The one statement that collides is
/// `SET all TO DEFAULT`, which PostgreSQL rejects as an unrecognized parameter and
/// VaireDB treats as `RESET ALL`; no valid PostgreSQL depends on that error.
pub(super) fn parse_reset(sql: &str) -> Option<Vec<Statement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let (keyword, rest) = trimmed.split_at_checked("RESET".len())?;
    if !keyword.eq_ignore_ascii_case("RESET") {
        return None;
    }
    // `RESETTING` starts with those five letters but is an identifier, not the
    // keyword. `RESET` with no target is a syntax error, and stays one.
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let target = rest.trim();

    // The multi-word forms PostgreSQL spells out, mapped to the parameter each one
    // resets. Anything else has to be a single parameter name.
    let name = match target.to_ascii_uppercase().as_str() {
        "ALL" => RESET_ALL,
        "TIME ZONE" => "TimeZone",
        "SESSION AUTHORIZATION" => "session_authorization",
        _ if is_bare_name(target) => target,
        // `RESET ROLE`, or anything unrecognized: fall through to the parser so it
        // reports the syntax error rather than being quietly accepted here.
        _ => return None,
    };

    Some(vec![Statement::Set(Set::SingleAssignment {
        scope: None,
        hivevar: false,
        variable: ObjectName::from(vec![Ident::new(name)]),
        values: vec![Expr::Identifier(Ident::new("DEFAULT"))],
    })])
}

/// True for a single unquoted identifier, possibly dotted (`work_mem`,
/// `plpgsql.extra_errors`) — the only shape a parameter name takes.
fn is_bare_name(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|part| {
            part.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        })
}

/// What a `SHOW` names.
enum ShowTarget {
    One(&'static ParamSpec),
    All,
}

/// Resolve a `SHOW`'s target.
///
/// `variable` is whatever words followed `SHOW`: sqlparser's `parse_identifiers`
/// takes every word up to the end of the statement, so the spelled-out multi-word
/// settings arrive as several idents and are matched as phrases. (The `SHOW TABLES`
/// / `SHOW COLUMNS` / `SHOW SCHEMAS` family never reaches here — each parses into a
/// statement of its own and keeps its own refusal.)
fn show_target(variable: &[Ident]) -> PgWireResult<ShowTarget> {
    let phrase = variable
        .iter()
        .map(|i| i.value.as_str())
        .collect::<Vec<_>>()
        .join(" ");

    let name = match phrase.to_ascii_uppercase().as_str() {
        "ALL" => return Ok(ShowTarget::All),
        // PostgreSQL's spelled-out synonyms.
        "TIME ZONE" => "TimeZone",
        "TRANSACTION ISOLATION LEVEL" => "transaction_isolation",
        _ => phrase.as_str(),
    };

    spec_for(name)
        .map(ShowTarget::One)
        .ok_or_else(|| unrecognized_parameter(&phrase))
}

/// A `text`-typed result column. Every setting is reported as text, as PostgreSQL
/// reports it: `SHOW` has no per-parameter result type.
fn text_field(name: &str, format: &Format, idx: usize) -> FieldInfo {
    FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::VARCHAR,
        format.format_for(idx),
    )
}

/// The result columns a `SHOW` produces: one named after the parameter, or the
/// three of `SHOW ALL`.
fn show_fields(target: &ShowTarget, format: &Format) -> Vec<FieldInfo> {
    match target {
        // PostgreSQL labels the single column with the parameter's canonical name,
        // and a client keys its result map on that.
        ShowTarget::One(spec) => vec![text_field(spec.name, format, 0)],
        ShowTarget::All => vec![
            text_field("name", format, 0),
            text_field("setting", format, 1),
            text_field("description", format, 2),
        ],
    }
}

/// The result columns a session-parameter statement produces: `SHOW`'s columns, or
/// none for `SET`/`RESET`.
///
/// Describe and Execute share it, so the `RowDescription` a client is promised at
/// Describe is the one the `DataRow`s answer.
pub(super) fn result_fields(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let Statement::ShowVariable { variable } = stmt else {
        return Ok(vec![]);
    };
    Ok(show_fields(&show_target(variable)?, format))
}

/// Execute a `SET`, `SHOW` or `RESET` against the connection's own parameters.
pub(super) async fn handle_session_param(
    stmt: &Statement,
    session: &SessionState,
    format: &Format,
) -> PgWireResult<Response> {
    match stmt {
        Statement::Set(set) => apply_set(set, session).await,
        Statement::ShowVariable { variable } => show(variable, session, format).await,
        // Unreachable: the classifier routes exactly the two statements above here.
        // A third must be handled, not answered out of the parameter registry.
        _ => Err(make_vdb_error(
            VdbErrorCode::InternalError,
            "unhandled session parameter statement",
        )),
    }
}

/// Apply a `SET` (or the `SET … TO DEFAULT` a `RESET` became).
async fn apply_set(set: &Set, session: &SessionState) -> PgWireResult<Response> {
    // `SET TIME ZONE <v>` is PostgreSQL's spelled-out synonym for
    // `SET timezone TO <v>`, and sqlparser gives it a variant of its own.
    if let Set::SetTimeZone { local, value } = set {
        if *local {
            return Err(set_local_error());
        }
        let text = single_value_text(value).ok_or_else(|| non_literal_value("TimeZone"))?;
        session.params().await.set("TimeZone", &text)?;
        return Ok(Response::Execution(Tag::new("SET")));
    }

    let Set::SingleAssignment {
        scope,
        hivevar,
        variable,
        values,
    } = set
    else {
        return Err(unsupported_set_form(other_set_form(set)));
    };

    if *hivevar {
        return Err(unsupported_set_form("SET HIVEVAR"));
    }
    match scope {
        // `SET SESSION x = v` is plain `SET` in PostgreSQL.
        None | Some(ContextModifier::Session) => {}
        Some(ContextModifier::Local) => return Err(set_local_error()),
        Some(ContextModifier::Global) => return Err(unsupported_set_form("SET GLOBAL")),
    }

    let name = param_name(variable);
    let mut params = session.params().await;

    if is_default_keyword(values) {
        if name.eq_ignore_ascii_case(RESET_ALL) {
            params.reset_all();
        } else {
            params.reset(&name)?;
        }
        // PostgreSQL tags a `RESET` as `RESET` and a `SET x TO DEFAULT` as `SET`.
        // They arrive here as the same AST, so both are tagged `SET`: the tag
        // carries no row count and no client branches on it, whereas conflating the
        // two *values* would be a real difference.
        return Ok(Response::Execution(Tag::new("SET")));
    }

    let value = value_text(values).ok_or_else(|| non_literal_value(&name))?;
    params.set(&name, &value)?;
    Ok(Response::Execution(Tag::new("SET")))
}

/// The `SET` forms VaireDB does not model, named for the refusal.
///
/// Each is refused rather than mapped onto a parameter because each changes
/// something the coordinator does not implement — an identity, an isolation level,
/// a character set — and a client told `OK` would act as though it had.
fn other_set_form(set: &Set) -> &'static str {
    match set {
        // Both are handled before this point.
        Set::SingleAssignment { .. } => "SET",
        Set::SetTimeZone { .. } => "SET TIME ZONE",
        Set::ParenthesizedAssignments { .. } => "SET (a, b) = (1, 2)",
        Set::MultipleAssignments { .. } => "SET with several assignments",
        Set::SetSessionAuthorization(_) => "SET SESSION AUTHORIZATION",
        Set::SetSessionParam(_) => "SET <session parameter>",
        Set::SetRole { .. } => "SET ROLE",
        Set::SetNames { .. } => "SET NAMES",
        Set::SetNamesDefault {} => "SET NAMES DEFAULT",
        Set::SetTransaction { .. } => "SET TRANSACTION",
    }
}

/// Answer a `SHOW`.
async fn show(
    variable: &[Ident],
    session: &SessionState,
    format: &Format,
) -> PgWireResult<Response> {
    let target = show_target(variable)?;
    let fields = Arc::new(show_fields(&target, format));
    let params = session.params().await;

    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    let mut rows = Vec::new();
    match target {
        ShowTarget::One(spec) => push_row(&mut encoder, &[params.value_of(spec)], &mut rows)?,
        ShowTarget::All => {
            for (name, setting, description) in params.show_all() {
                push_row(&mut encoder, &[name, &setting, description], &mut rows)?;
            }
        }
    }

    Ok(Response::Query(QueryResponse::new(
        fields,
        futures::stream::iter(rows),
    )))
}

/// Encode one all-text row and append it to `rows`, leaving `encoder` ready for the
/// next one.
fn push_row(
    encoder: &mut DataRowEncoder,
    cells: &[&str],
    rows: &mut Vec<PgWireResult<DataRow>>,
) -> PgWireResult<()> {
    for cell in cells {
        encoder.encode_field(cell)?;
    }
    rows.push(Ok(encoder.take_row()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use pgwire::api::DefaultClient;

    use super::*;

    /// The SQLSTATE an error carries. What a client branches on, so it is what the
    /// tests assert rather than the message.
    fn sqlstate(err: &PgWireError) -> String {
        match err {
            PgWireError::UserError(info) => info.code.clone(),
            other => panic!("expected a user error, got {other}"),
        }
    }

    fn a_client() -> DefaultClient<()> {
        DefaultClient::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5432),
            false,
        )
    }

    /// Parse `sql` as the one statement it is, with the plain PostgreSQL dialect —
    /// or via [`parse_reset`], which is where `RESET` is recognized.
    fn parse_one(sql: &str) -> Statement {
        if let Some(mut statements) = parse_reset(sql) {
            return statements.remove(0);
        }
        let mut statements = crate::sqlparser::parser::Parser::parse_sql(
            &crate::sqlparser::dialect::PostgreSqlDialect {},
            sql,
        )
        .unwrap_or_else(|e| panic!("`{sql}` should parse: {e}"));
        assert_eq!(statements.len(), 1, "`{sql}` should be one statement");
        statements.remove(0)
    }

    /// Apply a `SET`/`RESET` to `params` the way the handler does, so the tests
    /// exercise the AST shapes a client actually sends rather than calling
    /// [`SessionParams::set`] directly.
    fn apply(params: &mut SessionParams, sql: &str) -> PgWireResult<()> {
        match parse_one(sql) {
            Statement::Set(Set::SetTimeZone {
                local: false,
                value,
            }) => {
                let text =
                    single_value_text(&value).ok_or_else(|| non_literal_value("TimeZone"))?;
                params.set("TimeZone", &text)
            }
            Statement::Set(Set::SingleAssignment {
                variable, values, ..
            }) => {
                let name = param_name(&variable);
                if is_default_keyword(&values) {
                    if name.eq_ignore_ascii_case(RESET_ALL) {
                        params.reset_all();
                        return Ok(());
                    }
                    return params.reset(&name);
                }
                let value = value_text(&values).ok_or_else(|| non_literal_value(&name))?;
                params.set(&name, &value)
            }
            other => panic!("`{sql}` is not a plain SET: {other:?}"),
        }
    }

    /// The value `SHOW <target>` would report.
    fn show_value(params: &SessionParams, target: &str) -> String {
        let Statement::ShowVariable { variable } = parse_one(&format!("SHOW {target}")) else {
            panic!("`SHOW {target}` should be a ShowVariable");
        };
        match show_target(&variable).unwrap() {
            ShowTarget::One(spec) => params.value_of(spec).to_string(),
            ShowTarget::All => panic!("`SHOW {target}` names every parameter"),
        }
    }

    // The defaults are pgwire's, not this module's: a session is seeded from the
    // `ParameterStatus` messages pgwire already sent. That only stays true while
    // every name it announces is one `PARAMS` declares — a name pgwire announces
    // and this module does not know is a value the client was told on connect and
    // would then be refused by `SHOW`, which is exactly the drift the seeding was
    // meant to rule out. So the announcement is compared against the registry
    // rather than trusted.
    #[test]
    fn every_announced_startup_parameter_has_a_spec() {
        // pgwire's own, not a PostgreSQL setting: it reports the SCRAM iteration
        // count so a client can size its key derivation. Nothing would `SHOW` it.
        const PGWIRE_ONLY: &[&str] = &["scram_iterations"];

        let announced = DefaultServerParameterProvider::default()
            .server_parameters(&a_client())
            .expect("pgwire announces startup parameters");
        assert!(
            !announced.is_empty(),
            "the seeding is pointless if pgwire announces nothing"
        );

        for name in announced.keys() {
            assert!(
                spec_for(name).is_some() || PGWIRE_ONLY.contains(&name.as_str()),
                "pgwire announces `{name}` at startup but PARAMS does not declare it, so SHOW \
                 would report 42704 for a value the client was already told. Add a spec for it, \
                 or add it to PGWIRE_ONLY if it is not a PostgreSQL setting."
            );
        }
    }

    #[test]
    fn seeds_from_the_startup_parameters_the_client_was_sent() {
        let announced = DefaultServerParameterProvider::default()
            .server_parameters(&a_client())
            .unwrap();
        let params = SessionParams::for_client(&a_client());

        for (name, value) in &announced {
            if let Some(spec) = spec_for(name) {
                assert_eq!(
                    params.value_of(spec),
                    value,
                    "SHOW {name} must report the value announced at startup"
                );
            }
        }
    }

    #[test]
    fn accepts_the_values_the_coordinator_matches() {
        let mut params = SessionParams::default();
        for sql in [
            "SET application_name = 'vairedb'",
            "SET client_encoding TO 'UTF8'",
            "SET client_encoding TO 'utf-8'",
            "SET extra_float_digits = 3",
            "SET DateStyle TO 'ISO, MDY'",
            "SET DateStyle TO ISO",
            "SET IntervalStyle = 'postgres'",
            "SET standard_conforming_strings = on",
            "SET statement_timeout = 0",
            "SET TimeZone TO 'UTC'",
            "SET TIME ZONE 'Etc/UTC'",
            "SET default_transaction_read_only = false",
            "SET client_min_messages TO warning",
            // The session scope is what plain `SET` means in PostgreSQL.
            "SET SESSION application_name = 'explicit-scope'",
        ] {
            apply(&mut params, sql)
                .unwrap_or_else(|e| panic!("`{sql}` should be accepted: {}", sqlstate(&e)));
        }
    }

    // The core of the module: a value the read path does not honour is refused
    // rather than remembered. Recording it would have `SHOW` promise a rendering or
    // a resolution rule that nothing applies, and the client would never find out.
    #[test]
    fn refuses_a_value_the_coordinator_does_not_match() {
        let mut params = SessionParams::default();
        for sql in [
            "SET client_encoding TO 'LATIN1'",
            "SET DateStyle TO 'German'",
            "SET DateStyle TO 'SQL, MDY'",
            "SET IntervalStyle = 'iso_8601'",
            "SET standard_conforming_strings = off",
            "SET statement_timeout = 5000",
            "SET TimeZone TO 'Europe/Rome'",
            "SET default_transaction_read_only = on",
            "SET extra_float_digits = 0",
            "SET extra_float_digits = -1",
        ] {
            let err = apply(&mut params, sql).expect_err(&format!("`{sql}` should be refused"));
            assert_eq!(
                sqlstate(&err),
                "22023",
                "`{sql}` should be an invalid parameter *value*, not a missing feature"
            );
        }
    }

    // A refused value must leave nothing behind: if the assignment were recorded
    // before the check, a client that ignored the error would read its own value
    // back from `SHOW` and conclude it had taken effect.
    #[test]
    fn a_refused_value_does_not_become_the_session_value() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET TimeZone TO 'Europe/Rome'").unwrap_err();
        assert_eq!(show_value(&params, "TimeZone"), "Etc/UTC");
    }

    #[test]
    fn refuses_an_unknown_parameter() {
        let mut params = SessionParams::default();
        // `42704` rather than `0A000`: the client's mistake is the name, and a
        // driver told "unsupported" would stop trying instead of fixing its typo.
        for sql in [
            "SET work_mem = '64MB'",
            "SET searchpath TO public",
            // An extension GUC. VaireDB loads no extensions, so the name is
            // unrecognized — which is what PostgreSQL reports too.
            "SET plpgsql.extra_errors = 'all'",
        ] {
            let err = apply(&mut params, sql).expect_err(&format!("`{sql}` should be refused"));
            assert_eq!(sqlstate(&err), "42704", "`{sql}` names no known parameter");
        }
    }

    #[test]
    fn refuses_a_read_only_parameter() {
        let mut params = SessionParams::default();
        for sql in [
            "SET server_version = '9.5'",
            "SET is_superuser = off",
            "SET transaction_isolation = 'serializable'",
        ] {
            let err = apply(&mut params, sql).expect_err(&format!("`{sql}` should be refused"));
            assert_eq!(
                sqlstate(&err),
                "55P02",
                "`{sql}` names a parameter fixed at startup"
            );
        }
    }

    // `search_path` is the one parameter refused by name rather than by value:
    // VaireDB resolves an unqualified relation to the default schema and nothing
    // else, so no search path it could record would be honoured.
    #[test]
    fn refuses_search_path_by_name() {
        let mut params = SessionParams::default();
        for sql in [
            "SET search_path TO myschema",
            // Refused even when the value happens to describe what VaireDB does:
            // accepting it would mean accepting the parameter, and the next
            // statement would set something else.
            "SET search_path TO public",
        ] {
            let err = apply(&mut params, sql).expect_err(&format!("`{sql}` should be refused"));
            assert_eq!(
                sqlstate(&err),
                "0A000",
                "`{sql}` should be a missing feature"
            );
        }
    }

    // The asymmetry that follows from the refusal above, and the reason it is
    // deliberate: `RESET` asks for the default, and the default is the behaviour
    // VaireDB has. A driver that resets the session on checkout gets an `OK` that
    // is true, while `SET` still cannot lie.
    #[test]
    fn reset_of_a_refused_or_read_only_parameter_succeeds() {
        let mut params = SessionParams::default();
        for sql in [
            "RESET search_path",
            "RESET session_authorization",
            "RESET SESSION AUTHORIZATION",
            "RESET transaction_isolation",
            // A pooler resets the role on checkout. Refusing it would break the
            // pool over a role VaireDB never assumed in the first place.
            "RESET ROLE",
        ] {
            apply(&mut params, sql)
                .unwrap_or_else(|e| panic!("`{sql}` should succeed: {}", sqlstate(&e)));
        }
    }

    #[test]
    fn reset_discards_the_session_value() {
        let mut params = SessionParams::for_client(&a_client());
        let announced = show_value(&params, "application_name");

        apply(&mut params, "SET application_name = 'mine'").unwrap();
        assert_eq!(show_value(&params, "application_name"), "mine");

        apply(&mut params, "RESET application_name").unwrap();
        assert_eq!(
            show_value(&params, "application_name"),
            announced,
            "RESET must restore what startup announced, not this module's own default"
        );
    }

    #[test]
    fn reset_all_discards_every_session_value() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET application_name = 'mine'").unwrap();
        apply(&mut params, "SET client_min_messages TO debug1").unwrap();

        apply(&mut params, "RESET ALL").unwrap();
        assert_eq!(show_value(&params, "application_name"), "");
        assert_eq!(show_value(&params, "client_min_messages"), "notice");
    }

    #[test]
    fn set_to_default_is_a_reset() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET application_name = 'mine'").unwrap();
        apply(&mut params, "SET application_name TO DEFAULT").unwrap();
        assert_eq!(show_value(&params, "application_name"), "");
    }

    // A parameter name is case-insensitive in PostgreSQL, but the *reported* name
    // is canonical: a client keys its result map on the column label `SHOW` sends,
    // so `SET datestyle` has to be readable as `DateStyle`.
    #[test]
    fn a_parameter_name_is_case_insensitive() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET timezone TO 'GMT'").unwrap();
        assert_eq!(show_value(&params, "TimeZone"), "GMT");
        assert_eq!(show_value(&params, "TIMEZONE"), "GMT");
        assert_eq!(spec_for("datestyle").unwrap().name, "DateStyle");
    }

    // The client's own spelling is reported back, not the equivalent VaireDB
    // matched it against: the reasoning about equivalence lives in `accepts`, and
    // reporting a different string than the client set would be the surprising
    // half of an accepted `SET`.
    #[test]
    fn an_accepted_value_is_reported_as_the_client_spelled_it() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET client_encoding TO 'utf-8'").unwrap();
        assert_eq!(show_value(&params, "client_encoding"), "utf-8");
    }

    #[test]
    fn refuses_an_expression_as_a_value() {
        let mut params = SessionParams::default();
        let err = apply(&mut params, "SET application_name = 1 + 1")
            .expect_err("an arithmetic expression is not a setting");
        assert_eq!(sqlstate(&err), "22023");
    }

    #[test]
    fn reads_a_value_list_the_way_show_reports_it() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET DateStyle TO 'ISO', 'MDY'").unwrap();
        assert_eq!(show_value(&params, "DateStyle"), "ISO, MDY");
    }

    #[test]
    fn refuses_set_local_and_the_other_set_forms() {
        // `SET LOCAL` is refused for a reason of its own: its scope is the
        // transaction, and the coordinator's block holds no parameter snapshot to
        // restore, so an accepted value would outlive the block that scoped it.
        assert_eq!(sqlstate(&set_local_error()), "0A000");
        assert_eq!(
            other_set_form(&Set::SetRole {
                context_modifier: None,
                role_name: None,
            }),
            "SET ROLE"
        );
        assert_eq!(sqlstate(&unsupported_set_form("SET ROLE")), "0A000");
    }

    #[test]
    fn parse_reset_rewrites_to_set_to_default() {
        for (sql, expected) in [
            ("RESET application_name", "application_name"),
            ("reset Application_Name", "Application_Name"),
            ("RESET application_name;", "application_name"),
            ("  RESET   application_name  ", "application_name"),
            ("RESET ALL", RESET_ALL),
            ("RESET TIME ZONE", "TimeZone"),
            ("RESET SESSION AUTHORIZATION", "session_authorization"),
        ] {
            let statements =
                parse_reset(sql).unwrap_or_else(|| panic!("`{sql}` should be a RESET"));
            let [
                Statement::Set(Set::SingleAssignment {
                    variable, values, ..
                }),
            ] = statements.as_slice()
            else {
                panic!("`{sql}` should rewrite to a single assignment: {statements:?}");
            };
            assert_eq!(param_name(variable), expected);
            assert!(
                is_default_keyword(values),
                "`{sql}` should assign DEFAULT, which is what RESET means"
            );
        }
    }

    // Everything else must fall through to the real parser, or `parse_reset` would
    // be shadowing statements it does not understand — including a statement batch,
    // which it cannot split without re-tokenizing string literals.
    #[test]
    fn parse_reset_leaves_every_other_statement_alone() {
        for sql in [
            "SELECT 1",
            "SET application_name = 'x'",
            // An identifier that merely starts with the five letters.
            "SELECT resetting FROM t",
            "RESETTING",
            // No target: a syntax error, and it stays one.
            "RESET",
            "RESET ",
            // Not a parameter name.
            "RESET 'quoted'",
            "RESET a, b",
            // A batch: splitting on `;` here would mis-handle a literal containing
            // one, so the parser gets it instead.
            "RESET a; SELECT 1",
        ] {
            assert!(
                parse_reset(sql).is_none(),
                "`{sql}` must reach the real parser"
            );
        }
    }

    // sqlparser's `parse_identifiers` takes every *word* after `SHOW` and skips the
    // punctuation between them, so the target arrives as a list of idents and has
    // to be matched as a space-joined phrase. Matching them dot-joined instead
    // would make every spelled-out form unrecognized.
    #[test]
    fn show_resolves_the_spelled_out_phrases() {
        for (target, expected) in [
            ("TimeZone", "TimeZone"),
            ("TIME ZONE", "TimeZone"),
            ("transaction_isolation", "transaction_isolation"),
            ("TRANSACTION ISOLATION LEVEL", "transaction_isolation"),
            ("search_path", "search_path"),
            ("DATESTYLE", "DateStyle"),
        ] {
            let Statement::ShowVariable { variable } = parse_one(&format!("SHOW {target}")) else {
                panic!("`SHOW {target}` should be a ShowVariable");
            };
            match show_target(&variable).unwrap() {
                ShowTarget::One(spec) => assert_eq!(spec.name, expected, "SHOW {target}"),
                ShowTarget::All => panic!("`SHOW {target}` is not SHOW ALL"),
            }
        }
    }

    // JDBC calls this from `getTransactionIsolation()`, and an exception there
    // fails a connection before it runs anything. The answer is the weakest level
    // because that is the one a multi-shard commit can stand behind.
    #[test]
    fn reports_the_isolation_level_jdbc_asks_for() {
        let params = SessionParams::default();
        assert_eq!(
            show_value(&params, "TRANSACTION ISOLATION LEVEL"),
            "read uncommitted"
        );
    }

    #[test]
    fn show_of_an_unknown_parameter_names_the_whole_phrase() {
        let Statement::ShowVariable { variable } = parse_one("SHOW no such thing") else {
            panic!("should be a ShowVariable");
        };
        let Err(err) = show_target(&variable) else {
            panic!("`SHOW no such thing` names no parameter");
        };
        assert_eq!(sqlstate(&err), "42704");
        match &err {
            PgWireError::UserError(info) => assert!(
                info.message.contains("no such thing"),
                "the refusal should name what the client asked for: {}",
                info.message
            ),
            other => panic!("expected a user error, got {other}"),
        }
    }

    #[test]
    fn show_all_lists_every_parameter_by_name() {
        let params = SessionParams::default();
        let rows = params.show_all();
        assert_eq!(rows.len(), PARAMS.len());

        let names: Vec<&str> = rows.iter().map(|(name, _, _)| *name).collect();
        let mut sorted = names.clone();
        sorted.sort_by_key(|name| name.to_ascii_lowercase());
        assert_eq!(
            names, sorted,
            "SHOW ALL is ordered by name, as PostgreSQL is"
        );

        assert!(rows.iter().all(|(_, _, desc)| !desc.is_empty()));
    }

    #[test]
    fn show_all_reports_the_current_value() {
        let mut params = SessionParams::default();
        apply(&mut params, "SET application_name = 'mine'").unwrap();
        let rows = params.show_all();
        let (_, setting, _) = rows
            .iter()
            .find(|(name, _, _)| *name == "application_name")
            .unwrap();
        assert_eq!(setting, "mine");
    }

    // Describe and Execute derive their columns from the same function, so a client
    // cannot be promised a row description that the rows then contradict.
    #[test]
    fn show_describes_the_columns_it_returns() {
        let format = Format::UnifiedText;

        let one = result_fields(&parse_one("SHOW DateStyle"), &format).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].name(), "DateStyle");
        assert_eq!(one[0].datatype(), &Type::VARCHAR);

        let all = result_fields(&parse_one("SHOW ALL"), &format).unwrap();
        let labels: Vec<&str> = all.iter().map(FieldInfo::name).collect();
        assert_eq!(labels, ["name", "setting", "description"]);

        // A `SET` returns no rows, so it describes no columns.
        let none = result_fields(&parse_one("SET application_name = 'x'"), &format).unwrap();
        assert!(none.is_empty());
    }

    // Every spec's own default must be one it would accept, or `RESET` could land
    // the session on a value `SET` refuses — a state no statement could have
    // produced.
    #[test]
    fn every_default_is_a_value_the_spec_accepts() {
        for spec in PARAMS {
            if let Disposition::Matching { accepts, .. } = spec.disposition {
                assert!(
                    accepts(spec.default),
                    "the default `{}` for {} is a value SET would refuse",
                    spec.default,
                    spec.name
                );
            }
        }
    }
}
