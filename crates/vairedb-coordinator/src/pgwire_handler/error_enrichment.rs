//! Turns internal coordinator/engine errors into rich PostgreSQL-style error
//! responses. Classifies an error into a `VdbErrorCode` and SQLSTATE, attaches
//! a client-facing `DETAIL` and `HINT` where useful, and sanitizes messages so
//! internal details (node IDs, storage internals, wire offsets) never leak to
//! clients.

use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use vairedb_common::error::{
    TransportedError, TransportedVariant, VaireDbError, code_of_tagged_message,
    recover_transported_error, sanitize_message, sqlstate_for_code,
};
use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{MetadataCatalog, ShardMeta, TableMeta};
use crate::error::CoordinatorError;
use crate::util::node_state_str;

/// Optional context threaded into error enrichment so `DETAIL`/`HINT` can name
/// the offending table and report the relevant replication factor.
#[derive(Default, Clone)]
pub struct ErrorContext {
    /// Relation the failing operation targeted, surfaced in the error's `table` field.
    pub table_name: Option<String>,
    /// Replication factor of the target table, used to enrich quorum hints.
    pub replication_factor: Option<u32>,
}

impl ErrorContext {
    /// Build a context naming the table the operation was acting on.
    pub fn for_table(name: &str) -> Self {
        Self {
            table_name: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// Attach the target table's replication factor for richer quorum hints.
    pub fn with_replication(mut self, factor: u32) -> Self {
        self.replication_factor = Some(factor);
        self
    }
}

/// The `ErrorInfo` every path in this module reports, before any of them adds the
/// context it happens to hold.
///
/// One place decides that severity is `ERROR`, that the SQLSTATE comes from the code
/// and that the message carries the `[VDB-NNNN]` prefix. Written out at each of the
/// four entry points — which is how this started — the three were free to disagree,
/// and a client reading SQLSTATE from one and the prefix from another would have been
/// told two different things about one failure.
fn reported(code: VdbErrorCode, message: &str) -> ErrorInfo {
    ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate_for_code(code).to_string(),
        VaireDbError::new(code, message).formatted_message(),
    )
}

/// Report `raw` under `code`, after giving [`reclassify_transported_error`] the chance
/// to recover a truer code from the text a transported error was rendered into.
///
/// The classifier is the caller's, because that is the only thing the untyped and the
/// `DataFusionError` path differ by: one infers the code from substrings, the other
/// reads it off the variant, and everything after that — recover, sanitize, format,
/// name the table — is the same sequence and has to stay the same sequence.
fn enrich_classified(raw: &str, code: VdbErrorCode, ctx: &ErrorContext) -> PgWireError {
    let (code, message) = reclassify_transported_error(raw, code);
    let mut info = reported(code, &sanitize_message(&message));
    info.table = ctx.table_name.clone();
    PgWireError::UserError(Box::new(info))
}

/// Construct a pgwire `UserError` from a `VdbErrorCode` and message, formatting
/// the message with the `[VDB-NNNN]` code prefix and the matching SQLSTATE.
pub fn make_vdb_error(code: VdbErrorCode, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(reported(code, &message.into())))
}

/// Enrich a typed `CoordinatorError` into a full pgwire error, classifying it
/// and attaching table name, a catalog-derived `DETAIL`, and a `HINT`. The
/// catalog is queried best-effort to build detail (e.g. listing alive nodes).
pub fn enrich_coordinator_error(
    err: &CoordinatorError,
    ctx: &ErrorContext,
    catalog: &Arc<MetadataCatalog>,
) -> PgWireError {
    let (code, message) = classify_error(err);
    // `classify_error` sanitizes what it returns; this is the one path whose message
    // was written for a client rather than recovered from engine text.
    let mut info = reported(code, &message);
    info.table = ctx.table_name.clone();
    info.detail = try_build_detail(err, ctx, catalog);
    info.hint = build_hint(err, ctx);

    PgWireError::UserError(Box::new(info))
}

/// The metadata of table `name`, reporting `42P01` when the catalog has no such table.
///
/// The read and its two enrichments are one operation, and six call sites across
/// `dml`, `merge` and `copy` had each written it out: a `get_table` whose `Err` is a
/// catalog failure and whose `Ok(None)` is a missing relation, both routed through the
/// same [`ErrorContext`]. It lives here rather than beside any one of them because what
/// it adds over [`MetadataCatalog::get_table`] is precisely the enrichment this module
/// owns — and because the half that drifts is the second one: a site that enriches the
/// catalog failure and then lets `None` fall through to the planner hands the client
/// DataFusion's wording for a table VaireDB knows nothing about.
pub fn require_table(
    catalog: &Arc<MetadataCatalog>,
    name: &str,
    ctx: &ErrorContext,
) -> PgWireResult<TableMeta> {
    catalog
        .get_table(name)
        .map_err(|e| enrich_coordinator_error(&e, ctx, catalog))?
        .ok_or_else(|| {
            let err = CoordinatorError::TableNotFound(name.to_string());
            enrich_coordinator_error(&err, ctx, catalog)
        })
}

/// The shards of `table`, refusing a table that has none rather than letting a write
/// dispatch to nowhere and report success for rows no node ever received.
pub fn require_shards(
    catalog: &Arc<MetadataCatalog>,
    table: &TableMeta,
    ctx: &ErrorContext,
) -> PgWireResult<Vec<ShardMeta>> {
    let shards = catalog
        .shards_for_table(&table.table_name)
        .map_err(|e| enrich_coordinator_error(&e, ctx, catalog))?;
    if shards.is_empty() {
        let err =
            CoordinatorError::ShardNotAssigned(format!("no shards for table {}", table.table_name));
        return Err(enrich_coordinator_error(&err, ctx, catalog));
    }
    Ok(shards)
}

/// Enrich an untyped (string-based) error, typically from the engine, by
/// inferring a `VdbErrorCode` from its message substrings and sanitizing it.
pub fn enrich_generic_error(e: &dyn Display, ctx: &ErrorContext) -> PgWireError {
    let raw = e.to_string();
    enrich_classified(&raw, classify_generic_error_code(&raw), ctx)
}

/// Enrich a `DataFusionError` whose type is still in hand, classifying it from its
/// **variant** rather than from the text its `Display` happens to produce.
///
/// Prefer this to [`enrich_generic_error`] everywhere the concrete error survives.
/// The substring classifier cannot be made to work here, and not merely because
/// message text drifts: `DataFusionError::Context` and `Diagnostic` have an empty
/// `error_prefix`, so the variant that matters is the *inner* one, and a scan of the
/// flattened string sees whichever phrase happens to appear first. Matching the
/// variant also means a DataFusion upgrade that rewords an error cannot silently
/// reclassify it — the compiler reports a new variant instead.
///
/// The variant is only in hand while the error stayed in this process, which is what
/// [`reclassify_transported_error`] is for: when it did not, the variant is recovered
/// from the text the scheduler rendered it into.
pub fn enrich_datafusion_error(e: &DataFusionError, ctx: &ErrorContext) -> PgWireError {
    enrich_classified(&e.to_string(), classify_datafusion_error_code(e), ctx)
}

/// Classify a [`DataFusionError`] by variant, refining within a variant only where
/// PostgreSQL draws a distinction DataFusion carries in the message.
///
/// The wrapper variants recurse, which is the point: `Context`, `Diagnostic` and
/// `Shared` exist to annotate an error without replacing it, so the classification
/// belongs to what they wrap. `Collection` takes its first error, matching
/// DataFusion's own `error_prefix`.
///
/// The catch-all arm is deliberate rather than lazy. `ParquetError`, `ObjectStore`
/// and `Ffi` are behind cargo features, so naming them here would make this module's
/// compilation depend on DataFusion's feature resolution; all three are engine
/// failures and `EngineError` is what they would map to anyway.
pub(crate) fn classify_datafusion_error_code(e: &DataFusionError) -> VdbErrorCode {
    match e {
        // Annotation wrappers — classify what is inside.
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            classify_datafusion_error_code(inner)
        }
        DataFusionError::Shared(inner) => classify_datafusion_error_code(inner),
        DataFusionError::Collection(errs) => {
            errs.first().map_or(VdbErrorCode::InternalError, |first| {
                classify_datafusion_error_code(first)
            })
        }

        // An honest refusal, and the class a client can act on.
        DataFusionError::NotImplemented(_) | DataFusionError::Substrait(_) => {
            VdbErrorCode::FeatureNotSupported
        }

        // A parse failure is the client's syntax, whoever noticed it.
        DataFusionError::SQL(_, _) => VdbErrorCode::SqlSyntaxError,

        // The name resolution errors, already typed by DataFusion — all four are a
        // column the query named and the schema does not have.
        DataFusionError::SchemaError(_, _) => VdbErrorCode::ColumnNotFound,

        // Planning covers several PostgreSQL classes; the message is the only thing
        // that separates them, but scoped to this variant rather than scanned globally.
        DataFusionError::Plan(msg) => classify_plan_message(msg),

        // Execution reached a row it could not process. Divide-by-zero and overflow are
        // data errors PostgreSQL names; the rest is genuinely the engine.
        DataFusionError::Execution(msg) => classify_execution_message(msg),
        DataFusionError::ArrowError(arrow, _) => classify_arrow_error(arrow),

        DataFusionError::ResourcesExhausted(_) => VdbErrorCode::WriteQueueFull,

        // Text that already crossed a process boundary: the type is gone, so this is
        // the one place the substring classifier is the best available answer.
        DataFusionError::External(inner) => match inner.downcast_ref::<DataFusionError>() {
            Some(df) => classify_datafusion_error_code(df),
            None => classify_generic_error_code(&inner.to_string()),
        },

        DataFusionError::IoError(_) | DataFusionError::ExecutionJoin(_) => {
            VdbErrorCode::EngineError
        }
        DataFusionError::Internal(_) | DataFusionError::Configuration(_) => {
            VdbErrorCode::InternalError
        }

        _ => VdbErrorCode::EngineError,
    }
}

/// Split a `DataFusionError::Plan` message into the PostgreSQL classes it covers.
///
/// Everything here is a defect in the statement rather than in the server, so the
/// fallback is `42601` and not `XX000`: a planning error means the query was read and
/// rejected, which is exactly what the syntax-error class tells a client.
fn classify_plan_message(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if lower.contains("aggregate function calls cannot be nested")
        || lower.contains("aggregate functions cannot be nested")
    {
        VdbErrorCode::GroupingError
    } else if lower.contains("window function") || lower.contains("over clause") {
        VdbErrorCode::WindowingError
    } else if lower.contains("no function matches")
        || lower.contains("invalid function")
        || lower.contains("not supported")
    {
        VdbErrorCode::FeatureNotSupported
    } else if lower.contains("no field named") || lower.contains("ambiguous") {
        VdbErrorCode::ColumnNotFound
    } else if (lower.contains("table") && lower.contains("not found"))
        || lower.contains("no table named")
    {
        VdbErrorCode::TableNotFound
    } else if lower.contains("cannot cast") || lower.contains("type mismatch") {
        VdbErrorCode::TypeMismatch
    } else {
        VdbErrorCode::SqlSyntaxError
    }
}

/// Split a `DataFusionError::Execution` message into the data errors PostgreSQL names.
///
/// `22012` and `22003` matter more than they look: both used to arrive as `XX000`,
/// which tells a client the server broke and invites a retry that cannot succeed. The
/// statement and the server were both fine and one row's data was not.
fn classify_execution_message(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if is_divide_by_zero(&lower) {
        VdbErrorCode::DivisionByZero
    } else if lower.contains("overflow") {
        VdbErrorCode::NumericValueOutOfRange
    } else if lower.contains("cannot cast string")
        || lower.contains("invalid input syntax")
        || lower.contains("error parsing")
    {
        VdbErrorCode::InvalidTextRepresentation
    } else {
        VdbErrorCode::EngineError
    }
}

/// DataFusion's physical-planner catch-all for a logical expression it will not build a
/// physical expression for — `not_impl_err!` in `datafusion-physical-expr`.
const PHYSICAL_EXPR_REFUSAL: &str = "Physical plan does not support logical expression ";

/// What PostgreSQL says when an aggregate appears somewhere aggregates are not evaluated
/// — inside another aggregate, in `WHERE`, in `GROUP BY`. Every such position is `42803`.
const AGGREGATE_NOT_ALLOWED: &str = "aggregate functions are not allowed in this context";

/// The same for a window function: nested in another window function, in `WHERE`, in
/// `GROUP BY` or in `HAVING`. Every such position is `42P20`.
const WINDOW_NOT_ALLOWED: &str = "window functions are not allowed in this context";

/// Recover the class, and the message, of an error that crossed the Ballista scheduler
/// boundary and arrived as text.
///
/// Classifying a `DataFusionError` by variant is the right default — it cannot rot when
/// DataFusion rewords a message — but it only works while there is a variant to read, and
/// an error raised inside an executor does not keep one. The scheduler renders the whole
/// failure into a `String` (Ballista's `FailedTask` has nowhere else to put it) and hands
/// it back under whichever wrapper the call site happened to use. That is why every
/// executor-side failure used to land `XX000 internal_error`: the one class that tells a
/// client the *server* broke and the statement is worth retrying, reported for a nested
/// aggregate that will never succeed however many times it is retried.
///
/// Three things are tried, most authoritative first:
///
/// 1. **A `[VDB-…]` tag.** VaireDB's own guards on an executor write their code into the
///    message (see [`vairedb_common::error::tagged_message`]), so the code is chosen where
///    the error is raised by the code that knows what went wrong. Nothing here has to
///    guess, and this beats even a successful variant classification.
/// 2. **DataFusion's physical-planner catch-all.** `AggregateFunction` or `WindowFunction`
///    reaching that arm means an aggregate or a window landed in a position the physical
///    planner does not build one for, which is `42803` / `42P20` and not "unsupported
///    feature". Every *legal* PostgreSQL form VaireDB has not implemented is refused
///    earlier, at the coordinator, so nothing legal reaches this arm. The unreadable
///    `Expr` debug dump is replaced by what PostgreSQL says.
/// 3. **The variant, recovered from the rendered text.** [`recover_transported_error`]
///    reads the variant name back out — it survives, because both of Ballista's
///    renderings spell it — and [`classify_transported`] then applies the *same*
///    per-variant classification the local path uses. This runs only when classification
///    already gave up (`EngineError` or `InternalError`, the two "we do not know"
///    answers), so a variant that is still in hand always wins.
///
/// Two wordings are then still owned by the modules that raise them, because their
/// SQLSTATEs are finer than any variant carries: a `bytea` literal that does not decode
/// ([`vairedb_common::bytea_in`], `22P02`/`22023`) and `nth_value(x, 0)`
/// ([`vairedb_common::nth_value`], `22016`, a code PostgreSQL spends on this one argument
/// of this one function). The divide-by-zero carve-out that used to sit beside them is
/// gone: step 3 recovers `ArrowError(DivideByZero)` structurally, so matching its `Debug`
/// spelling by hand is no longer needed.
fn reclassify_transported_error(raw: &str, code: VdbErrorCode) -> (VdbErrorCode, String) {
    let recovered = recover_transported_error(raw);
    // The innermost message a recovery reached, which is the only part of a transported
    // failure written for a client: everything around it is the job, stage and task
    // framing the scheduler added. `sanitize_message` cannot reach it on its own, because
    // it only unwraps a `Debug` dump that is the *whole* message.
    let innermost = || {
        recovered
            .as_ref()
            .map_or_else(|| raw.to_string(), |r| r.message.clone())
    };

    if let Some(tagged) = code_of_tagged_message(raw) {
        return (tagged, innermost());
    }
    if let Some(refusal) = reclassify_physical_expr_refusal(raw) {
        return refusal;
    }
    if !matches!(
        code,
        VdbErrorCode::EngineError | VdbErrorCode::InternalError
    ) {
        return (code, raw.to_string());
    }
    if let Some(recovered) = &recovered {
        let (recovered_code, message) = classify_transported(recovered);
        if !matches!(
            recovered_code,
            VdbErrorCode::EngineError | VdbErrorCode::InternalError
        ) {
            return (recovered_code, message);
        }
    }
    // A refusal raised inside an executor by one of VaireDB's own functions, recognized by
    // its wording because the type did not survive the Ballista boundary. Asked of the set
    // rather than of each family in turn, so a family that gains a classifiable refusal
    // reaches this line without it being edited — see
    // [`vairedb_common::distributed_functions::error_code_of_message`].
    if let Some(function_code) = vairedb_common::distributed_functions::error_code_of_message(raw) {
        return (function_code, innermost());
    }
    (code, raw.to_string())
}

/// Turn DataFusion's physical-planner catch-all into the class PostgreSQL gives the
/// misplaced expression, and PostgreSQL's own wording.
///
/// Matched anywhere in the text rather than at the front, because this arrives wrapped in
/// however many layers of job, stage and task framing the scheduler added — and the
/// wording is DataFusion's own, so there is nothing else it could be.
fn reclassify_physical_expr_refusal(raw: &str) -> Option<(VdbErrorCode, String)> {
    let at = raw.find(PHYSICAL_EXPR_REFUSAL)?;
    let expr = &raw[at + PHYSICAL_EXPR_REFUSAL.len()..];
    if expr.starts_with("AggregateFunction") {
        Some((
            VdbErrorCode::GroupingError,
            AGGREGATE_NOT_ALLOWED.to_string(),
        ))
    } else if expr.starts_with("WindowFunction") {
        Some((VdbErrorCode::WindowingError, WINDOW_NOT_ALLOWED.to_string()))
    } else {
        None
    }
}

/// Classify a [`TransportedError`] exactly as [`classify_datafusion_error_code`] would
/// have classified the value it was rendered from, and return the innermost message with
/// it.
///
/// One arm per variant, deliberately mirroring the local classifier: the point of the
/// whole exercise is that a client cannot tell from the SQLSTATE whether the error was
/// raised on the coordinator or on an executor, and the only way to keep that true is for
/// the two to make the same decision from the same evidence.
fn classify_transported(e: &TransportedError) -> (VdbErrorCode, String) {
    // Arrow is the one variant that also rewords, because it is the one whose rendering can
    // be a bare variant name — see [`classify_arrow_message`]. Every other variant keeps
    // the message it arrived with.
    if e.variant == TransportedVariant::Arrow {
        return classify_arrow_message(&e.message);
    }
    let code = match e.variant {
        TransportedVariant::NotImplemented | TransportedVariant::Substrait => {
            VdbErrorCode::FeatureNotSupported
        }
        TransportedVariant::Sql => VdbErrorCode::SqlSyntaxError,
        TransportedVariant::Schema => VdbErrorCode::ColumnNotFound,
        TransportedVariant::Plan => classify_plan_message(&e.message),
        TransportedVariant::Execution => classify_execution_message(&e.message),
        TransportedVariant::ResourcesExhausted => VdbErrorCode::WriteQueueFull,
        TransportedVariant::Io | TransportedVariant::ExecutionJoin | TransportedVariant::Arrow => {
            VdbErrorCode::EngineError
        }
        TransportedVariant::Internal | TransportedVariant::Configuration => {
            VdbErrorCode::InternalError
        }
    };
    (code, e.message.clone())
}

/// Recognize divide-by-zero in an already-lowercased message.
///
/// Both spellings are live: Arrow's `Display` writes "Divide by zero error" and DuckDB
/// writes "Division by zero". The `Debug` spelling this used to match as well —
/// `ArrowError(DivideByZero)`, which is what an executor-side division arrives as — is
/// handled structurally now, by [`reclassify_transported_error`].
fn is_divide_by_zero(lower: &str) -> bool {
    lower.contains("divide by zero") || lower.contains("division by zero")
}

/// Classify an [`ArrowError`] that arrives as the text it was rendered to, mapping each
/// spelling to the same code its typed arm in [`classify_arrow_error`] maps to, and
/// returning the message a client should read.
///
/// Anchored with `starts_with` rather than `contains`, and that matters: the string being
/// classified *is* the rendering of one `ArrowError`, so its variant name or its `Display`
/// prefix is at the front. A message that merely mentions a cast is not a cast error, and
/// scanning for the phrase anywhere would classify it as one.
///
/// `DivideByZero` is the one variant with no payload, so recovering it leaves the bare
/// Rust variant name where a message should be. `sanitize_message` unwraps the others —
/// `CastError("…")` is a single-field `Debug` dump and reduces to its text — but there is
/// nothing inside this one to unwrap, so PostgreSQL's own wording is substituted here.
fn classify_arrow_message(msg: &str) -> (VdbErrorCode, String) {
    let lower = msg.to_lowercase();
    let starts_with_any = |spellings: &[&str]| spellings.iter().any(|s| lower.starts_with(*s));
    if starts_with_any(&["dividebyzero", "divide by zero"]) {
        (VdbErrorCode::DivisionByZero, "division by zero".to_string())
    } else if starts_with_any(&["arithmeticoverflow", "arithmetic overflow"]) {
        (VdbErrorCode::NumericValueOutOfRange, msg.to_string())
    } else if starts_with_any(&["casterror", "cast error", "parseerror", "parser error"]) {
        (VdbErrorCode::InvalidTextRepresentation, msg.to_string())
    } else if starts_with_any(&["notyetimplemented", "not yet implemented"]) {
        (VdbErrorCode::FeatureNotSupported, msg.to_string())
    } else if starts_with_any(&["schemaerror", "schema error"]) {
        (VdbErrorCode::ColumnNotFound, msg.to_string())
    } else {
        (VdbErrorCode::EngineError, msg.to_string())
    }
}

/// Classify an [`ArrowError`] reached through `DataFusionError::ArrowError`.
///
/// Arrow types the two cases PostgreSQL cares about most, so unlike the string
/// variants above these need no message inspection at all: a failed cast or parse of
/// a text value is `22P02`, and its dedicated divide-by-zero variant is `22012`.
fn classify_arrow_error(e: &ArrowError) -> VdbErrorCode {
    match e {
        ArrowError::DivideByZero => VdbErrorCode::DivisionByZero,
        ArrowError::ArithmeticOverflow(_) => VdbErrorCode::NumericValueOutOfRange,
        ArrowError::CastError(_) | ArrowError::ParseError(_) => {
            VdbErrorCode::InvalidTextRepresentation
        }
        ArrowError::NotYetImplemented(_) => VdbErrorCode::FeatureNotSupported,
        ArrowError::SchemaError(_) => VdbErrorCode::ColumnNotFound,
        _ => VdbErrorCode::EngineError,
    }
}

/// Infer a `VdbErrorCode` from an untyped error message via case-insensitive
/// substring matching against known engine/driver phrasings, falling back to
/// `InternalError`. The first matching rule wins, so order is significant.
pub(crate) fn classify_generic_error_code(msg: &str) -> VdbErrorCode {
    let lower = msg.to_lowercase();
    if (lower.contains("table") && lower.contains("not found")) || lower.contains("no table named")
    {
        VdbErrorCode::TableNotFound
    } else if lower.contains("shard") && lower.contains("not found") {
        VdbErrorCode::ShardNotFound
    } else if lower.contains("no field named")
        || (lower.contains("column") && lower.contains("not found"))
        || lower.contains("ambiguous")
    {
        VdbErrorCode::ColumnNotFound
    } else if lower.contains("type mismatch") || lower.contains("cannot cast") {
        VdbErrorCode::TypeMismatch
    } else if lower.contains("syntax error") || lower.contains("unexpected token") {
        VdbErrorCode::SqlSyntaxError
    // `"this feature is not implemented"` and `"not supported"` are the phrases
    // DataFusion has emitted since 53 (`DataFusionError::error_prefix`); the two that
    // preceded them are kept because DuckDB and Ballista still use them.
    } else if lower.contains("this feature is not implemented")
        || lower.contains("not yet implemented")
        || lower.contains("not supported")
        || lower.contains("unsupported")
        || lower.contains("no function matches")
        || lower.contains("invalid function")
    {
        VdbErrorCode::FeatureNotSupported
    } else if lower.contains("resources exhausted") || lower.contains("memory limit") {
        VdbErrorCode::WriteQueueFull
    } else if is_divide_by_zero(&lower) {
        VdbErrorCode::DivisionByZero
    } else if lower.contains("overflow") {
        VdbErrorCode::NumericValueOutOfRange
    } else if lower.contains("unique constraint")
        || lower.contains("duplicate key")
        || lower.contains("primary key constraint")
    {
        VdbErrorCode::WriteConflict
    } else if lower.contains("not null constraint")
        || lower.contains("null value")
        || lower.contains("check constraint")
    {
        VdbErrorCode::EngineError
    } else if lower.contains("connection")
        && (lower.contains("refused") || lower.contains("unreachable"))
    {
        VdbErrorCode::NodeUnavailable
    } else {
        VdbErrorCode::InternalError
    }
}

/// Log `detail` for an operator and return `summary` as the client's message.
///
/// For the failures where those are the *same* sentence: the operator needs the redb
/// page or the endpoint that failed, the client must not see it, and both describe one
/// failure. Written out per arm the summary appeared twice, so a reworded client
/// message and the log line that was supposed to correspond to it could drift apart —
/// and the field naming already had, half the arms logging under `error` and half
/// under `detail`.
fn logged(detail: &dyn Display, summary: &str) -> String {
    tracing::error!(error = %detail, "{summary}");
    summary.to_string()
}

/// Map a `CoordinatorError` to its `VdbErrorCode` and a sanitized, client-safe
/// message. Logs the underlying detail for operator diagnosis while returning a
/// generic message so internal specifics (node IDs, storage internals) never leak.
pub(crate) fn classify_error(err: &CoordinatorError) -> (VdbErrorCode, String) {
    let code = err.vdb_error_code();
    let message = match err {
        CoordinatorError::TableNotFound(name) => {
            format!("table '{}' does not exist", name)
        }
        CoordinatorError::NodeNotFound(id) => {
            format!("node '{}' not found in cluster", id)
        }
        CoordinatorError::ShardNotAssigned(msg) => msg.clone(),
        CoordinatorError::NullShardKey(msg) => msg.clone(),
        CoordinatorError::UnroutableShardKey(msg) => msg.clone(),
        CoordinatorError::QuorumNotReached { needed, got } => {
            format!(
                "write quorum not reached: {}/{} nodes acknowledged",
                got, needed
            )
        }
        CoordinatorError::ShardUnavailable(pid) => {
            format!("shard '{}' unavailable: primary node unreachable", pid)
        }
        CoordinatorError::Grpc(status) => {
            format!(
                "node execution failed: {}",
                sanitize_message(status.message())
            )
        }
        // Not `logged`: an operator is told which transport failed, a client is told
        // only that one did, and the two sentences are deliberately different.
        CoordinatorError::GrpcTransport(e) => {
            tracing::error!(error = %e, "gRPC transport failure");
            "failed to communicate with storage node".to_string()
        }
        // `Display` already opens with "sql parser error", so this adds no prefix of
        // its own. No client reaches this arm: the only producer of the variant is
        // `parser::parse_sql`, and every one of its production callers renders the
        // failure itself — `handler.rs` with `make_vdb_error(e.vdb_error_code(), …)`,
        // the rest by naming the query they were building. The arm exists so a future
        // caller that does propagate it reports the same sentence rather than a second
        // wording, which is why it must stay a bare render.
        CoordinatorError::SqlParse(e) => e.to_string(),
        // Already written for the client, and already naming what to write instead.
        CoordinatorError::Unsupported(msg) => msg.clone(),
        // PostgreSQL's own wording for the same bad input, which is the point of the
        // variant — the code beside it is PostgreSQL's too.
        CoordinatorError::InvalidValue { message, .. } => message.clone(),
        CoordinatorError::NodeExecFailed(node_err) => {
            tracing::error!(
                node_id = %node_err.node_id,
                shard_id = %node_err.shard_id,
                error = %node_err.message,
                "node execution failed"
            );
            format!(
                "node execution failed: {}",
                sanitize_message(&node_err.message)
            )
        }
        CoordinatorError::CatalogTransaction(e) => logged(e, "catalog transaction failed"),
        CoordinatorError::CatalogStorage(e) => logged(e, "catalog storage error"),
        CoordinatorError::CatalogCommit(e) => logged(e, "catalog commit failed"),
        CoordinatorError::Catalog(e) => logged(e, "catalog error"),
        CoordinatorError::CatalogTable(e) => logged(e, "catalog table access failed"),
        CoordinatorError::NoAliveNodes => {
            "no alive nodes available for shard assignment".to_string()
        }
        CoordinatorError::Anonymization(msg) => msg.clone(),
        CoordinatorError::Serialization(s) => logged(s, "internal serialization error"),
        CoordinatorError::Internal(s) => logged(s, "internal error"),
    };
    (code, sanitize_message(&message))
}

/// Build the optional `DETAIL` line for an error, querying the catalog for
/// supporting facts (alive node counts, the unavailable shard's primary node,
/// etc.). Returns `None` when no useful detail applies or a catalog lookup fails.
pub(crate) fn try_build_detail(
    err: &CoordinatorError,
    ctx: &ErrorContext,
    catalog: &Arc<MetadataCatalog>,
) -> Option<String> {
    match err {
        CoordinatorError::QuorumNotReached { needed, .. } => {
            let nodes = catalog.list_alive_nodes().ok()?;
            let alive_count = nodes.len();
            Some(format!(
                "Alive nodes in cluster: {}. Required quorum: {}.",
                alive_count, needed
            ))
        }
        CoordinatorError::ShardUnavailable(pid) => {
            let table_name = ctx.table_name.as_ref()?;
            let shards = catalog.shards_for_table(table_name).ok()?;
            let p = shards.iter().find(|p| p.shard_id == *pid)?;
            let node = catalog.get_node(&p.primary_node_id).ok()??;
            tracing::debug!(
                shard_id = %pid,
                node_id = %node.node_id,
                address = %node.advertised_address,
                state = node_state_str(node.state),
                "shard unavailable detail"
            );
            Some(format!(
                "Primary node '{}' (state: {}).",
                node.node_id,
                node_state_str(node.state),
            ))
        }
        CoordinatorError::NodeNotFound(_) => {
            let nodes = catalog.list_alive_nodes().ok()?;
            if nodes.is_empty() {
                return Some("No alive nodes in cluster.".to_string());
            }
            let ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
            Some(format!("Known alive nodes: {}.", ids.join(", ")))
        }
        CoordinatorError::NodeExecFailed(node_err) => Some(format!(
            "Node '{}' failed on shard '{}'.",
            node_err.node_id, node_err.shard_id
        )),
        CoordinatorError::Grpc(status) => {
            let sanitized = sanitize_message(status.message());
            if sanitized.is_empty() {
                None
            } else {
                Some(sanitized)
            }
        }
        CoordinatorError::GrpcTransport(_) => None,
        _ => None,
    }
}

/// Build the optional `HINT` line suggesting a remediation or diagnostic query
/// for the error, returning `None` when no actionable hint applies.
pub(crate) fn build_hint(err: &CoordinatorError, ctx: &ErrorContext) -> Option<String> {
    match err {
        CoordinatorError::TableNotFound(_) => {
            Some("Run SELECT * FROM vairedb_catalog.tables to see available tables.".to_string())
        }
        CoordinatorError::NodeNotFound(_) => {
            Some("Check vairedb_catalog.nodes for registered nodes.".to_string())
        }
        CoordinatorError::ShardNotAssigned(_) => Some(
            "Verify the table was created successfully and shards were assigned.".to_string(),
        ),
        CoordinatorError::QuorumNotReached { .. } => {
            let rf_info = ctx
                .replication_factor
                .map(|rf| format!("Replication factor is {}. ", rf))
                .unwrap_or_default();
            Some(format!(
                "{}Check node health: SELECT * FROM vairedb_catalog.nodes WHERE state != 'ALIVE'.",
                rf_info
            ))
        }
        CoordinatorError::ShardUnavailable(_) => Some(
            "The primary node may be down. Writes will resume when the shard is reassigned."
                .to_string(),
        ),
        CoordinatorError::GrpcTransport(_) => {
            Some("Check that core nodes are running and reachable.".to_string())
        }
        CoordinatorError::NodeExecFailed(node_err) => {
            match VdbErrorCode::try_from(node_err.error_code) {
                Ok(VdbErrorCode::WriteConflict) => Some("Retry the transaction.".to_string()),
                Ok(VdbErrorCode::NodeShuttingDown) => {
                    Some("The node is shutting down. Retry after cluster rebalancing.".to_string())
                }
                _ => None,
            }
        }
        CoordinatorError::NoAliveNodes => Some(
            "Register at least one storage node before creating tables. Check vairedb_catalog.nodes."
                .to_string(),
        ),
        CoordinatorError::Catalog(_)
        | CoordinatorError::CatalogTransaction(_)
        | CoordinatorError::CatalogTable(_)
        | CoordinatorError::CatalogStorage(_)
        | CoordinatorError::CatalogCommit(_) => Some(
            "This is an internal metadata storage issue. Check disk space and coordinator logs."
                .to_string(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::write_path_test_helper::user_error;
    use super::*;
    use crate::catalog::catalog_test_helper::scratch_catalog;

    fn make_catalog() -> Arc<MetadataCatalog> {
        Arc::new(scratch_catalog("error_enrichment"))
    }

    /// A real `tonic` transport failure, which is the only kind there is: the error has
    /// no public constructor, so it has to come from a connect that genuinely fails.
    fn transport_error() -> CoordinatorError {
        let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:0");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let failed = runtime.block_on(async {
            endpoint
                .connect()
                .await
                .expect_err("port 0 on the loopback refuses")
        });
        CoordinatorError::GrpcTransport(Box::new(failed))
    }

    /// A failure a core node reported, carrying the code the *node* chose — which is the
    /// only thing classification reads, the node id and shard being detail for the log.
    fn node_exec_failed(code: VdbErrorCode, message: &str) -> CoordinatorError {
        CoordinatorError::NodeExecFailed(Box::new(crate::error::NodeError {
            message: message.to_string(),
            error_code: code as i32,
            shard_id: "orders_shard0".to_string(),
            node_id: "core-node-1".to_string(),
        }))
    }

    /// Wrap `raised` in the framing the Ballista scheduler adds to a failed task —
    /// measured on a live five-node cluster, and the shape every transported case below
    /// is built on.
    fn transported_task(raised: &str) -> String {
        format!(
            "Job 3QdcFzH failed: Job failed due to stage 1 failed: Task failed due to \
             runtime execution error: DataFusionError({raised})"
        )
    }

    /// The SQLSTATE and message a client is sent for `err`.
    fn enriched(err: &DataFusionError) -> (String, String) {
        user_error(enrich_datafusion_error(err, &ErrorContext::default()))
    }

    // --- classify_error ---

    /// One row per `CoordinatorError` variant, asserting the **code** alone.
    ///
    /// Wording is a separate axis with its own tests below: a table that checked both
    /// would have to be edited every time a sentence is reworded, and the edit is where
    /// a silently-dropped code assertion hides.
    #[test]
    fn each_variant_reports_its_own_code() {
        let cases = [
            (
                CoordinatorError::TableNotFound("orders".into()),
                VdbErrorCode::TableNotFound,
            ),
            (
                CoordinatorError::NodeNotFound("node-1".into()),
                VdbErrorCode::NodeNotFound,
            ),
            (
                CoordinatorError::ShardNotAssigned("shard0".into()),
                VdbErrorCode::ShardNotAssigned,
            ),
            (
                CoordinatorError::ShardUnavailable("shard3".into()),
                VdbErrorCode::ShardUnavailable,
            ),
            (
                CoordinatorError::QuorumNotReached { needed: 2, got: 1 },
                VdbErrorCode::QuorumNotReached,
            ),
            // A gRPC status classifies by its *status code*, not by its message.
            (
                CoordinatorError::Grpc(Box::new(tonic::Status::not_found("gone"))),
                VdbErrorCode::ShardNotFound,
            ),
            (
                CoordinatorError::Grpc(Box::new(tonic::Status::unavailable("down"))),
                VdbErrorCode::NodeUnavailable,
            ),
            (transport_error(), VdbErrorCode::NodeCommunicationError),
            // A node's own code is reported unchanged, which is what lets a shard-local
            // refusal reach the client as the thing the node called it.
            (
                node_exec_failed(VdbErrorCode::WriteConflict, "write conflict"),
                VdbErrorCode::WriteConflict,
            ),
            (
                node_exec_failed(VdbErrorCode::ShardNotFound, "shard missing"),
                VdbErrorCode::ShardNotFound,
            ),
            (
                node_exec_failed(VdbErrorCode::NodeShuttingDown, "shutting down"),
                VdbErrorCode::NodeShuttingDown,
            ),
            (CoordinatorError::NoAliveNodes, VdbErrorCode::NoAliveNodes),
            (
                CoordinatorError::CatalogStorage(redb::StorageError::Corrupted("disk".into())),
                VdbErrorCode::CatalogStorageError,
            ),
            (
                CoordinatorError::Serialization("bad wire type".into()),
                VdbErrorCode::SerializationError,
            ),
            (
                CoordinatorError::Internal("oops".into()),
                VdbErrorCode::InternalError,
            ),
        ];

        for (err, expected) in cases {
            assert_eq!(classify_error(&err).0, expected, "for {err:?}");
        }
    }

    /// The variants whose message is the answer: what the client asked about has to
    /// survive into it, or the error names nothing.
    #[test]
    fn a_message_names_what_the_client_asked_about() {
        let named = |err: CoordinatorError| classify_error(&err).1;
        assert!(named(CoordinatorError::TableNotFound("orders".into())).contains("orders"));
        assert!(
            named(CoordinatorError::QuorumNotReached { needed: 2, got: 1 }).contains("1/2"),
            "the quorum message reports got/needed"
        );
        assert!(named(CoordinatorError::NoAliveNodes).contains("no alive nodes"));
    }

    /// The internal half of a classified error stays in the log. One row per producer of
    /// detail a client must not be shown — a redb message, the endpoint that refused, a
    /// node id, a prost wire offset — paired with the sentence it is replaced by.
    #[test]
    fn a_message_reports_the_summary_and_never_the_internal_detail() {
        let cases: [(CoordinatorError, &str, &[&str]); 4] = [
            (
                CoordinatorError::CatalogStorage(redb::StorageError::Corrupted(
                    "metadata invalid".into(),
                )),
                "catalog storage error",
                &["metadata invalid"],
            ),
            (
                transport_error(),
                "failed to communicate with storage node",
                &["::1"],
            ),
            (
                node_exec_failed(VdbErrorCode::ShardNotFound, "table 'orders' not found"),
                "node execution failed",
                &["core-node-1"],
            ),
            (
                CoordinatorError::Serialization(
                    "failed to decode: invalid wire type 6 at offset 42".into(),
                ),
                "internal serialization error",
                &["wire type", "offset 42"],
            ),
        ];

        for (err, summary, leaks) in cases {
            let (_, message) = classify_error(&err);
            assert!(message.contains(summary), "for {err:?}: {message}");
            for leaked in leaks {
                assert!(
                    !message.contains(leaked),
                    "{leaked:?} survived for {err:?}: {message}"
                );
            }
        }
    }

    /// The one wording a parse failure is reported with, wherever it is reported from.
    ///
    /// No client reaches this arm today — see the comment beside it — but it used to
    /// prefix `"SQL syntax error: "` onto a `Display` that already opened with
    /// `"sql parser error"`, so the first caller to propagate one would have shown the
    /// client the same thing twice. It now renders exactly what `handler.rs` sends.
    #[test]
    fn a_parse_failure_is_described_once() {
        let err = CoordinatorError::SqlParse(crate::sqlparser::parser::ParserError::ParserError(
            "Expected: an expression, found: FROM".into(),
        ));
        let (code, message) = classify_error(&err);
        assert_eq!(code, VdbErrorCode::SqlSyntaxError);
        assert_eq!(message, err.to_string());
        assert_eq!(message.matches("parser error").count(), 1, "{message}");
    }

    // --- try_build_detail ---

    /// Which errors get a `DETAIL` line, against a catalog with nothing registered.
    ///
    /// The zero counts are the point: the line is built from the *catalog*, not from the
    /// error, so an empty cluster has to produce a truthful sentence rather than none.
    #[test]
    fn a_detail_is_built_only_where_the_catalog_has_something_to_add() {
        let catalog = make_catalog();
        let cases: [(CoordinatorError, Option<&str>); 4] = [
            (
                CoordinatorError::QuorumNotReached { needed: 2, got: 1 },
                Some("Alive nodes in cluster: 0"),
            ),
            (
                CoordinatorError::NodeNotFound("node-x".into()),
                Some("No alive nodes"),
            ),
            (CoordinatorError::TableNotFound("t".into()), None),
            (transport_error(), None),
        ];

        for (err, expected) in cases {
            let detail = try_build_detail(&err, &ErrorContext::default(), &catalog);
            match expected {
                Some(phrase) => assert!(
                    detail.as_deref().is_some_and(|d| d.contains(phrase)),
                    "for {err:?}: {detail:?}"
                ),
                None => assert!(detail.is_none(), "for {err:?}: {detail:?}"),
            }
        }
    }

    // --- build_hint ---

    /// One row per hint branch: the error, the replication factor the context carries,
    /// and the phrase the hint has to name so it tells someone what to do next.
    #[test]
    fn a_hint_names_something_actionable() {
        let cases: [(CoordinatorError, Option<u32>, &str); 4] = [
            (
                CoordinatorError::TableNotFound("t".into()),
                None,
                "vairedb_catalog.tables",
            ),
            (
                CoordinatorError::QuorumNotReached { needed: 2, got: 1 },
                Some(3),
                "Replication factor is 3",
            ),
            (
                CoordinatorError::CatalogStorage(redb::StorageError::Corrupted("bad".into())),
                None,
                "metadata storage issue",
            ),
            (
                CoordinatorError::NoAliveNodes,
                None,
                "Register at least one storage node",
            ),
        ];

        for (err, replication, expected) in cases {
            let ctx = match replication {
                Some(factor) => ErrorContext::default().with_replication(factor),
                None => ErrorContext::default(),
            };
            let hint = build_hint(&err, &ctx).unwrap_or_else(|| panic!("{err:?} should hint"));
            assert!(hint.contains(expected), "for {err:?}: {hint}");
        }
    }

    // --- classify_generic_error_code tests ---

    /// Each case maps a representative engine/driver error string to the
    /// `VdbErrorCode` its substring rules should produce. One row per branch of
    /// `classify_generic_error_code`; add a row when a branch is added.
    #[test]
    fn each_substring_rule_classifies_the_text_it_was_written_for() {
        let cases: &[(&str, VdbErrorCode)] = &[
            ("table 'orders' not found", VdbErrorCode::TableNotFound),
            (
                "No table named 'users' in schema",
                VdbErrorCode::TableNotFound,
            ),
            (
                "No field named 'age' in schema",
                VdbErrorCode::ColumnNotFound,
            ),
            (
                "column reference 'id' is ambiguous",
                VdbErrorCode::ColumnNotFound,
            ),
            (
                "type mismatch: expected Int32, got Utf8",
                VdbErrorCode::TypeMismatch,
            ),
            ("Cannot cast string to integer", VdbErrorCode::TypeMismatch),
            (
                "This feature is not yet implemented",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "Unsupported SQL type: GEOMETRY",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "no function matches the given name and argument types",
                VdbErrorCode::FeatureNotSupported,
            ),
            ("invalid function 'foo'", VdbErrorCode::FeatureNotSupported),
            (
                "syntax error at or near 'FROM'",
                VdbErrorCode::SqlSyntaxError,
            ),
            (
                "unexpected token in expression",
                VdbErrorCode::SqlSyntaxError,
            ),
            // Both used to be `EngineError`, i.e. `XX000`. They are data errors the
            // client caused, and reporting them as internal tells a driver to retry
            // something that cannot succeed.
            ("divide by zero", VdbErrorCode::DivisionByZero),
            (
                "integer overflow in computation",
                VdbErrorCode::NumericValueOutOfRange,
            ),
            // The phrasings DataFusion has used since 53, which the two rules above
            // this line did not match — so these reached clients as `XX000` too.
            (
                "This feature is not implemented: window EXCLUDE",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "Sort expression is not supported",
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                "NOT NULL constraint failed: column 'name' cannot be null",
                VdbErrorCode::EngineError,
            ),
            (
                "CHECK constraint failed: age > 0",
                VdbErrorCode::EngineError,
            ),
            (
                "resources exhausted: memory limit reached",
                VdbErrorCode::WriteQueueFull,
            ),
            (
                "shard 'orders_shard0' not found",
                VdbErrorCode::ShardNotFound,
            ),
            ("connection refused to host", VdbErrorCode::NodeUnavailable),
            (
                "connection unreachable for node",
                VdbErrorCode::NodeUnavailable,
            ),
            (
                "unique constraint violated: duplicate key value",
                VdbErrorCode::WriteConflict,
            ),
            (
                "duplicate key value violates unique constraint",
                VdbErrorCode::WriteConflict,
            ),
            (
                "PRIMARY KEY constraint failed for table",
                VdbErrorCode::WriteConflict,
            ),
            ("something went wrong", VdbErrorCode::InternalError),
        ];

        for (msg, expected) in cases {
            assert_eq!(
                classify_generic_error_code(msg),
                *expected,
                "classifying {msg:?}"
            );
        }
    }

    // --- ErrorContext ---

    #[test]
    fn a_context_carries_the_table_and_the_replication_factor() {
        let ctx = ErrorContext::for_table("orders").with_replication(3);
        assert_eq!(ctx.table_name.as_deref(), Some("orders"));
        assert_eq!(ctx.replication_factor, Some(3));
    }

    // --- enrich ---

    /// Everything the typed path adds on top of the classification: the SQLSTATE, the
    /// `[VDB-NNNN]` prefix, and the relation — which is the whole reason `ErrorContext`
    /// is threaded this far, and which nothing used to check.
    #[test]
    fn enriching_a_coordinator_error_reports_the_code_the_state_and_the_table() {
        let err = CoordinatorError::TableNotFound("orders".to_string());
        match enrich_coordinator_error(&err, &ErrorContext::for_table("orders"), &make_catalog()) {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "42P01");
                assert!(info.message.contains("[VDB-1000]"), "{}", info.message);
                assert_eq!(info.table.as_deref(), Some("orders"));
            }
            other => panic!("expected UserError, got: {other:?}"),
        }
    }

    /// An error with no recognizable substring is internal, and says so with the state a
    /// driver reads as "the server, not your statement".
    #[test]
    fn enriching_an_unrecognized_error_reports_it_as_internal() {
        let (sqlstate, message) = user_error(enrich_generic_error(
            &"something failed",
            &ErrorContext::default(),
        ));
        assert_eq!(sqlstate, "XX000");
        assert!(message.contains("[VDB-5001]"), "{message}");
    }

    // --- enrich_datafusion_error / classify_datafusion_error_code ---

    /// One case per variant this classifies deliberately, so an upstream reshuffle of
    /// `DataFusionError` shows up as a failure here rather than as a class silently
    /// falling back to `EngineError`.
    #[test]
    fn classifies_each_datafusion_variant_by_its_variant() {
        let cases: Vec<(DataFusionError, VdbErrorCode)> = vec![
            (
                DataFusionError::NotImplemented("window EXCLUDE".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Substrait("nope".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Internal("broke".into()),
                VdbErrorCode::InternalError,
            ),
            (
                DataFusionError::Configuration("bad setting".into()),
                VdbErrorCode::InternalError,
            ),
            (
                DataFusionError::ResourcesExhausted("no memory".into()),
                VdbErrorCode::WriteQueueFull,
            ),
            (
                DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None),
                VdbErrorCode::DivisionByZero,
            ),
            (
                DataFusionError::ArrowError(
                    Box::new(ArrowError::CastError("'x' -> i32".into())),
                    None,
                ),
                VdbErrorCode::InvalidTextRepresentation,
            ),
            (
                DataFusionError::ArrowError(
                    Box::new(ArrowError::ArithmeticOverflow("i64".into())),
                    None,
                ),
                VdbErrorCode::NumericValueOutOfRange,
            ),
            (
                DataFusionError::Execution("Divide by zero error".into()),
                VdbErrorCode::DivisionByZero,
            ),
            (
                DataFusionError::Execution("Overflow happened".into()),
                VdbErrorCode::NumericValueOutOfRange,
            ),
            (
                DataFusionError::Execution("some engine trouble".into()),
                VdbErrorCode::EngineError,
            ),
            (
                DataFusionError::Plan("No function matches the given name".into()),
                VdbErrorCode::FeatureNotSupported,
            ),
            (
                DataFusionError::Plan("Aggregate function calls cannot be nested".into()),
                VdbErrorCode::GroupingError,
            ),
            (
                DataFusionError::Plan("window function is not allowed in WHERE".into()),
                VdbErrorCode::WindowingError,
            ),
            (
                DataFusionError::Plan("No field named foo".into()),
                VdbErrorCode::ColumnNotFound,
            ),
            (
                DataFusionError::Plan("something else entirely".into()),
                VdbErrorCode::SqlSyntaxError,
            ),
        ];

        for (err, expected) in cases {
            assert_eq!(
                classify_datafusion_error_code(&err),
                expected,
                "misclassified {err:?}"
            );
        }
    }

    /// The reason a substring scan cannot do this job. `Context` has an empty
    /// `error_prefix`, so the flattened text of this error begins with the *annotation*
    /// — and the annotation here names planning while the error underneath is a
    /// division by zero. Only the inner variant is the truth.
    #[test]
    fn a_wrapped_error_is_classified_by_what_it_wraps() {
        let inner = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        let wrapped = DataFusionError::Context(
            "while evaluating projection during planning".into(),
            Box::new(inner),
        );
        assert_eq!(
            classify_datafusion_error_code(&wrapped),
            VdbErrorCode::DivisionByZero
        );
    }

    #[test]
    fn a_shared_and_a_collection_error_classify_by_their_contents() {
        let shared = DataFusionError::Shared(Arc::new(DataFusionError::NotImplemented("x".into())));
        assert_eq!(
            classify_datafusion_error_code(&shared),
            VdbErrorCode::FeatureNotSupported
        );

        let collection = DataFusionError::Collection(vec![
            DataFusionError::Execution("Divide by zero".into()),
            DataFusionError::Internal("noise".into()),
        ]);
        assert_eq!(
            classify_datafusion_error_code(&collection),
            VdbErrorCode::DivisionByZero
        );
    }

    /// A `DataFusionError` boxed inside `External` is what a Ballista stage failure
    /// looks like once it has crossed gRPC, so unwrapping it keeps the typed answer
    /// rather than falling back to the substring scan.
    #[test]
    fn an_external_error_unwraps_a_datafusion_error_inside_it() {
        let external = DataFusionError::External(Box::new(DataFusionError::NotImplemented(
            "something".into(),
        )));
        assert_eq!(
            classify_datafusion_error_code(&external),
            VdbErrorCode::FeatureNotSupported
        );
    }

    /// `SchemaError` is already typed by DataFusion, so no message reading is needed.
    #[test]
    fn a_schema_error_is_a_missing_column() {
        let err = DataFusionError::SchemaError(
            Box::new(datafusion::common::SchemaError::AmbiguousReference {
                field: Box::new(datafusion::common::Column::new_unqualified("id")),
            }),
            Box::new(None),
        );
        assert_eq!(
            classify_datafusion_error_code(&err),
            VdbErrorCode::ColumnNotFound
        );
    }

    /// The end-to-end shape a client sees: the right SQLSTATE, and no `Signature { … }`
    /// debug dump surviving into the message.
    #[test]
    fn enriching_reports_the_sqlstate_and_elides_a_signature_dump() {
        let err = DataFusionError::Plan(
            "No function matches 'lpad': Signature { type_signature: OneOf([Exact([Utf8])]), \
             volatility: Immutable }"
                .into(),
        );
        let (reported, message) = enriched(&err);
        assert_eq!(reported, "0A000");
        assert!(!message.contains("type_signature"), "{message}");
        assert!(message.contains("Signature { … }"), "{message}");
    }

    /// A division by zero used to reach clients as `XX000`, which says the server broke.
    #[test]
    fn a_division_by_zero_reports_the_data_error_sqlstate() {
        let err = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        assert_eq!(enriched(&err).0, "22012");
    }

    /// The same division by zero, but as it actually arrives from a five-node cluster:
    /// raised in an executor, formatted into a string by the scheduler, and handed back
    /// under a wrapper variant that carries no type information. Every one of these
    /// shapes was observed or is a plausible re-wrap of one that was, and all of them
    /// have to answer `22012` — which is the reason
    /// [`reclassify_transported_error`] recovers the variant from the rendered message
    /// rather than trusting the wrapper it arrived under.
    #[test]
    fn a_transported_division_by_zero_reports_the_data_error_sqlstate() {
        let ballista = transported_task("Execution(\"ArrowError(DivideByZero)\")");

        for err in [
            DataFusionError::Execution(ballista.clone()),
            DataFusionError::Internal(ballista.clone()),
            DataFusionError::Context(
                "collect".into(),
                Box::new(DataFusionError::Execution(ballista.clone())),
            ),
            DataFusionError::External(Box::new(std::io::Error::other(ballista.clone()))),
        ] {
            let (reported, message) = enriched(&err);
            assert_eq!(reported, "22012", "for {err:?}");
            // `ArrowError::DivideByZero` carries no payload, so recovering it leaves the
            // bare Rust variant name where a message should be.
            assert!(
                message.ends_with("division by zero"),
                "for {err:?}: {message}"
            );
            assert!(!message.contains("Job "), "{message}");
        }
    }

    /// The second named exception: a `bytea` literal the read path's UDF could not decode.
    /// The write path refuses the same literal at parse time with these codes, so this is
    /// what keeps `'\xzz'::bytea` from reporting `22023` in an `INSERT` and `XX000` in a
    /// `SELECT`.
    #[test]
    fn a_transported_bytea_input_error_reports_postgresqls_sqlstate() {
        for (raised, want) in [
            ("invalid hexadecimal digit: \\\"z\\\"", "22023"),
            ("invalid hexadecimal data: odd number of digits", "22023"),
            ("invalid input syntax for type bytea", "22P02"),
        ] {
            let err =
                DataFusionError::Internal(transported_task(&format!("Execution(\"{raised}\")")));
            assert_eq!(enriched(&err).0, want, "for {raised}");
        }
    }

    /// The third named exception: `nth_value(x, 0)`, refused inside the executor that
    /// evaluates the window. `22016` is PostgreSQL's code for this one argument, and the
    /// local shape is here too because the coordinator evaluates some windows itself —
    /// the client must not see the SQLSTATE change with the plan.
    #[test]
    fn a_transported_non_positive_nth_value_offset_reports_postgresqls_sqlstate() {
        const RAISED: &str = "argument of nth_value must be greater than zero";
        let ballista = transported_task(&format!("Execution(\"{RAISED}\")"));
        for err in [
            DataFusionError::Execution(RAISED.into()),
            DataFusionError::Execution(ballista.clone()),
            DataFusionError::Internal(ballista.clone()),
            DataFusionError::Context(
                "collect".into(),
                Box::new(DataFusionError::Execution(ballista.clone())),
            ),
            DataFusionError::External(Box::new(std::io::Error::other(ballista.clone()))),
        ] {
            assert_eq!(enriched(&err).0, "22016", "for {err:?}");
        }
    }

    /// The structured half of the fix: a code chosen where the error was raised beats
    /// every guess made at this end, and the tag itself never reaches the client.
    #[test]
    fn a_tagged_code_survives_the_scheduler_and_is_reported_verbatim() {
        for (code, sqlstate) in [
            (VdbErrorCode::FeatureNotSupported, "0A000"),
            (VdbErrorCode::InvalidParameterValue, "22023"),
            (VdbErrorCode::GroupingError, "42803"),
        ] {
            let raised = vairedb_common::error::tagged_message(code, "what the client reads");
            let err = DataFusionError::Internal(transported_task(&format!("Plan({raised:?})")));
            let (reported, message) = enriched(&err);
            assert_eq!(reported, sqlstate, "for {code:?}");
            assert_eq!(
                message,
                format!("[VDB-{}] what the client reads", code as i32)
            );
            // Exactly one code, and it is the coordinator's own formatting of it.
            assert_eq!(message.matches("[VDB-").count(), 1, "{message}");
        }
    }

    /// The nested aggregate and the misplaced window function, which are the rows this
    /// closes. Both reach the *physical* planner — on an executor — and DataFusion refuses
    /// them with one catch-all that says nothing about which; PostgreSQL says `42803` and
    /// `42P20`, and used to say `XX000` here.
    ///
    /// The unreadable `Expr` debug dump goes with the wrong class: it named DataFusion's
    /// internal planner rather than anything about the statement.
    #[test]
    fn a_misplaced_aggregate_or_window_reports_postgresqls_class_and_wording() {
        let cases = [
            (
                "AggregateFunction(AggregateFunction { func: AggregateUDF { inner: Max { \
                 signature: Signature { type_signature: UserDefined, volatility: Immutable } } }, \
                 args: [AggregateFunction(…)] })",
                "42803",
                AGGREGATE_NOT_ALLOWED,
            ),
            (
                "WindowFunction(WindowFunction { fun: WindowUDF(WindowUDF { inner: RowNumber { \
                 signature: Signature { type_signature: Nullary, volatility: Immutable } } }), \
                 params: WindowFunctionParams { … } })",
                "42P20",
                WINDOW_NOT_ALLOWED,
            ),
        ];

        for (expr, sqlstate, wording) in cases {
            let raised = format!("This feature is not implemented: {PHYSICAL_EXPR_REFUSAL}{expr}");
            // Both of Ballista's renderings, since either can carry this one.
            for err in [
                DataFusionError::Execution(format!(
                    "Job WV0k16o failed: DataFusion error: {raised}"
                )),
                DataFusionError::Internal(transported_task(&format!(
                    "NotImplemented({:?})",
                    format!("{PHYSICAL_EXPR_REFUSAL}{expr}")
                ))),
                // And the local shape, so a plan the coordinator executes itself agrees.
                DataFusionError::NotImplemented(format!("{PHYSICAL_EXPR_REFUSAL}{expr}")),
            ] {
                let (reported, message) = enriched(&err);
                assert_eq!(reported, sqlstate, "for {err:?}");
                assert!(message.ends_with(wording), "{message}");
                for leaked in ["Physical plan", "Signature", "AggregateUDF", "WindowUDF"] {
                    assert!(!message.contains(leaked), "{leaked} survived: {message}");
                }
            }
        }
    }

    /// Every other transported failure keeps the class its *variant* means, which is the
    /// whole point of § 1.3: a client cannot tell from the SQLSTATE which side of the
    /// scheduler noticed. Each row is the same error the local path already classifies
    /// this way — `count(DISTINCT a, b)` and a subquery in the select list among them.
    #[test]
    fn a_transported_error_keeps_the_class_of_the_variant_it_was_raised_as() {
        for (raised, want) in [
            (
                "NotImplemented(\"count DISTINCT with multiple arguments\")",
                "0A000",
            ),
            (
                "NotImplemented(\"Physical plan does not support logical expression \
                 Exists(Exists { .. })\")",
                "0A000",
            ),
            ("Plan(\"No field named nope\")", "42703"),
            ("Plan(\"table 'nope' not found\")", "42P01"),
            ("Plan(\"something the planner refused\")", "42601"),
            ("SQL(ParserError(\"unexpected token\"), None)", "42601"),
            ("Execution(\"ArrowError(DivideByZero)\")", "22012"),
            (
                "Execution(\"ArrowError(CastError(\\\"not a number\\\"))\")",
                "22P02",
            ),
            ("ResourcesExhausted(\"memory budget\")", "53000"),
        ] {
            let err = DataFusionError::Internal(transported_task(raised));
            let (reported, _) = enriched(&err);
            assert_eq!(reported, want, "for {raised}");
        }
    }

    /// The over-reach probe. Recovery runs only where classification gave up, so an error
    /// whose variant is still in hand is untouched, and text that names no variant keeps
    /// the answer it already had — a transport failure is still the server's fault.
    #[test]
    fn recovery_does_not_reclassify_what_it_was_not_asked_to() {
        // A variant still in hand: `Plan` wins even though the *message* mentions another.
        let (reported, _) = enriched(&DataFusionError::Plan(
            "No function matches 'Execution(x)'".into(),
        ));
        assert_eq!(reported, "0A000");

        // Nothing recoverable: an internal failure stays internal rather than being
        // forced into a class it does not have.
        for raised in [
            "Job 3QdcFzH failed: stage 1 failed",
            "connection refused",
            "invariant violated: partition count is zero",
        ] {
            let (reported, _) = enriched(&DataFusionError::Internal(raised.into()));
            assert_eq!(reported, "XX000", "for {raised}");
        }
    }

    /// An untyped error takes the same route, because a transported failure does not
    /// always arrive with a `DataFusionError` around it — the write path and the node RPC
    /// surface both hand back strings.
    #[test]
    fn an_untyped_transported_error_is_recovered_too() {
        let raised = vairedb_common::error::tagged_message(
            VdbErrorCode::FeatureNotSupported,
            "percentile_disc with an array of fractions is not supported",
        );
        let (reported, message) = user_error(enrich_generic_error(
            &transported_task(&format!("Plan({raised:?})")),
            &ErrorContext::default(),
        ));
        assert_eq!(reported, "0A000");
        assert!(
            message.ends_with("percentile_disc with an array of fractions is not supported"),
            "{message}"
        );
    }

    // --- make_vdb_error ---

    /// The code reaches the client twice, as a message prefix and as a SQLSTATE, and
    /// both have to be *that* code's: a client branching on one while a human reads the
    /// other must not be shown two different failures. The caller's sentence is passed
    /// through untouched, which is what makes this the path for a refusal the
    /// coordinator worded itself.
    #[test]
    fn make_vdb_error_reports_the_code_as_both_a_prefix_and_a_sqlstate() {
        for (code, prefix, sqlstate) in [
            (VdbErrorCode::TableNotFound, "[VDB-1000]", "42P01"),
            (VdbErrorCode::TableAlreadyExists, "[VDB-1005]", "42P07"),
            (VdbErrorCode::ColumnAlreadyExists, "[VDB-1006]", "42701"),
        ] {
            let (reported, message) = user_error(make_vdb_error(code, "the client's sentence"));
            assert_eq!(reported, sqlstate, "for {code:?}");
            assert!(message.starts_with(prefix), "for {code:?}: {message}");
            assert!(message.ends_with("the client's sentence"), "{message}");
        }
    }
}
