//! Pure, I/O-free shaping of table metadata from DDL ASTs: parsing a
//! `CREATE TABLE` into its sharding config + columns, and applying a single
//! `ALTER TABLE` operation to an in-memory [`TableMeta`]. Keeping these separate
//! from the orchestration in [`super::ddl`] means the rules that decide *what* a
//! table's metadata becomes are unit-testable without a catalog or network.

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType as ArrowType, Schema};

use crate::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, BinaryOperator, CharacterLength,
    ColumnDef as AstColumnDef, ColumnOption, ColumnOptionDef, CreateTable, CreateTableOptions,
    DataType, ExactNumberInfo, Expr, Ident, SqlOption, TimezoneInfo, Value,
};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ColumnDef, ConstraintMeta, TableMeta};
use crate::column_types::unserviceable_type_reason;
use crate::pgwire_handler::constraints;
use crate::pgwire_handler::constraints::constraints_from_create;
use crate::pgwire_handler::error_enrichment::make_vdb_error;
use crate::pgwire_handler::query_router::{
    canonical_table_name, canonicalize_ident, canonicalize_ident_str,
};
use pgwire::error::PgWireResult;

/// HMAC-SHA256 hex digests are 64 characters, so an anonymized column must be a
/// string type able to hold at least this many characters.
const DIGEST_LEN: u64 = 64;

/// The sharding configuration and columns parsed from a `CREATE TABLE`, before
/// any cluster-dependent defaulting. `shard_count == 0` means the statement did
/// not specify one, so the caller substitutes a node-count-derived default.
pub(super) struct CreateTableConfig {
    pub shard_count: u32,
    pub replication_factor: u32,
    pub shard_key: String,
    pub columns: Vec<ColumnDef>,
    /// Map of column name -> anonymization-secret id for pseudonymized columns.
    pub anonymized_columns: HashMap<String, String>,
    /// The table's constraints, column-level ones first. Only the kinds each shard
    /// can enforce correctly on its own rows get this far — see
    /// [`crate::pgwire_handler::constraints`].
    pub constraints: Vec<ConstraintMeta>,
}

/// Reject the `CREATE TABLE` forms that carry no column definitions of their own,
/// before any catalog or storage-node state is touched.
///
/// Sharding is decided once, at CREATE TABLE, from the column list: the shard key
/// is `shard_by` or else the first column. A statement that derives its columns
/// from somewhere else — `LIKE`, `CLONE` — gives the coordinator nothing to derive
/// it from, and the fallback would name a column the table may not even have.
/// Every later write then either rejects or routes on a key that does not exist,
/// and the per-shard DDL broadcast fails part-way, so the client sees a transport
/// error for a table that was already registered in the catalog. A single up-front
/// `0A000` is the honest answer.
///
/// `AS SELECT` is the one derived form that *is* supported: its columns come from
/// the query's result schema, so [`super::ddl`] materializes the query first and
/// re-enters CREATE TABLE with a real column list. It never reaches here.
pub(super) fn reject_unsupported_create_table_form(create: &CreateTable) -> PgWireResult<()> {
    let form = if create.like.is_some() {
        "CREATE TABLE ... LIKE"
    } else if create.clone.is_some() {
        "CREATE TABLE ... CLONE"
    } else if create.columns.is_empty() {
        "CREATE TABLE without column definitions"
    } else {
        return Ok(());
    };

    Err(make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!(
            "{form} is not supported by VaireDB: a table's shard key is fixed at \
             CREATE TABLE and is taken from its column list, which this statement \
             does not provide. Create the table with an explicit column list and \
             WITH (shard_by = '<column>'), then load the rows with INSERT"
        ),
    ))
}

/// Reject a column whose declared type cannot be read back, naming the column and what
/// to declare instead.
///
/// The check belongs at DDL and not at the first read for the reason
/// [`unserviceable_type_reason`] gives: the loss happens below the coordinator, so no
/// later stage can undo it, and every stage after this one has already accepted rows.
/// Applied wherever a type enters a table — `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`
/// and `ALTER COLUMN ... TYPE` — because a column added later is read by the same code
/// as one declared up front.
pub(super) fn reject_unserviceable_column_type(
    column_name: &str,
    data_type: &DataType,
) -> PgWireResult<()> {
    let declared = data_type.to_string();
    match unserviceable_type_reason(&declared) {
        None => Ok(()),
        Some(reason) => Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!("column \"{column_name}\" of type {declared} is not supported: {reason}"),
        )),
    }
}

/// [`reject_unserviceable_column_type`] for every column of a `CREATE TABLE`.
pub(super) fn reject_unserviceable_column_types(create: &CreateTable) -> PgWireResult<()> {
    for col in &create.columns {
        reject_unserviceable_column_type(&col.name.value, &col.data_type)?;
    }
    Ok(())
}

/// Parse the `WITH (...)` options and column list of a `CREATE TABLE` into a
/// [`CreateTableConfig`]. Recognizes `shards`, `replication_factor`, `shard_by`,
/// and `anonymized_columns`; unknown options are ignored. When no `shard_by` is
/// given the shard key defaults to the first column (or `"id"` if there are
/// none — a case [`reject_unsupported_create_table_form`] has already refused for
/// any statement that reached here through the handler), and a `HASH(col)` wrapper
/// is unwrapped to the bare column name.
/// `replication_factor` defaults to `default_replication_factor` when not
/// specified. Returns a client-facing error if `anonymized_columns` names a
/// column that does not exist or is not a string type long enough to hold the
/// 64-character digest.
pub(super) fn parse_create_table_config(
    create: &CreateTable,
    default_replication_factor: u32,
) -> PgWireResult<CreateTableConfig> {
    let mut shard_count: u32 = 0;
    let mut replication_factor: u32 = default_replication_factor;
    let mut shard_key = String::new();
    let mut anonymized_columns: HashMap<String, String> = HashMap::new();

    let with_options = match &create.table_options {
        CreateTableOptions::With(opts) => opts.as_slice(),
        _ => &[],
    };

    for option in with_options {
        if let SqlOption::KeyValue { key, value } = option {
            match key.value.to_lowercase().as_str() {
                "shards" => {
                    if let Expr::Value(v) = value
                        && let Some(n) = value_to_u32(&v.value)
                    {
                        shard_count = n;
                    }
                }
                "replication_factor" => {
                    if let Expr::Value(v) = value
                        && let Some(n) = value_to_u32(&v.value)
                    {
                        replication_factor = n;
                    }
                }
                "shard_by" => {
                    if let Expr::Value(v) = value {
                        shard_key = value_to_string(&v.value);
                    }
                }
                "anonymized_columns" => {
                    anonymized_columns = parse_anonymized_columns(value)?;
                }
                _ => {}
            }
        }
    }

    if shard_key.is_empty() {
        shard_key = create
            .columns
            .first()
            .map(|c| canonicalize_ident(&c.name))
            .unwrap_or_else(|| "id".to_string());
    }

    // The shard key is a catalog key that every write-path comparison folds
    // against, so store it canonical. `shard_by` arrives as a string literal, so
    // it is folded by [`canonicalize_ident_str`] rather than as a parsed ident.
    let shard_key = canonicalize_ident_str(
        shard_key
            .strip_prefix("HASH(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(&shard_key),
    );

    let columns: Vec<ColumnDef> = create.columns.iter().map(column_def_from_ast).collect();

    validate_anonymized_columns(&anonymized_columns, create)?;

    // Needs the columns and the shard key, since a constraint is validated against
    // both: its columns must exist, and uniqueness must cover the key. The name is
    // only what an unnamed constraint is named after; the caller has already
    // reported an unusable one.
    let table_name = canonical_table_name(&create.name).unwrap_or_default();
    let constraints = constraints_from_create(create, &table_name, &shard_key, &columns)?;

    Ok(CreateTableConfig {
        shard_count,
        replication_factor,
        shard_key,
        columns,
        anonymized_columns,
        constraints,
    })
}

/// The canonical column name given by `WITH (shard_by = '<column>')`, or `None`
/// when the statement does not name one.
///
/// [`parse_create_table_config`] defaults a missing `shard_by` to the first
/// column, which is right for a client-written column list but not for
/// `CREATE TABLE ... AS SELECT`: a query's leading result column is an accident of
/// how the SELECT was written, and the shard key it picks can never be changed
/// afterwards. Callers that need the key stated explicitly ask here first.
pub(super) fn explicit_shard_by(create: &CreateTable) -> Option<String> {
    let CreateTableOptions::With(options) = &create.table_options else {
        return None;
    };
    options.iter().find_map(|option| {
        let SqlOption::KeyValue { key, value } = option else {
            return None;
        };
        if !key.value.eq_ignore_ascii_case("shard_by") {
            return None;
        }
        let Expr::Value(v) = value else { return None };
        let name = value_to_string(&v.value);
        let name = name
            .strip_prefix("HASH(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(&name);
        if name.is_empty() {
            None
        } else {
            Some(canonicalize_ident_str(name))
        }
    })
}

/// The pseudonymized columns declared by `WITH (anonymized_columns = …)`, or an
/// empty map when the statement declares none.
///
/// [`parse_create_table_config`] produces the same map, but only alongside a
/// validation that each name is a real column of the table — which
/// `CREATE TABLE ... AS SELECT` cannot supply until its query has run. A caller that
/// needs to know *whether* the destination pseudonymizes, before spending the query,
/// asks here.
pub(super) fn declared_anonymized_columns(
    create: &CreateTable,
) -> PgWireResult<HashMap<String, String>> {
    let CreateTableOptions::With(options) = &create.table_options else {
        return Ok(HashMap::new());
    };
    for option in options {
        if let SqlOption::KeyValue { key, value } = option
            && key.value.eq_ignore_ascii_case("anonymized_columns")
        {
            return parse_anonymized_columns(value);
        }
    }
    Ok(HashMap::new())
}

/// Column definitions for a table whose shape comes from a query's result schema
/// rather than from a column list the client wrote — `CREATE TABLE ... AS SELECT`.
///
/// The accepted types are exactly those
/// [`crate::write_sql_cl::insert_statements_from_batches`] can re-emit as SQL
/// literals, because that is how the result rows are written into the table this
/// describes: a type accepted here but not there would create a table that can
/// never be filled. A rejected type is named with its column, and the caller
/// refuses before the table exists.
pub(super) fn column_defs_from_result_schema(
    schema: &Schema,
) -> std::result::Result<Vec<AstColumnDef>, String> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = duckdb_type_for(field.data_type())
                .map_err(|reason| format!("column \"{}\": {reason}", field.name()))?;
            // A result column that cannot be null is declared NOT NULL, matching
            // the source: `column_def_from_ast` reads the constraint back out.
            let options = if field.is_nullable() {
                Vec::new()
            } else {
                vec![ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                }]
            };
            Ok(AstColumnDef {
                // Quoted so the result schema's spelling survives into the
                // catalog: an unquoted ident would be folded to lowercase.
                name: Ident::with_quote('"', field.name()),
                data_type,
                options,
            })
        })
        .collect()
}

/// The DuckDB column type that stores a value of Arrow type `dt`.
///
/// The narrower unsigned integers widen to the next signed type that holds their
/// whole range: sqlparser can render `UTINYINT`, `USMALLINT` and `UBIGINT` but has
/// no `UINTEGER`, and widening keeps every value representable without depending on
/// which of those a dialect renders. `UInt64` is the one that cannot widen — no
/// signed type holds it — so it keeps DuckDB's own `UBIGINT`. The read path then
/// advertises that column as `numeric` rather than `bigint`
/// ([`crate::column_types::parse_data_type`]), which is the price of keeping values
/// above `i64::MAX`: they are the reason the column was not declared `BIGINT` here.
fn duckdb_type_for(dt: &ArrowType) -> std::result::Result<DataType, String> {
    Ok(match dt {
        ArrowType::Boolean => DataType::Boolean,
        ArrowType::Int8 => DataType::TinyInt(None),
        ArrowType::Int16 => DataType::SmallInt(None),
        ArrowType::Int32 => DataType::Integer(None),
        ArrowType::Int64 => DataType::BigInt(None),
        ArrowType::UInt8 => DataType::SmallInt(None),
        ArrowType::UInt16 => DataType::Integer(None),
        ArrowType::UInt32 => DataType::BigInt(None),
        ArrowType::UInt64 => DataType::UBigInt,
        ArrowType::Float16 | ArrowType::Float32 => DataType::Real,
        ArrowType::Float64 => DataType::DoublePrecision,
        ArrowType::Decimal128(precision, scale) | ArrowType::Decimal256(precision, scale) => {
            DataType::Decimal(ExactNumberInfo::PrecisionAndScale(
                *precision as u64,
                *scale as i64,
            ))
        }
        ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => DataType::Varchar(None),
        ArrowType::Date32 | ArrowType::Date64 => DataType::Date,
        ArrowType::Time32(_) | ArrowType::Time64(_) => DataType::Time(None, TimezoneInfo::None),
        ArrowType::Timestamp(_, tz) => DataType::Timestamp(
            None,
            match tz {
                Some(_) => TimezoneInfo::WithTimeZone,
                None => TimezoneInfo::None,
            },
        ),
        // An encoding, not a type: declare the column as what it encodes.
        ArrowType::Dictionary(_, value_type) => return duckdb_type_for(value_type),
        // `SELECT NULL AS x` produces this: there is no type to declare, and
        // guessing one would silently decide what the column can ever hold.
        ArrowType::Null => {
            return Err(
                "the query gives this column no type; cast it (e.g. `CAST(NULL AS INTEGER)`) so \
                 the table has one to declare"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "a column of type {other} cannot be created this way: the coordinator \
                 materializes the query's rows and writes them back as literal values, and this \
                 type has no literal form that stores the same value"
            ));
        }
    })
}

/// Parse the `anonymized_columns = [ col -> 'secret_id', ... ]` option value into
/// a column-name -> secret-id map. Each element is a `col -> 'id'` arrow
/// expression. Returns a syntax error for any other shape.
fn parse_anonymized_columns(value: &Expr) -> PgWireResult<HashMap<String, String>> {
    let Expr::Array(array) = value else {
        return Err(make_vdb_error(
            VdbErrorCode::SqlSyntaxError,
            "anonymized_columns must be a list of `column -> 'secret_id'` mappings",
        ));
    };

    let mut map = HashMap::new();
    for elem in &array.elem {
        let Expr::BinaryOp { left, op, right } = elem else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "each anonymized_columns entry must be `column -> 'secret_id'`",
            ));
        };
        if !matches!(op, BinaryOperator::Arrow) {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "anonymized_columns entries must use the `->` mapping operator",
            ));
        }
        let Expr::Identifier(col_ident) = left.as_ref() else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "the left side of an anonymized_columns entry must be a column name",
            ));
        };
        let Expr::Value(secret_val) = right.as_ref() else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "the secret id in an anonymized_columns entry must be a string literal",
            ));
        };
        let Value::SingleQuotedString(secret_id) = &secret_val.value else {
            return Err(make_vdb_error(
                VdbErrorCode::SqlSyntaxError,
                "the secret id in an anonymized_columns entry must be a string literal",
            ));
        };
        // Key on the lowercased column name so matching is case-insensitive, the
        // way SQL identifiers are: a column declared `email` must still be hashed
        // when a client writes `EMAIL`. The lookup side (anonymize_statement)
        // lowercases identically. The secret id is a catalog value, not an
        // identifier, so it keeps its exact case.
        map.insert(col_ident.value.to_ascii_lowercase(), secret_id.clone());
    }
    Ok(map)
}

/// Validate that every anonymized column exists and is a string type able to
/// hold the 64-character digest (per the column-length rule). A `VARCHAR`/`TEXT`
/// with no declared length is accepted (unbounded); a bounded length below 64 is
/// rejected, as is a non-string type.
fn validate_anonymized_columns(
    anonymized_columns: &HashMap<String, String>,
    create: &CreateTable,
) -> PgWireResult<()> {
    for col_name in anonymized_columns.keys() {
        // `col_name` is already lowercased (see parse_anonymized_columns); compare
        // case-insensitively against the declared column identifiers.
        let column = create
            .columns
            .iter()
            .find(|c| c.name.value.eq_ignore_ascii_case(col_name));
        let Some(column) = column else {
            return Err(make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!("anonymized column \"{col_name}\" does not exist in the table"),
            ));
        };
        if !string_type_holds_digest(&column.data_type) {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "anonymized column \"{col_name}\" must be a string type of at least {DIGEST_LEN} characters to hold the HMAC-SHA256 digest"
                ),
            ));
        }
    }
    Ok(())
}

/// Whether `dt` is a string type that can hold a 64-character digest: a
/// character type with length >= 64 or unbounded, or an unbounded text type.
fn string_type_holds_digest(dt: &DataType) -> bool {
    match dt {
        DataType::Varchar(len)
        | DataType::CharVarying(len)
        | DataType::CharacterVarying(len)
        | DataType::Char(len)
        | DataType::Character(len)
        | DataType::Nvarchar(len) => character_length_holds_digest(len.as_ref()),
        DataType::Text | DataType::String(None) | DataType::TinyText => true,
        DataType::String(Some(n)) => *n >= DIGEST_LEN,
        _ => false,
    }
}

/// A character-length constraint is adequate when it is absent (unbounded) or
/// specifies at least `DIGEST_LEN` characters. A length in bytes is treated the
/// same, since the digest is ASCII hex (one byte per character).
fn character_length_holds_digest(len: Option<&CharacterLength>) -> bool {
    match len {
        None => true,
        Some(CharacterLength::Max) => true,
        Some(CharacterLength::IntegerLength { length, .. }) => *length >= DIGEST_LEN,
    }
}

/// Build a catalog [`ColumnDef`] from a parsed column definition. A column is
/// nullable unless it carries a `NOT NULL` constraint. The name is stored
/// canonical (see [`canonicalize_ident`]) because it is what the write path
/// compares a client's identifiers against.
fn column_def_from_ast(col: &crate::sqlparser::ast::ColumnDef) -> ColumnDef {
    let nullable = !col
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull));
    ColumnDef {
        name: canonicalize_ident(&col.name),
        data_type: col.data_type.to_string(),
        nullable,
        default_expr: String::new(),
    }
}

/// Apply one `ALTER TABLE` operation to `table_meta` in place. Returns a
/// client-facing error for unsupported operations or invalid column references
/// (missing column, dropping the shard key, duplicate add). Pure: mutates only
/// the passed metadata, performing no catalog or network I/O.
pub(super) fn apply_alter_operation(
    table_meta: &mut TableMeta,
    op: &AlterTableOperation,
) -> PgWireResult<()> {
    match op {
        AlterTableOperation::AddColumn {
            column_def,
            if_not_exists,
            ..
        } => {
            let col_name = &canonicalize_ident(&column_def.name);
            if table_meta.columns.iter().any(|c| c.name == *col_name) {
                if *if_not_exists {
                    return Ok(());
                }
                return Err(make_vdb_error(
                    VdbErrorCode::ColumnAlreadyExists,
                    format!("column \"{}\" of relation already exists", col_name),
                ));
            }
            reject_unserviceable_column_type(&column_def.name.value, &column_def.data_type)?;
            table_meta.columns.push(column_def_from_ast(column_def));
        }
        AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            for column_name in column_names {
                let name = &canonicalize_ident(column_name);
                reject_if_anonymized(table_meta, name, "drop")?;
                reject_if_indexed(table_meta, name, "drop")?;
                constraints::reject_drop_of_constrained_column(table_meta, name)?;
                if *name == table_meta.shard_key {
                    return Err(make_vdb_error(
                        VdbErrorCode::FeatureNotSupported,
                        format!(
                            "cannot drop column \"{}\" because it is the shard key",
                            name
                        ),
                    ));
                }
                let pos = table_meta.columns.iter().position(|c| c.name == *name);
                match pos {
                    Some(i) => {
                        table_meta.columns.remove(i);
                        // Whatever CHECK covered it goes with it, silently, exactly
                        // as it does on the shards.
                        constraints::forget_constraints_on_dropped_column(table_meta, name);
                    }
                    None => {
                        if !*if_exists {
                            return Err(make_vdb_error(
                                VdbErrorCode::ColumnNotFound,
                                format!("column \"{}\" does not exist", name),
                            ));
                        }
                    }
                }
            }
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            let old_name = &canonicalize_ident(old_column_name);
            let new_name = &canonicalize_ident(new_column_name);
            reject_if_anonymized(table_meta, old_name, "rename")?;
            reject_if_indexed(table_meta, old_name, "rename")?;
            let col = find_column_mut(table_meta, old_name)?;
            col.name = new_name.clone();
            if table_meta.shard_key == *old_name {
                table_meta.shard_key = new_name.clone();
            }
            // The shards rewrite a constraint around the new name rather than
            // dropping it, so the recorded constraints follow the column.
            constraints::rename_column_in_constraints(table_meta, old_name, new_name);
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            let name = &canonicalize_ident(column_name);
            reject_if_anonymized(table_meta, name, "alter")?;
            // A default lives in the table's metadata and leaves every index valid,
            // so the shards accept it while one exists. A type or nullability change
            // rewrites the column, which they refuse.
            let indexed_verb = match op {
                AlterColumnOperation::SetDataType { .. } => Some("change the type of"),
                AlterColumnOperation::SetNotNull | AlterColumnOperation::DropNotNull => {
                    Some("change the nullability of")
                }
                _ => None,
            };
            if let Some(verb) = indexed_verb {
                reject_if_indexed(table_meta, name, verb)?;
            }
            // A declared constraint blocks a narrower set of changes than an index
            // does, and each for its own reason — see
            // [`crate::pgwire_handler::constraints`].
            match op {
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    reject_unserviceable_column_type(name, data_type)?;
                    constraints::reject_retype_of_constrained_column(table_meta, name)?;
                }
                AlterColumnOperation::DropNotNull => {
                    constraints::reject_nullable_primary_key_column(table_meta, name)?;
                }
                _ => {}
            }
            let col = find_column_mut(table_meta, name)?;
            match op {
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    col.data_type = data_type.to_string();
                }
                AlterColumnOperation::SetNotNull => {
                    col.nullable = false;
                }
                AlterColumnOperation::DropNotNull => {
                    col.nullable = true;
                }
                AlterColumnOperation::SetDefault { value } => {
                    col.default_expr = value.to_string();
                }
                AlterColumnOperation::DropDefault => {
                    col.default_expr = String::new();
                }
                other => {
                    return Err(make_vdb_error(
                        VdbErrorCode::FeatureNotSupported,
                        format!("ALTER COLUMN operation not supported: {}", other),
                    ));
                }
            }
        }
        other => {
            return Err(make_vdb_error(
                VdbErrorCode::FeatureNotSupported,
                format!("ALTER TABLE operation not supported: {}", other),
            ));
        }
    }
    Ok(())
}

/// Reject an `ALTER TABLE` operation (`op` names the verb, e.g. "rename") that
/// targets an anonymized column. Renaming, dropping, or retyping such a column
/// would silently desync the `anonymized_columns` map from the live schema and
/// disable pseudonymization on that column, so we refuse it outright rather than
/// leak plaintext on the next write. Matched case-insensitively, since the map
/// is keyed on lowercased column names.
fn reject_if_anonymized(table_meta: &TableMeta, name: &str, op: &str) -> PgWireResult<()> {
    if table_meta
        .anonymized_columns
        .contains_key(&name.to_ascii_lowercase())
    {
        return Err(make_vdb_error(
            VdbErrorCode::FeatureNotSupported,
            format!(
                "cannot {op} column \"{name}\" because it is an anonymized column; \
                 drop and recreate the table to change anonymized columns"
            ),
        ));
    }
    Ok(())
}

/// A physical index on the shards that stands in the way of a column change, and the
/// statement that removes it. Two things produce one: a secondary index, and a
/// constraint VaireDB enforces with an index of its own (see
/// [`crate::pgwire_handler::constraints`]). The shards cannot tell them apart, so
/// they block the same operations; only the removal statement differs.
struct BlockingIndex<'a> {
    name: &'a str,
    /// What to call it in the error: "index" or "constraint".
    noun: &'static str,
    /// A statement the client can copy to remove it.
    removal: String,
    /// Whether it is built on the column being changed.
    on_this_column: bool,
}

/// Reject a column-changing `ALTER TABLE` operation (`op` names the verb, e.g.
/// "drop") on a table that carries an index, naming what to drop first.
///
/// **The block is per table, not per column.** The shards' engine treats an index
/// as a dependency on the whole table: `Dependency Error: Cannot alter entry
/// "<table>"` comes back for a column the index does not even cover. Only adding a
/// column and changing a column's default are metadata-only enough to be allowed
/// while an index exists — see the caller.
///
/// Without this check the client gets "ALTER TABLE partially failed: could not
/// reach N node(s)" — the wrong error, pointing at the cluster instead of at the
/// index — and the metadata would go on listing an index over a column that no
/// longer exists under that name or type.
fn reject_if_indexed(table_meta: &TableMeta, name: &str, op: &str) -> PgWireResult<()> {
    let blocking: Vec<BlockingIndex<'_>> = table_meta
        .indexes
        .iter()
        .map(|idx| BlockingIndex {
            name: &idx.name,
            noun: "index",
            removal: format!("DROP INDEX \"{}\"", idx.name),
            on_this_column: idx.columns.iter().any(|c| c == name),
        })
        .chain(
            table_meta
                .constraints
                .iter()
                .filter(|c| c.index_backed)
                .map(|c| BlockingIndex {
                    name: &c.name,
                    noun: "constraint",
                    removal: format!(
                        "ALTER TABLE {} DROP CONSTRAINT \"{}\"",
                        table_meta.table_name, c.name
                    ),
                    on_this_column: c.columns.iter().any(|covered| covered == name),
                }),
        )
        .collect();
    if blocking.is_empty() {
        return Ok(());
    }
    // One built on this very column is the clearest thing to point at; when none is,
    // every index on the table has to go, since any one of them blocks the operation.
    let (reason, blocking) = match blocking.iter().find(|b| b.on_this_column) {
        Some(index) => (
            format!("{} \"{}\" is built on it", index.noun, index.name),
            vec![index],
        ),
        None => (
            "the table carries an index — its own, or one enforcing a constraint — and \
             the shards' engine refuses to alter a table an index depends on, even a \
             column no index covers"
                .to_string(),
            blocking.iter().collect(),
        ),
    };
    // One gives a statement the client can copy; several cannot, since each removal
    // statement takes one name.
    let hint = match blocking.as_slice() {
        [only] => format!("{} first", only.removal),
        many => format!(
            "remove all of them first: {}",
            many.iter()
                .map(|b| b.removal.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        ),
    };
    Err(make_vdb_error(
        VdbErrorCode::FeatureNotSupported,
        format!("cannot {op} column \"{name}\" because {reason}; {hint}"),
    ))
}

/// Mutably borrow the column named `name`, or a `ColumnNotFound` error.
fn find_column_mut<'a>(
    table_meta: &'a mut TableMeta,
    name: &str,
) -> PgWireResult<&'a mut ColumnDef> {
    table_meta
        .columns
        .iter_mut()
        .find(|c| c.name == *name)
        .ok_or_else(|| {
            make_vdb_error(
                VdbErrorCode::ColumnNotFound,
                format!("column \"{}\" does not exist", name),
            )
        })
}

fn value_to_u32(v: &Value) -> Option<u32> {
    match v {
        Value::Number(n, _) => n.parse::<u32>().ok(),
        Value::SingleQuotedString(s) => s.parse::<u32>().ok(),
        _ => None,
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::SingleQuotedString(s) => s.clone(),
        Value::Number(n, _) => n.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::ShardStrategy;
    use crate::pgwire_handler::parser;
    use crate::sqlparser::ast::Statement;
    use datafusion::arrow::datatypes::Field;

    // --- value_to_u32 tests ---

    #[test]
    fn test_value_to_u32_number() {
        let v = Value::Number("42".to_string(), false);
        assert_eq!(value_to_u32(&v), Some(42));
    }

    #[test]
    fn test_value_to_u32_number_long() {
        let v = Value::Number("0".to_string(), true);
        assert_eq!(value_to_u32(&v), Some(0));
    }

    #[test]
    fn test_value_to_u32_single_quoted_string() {
        let v = Value::SingleQuotedString("10".to_string());
        assert_eq!(value_to_u32(&v), Some(10));
    }

    #[test]
    fn test_value_to_u32_invalid_string() {
        let v = Value::SingleQuotedString("not_a_number".to_string());
        assert_eq!(value_to_u32(&v), None);
    }

    #[test]
    fn test_value_to_u32_null() {
        let v = Value::Null;
        assert_eq!(value_to_u32(&v), None);
    }

    #[test]
    fn test_value_to_u32_negative_number() {
        let v = Value::Number("-1".to_string(), false);
        assert_eq!(value_to_u32(&v), None);
    }

    // --- value_to_string tests ---

    #[test]
    fn test_value_to_string_single_quoted() {
        let v = Value::SingleQuotedString("hello".to_string());
        assert_eq!(value_to_string(&v), "hello");
    }

    #[test]
    fn test_value_to_string_number() {
        let v = Value::Number("123".to_string(), false);
        assert_eq!(value_to_string(&v), "123");
    }

    #[test]
    fn test_value_to_string_null() {
        let v = Value::Null;
        let result = value_to_string(&v);
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_value_to_string_boolean_true() {
        let v = Value::Boolean(true);
        let result = value_to_string(&v);
        assert!(result.contains("TRUE") || result.contains("true"));
    }

    // --- parse_create_table_config tests ---

    fn parse_create(sql: &str) -> CreateTable {
        let stmts = parser::parse_sql(sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            Statement::CreateTable(create) => create,
            _ => panic!("expected CREATE TABLE statement"),
        }
    }

    // --- reject_unsupported_create_table_form tests ---

    fn rejection_reason(sql: &str) -> String {
        let create = parse_create(sql);
        let err = reject_unsupported_create_table_form(&create)
            .expect_err("form supplies no column list, so it must be rejected");
        err.to_string()
    }

    #[test]
    fn create_table_like_is_rejected() {
        let reason = rejection_reason("CREATE TABLE t LIKE u");
        assert!(reason.contains("LIKE"), "got: {reason}");
    }

    #[test]
    fn create_table_with_a_column_list_is_accepted() {
        let create = parse_create("CREATE TABLE t (id INT, v TEXT) WITH (shards = 3)");
        assert!(reject_unsupported_create_table_form(&create).is_ok());
    }

    // --- reject_unserviceable_column_types tests ---

    #[test]
    fn a_column_that_cannot_be_read_back_is_refused_at_create_table() {
        let create = parse_create("CREATE TABLE t (id INT, big HUGEINT)");
        let err = reject_unserviceable_column_types(&create)
            .expect_err("a HUGEINT column cannot be read back, so CREATE TABLE must refuse it")
            .to_string();
        // The column, the type and the way out all have to be in the message: the client
        // has to know which of its columns to change and to what.
        assert!(err.contains("\"big\""), "got: {err}");
        assert!(err.contains("HUGEINT"), "got: {err}");
        assert!(err.contains("DECIMAL(38,0)"), "got: {err}");
    }

    #[test]
    fn a_table_of_serviceable_columns_is_accepted() {
        let create = parse_create(
            "CREATE TABLE t (id INT, amount NUMERIC(10,2), ts TIMESTAMPTZ, t TIME, \
             i INTERVAL, u UBIGINT, b BLOB, tags TEXT[])",
        );
        assert!(reject_unserviceable_column_types(&create).is_ok());
    }

    #[test]
    fn adding_or_retyping_a_column_is_held_to_the_same_rule() {
        let mut table = sample_table();
        let added = apply_alter_operation(
            &mut table,
            &parse_alter_ops("ALTER TABLE t ADD COLUMN m MAP(VARCHAR, INTEGER)")[0],
        )
        .expect_err("a MAP column cannot be read back")
        .to_string();
        assert!(added.contains("MAP"), "got: {added}");

        let retyped = apply_alter_operation(
            &mut table,
            &parse_alter_ops("ALTER TABLE t ALTER COLUMN amount TYPE BIT(8)")[0],
        )
        .expect_err("a BIT column cannot be read back")
        .to_string();
        assert!(retyped.contains("BIT"), "got: {retyped}");
        // Refused means unchanged, not half-applied.
        assert!(table.columns.iter().all(|c| c.name != "m"));
        assert_eq!(
            table
                .columns
                .iter()
                .find(|c| c.name == "amount")
                .map(|c| c.data_type.as_str()),
            Some("DECIMAL(10,2)")
        );
    }

    #[test]
    fn parse_config_reads_with_options() {
        let create = parse_create(
            "CREATE TABLE t (id INT, v TEXT) WITH (shards = 4, replication_factor = 2, shard_by = 'v')",
        );
        let cfg = parse_create_table_config(&create, 3).unwrap();
        assert_eq!(cfg.shard_count, 4);
        assert_eq!(cfg.replication_factor, 2);
        assert_eq!(cfg.shard_key, "v");
        assert_eq!(cfg.columns.len(), 2);
    }

    #[test]
    fn parse_config_defaults_when_unspecified() {
        let create = parse_create("CREATE TABLE t (id INT, v TEXT)");
        let cfg = parse_create_table_config(&create, 3).unwrap();
        // shard_count 0 signals "let the caller derive it from node count".
        assert_eq!(cfg.shard_count, 0);
        assert_eq!(cfg.replication_factor, 3);
        // No shard_by => first column.
        assert_eq!(cfg.shard_key, "id");
        assert!(cfg.anonymized_columns.is_empty());
    }

    #[test]
    fn parse_config_unwraps_hash_shard_key() {
        let create = parse_create("CREATE TABLE t (id INT, v TEXT) WITH (shard_by = 'HASH(id)')");
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert_eq!(cfg.shard_key, "id");
    }

    #[test]
    fn parse_config_marks_not_null_columns() {
        let create = parse_create("CREATE TABLE t (id INT NOT NULL, v TEXT)");
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert!(!cfg.columns[0].nullable);
        assert!(cfg.columns[1].nullable);
    }

    // --- canonical identifier storage ---
    //
    // Catalog column names and the shard key are what every write-path comparison
    // folds against, so they must be stored canonical: unquoted names lowercased,
    // quoted names verbatim. A raw-cased name in the catalog makes a correctly
    // written INSERT miss its shard key and broadcast instead.

    #[test]
    fn column_names_are_stored_lowercased_when_unquoted() {
        let create = parse_create("CREATE TABLE t (ID INT, MixedCase TEXT)");
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert_eq!(cfg.columns[0].name, "id");
        assert_eq!(cfg.columns[1].name, "mixedcase");
    }

    #[test]
    fn quoted_column_names_keep_their_case() {
        let create = parse_create("CREATE TABLE t (\"ID\" INT, \"MixedCase\" TEXT)");
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert_eq!(cfg.columns[0].name, "ID");
        assert_eq!(cfg.columns[1].name, "MixedCase");
    }

    #[test]
    fn default_shard_key_is_canonical() {
        let create = parse_create("CREATE TABLE t (ID INT, v TEXT)");
        assert_eq!(
            parse_create_table_config(&create, 1).unwrap().shard_key,
            "id"
        );
    }

    #[test]
    fn shard_by_option_is_folded_like_an_identifier() {
        let create = parse_create("CREATE TABLE t (id INT, Cust TEXT) WITH (shard_by = 'CUST')");
        assert_eq!(
            parse_create_table_config(&create, 1).unwrap().shard_key,
            "cust"
        );
    }

    #[test]
    fn shard_by_option_honors_inner_double_quotes() {
        // `shard_by = '"Cust"'` names a quoted column, so its case survives.
        let create =
            parse_create("CREATE TABLE t (id INT, \"Cust\" TEXT) WITH (shard_by = '\"Cust\"')");
        assert_eq!(
            parse_create_table_config(&create, 1).unwrap().shard_key,
            "Cust"
        );
    }

    #[test]
    fn hash_wrapper_shard_key_is_also_folded() {
        let create = parse_create("CREATE TABLE t (id INT, v TEXT) WITH (shard_by = 'HASH(ID)')");
        assert_eq!(
            parse_create_table_config(&create, 1).unwrap().shard_key,
            "id"
        );
    }

    #[test]
    fn alter_table_matches_columns_case_insensitively() {
        // The catalog stores `amount`; `ALTER ... AMOUNT` must find it rather than
        // report a missing column.
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN AMOUNT SET DATA TYPE BIGINT");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[2].data_type, "BIGINT");
    }

    #[test]
    fn alter_table_add_column_stores_a_canonical_name() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN Status VARCHAR");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[3].name, "status");
    }

    #[test]
    fn alter_table_add_duplicate_in_another_case_is_rejected() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN ID INTEGER");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn alter_table_cannot_drop_the_shard_key_in_another_case() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN CUSTOMER_ID");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn alter_table_rename_keeps_the_shard_key_in_sync_across_cases() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN CUSTOMER_ID TO Cust_Id");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[1].name, "cust_id");
        assert_eq!(table.shard_key, "cust_id");
    }

    // --- anonymized_columns tests ---

    #[test]
    fn parse_config_reads_anonymized_columns() {
        let create = parse_create(
            "CREATE TABLE t (id INT, name VARCHAR(64), email VARCHAR(128)) WITH (anonymized_columns = [ name -> 'sid', email -> 'sid' ])",
        );
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert_eq!(cfg.anonymized_columns.get("name"), Some(&"sid".to_string()));
        assert_eq!(
            cfg.anonymized_columns.get("email"),
            Some(&"sid".to_string())
        );
    }

    #[test]
    fn anonymized_column_accepts_unbounded_text() {
        let create = parse_create(
            "CREATE TABLE t (id INT, name TEXT) WITH (anonymized_columns = [ name -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_ok());
    }

    #[test]
    fn anonymized_column_too_short_is_rejected() {
        let create = parse_create(
            "CREATE TABLE t (id INT, name VARCHAR(32)) WITH (anonymized_columns = [ name -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_err());
    }

    #[test]
    fn anonymized_column_exactly_64_is_accepted() {
        let create = parse_create(
            "CREATE TABLE t (id INT, name VARCHAR(64)) WITH (anonymized_columns = [ name -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_ok());
    }

    #[test]
    fn anonymized_column_non_string_type_is_rejected() {
        let create = parse_create(
            "CREATE TABLE t (id INT, age INT) WITH (anonymized_columns = [ age -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_err());
    }

    #[test]
    fn anonymized_column_missing_column_is_rejected() {
        let create = parse_create(
            "CREATE TABLE t (id INT, name VARCHAR(64)) WITH (anonymized_columns = [ ghost -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_err());
    }

    #[test]
    fn anonymized_columns_are_keyed_lowercase() {
        // A column declared in mixed case is stored lowercased so the write-path
        // lookup (which also lowercases) matches regardless of the case a client
        // uses in INSERT/UPDATE.
        let create = parse_create(
            "CREATE TABLE t (id INT, Email VARCHAR(64)) WITH (anonymized_columns = [ Email -> 'sid' ])",
        );
        let cfg = parse_create_table_config(&create, 1).unwrap();
        assert_eq!(
            cfg.anonymized_columns.get("email"),
            Some(&"sid".to_string())
        );
        assert!(!cfg.anonymized_columns.contains_key("Email"));
    }

    #[test]
    fn anonymized_column_validates_case_insensitively() {
        // Rule references the column in a different case than its declaration;
        // validation must still find it and pass.
        let create = parse_create(
            "CREATE TABLE t (id INT, email VARCHAR(64)) WITH (anonymized_columns = [ EMAIL -> 'sid' ])",
        );
        assert!(parse_create_table_config(&create, 1).is_ok());
    }

    // --- apply_alter_operation tests ---

    fn sample_table() -> TableMeta {
        TableMeta {
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_expr: String::new(),
                },
                ColumnDef {
                    name: "customer_id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_expr: String::new(),
                },
                ColumnDef {
                    name: "amount".to_string(),
                    data_type: "DECIMAL(10,2)".to_string(),
                    nullable: true,
                    default_expr: String::new(),
                },
            ],
            shard_strategy: ShardStrategy::Hash as i32,
            shard_key: "customer_id".to_string(),
            shard_count: 6,
            replication_factor: 3,
            created_at: None,
            anonymized_columns: std::collections::HashMap::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
        }
    }

    fn parse_alter_ops(sql: &str) -> Vec<AlterTableOperation> {
        let stmts = parser::parse_sql(sql).unwrap();
        match &stmts[0] {
            Statement::AlterTable(alter) => alter.operations.clone(),
            _ => panic!("expected ALTER TABLE statement"),
        }
    }

    /// A sample table whose `amount` column is anonymized (map keyed lowercase,
    /// as CREATE TABLE parsing produces).
    fn table_with_anonymized_amount() -> TableMeta {
        let mut table = sample_table();
        table
            .anonymized_columns
            .insert("amount".to_string(), "sid".to_string());
        table
    }

    #[test]
    fn test_rename_anonymized_column_is_rejected() {
        let mut table = table_with_anonymized_amount();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN amount TO total");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn test_rename_anonymized_column_case_insensitive_is_rejected() {
        let mut table = table_with_anonymized_amount();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN AMOUNT TO total");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn test_drop_anonymized_column_is_rejected() {
        let mut table = table_with_anonymized_amount();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN amount");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn test_alter_anonymized_column_type_is_rejected() {
        let mut table = table_with_anonymized_amount();
        let ops =
            parse_alter_ops("ALTER TABLE orders ALTER COLUMN amount SET DATA TYPE VARCHAR(64)");
        assert!(apply_alter_operation(&mut table, &ops[0]).is_err());
    }

    #[test]
    fn test_alter_non_anonymized_column_still_allowed() {
        // The guard must only fire for anonymized columns; a normal column is
        // unaffected.
        let mut table = table_with_anonymized_amount();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN id TO ident");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert!(table.columns.iter().any(|c| c.name == "ident"));
    }

    /// A sample table with a secondary index on `amount`, as
    /// `pgwire_handler::indexes` would have recorded it.
    fn table_with_index_on_amount() -> TableMeta {
        let mut table = sample_table();
        table.indexes.push(crate::catalog::IndexMeta {
            name: "idx_amount".to_string(),
            columns: vec!["amount".to_string()],
            unique: false,
        });
        table
    }

    // The shards' engine refuses all three, so catching them here is what turns a
    // misleading "the broadcast partially failed" into an error naming the index —
    // and keeps the metadata from listing an index over a column that moved.
    #[test]
    fn test_altering_an_indexed_column_is_rejected_naming_the_index() {
        for sql in [
            "ALTER TABLE orders DROP COLUMN amount",
            "ALTER TABLE orders RENAME COLUMN amount TO total",
            "ALTER TABLE orders ALTER COLUMN amount SET DATA TYPE BIGINT",
        ] {
            let mut table = table_with_index_on_amount();
            let ops = parse_alter_ops(sql);
            let err = apply_alter_operation(&mut table, &ops[0])
                .expect_err(&format!("`{sql}` must be rejected"));
            let msg = err.to_string();
            assert!(
                msg.contains("idx_amount"),
                "`{sql}` must name the index: {msg}"
            );
            assert!(
                msg.contains("DROP INDEX"),
                "`{sql}` must say what to do: {msg}"
            );
        }
    }

    // The shards' engine blocks per *table*, not per column: it refuses to alter a
    // table any index depends on, even for a column the index does not cover. The
    // error still has to name an index to drop.
    #[test]
    fn test_altering_an_unindexed_column_of_an_indexed_table_is_rejected() {
        for sql in [
            "ALTER TABLE orders DROP COLUMN id",
            "ALTER TABLE orders RENAME COLUMN id TO ident",
            "ALTER TABLE orders ALTER COLUMN id SET DATA TYPE BIGINT",
            "ALTER TABLE orders ALTER COLUMN id SET NOT NULL",
            "ALTER TABLE orders ALTER COLUMN id DROP NOT NULL",
        ] {
            let mut table = table_with_index_on_amount();
            let ops = parse_alter_ops(sql);
            let err = apply_alter_operation(&mut table, &ops[0])
                .expect_err(&format!("`{sql}` must be rejected"));
            let msg = err.to_string();
            assert!(
                msg.contains("idx_amount"),
                "`{sql}` must name an index to drop: {msg}"
            );
        }
    }

    // Adding a column and changing a default are metadata-only, so the shards apply
    // them with an index in place and the guard must not fire.
    #[test]
    fn test_an_index_does_not_block_metadata_only_changes() {
        let mut table = table_with_index_on_amount();
        for sql in [
            "ALTER TABLE orders ALTER COLUMN amount SET DEFAULT 0",
            "ALTER TABLE orders ALTER COLUMN amount DROP DEFAULT",
            "ALTER TABLE orders ADD COLUMN note VARCHAR",
        ] {
            let ops = parse_alter_ops(sql);
            apply_alter_operation(&mut table, &ops[0])
                .unwrap_or_else(|e| panic!("`{sql}` must be allowed: {e}"));
        }
    }

    // Several indexes cannot be dropped by one statement, so the error lists them
    // all rather than handing over a statement that would leave the rest in place.
    #[test]
    fn test_every_blocking_index_is_named_when_the_column_is_not_indexed() {
        let mut table = table_with_index_on_amount();
        table.indexes.push(crate::catalog::IndexMeta {
            name: "idx_customer".to_string(),
            columns: vec!["customer_id".to_string()],
            unique: false,
        });
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN id");
        let msg = apply_alter_operation(&mut table, &ops[0])
            .expect_err("must be rejected")
            .to_string();
        assert!(msg.contains("idx_amount"), "{msg}");
        assert!(msg.contains("idx_customer"), "{msg}");
    }

    // --- constraints under column changes ---

    /// [`sample_table`] with a declared `CHECK` on `amount` and a declared
    /// `PRIMARY KEY` on the shard key — the two shapes a `CREATE TABLE` can leave.
    fn table_with_declared_constraints() -> TableMeta {
        let mut table = sample_table();
        table.constraints = vec![
            crate::catalog::ConstraintMeta {
                name: "orders_pkey".to_string(),
                kind: crate::catalog::ConstraintKind::PrimaryKey as i32,
                columns: vec!["customer_id".to_string()],
                definition: "PRIMARY KEY (customer_id)".to_string(),
                index_backed: false,
            },
            crate::catalog::ConstraintMeta {
                name: "ck_amount".to_string(),
                kind: crate::catalog::ConstraintKind::Check as i32,
                columns: vec!["amount".to_string()],
                definition: "CHECK (amount > 0)".to_string(),
                index_backed: false,
            },
        ];
        table
    }

    // The shards refuse each of these under a constraint they declared, so catching
    // them here turns a broadcast that fails on every node into an error naming the
    // constraint — and keeps the metadata from describing a table nobody has.
    #[test]
    fn test_a_declared_constraint_blocks_the_changes_the_shards_refuse() {
        for (sql, needle) in [
            ("ALTER TABLE orders DROP COLUMN customer_id", "orders_pkey"),
            (
                "ALTER TABLE orders ALTER COLUMN customer_id SET DATA TYPE BIGINT",
                "orders_pkey",
            ),
            (
                "ALTER TABLE orders ALTER COLUMN customer_id DROP NOT NULL",
                "orders_pkey",
            ),
            (
                "ALTER TABLE orders ALTER COLUMN amount SET DATA TYPE BIGINT",
                "ck_amount",
            ),
        ] {
            let mut table = table_with_declared_constraints();
            let ops = parse_alter_ops(sql);
            let msg = apply_alter_operation(&mut table, &ops[0])
                .expect_err(&format!("`{sql}` must be rejected"))
                .to_string();
            assert!(
                msg.contains(needle),
                "`{sql}` must name the constraint: {msg}"
            );
        }
    }

    // Unlike an index, a declared constraint is not a dependency on the whole table:
    // the shards apply a change to a column it does not cover, and so does the
    // catalog. Dropping a column a CHECK covers is allowed too — the shards discard
    // the CHECK with it, and so must we.
    #[test]
    fn test_a_declared_constraint_leaves_the_rest_of_the_table_alone() {
        let mut table = table_with_declared_constraints();
        for sql in [
            "ALTER TABLE orders ALTER COLUMN amount SET NOT NULL",
            "ALTER TABLE orders ALTER COLUMN amount SET DEFAULT 0",
            "ALTER TABLE orders ADD COLUMN note VARCHAR",
            "ALTER TABLE orders ALTER COLUMN id SET DATA TYPE BIGINT",
        ] {
            let ops = parse_alter_ops(sql);
            apply_alter_operation(&mut table, &ops[0])
                .unwrap_or_else(|e| panic!("`{sql}` must be allowed: {e}"));
        }

        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN amount");
        apply_alter_operation(&mut table, &ops[0]).expect("a CHECK must not block the drop");
        assert_eq!(
            table
                .constraints
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["orders_pkey"],
            "the CHECK over the dropped column must be forgotten"
        );
    }

    // The shards rewrite the constraint around the new name, so the metadata has to
    // as well or its column list would point at a column that no longer exists.
    #[test]
    fn test_renaming_a_column_carries_its_constraints_along() {
        let mut table = table_with_declared_constraints();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN amount TO total");
        apply_alter_operation(&mut table, &ops[0]).expect("the rename must be allowed");
        let check = table
            .constraints
            .iter()
            .find(|c| c.name == "ck_amount")
            .expect("the CHECK survives the rename");
        assert_eq!(check.columns, vec!["total"]);
        assert_eq!(check.definition, "CHECK (total > 0)");
    }

    // A constraint VaireDB enforces with a per-shard index is an index on the
    // shards, which refuse to alter *any* column of a table an index depends on. The
    // error has to name the statement that removes it, which is not DROP INDEX.
    #[test]
    fn test_an_index_backed_constraint_blocks_column_changes_like_an_index() {
        for sql in [
            "ALTER TABLE orders DROP COLUMN id",
            "ALTER TABLE orders RENAME COLUMN id TO ident",
            "ALTER TABLE orders ALTER COLUMN amount SET DATA TYPE BIGINT",
        ] {
            let mut table = sample_table();
            table.constraints.push(crate::catalog::ConstraintMeta {
                name: "uq_customer".to_string(),
                kind: crate::catalog::ConstraintKind::Unique as i32,
                columns: vec!["customer_id".to_string()],
                definition: "UNIQUE (customer_id)".to_string(),
                index_backed: true,
            });
            let ops = parse_alter_ops(sql);
            let msg = apply_alter_operation(&mut table, &ops[0])
                .expect_err(&format!("`{sql}` must be rejected"))
                .to_string();
            assert!(msg.contains("uq_customer"), "`{sql}`: {msg}");
            assert!(
                msg.contains("DROP CONSTRAINT"),
                "`{sql}` must say what removes it: {msg}"
            );
        }
    }

    #[test]
    fn test_add_column_basic() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN status VARCHAR");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.columns[3].name, "status");
        assert_eq!(table.columns[3].data_type, "VARCHAR");
        assert!(table.columns[3].nullable);
    }

    #[test]
    fn test_add_column_not_null() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN status VARCHAR NOT NULL");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[3].name, "status");
        assert!(!table.columns[3].nullable);
    }

    #[test]
    fn test_add_column_if_not_exists_when_exists() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN IF NOT EXISTS id INTEGER");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn test_add_column_duplicate_errors() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN id INTEGER");
        let result = apply_alter_operation(&mut table, &ops[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_column_basic() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN amount");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 2);
        assert!(table.columns.iter().all(|c| c.name != "amount"));
    }

    #[test]
    fn test_drop_column_if_exists_when_missing() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN IF EXISTS nonexistent");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn test_drop_column_nonexistent_errors() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN nonexistent");
        let result = apply_alter_operation(&mut table, &ops[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_shard_key_column_errors() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN customer_id");
        let result = apply_alter_operation(&mut table, &ops[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_column_basic() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN amount TO total");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[2].name, "total");
        assert_eq!(table.columns[2].data_type, "DECIMAL(10,2)");
    }

    #[test]
    fn test_rename_shard_key_column() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN customer_id TO cust_id");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[1].name, "cust_id");
        assert_eq!(table.shard_key, "cust_id");
    }

    #[test]
    fn test_rename_column_nonexistent_errors() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders RENAME COLUMN nonexistent TO new_name");
        let result = apply_alter_operation(&mut table, &ops[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_alter_column_set_data_type() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN amount SET DATA TYPE BIGINT");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[2].data_type, "BIGINT");
    }

    #[test]
    fn test_alter_column_set_not_null() {
        let mut table = sample_table();
        assert!(table.columns[2].nullable);
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN amount SET NOT NULL");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert!(!table.columns[2].nullable);
    }

    #[test]
    fn test_alter_column_drop_not_null() {
        let mut table = sample_table();
        assert!(!table.columns[0].nullable);
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN id DROP NOT NULL");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert!(table.columns[0].nullable);
    }

    #[test]
    fn test_alter_column_set_default() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN amount SET DEFAULT 0");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[2].default_expr, "0");
    }

    #[test]
    fn test_alter_column_drop_default() {
        let mut table = sample_table();
        table.columns[2].default_expr = "100".to_string();
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN amount DROP DEFAULT");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns[2].default_expr, "");
    }

    #[test]
    fn test_alter_column_nonexistent_errors() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ALTER COLUMN nonexistent SET NOT NULL");
        let result = apply_alter_operation(&mut table, &ops[0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_add_columns() {
        let mut table = sample_table();
        let ops = parse_alter_ops(
            "ALTER TABLE orders ADD COLUMN status VARCHAR, ADD COLUMN created_at TIMESTAMP",
        );
        for op in &ops {
            apply_alter_operation(&mut table, op).unwrap();
        }
        assert_eq!(table.columns.len(), 5);
        assert_eq!(table.columns[3].name, "status");
        assert_eq!(table.columns[4].name, "created_at");
    }

    #[test]
    fn test_add_then_drop_column() {
        let mut table = sample_table();
        let ops = parse_alter_ops("ALTER TABLE orders ADD COLUMN status VARCHAR");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 4);

        let ops = parse_alter_ops("ALTER TABLE orders DROP COLUMN status");
        apply_alter_operation(&mut table, &ops[0]).unwrap();
        assert_eq!(table.columns.len(), 3);
        assert!(table.columns.iter().all(|c| c.name != "status"));
    }

    // --- explicit_shard_by tests ---

    #[test]
    fn explicit_shard_by_reads_the_option_and_folds_it_like_an_identifier() {
        for (sql, expected) in [
            ("CREATE TABLE t WITH (shard_by = 'id') AS SELECT 1", "id"),
            // Unquoted names fold to lowercase, quoted ones keep their case —
            // the same rule the column list is stored under.
            ("CREATE TABLE t WITH (shard_by = 'ID') AS SELECT 1", "id"),
            (
                "CREATE TABLE t WITH (shard_by = '\"Id\"') AS SELECT 1",
                "Id",
            ),
            // `HASH(col)` is the spelling the read path prints; accept it here too.
            (
                "CREATE TABLE t WITH (shard_by = 'HASH(id)') AS SELECT 1",
                "id",
            ),
            (
                "CREATE TABLE t WITH (shards = 3, shard_by = 'customer_id') AS SELECT 1",
                "customer_id",
            ),
        ] {
            assert_eq!(
                explicit_shard_by(&parse_create(sql)).as_deref(),
                Some(expected),
                "`{sql}`"
            );
        }
    }

    // Unlike `parse_create_table_config`, a missing option is *not* defaulted to
    // the first column: the caller needs to know it was never stated.
    #[test]
    fn explicit_shard_by_is_none_when_the_statement_does_not_name_one() {
        for sql in [
            "CREATE TABLE t AS SELECT 1",
            "CREATE TABLE t WITH (shards = 3) AS SELECT 1",
            "CREATE TABLE t WITH (shard_by = '') AS SELECT 1",
            "CREATE TABLE t (id INT, v TEXT)",
        ] {
            assert_eq!(explicit_shard_by(&parse_create(sql)), None, "`{sql}`");
        }
    }

    // --- column_defs_from_result_schema tests ---

    fn schema_of(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    /// The rendered `name TYPE [NOT NULL]` of each derived column.
    fn derived(fields: Vec<Field>) -> Vec<String> {
        column_defs_from_result_schema(&schema_of(fields))
            .expect("every type here is storable")
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    #[test]
    fn result_columns_become_column_defs_with_duckdb_types() {
        let columns = derived(vec![
            Field::new("id", ArrowType::Int64, false),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("ratio", ArrowType::Float64, true),
            Field::new("ok", ArrowType::Boolean, true),
        ]);
        assert_eq!(
            columns,
            vec![
                // Quoted so the result schema's spelling survives canonicalization.
                "\"id\" BIGINT NOT NULL".to_string(),
                "\"name\" VARCHAR".to_string(),
                "\"ratio\" DOUBLE PRECISION".to_string(),
                "\"ok\" BOOLEAN".to_string(),
            ]
        );
    }

    // An anonymized column is validated by matching concrete string-type variants,
    // so a derived string column has to be one of them — not a custom type name.
    #[test]
    fn a_derived_string_column_is_a_type_anonymization_recognizes() {
        let schema = schema_of(vec![Field::new("v", ArrowType::Utf8, true)]);
        let columns = column_defs_from_result_schema(&schema).unwrap();
        assert!(
            string_type_holds_digest(&columns[0].data_type),
            "got: {:?}",
            columns[0].data_type
        );
    }

    // Unsigned widths widen to a signed type that holds them: DuckDB spells its own
    // unsigned types differently and the value has to round-trip as a literal. The
    // exception is UInt64, which no signed type holds — and which must not fall back to
    // HUGEINT, because a HUGEINT column cannot be read back at all.
    #[test]
    fn unsigned_result_columns_widen_to_a_signed_type_that_holds_them() {
        let columns = derived(vec![
            Field::new("a", ArrowType::UInt8, true),
            Field::new("b", ArrowType::UInt16, true),
            Field::new("c", ArrowType::UInt32, true),
            Field::new("d", ArrowType::UInt64, true),
        ]);
        assert_eq!(
            columns,
            vec![
                "\"a\" SMALLINT".to_string(),
                "\"b\" INTEGER".to_string(),
                "\"c\" BIGINT".to_string(),
                "\"d\" UBIGINT".to_string(),
            ]
        );
        // And what it derives has to be a type the table can then be created with.
        for column in &columns {
            let declared = column.rsplit(' ').next().unwrap();
            assert_eq!(
                unserviceable_type_reason(declared),
                None,
                "a derived column of type {declared} could not be created"
            );
        }
    }

    // A dictionary is an encoding, not a type: the table stores what it decodes to.
    #[test]
    fn a_dictionary_encoded_result_column_takes_its_value_type() {
        let columns = derived(vec![Field::new(
            "v",
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8)),
            true,
        )]);
        assert_eq!(columns, vec!["\"v\" VARCHAR".to_string()]);
    }

    // A column the query gives no type is refused rather than guessed at, and the
    // message names the column so the client knows where to put the cast.
    #[test]
    fn an_untyped_result_column_is_refused_by_name() {
        let schema = schema_of(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("v", ArrowType::Null, true),
        ]);
        let reason =
            column_defs_from_result_schema(&schema).expect_err("NULL has no storable type");
        assert!(reason.contains("\"v\""), "got: {reason}");
        assert!(reason.contains("cast"), "got: {reason}");
    }

    // The accepted set is exactly what the row re-emitter can write back as a
    // literal, so a type it cannot render must not become a column.
    #[test]
    fn a_result_column_with_no_literal_form_is_refused_by_name() {
        for arrow_type in [
            ArrowType::Binary,
            ArrowType::Duration(datafusion::arrow::datatypes::TimeUnit::Second),
            ArrowType::List(Arc::new(Field::new("item", ArrowType::Int32, true))),
        ] {
            let schema = schema_of(vec![Field::new("v", arrow_type.clone(), true)]);
            let reason = column_defs_from_result_schema(&schema).expect_err(&format!(
                "{arrow_type} has no literal form, so it must be refused"
            ));
            assert!(reason.contains("\"v\""), "{arrow_type}: {reason}");
            assert!(
                reason.contains(&arrow_type.to_string()),
                "the type must be named, {arrow_type}: {reason}"
            );
        }
    }
}
