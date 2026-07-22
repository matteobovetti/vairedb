//! Pure, I/O-free shaping of table metadata from DDL ASTs: parsing a
//! `CREATE TABLE` into its sharding config + columns, and applying a single
//! `ALTER TABLE` operation to an in-memory [`TableMeta`]. Keeping these separate
//! from the orchestration in [`super::ddl`] means the rules that decide *what* a
//! table's metadata becomes are unit-testable without a catalog or network.

use std::collections::HashMap;

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, BinaryOperator, CharacterLength, ColumnOption,
    CreateTable, CreateTableOptions, DataType, Expr, SqlOption, Value,
};

use vairedb_common::proto::vairedb::v1::VdbErrorCode;

use crate::catalog::{ColumnDef, TableMeta};
use crate::pgwire_handler::error_enrichment::make_vdb_error;
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
}

/// Parse the `WITH (...)` options and column list of a `CREATE TABLE` into a
/// [`CreateTableConfig`]. Recognizes `shards`, `replication_factor`, `shard_by`,
/// and `anonymized_columns`; unknown options are ignored. When no `shard_by` is
/// given the shard key defaults to the first column (or `"id"` if there are
/// none), and a `HASH(col)` wrapper is unwrapped to the bare column name.
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
            .map(|c| c.name.value.clone())
            .unwrap_or_else(|| "id".to_string());
    }

    let shard_key = shard_key
        .strip_prefix("HASH(")
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(&shard_key)
        .to_string();

    let columns: Vec<ColumnDef> = create.columns.iter().map(column_def_from_ast).collect();

    validate_anonymized_columns(&anonymized_columns, create)?;

    Ok(CreateTableConfig {
        shard_count,
        replication_factor,
        shard_key,
        columns,
        anonymized_columns,
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
/// nullable unless it carries a `NOT NULL` constraint.
fn column_def_from_ast(col: &sqlparser::ast::ColumnDef) -> ColumnDef {
    let nullable = !col
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull));
    ColumnDef {
        name: col.name.value.clone(),
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
            let col_name = &column_def.name.value;
            if table_meta.columns.iter().any(|c| c.name == *col_name) {
                if *if_not_exists {
                    return Ok(());
                }
                return Err(make_vdb_error(
                    VdbErrorCode::ColumnAlreadyExists,
                    format!("column \"{}\" of relation already exists", col_name),
                ));
            }
            table_meta.columns.push(column_def_from_ast(column_def));
        }
        AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            for column_name in column_names {
                let name = &column_name.value;
                reject_if_anonymized(table_meta, name, "drop")?;
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
            let old_name = &old_column_name.value;
            let new_name = &new_column_name.value;
            reject_if_anonymized(table_meta, old_name, "rename")?;
            let col = find_column_mut(table_meta, old_name)?;
            col.name = new_name.clone();
            if table_meta.shard_key == *old_name {
                table_meta.shard_key = new_name.clone();
            }
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            let name = &column_name.value;
            reject_if_anonymized(table_meta, name, "alter")?;
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
    use super::*;
    use crate::catalog::ShardStrategy;
    use crate::sql_compat;
    use sqlparser::ast::Statement;

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
        let stmts = sql_compat::parse_sql(sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            Statement::CreateTable(create) => create,
            _ => panic!("expected CREATE TABLE statement"),
        }
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
        }
    }

    fn parse_alter_ops(sql: &str) -> Vec<AlterTableOperation> {
        let stmts = sql_compat::parse_sql(sql).unwrap();
        match &stmts[0] {
            Statement::AlterTable { operations, .. } => operations.clone(),
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
}
