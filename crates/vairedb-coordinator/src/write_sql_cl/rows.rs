//! Re-emitting materialized result rows as literal `INSERT ... VALUES`
//! statements — the primitive behind every write whose rows the client did not
//! spell out (`INSERT ... SELECT`, `CREATE TABLE AS SELECT`, `COPY ... FROM`).
//!
//! The coordinator hashes a row's shard key *before* the write leaves it, so a
//! row whose values it cannot see cannot be placed; that is the single reason
//! those statements are refused today (see
//! [`super::validate_insert_shard_key`]). Running the source query on the read
//! path first and rendering its rows back as literals removes the reason: what
//! reaches the write path is then an ordinary multi-row `INSERT ... VALUES`, so
//! the shard-key check, the `ON CONFLICT` check, the anonymization rewrite, the
//! per-shard row split and the exact row count all apply unchanged.
//!
//! Literals rather than bind parameters, deliberately: the anonymization rewrite
//! replaces a plaintext value with its digest and cannot digest a value it does
//! not hold, so a parameterized rendering would either fail on every anonymized
//! table or ship plaintext to a shard.
//!
//! A cell is rendered with [`arrow_array_value_to_string`] — the same renderer
//! the read path encodes a text result column with — so a value that
//! round-trips through this path hashes to the shard a literal of the same value
//! routes to, and a client reading the row back sees what it sent.

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;

use crate::pgwire_handler::encoding::arrow_array_value_to_string;
use crate::pgwire_handler::parser::parse_sql;
use crate::sqlparser::ast::{Expr, Insert, Parens, Query, SetExpr, Statement, Value, Values};

/// Rows per synthesized `INSERT`. The whole result is materialized either way
/// (the read path already collects it to encode a response), so this does not
/// bound memory — it bounds the size of one *statement*: the SQL text a shard
/// receives, the AST the anonymization rewrite walks, and the unit a partial
/// failure is reported in.
pub const ROWS_PER_STATEMENT: usize = 1000;

/// How a rendered cell is spelled as a SQL literal.
#[derive(Clone, Copy)]
enum LiteralKind {
    /// A bare numeric token (`42`, `-7`).
    Number,
    /// `true` / `false`.
    Boolean,
    /// A single-quoted string, which DuckDB casts to the target column's type on
    /// the way in. Also the form used for values that *are* numeric but whose
    /// text form is not always a valid numeric token (`NaN`, `inf`): a quoted
    /// numeric string canonicalizes to the same routing value as the bare number
    /// (see [`super::routing_value`]), so quoting costs nothing.
    Quoted,
}

/// The literal spelling for a column of Arrow type `dt`, or `Err(reason)` for a
/// type whose text rendering is not a literal that reads back as the same value.
///
/// Refusing is the honest answer for those: a `Binary` cell renders as hex
/// digits, an `Interval` as a struct-ish debug form, a `List` as `[1, 2]` — each
/// would either fail on the shard or, worse, store something else than the row
/// the source query produced.
fn literal_kind(dt: &DataType) -> std::result::Result<LiteralKind, String> {
    Ok(match dt {
        DataType::Boolean => LiteralKind::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => LiteralKind::Number,
        // Floats and decimals go out quoted: `NaN`/`inf` are not numeric tokens,
        // and a quoted numeric string routes identically to the bare form.
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => LiteralKind::Quoted,
        // Text, and every temporal type: rendered in PostgreSQL's text form,
        // which is exactly the string literal a client would write for it.
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(..) => LiteralKind::Quoted,
        // An all-NULL column carries no value to spell; every cell renders as
        // `NULL` before the kind is consulted.
        DataType::Null => LiteralKind::Quoted,
        // A dictionary is an encoding, not a type: spell its values.
        DataType::Dictionary(_, value_type) => return literal_kind(value_type),
        other => {
            return Err(format!(
                "a value of type {other} cannot be re-emitted as a SQL literal: the \
                 coordinator materializes the source rows and writes them back as \
                 literal values, and this type has no literal form that stores the \
                 same value"
            ));
        }
    })
}

/// Render the cell at `row` of `col` as the SQL literal expression that stores
/// the same value.
fn cell_literal(col: &dyn Array, row: usize, kind: LiteralKind) -> Expr {
    if crate::pgwire_handler::encoding::is_null_on_the_wire(col, row) {
        return Expr::Value(Value::Null.into());
    }
    let text = arrow_array_value_to_string(col, row);
    let value = match kind {
        LiteralKind::Number => Value::Number(text, false),
        // `BooleanArray` renders exactly `true` or `false`.
        LiteralKind::Boolean => Value::Boolean(text == "true"),
        LiteralKind::Quoted => Value::SingleQuotedString(text),
    };
    Expr::Value(value.into())
}

/// The `INSERT INTO table (columns)` statement to use as a template for rows the
/// client's statement did not spell out as an INSERT at all — `COPY ... FROM`,
/// `CREATE TABLE ... AS SELECT`.
///
/// Every identifier is emitted quoted, so a name whose case the catalog kept
/// survives to the shard instead of being folded there. The placeholder row this
/// carries is never written: [`insert_statements_from_batches`] replaces the
/// source with the materialized rows.
pub fn insert_template(table: &str, columns: &[&str]) -> std::result::Result<Statement, String> {
    if columns.is_empty() {
        return Err("INSERT must specify an explicit column list".to_string());
    }
    let column_list = columns
        .iter()
        .map(|c| quoted(c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholder_row = vec!["NULL"; columns.len()].join(", ");
    let sql = format!(
        "INSERT INTO {} ({column_list}) VALUES ({placeholder_row})",
        quoted(table)
    );
    parse_sql(&sql)
        .map_err(|e| format!("could not build an INSERT for \"{table}\": {e}"))?
        .into_iter()
        .next()
        .ok_or_else(|| format!("could not build an INSERT for \"{table}\""))
}

/// An identifier as a double-quoted SQL literal identifier, with any embedded
/// quote doubled.
fn quoted(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Build the `INSERT ... VALUES` statements that write `batches` into the target
/// of `template`, in chunks of `rows_per_statement` rows.
///
/// `template` supplies everything but the rows: the target table, the column
/// list (which must already be resolved — its length is what the source rows are
/// matched against, positionally, as PostgreSQL matches them) and any
/// `ON CONFLICT` clause. Its query source is discarded, along with the
/// `ORDER BY`/`LIMIT`/`WITH` that belonged to it — those shaped the rows already
/// materialized in `batches` and must not be re-applied to the VALUES list.
///
/// Returns an empty vector when the source produced no rows: nothing to write is
/// not an error, it is `INSERT 0`.
pub fn insert_statements_from_batches(
    template: &Statement,
    batches: &[RecordBatch],
    rows_per_statement: usize,
) -> std::result::Result<Vec<Statement>, String> {
    let Statement::Insert(insert) = template else {
        return Err("expected an INSERT statement".to_string());
    };
    if insert.columns.is_empty() {
        return Err("INSERT must specify an explicit column list".to_string());
    }
    let arity = insert.columns.len();
    let chunk_rows = rows_per_statement.max(1);

    let mut statements = Vec::new();
    let mut chunk: Vec<Vec<Expr>> = Vec::with_capacity(chunk_rows.min(1024));

    for batch in batches {
        if batch.num_columns() != arity {
            return Err(format!(
                "INSERT has {arity} target column(s) but the source query produces {}",
                batch.num_columns()
            ));
        }
        // One kind per column, resolved once per batch rather than per cell.
        let kinds = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                literal_kind(field.data_type())
                    .map_err(|reason| format!("column \"{}\": {reason}", field.name()))
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;

        for row in 0..batch.num_rows() {
            chunk.push(
                (0..arity)
                    .map(|col| cell_literal(batch.column(col).as_ref(), row, kinds[col]))
                    .collect(),
            );
            if chunk.len() >= chunk_rows {
                statements.push(insert_with_values(insert, std::mem::take(&mut chunk)));
            }
        }
    }

    if !chunk.is_empty() {
        statements.push(insert_with_values(insert, chunk));
    }
    Ok(statements)
}

/// `insert` with its source replaced by a literal `VALUES` list of `rows`.
fn insert_with_values(insert: &Insert, rows: Vec<Vec<Expr>>) -> Statement {
    let mut new_insert = insert.clone();
    new_insert.source = Some(Box::new(values_query(rows)));
    Statement::Insert(new_insert)
}

/// A bare `VALUES (...), (...)` query — no CTE, no ordering, no limit — usable as
/// an `INSERT`'s source.
fn values_query(rows: Vec<Vec<Expr>>) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Values(Values {
            explicit_row: false,
            value_keyword: false,
            // sqlparser carries each row's parenthesis tokens; these rows are
            // synthesized, so they get an empty span and render as plain `(...)`.
            rows: rows.into_iter().map(Parens::with_empty_span).collect(),
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};

    use super::*;
    use crate::pgwire_handler::parser::parse_sql;
    use crate::write_sql_cl::statement_to_sql;

    fn parse_one(sql: &str) -> Statement {
        parse_sql(sql).unwrap().into_iter().next().unwrap()
    }

    fn batch(fields: Vec<Field>, columns: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// Every accepted type must come out as a literal that stores the value the
    /// source row held — the whole point of the rendering.
    #[test]
    fn each_supported_type_renders_as_its_literal() {
        let b = batch(
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("f", DataType::Float64, true),
                Field::new("s", DataType::Utf8, true),
                Field::new("b", DataType::Boolean, true),
                Field::new("d", DataType::Date32, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(StringArray::from(vec!["it's"])),
                Arc::new(BooleanArray::from(vec![true])),
                // 2024-01-02 is 19724 days after the epoch.
                Arc::new(Date32Array::from(vec![19724])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_704_193_200_000_000])),
            ],
        );

        let template = parse_one("INSERT INTO t (i, f, s, b, d, ts) SELECT * FROM u");
        let stmts = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT).unwrap();
        let sql = statement_to_sql(&stmts[0]);

        // Integers bare, floats/strings/temporals quoted, embedded quote doubled,
        // and the timestamp in PostgreSQL's space-separated text form.
        assert!(
            sql.contains("VALUES (7, '1.5', 'it''s', true, '2024-01-02', '2024-01-02 11:00:00')"),
            "got: {sql}"
        );
    }

    #[test]
    fn a_null_cell_renders_as_null() {
        let b = batch(
            vec![Field::new("i", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![None, Some(1)]))],
        );
        let template = parse_one("INSERT INTO t (i) SELECT i FROM u");
        let stmts = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT).unwrap();
        assert!(
            statement_to_sql(&stmts[0]).contains("VALUES (NULL), (1)"),
            "got: {}",
            statement_to_sql(&stmts[0])
        );
    }

    // A type whose text form is not a literal of the same value must be refused
    // by name, not written as something else.
    #[test]
    fn an_unrenderable_type_is_refused_naming_the_column() {
        let b = batch(
            vec![Field::new("payload", DataType::Binary, true)],
            vec![Arc::new(datafusion::arrow::array::BinaryArray::from(vec![
                b"\x00\x01".as_ref(),
            ]))],
        );
        let template = parse_one("INSERT INTO t (payload) SELECT payload FROM u");
        let err = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT)
            .expect_err("Binary has no literal form");
        assert!(err.contains("column \"payload\""), "got: {err}");
        assert!(err.contains("Binary"), "got: {err}");
    }

    // The statement the rows are chunked into is what a shard receives and what
    // the anonymization rewrite walks, so the chunk size has to be honored.
    #[test]
    fn rows_are_chunked_into_several_statements() {
        let b = batch(
            vec![Field::new("i", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from((0..5).collect::<Vec<i32>>()))],
        );
        let template = parse_one("INSERT INTO t (i) SELECT i FROM u");
        let stmts = insert_statements_from_batches(&template, &[b], 2).unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(statement_to_sql(&stmts[0]).contains("VALUES (0), (1)"));
        assert!(statement_to_sql(&stmts[2]).contains("VALUES (4)"));
    }

    // Several batches are one row stream: a chunk may span them, and the tail
    // must not be dropped.
    #[test]
    fn batches_are_concatenated_into_one_row_stream() {
        let field = || Field::new("i", DataType::Int32, true);
        let first = batch(vec![field()], vec![Arc::new(Int32Array::from(vec![1, 2]))]);
        let second = batch(vec![field()], vec![Arc::new(Int32Array::from(vec![3]))]);
        let template = parse_one("INSERT INTO t (i) SELECT i FROM u");
        let stmts = insert_statements_from_batches(&template, &[first, second], ROWS_PER_STATEMENT)
            .unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            statement_to_sql(&stmts[0]).contains("VALUES (1), (2), (3)"),
            "got: {}",
            statement_to_sql(&stmts[0])
        );
    }

    // Nothing to write is `INSERT 0`, not a failure.
    #[test]
    fn an_empty_result_yields_no_statements() {
        let b = batch(
            vec![Field::new("i", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        );
        let template = parse_one("INSERT INTO t (i) SELECT i FROM u WHERE false");
        assert!(
            insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT)
                .unwrap()
                .is_empty()
        );
        assert!(
            insert_statements_from_batches(&template, &[], ROWS_PER_STATEMENT)
                .unwrap()
                .is_empty()
        );
    }

    // A column list shorter or longer than the source row is a client error, and
    // silently writing the columns that do line up would store a wrong row.
    #[test]
    fn a_column_count_mismatch_is_refused() {
        let b = batch(
            vec![
                Field::new("i", DataType::Int32, true),
                Field::new("j", DataType::Int32, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        );
        let template = parse_one("INSERT INTO t (i) SELECT i, j FROM u");
        let err = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT)
            .expect_err("arity mismatch must be refused");
        assert!(err.contains("1 target column(s)"), "got: {err}");
    }

    // The rows were already shaped by the source query on the read path; keeping
    // its `ORDER BY`/`LIMIT`/`WITH` would apply them a second time to the VALUES
    // list, and `LIMIT` would drop rows the client was told were written.
    #[test]
    fn the_source_querys_clauses_are_not_carried_onto_the_values_list() {
        let b = batch(
            vec![Field::new("i", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        );
        let template = parse_one(
            "INSERT INTO t (i) WITH w AS (SELECT 1 AS i) SELECT i FROM w ORDER BY i LIMIT 1",
        );
        let stmts = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT).unwrap();
        let sql = statement_to_sql(&stmts[0]);
        assert!(sql.contains("VALUES (1), (2), (3)"), "got: {sql}");
        for clause in ["LIMIT", "ORDER BY", "WITH"] {
            assert!(!sql.contains(clause), "{clause} survived into: {sql}");
        }
    }

    // The rendered rows have to reach the write path as an INSERT it recognizes:
    // one that reports an exact row count and can be split by shard.
    #[test]
    fn the_synthesized_statement_is_one_the_write_path_can_route() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("v", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        );
        let template = parse_one("INSERT INTO t (id, v) SELECT id, v FROM u");
        let stmts = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT).unwrap();
        let stmt = &stmts[0];

        assert!(super::super::validate_insert_shard_key(stmt, "id", &[]).is_ok());
        assert_eq!(super::super::insert_values_row_count(stmt), Some(2));
        assert_eq!(
            super::super::extract_insert_row_shard_keys(stmt, "id", &[]),
            Some(vec![(0, "1".to_string()), (1, "2".to_string())])
        );
    }

    // An `ON CONFLICT` clause belongs to the INSERT, not to its source, so it
    // must survive — the shard still has to resolve conflicts among the rows it
    // receives.
    #[test]
    fn an_on_conflict_clause_survives() {
        let b = batch(
            vec![Field::new("id", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        let template = parse_one("INSERT INTO t (id) SELECT id FROM u ON CONFLICT (id) DO NOTHING");
        let stmts = insert_statements_from_batches(&template, &[b], ROWS_PER_STATEMENT).unwrap();
        assert!(
            statement_to_sql(&stmts[0]).contains("ON CONFLICT(id) DO NOTHING"),
            "got: {}",
            statement_to_sql(&stmts[0])
        );
    }
}
