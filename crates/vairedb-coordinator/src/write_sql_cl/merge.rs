//! `MERGE INTO`: the AST-level rules that decide whether a MERGE can be applied
//! shard by shard, and the rewrites that make one renderable for the shards'
//! DuckDB.
//!
//! Everything here is pure — no catalog, no cluster — so the reasoning that keeps
//! a MERGE correct under sharding is testable on its own. The distributed
//! semantics themselves are documented in
//! [`crate::pgwire_handler::merge`], which is where the catalog checks live.

use crate::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, Ident, MergeAction, MergeClauseKind, MergeInsertKind,
    ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableAlias, TableFactor, Values,
};
use datafusion::scalar::ScalarValue;

use crate::pgwire_handler::query_router::{canonical_table_name, canonicalize_ident};

use super::routing_value::{RoutedValue, expr_routing_value};
use super::statement::assignments_target_shard_key;

/// What a MERGE's `USING` clause reads.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeSource {
    /// `USING <table> [AS s]` — another sharded catalog relation.
    Table {
        /// Canonical logical name of the source table.
        name: String,
        /// Name the ON clause and the clause bodies refer to its columns by.
        qualifier: String,
    },
    /// `USING (VALUES …) AS s(a, b)` — an inline row list the coordinator can
    /// split by shard, the way it splits a multi-row INSERT.
    Values {
        /// Name the ON clause and the clause bodies refer to its columns by.
        qualifier: String,
        /// The alias's column names, in order — the positions of the row list.
        columns: Vec<String>,
    },
}

/// The relations a MERGE joins, reduced to the names the rest of the write path
/// needs: the target table, and how each side's columns are qualified.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeShape {
    /// Canonical logical name of the table being merged into.
    pub target_table: String,
    /// Name the ON clause and the clause bodies refer to the target's columns by.
    pub target_qualifier: String,
    /// What `USING` reads.
    pub source: MergeSource,
}

/// Read the relations a MERGE joins, or `Err(message)` for a source shape VaireDB
/// cannot place.
///
/// Only two sources are recognized, because those are the two whose rows the
/// coordinator can prove will meet the right target rows: another sharded table
/// (checked for co-location by the caller) and an inline `VALUES` list with named
/// columns (split per shard by the caller). A subquery source is refused rather
/// than broadcast — its rows are unknown here, so the coordinator cannot tell
/// which shard each one belongs on, and a broadcast `WHEN NOT MATCHED THEN
/// INSERT` would store every row on every shard.
pub fn merge_shape(stmt: &Statement) -> std::result::Result<MergeShape, String> {
    let Statement::Merge(merge) = stmt else {
        return Err("expected a MERGE statement".to_string());
    };

    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &merge.table
    else {
        return Err(
            "MERGE INTO must name a table: a subquery or table function cannot be merged into"
                .to_string(),
        );
    };
    let target_table = canonical_table_name(name)
        .ok_or_else(|| "could not determine the MERGE target table".to_string())?;
    let target_qualifier = alias
        .as_ref()
        .map(|a| canonicalize_ident(&a.name))
        .unwrap_or_else(|| target_table.clone());

    let source = match &merge.source {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let source_name = canonical_table_name(name)
                .ok_or_else(|| "could not determine the MERGE source table".to_string())?;
            let qualifier = alias
                .as_ref()
                .map(|a| canonicalize_ident(&a.name))
                .unwrap_or_else(|| source_name.clone());
            MergeSource::Table {
                name: source_name,
                qualifier,
            }
        }
        TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(alias),
            ..
        } if is_plain_values(subquery) => {
            if alias.columns.is_empty() {
                return Err(
                    "a MERGE whose source is a VALUES list must name its columns, as in \
                     USING (VALUES (1, 'a')) AS s(id, value): the ON clause has to name the \
                     column that decides which shard each row belongs on"
                        .to_string(),
                );
            }
            MergeSource::Values {
                qualifier: canonicalize_ident(&alias.name),
                columns: alias
                    .columns
                    .iter()
                    .map(|c| canonicalize_ident(&c.name))
                    .collect(),
            }
        }
        _ => {
            return Err(
                "MERGE ... USING is supported for another VaireDB table (USING other_table AS s) \
                 or an inline row list (USING (VALUES (1, 'a')) AS s(id, value)) only. A subquery \
                 source is not supported: VaireDB cannot tell which shard each of its rows belongs \
                 on, so the rows a WHEN NOT MATCHED clause inserts could land on every shard. \
                 Materialize the source into a table first"
                    .to_string(),
            );
        }
    };

    Ok(MergeShape {
        target_table,
        target_qualifier,
        source,
    })
}

/// Whether `query` is a bare `VALUES (…), (…)` with nothing that changes which
/// rows it yields. A `LIMIT`, `ORDER BY`, `FETCH`, CTE or pipe operator would
/// decide the row set at execution time, and the coordinator splits the rows by
/// shard before then — so its split would not match what the shards see.
fn is_plain_values(query: &Query) -> bool {
    matches!(query.body.as_ref(), SetExpr::Values(_))
        && query.with.is_none()
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none()
        && query.pipe_operators.is_empty()
}

/// Give the MERGE's target and source relations an explicit alias equal to the
/// name they already carry, when they have none.
///
/// Required before the shard-local rewrite: that rewrite renames the relation
/// itself (`orders` → `orders_shard1`), while a `MERGE INTO orders USING inbox ON
/// orders.id = inbox.id` refers to its columns *through* the old name. Without an
/// alias to keep that name alive the rendered statement would reference a relation
/// that no longer exists. The alias is the client's own identifier, quote style
/// included, so every reference that resolved before still resolves.
pub fn ensure_merge_relation_aliases(stmt: &mut Statement) {
    let Statement::Merge(merge) = stmt else {
        return;
    };
    alias_relation_by_its_own_name(&mut merge.table);
    alias_relation_by_its_own_name(&mut merge.source);
}

fn alias_relation_by_its_own_name(factor: &mut TableFactor) {
    if let TableFactor::Table {
        name,
        alias: alias @ None,
        ..
    } = factor
        && let Some(ident) = name.0.last().and_then(|p| p.as_ident())
    {
        *alias = Some(TableAlias {
            explicit: true,
            name: ident.clone(),
            columns: Vec::new(),
        });
    }
}

/// Drop the target's qualifier from the column names a MERGE clause *writes*,
/// leaving the columns it reads untouched.
///
/// `WHEN MATCHED THEN UPDATE SET t.value = s.value` is how MSSQL, Snowflake and
/// Oracle spell it, and it is unambiguous — the left of an assignment can only be
/// a target column. PostgreSQL and DuckDB both reject the qualifier there, so it
/// is removed rather than passed on. A qualifier naming anything *else* is
/// refused: it would be silently dropped, writing a column of the target the
/// client did not name.
pub fn normalize_merge_column_qualifiers(stmt: &mut Statement) -> std::result::Result<(), String> {
    let shape = merge_shape(stmt)?;
    let target = &shape.target_qualifier;
    let Statement::Merge(merge) = stmt else {
        return Ok(());
    };

    for clause in &mut merge.clauses {
        match &mut clause.action {
            MergeAction::Update(update) => {
                for assignment in &mut update.assignments {
                    match &mut assignment.target {
                        AssignmentTarget::ColumnName(name) => {
                            strip_target_qualifier(name, target, "UPDATE SET")?;
                        }
                        AssignmentTarget::Tuple(names) => {
                            for name in names {
                                strip_target_qualifier(name, target, "UPDATE SET")?;
                            }
                        }
                    }
                }
            }
            MergeAction::Insert(insert) => {
                for name in &mut insert.columns {
                    strip_target_qualifier(name, target, "INSERT")?;
                }
            }
            MergeAction::Delete { .. } => {}
        }
    }
    Ok(())
}

/// Reduce `name` to its final identifier when it is qualified by `target`, or
/// report the qualifier that cannot be honored.
fn strip_target_qualifier(
    name: &mut ObjectName,
    target: &str,
    clause: &str,
) -> std::result::Result<(), String> {
    if name.0.len() < 2 {
        return Ok(());
    }
    let qualifier = name.0[name.0.len() - 2]
        .as_ident()
        .map(canonicalize_ident)
        .unwrap_or_default();
    let Some(column) = name.0.last().and_then(|p| p.as_ident()).cloned() else {
        return Err(format!(
            "could not read a column name in the MERGE {clause} clause"
        ));
    };
    if qualifier != target {
        return Err(format!(
            "the MERGE {clause} clause writes \"{qualifier}\".\"{}\", but a MERGE clause can only \
             write columns of the table being merged into. Name the target's column",
            canonicalize_ident(&column)
        ));
    }
    name.0 = vec![ObjectNamePart::Identifier(column)];
    Ok(())
}

/// The source column the ON clause equates to the target's shard key, or `None`
/// when it equates nothing to it.
///
/// This is the predicate the whole of MERGE's correctness under sharding rests on:
/// equal shard keys always hash to the same shard, so an ON clause that requires
/// `target.<shard key> = source.<column>` can only ever match rows the same shard
/// holds. Both sides must be qualified — with the sides unlabelled there is no
/// telling which relation a bare column belongs to, and guessing wrong would pin
/// the fan-out on the wrong column.
///
/// Only `AND` is walked. An `OR` in the ON clause would let a target row match a
/// source row with a *different* key, which lives on another shard, so a MERGE
/// whose key equality sits under an `OR` finds nothing here and is refused.
pub fn merge_key_column(
    stmt: &Statement,
    shape: &MergeShape,
    target_shard_key: &str,
) -> Option<String> {
    let Statement::Merge(merge) = stmt else {
        return None;
    };
    let source_qualifier = match &shape.source {
        MergeSource::Table { qualifier, .. } | MergeSource::Values { qualifier, .. } => qualifier,
    };
    key_column_in_conjuncts(
        &merge.on,
        &shape.target_qualifier,
        source_qualifier,
        target_shard_key,
    )
}

fn key_column_in_conjuncts(
    expr: &Expr,
    target_qualifier: &str,
    source_qualifier: &str,
    target_shard_key: &str,
) -> Option<String> {
    match unnest(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => key_column_in_conjuncts(left, target_qualifier, source_qualifier, target_shard_key)
            .or_else(|| {
                key_column_in_conjuncts(right, target_qualifier, source_qualifier, target_shard_key)
            }),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let left = qualified_column(left, target_qualifier, source_qualifier)?;
            let right = qualified_column(right, target_qualifier, source_qualifier)?;
            match (left, right) {
                (Side::Target(key), Side::Source(column))
                | (Side::Source(column), Side::Target(key))
                    if key == target_shard_key =>
                {
                    Some(column)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Which side of the join a qualified column reference belongs to.
enum Side {
    Target(String),
    Source(String),
}

/// Read `expr` as a column qualified by the target or the source, canonicalized.
fn qualified_column(expr: &Expr, target_qualifier: &str, source_qualifier: &str) -> Option<Side> {
    let Expr::CompoundIdentifier(parts) = unnest(expr) else {
        return None;
    };
    let [.., qualifier, column] = parts.as_slice() else {
        return None;
    };
    let qualifier = canonicalize_ident(qualifier);
    let column = canonicalize_ident(column);
    if qualifier == target_qualifier {
        Some(Side::Target(column))
    } else if qualifier == source_qualifier {
        Some(Side::Source(column))
    } else {
        None
    }
}

/// Strip redundant parentheses so `ON (t.id = s.id)` reads like `ON t.id = s.id`.
fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

/// Validate every `WHEN` clause of a MERGE against the shard key, returning
/// `Err(message)` naming what to change.
///
/// What is refused, and why:
///
/// - **`OUTPUT` / `RETURNING`.** The rows come back from every shard the MERGE
///   touched; there is no one result set to return them in.
/// - **`UPDATE SET <shard key>`.** It would move the row to a shard the router did
///   not write it to, exactly as a plain `UPDATE` of the shard key would.
/// - **An `INSERT` clause that does not take its shard key from
///   `source_key_column`.** The clause runs on the shard the *source* row is on,
///   so the row it inserts must belong there too — which is guaranteed only when
///   the value it stores in the shard key is the very column the ON clause
///   matched on. Any other value could hash elsewhere, and the row would be
///   stored on a shard no lookup of it ever visits.
/// - **`INSERT ROW`, and Oracle's per-clause `WHERE` / `DELETE WHERE`.** VaireDB
///   would have to render them for an engine that cannot express them, so the
///   statement would fail on the shards with a message about a node rather than
///   about the SQL.
pub fn validate_merge(
    stmt: &Statement,
    target_shard_key: &str,
    source_key_column: &str,
) -> std::result::Result<(), String> {
    let Statement::Merge(merge) = stmt else {
        return Ok(());
    };

    if merge.output.is_some() {
        return Err(
            "OUTPUT/RETURNING is not supported on a MERGE: the rows are merged on several shards \
             and VaireDB cannot return them as one result set. Read the table afterwards"
                .to_string(),
        );
    }

    for clause in &merge.clauses {
        match &clause.action {
            MergeAction::Update(update) => {
                if update.update_predicate.is_some() || update.delete_predicate.is_some() {
                    return Err(
                        "MERGE ... UPDATE SET ... WHERE / DELETE WHERE is not supported by \
                         VaireDB; put the condition on the clause instead, as in \
                         WHEN MATCHED AND <condition> THEN UPDATE SET ..."
                            .to_string(),
                    );
                }
                if assignments_target_shard_key(&update.assignments, target_shard_key) {
                    return Err(format!(
                        "a MERGE clause cannot assign shard key column \"{target_shard_key}\"; \
                         relocating a row to a different shard is not supported in v0.1"
                    ));
                }
            }
            MergeAction::Insert(insert) => {
                if insert.insert_predicate.is_some() {
                    return Err(
                        "MERGE ... INSERT ... WHERE is not supported by VaireDB; put the condition \
                         on the clause instead, as in WHEN NOT MATCHED AND <condition> THEN \
                         INSERT ..."
                            .to_string(),
                    );
                }
                let MergeInsertKind::Values(values) = &insert.kind else {
                    return Err(
                        "MERGE ... INSERT ROW is not supported by VaireDB; name the columns and \
                         their values, as in INSERT (id, value) VALUES (s.id, s.value)"
                            .to_string(),
                    );
                };
                let [row] = values.rows.as_slice() else {
                    return Err(
                        "a MERGE INSERT clause takes exactly one VALUES row: it inserts the source \
                         row it is matching"
                            .to_string(),
                    );
                };
                if insert.columns.len() != row.len() {
                    return Err(format!(
                        "the MERGE INSERT clause names {} column(s) but supplies {} value(s)",
                        insert.columns.len(),
                        row.len()
                    ));
                }
                let Some(key_idx) = insert
                    .columns
                    .iter()
                    .position(|name| object_name_is(name, target_shard_key))
                else {
                    return Err(format!(
                        "the MERGE INSERT clause must supply shard key column \
                         \"{target_shard_key}\": without it VaireDB cannot tell which shard the \
                         new row belongs on"
                    ));
                };
                if !expr_reads_column(&row[key_idx], source_key_column) {
                    return Err(format!(
                        "the MERGE INSERT clause must set shard key column \
                         \"{target_shard_key}\" to the source column the ON clause matches on \
                         (\"{source_key_column}\"): the clause runs on the shard that source row \
                         lives on, so any other value could hash to a different shard and the row \
                         would be stored where no lookup of it looks"
                    ));
                }
            }
            MergeAction::Delete { .. } => {}
        }
    }

    Ok(())
}

/// Whether an `ObjectName`'s final identifier is the canonical `column`.
fn object_name_is(name: &ObjectName, column: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| canonicalize_ident(ident) == column)
}

/// Whether `expr` is a plain reference to `column`, qualified or not.
fn expr_reads_column(expr: &Expr, column: &str) -> bool {
    match unnest(expr) {
        Expr::Identifier(ident) => canonicalize_ident(ident) == column,
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| canonicalize_ident(ident) == column),
        _ => false,
    }
}

/// True when any `WHEN` clause of the MERGE is `NOT MATCHED BY SOURCE`, whose
/// action applies to target rows the source does *not* contain.
///
/// Load-bearing for a `VALUES` source: those rows are split per shard, so a shard
/// with no source rows receives no statement at all — and its target rows, which
/// are exactly the ones `NOT MATCHED BY SOURCE` is about, would go untouched. The
/// caller refuses the combination instead of applying it to part of the table.
pub fn merge_has_not_matched_by_source(stmt: &Statement) -> bool {
    let Statement::Merge(merge) = stmt else {
        return false;
    };
    merge
        .clauses
        .iter()
        .any(|clause| clause.clause_kind == MergeClauseKind::NotMatchedBySource)
}

/// Fill in the implicit column list of a MERGE `INSERT` clause from
/// `table_columns`, the target's columns in declaration order.
///
/// The counterpart of [`super::materialize_insert_columns`], and needed for the
/// same reason: every check after this point — the shard-key rule, the value that
/// decides where the new row lands — locates a column by name, and finds nothing
/// in a clause that names none.
pub fn materialize_merge_insert_columns(
    stmt: &mut Statement,
    table_columns: &[&str],
) -> std::result::Result<(), String> {
    let Statement::Merge(merge) = stmt else {
        return Ok(());
    };
    if table_columns.is_empty() {
        return Ok(());
    }

    for clause in &mut merge.clauses {
        let MergeAction::Insert(insert) = &mut clause.action else {
            continue;
        };
        if !insert.columns.is_empty() {
            continue;
        }
        let MergeInsertKind::Values(values) = &insert.kind else {
            continue;
        };
        let [row] = values.rows.as_slice() else {
            continue;
        };
        if row.is_empty() {
            continue;
        }
        if row.len() > table_columns.len() {
            return Err(format!(
                "the MERGE INSERT clause supplies {} value(s) for a table with {} column(s)",
                row.len(),
                table_columns.len()
            ));
        }
        insert.columns = table_columns[..row.len()]
            .iter()
            .map(|name| {
                ObjectName(vec![ObjectNamePart::Identifier(Ident::with_quote(
                    '"', *name,
                ))])
            })
            .collect();
    }
    Ok(())
}

/// The `VALUES` rows of a MERGE whose source is an inline row list.
fn merge_values(stmt: &Statement) -> Option<&Values> {
    let Statement::Merge(merge) = stmt else {
        return None;
    };
    let TableFactor::Derived { subquery, .. } = &merge.source else {
        return None;
    };
    match subquery.body.as_ref() {
        SetExpr::Values(values) => Some(values),
        _ => None,
    }
}

/// Extract the `(row_index, routing_value)` pair for every row of a MERGE's
/// `VALUES` source, reading the key from position `key_index`. Returns `None`
/// unless *every* row resolves to a routable value.
///
/// All-or-nothing for the same reason [`super::extract_insert_row_shard_keys`] is:
/// a row missing from this list is a row missing from the shard split, so the
/// MERGE would silently skip it while reporting success.
pub fn merge_row_shard_keys(
    stmt: &Statement,
    key_index: usize,
    params: &[ScalarValue],
) -> Option<Vec<(usize, String)>> {
    let values = merge_values(stmt)?;
    let mut keys = Vec::with_capacity(values.rows.len());
    for (row_idx, row) in values.rows.iter().enumerate() {
        let RoutedValue::Value(value) = expr_routing_value(row.get(key_index)?, params) else {
            return None;
        };
        keys.push((row_idx, value));
    }
    Some(keys)
}

/// Build a MERGE carrying only the `VALUES` source rows at `row_indices`, so each
/// shard receives only the rows it owns. Returns `None` if the statement has no
/// inline row list or no row is selected.
pub fn split_merge_by_rows(stmt: &Statement, row_indices: &[usize]) -> Option<Statement> {
    let Statement::Merge(merge) = stmt else {
        return None;
    };
    let values = merge_values(stmt)?;

    let selected: Vec<Vec<Expr>> = row_indices
        .iter()
        .filter_map(|&idx| values.rows.get(idx).cloned())
        .collect();
    if selected.is_empty() {
        return None;
    }

    let mut split = merge.clone();
    let TableFactor::Derived { subquery, .. } = &mut split.source else {
        return None;
    };
    let new_values = Values {
        rows: selected,
        ..values.clone()
    };
    // Only the rows change; everything else about the source query stays as the
    // client wrote it.
    *subquery.body = SetExpr::Values(new_values);

    Some(Statement::Merge(split))
}

#[cfg(test)]
mod tests {
    use super::super::{rewrite_to_shard_local, statement_to_sql, transform_to_duckdb};
    use super::*;
    use crate::pgwire_handler::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
        assert_eq!(stmts.len(), 1, "`{sql}` must parse to one statement");
        stmts.remove(0)
    }

    /// The canonical target/source shape of a MERGE, panicking if it is refused.
    fn shape(sql: &str) -> MergeShape {
        merge_shape(&parse_one(sql)).unwrap_or_else(|e| panic!("`{sql}` must be accepted: {e}"))
    }

    /// The source column a MERGE's ON clause matches the shard key `key` against.
    fn key_column(sql: &str, key: &str) -> Option<String> {
        let stmt = parse_one(sql);
        let shape = merge_shape(&stmt).expect("shape");
        merge_key_column(&stmt, &shape, key)
    }

    const TABLE_SOURCE: &str = "MERGE INTO orders t USING inbox s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)";

    #[test]
    fn a_table_source_reports_both_qualifiers() {
        assert_eq!(
            shape(TABLE_SOURCE),
            MergeShape {
                target_table: "orders".to_string(),
                target_qualifier: "t".to_string(),
                source: MergeSource::Table {
                    name: "inbox".to_string(),
                    qualifier: "s".to_string(),
                },
            }
        );
    }

    // Without an alias the relation's own name is the qualifier — which is what
    // makes `ON orders.id = inbox.id` resolvable.
    #[test]
    fn unaliased_relations_are_qualified_by_their_own_names() {
        let shape =
            shape("MERGE INTO Orders USING inbox ON Orders.id = inbox.id WHEN MATCHED THEN DELETE");
        assert_eq!(shape.target_qualifier, "orders");
        assert_eq!(
            shape.source,
            MergeSource::Table {
                name: "inbox".to_string(),
                qualifier: "inbox".to_string(),
            }
        );
    }

    #[test]
    fn a_values_source_reports_its_column_positions() {
        let shape = shape(
            "MERGE INTO orders t USING (VALUES (1, 10), (2, 20)) AS s(id, amount) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount",
        );
        assert_eq!(
            shape.source,
            MergeSource::Values {
                qualifier: "s".to_string(),
                columns: vec!["id".to_string(), "amount".to_string()],
            }
        );
    }

    // A source whose rows the coordinator cannot see cannot be placed: a
    // `WHEN NOT MATCHED THEN INSERT` over it would have to be broadcast, storing
    // every row on every shard.
    #[test]
    fn a_source_that_cannot_be_placed_is_refused() {
        for sql in [
            "MERGE INTO orders t USING (SELECT id, amount FROM inbox) s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
            // A VALUES list with no column names: the ON clause cannot name the
            // column that decides the shard.
            "MERGE INTO orders t USING (VALUES (1, 10)) AS s ON t.id = s.column1 \
             WHEN MATCHED THEN DELETE",
            // LIMIT decides the row set on the shard, after the split.
            "MERGE INTO orders t USING (VALUES (1, 10), (2, 20) LIMIT 1) AS s(id, amount) \
             ON t.id = s.id WHEN MATCHED THEN DELETE",
        ] {
            assert!(
                merge_shape(&parse_one(sql)).is_err(),
                "`{sql}` must be refused"
            );
        }
    }

    // --- the ON clause: the one predicate the whole design rests on ---

    #[test]
    fn the_key_column_is_read_from_either_operand_order() {
        assert_eq!(key_column(TABLE_SOURCE, "id").as_deref(), Some("id"));
        assert_eq!(
            key_column(
                "MERGE INTO orders t USING inbox s ON s.ref = t.id WHEN MATCHED THEN DELETE",
                "id"
            )
            .as_deref(),
            Some("ref")
        );
    }

    // The equality may sit among other conjuncts, which only narrow the match and
    // are evaluated by the shard against its own rows.
    #[test]
    fn the_key_column_is_found_among_conjuncts() {
        for sql in [
            "MERGE INTO orders t USING inbox s ON t.region = s.region AND t.id = s.id \
             WHEN MATCHED THEN DELETE",
            "MERGE INTO orders t USING inbox s ON (t.id = s.id) AND t.region = s.region \
             WHEN MATCHED THEN DELETE",
        ] {
            assert_eq!(
                key_column(sql, "id").as_deref(),
                Some("id"),
                "`{sql}` pins the shard key"
            );
        }
    }

    // An `OR` lets a target row match a source row with a different key, which
    // lives on another shard — so no key column is reported and the caller refuses.
    #[test]
    fn an_or_in_the_on_clause_pins_nothing() {
        assert_eq!(
            key_column(
                "MERGE INTO orders t USING inbox s ON t.id = s.id OR t.ref = s.ref \
                 WHEN MATCHED THEN DELETE",
                "id"
            ),
            None
        );
    }

    // With a bare column on either side there is no telling which relation it
    // belongs to, and pinning the fan-out on the wrong one would be silent.
    #[test]
    fn an_unqualified_on_clause_pins_nothing() {
        for sql in [
            "MERGE INTO orders t USING inbox s ON id = s.id WHEN MATCHED THEN DELETE",
            "MERGE INTO orders t USING inbox s ON t.id = id WHEN MATCHED THEN DELETE",
        ] {
            assert_eq!(key_column(sql, "id"), None, "`{sql}` must pin nothing");
        }
    }

    // An ON clause that equates some *other* column pins nothing: the target rows
    // it matches could live on any shard.
    #[test]
    fn an_on_clause_not_naming_the_shard_key_pins_nothing() {
        assert_eq!(key_column(TABLE_SOURCE, "customer_id"), None);
    }

    // Identifiers fold like PostgreSQL's, so an unquoted `T.ID` still names the
    // canonical `t`.`id`.
    #[test]
    fn the_on_clause_folds_identifiers() {
        assert_eq!(
            key_column(
                "MERGE INTO orders T USING inbox S ON T.ID = S.Ref WHEN MATCHED THEN DELETE",
                "id"
            )
            .as_deref(),
            Some("ref")
        );
    }

    // --- clause validation ---

    /// The message a refused MERGE reports, or a panic if it is accepted.
    fn rejection(sql: &str, key: &str, source_key: &str) -> String {
        validate_merge(&parse_one(sql), key, source_key)
            .expect_err(&format!("`{sql}` must be refused"))
    }

    #[test]
    fn the_supported_shape_is_accepted() {
        assert!(validate_merge(&parse_one(TABLE_SOURCE), "id", "id").is_ok());
    }

    // Every clause kind and a per-clause condition are all evaluated by the shard
    // against its own rows, so none of them needs refusing.
    #[test]
    fn every_clause_kind_and_condition_is_accepted() {
        let sql = "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED AND s.amount > 0 THEN UPDATE SET amount = s.amount \
             WHEN MATCHED THEN DELETE \
             WHEN NOT MATCHED BY TARGET THEN INSERT (id, amount) VALUES (s.id, s.amount) \
             WHEN NOT MATCHED BY SOURCE THEN DELETE";
        assert!(validate_merge(&parse_one(sql), "id", "id").is_ok());
        assert!(merge_has_not_matched_by_source(&parse_one(sql)));
        assert!(!merge_has_not_matched_by_source(&parse_one(TABLE_SOURCE)));
    }

    // Assigning the shard key relocates the row to a shard the router did not
    // write it to — the same defect as a plain `UPDATE` of it.
    #[test]
    fn a_clause_assigning_the_shard_key_is_refused() {
        let message = rejection(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET id = s.ref",
            "id",
            "id",
        );
        assert!(message.contains("shard key"), "got: {message}");
    }

    // The INSERT clause runs on the shard the source row is on, so the value it
    // stores in the shard key has to be the column the ON clause matched — any
    // other value could hash elsewhere and the row would be unreachable.
    #[test]
    fn an_insert_clause_that_could_place_a_row_wrongly_is_refused() {
        for sql in [
            // Omits the shard key: nothing to place the row by.
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (amount) VALUES (s.amount)",
            // Stores a different column in it.
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.ref, s.amount)",
            // Stores a computed value in it.
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id + 1, s.amount)",
            // Stores a literal in it: correct only for the one shard that literal
            // hashes to, and this clause runs on every shard.
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (7, s.amount)",
        ] {
            let message = rejection(sql, "id", "id");
            assert!(
                message.contains("shard key"),
                "`{sql}` should be refused over the shard key, got: {message}"
            );
        }
    }

    #[test]
    fn returning_is_refused() {
        let message = rejection(
            "MERGE INTO orders t USING inbox s ON t.id = s.id WHEN MATCHED THEN DELETE \
             RETURNING t.id",
            "id",
            "id",
        );
        assert!(message.contains("RETURNING"), "got: {message}");
    }

    // Oracle's per-clause predicates and BigQuery's `INSERT ROW` would have to be
    // rendered for an engine that cannot express them, so the refusal happens here
    // rather than as a node error.
    #[test]
    fn clause_forms_the_shards_cannot_run_are_refused() {
        for sql in [
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount WHERE s.amount > 0",
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount DELETE WHERE s.amount = 0",
        ] {
            assert!(
                validate_merge(&parse_one(sql), "id", "id").is_err(),
                "`{sql}` must be refused"
            );
        }
    }

    // --- the writable-column qualifier ---

    // `SET t.value = ...` is how MSSQL, Snowflake and Oracle spell it and is
    // unambiguous, so it is accepted by removing the qualifier PostgreSQL and
    // DuckDB both reject.
    #[test]
    fn a_target_qualified_write_target_is_normalized_away() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.amount = s.amount \
             WHEN NOT MATCHED THEN INSERT (t.id, t.amount) VALUES (s.id, s.amount)",
        );
        normalize_merge_column_qualifiers(&mut stmt).expect("must be normalized");
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("SET amount = s.amount"), "got: {sql}");
        assert!(sql.contains("INSERT (id, amount)"), "got: {sql}");
        // Reads keep their qualifier: only the written column is the target's.
        assert!(sql.contains("VALUES (s.id, s.amount)"), "got: {sql}");
        // And the shard-key rules still see the columns by name afterwards.
        assert!(validate_merge(&stmt, "id", "id").is_ok());
    }

    // A qualifier naming anything but the target would be silently dropped,
    // writing a column of the target the client did not name.
    #[test]
    fn a_foreign_qualified_write_target_is_refused() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET s.amount = 1",
        );
        let message = normalize_merge_column_qualifiers(&mut stmt).expect_err("must be refused");
        assert!(message.contains("merged into"), "got: {message}");
    }

    // --- the implicit INSERT column list ---

    #[test]
    fn an_insert_clause_naming_no_columns_takes_the_tables_leading_ones() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.amount)",
        );
        materialize_merge_insert_columns(&mut stmt, &["id", "amount", "region"]).unwrap();
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("INSERT (\"id\", \"amount\")"), "got: {sql}");
        // Which is what lets the shard-key rule apply to it at all.
        assert!(validate_merge(&stmt, "id", "id").is_ok());
    }

    // A row wider than the table has no positional mapping, and the shard-key rule
    // would otherwise report something less useful.
    #[test]
    fn an_insert_clause_wider_than_the_table_is_refused() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.amount, s.region)",
        );
        assert!(materialize_merge_insert_columns(&mut stmt, &["id", "amount"]).is_err());
    }

    // --- splitting a VALUES source by shard ---

    const VALUES_SOURCE: &str = "MERGE INTO orders t USING (VALUES (1, 10), (2, 20), (3, 30)) AS s(id, amount) \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)";

    #[test]
    fn the_rows_of_a_values_source_are_keyed_by_position() {
        let keys = merge_row_shard_keys(&parse_one(VALUES_SOURCE), 0, &[]).expect("all routable");
        assert_eq!(
            keys,
            vec![
                (0, "1".to_string()),
                (1, "2".to_string()),
                (2, "3".to_string())
            ]
        );
    }

    // A row missing from the key list is a row missing from the split, so one
    // unroutable row gives up the whole list rather than dropping it.
    #[test]
    fn row_keys_are_all_or_nothing() {
        let sql = "MERGE INTO orders t USING (VALUES (1, 10), (2 + 2, 20)) AS s(id, amount) \
             ON t.id = s.id WHEN MATCHED THEN DELETE";
        assert!(merge_row_shard_keys(&parse_one(sql), 0, &[]).is_none());
    }

    #[test]
    fn a_split_merge_carries_only_the_selected_rows() {
        let split = split_merge_by_rows(&parse_one(VALUES_SOURCE), &[0, 2]).expect("must split");
        let sql = statement_to_sql(&split);
        assert!(
            sql.contains("(1, 10)") && sql.contains("(3, 30)"),
            "got: {sql}"
        );
        assert!(!sql.contains("(2, 20)"), "got: {sql}");
        // Everything but the rows survives the split.
        assert!(sql.contains("AS s (id, amount)"), "got: {sql}");
        assert!(sql.contains("WHEN NOT MATCHED"), "got: {sql}");
    }

    #[test]
    fn splitting_a_table_source_is_not_possible() {
        assert!(split_merge_by_rows(&parse_one(TABLE_SOURCE), &[0]).is_none());
        assert!(split_merge_by_rows(&parse_one(VALUES_SOURCE), &[]).is_none());
    }

    // --- the shard-local render ---

    // Both relations move, and the aliases are what keep the column references
    // resolvable once they have: `orders` is gone, `t` is not.
    #[test]
    fn the_shard_local_render_moves_both_relations_and_keeps_the_references() {
        let mut stmt = parse_one(
            "MERGE INTO orders USING inbox ON orders.id = inbox.id WHEN MATCHED THEN DELETE",
        );
        ensure_merge_relation_aliases(&mut stmt);
        rewrite_to_shard_local(&mut stmt, "shard2");
        transform_to_duckdb(&mut stmt);

        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("orders_shard2 AS orders"), "got: {sql}");
        assert!(sql.contains("inbox_shard2 AS inbox"), "got: {sql}");
        assert!(sql.contains("ON orders.id = inbox.id"), "got: {sql}");
        // DuckDB's grammar requires INTO, PostgreSQL's makes it optional.
        assert!(sql.starts_with("MERGE INTO"), "got: {sql}");
    }

    // An existing alias is left alone, so the client's own name keeps working.
    #[test]
    fn an_explicit_alias_survives_the_render() {
        let mut stmt = parse_one(TABLE_SOURCE);
        ensure_merge_relation_aliases(&mut stmt);
        rewrite_to_shard_local(&mut stmt, "shard0");
        transform_to_duckdb(&mut stmt);

        // Rendered as the client spelled it — without `AS`, since they omitted it.
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("orders_shard0 t "), "got: {sql}");
        assert!(sql.contains("inbox_shard0 s "), "got: {sql}");
        assert!(
            !sql.contains(" t t"),
            "the alias must not be doubled: {sql}"
        );
    }

    // `WHEN NOT MATCHED` already means `BY TARGET`; spelling it out removes the
    // reliance on the shards' engine defaulting the same way.
    #[test]
    fn a_bare_not_matched_clause_is_spelled_out_for_duckdb() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING inbox s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
        );
        transform_to_duckdb(&mut stmt);
        assert!(
            statement_to_sql(&stmt).contains("WHEN NOT MATCHED BY TARGET"),
            "got: {}",
            statement_to_sql(&stmt)
        );
    }

    // Placeholders inside a MERGE must be reachable by the renumbering the write
    // path does per shard, or a split statement would bind the wrong parameters.
    #[test]
    fn placeholders_survive_the_shard_local_render() {
        let mut stmt = parse_one(
            "MERGE INTO orders t USING (VALUES ($1, $2)) AS s(id, amount) ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET amount = s.amount",
        );
        rewrite_to_shard_local(&mut stmt, "shard1");
        let order = super::super::renumber_placeholders(&mut stmt).expect("renumbered");
        assert_eq!(order, vec![0, 1]);
        let sql = statement_to_sql(&stmt);
        assert!(sql.contains("$1") && sql.contains("$2"), "got: {sql}");
    }
}
