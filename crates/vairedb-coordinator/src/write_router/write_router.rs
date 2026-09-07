//! Routing of write statements to shards: choosing target shards by shard key,
//! rewriting SQL to its shard-local form, and computing quorum/replica targets.

use std::sync::Arc;

use crate::sqlparser::ast::Statement;
use datafusion::scalar::ScalarValue;

use crate::catalog::{MetadataCatalog, ShardMeta, TableMeta};
use crate::error::{CoordinatorError, Result};
use crate::write_sql_cl;
use vairedb_common::proto::vairedb::v1::{WriteParam, write_param};

/// Resolves where a write goes and how it is expressed against the storage
/// nodes, using shard metadata from the catalog.
pub struct WriteRouter {
    catalog: Arc<MetadataCatalog>,
}

impl WriteRouter {
    /// Create a router backed by the shared metadata catalog.
    pub fn new(catalog: Arc<MetadataCatalog>) -> Self {
        Self { catalog }
    }

    /// Determine which shard(s) a write must target.
    ///
    /// Hashes the shard-key value to a single shard when the statement pins the
    /// key, or returns all shards for a broadcast write. Returns `Err` if the
    /// table has no shards (`ShardNotAssigned`), the routed key is NULL
    /// (`NullShardKey`), or the key is pinned to something not reducible to a
    /// value (`UnroutableShardKey`).
    pub fn resolve_target_shards(
        &self,
        stmt: &Statement,
        table_meta: &TableMeta,
        params: &[ScalarValue],
    ) -> Result<Vec<ShardMeta>> {
        let shards = self.catalog.get_shards_for_table(&table_meta.table_name)?;
        if shards.is_empty() {
            return Err(CoordinatorError::ShardNotAssigned(format!(
                "no shards for table {}",
                table_meta.table_name
            )));
        }

        let key_column = &table_meta.shard_key;

        match write_sql_cl::route_target(stmt, key_column, params) {
            write_sql_cl::ShardRouting::One(value) => {
                let bucket = compute_shard_index(&value, shards.len());
                Ok(vec![
                    shard_for_bucket(&shards, bucket, &table_meta.table_name)?.clone(),
                ])
            }
            write_sql_cl::ShardRouting::Null => Err(CoordinatorError::NullShardKey(format!(
                "shard key \"{key_column}\" cannot be NULL"
            ))),
            write_sql_cl::ShardRouting::Unroutable(reason) => {
                Err(CoordinatorError::UnroutableShardKey(format!(
                    "cannot route on shard key \"{key_column}\": {reason}"
                )))
            }
            write_sql_cl::ShardRouting::Broadcast => Ok(shards),
        }
    }

    /// Build the shard-local SQL for `stmt`, returning the rewritten SQL together
    /// with the dense, positionally-ordered bind parameters that statement
    /// references. Placeholders are renumbered to a contiguous `$1..$k` sequence
    /// so DuckDB can bind them positionally; `params` is the full decoded
    /// parameter list for the original statement.
    pub fn generate_shard_local_sql(
        &self,
        stmt: &Statement,
        shard: &ShardMeta,
        params: &[ScalarValue],
    ) -> Result<(String, Vec<WriteParam>)> {
        let mut stmt_clone = stmt.clone();
        let shard_suffix = format!("shard{}", shard.hash_bucket);
        write_sql_cl::rewrite_to_shard_local(&mut stmt_clone, &shard_suffix);
        write_sql_cl::transform_to_duckdb(&mut stmt_clone);

        let write_params = if params.is_empty() {
            Vec::new()
        } else {
            match write_sql_cl::renumber_placeholders(&mut stmt_clone) {
                Some(order) => order
                    .into_iter()
                    .map(|idx| scalar_to_write_param(params.get(idx)))
                    .collect(),
                None => {
                    return Err(CoordinatorError::Internal(format!(
                        "failed to renumber bind placeholders for shard-local SQL on shard {} \
                         (table {}): malformed placeholder in statement",
                        shard.hash_bucket, shard.table_name
                    )));
                }
            }
        };

        Ok((write_sql_cl::statement_to_sql(&stmt_clone), write_params))
    }

    /// Number of acknowledgements needed for a majority quorum given the
    /// replication factor (`floor(rf/2) + 1`).
    pub fn compute_quorum_size(&self, replication_factor: u32) -> usize {
        (replication_factor as usize / 2) + 1
    }

    /// List the node IDs that hold `shard`, with the primary first followed by
    /// its replicas.
    pub fn get_target_nodes(&self, shard: &ShardMeta) -> Vec<String> {
        let mut nodes = vec![shard.primary_node_id.clone()];
        nodes.extend(shard.replica_node_ids.clone());
        nodes
    }
}

/// Map a shard-key value to a hash bucket in `0..shard_count` via xxh3 hashing.
/// This hash function is the routing contract; it must match wherever shard
/// placement is decided. The bucket it returns is a *bucket number*, not an index
/// into a shard list — use [`shard_for_bucket`] to turn it into a shard.
pub fn compute_shard_index(value: &str, shard_count: usize) -> usize {
    let hash = xxhash_rust::xxh3::xxh3_64(value.as_bytes());
    (hash as usize) % shard_count
}

/// The shard of `table_name` that owns `bucket`, matched on the `hash_bucket`
/// each record carries rather than on its position in `shards`.
///
/// Position is not the bucket. Shard records are stored under the string key
/// `"{table}:shard{n}"`, so a scan returns them in lexicographic order:
/// `shard10` before `shard2`. Up to ten shards the two orders coincide and
/// positional indexing happens to be right; from eleven on it is not, and it
/// fails silently — every shard can execute the statement, so the row is simply
/// written to, or looked for on, a shard that does not own its key.
/// [`MetadataCatalog::get_shards_for_table`] now orders by bucket, but routing
/// must not depend on that: the ordering is a convenience for callers that read
/// the list in order, this is the correctness guarantee.
///
/// A bucket with no shard record is an error, not something to fall back from: the
/// layout is incomplete and any other shard would be the wrong one.
///
/// [`MetadataCatalog::get_shards_for_table`]: crate::catalog::MetadataCatalog::get_shards_for_table
pub fn shard_for_bucket<'a>(
    shards: &'a [ShardMeta],
    bucket: usize,
    table_name: &str,
) -> Result<&'a ShardMeta> {
    shards
        .iter()
        .find(|shard| shard.hash_bucket as usize == bucket)
        .ok_or_else(|| {
            CoordinatorError::ShardNotAssigned(format!(
                "table {table_name} has no shard for hash bucket {bucket}: its shard layout is \
                 incomplete ({} of the buckets a key can hash to are assigned)",
                shards.len()
            ))
        })
}

/// Convert a decoded bind parameter into a typed `WriteParam` for transport to
/// the storage node. NULLs (and any value not given) map to the `is_null`
/// variant. Types DuckDB does not have a dedicated bind value for are carried as
/// their string form and cast on bind.
fn scalar_to_write_param(scalar: Option<&ScalarValue>) -> WriteParam {
    let value = match scalar {
        None => write_param::Value::IsNull(true),
        Some(s) if s.is_null() => write_param::Value::IsNull(true),
        Some(s) => match s {
            ScalarValue::Boolean(Some(b)) => write_param::Value::BoolVal(*b),
            ScalarValue::Int8(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::Int16(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::Int32(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::Int64(Some(v)) => write_param::Value::IntVal(*v),
            ScalarValue::UInt8(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::UInt16(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::UInt32(Some(v)) => write_param::Value::IntVal(*v as i64),
            ScalarValue::Float32(Some(v)) => write_param::Value::DoubleVal(*v as f64),
            ScalarValue::Float64(Some(v)) => write_param::Value::DoubleVal(*v),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => write_param::Value::StringVal(v.clone()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::BinaryView(Some(v)) => write_param::Value::BytesVal(v.clone()),
            other => write_param::Value::StringVal(other.to_string()),
        },
    };
    WriteParam { value: Some(value) }
}
