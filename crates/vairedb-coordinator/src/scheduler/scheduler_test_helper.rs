//! What the scheduler's tests share: the join fixture the cluster measurements were taken
//! on, a way to read a plan, and a way to read a column of keys.
//!
//! These hold no rule. Each module asserts its own, and this is only the scaffolding those
//! assertions are written on — the fixture in particular, because three modules reason
//! about the same four left keys and three right keys, and a copy that drifted would make
//! two tests that look comparable stop being so.
//!
//! Deliberately not here: each module's `contexts()`. They register different tables under
//! different configs, and the one thing they have in common — appending a rule to the
//! default physical optimizer set — is a line, not an abstraction.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::{ExecutionPlan, displayable};

/// `l.k = 10, 20, 30, NULL` — the build side of the cluster measurements' fixture. The
/// NULL is the whole point of it: it is what a null-aware join has to treat as unknown
/// rather than as merely unequal.
pub const L_KEYS: [Option<i32>; 4] = [Some(10), Some(20), Some(30), None];
/// `r.k = 20, 99, 50` — the probe side. One key matches, one is above every left key, one
/// is between them.
pub const R_KEYS: [Option<i32>; 3] = [Some(20), Some(99), Some(50)];

/// The fixture's schema: a single nullable `k`.
pub fn key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]))
}

/// A plan as its indented text, the form an assertion on plan *shape* compares.
pub fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(false).to_string()
}

/// Every value of the batches' first column, sorted, with a NULL rendered as `-1`.
///
/// Sorted because the tests read rows across partitions, where the order is whatever the
/// partitions finished in; `-1` because a NULL dropped from the vector would make a missing
/// row and a null row assert the same.
pub fn sorted_keys(batches: &[RecordBatch]) -> Vec<i32> {
    let mut keys = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("an int4 column");
        for row in 0..batch.num_rows() {
            keys.push(if column.is_null(row) {
                -1
            } else {
                column.value(row)
            });
        }
    }
    keys.sort_unstable();
    keys
}
