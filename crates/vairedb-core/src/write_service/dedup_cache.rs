use std::collections::{HashMap, VecDeque};

use vairedb_common::proto::vairedb::v1::WriteResult;

/// Bounded, insertion-ordered cache of write results keyed by `write_id`, used to
/// make `execute_write` idempotent. Once at capacity, the oldest entry is evicted.
pub(crate) struct DedupCache {
    results: HashMap<String, Vec<WriteResult>>,
    order: VecDeque<String>,
    capacity: usize,
}

impl DedupCache {
    /// Create an empty cache that retains at most `capacity` entries.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            results: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Return the cached results for `write_id`, if the batch has run before.
    pub(crate) fn get(&self, write_id: &str) -> Option<&Vec<WriteResult>> {
        self.results.get(write_id)
    }

    /// Record the `results` for `write_id`, evicting the oldest entry if the
    /// cache is at capacity. A `write_id` already present is left unchanged.
    pub(crate) fn insert(&mut self, write_id: String, results: Vec<WriteResult>) {
        if self.results.contains_key(&write_id) {
            return;
        }
        if self.order.len() >= self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.results.remove(&evicted);
        }
        self.order.push_back(write_id.clone());
        self.results.insert(write_id, results);
    }
}
