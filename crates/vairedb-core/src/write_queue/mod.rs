#![allow(clippy::module_inception)]

mod bulk_insert;
mod write_queue;

pub(crate) use bulk_insert::BulkInsert;
#[cfg(test)]
pub(crate) use bulk_insert::test_support;
pub(crate) use write_queue::QueuedStatement;
pub use write_queue::{WriteQueue, WriteQueueHandle};
