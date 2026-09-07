#![allow(clippy::module_inception)]

mod write_queue;

pub(crate) use write_queue::QueuedStatement;
pub use write_queue::{WriteQueue, WriteQueueHandle};
