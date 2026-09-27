//! Fixtures shared by the integration-test binaries in this directory.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};

use vairedb_coordinator::catalog::MetadataCatalog;

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// An empty catalog at a path no other catalog in this test run uses.
///
/// Each test binary is its own process, so the pid is what keeps two suites running
/// concurrently off each other's database; the counter only has to separate the
/// catalogs within one suite.
pub fn temp_catalog() -> MetadataCatalog {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = format!("/tmp/vairedb_test_{}_{}.redb", std::process::id(), id);
    MetadataCatalog::open(&path).expect("a fresh temp path opens")
}
