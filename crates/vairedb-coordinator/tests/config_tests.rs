//! Loading the coordinator's configuration from a file: what a complete file yields, and
//! which fields it may not leave out.
//!
//! Reading, parsing and refusing are not asserted here. They belong to
//! `vairedb_common::config`, which has its own tests for a file that is missing, one that is
//! not YAML, and one whose values a node cannot run on; repeating them per crate only
//! duplicates the coverage and leaves two places to update. What is specific to this crate
//! is the *field set* — that a complete file yields every value, and that none of the fields
//! has quietly acquired a default — and the rules, which are a pure function of the struct
//! and are tested beside it in `config/config.rs`.

use std::io::Write;

use tempfile::NamedTempFile;

use vairedb_coordinator::config::CoordinatorConfig;

/// A complete, valid configuration as `(field, YAML value)` pairs.
///
/// Every test starts from this and states only what it changes, so a field added to
/// `CoordinatorConfig` is written here once rather than in a ten-line literal per test — the
/// shape this file had, where a new field meant editing four near-identical blocks and the
/// one that was missed failed as "missing field" in an unrelated test.
fn valid_fields() -> Vec<(&'static str, &'static str)> {
    vec![
        ("log_level", "debug"),
        ("metadata_dir", "/tmp/vairedb-test-meta"),
        ("grpc_listen_addr", "\"127.0.0.1:9000\""),
        ("pg_listen_addr", "\"127.0.0.1:9001\""),
        ("heartbeat_timeout_secs", "30"),
        ("default_replication_factor", "5"),
        ("allow_cross_shard_transactions", "true"),
        ("tail_retry_initial_ms", "200"),
        ("tail_retry_max_ms", "10000"),
        ("ballista_scheduler_listen_addr", "\"127.0.0.1:50050\""),
    ]
}

/// Write `fields` to a temp file and load it.
///
/// The file is a [`NamedTempFile`]: its name is unique, so two `cargo test` runs at once
/// cannot read each other's file, and it is removed in `Drop` rather than on the last line of
/// a test — a failing assertion panics, so cleaning up at the end leaks the file exactly when
/// the test failed, and the next run then reads the previous run's leftovers.
fn load(fields: &[(&str, &str)]) -> Result<CoordinatorConfig, String> {
    let mut file = NamedTempFile::new().expect("the temp directory is writable");
    for (field, value) in fields {
        writeln!(file, "{field}: {value}").expect("the temp file is writable");
    }

    CoordinatorConfig::from_file(file.path()).map_err(|e| e.to_string())
}

/// The valid configuration with `field` left out.
fn without(field: &str) -> Vec<(&'static str, &'static str)> {
    valid_fields()
        .into_iter()
        .filter(|(name, _)| *name != field)
        .collect()
}

/// Every value survives the round trip. Without this, nothing here would notice a loader that
/// only ever failed: the other test is about an error, which a never-succeeding `from_file`
/// satisfies on its own.
#[test]
fn a_complete_file_loads_every_field() {
    let config = load(&valid_fields()).expect("the file is a complete configuration");

    assert_eq!(config.log_level, "debug");
    assert_eq!(config.metadata_dir, "/tmp/vairedb-test-meta");
    assert_eq!(config.grpc_listen_addr, "127.0.0.1:9000");
    assert_eq!(config.pg_listen_addr, "127.0.0.1:9001");
    assert_eq!(config.heartbeat_timeout_secs, 30);
    assert_eq!(config.default_replication_factor, 5);
    assert!(config.allow_cross_shard_transactions);
    assert_eq!(config.tail_retry_initial_ms, 200);
    assert_eq!(config.tail_retry_max_ms, 10000);
    assert_eq!(config.ballista_scheduler_listen_addr, "127.0.0.1:50050");
}

/// "All fields are required — there are no defaults" is the documented contract, and it is
/// asserted for every field rather than for one of them.
///
/// A `#[serde(default)]` added to any single field would make a coordinator start on a value
/// nobody wrote, which is the kind of thing that is only noticed in production; asserting one
/// field would not catch it in the other nine.
#[test]
fn no_field_has_a_default_and_leaving_any_one_out_is_refused() {
    for (field, _) in valid_fields() {
        let error = load(&without(field)).expect_err("a field is missing");

        assert!(
            error.contains(field),
            "leaving out {field} must be refused by name; got: {error}"
        );
    }
}
