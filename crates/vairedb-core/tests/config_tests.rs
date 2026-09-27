//! Loading a core node's configuration from a file: what a complete file yields, which
//! fields it may not leave out, and the one it may.
//!
//! Reading, parsing and refusing are not asserted here. They belong to
//! `vairedb_common::config`, which has its own tests for a file that is missing, one that is
//! not YAML, and one whose values a node cannot run on. What is specific to this crate is the
//! *field set*; the rules and the address helpers are pure functions of the struct and are
//! tested beside them in `config/config.rs`.

use std::io::Write;

use tempfile::NamedTempFile;

use vairedb_core::config::CoreConfig;

/// A complete, valid configuration as `(field, YAML value)` pairs.
///
/// Every test starts from this and states only what it changes, so a field added to
/// `CoreConfig` is written here once rather than in a ten-line literal per test.
fn valid_fields() -> Vec<(&'static str, &'static str)> {
    vec![
        ("log_level", "debug"),
        ("node_id", "\"test-node-1\""),
        ("data_dir", "/tmp/vairedb-test-data"),
        ("grpc_listen_addr", "\"127.0.0.1:60000\""),
        ("advertised_address", "\"10.0.0.5:60000\""),
        ("coordinator_addr", "\"http://10.0.0.1:50040\""),
        ("heartbeat_interval_secs", "10"),
        ("write_queue_capacity", "512"),
        ("ballista_scheduler_addr", "\"http://10.0.0.2:50050\""),
        ("ballista_concurrent_tasks", "8"),
    ]
}

/// The one field a file may leave out, because the node derives it from the listen address.
const OPTIONAL_FIELD: &str = "advertised_address";

/// Write `fields` to a temp file and load it.
///
/// The file is a [`NamedTempFile`]: its name is unique, so two `cargo test` runs at once
/// cannot read each other's file, and it is removed in `Drop` rather than on the last line of
/// a test — a failing assertion panics, so cleaning up at the end leaks the file exactly when
/// the test failed.
fn load(fields: &[(&str, &str)]) -> Result<CoreConfig, String> {
    let mut file = NamedTempFile::new().expect("the temp directory is writable");
    for (field, value) in fields {
        writeln!(file, "{field}: {value}").expect("the temp file is writable");
    }

    CoreConfig::from_file(file.path()).map_err(|e| e.to_string())
}

/// The valid configuration with `field` left out.
fn without(field: &str) -> Vec<(&'static str, &'static str)> {
    valid_fields()
        .into_iter()
        .filter(|(name, _)| *name != field)
        .collect()
}

/// Every value survives the round trip. Without this, nothing here would notice a loader that
/// only ever failed.
#[test]
fn a_complete_file_loads_every_field() {
    let config = load(&valid_fields()).expect("the file is a complete configuration");

    assert_eq!(config.log_level, "debug");
    assert_eq!(config.node_id, "test-node-1");
    assert_eq!(config.data_dir, "/tmp/vairedb-test-data");
    assert_eq!(config.grpc_listen_addr, "127.0.0.1:60000");
    assert_eq!(config.advertised_address.as_deref(), Some("10.0.0.5:60000"));
    assert_eq!(config.coordinator_addr, "http://10.0.0.1:50040");
    assert_eq!(config.heartbeat_interval_secs, 10);
    assert_eq!(config.write_queue_capacity, 512);
    assert_eq!(config.ballista_scheduler_addr, "http://10.0.0.2:50050");
    assert_eq!(config.ballista_concurrent_tasks, 8);
}

/// Which fields a file may leave out, asserted for all of them at once.
///
/// The two halves belong in one test because they are one rule: exactly one field is
/// optional. Asserting the refusals alone would still pass if `advertised_address` became
/// required — breaking every deployment that relies on the fallback — and asserting the
/// fallback alone would still pass if a `#[serde(default)]` crept onto a field that must be
/// written, letting a node start on a value nobody chose.
#[test]
fn only_the_advertised_address_may_be_left_out() {
    for (field, _) in valid_fields() {
        if field == OPTIONAL_FIELD {
            continue;
        }

        let error = load(&without(field)).expect_err("a required field is missing");
        assert!(
            error.contains(field),
            "leaving out {field} must be refused by name; got: {error}"
        );
    }

    let config = load(&without(OPTIONAL_FIELD)).expect("the advertised address is optional");
    assert_eq!(config.advertised_address, None);
    assert_eq!(config.effective_advertised_address(), "127.0.0.1:60000");
}
