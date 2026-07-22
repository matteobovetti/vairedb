use std::io::Write;

use tempfile::NamedTempFile;

use vairedb_core::config::CoreConfig;

#[test]
fn test_from_file_full() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"
log_level: debug
node_id: "test-node-1"
data_dir: /tmp/test-data
grpc_listen_addr: "127.0.0.1:60000"
advertised_address: "10.0.0.5:60000"
coordinator_addr: "http://10.0.0.1:50040"
heartbeat_interval_secs: 10
write_queue_capacity: 512
ballista_scheduler_addr: "http://10.0.0.2:50050"
ballista_concurrent_tasks: 8
"#
    )
    .unwrap();

    let config = CoreConfig::from_file(file.path()).unwrap();
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.node_id, "test-node-1");
    assert_eq!(config.data_dir, "/tmp/test-data");
    assert_eq!(config.grpc_listen_addr, "127.0.0.1:60000");
    assert_eq!(config.advertised_address.as_deref(), Some("10.0.0.5:60000"));
    assert_eq!(config.coordinator_addr, "http://10.0.0.1:50040");
    assert_eq!(config.heartbeat_interval_secs, 10);
    assert_eq!(config.write_queue_capacity, 512);
    assert_eq!(config.ballista_scheduler_addr, "http://10.0.0.2:50050");
    assert_eq!(config.ballista_concurrent_tasks, 8);
}

#[test]
fn test_from_file_optional_advertised_address_missing() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"
log_level: info
node_id: "node-no-adv"
data_dir: data/core
grpc_listen_addr: "0.0.0.0:50041"
coordinator_addr: "http://127.0.0.1:50040"
heartbeat_interval_secs: 5
write_queue_capacity: 1024
ballista_scheduler_addr: "http://127.0.0.1:50050"
ballista_concurrent_tasks: 4
"#
    )
    .unwrap();

    let config = CoreConfig::from_file(file.path()).unwrap();
    assert_eq!(config.advertised_address, None);
    assert_eq!(config.effective_advertised_address(), "0.0.0.0:50041");
}

#[test]
fn test_from_file_missing_required_field() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"
log_level: info
node_id: "node-1"
data_dir: data/core
"#
    )
    .unwrap();

    let result = CoreConfig::from_file(file.path());
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("missing field"),
        "expected 'missing field' in error, got: {err_msg}"
    );
}

#[test]
fn test_from_file_not_found() {
    let result = CoreConfig::from_file(std::path::Path::new("/nonexistent/path.yml"));
    assert!(result.is_err());
}

#[test]
fn test_from_file_invalid_yaml() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "{{{{invalid yaml").unwrap();

    let result = CoreConfig::from_file(file.path());
    assert!(result.is_err());
}

#[test]
fn test_effective_advertised_address_with_explicit_value() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"
log_level: info
node_id: "node-adv"
data_dir: data/core
grpc_listen_addr: "0.0.0.0:50041"
advertised_address: "10.0.0.5:50041"
coordinator_addr: "http://127.0.0.1:50040"
heartbeat_interval_secs: 5
write_queue_capacity: 1024
ballista_scheduler_addr: "http://127.0.0.1:50050"
ballista_concurrent_tasks: 4
"#
    )
    .unwrap();

    let config = CoreConfig::from_file(file.path()).unwrap();
    assert_eq!(config.effective_advertised_address(), "10.0.0.5:50041");
}

#[test]
fn test_from_file_addresses_parseable() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"
log_level: info
node_id: "node-parse"
data_dir: data/core
grpc_listen_addr: "0.0.0.0:50041"
coordinator_addr: "http://127.0.0.1:50040"
heartbeat_interval_secs: 5
write_queue_capacity: 1024
ballista_scheduler_addr: "http://127.0.0.1:50050"
ballista_concurrent_tasks: 4
"#
    )
    .unwrap();

    let config = CoreConfig::from_file(file.path()).unwrap();
    let addr: std::net::SocketAddr = config.grpc_listen_addr.parse().unwrap();
    assert_eq!(addr.port(), 50041);
}
