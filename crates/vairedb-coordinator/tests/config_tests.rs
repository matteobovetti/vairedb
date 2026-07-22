use std::io::Write;
use std::path::Path;

use vairedb_coordinator::config::CoordinatorConfig;

#[test]
fn test_from_file_full() {
    let yaml = r#"
log_level: debug
metadata_dir: /tmp/test_meta
grpc_listen_addr: "127.0.0.1:9000"
pg_listen_addr: "127.0.0.1:9001"
heartbeat_timeout_secs: 30
default_replication_factor: 5
tail_retry_initial_ms: 200
tail_retry_max_ms: 10000
ballista_scheduler_listen_addr: "127.0.0.1:50050"
"#;
    let path = "/tmp/vairedb_test_config_full.yml";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(yaml.as_bytes()).unwrap();

    let config = CoordinatorConfig::from_file(Path::new(path)).unwrap();
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.metadata_dir, "/tmp/test_meta");
    assert_eq!(config.grpc_listen_addr, "127.0.0.1:9000");
    assert_eq!(config.pg_listen_addr, "127.0.0.1:9001");
    assert_eq!(config.heartbeat_timeout_secs, 30);
    assert_eq!(config.default_replication_factor, 5);
    assert_eq!(config.tail_retry_initial_ms, 200);
    assert_eq!(config.tail_retry_max_ms, 10000);
    assert_eq!(config.ballista_scheduler_listen_addr, "127.0.0.1:50050");

    std::fs::remove_file(path).ok();
}

#[test]
fn test_from_file_missing_field_errors() {
    let yaml = r#"
log_level: info
metadata_dir: data/coordinator
grpc_listen_addr: "0.0.0.0:50040"
pg_listen_addr: "0.0.0.0:5432"
heartbeat_timeout_secs: 15
default_replication_factor: 3
tail_retry_initial_ms: 100
"#;
    let path = "/tmp/vairedb_test_config_missing_field.yml";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(yaml.as_bytes()).unwrap();

    let result = CoordinatorConfig::from_file(Path::new(path));
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("missing field"),
        "expected 'missing field' in error, got: {err_msg}"
    );

    std::fs::remove_file(path).ok();
}

#[test]
fn test_from_file_empty_errors() {
    let yaml = "{}";
    let path = "/tmp/vairedb_test_config_empty.yml";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(yaml.as_bytes()).unwrap();

    let result = CoordinatorConfig::from_file(Path::new(path));
    assert!(result.is_err());

    std::fs::remove_file(path).ok();
}

#[test]
fn test_from_file_not_found() {
    let result = CoordinatorConfig::from_file(Path::new("/tmp/nonexistent_vairedb.yml"));
    assert!(result.is_err());
}

#[test]
fn test_from_file_invalid_yaml() {
    let invalid_yaml = "{{{{not: valid: yaml: [[[";
    let path = "/tmp/vairedb_test_config_invalid.yml";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(invalid_yaml.as_bytes()).unwrap();

    let result = CoordinatorConfig::from_file(Path::new(path));
    assert!(result.is_err());

    std::fs::remove_file(path).ok();
}

#[test]
fn test_from_file_addresses_parseable() {
    let yaml = r#"
log_level: info
metadata_dir: data/coordinator
grpc_listen_addr: "0.0.0.0:50040"
pg_listen_addr: "0.0.0.0:5432"
heartbeat_timeout_secs: 15
default_replication_factor: 3
tail_retry_initial_ms: 100
tail_retry_max_ms: 5000
ballista_scheduler_listen_addr: "0.0.0.0:50050"
"#;
    let path = "/tmp/vairedb_test_config_parseable.yml";
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(yaml.as_bytes()).unwrap();

    let config = CoordinatorConfig::from_file(Path::new(path)).unwrap();

    let grpc_addr: std::net::SocketAddr = config.grpc_listen_addr.parse().unwrap();
    assert_eq!(grpc_addr.port(), 50040);

    let pg_addr: std::net::SocketAddr = config.pg_listen_addr.parse().unwrap();
    assert_eq!(pg_addr.port(), 5432);

    let ballista_addr: std::net::SocketAddr =
        config.ballista_scheduler_listen_addr.parse().unwrap();
    assert_eq!(ballista_addr.port(), 50050);

    std::fs::remove_file(path).ok();
}
