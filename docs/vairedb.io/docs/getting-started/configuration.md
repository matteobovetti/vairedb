# Configuration

Both binaries are configured with a **single YAML file** passed via the
`--config-file <path>` CLI argument. There are **no defaults** — every field is
required.

```bash
vairedb-coordinator --config-file config/coordinator/config.yml
vairedb-core        --config-file config/core/config.yml
```

This page is a quick orientation. For the field-by-field reference, see the
[Configuration Reference](../reference/configuration.md).

## Coordinator configuration

```yaml title="config/coordinator/config.yml"
log_level: info
metadata_dir: data/coordinator
grpc_listen_addr: "0.0.0.0:50040"
pg_listen_addr: "0.0.0.0:5432"
heartbeat_timeout_secs: 15
default_replication_factor: 3
tail_retry_initial_ms: 100
tail_retry_max_ms: 5000
ballista_scheduler_listen_addr: "0.0.0.0:50050"
```

Key points:

- `pg_listen_addr` is where clients connect over the PostgreSQL wire protocol.
- `grpc_listen_addr` is where core nodes register and heartbeat.
- `ballista_scheduler_listen_addr` is where core node executors pull query
  stages from.
- `default_replication_factor` is the cluster-wide replica count, overridable
  per table at creation time.

## Core node configuration

```yaml title="config/core/config.yml"
log_level: info
node_id: "core-1"
data_dir: data/core
grpc_listen_addr: "0.0.0.0:50041"
advertised_address: "core-1:50041"
coordinator_addr: "http://coordinator:50040"
heartbeat_interval_secs: 2
write_queue_capacity: 1024
ballista_scheduler_addr: "http://coordinator:50050"
ballista_concurrent_tasks: 4
```

Key points:

- `node_id` and `advertised_address` must be **unique per core node**.
- `advertised_address` is the address the coordinator and other components use
  to reach this node — set it to a hostname that resolves on your network (a
  Compose service name, a DNS name, or a routable IP).
- `coordinator_addr` and `ballista_scheduler_addr` point back at the
  coordinator's gRPC and Ballista scheduler endpoints.

## Default ports

| Port | Service |
|------|---------|
| `5432` | PostgreSQL wire protocol (client connections) |
| `50040` | Coordinator gRPC (node registration, heartbeats) |
| `50041` | Core node gRPC (write dispatch) |
| `50050` | Ballista scheduler (distributed query execution) |

## Data directories

- The coordinator persists its metadata catalog under `metadata_dir`.
- Each core node stores its DuckDB shard files under `data_dir`.

When running in Docker, mount a host directory or a named volume over these
paths to persist data across container restarts (the
[Quick Start](quickstart.md) shows the commented-out volume mount for core
nodes).
