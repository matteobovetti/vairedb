# Configuration Reference

Both binaries take a single `--config-file <path>` argument pointing at a YAML
file. **All fields are required — there are no defaults.**

## Coordinator

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

| Field | Description |
|-------|-------------|
| `log_level` | Logging verbosity (e.g. `info`, `debug`). |
| `metadata_dir` | Directory where the coordinator persists its metadata catalog. |
| `grpc_listen_addr` | Address for the coordinator gRPC server (core node registration, heartbeats). |
| `pg_listen_addr` | Address for the PostgreSQL wire protocol listener — clients connect here. |
| `heartbeat_timeout_secs` | How long a core node may miss heartbeats before being declared dead. |
| `default_replication_factor` | Cluster-wide replica count (`N`) for shards, overridable per table at creation. |
| `tail_retry_initial_ms` | Initial backoff before retrying a write to a lagging replica (tail replication). |
| `tail_retry_max_ms` | Maximum backoff for tail-replication retries. |
| `ballista_scheduler_listen_addr` | Address of the embedded Ballista scheduler; core node executors connect here. |

## Core node

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

| Field | Description |
|-------|-------------|
| `log_level` | Logging verbosity. |
| `node_id` | Unique identifier for this core node. Must be unique across the cluster. |
| `data_dir` | Directory where this node stores its DuckDB shard files. |
| `grpc_listen_addr` | Address for this node's gRPC write service (receives DML from the coordinator). |
| `advertised_address` | Address other components use to reach this node. Must be reachable on the network and unique per node. |
| `coordinator_addr` | URL of the coordinator's gRPC endpoint (for registration and heartbeats). |
| `heartbeat_interval_secs` | How often this node sends heartbeats to the coordinator. |
| `write_queue_capacity` | Capacity of the per-node write queue that serializes writes to DuckDB. |
| `ballista_scheduler_addr` | URL of the coordinator's Ballista scheduler, which this node's executor connects to. |
| `ballista_concurrent_tasks` | Maximum number of query stages this node's executor runs concurrently. |

## Default ports

| Port | Service |
|------|---------|
| `5432` | PostgreSQL wire protocol (client connections) |
| `50040` | Coordinator gRPC (node registration, heartbeats) |
| `50041` | Core node gRPC (write dispatch) |
| `50050` | Ballista scheduler (distributed query execution) |
