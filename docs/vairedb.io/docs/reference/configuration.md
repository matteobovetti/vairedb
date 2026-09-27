# Configuration Reference

Both binaries take a single `--config-file <path>` argument pointing at a YAML
file. **All fields are required — there are no defaults**, with one exception: a core node
derives `advertised_address` from its `grpc_listen_addr` when the field is absent.

A node that reads its file and finds a value it cannot work with **refuses to start and names
the field**, rather than starting and misbehaving later. See [Refused values](#refused-values).

## Coordinator

```yaml title="config/coordinator/config.yml"
log_level: info
metadata_dir: data/coordinator
grpc_listen_addr: "0.0.0.0:50040"
pg_listen_addr: "0.0.0.0:5432"
heartbeat_timeout_secs: 15
default_replication_factor: 3
allow_cross_shard_transactions: false
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
| `heartbeat_timeout_secs` | How long a core node may miss heartbeats before being declared dead. A node silent for a third of this is first marked *suspect* and stops receiving new shards; the scan runs on that same third, at least once a second. Below three seconds the two thresholds coincide and a node goes straight from alive to dead. |
| `default_replication_factor` | Cluster-wide replica count (`N`) for shards, overridable per table at creation. |
| `allow_cross_shard_transactions` | Whether a `BEGIN` … `COMMIT` block whose writes span several shard groups may commit. `false` (recommended) refuses such a `COMMIT` and writes nothing; `true` applies the groups one at a time, so a failure part-way through leaves the earlier groups applied. See [Transactions & Consistency](../concepts/transactions-consistency.md). |
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
| `advertised_address` | Address other components use to reach this node. Must be reachable on the network and unique per node. Optional: defaults to `grpc_listen_addr`, which is only right when that address is itself reachable from the other nodes. |
| `coordinator_addr` | URL of the coordinator's gRPC endpoint (for registration and heartbeats). |
| `heartbeat_interval_secs` | How often this node sends heartbeats to the coordinator. |
| `write_queue_capacity` | Capacity of the per-node write queue that serializes writes to DuckDB. |
| `ballista_scheduler_addr` | URL of the coordinator's Ballista scheduler, which this node's executor connects to. |
| `ballista_concurrent_tasks` | Maximum number of query stages this node's executor runs concurrently. |

## Refused values

Each of these values is accepted by YAML and leaves the node unable to do its job, in a way
that is hard to recognise as a configuration problem once it happens — two of them are a panic
in a background task, and the rest are silent. The node refuses the file at startup instead,
naming the field.

| Field | Must be | What the refused value would do |
|-------|---------|--------------------------------|
| `heartbeat_timeout_secs` | `>= 1` | `0` marks every core node dead on the first scan. Only *alive* nodes can hold a shard, so `CREATE TABLE` then fails for want of nodes on a cluster where every node is heartbeating. |
| `default_replication_factor` | `>= 1` | A shard with no copies cannot be stored; `0` quietly behaves as `1`. |
| `tail_retry_initial_ms` | `>= 1`, and `<= tail_retry_max_ms` | `0` makes every retry wait no time at all, so an unreachable replica is retried in a tight loop. A value above the maximum is clamped to it, so the number written is never used. |
| `heartbeat_interval_secs` | `>= 1` | A zero interval panics the node's heartbeat task. The node keeps running but goes silent, and the coordinator declares it dead. |
| `write_queue_capacity` | `>= 1` | The write queue is a bounded channel, which cannot hold zero entries; the node fails to start. |
| `ballista_concurrent_tasks` | `>= 1` | The executor registers advertising no task slots, so the scheduler never gives it a share of a read. Nothing reports an error. |

## Default ports

| Port | Service |
|------|---------|
| `5432` | PostgreSQL wire protocol (client connections) |
| `50040` | Coordinator gRPC (node registration, heartbeats) |
| `50041` | Core node gRPC (write dispatch) |
| `50050` | Ballista scheduler (distributed query execution) |
