# Design Goals

| Goal | Description |
|------|-------------|
| **Horizontal scalability** | Scale read and write throughput by adding core nodes. |
| **SQL compatibility** | Expose a standard SQL interface. All SQL (SELECT, DML, DDL). A compatibility layer on the coordinator translates PostgreSQL-compatible SQL into DuckDB's dialect before forwarding shard-local statements to core nodes. |
| **Analytical performance** | Exploit DuckDB's columnar, vectorized engine for OLAP workloads. |
| **Fault tolerance** | Tolerate **core node** failures without data loss or prolonged downtime. The single coordinator is a known SPOF — coordinator HA is a non-goal (see below). |
| **Operational simplicity** | Minimize external dependencies; self-contained binaries. |
| **Compliance** | Defining a DAG (Directed Acyclic Graph) of connected tables, the feature creates a vectorized representation of data takeout or deletion that needs to be performed. Also, implements a set of anonymization algorithms for hashing specified columns. |
| **Data quality** | The system is able to perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of DuckDB SQL dialect). |

| Non Goal | Description |
|------|-------------|
| **Resharding** | Online resharding when nodes are added or removed — shard splits and merges without full cluster downtime, with data migration via streaming reads from the source DuckDB instance. The sharding strategy is fixed at table creation and data load time. |
| **Multi-coordinator HA** | Multiple stateless coordinator nodes behind a load balancer, backed by a distributed KV store with Raft consensus. |
| **Automatic failover and recovery** | Automatic shard rebuild from replicas on permanent node failure, and automatic traffic redirection to healthy replicas on network partitions. Both scenarios currently require manual intervention or reconnect. |
| **VaireDB snapshot system** | A dedicated snapshot mechanism that does not require pausing writes (`FORCE CHECKPOINT` + file copy). The goal is to support consistent snapshots without draining the write queue — e.g. via copy-on-write semantics, incremental backups, or DuckDB's `EXPORT DATABASE` running concurrently with writes. |
| **Security** | Authentication (client-to-coordinator: username/password, mTLS, JWT; node-to-node: mTLS with auto-rotated certificates), authorization (RBAC at table/schema level, SQL GRANT/REVOKE), and encryption (TLS 1.3 in transit; filesystem-level or DuckDB encryption extension at rest). |
| **Observability** | Prometheus-compatible metrics endpoint per node (query latency, shard size, replication lag, DuckDB stats), structured logging (JSON) with correlation IDs, OpenTelemetry tracing across coordinator and core nodes, `/health` endpoint per node with cluster-level aggregation. |
| **Deployment topologies** | Single-node mode (coordinator + core node in one process for development), multi-coordinator HA (stateless coordinators behind a load balancer), geo-distributed clusters (cross-region replication). The current supported topology is 1 coordinator + N core nodes. |
