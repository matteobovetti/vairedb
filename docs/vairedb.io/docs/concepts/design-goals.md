# Design Goals

VaireDB targets analytical (OLAP) workloads on a horizontally scalable cluster,
with operational simplicity as a guiding principle. The table below captures
what v0.1 optimizes for.

## Goals

| Goal | Description |
|------|-------------|
| **Horizontal scalability** | Scale read and write throughput by adding core nodes. |
| **SQL compatibility** | Expose a standard SQL interface for all SQL (SELECT, DML, DDL). A compatibility layer on the coordinator translates PostgreSQL-compatible SQL into DuckDB's dialect before forwarding shard-local statements to core nodes. |
| **Analytical performance** | Exploit DuckDB's columnar, vectorized engine for OLAP workloads. |
| **Fault tolerance** | Tolerate **core node** failures without data loss or prolonged downtime. The single coordinator is a known SPOF — coordinator HA is a non-goal (see below). |
| **Operational simplicity** | Minimize external dependencies; self-contained binaries. |
| **Compliance** *(planned)* | Defining a DAG of connected tables, the feature creates a vectorized representation of data takeout or deletion to be performed. Also implements a set of anonymization algorithms for hashing specified columns. |
| **Data quality** *(planned)* | Perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of the DuckDB SQL dialect). |

## Non-goals (for v0.1)

These are deliberately out of scope for the current version. Several are tracked
on the [Roadmap](../roadmap.md).

| Non-goal | Description |
|----------|-------------|
| **Resharding** | Online resharding when nodes are added or removed — shard splits and merges without full cluster downtime. The sharding strategy is fixed at table creation and data load time. |
| **Multi-coordinator HA** | Multiple stateless coordinator nodes behind a load balancer, backed by a distributed KV store with Raft consensus. |
| **Automatic failover and recovery** | Automatic shard rebuild from replicas on permanent node failure, and automatic traffic redirection to healthy replicas on network partitions. Both currently require manual intervention or reconnect. |
| **VaireDB snapshot system** | A dedicated snapshot mechanism that does not require pausing writes. The goal is consistent snapshots without draining the write queue. |
| **Security** | Authentication (username/password, mTLS, JWT), authorization (RBAC, SQL `GRANT`/`REVOKE`), and encryption (TLS 1.3 in transit; at-rest encryption). |
| **Observability** | Prometheus-compatible metrics, structured JSON logging with correlation IDs, OpenTelemetry tracing, and `/health` endpoints with cluster-level aggregation. |
| **Deployment topologies** | Single-node mode, multi-coordinator HA, and geo-distributed clusters. The current supported topology is **1 coordinator + N core nodes**. |

!!! warning "Single coordinator is a SPOF"
    Only a single coordinator node is supported. It is a single point of failure
    for both reads and writes. Coordinator high availability is explicitly a
    non-goal for v0.1.
