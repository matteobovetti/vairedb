# Glossary

| Term | Definition |
|------|------------|
| **Core Node** | A cluster member that stores data shards, executes distributed read queries via a Ballista executor, and applies write operations the coordinator sends to its gRPC write service. |
| **Coordinator Node** | A cluster member that accepts client queries, routes SELECTs through a Ballista scheduler, routes DML to core nodes through a dedicated gRPC write service, and manages the metadata catalog. |
| **Shard** | A horizontal subset of a table's data, stored entirely on one core node (plus replicas). |
| **Fragment / Query Stage** | A portion of a distributed query plan that executes on a single core node's Ballista executor. |
| **Replication Factor** | The number of copies of each shard maintained across the cluster (denoted `N` in quorum formulas). |
| **Exchange Operator** | A plan node inserted by the Ballista scheduler that transfers data between core node executors during distributed query execution via Arrow Flight. |
| **Push-Down** | Optimization technique where DataFusion moves computation (filters, projections, partial aggregations) closer to the data. |
| **Ballista Scheduler** | The Apache Ballista component running on the coordinator that decomposes SELECT queries into query stages and assigns them to executors, which pull work from the scheduler. |
| **Ballista Executor** | The Apache Ballista component running on each core node that executes query stages using DataFusion. |
| **TableProvider** | A custom DataFusion `TableProvider` on the coordinator that expands a logical table into per-shard remote scans; the shard-local DuckDB SQL is executed on the core node by a custom execution-plan node. |
| **DataFusion** | The Apache query engine used by Ballista for SQL parsing, optimization, and physical plan execution. |
| **Arrow Flight** | A gRPC-based protocol for high-performance transfer of Apache Arrow data between Ballista executors (and back to the coordinator) during distributed query execution. |
| **gRPC Write Service** | A dedicated gRPC service hosted on core nodes through which the coordinator sends shard-local DML statements for execution and replication, separate from the Ballista pipeline. |
| **Quorum** | The minimum number of nodes (`floor(N / 2) + 1`, including the primary) that must acknowledge a write before the coordinator responds to the client. |
| **Tail Replication** | The asynchronous background process by which the coordinator retries writes to replicas that did not acknowledge within the synchronous quorum. |
