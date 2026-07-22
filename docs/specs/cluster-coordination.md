# Cluster Coordination

## Node Discovery and Membership

**Client-to-coordinator discovery**

The client configuration specifies the coordinator address. The client connects to the single coordinator node; it never connects directly to core nodes.

**Coordinator-to-core-node membership**

The coordinator maintains the live set of core nodes via gRPC. Core nodes register themselves with the coordinator on startup and are tracked in the node registry (see [Metadata Catalog](coordinator-node.md#metadata-catalog-database-catalog)). Membership changes (node joins, departures) are handled entirely on the coordinator side; clients are unaware of the core node topology.

## Leader Election

- Leader election applies only to **shard primary assignment** — selecting which core node is the primary for a given shard. Since the architecture uses a single coordinator, the coordinator assigns primaries directly and persists the assignment in the local metadata catalog (see [Metadata Catalog](coordinator-node.md#metadata-catalog-database-catalog)). No distributed consensus protocol is required.
- Coordinator failover (and the consensus protocol it would require) is a future concern (see [Roadmap](roadmap.md)).

## Failure Detection

Failure detection operates at two levels: the **VaireDB cluster** (coordinator and core nodes) and the **Ballista cluster** (scheduler and executors). Ballista failures are the responsibility of the VaireDB node that hosts the failing Ballista component — the coordinator owns the Ballista scheduler, and each core node owns its Ballista executor. Similarly, DuckDB failures are the responsibility of the core node that hosts the embedded instance.

**VaireDB cluster**

| Mechanism | Description |
|-----------|-------------|
| **Heartbeats** | Core nodes periodically stream heartbeats to the coordinator, which acknowledges each one. |
| **Timeout threshold** | Configurable; trade-off between false positives and detection latency. |
| **Suspect / Dead states** | A node becomes suspect either by missing heartbeats or by self-reporting a local failure; suspect nodes are given a grace period before being declared dead, and any node that resumes heartbeating is restored to alive. |

When a core node detects a local failure (its Ballista executor or DuckDB instance), it reports the failure to the coordinator, which marks the node as suspect so it is excluded from new shard-primary assignments. Errors for operations that touched the failed node surface to the client through the normal query/write error path.

**Ballista cluster**

The Ballista scheduler (on the coordinator) monitors executor health through its built-in gRPC connection management. If an executor becomes unreachable, the scheduler marks it as unavailable and stops dispatching query stages to it. The coordinator surfaces executor failures to the client as query errors.

**Recovery scope**

Recovery is limited to a **reconnect strategy** — VaireDB nodes attempt to re-establish lost gRPC connections (coordinator-to-core, scheduler-to-executor). Data-level recovery strategies (e.g. shard rebuild from replicas, automatic failover) are deferred to a future version (see [Roadmap](roadmap.md)).
