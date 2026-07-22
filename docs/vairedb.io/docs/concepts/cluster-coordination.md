# Cluster Coordination

The coordinator owns cluster membership and failure detection. Clients are
unaware of the core node topology — they only ever talk to the coordinator.

## Node discovery and membership

**Client-to-coordinator discovery.** The client configuration specifies the
coordinator address. The client connects to the single coordinator node; it
never connects directly to core nodes.

**Coordinator-to-core-node membership.** The coordinator maintains the live set
of core nodes via gRPC. Core nodes register themselves on startup and are
tracked in the node registry. Membership changes (joins, departures) are handled
entirely on the coordinator side.

## Leader election

- Leader election applies only to **shard primary assignment** — selecting which
  core node is the primary for a given shard. Since the architecture uses a
  single coordinator, the coordinator assigns primaries directly and persists
  the assignment in its local metadata catalog. No distributed consensus
  protocol is required.
- Coordinator failover (and the consensus protocol it would require) is a future
  concern (see the [Roadmap](../roadmap.md)).

## Failure detection

Failure detection operates at two levels: the **VaireDB cluster** (coordinator
and core nodes) and the **Ballista cluster** (scheduler and executors). Ballista
failures are owned by the VaireDB node hosting the failing component — the
coordinator owns the scheduler, each core node owns its executor. DuckDB
failures are owned by the core node hosting the instance.

### VaireDB cluster

| Mechanism | Description |
|-----------|-------------|
| **Heartbeats** | Core nodes periodically stream heartbeats to the coordinator, which acknowledges each one. |
| **Timeout threshold** | Configurable; trades off false positives against detection latency. |
| **Suspect / Dead states** | A node becomes suspect by missing heartbeats or self-reporting a local failure. Suspect nodes get a grace period before being declared dead; any node that resumes heartbeating is restored to alive. |

When a core node detects a local failure (its Ballista executor or DuckDB
instance), it reports the failure to the coordinator, which marks the node
suspect so it is excluded from new shard-primary assignments. Errors for
operations that touched the failed node surface to the client through the normal
query/write error path.

### Ballista cluster

The Ballista scheduler monitors executor health through its built-in gRPC
connection management. If an executor becomes unreachable, the scheduler marks
it unavailable and stops dispatching query stages to it. Executor failures
surface to the client as query errors.

### Recovery scope

Recovery is limited to a **reconnect strategy** — VaireDB nodes attempt to
re-establish lost gRPC connections (coordinator-to-core,
scheduler-to-executor). Data-level recovery (shard rebuild from replicas,
automatic failover) is deferred to a future version (see the
[Roadmap](../roadmap.md)).
