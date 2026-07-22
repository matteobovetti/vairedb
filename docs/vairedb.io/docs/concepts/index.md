# Concepts

This section explains how VaireDB works — what each node does, how data is
distributed and replicated, how queries run, and what guarantees you can rely
on.

<div class="grid cards" markdown>

-   [:material-target: __Design Goals__](design-goals.md)

    What VaireDB optimizes for, and what is explicitly out of scope for v0.1.

-   [:material-sitemap: __System Architecture__](system-architecture.md)

    The coordinator/core topology and node responsibilities.

-   [:material-account-supervisor: __Coordinator Node__](coordinator-node.md)

    Query routing, distributed planning, and the metadata catalog.

-   [:material-database: __Core Node__](core-node.md)

    The embedded DuckDB engine, storage, and query execution.

-   [:material-call-split: __Data Distribution__](data-distribution.md)

    Hash sharding and quorum-based replication.

-   [:material-pipe: __Query Processing__](query-processing.md)

    The read, write, and DDL paths, end to end.

-   [:material-lan: __Cluster Coordination__](cluster-coordination.md)

    Node discovery, primary assignment, and failure detection.

-   [:material-transit-connection-variant: __Communication Layer__](communication-layer.md)

    Wire protocols between clients, the coordinator, and core nodes.

-   [:material-check-decagram: __Transactions & Consistency__](transactions-consistency.md)

    The per-shard consistency model and its limitations.

-   [:material-shield-refresh: __Fault Tolerance__](fault-tolerance.md)

    WAL, quorum, recovery, and planned snapshotting.

</div>
