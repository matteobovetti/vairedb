# Getting Started

This section gets you from zero to a running VaireDB cluster you can query with
any PostgreSQL client.

<div class="grid cards" markdown>

-   [:material-download: __Installation__](installation.md)

    Build the binaries from source or build the Docker images.

-   [:material-docker: __Quick Start (Docker Compose)__](quickstart.md)

    The fastest path: a coordinator plus five core nodes, then your first SQL.

-   [:material-cog: __Configuration__](configuration.md)

    The YAML settings for coordinator and core nodes.

</div>

## Prerequisites

- **Docker** and **Docker Compose** — for the quick start.
- **Rust** 1.95 or later (2024 edition) — only if building from source.
- **Protobuf compiler** (`protoc`) — only if building from source.
- A **PostgreSQL client** such as `psql` or `pgcli` to connect to the cluster.

## The shape of a cluster

A VaireDB deployment is **one coordinator node plus N core nodes**:

- The **coordinator** is the only node clients connect to. It speaks the
  PostgreSQL wire protocol on port `5432`.
- **Core nodes** hold the data shards and never receive client connections
  directly.

Continue to the [Quick Start](quickstart.md) to bring one up.
