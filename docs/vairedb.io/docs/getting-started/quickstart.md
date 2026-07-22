# Quick Start (Docker Compose)

This guide brings up a small VaireDB cluster — **one coordinator and five core
nodes** — using Docker Compose, then walks you through your first table and
queries. It mirrors the cluster used by the project's end-to-end tests
(`tests/e2e/docker-compose.yml`).

!!! tip "Prerequisites"
    You need **Docker** and **Docker Compose**, plus a PostgreSQL client such as
    `psql`. First build the images with `make docker` (see
    [Installation](installation.md)).

## 1. Build the images

The Compose file references the locally built `vairedb-coordinator` and
`vairedb-core` images, so build them first:

```bash
make docker
```

## 2. Create the Compose file

Create a working directory and add the following `docker-compose.yml`. It
defines one coordinator and five core nodes on a shared bridge network. The
coordinator exposes the PostgreSQL port (`5432`), the coordinator gRPC port
(`50040`), and the Ballista scheduler port (`50050`). Core nodes only talk to
the coordinator over the internal network.

```yaml title="docker-compose.yml"
services:
  coordinator:
    image: vairedb-coordinator
    container_name: vairedb-coordinator
    ports:
      - "5432:5432"     # PostgreSQL wire protocol (clients connect here)
      - "50040:50040"   # coordinator gRPC (node registration, heartbeats)
      - "50050:50050"   # Ballista scheduler (distributed query execution)
    volumes:
      - ./config/coordinator.yml:/app/config/coordinator/config.yml:ro
    healthcheck:
      test:
        ["CMD-SHELL", "timeout 2 bash -c '</dev/tcp/127.0.0.1/5432' || exit 1"]
      interval: 2s
      timeout: 3s
      retries: 15
      start_period: 5s
    networks:
      - vairedb

  core-1:
    image: vairedb-core
    container_name: vairedb-core-1
    depends_on:
      coordinator:
        condition: service_healthy
    volumes:
      - ./config/core-1.yml:/app/config/core/config.yml:ro
      # - ./data/core/1/:/app/data/core   # uncomment to persist this shard's data
    networks:
      - vairedb

  core-2:
    image: vairedb-core
    container_name: vairedb-core-2
    depends_on:
      coordinator:
        condition: service_healthy
    volumes:
      - ./config/core-2.yml:/app/config/core/config.yml:ro
    networks:
      - vairedb

  core-3:
    image: vairedb-core
    container_name: vairedb-core-3
    depends_on:
      coordinator:
        condition: service_healthy
    volumes:
      - ./config/core-3.yml:/app/config/core/config.yml:ro
    networks:
      - vairedb

  core-4:
    image: vairedb-core
    container_name: vairedb-core-4
    depends_on:
      coordinator:
        condition: service_healthy
    volumes:
      - ./config/core-4.yml:/app/config/core/config.yml:ro
    networks:
      - vairedb

  core-5:
    image: vairedb-core
    container_name: vairedb-core-5
    depends_on:
      coordinator:
        condition: service_healthy
    volumes:
      - ./config/core-5.yml:/app/config/core/config.yml:ro
    networks:
      - vairedb

networks:
  vairedb:
    driver: bridge
```

!!! note "Why core nodes wait for the coordinator"
    Core nodes register themselves with the coordinator on startup. The
    `depends_on … condition: service_healthy` clause delays each core node until
    the coordinator's PostgreSQL port answers, so registration does not race
    against a starting coordinator.

## 3. Add the config files

Create a `config/` directory next to the Compose file with one coordinator
config and one config per core node.

=== "config/coordinator.yml"

    ```yaml
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

=== "config/core-1.yml"

    ```yaml
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

=== "config/core-2.yml … core-5.yml"

    Each remaining core node is identical except for `node_id` and
    `advertised_address`, which must be unique. For `core-2`:

    ```yaml
    log_level: info
    node_id: "core-2"
    data_dir: data/core
    grpc_listen_addr: "0.0.0.0:50041"
    advertised_address: "core-2:50041"
    coordinator_addr: "http://coordinator:50040"
    heartbeat_interval_secs: 2
    write_queue_capacity: 1024
    ballista_scheduler_addr: "http://coordinator:50050"
    ballista_concurrent_tasks: 4
    ```

    Repeat for `core-3`, `core-4`, and `core-5`, changing `node_id` and the
    hostname in `advertised_address` to match the service name.

!!! info "Networking inside Compose"
    Each core node's `advertised_address` and `coordinator_addr` use the Compose
    **service names** (`core-1`, `coordinator`, …) as hostnames. Docker's
    embedded DNS resolves them on the shared `vairedb` network. The
    `default_replication_factor: 3` means each shard is kept on three nodes — so
    a five-node cluster comfortably tolerates a quorum write.

See the [Configuration Reference](../reference/configuration.md) for every field.

## 4. Start the cluster

```bash
docker compose up -d --wait
```

`--wait` blocks until the coordinator is healthy and the core nodes have
started. Check the status and logs with:

```bash
docker compose ps
docker compose logs -f coordinator
```

You should see the core nodes registering with the coordinator in the logs.

## 5. Connect and run your first queries

Connect with any PostgreSQL client:

```bash
psql -h 127.0.0.1 -p 5432 "sslmode=disable"
```

Create a sharded table, insert rows, and query it:

```sql
CREATE TABLE foo_table (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP
) WITH (
    shards = 3,
    replication_factor = 3,
    shard_by = 'HASH(id)'
);

INSERT INTO foo_table (id, name, email, created_at)
VALUES (1, 'Alice', 'alice@example.com', '2026-01-15 10:30:00');

INSERT INTO foo_table (id, name, email, created_at)
VALUES (2, 'Bob', 'bob@example.com', '2026-02-20 14:00:00');

SELECT * FROM foo_table;

SELECT id, name FROM foo_table WHERE id = 1;
```

Inspect the cluster catalog to see how the table was distributed:

```sql
SELECT * FROM vairedb_catalog.tables;
```

For the full SQL surface — DDL, DML, `ALTER TABLE`, and catalog introspection —
see the [SQL Guide](../sql/index.md).

## 6. Tear it down

```bash
docker compose down -v --remove-orphans
```

The `-v` flag removes the named volumes; drop it if you mounted host
directories and want to keep shard data between runs.

## What just happened

- The **coordinator** accepted your PostgreSQL connection, parsed each
  statement, and consulted its metadata catalog.
- `CREATE TABLE … WITH (shards = 3, replication_factor = 3, …)` registered the
  table and assigned its three shards (plus replicas) across the five core
  nodes.
- Each `INSERT` was routed to the owning shard's **primary and replicas**, and
  acknowledged once a [quorum](../concepts/fault-tolerance.md#quorum-and-availability)
  confirmed it.
- The `SELECT` was planned by the embedded **Ballista scheduler**, which
  dispatched shard-local scans to the core nodes' DuckDB instances and merged
  the results.

Read [Query Processing](../concepts/query-processing.md) to follow these paths
end to end.
