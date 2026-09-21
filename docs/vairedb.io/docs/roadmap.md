# Roadmap

VaireDB is early-stage software.

The tables below reflect the current planned work. Status labels are the project's own.

## Roadmap to v0.3

| Status | Description |
|--------|-------------|
| PLANNED | Fuzzy testing crate with massive testing suite for all the db features. |

## Roadmap to v0.4

| Status | Description |
|--------|-------------|
| TODO | Security: TLS, users, groups. |

## Roadmap to v0.5

| Status | Description |
|--------|-------------|
| TODO/SPEC READY | Feature **Compliance - Data Deletion (DDR) - Delete or anonymize all user data** |
| VALIDATE | Feature **Compliance - Data Takeout (SAR) - Subject Access Request: Provide all user data** |

## Roadmap to v0.6

| Status | Description |
|--------|-------------|
| TODO | Feature **Data quality** - The system is able to perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of DuckDB SQL dialect). |

## Roadmap to v0.7

| Status | Description |
|--------|-------------|
| TODO | Feature **Rich data catalog** - [Include](https://opendatacontract.com/) |

## Roadmap to v1.0 (production readiness)

| Status | Description |
|--------|-------------|
| TODO | Coordinator WAL. |
| TODO | Coordinator HA. |
| TODO | External salt-key for pseudonymization managed in a KMS. |
| TODO | Performance tests (distributed). |
| TODO | Microbenchmark core pieces of the code base. |

## Bank of ideas

| Status | Description |
|--------|-------------|
| VALIDATE | Metadata/Catalog API. |
| VALIDATE | Mutation batch with trigger interval and max number of command executed. |
| VALIDATE | Coordinator WAL. |

## Known limitations

These are tracked as [non-goals](concepts/design-goals.md#non-goals) for the
current version:

- **Single coordinator** — a single point of failure for reads and writes.
- **No online resharding** — shard count and key are fixed at table creation.
- **No multi-shard transactions** — atomicity stops at a shard's replica set. A
  `BEGIN` … `COMMIT` block is buffered and applied atomically when all its writes
  land on one shard group; one spanning several is refused rather than
  half-applied.
- **No automatic failover / shard rebuild** — recovery is reconnect-based;
  permanent failures need manual intervention.
- **No snapshots yet** — durability relies on each node's DuckDB WAL plus
  replication.
- **No sequences, and none planned** — `CREATE SEQUENCE`, `nextval()`, `SERIAL` and
  `GENERATED … AS IDENTITY` are refused with the reason. A sequence is one counter:
  per shard it hands out the same numbers everywhere (and each replica advances its
  own copy, since replication ships statements), and in the coordinator it puts
  every insert behind a single allocator. Generate ids client-side — a UUID, a ULID
  or a snowflake — which needs no coordination and spreads evenly over the shards.
- **No user-defined types, and none planned** — `CREATE TYPE` (enum, composite or
  range), `CREATE DOMAIN` and `ALTER TYPE` are refused with the reason. A type is
  cluster-wide state every replica must already hold, and the catalog models tables, so
  a node that joins or is rebuilt would come back without it; nor would the type reach
  the client, which receives a shard's enum column as text anyway. Declare the column
  with a built-in type (`VARCHAR` for an enum, one column per field for a composite) and
  validate the values in the application.
- **No built-in security or observability** — TLS, auth, RBAC, metrics, and
  tracing are planned. With no role model there is also no privilege check on
  server-side `COPY`: any client that can connect can make the coordinator read or
  write any path its process can.
