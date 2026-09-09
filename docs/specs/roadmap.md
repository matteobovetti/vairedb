# Roadmap

## Roadmap to v0.2 - Milestone: Close the database core functionality gap.

| Status | Description |
|--------|-------------|
| IN PROGRESS | Close the database core functionality gap for 0.2. Measured in [`gap-analysis.md`](gap-analysis.md); shipped this pass: streaming `COPY … FROM STDIN` / `TO STDOUT`, the typed error classifier and SQLSTATE table, the write path's expression translation and its zero-divisor guard, core-node re-registration on every attach, `avg(bigint)` widening, exact `percentile_cont`/`percentile_disc`, the join and set-operation axis, the `USING` key merge on full and right joins, the `42804` refusal of a `UNION`, `INTERSECT` or `EXCEPT` whose branches have no common PostgreSQL type, the distributed keyless join — an `EXISTS`/`NOT EXISTS` or a `LEFT`/`FULL JOIN` without an equijoin key silently lost rows on a cluster, which was the widest defect the new axis found — and the `pg_catalog` scalar functions, registered on every node that plans or executes so a plan carrying `format_type` over a column survives distribution and `psql`'s `\gdesc` works, which also closed a raw gRPC `Status { … }` leak to the client. Remaining is ranked in its § 6.2, top open item first: an error raised past the Ballista scheduler loses its SQLSTATE. |
| IN PROGRESS | Massive data import: the server side ships with `COPY … FROM STDIN` / `TO STDOUT`, so `psql`'s `\copy` works in both directions. Remaining is the VaireDB CLI itself. |
| PLANNED | Fuzzy testing crate with massive testing suite for all the db feature. |
| TODO | Security: TLS, users, groups. |

## Roadmap to v0.3
| TODO/SPEC READY | Feature **Compliance - Data Deletion (DDR) - Delete or anonymize all user data** |
| VALIDATE | Feature **Compliance - Data Takeout (SAR) - Subject Access Request: Provide all user data** |

## Roadmap to v0.4

| Status | Description |
|--------|-------------|
| TODO | Feature **Data quality** - The system is able to perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of DuckDB SQL dialect). |

## Roadmap to v0.5

| Status | Description |
|--------|-------------|
| TODO | Feature **Rich data catalog** - [Include](https://opendatacontract.com/) |

## Roadmap to v1.0 (production readiness)

| Status | Description |
|--------|-------------|
| TODO | Coordinator HA. |
| TODO | Performance tests (distributed). |
| TODO | Microbenshmark core piace of the code base. |

## Bank of ideas
| Status | Description |
|--------|-------------|
| VALIDATE | Metadata/Catalog API. |
| VALIDATE | Mutation batch with trigger interval and max number of command executed. |
| VALIDATE | Coordinator WAL. |
