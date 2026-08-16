# Roadmap

## Roadmap to v0.2 - Milestone: Close the database core functionality gap.

| Status | Description |
|--------|-------------|
| WIP | DuckDB Statement command gap. Analyze and implement missing commands, rethinking it in a distributed way. |
| PLANNED | DuckDB query sintax gap. Analyze and implement missing commands, rethinking it in a distributed way. |
| PLANNED | DuckDB query data types gap. Analyze and implement missing types. |
| PLANNED | DuckDB query Expressions gap. Analyze and implement missing expressions. |
| PLANNED | DuckDB query Functions gap. Analyze and implement missing functions. |
| PLANNED | DuckDB query Constraints gap. Analyze and implement missing constraints. |
| PLANNED | DuckDB query Indexes gap. Analyze and implement missing indexes. |
| PLANNED | Sort Keys for earch shareds. |
| TODO | VaireDB CLI with massive data import SQL command and psql client. |
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
