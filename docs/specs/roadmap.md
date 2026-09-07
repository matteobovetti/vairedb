# Roadmap

## Roadmap to v0.2 - Milestone: Close the database core functionality gap.

| Status | Description |
|--------|-------------|
| IN PROGRESS | Close the gap with Duckdb commands write path. |
| PLANNED | Close the gap with Datafusion commands read path. |
| PLANNED | Update docs to reflect command/type/expressions/indexes/constraints/functions gaps vs. implemented features. Users need to know which features are supported and which are not. |
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
