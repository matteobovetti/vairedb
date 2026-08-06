# Roadmap

## Roadmap to v0.2

| Status | Description |
|--------|-------------|
| WIP | Implement duckdb vs. pgsql command gap. |
| WIP | Feature **Compliance - Data Deletion (DDR) - Delete or anonymize all user data** |
| VALIDATE | Feature **Compliance - Data Takeout (SAR) - Subject Access Request: Provide all user data** |
| TODO | VaireDB CLI with massive data import SQL command and psql client. |
| TODO | Security: TLS, users, groups. |

## Roadmap to v0.3

| Status | Description |
|--------|-------------|
| TODO | Feature **Data quality** - The system is able to perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of DuckDB SQL dialect). |

## Roadmap to v0.4

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
