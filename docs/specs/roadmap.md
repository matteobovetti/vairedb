# Roadmap

## Roadmap to v0.2 - Milestone: Close the database core functionality gap.

| Status | Description |
|--------|-------------|
| IN PROGRESS | Integrate https://github.com/datafusion-contrib/datafusion-postgres/blob/master/datafusion-pg-functions |
| PLANNED | Data type gap re-evaluation and implementation following the prioritization made [here](gap-analysis-data-type.md). Take in consideration https://github.com/datafusion-contrib/datafusion-postgres/blob/master/arrow-pg/src/datatypes.rs|
| PLANNED | Double check the integration with [datafusion-postgres](https://github.com/datafusion-contrib/datafusion-postgres/tree/master/datafusion-postgres). Command statement gap implementation following the prioritization made [here](gap-analysis-command.md). |
| PLANNED | Overall gap re-evaluation after introduction of datafusion-functions. DECISION to take: Aggregate functions gap implementation following the prioritization made [here](gap-analysis-aggregate-function.md). Window functions gap implementation following the prioritization made [here](gap-analysis-window-function.md). Close the GAP with Datafusion [Scalar, Special](https://datafusion.apache.org/user-guide/sql/scalar_functions.html). |
| PLANNED | Operators and literals gap implementation following the prioritization made [here](gap-analysis-operator-literal.md). |
| PLANNED | Implement distributed indexing? |
| PLANNED | Implement schema in the catalog? |
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
