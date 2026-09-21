# Internal Roadmap

This document outlines the internal roadmap for the Vairedb project. Compared to the public roadmap, this document focuses on the internal task and milestone.
So here you can find a more granular view of the project's progress.

## Roadmap to v0.3

| Status | Description |
|--------|-------------|
| PLANNED | Improve performance of COPY FROM / TO with bulk inserts. |
| PLANNED | Fuzzy testing crate with massive testing suite for all the db features. |

## Roadmap to v0.4

| Status | Description |
|--------|-------------|
| TODO | Security: TLS, users, groups. |

## Roadmap to v0.5

| Status | Description |
|--------|-------------|
| TODO/SPEC READY | Feature **Compliance - Data Deletion (DDR) - Delete or anonymize all user data**. |
| VALIDATE | Feature **Compliance - Data Takeout (SAR) - Subject Access Request: Provide all user data**. |

## Roadmap to v0.6

| Status | Description |
|--------|-------------|
| TODO | Feature **Data quality** - The system is able to perform async data quality checks and metrics defined with a specific SQL instruction (designed on top of DuckDB SQL dialect). |

## Roadmap to v0.7

| Status | Description |
|--------|-------------|
| TODO | Feature **Rich data catalog** - [Include](https://opendatacontract.com/). |

## Roadmap to v1.0 (production readiness)

| Status | Description |
|--------|-------------|
| TODO | Coordinator WAL. |
| TODO | Coordinator HA. |
| TODO | Snapshotting. |
| TODO | External salt-key for pseudonymization managed in a KMS. |
| TODO | Performance tests (distributed). |
| TODO | Microbenchmark core pieces of the code base. |

## Bank of ideas

| Status | Description |
|--------|-------------|
| VALIDATE | Metadata/Catalog API. |
| VALIDATE | Mutation batch with trigger interval and max number of command executed. |
| VALIDATE | Coordinator WAL. |
