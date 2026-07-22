# AGENTS.md

## Build & Development Commands

All commands are defined in the [`Makefile`](Makefile) — read it directly for the authoritative, up-to-date list. Common entry points: `make build`, `make check`, `make test`, `make fmt`, `make lint`, `make run-coordinator`, `make run-core`, `make e2e`.

## Pull Request Procedure

Complete these steps in order before opening a PR:

1. **Develop the feature** — implement the change on a feature branch.
2. **Develop unit and integration tests** — implement unit and integration tests for the feature.
3. **Test the local crate** — run the affected crate's tests (e.g. `make test-coordinator`).
4. **Test all** — run the full suite with `make test`.
5. **Format** — run `make fmt`.
6. **Lint** — run `make lint` and resolve all warnings.
7. **End-to-end test** — run `make e2e` (requires Docker running).

## Project Structure

**Workspace crates:**

- `crates/vairedb-coordinator` — Coordinator node binary
- `crates/vairedb-core` — Core node binary (stub)
- `crates/vairedb-common` — Shared protobuf-generated code

**Coordinator modules (`crates/vairedb-coordinator/src/`):**

**Core modules (`crates/vairedb-core/src/`):**

**Proto definitions:** `proto/vairedb/v1/` (compiled by `vairedb-common/build.rs`)

## Documentation

All documentation lives in `docs/`. The architecture is split into individual files under `docs/architecture/`, with `docs/architecture/ARCHITECTURE.md` serving as the index.
