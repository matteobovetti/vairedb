# Contributing to VaireDB

First off, thank you for considering contributing to VaireDB! VaireDB is in
active development toward a production-ready v1.0 release, and contributions of
all kinds — bug reports, feature requests, documentation, and code — are
welcome.

This document explains how to report issues, propose features, set up a local
development environment, and submit pull requests.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Reporting Bugs](#reporting-bugs)
- [Requesting Features](#requesting-features)
- [Setting Up a Development Environment](#setting-up-a-development-environment)
- [Development Workflow](#development-workflow)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Coding Standards](#coding-standards)
- [Project Layout](#project-layout)
- [License](#license)

## Code of Conduct

This project and everyone participating in it is governed by the
[VaireDB Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are
expected to uphold this code. Please report unacceptable behavior as described
in that document.

## Reporting Bugs

Bugs are tracked as [GitHub Issues](https://github.com/matteobovetti/vairedb/issues).

Before opening a new issue, please:

1. **Search existing issues** to avoid duplicates.
2. **Confirm you are on a recent commit** of the `main` branch, since VaireDB
   changes rapidly.

When filing a bug report, include as much of the following as possible:

- A clear, descriptive title.
- Steps to reproduce, ideally a minimal SQL script or client sequence.
- What you expected to happen versus what actually happened.
- Relevant logs, error messages, or stack traces.
- Your environment: OS, Rust version (`rustc --version`), and how you launched
  the cluster (number of coordinator/core nodes, config used).
- In general, every bug report should include a minimal reproduction information.

## Requesting Features

Feature requests are also tracked as
[GitHub Issues](https://github.com/matteobovetti/vairedb/issues). Please label
them clearly (or state in the title) that they are feature requests.

A good feature request describes:

- The problem or use case you are trying to solve.
- Why existing functionality does not cover it.
- A proposed approach, if you have one in mind.

For large or architecturally significant changes, please open an issue to
discuss the design **before** investing time in an implementation. This helps
align the change with the [roadmap](docs/architecture/roadmap.md) and avoids
duplicated effort.

## Setting Up a Development Environment

### Prerequisites

- **Rust** 1.95 or later (2024 edition)
- **Protobuf compiler** (`protoc`) for gRPC code generation
- **Make** for build automation
- **Docker** (required for end-to-end tests)

### Clone and Build

```bash
git clone https://github.com/matteobovetti/vairedb.git
cd vairedb

# Debug build
make build

# Type-check only (faster feedback loop)
make check
```

Protobuf code under `proto/vairedb/v1/` is generated automatically by
`vairedb-common/build.rs` during `cargo build`.

### Running Locally

Start the coordinator in one terminal:

```bash
make run-coordinator
```

Start a core node in a separate terminal:

```bash
make run-core
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432
```

## Development Workflow

Common commands (see the [`Makefile`](Makefile) for the authoritative list):

```bash
# Format code
make fmt

# Run the linter (clippy, fails on warnings)
make lint

# Run all tests
make test

# Run tests for a single crate
cargo test --package vairedb-coordinator

# Run a specific test
cargo test --package vairedb-coordinator -- test_name

# Run end-to-end tests (requires Docker running)
make e2e
```

## Submitting a Pull Request

Please complete the following steps, in order, before opening a pull request:

1. **Develop the feature** on a dedicated feature branch.
2. **Add unit and integration tests** covering the change.
3. **Test the affected crate** (e.g. `cargo test --package vairedb-coordinator`).
4. **Run the full test suite** with `make test`.
5. **Format** the code with `make fmt`.
6. **Lint** with `make lint` and resolve all warnings.
7. **Run end-to-end tests** with `make e2e` (requires Docker running).

Then:

1. **Fork** the repository and create your branch from `main`.
2. Push your branch and **open a pull request** against `main`.
3. Fill in a clear description of **what** the change does and **why**. Link any
   related issues (e.g. `Closes #123`).
4. Ensure the CI pipeline passes. CI runs `cargo check`, `cargo fmt --check` and 
   `cargo clippy -D warnings` — the same checks as the Make
   targets above.
5. Manually run `make test` and `make e2e` to ensure all tests pass.
   (Unfortunately, these are not run by CI due to resource constraints.)
6. Be responsive to review feedback. Maintainers may request changes before
   merging.

Keep pull requests focused: one logical change per PR makes review faster and
history cleaner.

## Coding Standards

- Code must be formatted with `rustfmt` (`make fmt`).
- Code must pass `clippy` with no warnings (`make lint`); CI enforces
  `-D warnings`.
- New behavior should be covered by tests.
- We write ideomatic Rust code (where possible), and we havily leverage on 
  SOLID principles.
- If you are using agentic coding, you can find skills and `AGENTS.md` 
  in the `agentic` directory.

## Project Layout

```
vairedb/
├── config/                    # YAML configuration files
├── crates/
│   ├── vairedb-coordinator/   # Coordinator node binary
│   ├── vairedb-core/          # Core node binary
│   └── vairedb-common/        # Shared protobuf code, config, scan plans
├── docker/                    # Dockerfiles
├── docs/                      # Architecture, feature, and testing documentation
├── proto/vairedb/v1/          # Protobuf service definitions
├── tests/e2e/                 # End-to-end tests
└── Makefile                   # Build automation
```

For a deeper understanding of the system, start with the
[Architecture Documentation](docs/architecture/ARCHITECTURE.md).

## License

By contributing to VaireDB, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE), the same license that covers this
project. You also confirm that you have the right to submit the work under that
license.
