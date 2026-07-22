# Installation

VaireDB ships as two self-contained Rust binaries — `vairedb-coordinator` and
`vairedb-core` — that you can run either directly or as Docker images. For a
multi-node cluster, the Docker images are the recommended path
(see the [Quick Start](quickstart.md)).

## Option 1 — Docker images (recommended)

The repository ships a multi-stage build based on
[`cargo-chef`](https://github.com/LukeMathWalker/cargo-chef) for cached,
reproducible builds. A shared *builder* image compiles the workspace once; the
coordinator and core images then copy out the release binaries.

```bash
# Build the shared builder image, then the coordinator and core images
make docker
```

This produces three images:

| Image | Built from | Contains |
|-------|------------|----------|
| `vairedb-builder` | `docker/base.Dockerfile` | Cached release build of the whole workspace |
| `vairedb-coordinator` | `docker/coordinator.Dockerfile` | `vairedb-coordinator` binary + default config |
| `vairedb-core` | `docker/core.Dockerfile` | `vairedb-core` binary + default config |

You can also build them individually:

```bash
make docker-base          # build the builder image
make docker-coordinator   # build the coordinator image
make docker-core          # build the core image
```

Each runtime image runs its binary against a config file mounted at a fixed path:

- Coordinator: `/app/config/coordinator/config.yml` (exposes `5432`, `50040`)
- Core node: `/app/config/core/config.yml` (exposes `50041`)

## Option 2 — Build from source

### Prerequisites

- **Rust** 1.95 or later (2024 edition)
- **Protobuf compiler** (`protoc`) — gRPC code is generated at build time
- **Make** for the build automation targets

### Build

```bash
make build          # debug build of all crates
make build-release  # optimized release build
make check          # type-check only (fast feedback loop)
```

### Run locally

Start the coordinator in one terminal:

```bash
make run-coordinator
```

Start a core node in another terminal:

```bash
make run-core
```

Both binaries take a single `--config-file <path>` argument. The `make` targets
point at the defaults under `config/`:

```
config/
├── coordinator/
│   └── config.yml   # coordinator defaults
└── core/
    └── config.yml   # core node defaults
```

See the [Configuration](configuration.md) page for every setting.

## Verify the install

Once a coordinator (and at least one core node) is running, connect with any
PostgreSQL client:

```bash
psql -h 127.0.0.1 -p 5432 "sslmode=disable"
```

If the connection succeeds you are ready to [create tables and run queries](../sql/index.md).
