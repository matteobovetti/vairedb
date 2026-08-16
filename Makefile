.PHONY: build build-release check test fmt lint audit clean run-coordinator run-core doc \
       build-common check-common test-common \
       build-coordinator check-coordinator test-coordinator \
       build-core check-core test-core \
       coverage \
       docker-base docker-coordinator docker-core docker \
       e2e e2e-up e2e-test e2e-down

# Build all crates (debug)
build:
	cargo build --all

# Build all crates (release)
build-release:
	cargo build --all --release

# Type-check without producing binaries
check:
	cargo check --all

# Run all tests
test:
	cargo test --all

# Run all tests (continue on failure)
test-no-fail-fast:
	cargo test --all --no-fail-fast

# Format all code
fmt:
	cargo fmt --all

# Check formatting without modifying files
fmt-check:
	cargo fmt --all -- --check

# Run clippy lints
lint:
	cargo clippy --all -- -D warnings

# Run security audit against the advisory database
audit:
	cargo deny check advisories

# Build a single crate (debug)
build-common:
	cargo build --package vairedb-common

build-coordinator:
	cargo build --package vairedb-coordinator

build-core:
	cargo build --package vairedb-core

# Type-check a single crate
check-common:
	cargo check --package vairedb-common

check-coordinator:
	cargo check --package vairedb-coordinator

check-core:
	cargo check --package vairedb-core

# Run tests for a single crate
test-common:
	cargo test --package vairedb-common

test-coordinator:
	cargo test --package vairedb-coordinator

test-core:
	cargo test --package vairedb-core

# Run the coordinator node
run-coordinator:
	cargo run --package vairedb-coordinator -- --config-file config/coordinator/config.yml

# Run the core node
run-core:
	cargo run --package vairedb-core -- --config-file config/core/config.yml

# Generate documentation
doc:
	cargo doc --all --no-deps --open

# Generate code coverage
coverage:
	cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info
	cargo llvm-cov report --html --output-dir coverage

# Build the shared Docker builder image
docker-base:
	docker build -f docker/base.Dockerfile -t vairedb-builder .

# Build the coordinator Docker image
docker-coordinator: docker-base
	docker build -f docker/coordinator.Dockerfile -t vairedb-coordinator .

# Build the core Docker image
docker-core: docker-base
	docker build -f docker/core.Dockerfile -t vairedb-core .

# Build all Docker images
docker: docker-coordinator docker-core

# End-to-end tests (Docker Compose cluster + Rust test runner)
E2E_DIR := tests/e2e
E2E_COMPOSE := docker compose -f $(E2E_DIR)/docker-compose.yml -p vairedb-e2e

e2e: docker
	$(E2E_COMPOSE) up -d --wait
	cd $(E2E_DIR) && cargo test -- --test-threads=1; \
	  status=$$?; \
	  cd ../.. &&  $(MAKE) e2e-down; \
	  exit $$status

e2e-up: docker
	$(E2E_COMPOSE) up -d --wait

e2e-test:
	cd $(E2E_DIR) && cargo test -- --test-threads=1

e2e-down:
	$(E2E_COMPOSE) down -v --remove-orphans

# Remove build artifacts
clean:
	cargo clean
