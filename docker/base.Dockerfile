FROM lukemathwalker/cargo-chef:latest-rust-1.95.0 AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y lld clang protobuf-compiler && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release
