ARG BASE_IMAGE=vairedb-builder
FROM ${BASE_IMAGE} AS builder

FROM debian:trixie-slim AS runtime
WORKDIR /app
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/vairedb-core vairedb-core
COPY config/core/config.yml config/core/config.yml
RUN mkdir -p data/core

EXPOSE 50041

ENTRYPOINT ["./vairedb-core", "--config-file", "/app/config/core/config.yml"]
