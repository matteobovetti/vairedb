ARG BASE_IMAGE=vairedb-builder
FROM ${BASE_IMAGE} AS builder

FROM debian:trixie-slim AS runtime
WORKDIR /app
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/vairedb-coordinator vairedb-coordinator
COPY config/coordinator/config.yml config/coordinator/config.yml
RUN mkdir -p data/coordinator

EXPOSE 5432
EXPOSE 50040

ENTRYPOINT ["./vairedb-coordinator", "--config-file", "/app/config/coordinator/config.yml"]
