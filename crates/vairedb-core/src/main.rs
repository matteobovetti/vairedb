//! Core node entry point.
//!
//! Boots the node from its config file and wires together the long-lived
//! components: the DuckDB engine, the write queue, the heartbeat client, the
//! Ballista executor, and the gRPC `WriteService`. The process runs until the
//! coordinator sends a drain signal, which triggers a graceful gRPC shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use vairedb_core::ballista_exec;
use vairedb_core::config::CoreConfig;
use vairedb_core::engine::DuckDbEngine;
use vairedb_core::heartbeat::HeartbeatClient;
use vairedb_core::write_queue::WriteQueue;
use vairedb_core::write_service::WriteServiceImpl;

/// Command-line arguments for the core node binary.
#[derive(Parser)]
#[command(name = "vairedb-core")]
struct Cli {
    /// Path to the YAML configuration file. All fields are required; there are
    /// no defaults.
    #[arg(long)]
    config_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = CoreConfig::from_file(&cli.config_file)?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    tracing::info!(node_id = %config.node_id, "starting vairedb-core node");

    let data_dir = PathBuf::from(&config.data_dir);
    let engine = DuckDbEngine::open(&data_dir)?;
    tracing::info!(data_dir = %config.data_dir, "duckdb engine initialized");

    let write_conn = engine.write_connection()?;
    let write_queue = WriteQueue::start(write_conn, config.write_queue_capacity);
    tracing::info!(
        capacity = config.write_queue_capacity,
        "write queue started"
    );

    let shards = engine.list_tables().unwrap_or_default();
    tracing::info!(shard_count = shards.len(), "discovered local shards");

    let shared_engine = Arc::new(Mutex::new(engine));

    let heartbeat_client = HeartbeatClient::new(
        config.node_id.clone(),
        config.effective_advertised_address().to_string(),
        config.coordinator_addr.clone(),
        config.heartbeat_interval_secs,
    );

    if let Err(e) = heartbeat_client.register(shards).await {
        tracing::warn!(error = %e, "failed to register with coordinator (will retry via heartbeat)");
    }

    let mut drain_rx = heartbeat_client.spawn_with_reconnect();

    match ballista_exec::start_executor(
        &config.ballista_scheduler_addr,
        config.ballista_concurrent_tasks,
        &config.ballista_bind_addr(),
        &config.ballista_advertise_host(),
        Arc::clone(&shared_engine),
        &config.node_id,
    )
    .await
    {
        Ok(()) => tracing::info!(
            "Ballista executor connected to scheduler at {}",
            config.ballista_scheduler_addr
        ),
        Err(e) => {
            tracing::warn!(error = %e, "failed to start Ballista executor (reads unavailable)")
        }
    }

    let write_service = WriteServiceImpl::new(write_queue);

    let grpc_addr: SocketAddr = config.grpc_listen_addr.parse()?;
    let grpc_server = tonic::transport::Server::builder()
        .add_service(
            vairedb_common::proto::vairedb::v1::write_service_server::WriteServiceServer::new(
                write_service,
            ),
        )
        .serve_with_shutdown(grpc_addr, async move {
            let _ = drain_rx.wait_for(|drained| *drained).await;
            tracing::info!("drain signal received, shutting down gRPC server");
        });

    tracing::info!(%grpc_addr, "gRPC WriteService listening");

    grpc_server.await?;

    tracing::info!("vairedb-core node shut down gracefully");
    Ok(())
}
