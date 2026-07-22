//! Coordinator binary entry point.
//!
//! Loads configuration, opens the metadata catalog, and wires up the long-lived
//! services: the gRPC `NodeService` (heartbeats/registration from core nodes),
//! the Ballista scheduler, the failure detector, and the PostgreSQL wire
//! protocol listener for clients.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use vairedb_coordinator::catalog::MetadataCatalog;
use vairedb_coordinator::channel_pool::ChannelPool;
use vairedb_coordinator::config::CoordinatorConfig;
use vairedb_coordinator::node_service::{FailureDetector, NodeServiceImpl};
use vairedb_coordinator::pgwire_handler::VaireDbHandlers;
use vairedb_coordinator::replication::{ReplicationManager, RetryConfig};
use vairedb_coordinator::scheduler;

/// Command-line arguments for the coordinator binary.
#[derive(Parser)]
#[command(name = "vairedb-coordinator")]
struct Cli {
    /// Path to the YAML configuration file (all fields required, no defaults).
    #[arg(long)]
    config_file: PathBuf,
}

/// Start the coordinator: load config, open the catalog, launch the scheduler,
/// failure detector, gRPC server, and pgwire listener, then run until either
/// long-lived server task ends.
///
/// Returns `Err` if config loading, catalog open, address parsing, scheduler
/// startup, or socket binding fails during initialization.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = CoordinatorConfig::from_file(&cli.config_file)?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    let metadata_path = PathBuf::from(&config.metadata_dir).join("vairedb_meta.redb");
    let catalog = Arc::new(MetadataCatalog::open(metadata_path.to_str().unwrap())?);
    tracing::info!("metadata catalog opened at {}", metadata_path.display());

    let channel_pool = Arc::new(ChannelPool::new());

    let retry_config = RetryConfig {
        initial_retry_ms: config.tail_retry_initial_ms,
        max_retry_ms: config.tail_retry_max_ms,
    };
    let replication_manager = Arc::new(ReplicationManager::new(
        Arc::clone(&catalog),
        Arc::clone(&channel_pool),
        retry_config,
    ));

    let failure_detector =
        FailureDetector::new(Arc::clone(&catalog), config.heartbeat_timeout_secs);
    failure_detector.spawn();
    tracing::info!(
        "failure detector started (timeout={}s)",
        config.heartbeat_timeout_secs
    );

    let scheduler_handle =
        scheduler::start_scheduler(Arc::clone(&catalog), &config.ballista_scheduler_listen_addr)
            .await?;
    tracing::info!(
        scheduler_addr = %scheduler_handle.addr,
        "Ballista scheduler ready for executor connections"
    );

    let node_service = NodeServiceImpl::new(Arc::clone(&catalog));

    let grpc_addr: SocketAddr = config.grpc_listen_addr.parse()?;
    let grpc_server = tonic::transport::Server::builder()
        .add_service(
            vairedb_common::proto::vairedb::v1::node_service_server::NodeServiceServer::new(
                node_service,
            ),
        )
        .serve(grpc_addr);

    tracing::info!("gRPC NodeService listening on {}", grpc_addr);

    let pg_addr: SocketAddr = config.pg_listen_addr.parse()?;
    let pg_listener = TcpListener::bind(pg_addr).await?;
    tracing::info!("PostgreSQL wire protocol listening on {}", pg_addr);

    let handlers = Arc::new(VaireDbHandlers::new(
        Arc::clone(&catalog),
        Arc::clone(&replication_manager),
        Arc::clone(&channel_pool),
        Arc::clone(&scheduler_handle.session_ctx),
        Arc::clone(&scheduler_handle.local_ctx),
        config.default_replication_factor,
    ));

    let pg_server = async move {
        loop {
            match pg_listener.accept().await {
                Ok((socket, addr)) => {
                    tracing::debug!("new pg connection from {}", addr);
                    let h = Arc::clone(&handlers);
                    tokio::spawn(async move {
                        if let Err(e) = pgwire::tokio::process_socket(socket, None, h).await {
                            tracing::error!("pg connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("pg accept error: {}", e);
                }
            }
        }
    };

    tokio::select! {
        res = grpc_server => {
            if let Err(e) = res {
                tracing::error!("gRPC server error: {}", e);
            }
        }
        _ = pg_server => {}
    }

    Ok(())
}
