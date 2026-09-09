use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecutorOperatingSystemSpecification, ExecutorRegistration};
use ballista_core::serde::scheduler::ExecutorSpecification;
use ballista_core::utils::{GrpcServerConfig, create_grpc_server};
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_executor::execution_engine::DefaultExecutionEngine;
use ballista_executor::execution_loop;
use ballista_executor::executor::Executor;
use ballista_executor::flight_service::BallistaFlightService;
use ballista_executor::metrics::LoggingMetricsCollector;
use datafusion::execution::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::engine::DuckDbEngine;
use crate::error::CoreError;

use super::codec::VaireExecutorPhysicalCodec;

type LogicalPlanNode = datafusion_proto::protobuf::LogicalPlanNode;
type PhysicalPlanNode = datafusion_proto::protobuf::PhysicalPlanNode;

/// Start the Ballista executor and register it with the scheduler.
///
/// Connects to the scheduler at `scheduler_addr`, builds a session state wired
/// with the Vaire physical codec (so `DuckDbScanExec` plans run against
/// `engine`), binds a Flight server on `bind_addr`, and spawns the pull-based
/// poll loop. `advertise_host` is the host reported to the scheduler, and
/// `node_id` becomes the executor id. Returns once the executor is registered
/// and its background tasks are spawned; the executor keeps running on those
/// tasks after this returns.
pub async fn start_executor(
    scheduler_addr: &str,
    concurrent_tasks: usize,
    bind_addr: &str,
    advertise_host: &str,
    engine: Arc<Mutex<DuckDbEngine>>,
    node_id: &str,
) -> Result<(), CoreError> {
    let scheduler_url = normalize_scheduler_url(scheduler_addr);

    let scheduler = SchedulerGrpcClient::connect(scheduler_url.clone())
        .await
        .map_err(|e| CoreError::engine("failed to connect to Ballista scheduler", e))?;

    let session_state = build_session_state(scheduler_url, engine)?;
    let ballista_codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::new(
        session_state.config().ballista_logical_extension_codec(),
        session_state.config().ballista_physical_extension_codec(),
    );

    let config = session_state.config().clone().upgrade_for_ballista();
    let runtime = session_state.runtime_env().clone();
    let max_message_size = config.ballista_grpc_client_max_message_size();
    let config_producer: ConfigProducer = Arc::new(move || config.clone());
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    let listener = TcpListener::bind(bind_addr).await.map_err(|e| {
        CoreError::engine(
            format!("failed to bind Ballista executor to {bind_addr}"),
            e,
        )
    })?;
    let address = listener
        .local_addr()
        .map_err(|e| CoreError::engine("failed to get local address", e))?;

    let executor_meta =
        build_executor_registration(node_id, advertise_host, address, concurrent_tasks);

    let work_dir_handle =
        tempfile::TempDir::new().map_err(|e| CoreError::engine("failed to create temp dir", e))?;
    let work_dir = work_dir_handle
        .path()
        .to_str()
        .ok_or_else(|| CoreError::Engine("temp dir path is not valid UTF-8".to_string()))?
        .to_string();

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new((&session_state).into()),
        Arc::new(LoggingMetricsCollector::default()),
        concurrent_tasks,
        Arc::new(DefaultExecutionEngine::new()),
    ));

    spawn_flight_server(listener, work_dir, work_dir_handle, max_message_size);

    tokio::spawn(execution_loop::poll_loop(
        scheduler,
        executor,
        ballista_codec,
    ));

    tracing::info!(
        concurrent_tasks,
        %address,
        advertise_host,
        "Ballista executor started with custom codec"
    );

    Ok(())
}

/// Ballista's scheduler client requires an `http(s)://` URL; bare `host:port`
/// addresses from config are promoted to `http://`.
fn normalize_scheduler_url(scheduler_addr: &str) -> String {
    if scheduler_addr.starts_with("http") {
        scheduler_addr.to_string()
    } else {
        format!("http://{scheduler_addr}")
    }
}

/// Build a Ballista session state wired with the Vaire physical codec so the
/// scheduler can round-trip `DuckDbScanExec` plans through this executor.
fn build_session_state(
    scheduler_url: String,
    engine: Arc<Mutex<DuckDbEngine>>,
) -> Result<SessionState, CoreError> {
    let codec = Arc::new(VaireExecutorPhysicalCodec::new(engine));
    let session_config =
        SessionConfig::new_with_ballista().with_ballista_physical_extension_codec(codec);

    let session_state = SessionState::new_ballista_state(scheduler_url)
        .map_err(|e| CoreError::engine("failed to create Ballista session state", e))?;

    let mut state = SessionStateBuilder::new_from_existing(session_state)
        .with_config(session_config)
        .build();

    // A stage arrives as a plan referring to its functions by name, so the executor
    // has to hold the same set the coordinator planned against — a UDF missing here
    // fails the stage at deserialization, after the query was accepted. Kept in step
    // with `vairedb_coordinator::scheduler::register_postgres_functions`.
    let count = datafusion_pg_functions::register_all(&mut state);
    tracing::debug!(count, "registered PostgreSQL built-in functions");

    // Same reasoning for the `pg_catalog` scalar functions: a stage projecting
    // `format_type(oid, NULL)` resolves that name from *this* registry.
    vairedb_common::pg_udf::register_pg_catalog_scalar_functions(&mut state)
        .map_err(|e| CoreError::engine("failed to register the pg_catalog scalar functions", e))?;

    // Same reasoning for the ordered-set aggregates: a stage naming `percentile_cont`
    // is resolved from *this* registry, so an executor without them would fail a query
    // the coordinator had already accepted.
    vairedb_common::udaf::register_ordered_set_aggregates(&mut state)
        .map_err(|e| CoreError::engine("failed to register the ordered-set aggregates", e))?;

    Ok(state)
}

/// Build the registration metadata the scheduler stores for this executor:
/// its id, advertised host/port, and task-slot count.
fn build_executor_registration(
    node_id: &str,
    advertise_host: &str,
    address: SocketAddr,
    concurrent_tasks: usize,
) -> ExecutorRegistration {
    ExecutorRegistration {
        id: node_id.to_string(),
        host: Some(advertise_host.to_string()),
        port: address.port() as u32,
        // Pull-based executor: it only binds a Flight server on `address`, with no
        // separate gRPC server. The scheduler never dials grpc_port in pull mode, so
        // mirror the Flight port rather than advertise a port nothing listens on.
        grpc_port: address.port() as u32,
        specification: Some(
            ExecutorSpecification {
                task_slots: concurrent_tasks as u32,
            }
            .into(),
        ),
        os_info: Some(ExecutorOperatingSystemSpecification::default()),
    }
}

/// Spawn the Flight server that serves shuffle files out of `work_dir`. The
/// `TempDir` guard is moved into the task so the directory lives as long as the
/// server and is cleaned up on shutdown rather than leaked into the system temp dir.
fn spawn_flight_server(
    listener: TcpListener,
    work_dir: String,
    work_dir_handle: tempfile::TempDir,
    max_message_size: usize,
) {
    let service = BallistaFlightService::new(work_dir);
    let server = FlightServiceServer::new(service)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

    tokio::spawn(async move {
        let _work_dir_handle = work_dir_handle;
        create_grpc_server(&GrpcServerConfig::default())
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
}
