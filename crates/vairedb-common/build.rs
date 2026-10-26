const PROTOS: &[&str] = &[
    "../../proto/vairedb/v1/error.proto",
    "../../proto/vairedb/v1/node_service.proto",
    "../../proto/vairedb/v1/write_service.proto",
    "../../proto/vairedb/v1/catalog.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The protos live outside this package, so cargo's default "rebuild when a
    // package file changes" rule never sees them: without these lines an edited
    // proto leaves the generated code stale.
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_prost_build::configure()
        // A bulk payload is sent to every replica and kept for retries, so it is a
        // reference-counted buffer: each copy of the request shares the bytes.
        .bytes(".vairedb.v1.BulkRows.arrow_ipc")
        .compile_protos(PROTOS, &["../../proto"])?;
    Ok(())
}
