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

    tonic_prost_build::configure().compile_protos(PROTOS, &["../../proto"])?;
    Ok(())
}
