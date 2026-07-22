fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(
        &[
            "../../proto/vairedb/v1/error.proto",
            "../../proto/vairedb/v1/node_service.proto",
            "../../proto/vairedb/v1/write_service.proto",
            "../../proto/vairedb/v1/catalog.proto",
        ],
        &["../../proto"],
    )?;
    Ok(())
}
