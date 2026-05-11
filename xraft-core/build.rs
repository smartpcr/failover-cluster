fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&["../proto/raft.proto"], &["../proto"])?;
    Ok(())
}
