fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &["src/proto/foreman.proto", "src/proto/worker.proto"],
            &["proto"],
        )?;
    Ok(())
}
