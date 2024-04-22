fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &[
                "src/proto/deposit_datanode.proto",
                "src/proto/deposit_namenode.proto",
                "src/proto/datanode_namenode.proto",
                "src/proto/common.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
