
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &[
                "proto/deposit_datanode.proto",
                "proto/deposit_namenode.proto",
                "proto/datanode_namenode.proto",
                "proto/common.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
