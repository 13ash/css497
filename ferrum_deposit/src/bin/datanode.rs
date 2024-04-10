use chrono::Local;
use ferrum_deposit::config::datanode_config::DataNodeConfig;
use ferrum_deposit::datanode::datanode::DataNode;
use ferrum_deposit::error::Result;
use ferrum_deposit::proto::deposit_data_node_service_server::DepositDataNodeServiceServer;
use tonic::transport::Server;
use tracing::error;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DataNodeConfig::from_xml_file("config/datanode.xml")?;
    let addr = config.ipc_address.parse().unwrap();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let mut datanode: DataNode = DataNode::from_config(config).await;
    match datanode.start().await {
        Ok(_) => {}
        Err(_) => {
            error!("Failed to configure datanode.");
        }
    }

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // Server address
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(DepositDataNodeServiceServer::new(datanode))
        .serve(addr)
        .await?;

    Ok(())
}
