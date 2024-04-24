use chrono::Local;
use ferrum_deposit::config::deposit_config::DepositConfig;
use ferrum_deposit::core::namenode::NameNode;
use ferrum_deposit::error::Result;
use ferrum_deposit::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
use ferrum_deposit::proto::deposit_name_node_service_server::DepositNameNodeServiceServer;
use std::sync::Arc;
use tonic::transport::Server;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DepositConfig::from_xml_file("/config/deposit.xml")?;

    // add logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let namenode = NameNode::from_config(config.clone()).await?;

    let namenode_arc = Arc::new(namenode);
    let _namenode_clone = namenode_arc.clone();

    // tokio::spawn(async move {
    //     let mut interval = tokio::time::interval(Duration::from_millis(config.flush_interval));
    //
    //     loop {
    //         interval.tick().await;
    //         namenode_clone.flush_edit_log().await;
    //     }
    // });

    let addr = format!("0.0.0.0:{}", config.namenode_service_port)
        .parse()
        .unwrap(); //todo: unwrap

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // Server address
    println!("Listening on: {}", addr);

    // Start the server
    Server::builder()
        .add_service(DataNodeNameNodeServiceServer::new(namenode_arc.clone()))
        .add_service(DepositNameNodeServiceServer::new(namenode_arc.clone()))
        .serve(addr)
        .await?;

    Ok(())
}
