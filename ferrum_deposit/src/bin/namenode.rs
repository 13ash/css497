use chrono::Local;
use ferrum_deposit::config::namenode_config::NameNodeConfig;
use ferrum_deposit::error::Result;
use ferrum_deposit::namenode::namenode::NameNode;
use ferrum_deposit::proto::data_node_name_node_service_server::DataNodeNameNodeServiceServer;
use ferrum_deposit::proto::deposit_name_node_service_server::DepositNameNodeServiceServer;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let config = NameNodeConfig::from_xml_file("config/namenode.xml")?;
    let namenode = NameNode::from_config(config.clone()).await?;
    let namenode_arc = Arc::new(namenode);
    let namenode_clone = namenode_arc.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(config.flush_interval));

        loop {
            interval.tick().await;
            namenode_clone.flush_edit_log().await;
        }
    });

    let addr = config.ipc_address.parse().unwrap(); //todo: unwrap

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
