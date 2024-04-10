use chrono::Local;
use ferrum_refinery::config::foreman_config::ForemanConfig;
use ferrum_refinery::core::foreman::Foreman;
use ferrum_refinery::framework::errors::Result;
use ferrum_refinery::proto::foundry_service_server::FoundryServiceServer;
use tonic::transport::Server;

/// Foreman binary program
#[tokio::main]
async fn main() -> Result<()> {
    let config = ForemanConfig::from_xml_file("config/foreman.xml")?;

    let foreman = Foreman::from_config(config).await?;
    let addr = format!("{}:{}", foreman.hostname, foreman.port);

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(FoundryServiceServer::new(foreman))
        .serve(addr.parse().unwrap())
        .await?;

    Ok(())
}
