use chrono::Local;
use ferrum_refinery::config::refinery_config::RefineryConfig;
use ferrum_refinery::core::foreman::Foreman;
use ferrum_refinery::framework::errors::Result;
use ferrum_refinery::proto::foreman_service_server::ForemanServiceServer;
use tonic::transport::Server;
use tracing_subscriber::fmt::format::FmtSpan;

/// Foreman binary program
#[tokio::main]
async fn main() -> Result<()> {
    let config = RefineryConfig::from_xml_file("/config/refinery.xml")?;

    // add logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let foreman = Foreman::from_config(config).await?;
    let addr = format!("{}:{}", "0.0.0.0".to_string(), foreman.port);

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(ForemanServiceServer::new(foreman))
        .serve(addr.parse().unwrap())
        .await?;

    Ok(())
}
