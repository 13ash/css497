use chrono::Local;
use ferrum_refinery::config::refinery_config::RefineryConfig;
use ferrum_refinery::core::aggregator::Aggregator;
use ferrum_refinery::framework::errors::Result;
use ferrum_refinery::proto::aggregation_service_server::AggregationServiceServer;
use tonic::transport::Server;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<()> {
    let config = RefineryConfig::from_xml_file("config/refinery.xml")?;

    // add logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let aggregator = Aggregator::from_config(config).await;
    let addr = format!("{}:{}", "0.0.0.0", aggregator.port);

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(AggregationServiceServer::new(aggregator))
        .serve(addr.parse().unwrap())
        .await?;

    Ok(())
}
