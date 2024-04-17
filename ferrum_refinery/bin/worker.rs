use async_trait::async_trait;
use chrono::Local;
use ferrum_refinery::api::map::{KeyValue, MapOutput, Mapper};
use ferrum_refinery::api::reduce::Reducer;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use bytes::Bytes;
use tonic::transport::Server;

use ferrum_refinery::config::refinery_config::RefineryConfig;
use ferrum_refinery::core::worker::Worker;
use ferrum_refinery::framework::errors::Result;
use ferrum_refinery::proto::worker_service_server::WorkerServiceServer;

pub struct ExampleMapper;

pub struct ExampleReducer;

#[async_trait]
impl Mapper for ExampleMapper {
    async fn map(&self, kv: KeyValue) -> MapOutput {
        todo!()
    }
}

impl Reducer for ExampleReducer {
    async fn reduce(&self, key: Bytes, values: Box<dyn Iterator<Item=Bytes> + '_>) -> anyhow::Result<Bytes> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // load the refinery configuration
    let config = RefineryConfig::from_xml_file("config/refinery.xml")?;

    let port = config.worker_port.clone();
    let hostname = config.worker_hostname.clone();

    // instantiate your mapper and reducer
    let mapper = ExampleMapper;
    let reducer = ExampleReducer;

    // create your worker
    let worker = Worker::new(config, mapper, reducer).await;

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // listen for tasks from the foreman
    Server::builder()
        .add_service(WorkerServiceServer::new(worker))
        .serve(SocketAddr::new(
            IpAddr::from_str(hostname.as_str()).unwrap(),
            port,
        ))
        .await?;

    Ok(())
}
