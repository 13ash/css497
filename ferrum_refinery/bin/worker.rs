use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::Local;
use ferrum_refinery::api::map::{KeyValue, MapOutput, Mapper};
use ferrum_refinery::api::reduce::Reducer;
use std::collections::HashMap;


use tonic::transport::Server;
use tracing_subscriber::fmt::format::FmtSpan;

use ferrum_refinery::config::refinery_config::RefineryConfig;
use ferrum_refinery::core::worker::Worker;
use ferrum_refinery::framework::errors::Result;
use ferrum_refinery::proto::worker_service_server::WorkerServiceServer;

pub struct ExampleMapper;

pub struct ExampleReducer;

#[async_trait]
impl Mapper for ExampleMapper {
    async fn map(&self, kv: KeyValue) -> MapOutput {
        let text = String::from_utf8(kv.value.to_vec()).unwrap();
        let mut word_counts = HashMap::new();

        for word in text.split_whitespace() {
            let clean_word = word
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase();
            if !clean_word.is_empty() {
                *word_counts.entry(clean_word).or_insert(0) += 1;
            }
        }

        word_counts
            .into_iter()
            .map(|(word, count)| (Bytes::from(word), Bytes::from(count.to_string())))
            .collect()
    }
}

#[async_trait]
impl Reducer for ExampleReducer {
    async fn reduce(&self, map_output: MapOutput) -> anyhow::Result<Bytes> {
        let mut counts = HashMap::new();

        for (word, count_bytes) in map_output {
            let count = String::from_utf8(count_bytes.to_vec())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            *counts.entry(word).or_insert(0) += count;
        }

        let mut result = BytesMut::new();
        for (word, total) in counts {
            result.extend_from_slice(&word);
            result.extend_from_slice(b" ");
            result.extend_from_slice(total.to_string().as_bytes());
            result.extend_from_slice(b"\n");
        }

        Ok(result.freeze())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // load the refinery configuration
    let config = RefineryConfig::from_xml_file("config/refinery.xml")?;

    // add logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let port = config.worker_service_port.clone();
    let hostname = "0.0.0.0".to_string();
    let addr = format!("{}:{}", hostname, port);

    // instantiate your mapper and reducer
    let mapper = ExampleMapper;
    let reducer = ExampleReducer;

    // create your worker
    let worker = Worker::new(config, mapper, reducer).await;
    worker.start().await;

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    // listen for tasks from the foreman
    Server::builder()
        .add_service(WorkerServiceServer::new(worker))
        .serve(addr.parse().unwrap())
        .await?;
    Ok(())
}
