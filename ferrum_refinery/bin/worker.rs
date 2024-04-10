use chrono::Local;
use ferrum_refinery::core::worker::Worker;
use ferrum_refinery::config::worker_config::WorkerConfig;
use ferrum_refinery::framework::errors::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let config = WorkerConfig::from_xml_file("config/worker.xml")?;

    Worker::from_config(config).await?;

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));

    loop {}
}
