use clap::{Parser, Subcommand};
use ferrum_refinery::config::refinery_config::RefineryConfig;

use ferrum_refinery::framework::refinery::Refinery;
use ferrum_refinery::proto::foreman_service_client::ForemanServiceClient;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Submit {
        #[arg(short, long, num_args = 1..)]
        input: Vec<String>,
        #[arg(short, long)]
        output: String,
    },
}

#[tokio::main]
async fn main() -> ferrum_deposit::error::Result<()> {
    let args = Cli::parse();

    // add logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    match &args.command {
        Commands::Submit { input, output } => {
            let refinery_config = RefineryConfig::from_xml_file("/config/refinery.xml").unwrap();

            // connect to the foreman

            let foreman_client = ForemanServiceClient::connect(format!(
                "http://{}:{}",
                refinery_config.namenode_foreman_hostname, refinery_config.foreman_service_port
            ))
            .await
            .unwrap();

            let wrapped_client = Arc::new(Mutex::new(foreman_client));

            // create a refinery object
            let refinery = Refinery::new(input.to_vec(), output, wrapped_client);

            match refinery.refine().await {
                Ok(_) => {
                    info!("Job submitted to refinery: working...");
                }
                Err(err) => {
                    error!("Refinery Error: {:?}", err);
                }
            }

            Ok(())
        }
    }
}
