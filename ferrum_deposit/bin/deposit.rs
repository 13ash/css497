use clap::{Parser, Subcommand};
use ferrum_deposit::config::deposit_config::DepositConfig;
use ferrum_deposit::deposit::ferrum_deposit_client::{Client, FerrumDepositClient};
use ferrum_deposit::error::{FerrumDepositError, Result};
use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;
use ferrum_deposit::proto::{
    ConfirmFilePutRequest, DeleteFileRequest, GetRequest, LsRequest, PutFileRequest,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Get {
        #[arg(short, long)]
        fp: String,
    },
    Put {
        #[arg(short, long)]
        fp: String,
        #[arg(short, long)]
        lfp: String,
    },
    Delete {
        #[arg(short, long)]
        fp: String,
    },
    Ls {
        #[arg(short, long)]
        fp: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let config = DepositConfig::from_xml_file("/config/deposit.xml")?;
    let mut namenode_client =
        DepositNameNodeServiceClient::connect(format!("http://{}", config.namenode_address))
            .await?;
    let deposit_client = FerrumDepositClient::from_config(config);

    match &args.command {
        Commands::Get { fp } => {
            let request = GetRequest { path: fp.clone() };
            let unmatched_response = namenode_client.get(request).await;
            match unmatched_response {
                Ok(response) => {
                    let file_pathbuf = PathBuf::from(fp);

                    if let Some(file_name) = file_pathbuf.file_name() {
                        let file_name_str = file_name.to_string_lossy();

                        let new_path = PathBuf::from(format!("/tmp/{}", file_name_str));
                        deposit_client.get(new_path, response.into_inner()).await?;
                    } else {
                        eprintln!("Error: The file path does not have a file name component.");
                    }
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }

        Commands::Ls { fp } => {
            let request = LsRequest { path: fp.clone() };
            let unmatched_response = namenode_client.ls(request).await;
            match unmatched_response {
                Ok(response) => {
                    println!("{:?}", response.into_inner().inodes);
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }

        Commands::Put { fp, lfp } => {
            let local_file = tokio::fs::File::open(lfp)
                .await
                .map_err(|_| FerrumDepositError::FileSystemError("File not found.".to_string()))?;
            let local_file_size = local_file.metadata().await.unwrap().len();
            let request = PutFileRequest {
                path: fp.clone(),
                file_size: local_file_size,
            };
            let unmatched_response = namenode_client.put_file(request).await;
            match unmatched_response {
                Ok(response) => {
                    let inner_response = response.into_inner().clone();

                    match deposit_client.put(local_file, inner_response).await {
                        Ok(written_blocks) => {
                            match namenode_client
                                .confirm_file_put(ConfirmFilePutRequest {
                                    path: fp.clone(),
                                    block_ids: written_blocks,
                                })
                                .await
                            {
                                Ok(_) => println!("File write confirmed successfully."),
                                Err(e) => eprintln!("Error confirming file write: {:?}", e),
                            }
                        }
                        Err(e) => eprintln!("Error writing blocks: {:?}", e),
                    }
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }
        Commands::Delete { fp } => {
            let request = DeleteFileRequest { path: fp.clone() };
            let unmatched_response = namenode_client.delete_file(request).await;
            match unmatched_response {
                Ok(response) => {
                    println!("{:?}", response);
                }
                Err(e) => eprintln!("Error deleting blocks: {:?}", e),
            }
        }
        _ => {
            eprintln!("Not valid command")
        }
    }
    Ok(())
}
