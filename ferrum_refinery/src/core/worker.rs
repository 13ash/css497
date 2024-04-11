use std::error::Error;
use crate::proto::foundry_service_client::FoundryServiceClient;
use ferrum_deposit::deposit::ferrum_deposit_client::FerrumDepositClient;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;
use ferrum_deposit::config::deposit_config::DepositConfig;
use crate::config::worker_config::WorkerConfig;
use crate::core::task::Task;
use crate::framework::errors::FerrumRefineryError;

pub enum Profession {
    Mapper,
    Reducer,
}

#[async_trait]
pub trait Work {
    async fn start_job<T: Task>(&self, task: T) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub deposit_client: Arc<Mutex<FerrumDepositClient>>, // client to communicate with the hdfs deposit
    pub foundry_client: Arc<Mutex<FoundryServiceClient<Channel>>>, // client to communicate with foreman
}


impl Worker {
    pub async fn from_config(config : WorkerConfig) -> Result<Self, FerrumRefineryError> {
        Ok(Worker {
            id: Uuid::new_v4(),
            hostname: config.hostname,
            port: config.port,
            deposit_client: Arc::new(Mutex::new(FerrumDepositClient::from_config(DepositConfig {
                data_dir: config.data_dir,
                namenode_address: format!("http://{}:{}", config.namenode_hostname, config.namenode_port),
            }))),
            foundry_client: Arc::new(Mutex::new(FoundryServiceClient::connect(format!("http://{}:{}", config.foreman_hostname, config.foreman_port)).await.unwrap())),
        })
    }
}
