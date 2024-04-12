use crate::config::refinery_config::RefineryConfig;
use crate::core::task::Task;
use crate::core::worker::Profession;
use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::proto::foreman_service_server::ForemanService;
use crate::proto::{HeartBeatResponse, HeartbeatRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// In memory representation of a worker node
struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub profession: Profession,
}

/// Job Coordinator
pub struct Foreman {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub deposit_namenode_client: Arc<Mutex<DepositNameNodeServiceClient<Channel>>>,
    tasks: Arc<Mutex<HashMap<Uuid, Box<dyn Task>>>>, // map to track tasks
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>,      // map to track workers
}
impl Foreman {
    pub async fn from_config(config: RefineryConfig) -> Result<Self, FerrumRefineryError> {
        Ok(Foreman {
            id: Uuid::new_v4(),
            hostname: config.foreman_hostname,
            port: config.foreman_port,
            deposit_namenode_client: Arc::new(Mutex::new(
                DepositNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    config.deposit_namenode_hostname, config.deposit_namenode_port
                ))
                .await
                .unwrap(),
            )),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[tonic::async_trait]
impl ForemanService for Foreman {
    async fn send_heart_beat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        todo!()
    }
}
