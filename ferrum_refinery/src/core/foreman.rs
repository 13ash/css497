use crate::config::foreman_config::ForemanConfig;
use crate::core::task::Task;
use crate::core::worker::Profession;
use crate::framework::errors::FerrumRefineryError;
use crate::proto::foundry_service_server::FoundryService;
use crate::proto::{HeartBeatResponse, HeartbeatRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// In memory representation of a worker node
struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub profession: Profession,
}

/// A worker node is either a mapper or reducer

/// Job Coordinator
pub struct Foreman {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    tasks: Arc<Mutex<HashMap<Uuid, Task>>>, // map to track tasks
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>, // map to track workers
}

impl Foreman {
    pub async fn from_config(config: ForemanConfig) -> Result<Self, FerrumRefineryError> {
        Ok(Foreman {
            id: Uuid::new_v4(),
            hostname: config.hostname,
            port: config.port,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[tonic::async_trait]
impl FoundryService for Foreman {
    async fn send_heart_beat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        todo!()
    }
}
