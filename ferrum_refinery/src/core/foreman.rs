use crate::config::refinery_config::RefineryConfig;
use crate::core::worker::Profession;
use crate::framework::errors::FerrumRefineryError;
use std::cmp::PartialEq;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::proto::foreman_service_server::ForemanService;
use crate::proto::{CreateJobRequest, CreateJobResponse, HeartBeatResponse, HeartbeatRequest};
use std::collections::{HashMap, VecDeque};
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
    pub status: WorkerStatus,
}

enum WorkerStatus {
    Busy,
    Idle,
}

struct WorkTask {
    pub id: Uuid,
    pub job_id: Uuid,
    pub task_type: TaskType,
    pub block_id: Option<Uuid>,
    pub datanode_address: Option<String>,
}
enum TaskType {
    Map,
    Reduce,
}

/// Job Coordinator
pub struct Foreman {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub deposit_namenode_client: Arc<Mutex<DepositNameNodeServiceClient<Channel>>>,
    pub task_queue: Mutex<VecDeque<WorkTask>>,
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>,
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
            workers: Arc::new(Mutex::new(HashMap::new())),
            task_queue: Mutex::new(VecDeque::new()),
        })
    }

    pub async fn determine_reducer() -> Result<String, FerrumRefineryError> {
        Ok(String::from("test"))
    }
}

impl PartialEq for TaskType {
    fn eq(&self, other: &Self) -> bool {
        return self == other;
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

    async fn create_job(
        &self,
        _request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        todo!()
    }
}
