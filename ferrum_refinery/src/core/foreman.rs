use crate::config::refinery_config::RefineryConfig;

use crate::framework::errors::FerrumRefineryError;
use std::cmp::PartialEq;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::proto::foreman_service_server::ForemanService;
use crate::proto::{CreateJobRequest, CreateJobResponse, HeartBeatResponse, HeartbeatRequest, RegistrationRequest, RegistrationResponse};
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// In memory representation of a worker node
struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u32,
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

    async fn register_with_foreman(&self, request: Request<RegistrationRequest>) -> Result<Response<RegistrationResponse>, Status> {
        let inner_request = request.into_inner();

        let worker_id = inner_request.worker_id;

        let worker_uuid = Uuid::from_str(worker_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid".to_string())))?;
        let worker_hostname = inner_request.worker_hostname;
        let worker_port = inner_request.worker_port;

        let worker = Worker {
            id: worker_uuid,
            hostname: worker_hostname,
            port: worker_port,
            status: WorkerStatus::Idle,
        };

        let worker_list_clone = self.workers.clone();

        let mut worker_list_guard = worker_list_clone.lock().await;

        return match worker_list_guard.insert(worker_uuid, worker) {
            None => {
                Err(Status::from(FerrumRefineryError::RegistrationError("Failed to register".to_string())))
            }
            Some(_) => {
                Ok(Response::new(RegistrationResponse {
                    success: true,
                }))
            }
        }
    }

    async fn create_job(
        &self,
        _request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        todo!()
    }
}
