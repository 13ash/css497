use crate::config::refinery_config::RefineryConfig;

use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::proto::foreman_service_server::ForemanService;
use crate::proto::{
    CreateJobRequest, CreateJobResponse, HeartBeatResponse, HeartbeatRequest, RegistrationRequest,
    RegistrationResponse,
};
use ferrum_deposit::proto::GetRequest;
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

struct Task {
    pub id: Uuid,
    pub job_id: Uuid,
    pub block_id: Uuid,
    pub datanode_address: String,
}

/// Job Coordinator
pub struct Foreman {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub deposit_namenode_client: Arc<Mutex<DepositNameNodeServiceClient<Channel>>>,
    pub task_queue: Arc<Mutex<VecDeque<Task>>>,
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
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    pub async fn determine_reducer() -> Result<String, FerrumRefineryError> {
        Ok(String::from("test"))
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

    async fn register_with_foreman(
        &self,
        request: Request<RegistrationRequest>,
    ) -> Result<Response<RegistrationResponse>, Status> {
        let inner_request = request.into_inner();

        let worker_id = inner_request.worker_id;

        let worker_uuid = Uuid::from_str(worker_id.as_str()).map_err(|_| {
            Status::from(FerrumRefineryError::UuidError("Invalid Uuid".to_string()))
        })?;
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
            None => Err(Status::from(FerrumRefineryError::RegistrationError(
                "Failed to register".to_string(),
            ))),
            Some(_) => Ok(Response::new(RegistrationResponse { success: true })),
        };
    }

    async fn create_job(
        &self,
        request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        let inner_request = request.into_inner();

        let input_file_path = inner_request.input_data;
        let _output_file_path = inner_request.output_data;

        let namenode_client_clone = self.deposit_namenode_client.clone();

        let mut namenode_client_guard = namenode_client_clone.lock().await;

        let get_file_request = GetRequest {
            path: input_file_path,
        };

        match namenode_client_guard.get(get_file_request).await {
            Ok(response) => {
                let job_id = Uuid::new_v4(); // create new job_id

                let inner_response = response.into_inner();
                let file_blocks = inner_response.file_blocks;

                let task_queue_clone = self.task_queue.clone();

                for block in file_blocks {
                    let work_task = Task {
                        id: Uuid::new_v4(),
                        job_id: job_id.clone(),
                        block_id: block.block_id.parse().unwrap(),
                        datanode_address: block.datanodes[0].to_string(),
                    };

                    let mut task_queue_guard = task_queue_clone.lock().await;
                    task_queue_guard.push_back(work_task);
                }

                Ok(Response::new(CreateJobResponse { success: true }))
            }
            Err(err) => Err(Status::from(FerrumRefineryError::JobCreationError(
                err.to_string(),
            ))),
        }
    }
}
