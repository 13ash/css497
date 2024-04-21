use crate::config::refinery_config::RefineryConfig;

use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::core::worker::WorkerStatus;
use crate::proto::foreman_service_server::ForemanService;
use crate::proto::worker_service_client::WorkerServiceClient;
use crate::proto::DataLocality::{Local, Remote};
use crate::proto::{
    CreateJobRequest, CreateJobResponse, DataLocality, HeartBeatResponse, HeartbeatRequest,
    MapTaskRequest, ReduceTaskRequest, RegistrationRequest, RegistrationResponse,
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
    pub port: u16,
    pub status: WorkerStatus,
    pub client: Arc<Mutex<WorkerServiceClient<Channel>>>,
}

enum TaskType {
    Map,
    Reduce,
}

struct Task {
    pub id: Uuid,
    pub job_id: Uuid,
    pub block_id: Uuid,
    pub block_seq: u64,
    pub block_size: u64,
    pub task_type: TaskType,
    pub locality: DataLocality,
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
            hostname: config.namenode_foreman_hostname.clone(),
            port: config.foreman_service_port,
            deposit_namenode_client: Arc::new(Mutex::new(
                DepositNameNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    config.namenode_foreman_hostname, config.namenode_service_port
                ))
                    .await
                    .unwrap(),
            )),
            workers: Arc::new(Mutex::new(HashMap::new())),
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }
}

#[tonic::async_trait]
impl ForemanService for Foreman {
    async fn send_heart_beat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        // if the status of the worker is idle

        let inner_request = request.into_inner();
        let status = inner_request.status;

        if status == WorkerStatus::Idle {
            let mut task_queue_guard = self.task_queue.lock().await;
            let maybe_task = task_queue_guard.pop_front();

            return match maybe_task {
                None => Ok(Response::new(HeartBeatResponse { success: true })),
                Some(task) => {
                    // grab the worker from the map
                    let worker_map_guard = self.workers.lock().await;
                    let maybe_worker = worker_map_guard.get(
                        &Uuid::try_parse(inner_request.worker_id.as_str()).map_err(|_| {
                            FerrumRefineryError::UuidError("Failed to parse Uuid".to_string())
                        })?,
                    );

                    return match maybe_worker {
                        None => Err(Status::from(FerrumRefineryError::HeartbeatError(
                            "Worker not registered.".to_string(),
                        ))),
                        Some(worker) => {
                            let mut worker_client_guard = worker.client.lock().await;

                            match task.task_type {
                                TaskType::Map => {
                                    let worker_address =
                                        format!("{}:{}", worker.hostname.clone(), worker.port);
                                    let locality;
                                    let optional_datanode_address;

                                    if task.datanode_address == worker_address {
                                        locality = Local as i32;
                                        optional_datanode_address = None;
                                    } else {
                                        locality = Remote as i32;
                                        optional_datanode_address = Some(task.datanode_address);
                                    }

                                    let map_task_request = MapTaskRequest {
                                        job_id: task.job_id.to_string(),
                                        task_id: task.id.to_string(),
                                        block_id: task.block_id.to_string(),
                                        seq: task.block_seq,
                                        block_size: task.block_size,
                                        key: vec![],
                                        value: vec![],
                                        locality,
                                        datanode_address: optional_datanode_address,
                                    };

                                    match worker_client_guard
                                        .complete_map_task(map_task_request)
                                        .await
                                    {
                                        Ok(_) => {
                                            Ok(Response::new(HeartBeatResponse { success: true }))
                                        }
                                        Err(_) => {
                                            Ok(Response::new(HeartBeatResponse { success: false }))
                                        }
                                    }
                                }

                                TaskType::Reduce => {
                                    let reduce_task_request = ReduceTaskRequest {
                                        job_id: task.job_id.to_string(),
                                        task_id: task.id.to_string(),
                                        key: vec![],
                                        value: vec![],
                                        aggregator_address: "".to_string(),
                                    };

                                    match worker_client_guard
                                        .complete_reduce_task(reduce_task_request)
                                        .await
                                    {
                                        Ok(_) => {
                                            Ok(Response::new(HeartBeatResponse { success: true }))
                                        }
                                        Err(_) => {
                                            Ok(Response::new(HeartBeatResponse { success: false }))
                                        }
                                    }
                                }
                            }
                        }
                    };
                }
            };
        }
        // worker is busy
        Ok(Response::new(HeartBeatResponse { success: true }))
    }

    /// When a worker registers with the foreman, the foreman creates an atomically referenced
    /// client to send tasks to the worker.
    /// The foreman also places this worker into a HashMap for lookup.
    async fn register_with_foreman(
        &self,
        request: Request<RegistrationRequest>,
    ) -> Result<Response<RegistrationResponse>, Status> {
        let inner_request = request.into_inner();

        let worker_id = inner_request.worker_id;

        let worker_uuid = Uuid::from_str(worker_id.as_str()).map_err(|_| {
            Status::from(FerrumRefineryError::UuidError("Invalid Uuid".to_string()))
        })?;
        let worker_hostname = inner_request.worker_hostname.clone();
        let worker_port = inner_request.worker_port;

        let worker = Worker {
            id: worker_uuid,
            hostname: worker_hostname.clone(),
            port: worker_port as u16,
            status: WorkerStatus::Idle,
            client: Arc::new(Mutex::new(
                WorkerServiceClient::connect(format!("http://{}:{}", worker_hostname, worker_port))
                    .await
                    .unwrap(),
            )),
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

    /// A foreman receives a job from the refinery framework, and shards the job into map tasks.
    /// These map tasks correspond to a block within the deposit, the foreman places these tasks
    /// into a task queue, and assigns work to a worker when the worker heartbeats into the foreman
    /// (if the worker is not currently working).
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
                        block_seq: block.seq as u64,
                        block_size: block.block_size,
                        task_type: TaskType::Map,
                        locality: Remote,
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
