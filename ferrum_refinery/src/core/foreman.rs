use crate::config::refinery_config::RefineryConfig;

use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::core::worker::WorkerStatus;
use crate::proto::foreman_service_server::ForemanService;

use crate::proto::DataLocality::{Local, Remote};
use crate::proto::{
    CreateJobRequest, CreateJobResponse, DataLocality, GetReducerRequest, GetReducerResponse,
    HeartBeatResponse, HeartbeatRequest, MapTaskRequest, RegistrationRequest, RegistrationResponse,
};
use ferrum_deposit::proto::GetRequest;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{error, info};
use uuid::Uuid;

/// In memory representation of a worker node
#[derive(Debug)]
pub struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub cluster_number: u32,
}

#[derive(Debug)]
pub enum TaskType {
    Map,
    Reduce,
}

#[derive(Debug)]
pub struct Task {
    pub id: Uuid,
    pub job_id: Uuid,
    pub block_id: Uuid,
    pub block_seq: u64,
    pub block_size: u64,
    pub task_type: TaskType,
    pub locality: DataLocality,
    pub datanode_hostname: String,
    pub datanode_port: u16,
}

/// Job Coordinator
#[derive(Debug)]
pub struct Foreman {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub registered_workers: Mutex<u32>,
    pub deposit_namenode_client: Arc<Mutex<DepositNameNodeServiceClient<Channel>>>,
    pub task_queue: Arc<Mutex<VecDeque<Task>>>,
    workers: Arc<Mutex<HashMap<Uuid, Worker>>>,
}
impl Foreman {
    pub async fn from_config(config: RefineryConfig) -> Result<Self, FerrumRefineryError> {
        Ok(Foreman {
            id: Uuid::new_v4(),
            registered_workers: Mutex::new(0),
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

    async fn create_map_task_request(&self, worker: &Worker, task: &Task) -> MapTaskRequest {

        let worker_hostname = worker.hostname.clone();
        let datanode_hostname: Option<String>;
        let worker_port : Option<u32>;

        let locality : DataLocality;
        match worker_hostname == task.datanode_hostname {
            true => {
                locality = Local;
                datanode_hostname = None;
                worker_port = None;


            }
            false => {
                locality = Remote;
                datanode_hostname = Some(task.datanode_hostname.to_string());
                worker_port = Some(worker.port as u32);
            }
        }

        MapTaskRequest {
            job_id: task.job_id.to_string(),
            task_id: task.id.to_string(),
            block_id: task.block_id.to_string(),
            seq: task.block_seq,
            block_size: task.block_size,
            locality: locality as i32,
            datanode_hostname,
            datanode_service_port: worker_port
        }
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
        info!("Received Heartbeat from worker: {:?}", inner_request);

        if status == WorkerStatus::Idle {
            let mut task_queue_guard = self.task_queue.lock().await;
            let maybe_task = task_queue_guard.pop_front();

            return match maybe_task {
                None => Ok(Response::new(HeartBeatResponse {
                    work: None,
                })),
                Some(task) => {
                    // grab the worker from the map
                    let worker_map_guard = self.workers.lock().await;
                    let maybe_worker = worker_map_guard.get(
                        &Uuid::try_parse(inner_request.worker_id.as_str()).map_err(|_| {
                            FerrumRefineryError::UuidError("Failed to parse Uuid".to_string())
                        })?,
                    );

                    return match maybe_worker {
                        None => {
                            error!("Worker not registered with foreman.");
                            Err(Status::from(FerrumRefineryError::HeartbeatError(
                                "Worker not registered.".to_string(),
                            )))
                        }
                        Some(worker) => {
                            info!("Sending task to worker {:?}:{:?}", worker, task);
                            let map_task_request = self.create_map_task_request(worker, &task).await;
                            Ok(Response::new(HeartBeatResponse {
                                work: Some(map_task_request)
                            }))
                        }
                    };
                }
            };
        }
        // worker is busy
        Ok(Response::new(HeartBeatResponse { work: None}))
    }

    /// The foreman places this worker into a HashMap for lookup.
    async fn register_with_foreman(
        &self,
        request: Request<RegistrationRequest>,
    ) -> Result<Response<RegistrationResponse>, Status> {
        let inner_request = request.into_inner();
        info!("Received Worker Registration Request: {:?}", inner_request);
        let worker_id = inner_request.worker_id;

        let worker_uuid = Uuid::from_str(worker_id.as_str()).map_err(|_| {
            Status::from(FerrumRefineryError::UuidError("Invalid Uuid".to_string()))
        })?;
        let worker_hostname = inner_request.worker_hostname.clone();
        let worker_port: u16 = inner_request.worker_port as u16;

        // grab the lock on the registered_workers mutex
        let mut registered_workers_guard = self.registered_workers.lock().await;
        let number_of_workers = *registered_workers_guard;

        let worker = Worker {
            id: worker_uuid,
            cluster_number: number_of_workers,
            hostname: worker_hostname.clone(),
            port: worker_port,
        };

        // increment the number of registered workers
        *registered_workers_guard += 1;

        // drop the lock on the registered workers mutex
        drop(registered_workers_guard);

        let worker_list_clone = self.workers.clone();

        let mut worker_list_guard = worker_list_clone.lock().await;

        return match worker_list_guard.insert(worker_uuid, worker) {
            None => {
                info!("New worker registered.");
                Ok(Response::new(RegistrationResponse { success: true }))
            }
            Some(_) => {
                info!("Worker already registered previously.");
                Ok(Response::new(RegistrationResponse { success: true }))
            }
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
                        datanode_hostname: block.datanodes[0].split(':').next().unwrap().parse().unwrap(),
                        datanode_port: block.datanodes[0].split(':').last().unwrap().parse().unwrap(),
                    };

                    let mut task_queue_guard = task_queue_clone.lock().await;
                    task_queue_guard.push_back(work_task);
                }

                Ok(Response::new(CreateJobResponse {
                    success: true,
                    job_id: Some(job_id.to_string()),
                }))
            }
            Err(err) => {
                error!("Failed to create job: {}", err.to_string());
                Err(Status::from(FerrumRefineryError::JobCreationError(
                    err.to_string(),
                )))
            }
        }
    }

    async fn get_reducer(
        &self,
        request: Request<GetReducerRequest>,
    ) -> Result<Response<GetReducerResponse>, Status> {
        let inner_request = request.into_inner();
        let reducer = inner_request.reducer;
        let reducer_hostname: String;
        let workers_map_guard = self.workers.lock().await;
        for worker in workers_map_guard.iter() {
            if worker.1.cluster_number == reducer {
                reducer_hostname = worker.1.hostname.clone();
                return Ok(Response::new(GetReducerResponse {
                    hostname: reducer_hostname,
                    port: worker.1.port as u32,
                }));
            }
        }
        return Err(Status::from(FerrumRefineryError::GetReducerError(
            "No such worker is registered with the foreman.".to_string(),
        )));
    }
}
