use crate::config::refinery_config::RefineryConfig;
use crate::core::worker::Profession;
use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;

use crate::proto::foreman_service_server::ForemanService;
use crate::proto::{HeartBeatResponse, HeartbeatRequest, CreateJobRequest, CreateJobResponse, TaskRequest, TaskResponse, MapTask, ReduceTask};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::core::foreman::TaskType::Map;

/// In memory representation of a worker node
struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub profession: Profession,
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
    Reduce
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

#[tonic::async_trait]
impl ForemanService for Foreman {
    async fn send_heart_beat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartBeatResponse>, Status> {
        todo!()
    }

    async fn create_job(&self, request: Request<CreateJobRequest>) -> Result<Response<CreateJobResponse>, Status> {
        todo!()
    }

    async fn poll_for_task(&self, request: Request<TaskRequest>) -> Result<Response<TaskResponse>, Status> {

        let inner_request = request.into_inner();

        let mut task_queue_guard = self.task_queue.lock().await;

        return match task_queue_guard.pop_front() {
            None => {
                let response = TaskResponse {
                    map: None,
                    reduce: None,
                };
                Ok(Response::new(response))
            }
            Some(task) => {
                let task_type = task.task_type;

                if task_type == Map {
                    let map_task = MapTask {
                        job_id: task.job_id.to_string(),
                        task_id: task.id.to_string(),
                        block_id: task.block_id.ok_or(Err(FerrumRefineryError::TaskError("block_id not found.".to_string()))).to_string(),
                        datanode_address: task.datanode_address.ok_or(Err(FerrumRefineryError::TaskError("datanode address not found.".to_string()))).to_string(),
                        reducer_address: Self::determine_reducer().await?,
                    };

                    let response = Response::new(TaskResponse {
                        map: Some(map_task),
                        reduce: None,
                    });
                    Ok(response)
                } else {
                    let reduce_task = ReduceTask {
                        job_id: task.job_id.to_string(),
                        task_id: task.id.to_string(),
                    };
                    let response = Response::new(TaskResponse {
                        map: None,
                        reduce: Some(reduce_task)
                    });
                    Ok(response)
                }
            }
        }
    }
}
