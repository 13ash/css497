use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use bytes::{BufMut, BytesMut};
use tokio::sync::Mutex;
use crate::config::refinery_config::RefineryConfig;
use crate::proto::aggregation_service_server::AggregationService;
use crate::proto::{SendResultResponse, SendResultRequest, CreateAggregationJobRequest, CreateAggregationJobResponse, AttachWorkerRequest, AttachWorkerResponse};
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;
use crate::framework::errors::FerrumRefineryError;
use crate::framework::errors::FerrumRefineryError::AggregationError;


pub struct TaskWorker {
    pub id : Uuid,
    pub task: Uuid,
}
pub struct Aggregator {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub data_dir: String,
    pub job_map: Arc<Mutex<HashMap<Uuid, Vec<TaskWorker>>>>,
    pub results_map: Arc<Mutex<HashMap<Uuid, BytesMut>>>
}

impl Aggregator {
    pub async fn from_config(config: RefineryConfig) -> Self {
        Aggregator {
            id: Uuid::new_v4(),
            hostname: config.aggregator_hostname,
            port: config.aggregator_service_port,
            data_dir: config.aggregator_data_dir,
            job_map: Arc::new(Mutex::new(HashMap::new())),
            results_map: Arc::new(Mutex::new(HashMap::new()))
        }
    }
}

#[tonic::async_trait]
impl AggregationService for Aggregator {
    async fn create_aggregation_job(&self, request: Request<CreateAggregationJobRequest>) -> Result<Response<CreateAggregationJobResponse>, Status> {
        let inner_request = request.into_inner();
        let mut job_map_guard = self.job_map.lock().await;
        let job_uuid = Uuid::try_parse(inner_request.job_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        info!("Adding job: {}", inner_request.job_id.clone());
        job_map_guard.insert(job_uuid, vec![]);


        Ok(Response::new(CreateAggregationJobResponse {
            success: true,
        }))
    }

    async fn attach_worker(&self, request: Request<AttachWorkerRequest>) -> Result<Response<AttachWorkerResponse>, Status> {
        let inner_request = request.into_inner();
        let job_uuid = Uuid::try_parse(inner_request.job_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string".to_string())))?;
        let worker_uuid = Uuid::try_parse(inner_request.worker_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        let task_uuid = Uuid::try_parse(inner_request.task_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        let task_worker = TaskWorker {
            id: worker_uuid,
            task: task_uuid,
        };

        let mut job_map_guard = self.job_map.lock().await;
        match job_map_guard.get_mut(&job_uuid) {
            None => {
                return Err(Status::from(AggregationError("Job not found".to_string())))?;
            }
            Some(job) => {
                job.push(task_worker);
                info!("Attaching worker: {}, to job: {}", inner_request.worker_id.clone(), inner_request.job_id.clone());
                Ok(Response::new(AttachWorkerResponse {
                    success: true,
                }))
            }
        }
    }

    async fn send_result(
        &self,
        request: Request<SendResultRequest>,
    ) -> Result<Response<SendResultResponse>, Status> {
        let inner_request = request.into_inner();
        let job_uuid = Uuid::try_parse(inner_request.job_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        let _worker_uuid = Uuid::try_parse(inner_request.worker_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        let _task_uuid = Uuid::try_parse(inner_request.task_id.as_str()).map_err(|_| Status::from(FerrumRefineryError::UuidError("Invalid Uuid string.".to_string())))?;
        info!("Received results: {:?} ", inner_request.result);

        let mut results_map_guard = self.results_map.lock().await;

        match results_map_guard.get_mut(&job_uuid) {
            None => {
                return Err(Status::from(AggregationError("Job not found".to_string())));
            },
            Some(results) => {
                match results.writer().write(inner_request.result.as_slice()) {
                    Ok(bytes_written) => {
                        Ok(Response::new(SendResultResponse {
                            bytes: bytes_written as u64
                        }))
                    },
                    Err(err) => {
                        return Err(Status::from(AggregationError(err.to_string())));
                    }
                }
            }
        }
    }
}

