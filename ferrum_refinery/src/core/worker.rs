use crate::api::map::{KeyValue, Mapper};
use crate::api::reduce::Reducer;
use crate::config::refinery_config::RefineryConfig;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::foreman_service_server::ForemanService;
use crate::proto::worker_service_server::WorkerService;
use crate::proto::{
    HeartbeatRequest, MapTaskRequest, MapTaskResponse, RegistrationRequest, TaskResult,
};
use std::cmp::PartialEq;

use crate::core::worker::WorkerStatus::{Busy, Idle};
use crate::framework::errors::FerrumRefineryError;
use crate::proto::aggregation_service_client::AggregationServiceClient;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use sys_info::{loadavg, mem_info};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use tracing::{error, info};

use crate::core::worker::DataLocality::Remote;
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use ferrum_deposit::proto::GetBlockRequest;

enum DataLocality {
    Local,
    Remote,
}

impl PartialEq<DataLocality> for i32 {
    fn eq(&self, other: &DataLocality) -> bool {
        match other {
            DataLocality::Local => *self == 0,
            DataLocality::Remote => *self == 1,
        }
    }
}

struct HealthMetrics {
    pub cpu_load: f32,
    pub memory_usage: u64,
}

pub struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub metrics_interval: Duration,
    pub heartbeat_interval: Duration,
    pub metrics: Arc<Mutex<HealthMetrics>>,
    pub status: Arc<Mutex<WorkerStatus>>,
    pub mapper: Arc<Mutex<dyn Mapper>>,
    pub reducer: Arc<Mutex<dyn Reducer>>,
    pub deposit_data_dir: String,
    pub aggregator_client: Arc<Mutex<AggregationServiceClient<Channel>>>,
    // client to communicate with the result aggregator
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>, // client to communicate with foreman
}

#[derive(Copy, Clone, Debug)]
pub enum WorkerStatus {
    Idle = 0,
    Busy = 1,
}

impl PartialEq<WorkerStatus> for i32 {
    fn eq(&self, other: &WorkerStatus) -> bool {
        match other {
            Idle => *self == 0,
            Busy => *self == 1,
        }
    }
}

impl Worker {
    pub async fn new(config: RefineryConfig, mapper: impl Mapper, reducer: impl Reducer) -> Self {
        Worker {
            id: Uuid::new_v4(),
            hostname: config.datanode_worker_hostname,
            port: config.worker_service_port,
            metrics_interval: Duration::from_millis(config.worker_metrics_interval as u64),
            heartbeat_interval: Duration::from_millis(config.worker_heartbeat_interval as u64),
            metrics: Arc::new(Mutex::new(HealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
            })),
            status: Arc::new(Mutex::new(Idle)),
            mapper: Arc::new(Mutex::new(mapper)),
            reducer: Arc::new(Mutex::new(reducer)),
            deposit_data_dir: config.datanode_data_dir,
            aggregator_client: Arc::new(Mutex::new(
                AggregationServiceClient::connect(format!(
                    "http://{}:{}",
                    config.aggregator_hostname, config.aggregator_service_port
                ))
                .await
                .unwrap(),
            )),
            foreman_client: Arc::new(Mutex::new(
                ForemanServiceClient::connect(format!(
                    "http://{}:{}",
                    config.namenode_foreman_hostname, config.foreman_service_port
                ))
                .await
                .unwrap(),
            )),
        }
    }

    pub async fn start(&self) {
        self.register_with_foreman().await;
        self.start_heartbeat_worker().await;
        self.start_metrics_worker().await;
    }

    pub async fn register_with_foreman(&self) {
        let request = Request::new(RegistrationRequest {
            worker_id: self.id.to_string(),
            worker_hostname: self.hostname.clone(),
            worker_port: self.port.clone() as u32,
        });

        let foreman_client_clone = self.foreman_client.clone();

        let mut foreman_client_guard = foreman_client_clone.lock().await;

        info!("Sending registration request...");
        let result = foreman_client_guard.register_with_foreman(request).await;

        match result {
            Ok(response) => {
                info!(
                    "Received registration response: {}",
                    response.into_inner().success
                );
            }
            Err(err) => {
                error!("Registration error: {}", err.message().to_string())
            }
        }
    }

    pub async fn start_heartbeat_worker(&self) {
        let foreman_client_clone = self.foreman_client.clone();
        let metrics_clone = self.metrics.clone();
        let id = self.id;
        let status_clone = self.status.clone();
        let interval = self.heartbeat_interval.clone();

        // spawn a thread
        tokio::spawn(async move {
            let mut tokio_interval = tokio::time::interval(interval);

            loop {
                tokio_interval.tick().await;

                let metrics_guard = metrics_clone.lock().await;
                let metrics = crate::proto::HealthMetrics {
                    cpu_load: metrics_guard.cpu_load,
                    memory_usage: metrics_guard.memory_usage,
                };
                drop(metrics_guard);

                let status_guard = status_clone.lock().await;
                let status = *status_guard as i32;

                let request = Request::new(HeartbeatRequest {
                    worker_id: id.to_string(),
                    health_metrics: Some(metrics),
                    status,
                });

                let mut foreman_client_guard = foreman_client_clone.lock().await;

                info!("Sending heartbeat...");
                match foreman_client_guard.send_heart_beat(request).await {
                    Ok(response) => {
                        info!(
                            "Received heartbeat response: {}",
                            response.into_inner().success
                        );
                    }
                    Err(err) => {
                        error!("Heartbeat error: {}", err.message().to_string())
                    }
                }
                drop(foreman_client_guard);
            }
        });
    }

    pub async fn start_metrics_worker(&self) {
        let metrics_clone = self.metrics.clone();
        let interval = self.metrics_interval.clone();

        // spawn a thread

        tokio::spawn(async move {
            let mut tokio_interval = tokio::time::interval(interval);

            loop {
                tokio_interval.tick().await;

                // gather metrics
                let mem_info = mem_info().unwrap();
                let mem_usage = (mem_info.total / mem_info.avail) * 100;
                let cpu_load_avg = loadavg().unwrap().five as f32;

                // get the lock to the metrics object
                let mut metrics_guard = metrics_clone.lock().await;

                metrics_guard.memory_usage = mem_usage;
                metrics_guard.cpu_load = cpu_load_avg;

                info!(
                    "Gathered metrics: cpu_load: {}, memory_usage: {}",
                    metrics_guard.cpu_load, metrics_guard.memory_usage
                );
                drop(metrics_guard);
            }
        });
    }
}

#[tonic::async_trait]
impl WorkerService for Worker {
    async fn complete_map_task(
        &self,
        request: Request<MapTaskRequest>,
    ) -> Result<Response<MapTaskResponse>, Status> {
        // first set status to working

        let mut status_guard = self.status.lock().await;

        // dereference the status guard to set the value at the memory location to busy

        *status_guard = Busy;

        drop(status_guard);

        let inner_request = request.into_inner();
        info!("Received task from foreman: {:?}", inner_request);
        let block_size = inner_request.block_size as usize;

        // see if we need to grab the input data, or if the data is local to this node
        let locality = inner_request.locality;

        // let's allocate some memory to hold the block
        let mut buffer = vec![0u8; block_size];

        // if the block is located on this datanode
        // load the data from /deposit/data/{block_id}_{seq_num}.dat into memory
        if locality == DataLocality::Local {
            // next, we'll attempt to read the file into the buffer
            let file_path_string = format!(
                "{}/{}_{}.dat",
                self.deposit_data_dir, inner_request.block_id, inner_request.seq
            );

            match tokio::fs::File::open(file_path_string).await {
                Ok(mut file) => {
                    file.read_exact(&mut *buffer).await?;
                }

                Err(err) => {
                    return Err(Status::from(FerrumRefineryError::TaskError(
                        err.to_string(),
                    )));
                }
            }
        }

        // if the block is located on another datanode
        // connect to the other datanode and stream the block into memory

        if locality == Remote {
            let deposit_datanode_addr =
                format!("http://{}", inner_request.datanode_address.unwrap());
            let mut deposit_client = DepositDataNodeServiceClient::connect(deposit_datanode_addr)
                .await
                .unwrap();
            let get_block_request = GetBlockRequest {
                block_id: inner_request.block_id,
            };
            let mut stream = deposit_client
                .get_block_streamed(Request::new(get_block_request))
                .await
                .map_err(|e| FerrumRefineryError::TaskError(e.message().to_string()))?
                .into_inner();

            let mut offset = 0;
            while let Some(block_chunk) = stream
                .message()
                .await
                .map_err(|e| FerrumRefineryError::TaskError(e.to_string()))?
            {
                let chunk_data = block_chunk.chunked_data.as_slice();
                let chunk_data_size = chunk_data.len();
                if offset + chunk_data_size > buffer.len() {
                    return Err(Status::from(FerrumRefineryError::BufferOverflow));
                }
                buffer[offset..offset + chunk_data_size].copy_from_slice(chunk_data);
                offset += chunk_data_size;
            }
        }

        let kv = KeyValue {
            key: Bytes::from(inner_request.key),
            value: Bytes::from(buffer),
        };

        let mapper_clone = self.mapper.clone();

        // acquire the mapper lock
        let mapper_guard = mapper_clone.lock().await;

        // do the map task
        let map_result = mapper_guard.map(kv).await;

        info!("Finished map task...reducing");

        let reducer_clone = self.reducer.clone();

        let reducer_guard = reducer_clone.lock().await;

        let reduce_result = reducer_guard.reduce(map_result).await;

        return match reduce_result {
            Ok(result) => {
                info!("Finished reduce task...sending to aggregator");
                // send the results to the aggregator
                let aggregator_request = Request::new(TaskResult {
                    task_id: inner_request.task_id,
                    job_id: inner_request.job_id,
                    worker_id: self.id.to_string(),
                    result: result.to_vec(),
                });
                let aggregator_clone = self.aggregator_client.clone();
                let mut aggregator_guard = aggregator_clone.lock().await;
                match aggregator_guard.send_result(aggregator_request).await {
                    Ok(_) => Ok(Response::new(MapTaskResponse { success: true })),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(Status::from(FerrumRefineryError::TaskError(
                err.to_string(),
            ))),
        };
    }
}
