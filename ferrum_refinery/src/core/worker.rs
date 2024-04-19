use crate::api::map::{KeyValue, Mapper};
use crate::api::reduce::Reducer;
use crate::config::refinery_config::RefineryConfig;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::foreman_service_server::ForemanService;
use crate::proto::worker_service_server::WorkerService;
use crate::proto::{
    HeartbeatRequest, MapTaskRequest, MapTaskResponse, ReduceTaskRequest, ReduceTaskResponse,
    RegistrationRequest,
};
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use std::cmp::PartialEq;
use std::path::PathBuf;

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
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>, // client to communicate with the hdfs deposit
    pub aggregator_client: Arc<Mutex<AggregationServiceClient<Channel>>>, // client to communicate with the result aggregator
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>, // client to communicate with foreman
}

#[derive(Copy, Clone)]
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
            hostname: config.worker_hostname,
            port: config.worker_port,
            metrics_interval: Duration::from_millis(config.worker_metrics_interval),
            heartbeat_interval: Duration::from_millis(config.worker_heartbeat_interval),
            metrics: Arc::new(Mutex::new(HealthMetrics {
                cpu_load: 0.0,
                memory_usage: 0,
            })),
            status: Arc::new(Mutex::new(Idle)),
            mapper: Arc::new(Mutex::new(mapper)),
            reducer: Arc::new(Mutex::new(reducer)),
            deposit_data_dir: config.deposit_data_dir,
            deposit_client: Arc::new(Mutex::new(
                DepositDataNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    config.deposit_namenode_hostname, config.deposit_namenode_port
                ))
                .await
                .unwrap(),
            )),
            aggregator_client: Arc::new(Mutex::new(
                AggregationServiceClient::connect(format!(
                    "http://{}:{}",
                    config.aggregator_hostname, config.aggregator_port
                ))
                .await
                .unwrap(),
            )),
            foreman_client: Arc::new(Mutex::new(
                ForemanServiceClient::connect(format!(
                    "http://{}:{}",
                    config.foreman_hostname, config.foreman_port
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

        let result = foreman_client_guard.register_with_foreman(request).await;

        match result {
            Ok(response) => {
                println!("{}", response.into_inner().success);
            }
            Err(err) => {
                println!("{}", err.message())
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
                match foreman_client_guard.send_heart_beat(request).await {
                    Ok(response) => {
                        println!("{}", response.into_inner().success);
                    }
                    Err(err) => {
                        println!("{}", err.message())
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
        let block_size = inner_request.block_size as usize;

        // see if we need to grab the input data, or if the data is local to this node
        let locality = inner_request.locality;

        if locality == DataLocality::Local {
            // load the data from /deposit/data/{block_id}_{seq_num}.dat into memory

            // let's allocate some memory to hold the block

            let mut buffer = vec![0u8; block_size];

            // next, we'll attempt to read the file into the buffer
            let file_path_string = format!(
                "{}/{}_{}.dat",
                self.deposit_data_dir, inner_request.block_id, inner_request.seq
            );
            let file_path = PathBuf::from(file_path_string);

            match tokio::fs::File::open(file_path).await {
                Ok(mut file) => {
                    file.read_exact(&mut *buffer).await?;

                    // now we'll send the map task to the tokio scheduler

                    let kv = KeyValue {
                        key: Bytes::from(inner_request.key),
                        value: Bytes::from(buffer),
                    };

                    let mapper_clone = self.mapper.clone();

                    // acquire the mapper lock
                    let mapper_guard = mapper_clone.lock().await;

                    // do the map task
                    let _result = mapper_guard.map(kv).await;

                    todo!()
                }
                Err(err) => {
                    // set the worker to idle
                    let mut status_guard = self.status.lock().await;
                    *status_guard = Idle;
                    drop(status_guard);
                    Err(Status::from(FerrumRefineryError::TaskError(
                        err.to_string(),
                    )))
                }
            }
        } else {
            todo!()
        }
    }

    async fn complete_reduce_task(
        &self,
        _request: Request<ReduceTaskRequest>,
    ) -> Result<Response<ReduceTaskResponse>, Status> {
        todo!()
    }
}
