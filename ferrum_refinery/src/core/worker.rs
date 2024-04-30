use crate::api::map::{KeyValue, Mapper};
use crate::api::reduce::Reducer;
use crate::config::refinery_config::RefineryConfig;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::worker_service_server::WorkerService;
use crate::proto::{GetReducerRequest, HeartbeatRequest, RegistrationRequest, SendKeyValuePairRequest, SendKeyValuePairResponse, StartReduceRequest, StartReduceResponse};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::core::worker::WorkerStatus::{Busy, Idle};

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

use crate::core::worker::DataLocality::{Local, Remote};
use crate::framework::errors::FerrumRefineryError;

use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use ferrum_deposit::proto::GetBlockRequest;
use tracing::{error, info};
use crate::proto::worker_service_client::WorkerServiceClient;

pub enum DataLocality {
    Local,
    Remote,
}

impl PartialEq<DataLocality> for i32 {
    fn eq(&self, other: &DataLocality) -> bool {
        match other {
            Local => *self == 0,
            Remote => *self == 1,
        }
    }
}

pub struct HealthMetrics {
    pub cpu_load: f32,
    pub memory_usage: u64,
}

pub struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub map_output_kv_pairs: Mutex<HashMap<Bytes, Bytes>>,
    pub datanode_service_port: u16,
    pub metrics_interval: Duration,
    pub heartbeat_interval: Duration,
    pub metrics: Arc<Mutex<HealthMetrics>>,
    pub status: Arc<Mutex<WorkerStatus>>,
    pub mapper: Arc<Mutex<dyn Mapper>>,
    pub reducer: Arc<Mutex<dyn Reducer>>,
    pub number_of_workers: usize,
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
            hostname: sys_info::hostname().unwrap(),
            port: config.worker_service_port,
            datanode_service_port: config.datanode_service_port,
            metrics_interval: Duration::from_millis(config.worker_metrics_interval as u64),
            heartbeat_interval: Duration::from_millis(config.worker_heartbeat_interval as u64),
            map_output_kv_pairs: Mutex::new(HashMap::new()),
            number_of_workers: config.number_of_workers,
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

        let mut foreman_client_guard = self.foreman_client.lock().await;

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

    /// This function spawns a new thread to heartbeat to the foreman at the specified interval.
    /// The foreman responds to this heartbeat with an optional work task that this worker needs to complete.
    /// If such a task exists, the heartbeat worker thread spawns a new thread and works on this task
    /// once the map task is finished, the worker hashes each key in the map output to determine the reducer to send the key,value pair.
    pub async fn start_heartbeat_worker(&self) {
        let foreman_client_clone = self.foreman_client.clone();
        let metrics_clone = self.metrics.clone();
        let id = self.id;
        let status_clone = self.status.clone();
        let mapper_clone = self.mapper.clone();
        let interval = self.heartbeat_interval.clone();
        let data_dir_clone = self.deposit_data_dir.clone();
        let datanode_service_port_clone = self.datanode_service_port.clone();
        let num_reducers = self.number_of_workers.clone();

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
                drop(status_guard);

                let mut foreman_client_guard = foreman_client_clone.lock().await;

                info!("Sending heartbeat...");
                match foreman_client_guard.send_heart_beat(request).await {
                    Ok(response) => {
                        info!("Received heartbeat response: {:?}",response);

                        match response.into_inner().work {
                            None => {
                                info!("foreman responded with no work.");
                            }
                            Some(work) => {
                                info!("foreman responded with work.");
                                let locality = work.locality;
                                let mut buffer = vec![0; work.block_size as usize];

                                // input data is located on this datanode
                                if locality == Local {
                                    info!("Input data is local.");
                                    let local_path = format!(
                                        "{}/{}_{}.dat",
                                        data_dir_clone,
                                        work.block_id.clone(),
                                        work.seq
                                    );
                                    match tokio::fs::File::open(local_path).await {
                                        Ok(mut file) => match file.read_exact(&mut *buffer).await {
                                            Ok(bytes) => {
                                                info!("file read: {}", bytes);
                                            },
                                            Err(err) => {
                                                error!("{}", err);
                                            }
                                        },
                                        Err(err) => {
                                            error!("{}", err);
                                        }
                                    }
                                } else {
                                    info!("Input data is remote.");
                                    let hostname = work.datanode_hostname.unwrap();
                                    let datanode_addr = format!("http://{}:{}", hostname, datanode_service_port_clone);

                                    // try to open connection via the datanode client
                                    match DepositDataNodeServiceClient::connect(datanode_addr).await {
                                        Ok(mut client) => {
                                            info!("connected to remote datanode.");
                                            let get_block_request = GetBlockRequest {
                                                block_id: work.block_id,
                                            };
                                            let mut byte_offset = 0;
                                            match client.get_block_streamed(get_block_request).await {
                                                Ok(stream) => {
                                                    let mut inner_stream = stream.into_inner();
                                                    while let Ok(Some(chunk_result)) =
                                                        inner_stream.message().await.map_err(|e| {
                                                            FerrumRefineryError::TaskError(e.message().to_string())
                                                        })
                                                    {
                                                        let chunk_size = chunk_result.chunked_data.len();
                                                        if byte_offset + chunk_size > buffer.len() {
                                                            error!("buffer overflow!");
                                                        }
                                                        buffer[byte_offset..byte_offset + chunk_size]
                                                            .copy_from_slice(&chunk_result.chunked_data);
                                                        byte_offset += chunk_size;
                                                    }
                                                    info!("downloaded data from remote datanode");
                                                }
                                                Err(err) => {
                                                    error!("{}",err.to_string());
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            error!("{}", err.to_string());
                                        }
                                    }
                                }

                                // now that we have the data let's get to work
                                info!("let's get to work!");

                                let mut status_guard = status_clone.lock().await;
                                info!("setting status to busy");
                                *status_guard = Busy;


                                let kv = KeyValue {
                                    key: Bytes::from(vec![]),
                                    value: Bytes::from(buffer),
                                };
                                let mapper_guard = mapper_clone.lock().await;
                                let map_output = mapper_guard.map(kv).await;
                                info!("Total Key Value Pairs: {}", map_output.len());

                                for (key, value) in map_output.iter() {

                                    // hash the key into an integer
                                    let mut hasher = DefaultHasher::new();
                                    for byte in key.iter() {
                                        byte.hash(&mut hasher);
                                    }
                                    let hash = hasher.finish();
                                    let reducer_number = (hash as usize) % num_reducers;

                                    match foreman_client_guard.get_reducer(Request::new(GetReducerRequest {
                                        reducer: reducer_number as u32,
                                    })).await {
                                        Ok(response) => {
                                            let inner_response = response.into_inner();

                                            let reducer_hostname = inner_response.hostname;
                                            let reducer_port = inner_response.port;
                                            let reducer_addr = format!("http://{}:{}", reducer_hostname, reducer_port);
                                            // connect to reducer, and send kv pair
                                            let mut worker_service_client = WorkerServiceClient::connect(reducer_addr).await.unwrap();

                                            match worker_service_client.send_key_value_pair(Request::new(SendKeyValuePairRequest {
                                                key: key.to_vec(),
                                                value: value.to_vec(),
                                            })).await {
                                                Ok(_) => {}
                                                Err(error) => {
                                                    error!("{:?}", error);
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            error!("{:?}", error);
                                        }
                                    }
                                }
                                *status_guard = Idle;
                                drop(status_guard);
                                drop(mapper_guard);
                            }
                        }
                    },
                    Err(err) => {
                        error!("Failure occurred!: {}", err.to_string());
                    }
                }
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
                let mem_usage = (mem_info.avail / mem_info.total) * 100;
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

/// This service function shuffles the map result keys to the determined reducer based on a simple
/// hashing partition function
#[tonic::async_trait]
impl WorkerService for Worker {
    async fn send_key_value_pair(
        &self,
        request: Request<SendKeyValuePairRequest>,
    ) -> Result<Response<SendKeyValuePairResponse>, Status> {
        let inner_request = request.into_inner();
        let key = Bytes::from(inner_request.key);
        let value = Bytes::from(inner_request.value);

        let mut reducer_kv_map_guard = self.map_output_kv_pairs.lock().await;
        reducer_kv_map_guard.insert(key, value);

        Ok(Response::new(SendKeyValuePairResponse { success: true }))
    }


/// This service function reduces all key value pairs that have been shuffled to this reducer.
/// Once the reduction is complete, the reducer sends the results to the aggregator for upload back
/// into the deposit
    async fn start_reduce(&self, _request: Request<StartReduceRequest>) -> Result<Response<StartReduceResponse>, Status> {
        todo!()
    }
}
