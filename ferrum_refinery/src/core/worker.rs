use crate::api::map::Mapper;
use crate::api::reduce::Reducer;
use crate::config::refinery_config::RefineryConfig;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::foreman_service_server::ForemanService;
use crate::proto::worker_service_server::WorkerService;
use crate::proto::{HeartbeatRequest, RegistrationRequest, TaskRequest, TaskResponse};
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;

use std::sync::Arc;
use std::time::Duration;
use sys_info::{loadavg, mem_info};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub enum Profession {
    Mapper,
    Reducer,
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
    pub mapper: Arc<Mutex<dyn Mapper>>,
    pub reducer: Arc<Mutex<dyn Reducer>>,
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>, // client to communicate with the hdfs deposit
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>, // client to communicate with foreman
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
            mapper: Arc::new(Mutex::new(mapper)),
            reducer: Arc::new(Mutex::new(reducer)),
            deposit_client: Arc::new(Mutex::new(
                DepositDataNodeServiceClient::connect(format!(
                    "http://{}:{}",
                    config.deposit_namenode_hostname, config.deposit_namenode_port
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

                let request = Request::new(HeartbeatRequest {
                    worker_id: id.to_string(),
                    health_metrics: Some(metrics),
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
    async fn complete_task(
        &self,
        _request: Request<TaskRequest>,
    ) -> Result<Response<TaskResponse>, Status> {
        todo!()
    }
}
