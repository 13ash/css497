use crate::config::refinery_config::RefineryConfig;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::worker_service_server::WorkerService;
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;
use crate::api::map::{KeyValue, Mapper};
use crate::api::reduce::Reducer;
use crate::proto::TaskRequest;

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
    pub poll_interval: Duration,
    pub metrics: Arc<Mutex<HealthMetrics>>,
    pub mapper: Arc<Mutex<dyn Mapper>>,
    pub reducer: Arc<Mutex<dyn Reducer>>,
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>, // client to communicate with the hdfs deposit
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>, // client to communicate with foreman
}

impl Worker {
    pub async fn new(
        config: RefineryConfig,
        mapper: impl Mapper,
        reducer: impl Reducer,
    ) -> Self {
        Worker {
            id: Uuid::new_v4(),
            hostname: config.worker_hostname,
            port: config.worker_port,
            poll_interval: Duration::from_millis(config.worker_poll_interval),
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


    pub async fn start_polling(&self) {

        let id = self.id.to_string().clone();
        let metrics_clone = self.metrics.clone();
        let foreman_client = self.foreman_client.clone();
        let interval = self.poll_interval.clone();
        let mapper = self.mapper.clone();

        // spawn a thread and pass these references into the closure
        tokio::spawn(async move {

            // set the interval
            let mut tokio_interval = tokio::time::interval(interval);
            loop {
                // tick the interval
                tokio_interval.tick().await;

                // grab the current metrics
                let metrics_guard = metrics_clone.lock().await;

                // create the request
                let request = Request::new(TaskRequest {
                    worker_id: id.clone(),
                    health_metrics: Some(crate::proto::HealthMetrics {
                        cpu_load: metrics_guard.await.cpu_load,
                        memory_usage: metrics_guard.lock().await.memory_usage
                    })
                });


                // drop the guard
                drop(metrics_guard);

                // get the response from the foreman
                // contains either a map task or a reduce task
                let response = foreman_client.lock().await.poll_for_task(request).await;

                match response {
                    Ok(task_response) => {
                        let inner_response = task_response.into_inner();
                         if inner_response.map.is_some() {
                             let mapper_guard = mapper.lock().await;
                             mapper_guard.map(KeyValue {
                                 key: ,
                                 value:
                             })
                         }
                    }
                    Err(err) => {}
                }
            }



        });
    }
}

#[tonic::async_trait]
impl WorkerService for Worker {

}
`