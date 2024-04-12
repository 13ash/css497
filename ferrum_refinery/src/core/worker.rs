use crate::api::map::AsyncMapper;
use crate::api::reduce::AsyncReducer;
use crate::config::refinery_config::RefineryConfig;
use crate::core::task::MapTask;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::worker_service_server::WorkerService;
use crate::proto::{MapTaskRequest, MapTaskResponse};
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub enum Profession {
    Mapper,
    Reducer,
}
pub struct Worker<MK1, MV1, MK2, MV2, RK1, RV1, RK2, RV2> {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub mapper: Arc<Mutex<dyn AsyncMapper<MK1, MV1, MK2, MV2> + Send + Sync>>,
    pub reducer: Arc<Mutex<dyn AsyncReducer<RK1, RV1, RK2, RV2> + Send + Sync>>,
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>, // client to communicate with the hdfs deposit
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>, // client to communicate with foreman
}

impl<MK1, MV1, MK2, MV2, RK1, RV1, RK2, RV2> Worker<MK1, MV1, MK2, MV2, RK1, RV1, RK2, RV2> {
    pub async fn new(
        config: RefineryConfig,
        mapper: impl AsyncMapper<MK1, MV1, MK2, MV2>,
        reducer: impl AsyncReducer<RK1, RV1, RK2, RV2>,
    ) -> Self {
        Worker {
            id: Uuid::new_v4(),
            hostname: config.worker_hostname,
            port: config.worker_port,
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
}

#[tonic::async_trait]
impl<
        MK1: 'static,
        MV1: 'static,
        MK2: 'static,
        MV2: 'static,
        RK1: 'static,
        RV1: 'static,
        RK2: 'static,
        RV2: 'static,
    > WorkerService for Worker<MK1, MV1, MK2, MV2, RK1, RV1, RK2, RV2>
{
    async fn start_map_task(
        &self,
        request: Request<MapTaskRequest>,
    ) -> Result<Response<MapTaskResponse>, Status> {
        todo!()
    }
}
