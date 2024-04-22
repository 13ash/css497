use crate::proto::aggregation_service_server::AggregationService;
use crate::proto::{SendResultResponse, TaskResult};
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::config::refinery_config::RefineryConfig;

pub struct Aggregator {
    pub id: Uuid,
    pub hostname: String,
    pub port: u16,
    pub data_dir: String

}

impl Aggregator {
    pub async fn from_config(config: RefineryConfig) -> Self {
        Aggregator {
            id: Uuid::new_v4(),
            hostname: config.aggregator_hostname,
            port: config.aggregator_service_port,
            data_dir: config.aggregator_data_dir,
        }
    }
}

#[tonic::async_trait]
impl AggregationService for Aggregator {
    async fn send_result(
        &self,
        request: Request<TaskResult>,
    ) -> Result<Response<SendResultResponse>, Status> {
        todo!()
    }
}
