use crate::proto::aggregation_service_server::AggregationService;
use crate::proto::{SendResultResponse, TaskResult};
use tonic::{Request, Response, Status};

pub struct Aggregator {}

#[tonic::async_trait]
impl AggregationService for Aggregator {
    async fn send_result(
        &self,
        _request: Request<TaskResult>,
    ) -> Result<Response<SendResultResponse>, Status> {
        todo!()
    }
}
