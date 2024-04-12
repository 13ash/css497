use crate::api::map::AsyncMapper;
use async_trait::async_trait;
use ferrum_deposit::block::BLOCK_SIZE;
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use ferrum_deposit::proto::GetBlockRequest;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum TaskType {
    Map,
    Reduce,
}

#[async_trait]
pub trait Task: Send + Sync {
    async fn execute(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct MapTask<K1, V1, K2, V2> {
    pub task_id: Uuid,    // gets task_id from the job
    pub job_id: Uuid,     // overall job id for this task
    pub block_id: String, // gets the block_id on which to operate
    pub mapper: Arc<Mutex<dyn AsyncMapper<K1, V1, K2, V2>>>,
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>,
}

#[async_trait]

impl<K1, V1, K2, V2> Task for MapTask<K1, V1, K2, V2> {
    async fn execute(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _block_buffer = vec![0; BLOCK_SIZE];

        let deposit_request = GetBlockRequest {
            block_id: self.block_id.clone(),
        };

        let mut deposit_client_guard = self.deposit_client.lock().await;

        let mut stream = deposit_client_guard
            .get_block_streamed(deposit_request)
            .await?
            .into_inner();

        while let Some(_block_chunk) = stream.message().await? {
            // add to block buffer
        }

        todo!()
    }
}
