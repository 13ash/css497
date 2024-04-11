use std::error::Error;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;
use ferrum_deposit::proto::deposit_data_node_service_client::DepositDataNodeServiceClient;
use ferrum_deposit::proto::GetBlockRequest;
use crate::api::map::AsyncMapper;



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
pub struct MapTask<InitialKey, InitialValue, IntermediateKey, IntermediateValue> {
    pub task_id: Uuid, // gets task_id from the job
    pub job_id: Uuid,  // overall job id for this task
    pub block_id: String, // gets the block_id on which to operate
    pub key: InitialKey,
    pub value: InitialValue,
    pub deposit_client: Arc<Mutex<DepositDataNodeServiceClient<Channel>>>,
    pub mapper: Arc<dyn AsyncMapper<InitialKey, InitialValue, IntermediateKey, IntermediateValue> + Sync + Send>,
}

#[async_trait]
impl<InitialKey, InitialValue, IntermediateKey, IntermediateValue> Task for MapTask<InitialKey, InitialValue, IntermediateKey, IntermediateValue>
    where
        InitialKey: Send + Sync + Clone + 'static,
        InitialValue: Send + Sync + Clone + 'static,
        IntermediateKey: Send + Sync + Clone + 'static,
        IntermediateValue: Send + Sync + Clone + 'static,
{
    async fn execute(&self) -> Result<(), Box<dyn Error + Send + Sync>> {

        let deposit_request = GetBlockRequest {
            block_id
        };

        let mut deposit_client_guard = self.deposit_client.lock().await;

        let mut stream = deposit_client_guard.get_block_streamed(deposit_request).await?.into_inner();

        let result : Vec<(IntermediateKey, IntermediateValue)>= self.mapper.map(self.key.clone(), self.value.clone()).await;

        todo!()

    }
}