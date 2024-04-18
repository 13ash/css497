use crate::framework::errors::FerrumRefineryError;
use crate::proto::foreman_service_client::ForemanServiceClient;
use crate::proto::CreateJobRequest;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub struct Refinery {
    pub input_location: String,
    pub output_location: String,
    pub foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>,
}

impl Refinery {
    pub fn new(
        input_location: &str,
        output_location: &str,
        foreman_client: Arc<Mutex<ForemanServiceClient<Channel>>>,
    ) -> Self {
        Refinery {
            input_location: input_location.to_string(),
            output_location: output_location.to_string(),
            foreman_client,
        }
    }
    pub async fn refine(&self) -> Result<(), FerrumRefineryError> {
        // signal the foreman to create the job
        let create_job_request = CreateJobRequest {
            input_data: self.input_location.clone(),
            output_data: self.output_location.clone(),
        };
        let response = self
            .foreman_client
            .lock()
            .await
            .create_job(create_job_request)
            .await;
        match response {
            Ok(_success) => Ok(()),
            Err(error) => Err(FerrumRefineryError::JobCreationError(
                error.message().to_string(),
            )),
        }
    }
}
