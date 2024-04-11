use crate::api::reduce::Reducer;
use crate::core::errors::JobError;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Instant;
use uuid::Uuid;
use crate::api::map::AsyncMapper;

pub enum JobStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// A Job represents the highest level of abstraction over the MapReduce framework
/// A Job has all necessary map and reduce information, that it uses to split into tasks
pub struct Job<InitialKey, InitialValue, IntermediateKey, InterMediateValue, FinalKey, FinalValue> {
    pub id: Uuid,
    pub output_location: PathBuf,
    pub mapper_function:
        Arc<dyn AsyncMapper<InitialKey, InitialValue, IntermediateKey, InterMediateValue>>,
    pub reducer_function: Arc<dyn Reducer<IntermediateKey, InitialValue, FinalKey, FinalValue>>,
    pub num_mappers: usize,
    pub num_reducers: usize,
    pub status: JobStatus,
    pub start_time: Option<Instant>,
    pub end_time: Option<Instant>,
    pub error: JobError,
}

impl<InitialKey, InitialValue, IntermediateKey, IntermediateValue, FinalKey, FinalValue>
    Job<InitialKey, InitialValue, IntermediateKey, IntermediateValue, FinalKey, FinalValue>
{
    pub fn new(
        output_location: PathBuf,
        mapper_function: Arc<
            dyn AsyncMapper<InitialKey, InitialValue, IntermediateKey, IntermediateValue>,
        >,
        reducer_function: Arc<dyn Reducer<IntermediateKey, InitialValue, FinalKey, FinalValue>>,
        num_mappers: usize,
        num_reducers: usize,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            output_location,
            mapper_function,
            reducer_function,
            num_mappers,
            num_reducers,
            status: JobStatus::Pending,
            start_time: None,
            end_time: None,
            error: JobError::None,
        }
    }
}
