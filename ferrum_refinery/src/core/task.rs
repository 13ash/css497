use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum TaskType {
    Map,
    Reduce,
}

pub enum TaskStatus {
    Running,
    Pending,
    Completed,
    Failed,
}

pub enum InputType {
    InMemory(Vec<u8>),
    FilePath(PathBuf),
}

/// A task represents a unit of computation within a Job
///
/// A single Job has many tasks, which are executed across many nodes
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: Uuid,
    pub job_id: Uuid,
    pub task_type: TaskType,
}
