use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::core::task::Task;




/// In memory representation of a worker node
struct Worker {
    pub id : Uuid,
    pub hostname: String,
    pub port: i32,
    pub profession: Profession,
}

/// A worker node is either a mapper or reducer
pub enum Profession {
    Mapper,
    Reducer,
}

/// Job Coordinator
pub struct Foreman {
    tasks: Arc<Mutex<HashMap<Uuid, Task>>>, // map to track tasks
    workers: Arc<Mutex<HashMap<Uuid, Worker>>> // map to track workers
}