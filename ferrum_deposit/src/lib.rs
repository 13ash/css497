pub mod block;
pub mod config;
pub mod datanode;
pub mod deposit;
pub mod error;
pub mod namenode;
pub mod tests;

pub mod proto {
    tonic::include_proto!("ferrum_deposit.proto");
}
