pub mod block;
pub mod config;
pub mod core;
pub mod deposit;
pub mod error;

pub mod proto {
    tonic::include_proto!("ferrum_deposit.proto");
}
