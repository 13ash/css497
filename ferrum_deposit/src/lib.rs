pub mod block;
pub mod config;
pub mod deposit;
pub mod error;
pub mod tests;
pub mod core;

pub mod proto {
    tonic::include_proto!("ferrum_deposit.proto");
}
