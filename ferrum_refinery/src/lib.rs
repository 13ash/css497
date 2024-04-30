pub mod api;
pub mod config;
pub mod core;
pub mod framework;
pub mod tests;

pub mod proto {
    tonic::include_proto!("ferrum_refinery.proto");
}
