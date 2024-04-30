use crate::framework::errors::FerrumRefineryError;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct RefineryConfig {
    #[serde(rename = "namenode.foreman.hostname")]
    pub namenode_foreman_hostname: String,

    #[serde(rename = "aggregator.hostname")]
    pub aggregator_hostname: String,

    #[serde(rename = "aggregator.service.port")]
    pub aggregator_service_port: u16,

    #[serde(rename = "foreman.service.port")]
    pub foreman_service_port: u16,

    #[serde(rename = "worker.service.port")]
    pub worker_service_port: u16,

    #[serde(rename = "namenode.service.port")]
    pub namenode_service_port: u16,

    #[serde(rename = "datanode.service.port")]
    pub datanode_service_port: u16,

    #[serde(rename = "worker.data.dir")]
    pub worker_data_dir: String,

    #[serde(rename = "datanode.data.dir")]
    pub datanode_data_dir: String,

    #[serde(rename = "namenode.data.dir")]
    pub namenode_data_dir: String,

    #[serde(rename = "foreman.data.dir")]
    pub foreman_data_dir: String,

    #[serde(rename = "aggregator.data.dir")]
    pub aggregator_data_dir: String,

    #[serde(rename = "worker.heartbeat.interval")]
    pub worker_heartbeat_interval: u16,

    #[serde(rename = "worker.metrics.interval")]
    pub worker_metrics_interval: u16,

    #[serde(rename = "number.of.workers")]
    pub number_of_workers: usize,
}

impl RefineryConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumRefineryError> {
        let xml_str = std::fs::read_to_string(file_path)
            .map_err(|_| FerrumRefineryError::ConfigError("Xml config error".to_string()))?;
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
