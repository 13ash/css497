use crate::framework::errors::FerrumRefineryError;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct RefineryConfig {
    #[serde(rename = "foreman.hostname")]
    pub foreman_hostname: String,
    #[serde(rename = "foreman.port")]
    pub foreman_port: u16,
    #[serde(rename = "worker.data.dir")]
    pub worker_data_dir: String,
    #[serde(rename = "worker.hostname")]
    pub worker_hostname: String,
    #[serde(rename = "worker.port")]
    pub worker_port: u16,
    #[serde(rename = "deposit.namenode.hostname")]
    pub deposit_namenode_hostname: String,
    #[serde(rename = "deposit.namenode.port")]
    pub deposit_namenode_port: u16,
    #[serde(rename = "deposit.datanode.hostname")]
    pub deposit_datanode_hostname: String,
    #[serde(rename = "deposit.datanode.port")]
    pub deposit_datanode_port: u16,
}

impl RefineryConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumRefineryError> {
        let xml_str = std::fs::read_to_string(file_path)
            .map_err(|_| FerrumRefineryError::ConfigError("Xml config error".to_string()))?;
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
