use crate::framework::errors::FerrumRefineryError;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct WorkerConfig {
    pub hostname: String,
    pub port: u16,
    #[serde(rename = "data.dir")]
    pub data_dir : String,
    #[serde(rename = "namenode.hostname")]
    pub namenode_hostname: String,
    #[serde(rename = "namenode.port")]
    pub namenode_port : u16,

    #[serde(rename = "foreman.hostname")]
    pub foreman_hostname : String,

    #[serde(rename = "foreman.port")]
    pub foreman_port : u16,
}

impl WorkerConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumRefineryError> {
        let xml_str = std::fs::read_to_string(file_path)
            .map_err(|_| FerrumRefineryError::ConfigError("Xml config error".to_string()))?;
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
