use crate::error::FerrumDepositError;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DepositConfig {
    #[serde(rename = "data.dir")]
    pub data_dir: String,

    #[serde(rename = "namenode.address")]
    pub namenode_address: String,
}

impl DepositConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumDepositError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap();
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
