use crate::framework::errors::FerrumRefineryError;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ForemanConfig {
    pub hostname: String,
    pub port: u16,

}

impl ForemanConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumRefineryError> {
        let xml_str = std::fs::read_to_string(file_path)
            .map_err(|_| FerrumRefineryError::ConfigError("Xml config error".to_string()))?;
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
