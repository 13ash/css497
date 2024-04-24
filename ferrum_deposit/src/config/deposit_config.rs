use crate::error::FerrumDepositError;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DepositConfig {
    #[serde(rename = "namenode.data.dir")]
    pub namenode_data_dir: String,
    #[serde(rename = "datanode.data.dir")]
    pub datanode_data_dir: String,
    #[serde(rename = "namenode.hostname")]
    pub namenode_hostname: String,
    #[serde(rename = "deposit.client.data.dir")]
    pub deposit_client_data_dir: String,
    #[serde(rename = "datanode.block.report.interval")]
    pub datanode_block_report_interval: u64,
    #[serde(rename = "datanode.heartbeat.interval")]
    pub datanode_heartbeat_interval: u64,
    #[serde(rename = "datanode.metrics.interval")]
    pub datanode_metrics_interval: u64,
    #[serde(rename = "namenode.edit.log.flush.interval")]
    pub namenode_edit_log_flush_interval: u64,
    #[serde(rename = "datanode.service.port")]
    pub datanode_service_port: u16,
    #[serde(rename = "namenode.service.port")]
    pub namenode_service_port: u16,
    #[serde(rename = "replication.factor")]
    pub replication_factor: u8,
}

impl DepositConfig {
    pub fn from_xml_file(file_path: &str) -> Result<Self, FerrumDepositError> {
        let xml_str = std::fs::read_to_string(file_path).unwrap();
        let config = serde_xml_rs::from_str(&xml_str)?;
        Ok(config)
    }
}
