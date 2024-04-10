use serde_xml_rs::Error;

#[derive(Debug, PartialEq)]
pub enum FerrumRefineryError {
    ConfigError(String),
    DepositClientError(String),
    TransportError(String),
}


impl From<Error> for FerrumRefineryError {
    fn from(value: Error) -> Self {
        FerrumRefineryError::ConfigError(value.to_string())
    }
}

impl From<tonic::transport::Error> for FerrumRefineryError {
    fn from(value: tonic::transport::Error) -> Self {
        FerrumRefineryError::TransportError(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, FerrumRefineryError>;
