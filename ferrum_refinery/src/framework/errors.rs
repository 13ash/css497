use serde_xml_rs::Error;
use tonic::Status;

#[derive(Debug, PartialEq)]
pub enum FerrumRefineryError {
    ConfigError(String),
    DepositClientError(String),
    TransportError(String),
    JobCreationError(String),
    FinishShuffleError(String),
    AggregationError(String),
    TaskError(String),
    UuidError(String),
    RegistrationError(String),
    HeartbeatError(String),
    GetReducerError(String),
    BufferOverflow,
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

impl From<FerrumRefineryError> for Status {
    fn from(error: FerrumRefineryError) -> Self {
        match error {
            FerrumRefineryError::TransportError(msg) => Status::internal(msg),
            FerrumRefineryError::JobCreationError(msg) => Status::internal(msg),
            FerrumRefineryError::HeartbeatError(msg) => Status::internal(msg),
            FerrumRefineryError::GetReducerError(msg) => Status::internal(msg),
            FerrumRefineryError::FinishShuffleError(msg) => Status::internal(msg),
            _ => Status::unknown("Unknown error"),
        }
    }
}

pub type Result<T> = std::result::Result<T, FerrumRefineryError>;
