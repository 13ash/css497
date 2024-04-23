use crate::core::namenode::INode;
use std::sync::{PoisonError, RwLockWriteGuard};
use tonic::Status;

#[derive(Debug, PartialEq)]
pub enum FerrumDepositError {
    DiskError(String),
    ConfigError(String),
    ConnectionError(String),
    ChecksumError(String),
    TonicError(String),
    ProtoError(String),
    PutBlockStreamedError(String),
    GetBlockStreamedError(String),
    FileSystemError(String),
    InvalidPathError(String),
    IOError(String),
    InsufficientSpace(String),
    InsufficientDataNodes(String),
    HeartBeatFailed(String),
    RegistrationError(String),
    LockError(String),
    BlockMapError(String),
    MetricsError(String),
    ReadError(String),
    WriteError(String),
    DataValidationError(String),
    GrpcError(String),
    StreamError(String),
    EditLogError(String),
    UUIDError(String),
    BlockReportError(String),
    GetError(String),
    PutError(String),
}

impl From<tonic::transport::Error> for FerrumDepositError {
    fn from(error: tonic::transport::Error) -> Self {
        FerrumDepositError::TonicError(error.to_string())
    }
}

impl From<toml::de::Error> for FerrumDepositError {
    fn from(error: toml::de::Error) -> Self {
        FerrumDepositError::ConfigError(error.to_string())
    }
}

impl From<PoisonError<RwLockWriteGuard<'_, INode>>> for FerrumDepositError {
    fn from(error: PoisonError<RwLockWriteGuard<'_, INode>>) -> FerrumDepositError {
        FerrumDepositError::LockError(format!("Lock error: {}", error.to_string()))
    }
}

impl From<std::io::Error> for FerrumDepositError {
    fn from(error: std::io::Error) -> Self {
        FerrumDepositError::IOError(error.to_string())
    }
}

impl From<prost::EncodeError> for FerrumDepositError {
    fn from(error: prost::EncodeError) -> Self {
        FerrumDepositError::ProtoError(error.to_string())
    }
}

impl From<serde_xml_rs::Error> for FerrumDepositError {
    fn from(error: serde_xml_rs::Error) -> Self {
        FerrumDepositError::ConfigError(error.to_string())
    }
}

impl From<prost::DecodeError> for FerrumDepositError {
    fn from(error: prost::DecodeError) -> Self {
        FerrumDepositError::ProtoError(error.to_string())
    }
}

impl From<FerrumDepositError> for Status {
    fn from(error: FerrumDepositError) -> Self {
        match error {
            FerrumDepositError::InvalidPathError(msg) => Status::invalid_argument(msg),
            FerrumDepositError::FileSystemError(msg) => Status::internal(msg),
            FerrumDepositError::LockError(msg) => Status::aborted(msg),
            FerrumDepositError::PutBlockStreamedError(msg) => Status::internal(msg),
            FerrumDepositError::GetBlockStreamedError(msg) => Status::internal(msg),
            FerrumDepositError::PutError(msg) => Status::internal(msg),
            FerrumDepositError::IOError(msg) => Status::internal(msg),
            _ => Status::unknown("Unknown error"),
        }
    }
}

pub type Result<T> = std::result::Result<T, FerrumDepositError>;
