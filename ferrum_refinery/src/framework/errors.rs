
#[derive(Debug, PartialEq)]
pub enum RefineryError {
    ConfigError(String),
    DepositClientError(String)
}