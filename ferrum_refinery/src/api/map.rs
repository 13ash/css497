use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Mapper: Send + Sync + 'static {
    async fn map(&self, kv: KeyValue) -> MapOutput;
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

pub type MapOutput = Vec<(Bytes, Bytes)>;
