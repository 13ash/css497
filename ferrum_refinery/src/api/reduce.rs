use crate::api::map::MapOutput;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Reducer: Send + Sync + 'static {
    async fn reduce(&self, map_output: MapOutput) -> Result<Bytes>;
}
