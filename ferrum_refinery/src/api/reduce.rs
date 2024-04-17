use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Reducer : Send + Sync + 'static {
    async fn reduce(&self, key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>) -> anyhow::Result<Bytes>;
}
