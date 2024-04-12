use async_trait::async_trait;

#[async_trait]
pub trait AsyncMapper<K1, V1, K2, V2>: Send + Sync + 'static {
    async fn map(&self, key: K1, value: V1) -> Vec<(K2, V2)>;
}
