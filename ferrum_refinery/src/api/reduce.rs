use async_trait::async_trait;

#[async_trait]
pub trait AsyncReducer<RK1, RV1, RK2, RV2>: Send + Sync + 'static {
    async fn reduce(&self, key: RK1, values: Vec<RV1>) -> Vec<(RK2, RV2)>;
}
