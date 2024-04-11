use async_trait::async_trait;

#[async_trait]
pub trait AsyncMapper<ItemKey, ItemValue, IntermediateKey, IntermediateValue>: Sync {
    async fn map(
        &self,
        key: ItemKey,
        value: ItemValue,
    ) -> Vec<(IntermediateKey, IntermediateValue)>;
}