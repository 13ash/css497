use async_trait::async_trait;

/// AsyncMapper is used to perform IO-bound / concurrent tasks
/// large datasets that require reading portions of the data into memory
#[async_trait]
pub trait AsyncMapper<ItemKey,ItemValue,IntermediateKey,IntermediateValue> {

    async fn map(&self, key: ItemKey, value: ItemValue) -> Vec<(IntermediateKey, IntermediateValue)>;
}

// Mapper is used to perform CPU-bound jobs, entire dataset is already in memory
pub trait Mapper<ItemKey,ItemValue,IntermediateKey,IntermediateValue> {
    fn map(&self, key: ItemKey, value: ItemValue) -> Vec<(IntermediateKey, IntermediateValue)>;
}
