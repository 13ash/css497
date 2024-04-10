use async_trait::async_trait;

#[async_trait]
pub trait AsyncReducer<IntermediateKey, IntermediateValue, FinalKey, FinalValue> {
    async fn reduce(&self, key: IntermediateKey, values: Vec<IntermediateValue>) -> Vec<(FinalKey, FinalValue)>;
}


pub trait Reducer<IntermediateKey, IntermediateValue, FinalKey, FinalValue> {
    fn reduce(&self, key: IntermediateKey, values: Vec<IntermediateValue>) -> Vec<(FinalKey, FinalValue)>;
}
