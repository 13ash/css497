use async_trait::async_trait;
use ferrum_deposit::config::deposit_config::DepositConfig;
use ferrum_deposit::deposit::ferrum_deposit_client::FerrumDepositClient;
use ferrum_refinery::api::map::{AsyncMapper};
use ferrum_refinery::api::reduce::Reducer;
use ferrum_refinery::framework::refinery::RefineryBuilder;

struct WordCounter;

#[async_trait]
impl AsyncMapper<usize, String, String, i32> for WordCounter {
    async fn map(&self, _key: usize, value: String) -> Vec<(String, i32)> {
        let word_counts: Vec<(String, i32)> = value
            .split_whitespace()
            .map(|word| (word.to_string(), 1))
            .collect();
        word_counts
    }
}

struct WordCountReducer;

impl Reducer<String, i32, String, i32> for WordCountReducer {
    fn reduce(&self, key: String, values: Vec<i32>) -> Vec<(String, i32)> {
        let count = values.iter().sum();
        vec![(key, count)]
    }
}

fn main() {
    // configure the deposit client

    let deposit = FerrumDepositClient::from_config(DepositConfig {
        data_dir: "local_data_dir".to_string(),
        namenode_address: "namenode:50000".to_string(),
    });

    // create a refinery
    let refinery = RefineryBuilder::new()
        .with_deposit(deposit)
        .with_input_location("deposit://path/to/input".to_string())
        .with_output_location("deposit://path/to/output".to_string())
        .with_mappers(vec![
            "hostname:port".to_string(),
            "hostname.port".to_string(),
        ])
        .with_reducers(vec!["hostname:port".to_string()])
        .build()
        .unwrap();

    // create a mapper
    let word_counter_mapper = WordCounter;

    // create a reducer
    let word_count_reducer = WordCountReducer;

    // submit a job to the refinery
    refinery
        .refine(word_counter_mapper, word_count_reducer)
        .unwrap();
}
