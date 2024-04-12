use ferrum_refinery::framework::refinery::RefineryBuilder;

fn main() {
    let refinery = RefineryBuilder::new()
        .with_reducers(vec!["reducers".to_string()])
        .with_mappers(vec!["mappers".to_string()])
        .with_input_location("deposit://path/to/input".to_string())
        .with_output_location("deposit://path/to/output".to_string())
        .build()
        .unwrap();
}
