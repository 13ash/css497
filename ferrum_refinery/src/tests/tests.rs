// #[cfg(test)]
// mod refinery_tests {
//     use std::fs::File;
//     use std::sync::Arc;
//     use std::time::Duration;
//     use async_trait::async_trait;
//     use bytes::Bytes;
//     use tokio::io::AsyncReadExt;
//     use tokio_test::assert_err;
//     use tonic::transport::Server;
//     use ferrum_deposit::config::deposit_config::DepositConfig;
//     use ferrum_deposit::core::datanode::DataNode;
//     use ferrum_deposit::core::namenode::NameNode;
//     use ferrum_deposit::deposit::ferrum_deposit_client::{Client, FerrumDepositClient};
//     use ferrum_deposit::proto::deposit_data_node_service_server::DepositDataNodeServiceServer;
//     use ferrum_deposit::proto::deposit_name_node_service_client::DepositNameNodeServiceClient;
//     use ferrum_deposit::proto::deposit_name_node_service_server::DepositNameNodeServiceServer;
//     use ferrum_deposit::proto::PutFileRequest;
//     use crate::api::map::{KeyValue, MapOutput, Mapper};
//     use crate::api::reduce::Reducer;
//     use crate::config::refinery_config::RefineryConfig;
//     use crate::core::aggregator::Aggregator;
//     use crate::core::foreman::Foreman;
//     use crate::core::worker::Worker;
//     use crate::proto::aggregation_service_server::AggregationServiceServer;
//     use crate::proto::CreateJobRequest;
//     use crate::proto::foreman_service_client::ForemanServiceClient;
//     use crate::proto::foreman_service_server::ForemanServiceServer;
//     use crate::proto::worker_service_server::WorkerServiceServer;
//
//     const LOCALHOST_IPV4: &str = "127.0.0.1";
//
//     const NAMENODE_SERVICE_PORT: u16 = 50000;
//     const FOREMAN_SERVICE_PORT: u16 = 40000;
//     const AGGREGATOR_SERVICE_PORT: u16 = 60000;
//     const DATANODE_BLOCK_REPORT_INTERVAL: u64 = 300000;
//     const DATANODE_HEARTBEAT_INTERVAL: u64 = 3000;
//     const DATANODE_METRICS_INTERVAL: u64 = 10000;
//     const NAMENODE_EDIT_LOG_FLUSH_INTERVAL: u64 = 30000;
//     const REPLICATION_FACTOR: u8 = 3;
//
//     const DEPOSIT_CLIENT_DATA_DIR: &str = "src/tests/deposit/client/data";
//     const AGGREGATOR_DATA_DIR: &str = "src/tests/refinery/aggregator/data";
//     const FOREMAN_DATA_DIR: &str = "src/tests/refinery/foreman/data";
//     const WORKER_ONE_DATA_DIR: &str = "src/tests/refinery/worker1/data";
//     const WORKER_TWO_DATA_DIR: &str = "src/tests/refinery/worker2/data";
//     const WORKER_THREE_DATA_DIR: &str = "src/tests/refinery/worker3/data";
//     const DATANODE_ONE_DATA_DIR: &str = "src/tests/deposit/datanode1/data";
//     const DATANODE_TWO_DATA_DIR: &str = "src/tests/deposit/datanode2/data";
//     const DATANODE_THREE_DATA_DIR: &str = "src/tests/deposit/datanode3/data";
//     const NAMENODE_DATA_DIR: &str = "src/tests/deposit/namenode/data";
//     const DEPOSIT_TEST_FILE_PATH: &str = "/tests/test_file.txt";
//     const DEPOSIT_TEST_FILE_OUTPUT_PATH: &str = "/results/test_file.txt";
//     const TEST_FILE: &str = "test_file.txt";
//
//     struct TestMapper;
//
//     struct TestReducer;
//
//     #[async_trait]
//     impl Mapper for TestMapper {
//         async fn map(&self, _kv: KeyValue) -> MapOutput {
//             todo!()
//         }
//     }
//
//     #[async_trait]
//     impl Reducer for TestReducer {
//         async fn reduce(&self, _map_output: MapOutput) -> anyhow::Result<Bytes> {
//             todo!()
//         }
//     }
//
//     async fn start_namenode(namenode: Arc<NameNode>, port: u16) {
//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(DepositNameNodeServiceServer::new(namenode.clone()))
//                 .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
//                 .await
//         });
//     }
//
//     async fn start_datanode(datanode: DataNode, port: u16) {
//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(DepositDataNodeServiceServer::new(datanode))
//                 .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
//                 .await
//         });
//     }
//
//     async fn start_foreman(foreman: Foreman, port: u16) {
//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(ForemanServiceServer::new(foreman))
//                 .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
//                 .await
//         });
//     }
//
//     async fn start_worker(worker: Worker, port: u16) {
//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(WorkerServiceServer::new(worker))
//                 .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
//                 .await
//         });
//     }
//
//     async fn start_aggregator(aggregator: Aggregator, port: u16) {
//         tokio::spawn(async move {
//             Server::builder()
//                 .add_service(AggregationServiceServer::new(aggregator))
//                 .serve(format!("{}:{}", LOCALHOST_IPV4, port).parse().unwrap())
//                 .await
//         });
//     }
//
//     async fn setup_test_environment() {
//         // start the namenode service
//         let namenode_config = create_deposit_config();
//         let namenode = NameNode::from_config(namenode_config.clone()).await.unwrap();
//         let arc_namenode = Arc::new(namenode);
//
//         start_namenode(arc_namenode, namenode_config.namenode_service_port.clone()).await;
//         tokio::time::sleep(Duration::from_millis(1000)).await;
//
//
//         // start the datanode services
//         let mut datanode_one_config = create_deposit_config();
//         datanode_one_config.datanode_data_dir = DATANODE_ONE_DATA_DIR.to_string();
//         datanode_one_config.datanode_service_port = 50001;
//
//         let mut datanode_two_config = create_deposit_config();
//         datanode_two_config.datanode_data_dir = DATANODE_TWO_DATA_DIR.to_string();
//         datanode_two_config.datanode_service_port = 50002;
//
//
//         let mut datanode_three_config = create_deposit_config();
//         datanode_three_config.datanode_data_dir = DATANODE_THREE_DATA_DIR.to_string();
//         datanode_three_config.datanode_service_port = 50003;
//
//
//         let datanode_one = DataNode::from_config(datanode_one_config.clone()).await;
//         start_datanode(datanode_one, datanode_one_config.datanode_service_port.clone()).await;
//
//
//         let datanode_two = DataNode::from_config(datanode_two_config.clone()).await;
//         start_datanode(datanode_two, datanode_two_config.datanode_service_port.clone()).await;
//
//
//         let datanode_three = DataNode::from_config(datanode_three_config.clone()).await;
//         start_datanode(datanode_three, datanode_three_config.datanode_service_port.clone()).await;
//
//
//         // start the foreman service
//
//         let foreman_config = create_refinery_config();
//         let foreman_port = foreman_config.foreman_service_port;
//
//         let foreman = Foreman::from_config(foreman_config).await.unwrap();
//         start_foreman(foreman, foreman_port).await;
//
//
//         // start the aggregation service
//         let aggregator_config = create_refinery_config();
//         let aggregator = Aggregator::from_config(aggregator_config.clone()).await;
//         start_aggregator(aggregator, aggregator_config.aggregator_service_port.clone()).await;
//
//
//         // instantiate the mapper and reducer trait objects
//         let mapper_one = TestMapper;
//         let reducer_one = TestReducer;
//
//         let mapper_two = TestMapper;
//         let reducer_two = TestReducer;
//
//         let mapper_three = TestMapper;
//         let reducer_three = TestReducer;
//
//         // start the worker services
//
//         let mut worker_one_config = create_refinery_config();
//         worker_one_config.worker_data_dir = WORKER_ONE_DATA_DIR.to_string();
//         worker_one_config.worker_service_port = 40001;
//
//         let mut worker_two_config = create_refinery_config();
//         worker_two_config.worker_data_dir = WORKER_TWO_DATA_DIR.to_string();
//         worker_two_config.worker_service_port = 40002;
//
//
//         let mut worker_three_config = create_refinery_config();
//         worker_three_config.worker_data_dir = WORKER_THREE_DATA_DIR.to_string();
//         worker_three_config.worker_service_port = 40003;
//
//
//         let worker_one = Worker::new(worker_one_config.clone(), mapper_one, reducer_one).await;
//         start_worker(worker_one, worker_one_config.worker_service_port.clone()).await;
//
//
//         let worker_two = Worker::new(worker_two_config.clone(), mapper_two, reducer_two).await;
//         start_worker(worker_two, worker_two_config.worker_service_port.clone()).await;
//
//
//         let worker_three = Worker::new(worker_three_config.clone(), mapper_three, reducer_three).await;
//         start_worker(worker_three, worker_three_config.worker_service_port.clone()).await;
//
//
//         tokio::time::sleep(Duration::from_millis(5000)).await;
//
//
//         // put the file into the deposit
//         let _local_file = File::open(format!("{}/{}", DEPOSIT_CLIENT_DATA_DIR, TEST_FILE)).unwrap();
//         let namenode_service_address = format!("http://{}:{}", LOCALHOST_IPV4, NAMENODE_SERVICE_PORT);
//         let mut deposit_namenode_client = DepositNameNodeServiceClient::connect(namenode_service_address).await.unwrap();
//
//         let mut test_file_buffer = vec![];
//         let size = tokio::fs::File::open(format!("{}/{}", DEPOSIT_CLIENT_DATA_DIR, TEST_FILE)).await.unwrap().read_to_end(&mut test_file_buffer).await.unwrap();
//         let put_file_request = PutFileRequest {
//             path: DEPOSIT_TEST_FILE_PATH.to_string(),
//             file_size: size as u64,
//         };
//
//         // put the test file into the deposit namespace
//         let response = deposit_namenode_client.put_file(put_file_request).await.unwrap();
//
//         // stream the file blocks to the datanodes
//         let deposit_client_config = create_deposit_config();
//         let deposit_client = FerrumDepositClient::from_config(deposit_client_config);
//         let file = tokio::fs::File::open(format!("{}/{}", DEPOSIT_CLIENT_DATA_DIR, TEST_FILE)).await.unwrap();
//         let _result = deposit_client.put(file, response.into_inner()).await.unwrap();
//     }
//
//     fn create_refinery_config() -> RefineryConfig {
//         RefineryConfig {
//             namenode_foreman_hostname: LOCALHOST_IPV4.to_string(),
//             aggregator_hostname: LOCALHOST_IPV4.to_string(),
//             aggregator_service_port: AGGREGATOR_SERVICE_PORT,
//             foreman_service_port: FOREMAN_SERVICE_PORT,
//             worker_service_port: 0, // change based on worker
//             namenode_service_port: NAMENODE_SERVICE_PORT,
//             datanode_service_port: 0, // change based on datanode,
//             worker_data_dir: "".to_string(), // change based on worker
//             datanode_data_dir: "".to_string(), // change based on datanode
//             namenode_data_dir: NAMENODE_DATA_DIR.to_string(),
//             foreman_data_dir: FOREMAN_DATA_DIR.to_string(),
//             aggregator_data_dir: AGGREGATOR_DATA_DIR.to_string(),
//             worker_heartbeat_interval: 5000,
//             worker_metrics_interval: 10000,
//             number_of_workers: 3,
//         }
//     }
//
//     fn create_deposit_config() -> DepositConfig {
//         DepositConfig {
//             namenode_data_dir: NAMENODE_DATA_DIR.to_string(),
//             datanode_data_dir: "".to_string(), // change based on datanode
//             namenode_hostname: LOCALHOST_IPV4.to_string(),
//             deposit_client_data_dir: DEPOSIT_CLIENT_DATA_DIR.to_string(),
//             datanode_block_report_interval: DATANODE_BLOCK_REPORT_INTERVAL,
//             datanode_heartbeat_interval: DATANODE_HEARTBEAT_INTERVAL,
//             datanode_metrics_interval: DATANODE_METRICS_INTERVAL,
//             namenode_edit_log_flush_interval: NAMENODE_EDIT_LOG_FLUSH_INTERVAL,
//             datanode_service_port: 0, // change based on datanode
//             namenode_service_port: NAMENODE_SERVICE_PORT,
//             replication_factor: REPLICATION_FACTOR,
//         }
//     }
//
//     /// foreman receives a job from the refinery, shards the job into map tasks, and places them
//     /// inside the task queue
//     #[tokio::test]
//     async fn create_job_expects_success() {
//
//         setup_test_environment().await;
//
//         // create the job request
//         let create_job_request = CreateJobRequest {
//             input_data: DEPOSIT_TEST_FILE_PATH.to_string(),
//             output_data: DEPOSIT_TEST_FILE_OUTPUT_PATH.to_string(),
//         };
//
//         // submit the map reduce job to the foreman
//         let foreman_service_address = format!("http://{}:{}", LOCALHOST_IPV4, FOREMAN_SERVICE_PORT);
//         let mut foreman_client = ForemanServiceClient::connect(foreman_service_address).await.unwrap();
//         let result = foreman_client.create_job(create_job_request).await;
//
//         assert_err!(result);
//     }
// }
