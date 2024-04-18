#[tokio::main]
async fn main() -> Result<()> {
    let config = RefineryConfig::from_xml_file("config/refinery.xml")?;

    let foreman = Foreman::from_config(config).await?;
    let addr = format!("{}:{}", foreman.hostname, foreman.port);

    let now = Local::now();
    println!("Time: {}", now.format("%Y-%m-%d %H:%M:%S"));
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(ForemanServiceServer::new(foreman))
        .serve(addr.parse().unwrap())
        .await?;

    Ok(())
}
