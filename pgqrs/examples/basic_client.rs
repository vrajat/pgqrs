use pgqrs::PgqrsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client
    let mut client = PgqrsClient::builder()
        .endpoint("http://localhost:50051")
        .api_key("your-api-key")
        .connect_timeout(std::time::Duration::from_secs(10))
        .rpc_timeout(std::time::Duration::from_secs(30))
        .build()
        .await?;

    // Check liveness
    match client.liveness().await {
        Ok(response) => println!("Liveness check: {}", response.status),
        Err(e) => println!("Liveness check failed: {}", e),
    }

    // Check readiness
    match client.readiness().await {
        Ok(response) => {
            println!("Readiness check: {}", response.status);
            if !response.failing_services.is_empty() {
                println!("Failing services: {:?}", response.failing_services);
            }
        }
        Err(e) => println!("Readiness check failed: {}", e),
    }

    Ok(())
}
