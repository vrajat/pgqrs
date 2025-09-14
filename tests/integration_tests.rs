mod common;


#[tokio::test]
async fn verify() {
	let admin = common::get_pgqrs_client().await;
	// Verify should succeed
	assert!(admin.verify().is_ok());
}
