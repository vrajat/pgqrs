use pgqrs::store::{ConcurrencyModel, Store};
use pgqrs::store::any::AnyStore;
use pgqrs::config::Config;

mod common;

#[tokio::test]
async fn test_sqlite_connect() {
    let dsn = "sqlite::memory:";
    let config = Config::from_dsn(dsn);
    let store = pgqrs::connect_with_config(&config).await.expect("Failed to connect to sqlite");

    assert_eq!(store.backend_name(), "sqlite");
    assert_eq!(store.concurrency_model(), ConcurrencyModel::SingleProcess);
}
