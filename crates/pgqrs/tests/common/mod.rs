#![allow(clippy::await_holding_lock)]

use ctor::dtor;
use pgqrs::{store::BackendType, Store};

/// Get the current test backend.
#[allow(dead_code)]
pub fn current_backend() -> BackendType {
    BackendType::Postgres
}

/// Create a store for the currently selected test backend.
#[allow(dead_code)]
pub async fn create_store(schema: &str) -> pgqrs::store::AnyStore {
    create_store_with_config(schema, |_: &mut pgqrs::config::Config| {}).await
}

/// Create a store for the currently selected test backend with config customization.
#[allow(dead_code)]
pub async fn create_store_with_config(
    schema: &str,
    mutator: impl FnOnce(&mut pgqrs::config::Config),
) -> pgqrs::store::AnyStore {
    let dsn = get_test_dsn(schema).await;

    let mut config =
        pgqrs::config::Config::from_dsn_with_schema(&dsn, schema).expect("Failed to create config");
    mutator(&mut config);

    let store = pgqrs::connect_with_config(&config)
        .await
        .unwrap_or_else(|e| panic!("Failed to create store with DSN: {}. Error: {:?}", dsn, e));

    store.bootstrap().await.expect("Failed to bootstrap schema");
    store
}

/// Get DSN for the current test backend.
#[allow(dead_code)]
pub async fn get_test_dsn(_schema: &str) -> String {
    std::env::var("PGQRS_TEST_POSTGRES_DSN")
        .or_else(|_| std::env::var("PGQRS_TEST_DSN"))
        .unwrap_or_else(|_| {
            panic!(
                "Postgres tests require PGQRS_TEST_DSN or PGQRS_TEST_POSTGRES_DSN. \
                 Run 'make test-postgres' or set one of the env vars manually."
            )
        })
}

/// Skip test if not running on specified backend.
#[macro_export]
macro_rules! skip_unless_backend {
    ($backend:expr) => {
        if common::current_backend() != $backend {
            eprintln!("Skipping test: requires {:?} backend", $backend);
            return;
        }
    };
}

/// Skip test if running on specified backend.
#[macro_export]
macro_rules! skip_on_backend {
    ($backend:expr) => {
        if common::current_backend() == $backend {
            eprintln!("Skipping test: not supported on {:?} backend", $backend);
            return;
        }
    };
}

#[dtor]
fn drop_database() {
    if keep_test_data() {
        return;
    }
}

fn keep_test_data() -> bool {
    std::env::var("PGQRS_KEEP_TEST_DATA")
        .map(|v| {
            let lowered = v.trim().to_ascii_lowercase();
            !(lowered.is_empty() || lowered == "0" || lowered == "false" || lowered == "no")
        })
        .unwrap_or(false)
}
