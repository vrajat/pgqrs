//! Shared test utilities for PGQRS
//!
//! This crate provides common test infrastructure for both server and client tests,
//! including mock repositories, test server setup, and helper utilities.

pub mod helpers;
pub mod mocks;
pub mod postgres;
pub mod server;

// Re-export commonly used items
pub use helpers::{
    assert_performance, measure_performance, test_endpoint, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_RPC_TIMEOUT, PERFORMANCE_TEST_ITERATIONS, PERFORMANCE_TEST_TIMEOUT,
};
pub use mocks::{MockMessageRepo, MockQueueRepo};
pub use postgres::{get_pgqrs_client, start_postgres_container};
pub use server::{start_test_server, start_test_server_with_postgres, wait_for_server_ready};

// Re-export for convenience
pub use std::net::SocketAddr;
pub use tokio::time::Duration;
