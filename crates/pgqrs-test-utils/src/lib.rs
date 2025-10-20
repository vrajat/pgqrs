//! Shared test utilities for PGQRS
//! 
//! This crate provides common test infrastructure for both server and client tests,
//! including mock repositories, test server setup, and helper utilities.

pub mod mocks;
pub mod server;
pub mod helpers;

// Re-export commonly used items
pub use mocks::{MockQueueRepo, MockMessageRepo};
pub use server::{start_test_server, wait_for_server_ready};
pub use helpers::{
    test_endpoint, measure_performance, assert_performance,
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_RPC_TIMEOUT, 
    PERFORMANCE_TEST_TIMEOUT, PERFORMANCE_TEST_ITERATIONS,
};

// Re-export for convenience
pub use tokio::time::Duration;
pub use std::net::SocketAddr;