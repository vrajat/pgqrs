// Test configuration constants

// Connection configuration
#[cfg(feature = "postgres")]
pub const MAX_CONNECTIONS: u32 = 1;
#[cfg(feature = "postgres")]
pub const CONNECTION_TIMEOUT_SECS: u64 = 5;
#[cfg(feature = "postgres")]
pub const VERIFICATION_QUERY: &str = "SELECT 1";
