// Common modules shared between client and binary
pub mod api;
pub mod codec;
pub mod error;

// Client library module
pub mod client;

// CLI binary modules (for internal use by the binary)
pub mod cli;

// Re-export commonly used client types for convenience
pub use client::{PgqrsClient, PgqrsClientBuilder};
pub use codec::{json, JsonCodec, PgqrsPayloadCodec};

// Re-export commonly used types
pub use api::{
    AckRequest, CreateQueueRequest, DeleteQueueRequest, DequeueRequest, DequeueResponse,
    EnqueueRequest, EnqueueResponse, ExtendLeaseRequest, GetQueueRequest, HealthCheckRequest,
    HealthCheckResponse, ListDeadLettersRequest, ListInFlightRequest, ListQueuesRequest,
    ListQueuesResponse, LivenessRequest, LivenessResponse, Message, NackRequest, PeekRequest,
    PeekResponse, PurgeDeadLettersRequest, Queue, ReadinessRequest, ReadinessResponse,
    RequeueRequest, StatsRequest, StatsResponse,
};
