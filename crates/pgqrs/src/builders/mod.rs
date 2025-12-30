// High-level builder functions
mod admin;
mod consume;
mod produce;

// Re-export high-level functions
pub use admin::admin;
pub use consume::{consume, consume_batch};
pub use produce::{produce, produce_batch};

// Lower-level builders (for future use)
mod dequeue;
mod enqueue;

// Re-export builders for advanced use
pub use admin::AdminBuilder;
pub use dequeue::DequeueBuilder;
pub use enqueue::EnqueueBuilder;
