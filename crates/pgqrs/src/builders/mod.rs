// High-level builder functions
mod admin;
mod consume;
mod produce;
mod tables;

// Worker creation builders
mod consumer;
mod producer;

// Re-export high-level functions
pub use admin::admin;
pub use consume::{consume, consume_batch};
pub use consumer::consumer;
pub use dequeue::dequeue;
pub use enqueue::enqueue;
pub use produce::{produce, produce_batch};
pub use producer::producer;
pub use tables::tables;

// Lower-level builders (for future use)
mod dequeue;
mod enqueue;
mod worker_handle;

// Re-export builders for advanced use
pub use admin::AdminBuilder;
pub use consumer::ConsumerBuilder;
pub use dequeue::DequeueBuilder;
pub use enqueue::EnqueueBuilder;
pub use producer::ProducerBuilder;
pub use tables::TablesBuilder;

pub use worker_handle::worker_handle;
