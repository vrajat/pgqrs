use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use crate::error::{PgqrsClientError, Result};

/// Trait for encoding and decoding message payloads
#[async_trait]
pub trait PgqrsPayloadCodec<T> {
    /// Encode a payload to bytes
    async fn encode(&self, payload: &T) -> Result<Vec<u8>>;
    
    /// Decode bytes to a payload
    async fn decode(&self, bytes: &[u8]) -> Result<T>;
}

/// Default JSON codec implementation
pub struct JsonCodec;

#[async_trait]
impl<T> PgqrsPayloadCodec<T> for JsonCodec
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    async fn encode(&self, payload: &T) -> Result<Vec<u8>> {
        let json_string = serde_json::to_string(payload)?;
        Ok(json_string.into_bytes())
    }

    async fn decode(&self, bytes: &[u8]) -> Result<T> {
        let json_string = String::from_utf8(bytes.to_vec())
            .map_err(|_| PgqrsClientError::InvalidConfig("Invalid UTF-8 in payload".to_string()))?;
        let payload = serde_json::from_str(&json_string)?;
        Ok(payload)
    }
}

/// Convenience functions for JSON serialization/deserialization
pub mod json {
    use super::*;
    
    /// Serialize a value to JSON bytes
    pub fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
        let json_string = serde_json::to_string(value)?;
        Ok(json_string.into_bytes())
    }
    
    /// Deserialize JSON bytes to a value
    pub fn from_bytes<T>(bytes: &[u8]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let json_string = String::from_utf8(bytes.to_vec())
            .map_err(|_| PgqrsClientError::InvalidConfig("Invalid UTF-8 in payload".to_string()))?;
        let value = serde_json::from_str(&json_string)?;
        Ok(value)
    }
}