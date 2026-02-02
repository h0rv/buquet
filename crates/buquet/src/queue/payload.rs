//! Payload reference handling for large input/output offloading.
//!
//! This module provides utilities for storing and loading task payloads
//! externally from the task object, reducing rewrite cost for state transitions.

use serde_json::Value;
use uuid::Uuid;

use crate::storage::{PutCondition, S3Client, StorageError};

/// Default threshold for offloading payloads (256 KB).
pub const DEFAULT_PAYLOAD_REF_THRESHOLD_BYTES: usize = 256 * 1024;

/// Type of payload (input or output).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadType {
    /// Task input payload.
    Input,
    /// Task output payload.
    Output,
}

impl PayloadType {
    /// Returns the filename suffix for this payload type.
    #[must_use]
    pub const fn filename(&self) -> &'static str {
        match self {
            Self::Input => "input.json",
            Self::Output => "output.json",
        }
    }
}

/// A reference to an externally stored payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadRef {
    /// The S3 key where the payload is stored.
    pub key: String,
}

impl PayloadRef {
    /// Creates a new payload reference with the given key.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Generates a payload key for a task.
    ///
    /// Format: `payloads/{task_id}/input.json` or `payloads/{task_id}/output.json`
    #[must_use]
    pub fn for_task(task_id: Uuid, payload_type: PayloadType) -> Self {
        Self {
            key: format!("payloads/{}/{}", task_id, payload_type.filename()),
        }
    }
}

/// Stores a payload to S3 and returns the reference key.
///
/// # Arguments
///
/// * `client` - The S3 client to use
/// * `task_id` - The task ID for generating the key
/// * `payload_type` - Whether this is an input or output payload
/// * `data` - The JSON data to store
///
/// # Returns
///
/// The S3 key where the payload was stored.
pub async fn store_payload(
    client: &S3Client,
    task_id: Uuid,
    payload_type: PayloadType,
    data: &Value,
) -> Result<String, StorageError> {
    let payload_ref = PayloadRef::for_task(task_id, payload_type);

    let body =
        serde_json::to_vec(data).map_err(|e| StorageError::SerializationError(e.to_string()))?;

    client
        .put_object(&payload_ref.key, body, PutCondition::None)
        .await?;

    Ok(payload_ref.key)
}

/// Loads a payload from S3.
///
/// # Arguments
///
/// * `client` - The S3 client to use
/// * `ref_key` - The S3 key of the payload
///
/// # Returns
///
/// The deserialized JSON value.
pub async fn load_payload(client: &S3Client, ref_key: &str) -> Result<Value, StorageError> {
    let (body, _etag) = client.get_object(ref_key).await?;

    let value: Value = serde_json::from_slice(&body)
        .map_err(|e| StorageError::SerializationError(e.to_string()))?;

    Ok(value)
}

/// Checks if a payload should be offloaded based on its serialized size.
///
/// # Arguments
///
/// * `data` - The JSON data to check
/// * `threshold_bytes` - The size threshold in bytes
///
/// # Returns
///
/// `true` if the payload exceeds the threshold and should be offloaded.
#[must_use]
pub fn should_offload_payload(data: &Value, threshold_bytes: usize) -> bool {
    // Estimate size by serializing to JSON
    // This is slightly inefficient but gives accurate results
    serde_json::to_vec(data).is_ok_and(|bytes| bytes.len() > threshold_bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_ref_for_task_input() {
        let task_id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").unwrap();
        let payload_ref = PayloadRef::for_task(task_id, PayloadType::Input);
        assert_eq!(
            payload_ref.key,
            "payloads/a1234567-89ab-cdef-0123-456789abcdef/input.json"
        );
    }

    #[test]
    fn test_payload_ref_for_task_output() {
        let task_id = Uuid::parse_str("a1234567-89ab-cdef-0123-456789abcdef").unwrap();
        let payload_ref = PayloadRef::for_task(task_id, PayloadType::Output);
        assert_eq!(
            payload_ref.key,
            "payloads/a1234567-89ab-cdef-0123-456789abcdef/output.json"
        );
    }

    #[test]
    fn test_should_offload_small_payload() {
        let data = serde_json::json!({"key": "value"});
        assert!(!should_offload_payload(&data, 1024));
    }

    #[test]
    fn test_should_offload_large_payload() {
        // Create a payload larger than threshold
        let large_string = "x".repeat(1024);
        let data = serde_json::json!({"data": large_string});
        assert!(should_offload_payload(&data, 512));
    }

    #[test]
    fn test_should_offload_at_threshold() {
        // Create exactly at threshold - should not offload
        let data = serde_json::json!({"a": "b"});
        let size = serde_json::to_vec(&data).unwrap().len();
        assert!(!should_offload_payload(&data, size));
        assert!(should_offload_payload(&data, size - 1));
    }

    #[test]
    fn test_payload_type_filename() {
        assert_eq!(PayloadType::Input.filename(), "input.json");
        assert_eq!(PayloadType::Output.filename(), "output.json");
    }
}
