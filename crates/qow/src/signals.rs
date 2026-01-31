//! Signal handling operations for workflows.
//!
//! Signals allow external systems to send data to running workflows.
//! Signals are stored as immutable S3 objects with timestamp-based keys
//! for ordering.

use crate::error::Result;
use crate::types::Signal;
use qo::storage::S3Client;
use std::sync::Arc;
use uuid::Uuid;

/// Get the S3 key prefix for signals of a given name.
fn signal_prefix(wf_id: &str, name: &str) -> String {
    format!("workflow/{wf_id}/signals/{name}/")
}

/// Get the full S3 key for a signal.
fn signal_key(wf_id: &str, name: &str, timestamp: &str, signal_id: &str) -> String {
    format!("workflow/{wf_id}/signals/{name}/{timestamp}_{signal_id}.json")
}

/// Parse a signal key into its components.
///
/// # Arguments
///
/// * `key` - Full S3 key (e.g., "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json")
///
/// # Returns
///
/// Tuple of (`workflow_id`, `signal_name`, timestamp, `signal_id`), or None if parsing fails.
pub fn parse_signal_key(key: &str) -> Option<(String, String, String, String)> {
    // Remove .json suffix
    let key = key.strip_suffix(".json")?;

    // Split path: workflow/{wf_id}/signals/{name}/{timestamp}_{signal_id}
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() != 5 || parts[0] != "workflow" || parts[2] != "signals" {
        return None;
    }

    let wf_id = parts[1].to_string();
    let signal_name = parts[3].to_string();
    let suffix = parts[4];

    // Split suffix into timestamp and signal_id
    // Format: {timestamp}_{uuid}
    // Timestamp can contain hyphens, so split from the right
    let last_underscore = suffix.rfind('_')?;
    let timestamp = suffix[..last_underscore].to_string();
    let signal_id = suffix[last_underscore + 1..].to_string();

    Some((wf_id, signal_name, timestamp, signal_id))
}

/// Manager for workflow signal operations.
#[derive(Clone)]
pub struct SignalManager<S = S3Client> {
    client: Arc<S>,
}

impl SignalManager<S3Client> {
    /// Create a new `SignalManager` with the given S3 client.
    #[must_use]
    pub const fn new(client: Arc<S3Client>) -> Self {
        Self { client }
    }

    /// Send a signal to a workflow.
    ///
    /// Signals are stored as immutable S3 objects with timestamp-based keys
    /// for ordering. The timestamp is provided externally to allow for
    /// consistent time handling.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name (e.g., "approval", `"payment_received"`)
    /// * `payload` - Signal data
    /// * `now` - Current timestamp in ISO 8601 format
    ///
    /// # Returns
    ///
    /// The signal ID (UUID).
    pub async fn send(
        &self,
        wf_id: &str,
        name: &str,
        payload: serde_json::Value,
        now: &str,
    ) -> Result<String> {
        let signal_id = Uuid::new_v4().to_string();

        let signal = Signal::new(&signal_id, name, payload, now);

        let key = signal_key(wf_id, name, now, &signal_id);
        let body = signal.to_json()?;

        self.client
            .put_object(&key, body, qo::storage::PutCondition::None)
            .await?;

        Ok(signal_id)
    }

    /// List signals after the cursor.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Optional cursor (`timestamp_uuid` suffix) to start after
    /// * `limit` - Maximum number of signals to return
    ///
    /// # Returns
    ///
    /// List of (suffix, Signal) tuples, where suffix is the cursor value.
    pub async fn list(
        &self,
        wf_id: &str,
        name: &str,
        cursor: Option<&str>,
        limit: i32,
    ) -> Result<Vec<(String, Signal)>> {
        let prefix = signal_prefix(wf_id, name);

        // Use start_after for cursor-based pagination
        let start_after = cursor.map(|c| format!("{prefix}{c}"));

        let (keys, _) = self
            .client
            .list_objects_after(&prefix, limit, None, start_after.as_deref())
            .await?;

        let mut signals = Vec::with_capacity(keys.len());
        for key in keys {
            let (data, _) = self.client.get_object(&key).await?;
            let signal = Signal::from_json(&data)?;

            // Extract suffix (timestamp_uuid) from key
            let suffix = key
                .strip_prefix(&prefix)
                .and_then(|s| s.strip_suffix(".json"))
                .unwrap_or("")
                .to_string();

            signals.push((suffix, signal));
        }

        Ok(signals)
    }

    /// Get the next unconsumed signal.
    ///
    /// This is a convenience wrapper around list that returns only the first signal.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Current cursor position
    ///
    /// # Returns
    ///
    /// Tuple of (`new_cursor`, Signal) or None if no signals available.
    pub async fn get_next(
        &self,
        wf_id: &str,
        name: &str,
        cursor: Option<&str>,
    ) -> Result<Option<(String, Signal)>> {
        let signals = self.list(wf_id, name, cursor, 1).await?;
        Ok(signals.into_iter().next())
    }

    /// Count signals after the cursor.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    /// * `cursor` - Optional cursor to count signals after
    ///
    /// # Returns
    ///
    /// Number of signals available.
    pub async fn count(&self, wf_id: &str, name: &str, cursor: Option<&str>) -> Result<usize> {
        let prefix = signal_prefix(wf_id, name);
        let start_after = cursor.map(|c| format!("{prefix}{c}"));

        let (keys, _) = self
            .client
            .list_objects_after(&prefix, 1000, None, start_after.as_deref())
            .await?;

        Ok(keys.len())
    }

    /// Delete all signals of a given name (for cleanup/testing).
    ///
    /// Note: In production, signals should be retained for audit purposes.
    /// Use retention policies instead of explicit deletion.
    ///
    /// # Arguments
    ///
    /// * `wf_id` - Workflow ID
    /// * `name` - Signal name
    ///
    /// # Returns
    ///
    /// Number of signals deleted.
    pub async fn delete_all(&self, wf_id: &str, name: &str) -> Result<usize> {
        let prefix = signal_prefix(wf_id, name);
        let keys = self.client.list_objects_paginated(&prefix, 1000).await?;

        let count = keys.len();
        for key in keys {
            self.client.delete_object(&key).await?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_prefix() {
        assert_eq!(
            signal_prefix("wf-123", "approval"),
            "workflow/wf-123/signals/approval/"
        );
    }

    #[test]
    fn test_signal_key() {
        assert_eq!(
            signal_key("wf-123", "approval", "2026-01-28T12:00:00Z", "abc123"),
            "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json"
        );
    }

    #[test]
    fn test_parse_signal_key() {
        let key = "workflow/wf-123/signals/approval/2026-01-28T12:00:00Z_abc123.json";
        let result = parse_signal_key(key);

        assert!(result.is_some());
        let (wf_id, name, timestamp, signal_id) = match result {
            Some(value) => value,
            None => {
                assert!(false, "expected signal key to parse");
                return;
            }
        };
        assert_eq!(wf_id, "wf-123");
        assert_eq!(name, "approval");
        assert_eq!(timestamp, "2026-01-28T12:00:00Z");
        assert_eq!(signal_id, "abc123");
    }

    #[test]
    fn test_parse_signal_key_with_uuid() {
        let key =
            "workflow/wf-abc/signals/payment/2026-01-28T12:00:00Z_550e8400-e29b-41d4-a716-446655440000.json";
        let result = parse_signal_key(key);

        assert!(result.is_some());
        let (wf_id, name, timestamp, signal_id) = match result {
            Some(value) => value,
            None => {
                assert!(false, "expected signal key to parse");
                return;
            }
        };
        assert_eq!(wf_id, "wf-abc");
        assert_eq!(name, "payment");
        assert_eq!(timestamp, "2026-01-28T12:00:00Z");
        assert_eq!(signal_id, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_parse_signal_key_invalid() {
        // Missing .json suffix
        assert!(parse_signal_key("workflow/wf-123/signals/approval/ts_id").is_none());

        // Wrong prefix
        assert!(parse_signal_key("workflows/wf-123/signals/approval/ts_id.json").is_none());

        // Missing parts
        assert!(parse_signal_key("workflow/wf-123/approval/ts_id.json").is_none());

        // No underscore in suffix
        assert!(parse_signal_key("workflow/wf-123/signals/approval/tsid.json").is_none());
    }
}
