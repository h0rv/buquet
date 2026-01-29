//! S3 storage client wrapper for the dex task queue.
//!
//! This module provides a simplified interface for interacting with S3-compatible
//! object storage, including support for conditional writes (If-None-Match) which
//! are essential for atomic task claiming.

mod client;
mod error;

pub use client::{ObjectVersion, PutCondition, S3Client, S3Config};
pub use error::StorageError;
