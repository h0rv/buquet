//! Integration tests for buquet task queue.
//!
//! These tests require a running S3-compatible storage.
//! Run: docker-compose up -d
//! Then: cargo test --test integration

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod cancellation;
mod common;
mod concurrent_claim;
mod expiration;
mod graceful_shutdown;
mod idempotency;
mod payload_refs;
mod retry_exhaustion;
mod scheduling;
mod shard_leasing;
mod submit_claim_complete;
mod timeout_recovery;
mod version_history;
mod worker_registration;
