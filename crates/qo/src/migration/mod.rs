//! Shard migration tools for changing shard prefix length.
//!
//! This module provides utilities for migrating tasks between different
//! shard configurations. When changing `shard_prefix_len`, all existing
//! tasks need to be moved to their new shard locations.

mod shard_migrate;

pub use shard_migrate::{
    MigrationPlan, MigrationProgress, MigrationResult, ObjectMove, ShardMigration,
};
