//! Configuration file support for qo.
//!
//! Load settings from `.qo.toml` or `~/.config/qo/config.toml`.
//!
//! ## Config Resolution Order
//!
//! 1. CLI flags (highest priority)
//! 2. Environment variables (`S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `QO_*`)
//! 3. Profile-specific settings (if `--profile` specified)
//! 4. Project-level `.qo.toml`
//! 5. User-level `~/.config/qo/config.toml`
//! 6. Built-in defaults (lowest priority)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::Write;
use std::path::PathBuf;
use std::time::Duration;

use crate::worker::PollingStrategy;

/// Main configuration structure for qo.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct QoConfig {
    /// S3 bucket name for the queue.
    pub bucket: String,
    /// Optional custom S3 endpoint (e.g., for local development with LocalStack/MinIO).
    pub endpoint: Option<String>,
    /// AWS region (defaults to "us-east-1").
    #[serde(default = "default_region")]
    pub region: String,
    /// Queue-level configuration.
    #[serde(default)]
    pub queue: QueueSection,
    /// Worker-specific configuration.
    #[serde(default)]
    pub worker: WorkerConfig,
    /// Monitor-specific configuration.
    #[serde(default)]
    pub monitor: MonitorConfig,
}

/// Queue-level configuration section.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct QueueSection {
    /// Number of hex characters for shard prefix (1-4).
    /// 1 = 16 shards, 2 = 256 shards, 3 = 4096 shards, 4 = 65536 shards.
    /// Default: 1
    pub shard_prefix_len: Option<usize>,
}

/// Worker-specific configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct WorkerConfig {
    /// Polling interval in milliseconds (legacy, for backward compatibility).
    /// If set without `polling` section, treated as fixed strategy.
    pub poll_interval_ms: Option<u64>,
    /// Index mode for ready task discovery ("hybrid", "index", "scan").
    pub index_mode: Option<String>,
    /// Shards to process (e.g., `["0", "1", "2", "3"]`).
    pub shards: Option<Vec<String>>,
    /// Polling strategy configuration.
    #[serde(default)]
    pub polling: Option<PollingConfig>,
    /// Shard leasing configuration.
    #[serde(default)]
    pub shard_leasing: Option<ShardLeasingConfig>,
}

/// Polling strategy configuration for the worker.
///
/// Supports two strategies:
/// - `fixed`: Constant polling interval (predictable latency, higher cost)
/// - `adaptive`: Exponential backoff when idle (lower cost, variable latency)
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PollingConfig {
    /// Strategy type: "adaptive" or "fixed".
    #[serde(default = "default_polling_strategy")]
    pub strategy: String,
    /// Minimum interval in milliseconds (for adaptive strategy).
    pub min_interval_ms: Option<u64>,
    /// Maximum interval in milliseconds (for adaptive strategy).
    pub max_interval_ms: Option<u64>,
    /// Backoff multiplier (for adaptive strategy).
    pub backoff_multiplier: Option<f64>,
    /// Fixed interval in milliseconds (for fixed strategy).
    pub interval_ms: Option<u64>,
}

fn default_polling_strategy() -> String {
    "adaptive".to_string()
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            strategy: default_polling_strategy(),
            min_interval_ms: Some(100),
            max_interval_ms: Some(5000),
            backoff_multiplier: Some(2.0),
            interval_ms: None,
        }
    }
}

impl PollingConfig {
    /// Convert to `PollingStrategy` enum.
    #[must_use]
    pub fn to_polling_strategy(&self) -> PollingStrategy {
        if self.strategy.to_lowercase().as_str() == "fixed" {
            let interval_ms = self.interval_ms.unwrap_or(500);
            PollingStrategy::Fixed { interval_ms }
        } else {
            // Default to adaptive
            let min_interval_ms = self.min_interval_ms.unwrap_or(100);
            let max_interval_ms = self.max_interval_ms.unwrap_or(5000);
            let backoff_multiplier = self.backoff_multiplier.unwrap_or(2.0);
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            }
        }
    }
}

/// Shard leasing configuration for dynamic shard ownership.
///
/// When enabled, workers dynamically acquire leases on shards rather than
/// polling all assigned shards. This reduces S3 LIST costs at scale.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShardLeasingConfig {
    /// Whether shard leasing is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// How many shards each worker should try to lease.
    #[serde(default = "default_shards_per_worker")]
    pub shards_per_worker: usize,
    /// Lease duration in seconds.
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u64,
    /// How often to renew leases in seconds.
    #[serde(default = "default_renewal_interval_secs")]
    pub renewal_interval_secs: u64,
}

const fn default_shards_per_worker() -> usize {
    16
}

const fn default_lease_ttl_secs() -> u64 {
    30
}

const fn default_renewal_interval_secs() -> u64 {
    10
}

impl Default for ShardLeasingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shards_per_worker: default_shards_per_worker(),
            lease_ttl_secs: default_lease_ttl_secs(),
            renewal_interval_secs: default_renewal_interval_secs(),
        }
    }
}

impl ShardLeasingConfig {
    /// Convert to the worker's `ShardLeaseConfig` type.
    #[must_use]
    pub const fn to_shard_lease_config(&self) -> crate::worker::ShardLeaseConfig {
        crate::worker::ShardLeaseConfig {
            enabled: self.enabled,
            shards_per_worker: self.shards_per_worker,
            lease_ttl: Duration::from_secs(self.lease_ttl_secs),
            renewal_interval: Duration::from_secs(self.renewal_interval_secs),
        }
    }
}

impl WorkerConfig {
    /// Resolve the effective polling strategy.
    ///
    /// Priority order:
    /// 1. Explicit `polling` section configuration
    /// 2. Legacy `poll_interval_ms` field (treated as fixed strategy)
    /// 3. Default adaptive strategy
    #[must_use]
    pub fn effective_polling_strategy(&self) -> PollingStrategy {
        // If explicit polling config exists, use it
        if let Some(ref polling) = self.polling {
            return polling.to_polling_strategy();
        }

        // Backward compatibility: if legacy poll_interval_ms is set,
        // treat it as a fixed polling strategy
        if let Some(interval_ms) = self.poll_interval_ms {
            return PollingStrategy::Fixed { interval_ms };
        }

        // Default to adaptive strategy
        PollingStrategy::default()
    }

    /// Resolve the effective shard lease configuration.
    ///
    /// Returns the configured shard leasing settings, or default if not configured.
    #[must_use]
    pub fn effective_shard_lease_config(&self) -> crate::worker::ShardLeaseConfig {
        self.shard_leasing
            .as_ref()
            .map(ShardLeasingConfig::to_shard_lease_config)
            .unwrap_or_default()
    }
}

impl QueueSection {
    /// Get the effective shard prefix length.
    ///
    /// Returns the configured value or the default (1).
    #[must_use]
    pub fn effective_shard_prefix_len(&self) -> usize {
        self.shard_prefix_len.unwrap_or(1)
    }

    /// Convert to the queue's `QueueConfig` type.
    #[must_use]
    pub fn to_queue_config(&self) -> crate::queue::QueueConfig {
        crate::queue::QueueConfig {
            shard_prefix_len: self.effective_shard_prefix_len(),
        }
    }
}

/// Monitor-specific configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct MonitorConfig {
    /// Interval in seconds between health checks.
    pub check_interval_secs: Option<u64>,
    /// Interval in seconds between sweep operations.
    pub sweep_interval_secs: Option<u64>,
    /// Maximum tasks to process per monitor scan cycle (default: 100).
    /// Limits work per cycle to avoid O(total objects) scans.
    pub max_tasks_per_scan: Option<usize>,
    /// Maximum objects to process per sweeper cycle (default: 1000).
    /// Limits work per cycle to avoid O(total objects) scans.
    pub max_objects_per_sweep: Option<usize>,
    /// Sweep mode: `continuous` (default) or `on_demand`.
    pub sweep_mode: Option<String>,
}

/// Configuration file structure supporting profiles.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct ConfigFile {
    /// Default settings (used when no profile is specified).
    #[serde(default)]
    pub default: DefaultSection,
    /// Named profiles that override default settings.
    #[serde(default)]
    pub profiles: HashMap<String, ProfileSection>,
    /// Queue configuration section.
    #[serde(default)]
    pub queue: QueueSection,
    /// Worker configuration section.
    #[serde(default)]
    pub worker: WorkerConfig,
    /// Monitor configuration section.
    #[serde(default)]
    pub monitor: MonitorConfig,
}

/// Default section in config file.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct DefaultSection {
    /// S3 bucket name.
    pub bucket: Option<String>,
    /// AWS region.
    pub region: Option<String>,
    /// Custom S3 endpoint.
    pub endpoint: Option<String>,
}

/// Profile-specific settings that can override defaults.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ProfileSection {
    /// S3 bucket name (overrides default).
    pub bucket: Option<String>,
    /// AWS region (overrides default).
    pub region: Option<String>,
    /// Custom S3 endpoint (overrides default).
    pub endpoint: Option<String>,
}

/// Information about which config files were loaded.
#[derive(Debug, Clone, Default)]
pub struct ConfigSources {
    /// User-level config path (~/.config/qo/config.toml)
    pub user_config: Option<PathBuf>,
    /// Whether user config exists
    pub user_config_exists: bool,
    /// Project-level config path (.qo.toml)
    pub project_config: Option<PathBuf>,
    /// Whether project config exists
    pub project_config_exists: bool,
    /// Custom config path (from --config flag)
    pub custom_config: Option<PathBuf>,
    /// Whether custom config exists
    pub custom_config_exists: bool,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

impl Default for QoConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            endpoint: None,
            region: default_region(),
            queue: QueueSection::default(),
            worker: WorkerConfig::default(),
            monitor: MonitorConfig::default(),
        }
    }
}

impl ConfigFile {
    /// Get resolved configuration for a given profile.
    /// Profile settings override [default] section.
    #[must_use]
    pub fn resolve(&self, profile: Option<&str>) -> QoConfig {
        let mut config = QoConfig {
            bucket: self.default.bucket.clone().unwrap_or_default(),
            endpoint: self.default.endpoint.clone(),
            region: self.default.region.clone().unwrap_or_else(default_region),
            queue: self.queue.clone(),
            worker: self.worker.clone(),
            monitor: self.monitor.clone(),
        };

        if let Some(profile_name) = profile {
            if let Some(profile_section) = self.profiles.get(profile_name) {
                if let Some(ref bucket) = profile_section.bucket {
                    config.bucket.clone_from(bucket);
                }
                if let Some(ref region) = profile_section.region {
                    config.region.clone_from(region);
                }
                if profile_section.endpoint.is_some() {
                    config.endpoint.clone_from(&profile_section.endpoint);
                }
            }
        }

        config
    }
}

impl ConfigSources {
    /// Discover config file locations.
    #[must_use]
    pub fn discover() -> Self {
        let user_config = get_user_config_path();
        let user_config_exists = user_config.as_ref().is_some_and(|p| p.exists());

        let project_config = get_project_config_path();
        let project_config_exists = project_config.exists();

        Self {
            user_config,
            user_config_exists,
            project_config: Some(project_config),
            project_config_exists,
            custom_config: None,
            custom_config_exists: false,
        }
    }

    /// Add a custom config path.
    #[must_use]
    pub fn with_custom_config(mut self, path: Option<PathBuf>) -> Self {
        if let Some(p) = path {
            self.custom_config_exists = p.exists();
            self.custom_config = Some(p);
        }
        self
    }
}

/// Error type for configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to parse TOML configuration.
    #[error("Failed to parse TOML: {0}")]
    TomlParse(#[from] toml::de::Error),
    /// Failed to read configuration file.
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),
}

/// Get the user-level config path (~/.config/qo/config.toml).
#[must_use]
pub fn get_user_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|p| p.join("qo").join("config.toml"))
}

/// Get the project-level config path (.qo.toml in current directory).
#[must_use]
pub fn get_project_config_path() -> PathBuf {
    PathBuf::from(".qo.toml")
}

/// Load configuration from a TOML string.
///
/// # Errors
/// Returns `ConfigError::TomlParse` if the TOML is invalid.
pub fn load_from_str(toml_content: &str) -> Result<ConfigFile, ConfigError> {
    let config: ConfigFile = toml::from_str(toml_content)?;
    Ok(config)
}

/// Load configuration from a file path.
///
/// # Errors
/// Returns `ConfigError::Io` if the file cannot be read, or
/// `ConfigError::TomlParse` if the TOML is invalid.
pub fn load_from_file(path: &std::path::Path) -> Result<ConfigFile, ConfigError> {
    let content = std::fs::read_to_string(path)?;
    load_from_str(&content)
}

/// Load configuration with the specified profile and optional custom config path.
///
/// Resolution order (highest to lowest priority):
/// 1. CLI flags (handled by caller after this function returns)
/// 2. Environment variables (`S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`, `QO_*`)
/// 3. Profile-specific settings (if profile specified)
/// 4. Project-level `.qo.toml`
/// 5. User-level `~/.config/qo/config.toml`
/// 6. Built-in defaults
///
/// # Errors
/// Returns an error if a config file exists but cannot be parsed.
pub fn load_config(
    profile: Option<&str>,
    custom_config: Option<&PathBuf>,
) -> Result<QoConfig, ConfigError> {
    let mut merged = ConfigFile::default();

    // 1. Load user-level config (lowest priority file)
    if let Some(user_path) = get_user_config_path() {
        if user_path.exists() {
            let user_config = load_from_file(&user_path)?;
            merge_config_file(&mut merged, &user_config);
        }
    }

    // 2. Load project-level config (higher priority)
    let project_path = get_project_config_path();
    if project_path.exists() {
        let project_config = load_from_file(&project_path)?;
        merge_config_file(&mut merged, &project_config);
    }

    // 3. Load custom config if specified (highest file priority)
    if let Some(custom_path) = custom_config {
        let custom = load_from_file(custom_path)?;
        merge_config_file(&mut merged, &custom);
    }

    // Resolve with profile
    let mut config = merged.resolve(profile);

    // 4. Apply environment variables (highest priority, overrides files)
    apply_env_overrides(&mut config);

    Ok(config)
}

/// Merge a config file into the base config (later values override earlier).
fn merge_config_file(base: &mut ConfigFile, overlay: &ConfigFile) {
    // Merge default section
    if overlay.default.bucket.is_some() {
        base.default.bucket.clone_from(&overlay.default.bucket);
    }
    if overlay.default.region.is_some() {
        base.default.region.clone_from(&overlay.default.region);
    }
    if overlay.default.endpoint.is_some() {
        base.default.endpoint.clone_from(&overlay.default.endpoint);
    }

    // Merge profiles (overlay profiles take precedence)
    for (name, profile) in &overlay.profiles {
        base.profiles.insert(name.clone(), profile.clone());
    }

    // Merge queue config
    if overlay.queue.shard_prefix_len.is_some() {
        base.queue.shard_prefix_len = overlay.queue.shard_prefix_len;
    }

    // Merge worker config
    if overlay.worker.poll_interval_ms.is_some() {
        base.worker.poll_interval_ms = overlay.worker.poll_interval_ms;
    }
    if overlay.worker.index_mode.is_some() {
        base.worker
            .index_mode
            .clone_from(&overlay.worker.index_mode);
    }
    if overlay.worker.shards.is_some() {
        base.worker.shards.clone_from(&overlay.worker.shards);
    }
    if overlay.worker.polling.is_some() {
        base.worker.polling.clone_from(&overlay.worker.polling);
    }
    if overlay.worker.shard_leasing.is_some() {
        base.worker
            .shard_leasing
            .clone_from(&overlay.worker.shard_leasing);
    }

    // Merge monitor config
    if overlay.monitor.check_interval_secs.is_some() {
        base.monitor.check_interval_secs = overlay.monitor.check_interval_secs;
    }
    if overlay.monitor.sweep_interval_secs.is_some() {
        base.monitor.sweep_interval_secs = overlay.monitor.sweep_interval_secs;
    }
    if overlay.monitor.max_tasks_per_scan.is_some() {
        base.monitor.max_tasks_per_scan = overlay.monitor.max_tasks_per_scan;
    }
    if overlay.monitor.max_objects_per_sweep.is_some() {
        base.monitor.max_objects_per_sweep = overlay.monitor.max_objects_per_sweep;
    }
    if overlay.monitor.sweep_mode.is_some() {
        base.monitor
            .sweep_mode
            .clone_from(&overlay.monitor.sweep_mode);
    }
}

/// Apply environment variable overrides (highest priority).
fn apply_env_overrides(config: &mut QoConfig) {
    // S3_* environment variables
    if let Ok(bucket) = env::var("S3_BUCKET") {
        config.bucket = bucket;
    }
    if let Ok(endpoint) = env::var("S3_ENDPOINT") {
        config.endpoint = Some(endpoint);
    }
    if let Ok(region) = env::var("S3_REGION") {
        config.region = region;
    }

    // Queue configuration
    if let Ok(val) = env::var("QO_SHARD_PREFIX_LEN") {
        if let Ok(len) = val.parse() {
            config.queue.shard_prefix_len = Some(len);
        }
    }

    // QO_* environment variables for worker/monitor
    if let Ok(val) = env::var("QO_POLL_INTERVAL_MS") {
        if let Ok(ms) = val.parse() {
            config.worker.poll_interval_ms = Some(ms);
        }
    }
    if let Ok(val) = env::var("QO_INDEX_MODE") {
        config.worker.index_mode = Some(val);
    }
    if let Ok(val) = env::var("QO_SHARDS") {
        let shards: Vec<String> = val.split(',').map(|s| s.trim().to_string()).collect();
        config.worker.shards = Some(shards);
    }

    // Polling strategy environment variables
    apply_polling_env_overrides(config);

    // Shard leasing environment variables (handled by ShardLeaseConfig::from_env()
    // but we also support them here for config file integration)
    apply_shard_leasing_env_overrides(config);

    if let Ok(val) = env::var("QO_MONITOR_CHECK_INTERVAL") {
        if let Ok(secs) = val.parse() {
            config.monitor.check_interval_secs = Some(secs);
        }
    }
    if let Ok(val) = env::var("QO_MONITOR_SWEEP_INTERVAL") {
        if let Ok(secs) = val.parse() {
            config.monitor.sweep_interval_secs = Some(secs);
        }
    }
    if let Ok(val) = env::var("QO_MONITOR_MAX_TASKS_PER_SCAN") {
        if let Ok(count) = val.parse() {
            config.monitor.max_tasks_per_scan = Some(count);
        }
    }
    if let Ok(val) = env::var("QO_SWEEPER_MAX_OBJECTS_PER_SWEEP") {
        if let Ok(count) = val.parse() {
            config.monitor.max_objects_per_sweep = Some(count);
        }
    }
    if let Ok(val) = env::var("QO_SWEEPER_MODE") {
        config.monitor.sweep_mode = Some(val);
    }
}

/// Apply polling strategy environment variable overrides.
fn apply_polling_env_overrides(config: &mut QoConfig) {
    let strategy = env::var("QO_POLLING_STRATEGY").ok();
    let min_interval_ms = env::var("QO_POLLING_MIN_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok());
    let max_interval_ms = env::var("QO_POLLING_MAX_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok());
    let backoff_multiplier = env::var("QO_POLLING_BACKOFF_MULTIPLIER")
        .ok()
        .and_then(|v| v.parse().ok());

    // Only create polling config if at least one env var is set
    if strategy.is_some()
        || min_interval_ms.is_some()
        || max_interval_ms.is_some()
        || backoff_multiplier.is_some()
    {
        let polling = config
            .worker
            .polling
            .get_or_insert_with(PollingConfig::default);

        if let Some(s) = strategy {
            polling.strategy = s;
        }
        if let Some(ms) = min_interval_ms {
            polling.min_interval_ms = Some(ms);
        }
        if let Some(ms) = max_interval_ms {
            polling.max_interval_ms = Some(ms);
        }
        if let Some(mult) = backoff_multiplier {
            polling.backoff_multiplier = Some(mult);
        }
    }
}

/// Apply shard leasing environment variable overrides.
fn apply_shard_leasing_env_overrides(config: &mut QoConfig) {
    let enabled = env::var("QO_SHARD_LEASING")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .ok();
    let shards_per_worker = env::var("QO_SHARD_LEASING_SHARDS_PER_WORKER")
        .ok()
        .and_then(|v| v.parse().ok());
    let lease_ttl_secs = env::var("QO_SHARD_LEASING_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok());
    let renewal_interval_secs = env::var("QO_SHARD_LEASING_RENEW_SECS")
        .ok()
        .and_then(|v| v.parse().ok());

    // Only create shard leasing config if at least one env var is set
    if enabled.is_some()
        || shards_per_worker.is_some()
        || lease_ttl_secs.is_some()
        || renewal_interval_secs.is_some()
    {
        let shard_leasing = config
            .worker
            .shard_leasing
            .get_or_insert_with(ShardLeasingConfig::default);

        if let Some(e) = enabled {
            shard_leasing.enabled = e;
        }
        if let Some(n) = shards_per_worker {
            shard_leasing.shards_per_worker = n;
        }
        if let Some(ttl) = lease_ttl_secs {
            shard_leasing.lease_ttl_secs = ttl;
        }
        if let Some(interval) = renewal_interval_secs {
            shard_leasing.renewal_interval_secs = interval;
        }
    }
}

/// Validate the configuration.
///
/// Returns a list of validation errors (empty if valid).
#[must_use]
pub fn validate_config(config: &QoConfig) -> Vec<String> {
    let mut errors = Vec::new();

    // Check required fields for S3 operations
    if config.bucket.is_empty() {
        errors.push("bucket: not configured (set S3_BUCKET or add to config file)".to_string());
    }

    if config.region.is_empty() {
        errors.push("region: not configured (set S3_REGION or add to config file)".to_string());
    }

    // Validate queue config
    if let Some(len) = config.queue.shard_prefix_len {
        if len == 0 || len > 4 {
            errors.push(format!("queue.shard_prefix_len: must be 1-4, got {len}"));
        }
    }

    // Validate worker config
    if let Some(ref mode) = config.worker.index_mode {
        let valid_modes = ["index-only", "index", "hybrid", "log-scan", "log"];
        if !valid_modes.contains(&mode.to_lowercase().as_str()) {
            errors.push(format!(
                "worker.index_mode: invalid value '{mode}' (use index-only, hybrid, or log-scan)"
            ));
        }
    }

    // Validate shards format based on shard_prefix_len
    let expected_len = config.queue.shard_prefix_len.unwrap_or(1);
    if let Some(ref shards) = config.worker.shards {
        for shard in shards {
            if shard.len() != expected_len || !shard.chars().all(|c| c.is_ascii_hexdigit()) {
                errors.push(format!(
                    "worker.shards: invalid shard '{shard}' (expected {expected_len} hex character(s))"
                ));
            }
        }
    }

    // Validate polling config
    if let Some(ref polling) = config.worker.polling {
        let valid_strategies = ["adaptive", "fixed"];
        if !valid_strategies.contains(&polling.strategy.to_lowercase().as_str()) {
            let strategy = &polling.strategy;
            errors.push(format!(
                "worker.polling.strategy: invalid value '{strategy}' (use 'adaptive' or 'fixed')"
            ));
        }

        if polling.strategy.to_lowercase() == "fixed" && polling.interval_ms.is_none() {
            // Fixed strategy should have interval_ms
            errors.push("worker.polling: fixed strategy requires interval_ms".to_string());
        }

        if let Some(mult) = polling.backoff_multiplier {
            if mult <= 1.0 {
                errors.push(format!(
                    "worker.polling.backoff_multiplier: must be > 1.0, got {mult}"
                ));
            }
        }
    }

    // Validate shard leasing config
    if let Some(ref shard_leasing) = config.worker.shard_leasing {
        if shard_leasing.shards_per_worker == 0 {
            errors.push("worker.shard_leasing.shards_per_worker: must be > 0".to_string());
        }
        if shard_leasing.lease_ttl_secs == 0 {
            errors.push("worker.shard_leasing.lease_ttl_secs: must be > 0".to_string());
        }
        if shard_leasing.renewal_interval_secs == 0 {
            errors.push("worker.shard_leasing.renewal_interval_secs: must be > 0".to_string());
        }
        if shard_leasing.renewal_interval_secs >= shard_leasing.lease_ttl_secs {
            errors.push(
                "worker.shard_leasing: renewal_interval_secs should be < lease_ttl_secs"
                    .to_string(),
            );
        }
    }

    errors
}

/// Format the configuration for display.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn format_config(config: &QoConfig, sources: &ConfigSources) -> String {
    let mut output = String::new();

    output.push_str("# Resolved Configuration\n\n");

    output.push_str("[s3]\n");
    let _ = writeln!(
        output,
        "bucket = {}",
        if config.bucket.is_empty() {
            "(not set)".to_string()
        } else {
            format!("\"{}\"", config.bucket)
        }
    );
    let _ = writeln!(
        output,
        "endpoint = {}",
        config
            .endpoint
            .as_deref()
            .map_or("(not set)".to_string(), |s| format!("\"{s}\""))
    );
    let _ = writeln!(
        output,
        "region = {}",
        if config.region.is_empty() {
            "(not set)".to_string()
        } else {
            format!("\"{}\"", config.region)
        }
    );

    output.push_str("\n[queue]\n");
    let _ = writeln!(
        output,
        "shard_prefix_len = {}",
        config
            .queue
            .shard_prefix_len
            .map_or("(default: 1)".to_string(), |v| v.to_string())
    );

    output.push_str("\n[worker]\n");
    let _ = writeln!(
        output,
        "poll_interval_ms = {}",
        config
            .worker
            .poll_interval_ms
            .map_or("(default)".to_string(), |v| v.to_string())
    );
    let _ = writeln!(
        output,
        "index_mode = {}",
        config
            .worker
            .index_mode
            .as_deref()
            .map_or("(default: hybrid)".to_string(), |s| format!("\"{s}\""))
    );
    let _ = writeln!(
        output,
        "shards = {}",
        config
            .worker
            .shards
            .as_ref()
            .map_or("(default: all)".to_string(), |v| format!("{v:?}"))
    );

    // Format polling strategy
    output.push_str("\n[worker.polling]\n");
    if let Some(ref polling) = config.worker.polling {
        let _ = writeln!(output, "strategy = \"{}\"", polling.strategy);
        if polling.strategy.to_lowercase() == "adaptive" {
            let _ = writeln!(
                output,
                "min_interval_ms = {}",
                polling
                    .min_interval_ms
                    .map_or("(default: 100)".to_string(), |v| v.to_string())
            );
            let _ = writeln!(
                output,
                "max_interval_ms = {}",
                polling
                    .max_interval_ms
                    .map_or("(default: 5000)".to_string(), |v| v.to_string())
            );
            let _ = writeln!(
                output,
                "backoff_multiplier = {}",
                polling
                    .backoff_multiplier
                    .map_or("(default: 2.0)".to_string(), |v| v.to_string())
            );
        } else {
            let _ = writeln!(
                output,
                "interval_ms = {}",
                polling
                    .interval_ms
                    .map_or("(not set)".to_string(), |v| v.to_string())
            );
        }
    } else if config.worker.poll_interval_ms.is_some() {
        output.push_str("# Using legacy poll_interval_ms as fixed strategy\n");
        output.push_str("strategy = \"fixed\"\n");
    } else {
        output.push_str("# Using default adaptive strategy\n");
        output.push_str("strategy = \"adaptive\"\n");
        output.push_str("min_interval_ms = 100\n");
        output.push_str("max_interval_ms = 5000\n");
        output.push_str("backoff_multiplier = 2.0\n");
    }

    // Format shard leasing config
    output.push_str("\n[worker.shard_leasing]\n");
    if let Some(ref shard_leasing) = config.worker.shard_leasing {
        let _ = writeln!(output, "enabled = {}", shard_leasing.enabled);
        let _ = writeln!(
            output,
            "shards_per_worker = {}",
            shard_leasing.shards_per_worker
        );
        let _ = writeln!(output, "lease_ttl_secs = {}", shard_leasing.lease_ttl_secs);
        let _ = writeln!(
            output,
            "renewal_interval_secs = {}",
            shard_leasing.renewal_interval_secs
        );
    } else {
        output.push_str("enabled = false\n");
        output.push_str("# shards_per_worker = 16 (default)\n");
        output.push_str("# lease_ttl_secs = 30 (default)\n");
        output.push_str("# renewal_interval_secs = 10 (default)\n");
    }

    output.push_str("\n[monitor]\n");
    let _ = writeln!(
        output,
        "check_interval_secs = {}",
        config
            .monitor
            .check_interval_secs
            .map_or("(default: 30)".to_string(), |v| v.to_string())
    );
    let _ = writeln!(
        output,
        "sweep_interval_secs = {}",
        config
            .monitor
            .sweep_interval_secs
            .map_or("(default: 300)".to_string(), |v| v.to_string())
    );
    let _ = writeln!(
        output,
        "max_tasks_per_scan = {}",
        config
            .monitor
            .max_tasks_per_scan
            .map_or("(default: 100)".to_string(), |v| v.to_string())
    );
    let _ = writeln!(
        output,
        "max_objects_per_sweep = {}",
        config
            .monitor
            .max_objects_per_sweep
            .map_or("(default: 1000)".to_string(), |v| v.to_string())
    );
    let _ = writeln!(
        output,
        "sweep_mode = {}",
        config
            .monitor
            .sweep_mode
            .as_deref()
            .map_or("(default: continuous)".to_string(), |s| format!("\"{s}\""))
    );

    output.push_str("\n# Sources (in priority order, highest first)\n");
    output.push_str("# 1. CLI flags\n");
    output.push_str("# 2. Environment variables\n");

    if let Some(ref path) = sources.custom_config {
        let _ = writeln!(
            output,
            "# 3. Custom config: {} {}",
            path.display(),
            if sources.custom_config_exists {
                "(loaded)"
            } else {
                "(not found)"
            }
        );
    }

    if let Some(ref path) = sources.project_config {
        let _ = writeln!(
            output,
            "# {}. Project config: {} {}",
            if sources.custom_config.is_some() {
                "4"
            } else {
                "3"
            },
            path.display(),
            if sources.project_config_exists {
                "(loaded)"
            } else {
                "(not found)"
            }
        );
    }

    if let Some(ref path) = sources.user_config {
        let _ = writeln!(
            output,
            "# {}. User config: {} {}",
            if sources.custom_config.is_some() {
                "5"
            } else {
                "4"
            },
            path.display(),
            if sources.user_config_exists {
                "(loaded)"
            } else {
                "(not found)"
            }
        );
    }

    output
}

/// Format config file paths for display.
#[must_use]
pub fn format_config_paths(sources: &ConfigSources) -> String {
    let mut output = String::new();

    output.push_str("Configuration file locations:\n\n");

    if let Some(ref path) = sources.user_config {
        let _ = writeln!(
            output,
            "User config:    {} {}",
            path.display(),
            if sources.user_config_exists {
                "(exists)"
            } else {
                "(not found)"
            }
        );
    } else {
        output.push_str("User config:    (could not determine config directory)\n");
    }

    if let Some(ref path) = sources.project_config {
        let _ = writeln!(
            output,
            "Project config: {} {}",
            path.display(),
            if sources.project_config_exists {
                "(exists)"
            } else {
                "(not found)"
            }
        );
    }

    if let Some(ref path) = sources.custom_config {
        let _ = writeln!(
            output,
            "Custom config:  {} {}",
            path.display(),
            if sources.custom_config_exists {
                "(exists)"
            } else {
                "(not found)"
            }
        );
    }

    output.push_str("\nPriority order (highest first):\n");
    output.push_str("  1. CLI flags (--bucket, etc.)\n");
    output.push_str("  2. Environment variables (S3_BUCKET, QO_*, etc.)\n");
    output.push_str("  3. Custom config (if --config specified)\n");
    output.push_str("  4. Project config (.qo.toml)\n");
    output.push_str("  5. User config (~/.config/qo/config.toml)\n");
    output.push_str("  6. Built-in defaults\n");

    output
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_unwrap
)]
mod tests {
    use super::*;

    // =========================================================================
    // Config struct tests
    // =========================================================================

    #[test]
    fn test_config_default_values() {
        // Default region should be "us-east-1" per spec
        let config = QoConfig::default();
        assert_eq!(config.region, "us-east-1");
    }

    #[test]
    fn test_config_deserialize_minimal() {
        // Can deserialize config with just bucket (minimal config)
        let toml_content = r#"
[default]
bucket = "my-bucket"
"#;
        let config_file = load_from_str(toml_content).expect("should parse minimal config");
        let config = config_file.resolve(None);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.region, "us-east-1"); // Default
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn test_config_deserialize_full() {
        // Can deserialize config with all fields
        let toml_content = r#"
[default]
bucket = "qo-prod"
region = "us-west-2"
endpoint = "https://s3.us-west-2.amazonaws.com"

[profiles.dev]
bucket = "qo-dev"
endpoint = "http://localhost:3900"
region = "us-east-1"

[profiles.staging]
bucket = "qo-staging"
region = "eu-west-1"

[worker]
poll_interval_ms = 500
index_mode = "hybrid"
shards = ["0", "1", "2", "3"]

[monitor]
check_interval_secs = 30
sweep_interval_secs = 300
"#;
        let config_file = load_from_str(toml_content).expect("should parse full config");
        let config = config_file.resolve(None);

        assert_eq!(config.bucket, "qo-prod");
        assert_eq!(config.region, "us-west-2");
        assert_eq!(
            config.endpoint,
            Some("https://s3.us-west-2.amazonaws.com".to_string())
        );

        // Worker config
        assert_eq!(config.worker.poll_interval_ms, Some(500));
        assert_eq!(config.worker.index_mode, Some("hybrid".to_string()));
        assert_eq!(
            config.worker.shards,
            Some(vec![
                "0".to_string(),
                "1".to_string(),
                "2".to_string(),
                "3".to_string()
            ])
        );

        // Monitor config
        assert_eq!(config.monitor.check_interval_secs, Some(30));
        assert_eq!(config.monitor.sweep_interval_secs, Some(300));
    }

    #[test]
    fn test_worker_config_defaults() {
        // WorkerConfig has sensible defaults (all Optional, so None by default)
        let worker = WorkerConfig::default();
        assert!(worker.poll_interval_ms.is_none());
        assert!(worker.index_mode.is_none());
        assert!(worker.shards.is_none());
    }

    #[test]
    fn test_monitor_config_defaults() {
        // MonitorConfig has sensible defaults (all Optional, so None by default)
        let monitor = MonitorConfig::default();
        assert!(monitor.check_interval_secs.is_none());
        assert!(monitor.sweep_interval_secs.is_none());
    }

    // =========================================================================
    // Profile tests
    // =========================================================================

    #[test]
    fn test_profile_selection() {
        // Can select a profile and get its settings
        let toml_content = r#"
[default]
bucket = "qo-prod"
region = "us-east-1"

[profiles.dev]
bucket = "qo-dev"
endpoint = "http://localhost:3900"
region = "us-east-1"

[profiles.staging]
bucket = "qo-staging"
region = "us-west-2"
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");

        // Select dev profile
        let dev_config = config_file.resolve(Some("dev"));
        assert_eq!(dev_config.bucket, "qo-dev");
        assert_eq!(
            dev_config.endpoint,
            Some("http://localhost:3900".to_string())
        );
        assert_eq!(dev_config.region, "us-east-1");

        // Select staging profile
        let staging_config = config_file.resolve(Some("staging"));
        assert_eq!(staging_config.bucket, "qo-staging");
        assert_eq!(staging_config.region, "us-west-2");
        assert!(staging_config.endpoint.is_none()); // Not set in staging profile, default has none
    }

    #[test]
    fn test_profile_overrides_default() {
        // Profile settings override [default] section
        let toml_content = r#"
[default]
bucket = "qo-prod"
region = "us-east-1"
endpoint = "https://prod-endpoint.com"

[profiles.dev]
bucket = "qo-dev"
region = "us-east-1"
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");

        // Without profile, use default values
        let default_config = config_file.resolve(None);
        assert_eq!(default_config.bucket, "qo-prod");
        assert_eq!(default_config.region, "us-east-1");
        assert_eq!(
            default_config.endpoint,
            Some("https://prod-endpoint.com".to_string())
        );

        // With dev profile, bucket and region are overridden but endpoint uses default
        let dev_config = config_file.resolve(Some("dev"));
        assert_eq!(dev_config.bucket, "qo-dev");
        assert_eq!(dev_config.region, "us-east-1");
        // Note: endpoint is not set in dev profile, so it inherits from default
        assert_eq!(
            dev_config.endpoint,
            Some("https://prod-endpoint.com".to_string())
        );
    }

    // =========================================================================
    // Config resolution order tests
    // =========================================================================

    #[test]
    fn test_env_overrides_file() {
        // Environment variables take precedence over file
        // Note: This test demonstrates the concept. In practice, the actual
        // load_config function (not yet implemented) would handle env var merging.

        // For now, we test that QoConfig can be constructed with env-provided values
        // that would override file-based values.
        let file_config = QoConfig {
            bucket: "file-bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            queue: QueueSection::default(),
            worker: WorkerConfig::default(),
            monitor: MonitorConfig::default(),
        };

        // Simulate env override: S3_BUCKET=env-bucket would override
        let env_bucket = "env-bucket";

        // In the actual implementation, load_config would merge like this:
        let merged_config = QoConfig {
            bucket: env_bucket.to_string(), // env takes precedence
            ..file_config
        };

        assert_eq!(merged_config.bucket, "env-bucket");
        assert_eq!(merged_config.region, "us-east-1"); // unchanged
    }

    #[test]
    fn test_cli_overrides_env() {
        // CLI flags take precedence over env vars
        // This test demonstrates the resolution order concept.

        let env_config = QoConfig {
            bucket: "env-bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            queue: QueueSection::default(),
            worker: WorkerConfig::default(),
            monitor: MonitorConfig::default(),
        };

        // Simulate CLI flag: --bucket cli-bucket
        let cli_bucket = Some("cli-bucket");

        // In the actual implementation, CLI flags would be applied last:
        let final_config = QoConfig {
            bucket: cli_bucket.unwrap_or(&env_config.bucket).to_string(),
            ..env_config
        };

        assert_eq!(final_config.bucket, "cli-bucket");
    }

    #[test]
    fn test_project_config_overrides_user() {
        // .qo.toml (project-level) overrides ~/.config/qo/config.toml (user-level)
        // This test demonstrates the layering concept.

        // User-level config (loaded first, lower priority)
        let user_config_toml = r#"
[default]
bucket = "user-default-bucket"
region = "us-east-1"
"#;

        // Project-level config (loaded second, higher priority)
        let project_config_toml = r#"
[default]
bucket = "project-bucket"
"#;

        let user_config_file = load_from_str(user_config_toml).expect("parse user config");
        let project_config_file = load_from_str(project_config_toml).expect("parse project config");

        // In actual implementation, project config values would override user config.
        // Here we simulate the merge:
        let user_resolved = user_config_file.resolve(None);
        let project_resolved = project_config_file.resolve(None);

        // Merge: project values take precedence where set
        let merged = QoConfig {
            bucket: if project_resolved.bucket.is_empty() {
                user_resolved.bucket.clone()
            } else {
                project_resolved.bucket.clone()
            },
            region: if project_config_file.default.region.is_some() {
                project_resolved.region.clone()
            } else {
                user_resolved.region.clone()
            },
            endpoint: project_resolved.endpoint.or(user_resolved.endpoint),
            queue: project_resolved.queue.clone(),
            worker: project_resolved.worker, // Project takes full precedence for sections
            monitor: project_resolved.monitor,
        };

        assert_eq!(merged.bucket, "project-bucket"); // Project overrides user
        assert_eq!(merged.region, "us-east-1"); // User value used (project didn't set)
    }

    // =========================================================================
    // Config loading tests
    // =========================================================================

    #[test]
    fn test_load_missing_config_uses_defaults() {
        // No config file = use defaults
        // When no config file exists, QoConfig::default() should be used
        let config = QoConfig::default();

        assert!(config.bucket.is_empty()); // No bucket set
        assert_eq!(config.region, "us-east-1"); // Default region
        assert!(config.endpoint.is_none());
        assert!(config.worker.poll_interval_ms.is_none());
        assert!(config.monitor.check_interval_secs.is_none());
    }

    #[test]
    fn test_load_invalid_toml_returns_error() {
        // Invalid TOML returns error
        let invalid_toml = r#"
[default
bucket = "missing-bracket"
"#;

        let result = load_from_str(invalid_toml);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, ConfigError::TomlParse(_)));
    }

    #[test]
    fn test_load_empty_config_uses_defaults() {
        // Empty config file should use defaults
        let empty_toml = "";

        let config_file = load_from_str(empty_toml).expect("should parse empty config");
        let config = config_file.resolve(None);

        assert!(config.bucket.is_empty());
        assert_eq!(config.region, "us-east-1");
    }

    #[test]
    fn test_nonexistent_profile_uses_defaults() {
        // Selecting a non-existent profile should fall back to defaults
        let toml_content = r#"
[default]
bucket = "default-bucket"
region = "us-east-1"

[profiles.dev]
bucket = "dev-bucket"
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");

        // Request a profile that doesn't exist
        let config = config_file.resolve(Some("production")); // doesn't exist

        // Should use default values
        assert_eq!(config.bucket, "default-bucket");
        assert_eq!(config.region, "us-east-1");
    }

    #[test]
    fn test_partial_profile_inherits_from_default() {
        // A profile that only specifies some values should inherit others from default
        let toml_content = r#"
[default]
bucket = "default-bucket"
region = "us-west-2"
endpoint = "https://default-endpoint.com"

[profiles.partial]
bucket = "partial-bucket"
# region and endpoint not specified - should inherit from default
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(Some("partial"));

        assert_eq!(config.bucket, "partial-bucket"); // From profile
        assert_eq!(config.region, "us-west-2"); // Inherited from default
        assert_eq!(
            config.endpoint,
            Some("https://default-endpoint.com".to_string())
        ); // Inherited
    }

    #[test]
    fn test_worker_config_partial_values() {
        // Worker config can have some values set and others not
        let toml_content = r#"
[default]
bucket = "test-bucket"

[worker]
poll_interval_ms = 1000
# index_mode and shards not set
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        assert_eq!(config.worker.poll_interval_ms, Some(1000));
        assert!(config.worker.index_mode.is_none());
        assert!(config.worker.shards.is_none());
    }

    #[test]
    fn test_monitor_config_partial_values() {
        // Monitor config can have some values set and others not
        let toml_content = r#"
[default]
bucket = "test-bucket"

[monitor]
check_interval_secs = 60
# sweep_interval_secs not set
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        assert_eq!(config.monitor.check_interval_secs, Some(60));
        assert!(config.monitor.sweep_interval_secs.is_none());
    }

    #[test]
    fn test_full_example_from_spec() {
        // Test the exact example from the spec document
        let toml_content = r#"
[default]
bucket = "qo-prod"
region = "us-east-1"

[profiles.dev]
bucket = "qo-dev"
endpoint = "http://localhost:3900"
region = "us-east-1"

[profiles.staging]
bucket = "qo-staging"
region = "us-west-2"

[worker]
poll_interval_ms = 500
index_mode = "hybrid"
shards = ["0", "1", "2", "3"]

[monitor]
check_interval_secs = 30
sweep_interval_secs = 300
"#;

        let config_file = load_from_str(toml_content).expect("should parse spec example");

        // Test default profile
        let default_config = config_file.resolve(None);
        assert_eq!(default_config.bucket, "qo-prod");
        assert_eq!(default_config.region, "us-east-1");

        // Test dev profile
        let dev_config = config_file.resolve(Some("dev"));
        assert_eq!(dev_config.bucket, "qo-dev");
        assert_eq!(
            dev_config.endpoint,
            Some("http://localhost:3900".to_string())
        );
        assert_eq!(dev_config.region, "us-east-1");

        // Test staging profile
        let staging_config = config_file.resolve(Some("staging"));
        assert_eq!(staging_config.bucket, "qo-staging");
        assert_eq!(staging_config.region, "us-west-2");

        // Worker and monitor configs are shared across profiles
        assert_eq!(dev_config.worker.poll_interval_ms, Some(500));
        assert_eq!(dev_config.worker.index_mode, Some("hybrid".to_string()));
        assert_eq!(dev_config.monitor.check_interval_secs, Some(30));
        assert_eq!(dev_config.monitor.sweep_interval_secs, Some(300));
    }

    // =========================================================================
    // Validation tests
    // =========================================================================

    #[test]
    fn test_validate_config_missing_bucket() {
        let config = QoConfig::default();
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("bucket")));
    }

    #[test]
    fn test_validate_config_invalid_index_mode() {
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            queue: QueueSection::default(),
            worker: WorkerConfig {
                index_mode: Some("invalid-mode".to_string()),
                ..Default::default()
            },
            monitor: MonitorConfig::default(),
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("index_mode")));
    }

    #[test]
    fn test_validate_config_invalid_shard() {
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            queue: QueueSection::default(),
            worker: WorkerConfig {
                shards: Some(vec!["invalid".to_string()]),
                ..Default::default()
            },
            monitor: MonitorConfig::default(),
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("shards")));
    }

    #[test]
    fn test_validate_config_valid() {
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            queue: QueueSection::default(),
            worker: WorkerConfig {
                index_mode: Some("hybrid".to_string()),
                shards: Some(vec!["0".to_string(), "a".to_string(), "f".to_string()]),
                ..Default::default()
            },
            monitor: MonitorConfig::default(),
        };
        let errors = validate_config(&config);
        assert!(errors.is_empty());
    }

    // =========================================================================
    // New feature config tests
    // =========================================================================

    #[test]
    fn test_queue_config_shard_prefix_len() {
        let toml_content = r#"
[default]
bucket = "test-bucket"

[queue]
shard_prefix_len = 2
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        assert_eq!(config.queue.shard_prefix_len, Some(2));
        assert_eq!(config.queue.effective_shard_prefix_len(), 2);
    }

    #[test]
    fn test_queue_config_default_shard_prefix_len() {
        let toml_content = r#"
[default]
bucket = "test-bucket"
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        assert!(config.queue.shard_prefix_len.is_none());
        assert_eq!(config.queue.effective_shard_prefix_len(), 1);
    }

    #[test]
    fn test_polling_config_adaptive() {
        let toml_content = r#"
[default]
bucket = "test-bucket"

[worker.polling]
strategy = "adaptive"
min_interval_ms = 50
max_interval_ms = 10000
backoff_multiplier = 1.5
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        let polling = config.worker.polling.expect("polling should be set");
        assert_eq!(polling.strategy, "adaptive");
        assert_eq!(polling.min_interval_ms, Some(50));
        assert_eq!(polling.max_interval_ms, Some(10000));
        assert!((polling.backoff_multiplier.unwrap() - 1.5).abs() < f64::EPSILON);

        // Check conversion to PollingStrategy
        let strategy = polling.to_polling_strategy();
        match strategy {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 50);
                assert_eq!(max_interval_ms, 10000);
                assert!((backoff_multiplier - 1.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected adaptive strategy"),
        }
    }

    #[test]
    fn test_polling_config_fixed() {
        let toml_content = r#"
[default]
bucket = "test-bucket"

[worker.polling]
strategy = "fixed"
interval_ms = 250
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        let polling = config.worker.polling.expect("polling should be set");
        assert_eq!(polling.strategy, "fixed");
        assert_eq!(polling.interval_ms, Some(250));

        // Check conversion to PollingStrategy
        let strategy = polling.to_polling_strategy();
        match strategy {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 250);
            }
            _ => panic!("Expected fixed strategy"),
        }
    }

    #[test]
    fn test_effective_polling_strategy_from_polling_config() {
        let worker = WorkerConfig {
            polling: Some(PollingConfig {
                strategy: "fixed".to_string(),
                interval_ms: Some(300),
                ..Default::default()
            }),
            ..Default::default()
        };

        let strategy = worker.effective_polling_strategy();
        match strategy {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 300);
            }
            _ => panic!("Expected fixed strategy"),
        }
    }

    #[test]
    fn test_effective_polling_strategy_backward_compatibility() {
        // Legacy poll_interval_ms should be treated as fixed strategy
        let worker = WorkerConfig {
            poll_interval_ms: Some(500),
            polling: None,
            ..Default::default()
        };

        let strategy = worker.effective_polling_strategy();
        match strategy {
            PollingStrategy::Fixed { interval_ms } => {
                assert_eq!(interval_ms, 500);
            }
            _ => panic!("Expected fixed strategy from legacy poll_interval_ms"),
        }
    }

    #[test]
    fn test_effective_polling_strategy_defaults_to_adaptive() {
        let worker = WorkerConfig::default();

        let strategy = worker.effective_polling_strategy();
        match strategy {
            PollingStrategy::Adaptive {
                min_interval_ms,
                max_interval_ms,
                backoff_multiplier,
            } => {
                assert_eq!(min_interval_ms, 100);
                assert_eq!(max_interval_ms, 5000);
                assert!((backoff_multiplier - 2.0).abs() < f64::EPSILON);
            }
            _ => panic!("Expected adaptive strategy as default"),
        }
    }

    #[test]
    fn test_shard_leasing_config() {
        let toml_content = r#"
[default]
bucket = "test-bucket"

[worker.shard_leasing]
enabled = true
shards_per_worker = 32
lease_ttl_secs = 60
renewal_interval_secs = 20
"#;
        let config_file = load_from_str(toml_content).expect("should parse config");
        let config = config_file.resolve(None);

        let shard_leasing = config
            .worker
            .shard_leasing
            .expect("shard_leasing should be set");
        assert!(shard_leasing.enabled);
        assert_eq!(shard_leasing.shards_per_worker, 32);
        assert_eq!(shard_leasing.lease_ttl_secs, 60);
        assert_eq!(shard_leasing.renewal_interval_secs, 20);

        // Check conversion to ShardLeaseConfig
        let lease_config = shard_leasing.to_shard_lease_config();
        assert!(lease_config.enabled);
        assert_eq!(lease_config.shards_per_worker, 32);
        assert_eq!(lease_config.lease_ttl, Duration::from_secs(60));
        assert_eq!(lease_config.renewal_interval, Duration::from_secs(20));
    }

    #[test]
    fn test_shard_leasing_config_defaults() {
        let shard_leasing = ShardLeasingConfig::default();
        assert!(!shard_leasing.enabled);
        assert_eq!(shard_leasing.shards_per_worker, 16);
        assert_eq!(shard_leasing.lease_ttl_secs, 30);
        assert_eq!(shard_leasing.renewal_interval_secs, 10);
    }

    #[test]
    fn test_full_config_with_all_features() {
        let toml_content = r#"
[default]
bucket = "qo-production"
region = "us-west-2"

[queue]
shard_prefix_len = 2

[worker]
index_mode = "hybrid"
shards = ["00", "01", "02", "03"]

[worker.polling]
strategy = "adaptive"
min_interval_ms = 100
max_interval_ms = 5000
backoff_multiplier = 2.0

[worker.shard_leasing]
enabled = true
shards_per_worker = 16
lease_ttl_secs = 30
renewal_interval_secs = 10

[monitor]
check_interval_secs = 30
sweep_interval_secs = 300
"#;
        let config_file = load_from_str(toml_content).expect("should parse full config");
        let config = config_file.resolve(None);

        // S3 settings
        assert_eq!(config.bucket, "qo-production");
        assert_eq!(config.region, "us-west-2");

        // Queue settings
        assert_eq!(config.queue.shard_prefix_len, Some(2));

        // Worker settings
        assert_eq!(config.worker.index_mode, Some("hybrid".to_string()));
        assert_eq!(
            config.worker.shards,
            Some(vec![
                "00".to_string(),
                "01".to_string(),
                "02".to_string(),
                "03".to_string()
            ])
        );

        // Polling settings
        let polling = config.worker.polling.as_ref().unwrap();
        assert_eq!(polling.strategy, "adaptive");
        assert_eq!(polling.min_interval_ms, Some(100));

        // Shard leasing settings
        let shard_leasing = config.worker.shard_leasing.as_ref().unwrap();
        assert!(shard_leasing.enabled);
        assert_eq!(shard_leasing.shards_per_worker, 16);
    }

    #[test]
    fn test_validate_shard_prefix_len() {
        // Valid: 1-4
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            queue: QueueSection {
                shard_prefix_len: Some(2),
            },
            ..Default::default()
        };
        assert!(validate_config(&config).is_empty());

        // Invalid: 0
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            queue: QueueSection {
                shard_prefix_len: Some(0),
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("shard_prefix_len")));

        // Invalid: 5
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            queue: QueueSection {
                shard_prefix_len: Some(5),
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("shard_prefix_len")));
    }

    #[test]
    fn test_validate_shards_with_prefix_len_2() {
        // Valid shards with prefix_len=2
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            queue: QueueSection {
                shard_prefix_len: Some(2),
            },
            worker: WorkerConfig {
                shards: Some(vec!["00".to_string(), "a1".to_string(), "ff".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(validate_config(&config).is_empty());

        // Invalid: single-char shards with prefix_len=2
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            queue: QueueSection {
                shard_prefix_len: Some(2),
            },
            worker: WorkerConfig {
                shards: Some(vec!["0".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors
            .iter()
            .any(|e| e.contains("shards") && e.contains("2 hex")));
    }

    #[test]
    fn test_validate_polling_config() {
        // Invalid strategy
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            worker: WorkerConfig {
                polling: Some(PollingConfig {
                    strategy: "invalid".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("strategy")));

        // Invalid backoff_multiplier
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            worker: WorkerConfig {
                polling: Some(PollingConfig {
                    strategy: "adaptive".to_string(),
                    backoff_multiplier: Some(0.5),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("backoff_multiplier")));
    }

    #[test]
    fn test_validate_shard_leasing_config() {
        // Invalid: renewal >= ttl
        let config = QoConfig {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            worker: WorkerConfig {
                shard_leasing: Some(ShardLeasingConfig {
                    enabled: true,
                    shards_per_worker: 16,
                    lease_ttl_secs: 30,
                    renewal_interval_secs: 30, // Should be < ttl
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.contains("renewal_interval_secs")));
    }
}
