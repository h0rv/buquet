# Configuration File Support

> **Status:** Implemented
> **Effort:** ~150 lines of Rust
> **Tier:** 3 (Polish)

Load settings from `.oq.toml` or `~/.config/oq/config.toml`.

## Why

- Environment variables are verbose and error-prone
- Teams want shared configuration
- Different environments (dev/staging/prod) need profiles

## Config File Format

```toml
# .oq.toml (project-level) or ~/.config/oq/config.toml (user-level)

[default]
bucket = "oq-prod"
region = "us-east-1"

[profiles.dev]
bucket = "oq-dev"
endpoint = "http://localhost:3900"
region = "us-east-1"

[profiles.staging]
bucket = "oq-staging"
region = "us-west-2"

[worker]
poll_interval_ms = 500
index_mode = "hybrid"
shards = ["0", "1", "2", "3"]

[monitor]
check_interval_secs = 30
sweep_interval_secs = 300
```

## Usage

```bash
# Use default profile
oq list

# Use specific profile
oq --profile=dev list

# Override with env vars (always takes precedence)
S3_BUCKET=oq-test oq list
```

## Config Resolution Order

1. CLI flags (highest priority)
2. Environment variables
3. Profile-specific settings (if `--profile` specified)
4. Project-level `.oq.toml`
5. User-level `~/.config/oq/config.toml`
6. Built-in defaults (lowest priority)

## Implementation

```rust
use config::{Config, File, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct OqConfig {
    bucket: String,
    endpoint: Option<String>,
    region: String,
    #[serde(default)]
    worker: WorkerConfig,
    #[serde(default)]
    monitor: MonitorConfig,
}

#[derive(Debug, Deserialize, Default)]
struct WorkerConfig {
    poll_interval_ms: Option<u64>,
    index_mode: Option<String>,
    shards: Option<Vec<String>>,
}

fn load_config(profile: Option<&str>) -> Result<OqConfig> {
    let home = env::var("HOME")?;

    let mut builder = Config::builder()
        // 1. Start with defaults
        .set_default("region", "us-east-1")?

        // 2. Load user-level config
        .add_source(
            File::with_name(&format!("{}/.config/oq/config", home))
                .required(false)
        )

        // 3. Load project-level config
        .add_source(File::with_name(".oq").required(false))

        // 4. Load profile-specific settings
        .set_override_option(
            "bucket",
            profile.map(|p| format!("profiles.{}.bucket", p))
        )?

        // 5. Override with environment variables (highest priority)
        .add_source(
            Environment::with_prefix("S3")
                .separator("_")
                .try_parsing(true)
        )
        .add_source(
            Environment::with_prefix("OQ")
                .separator("_")
                .try_parsing(true)
        );

    builder.build()?.try_deserialize()
}
```

## CLI Changes

```rust
#[derive(Parser)]
struct Cli {
    /// Configuration profile to use
    #[arg(long, global = true)]
    profile: Option<String>,

    /// Path to config file (overrides default locations)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}
```

## Validation

Add `oq config` subcommand:

```bash
# Show resolved configuration
oq config show

# Validate configuration
oq config validate

# Show config file locations
oq config paths
```

## Files to Change

- `Cargo.toml` - Add `config` crate
- `crates/oq/src/config.rs` - New module for config loading
- `crates/oq/src/cli/commands.rs` - Add global `--profile` and `--config` flags
- `crates/oq/src/main.rs` - Use config for S3 client initialization

## Dependencies

`config` crate
