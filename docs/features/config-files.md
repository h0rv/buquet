# Configuration File Support

> **Status:** Implemented
> **Effort:** ~150 lines of Rust
> **Tier:** 3 (Polish)

Load settings from `.qo.toml` or `~/.config/qo/config.toml`.

## Why

- Environment variables are verbose and error-prone
- Teams want shared configuration
- Different environments (dev/staging/prod) need profiles

## Config File Format

```toml
# .qo.toml (project-level) or ~/.config/qo/config.toml (user-level)

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
```

## Usage

```bash
# Use default profile
qo list

# Use specific profile
qo --profile=dev list

# Override with env vars (always takes precedence)
S3_BUCKET=qo-test qo list
```

## Config Resolution Order

1. CLI flags (highest priority)
2. Environment variables
3. Profile-specific settings (if `--profile` specified)
4. Project-level `.qo.toml`
5. User-level `~/.config/qo/config.toml`
6. Built-in defaults (lowest priority)

## Implementation

```rust
use config::{Config, File, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct QoConfig {
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

fn load_config(profile: Option<&str>) -> Result<QoConfig> {
    let home = env::var("HOME")?;

    let mut builder = Config::builder()
        // 1. Start with defaults
        .set_default("region", "us-east-1")?

        // 2. Load user-level config
        .add_source(
            File::with_name(&format!("{}/.config/qo/config", home))
                .required(false)
        )

        // 3. Load project-level config
        .add_source(File::with_name(".qo").required(false))

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
            Environment::with_prefix("QO")
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

Add `qo config` subcommand:

```bash
# Show resolved configuration
qo config show

# Validate configuration
qo config validate

# Show config file locations
qo config paths
```

## Files to Change

- `Cargo.toml` - Add `config` crate
- `crates/qo/src/config.rs` - New module for config loading
- `crates/qo/src/cli/commands.rs` - Add global `--profile` and `--config` flags
- `crates/qo/src/main.rs` - Use config for S3 client initialization

## Dependencies

`config` crate
