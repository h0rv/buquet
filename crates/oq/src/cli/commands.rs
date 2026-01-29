//! CLI commands for the oq task queue.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use uuid::Uuid;

/// oq - S3-only task queue CLI
#[derive(Parser, Debug)]
#[command(name = "oq", version, about = "S3-only distributed task queue")]
pub struct Cli {
    /// Configuration profile to use
    #[arg(long, global = true, env = "OQ_PROFILE")]
    pub profile: Option<String>,

    /// Path to config file (overrides default locations)
    #[arg(long, global = true, env = "OQ_CONFIG")]
    pub config: Option<PathBuf>,

    /// The subcommand to run
    #[command(subcommand)]
    pub command: Commands,
}

/// Available CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Submit a new task to the queue
    Submit {
        /// Task type (e.g., `send_email`, `process_image`)
        #[arg(long, short = 't')]
        task_type: String,

        /// Task input as JSON string
        #[arg(long, short = 'i')]
        input: String,

        /// Timeout in seconds (default: 300)
        #[arg(long)]
        timeout: Option<u64>,

        /// Max retries (default: 3)
        #[arg(long)]
        retries: Option<u32>,

        /// Max reschedules before failing (default: unlimited)
        #[arg(long)]
        max_reschedules: Option<u32>,

        /// Schedule task to run at a specific time (RFC3339, e.g., 2026-01-28T09:00:00Z)
        #[arg(long, value_name = "DATETIME", conflicts_with = "delay")]
        at: Option<String>,

        /// Schedule task to run after a delay (e.g., 1h, 30m, 1d, 90s)
        #[arg(long, value_name = "DURATION", conflicts_with = "at")]
        delay: Option<String>,

        /// Task TTL (time-to-live) as duration (e.g., 1h, 30m, 1d, 7d)
        /// Task expires if not processed within this time
        #[arg(long, value_name = "DURATION", conflicts_with = "expires_at")]
        ttl: Option<String>,

        /// Absolute expiration time (RFC3339, e.g., 2026-01-28T09:00:00Z)
        #[arg(long, value_name = "DATETIME", conflicts_with = "ttl")]
        expires_at: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Get task status
    Status {
        /// Task ID (UUID)
        task_id: Uuid,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show task version history
    History {
        /// Task ID (UUID)
        task_id: Uuid,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// List tasks
    List {
        /// Filter by shard (0-f)
        #[arg(long, short = 's')]
        shard: Option<String>,

        /// Filter by status
        #[arg(long)]
        status: Option<String>,

        /// Filter by task type
        #[arg(short = 't', long)]
        task_type: Option<String>,

        /// Max tasks to return
        #[arg(long, default_value = "100")]
        limit: i32,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Replay a failed task
    Replay {
        /// Task ID (UUID)
        task_id: Uuid,
    },

    /// Archive a completed/failed task
    Archive {
        /// Task ID (UUID)
        task_id: Uuid,
    },

    /// Cancel pending task(s)
    Cancel {
        /// Task ID(s) (UUIDs) - omit if using --task-type
        #[arg(conflicts_with = "task_type")]
        task_ids: Vec<Uuid>,

        /// Cancel all pending tasks of this type
        #[arg(long, short = 't', conflicts_with = "task_ids")]
        task_type: Option<String>,

        /// Reason for cancellation (stored in `last_error`)
        #[arg(long, short = 'r')]
        reason: Option<String>,

        /// Who/what is cancelling the task (e.g., "user:alice")
        #[arg(long, short = 'b')]
        cancelled_by: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// List registered workers
    Workers {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Run a worker
    Worker {
        /// Worker ID (default: hostname-random)
        #[arg(long)]
        id: Option<String>,

        /// Shards to poll (comma-separated, default: all)
        #[arg(long)]
        shards: Option<String>,

        /// Index mode: index-only, hybrid, or log-scan
        #[arg(long, env = "OQ_INDEX_MODE", default_value = "hybrid")]
        index_mode: String,

        /// Page size for ready index listing
        #[arg(long, env = "OQ_READY_PAGE_SIZE", default_value = "100")]
        ready_page_size: i32,

        /// Page size for log scan listing
        #[arg(long, env = "OQ_LOG_SCAN_PAGE_SIZE", default_value = "50")]
        log_scan_page_size: i32,

        /// Disable the embedded monitor (advanced/explicit use only)
        #[arg(long, env = "OQ_NO_MONITOR")]
        no_monitor: bool,
    },

    /// Start the web dashboard
    Serve {
        /// Port to listen on
        #[arg(long, default_value = "3000")]
        port: u16,
    },

    /// Run the timeout monitor and index sweeper
    Monitor {
        /// Check interval in seconds
        #[arg(long, env = "OQ_MONITOR_CHECK_INTERVAL", default_value = "30")]
        check_interval: u64,

        /// Worker health threshold in seconds
        #[arg(long, env = "OQ_MONITOR_WORKER_HEALTH_THRESHOLD", default_value = "60")]
        worker_health_threshold: i64,

        /// Sweep interval in seconds (0 disables sweeping)
        #[arg(long, env = "OQ_MONITOR_SWEEP_INTERVAL", default_value = "300")]
        sweep_interval: u64,

        /// Page size for sweep list operations
        #[arg(long, env = "OQ_MONITOR_SWEEP_PAGE_SIZE", default_value = "1000")]
        sweep_page_size: i32,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },

    /// Check environment and S3 connectivity
    Doctor {
        /// Run write test (creates and deletes a test object)
        #[arg(long)]
        write_test: bool,
    },

    /// Stream live task events
    Tail {
        /// Filter by task type
        #[arg(short = 't', long)]
        task_type: Option<String>,

        /// Filter by status (pending, running, completed, failed)
        #[arg(long)]
        status: Option<String>,

        /// Output as JSON (one event per line)
        #[arg(long)]
        json: bool,

        /// Stop after N events
        #[arg(long)]
        limit: Option<usize>,

        /// Poll interval in milliseconds
        #[arg(long, default_value = "1000")]
        interval: u64,
    },

    /// Manage task schemas
    Schema {
        /// The schema subcommand to run
        #[command(subcommand)]
        command: SchemaCommands,
    },

    /// Manage configuration
    Config {
        /// The config subcommand to run
        #[command(subcommand)]
        command: ConfigCommands,
    },

    /// Migrate tasks to a new shard prefix length
    MigrateShards {
        /// Source shard prefix length (1-4)
        #[arg(long)]
        from: usize,

        /// Target shard prefix length (1-4)
        #[arg(long)]
        to: usize,

        /// Actually execute the migration (default: dry-run)
        #[arg(long)]
        execute: bool,

        /// Concurrency level for migration operations
        #[arg(long, default_value = "10")]
        concurrency: usize,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Manage recurring schedules
    Schedule {
        /// The schedule subcommand to run
        #[command(subcommand)]
        command: ScheduleCommands,
    },
}

/// Schema management subcommands
#[derive(Subcommand, Debug)]
pub enum SchemaCommands {
    /// Publish a schema for a task type
    Publish {
        /// The task type
        task_type: String,
        /// Path to JSON schema file
        schema_file: PathBuf,
    },
    /// Get a schema for a task type
    Get {
        /// The task type
        task_type: String,
    },
    /// List all task types with schemas
    List,
    /// Delete a schema
    Delete {
        /// The task type
        task_type: String,
    },
    /// Validate data against a schema
    Validate {
        /// The task type
        task_type: String,
        /// Input JSON to validate (mutually exclusive with --output)
        #[arg(long, conflicts_with = "output")]
        input: Option<String>,
        /// Output JSON to validate (mutually exclusive with --input)
        #[arg(long, conflicts_with = "input")]
        output: Option<String>,
    },
}

/// Configuration management subcommands
#[derive(Subcommand, Debug)]
pub enum ConfigCommands {
    /// Show resolved configuration
    Show,
    /// Validate configuration
    Validate,
    /// Show config file locations
    Paths,
}

/// Schedule management subcommands
#[derive(Subcommand, Debug)]
pub enum ScheduleCommands {
    /// Create a new recurring schedule
    Create {
        /// Schedule ID (unique identifier)
        schedule_id: String,

        /// Task type to submit
        #[arg(long, short = 't')]
        task_type: String,

        /// Task input as JSON string
        #[arg(long, short = 'i', default_value = "{}")]
        input: String,

        /// Cron expression (5-field: minute hour day month weekday)
        #[arg(long, short = 'c')]
        cron: String,

        /// Task timeout in seconds
        #[arg(long)]
        timeout: Option<u64>,

        /// Task max retries
        #[arg(long)]
        retries: Option<u32>,
    },

    /// List all schedules
    List {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show schedule details
    Show {
        /// Schedule ID
        schedule_id: String,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Enable a schedule
    Enable {
        /// Schedule ID
        schedule_id: String,
    },

    /// Disable a schedule
    Disable {
        /// Schedule ID
        schedule_id: String,
    },

    /// Delete a schedule
    Delete {
        /// Schedule ID
        schedule_id: String,
    },

    /// Trigger a schedule immediately (outside normal schedule)
    Trigger {
        /// Schedule ID
        schedule_id: String,
    },

    /// Run the scheduler daemon
    Run {
        /// Check interval in seconds
        #[arg(long, default_value = "60")]
        interval: u64,
    },
}

impl Cli {
    /// Parse CLI arguments
    #[must_use]
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
