//! qo - S3-only distributed task queue

use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::CommandFactory;
use clap_complete::generate;
use qo::cli::{Cli, Commands, ConfigCommands, ScheduleCommands, SchemaCommands};
use qo::config::{
    format_config, format_config_paths, load_config, validate_config, ConfigSources, QoConfig,
};
use qo::migration::ShardMigration;
use qo::models::{Task, TaskStatus, WorkerInfo};
use qo::queue::{Queue, SubmitOptions, TaskSchema};
use qo::storage::{PutCondition, S3Client, S3Config, StorageError};
use qo::worker::{
    shutdown_signal, wait_for_shutdown_signal, IndexMode, MonitorConfig, RunnerConfig,
    TimeoutMonitor, Worker,
};
use serde::Serialize;
use tracing_subscriber::EnvFilter;

/// Displays an error with rich context and suggestions if available.
///
/// This function checks if the error can be downcast to known error types
/// that support rich display (with suggestions), and formats them nicely.
/// Falls back to standard error display for other error types.
fn display_error(err: &anyhow::Error) {
    // Try to extract and display rich errors
    if let Some(storage_err) = err.downcast_ref::<StorageError>() {
        eprintln!("{}", storage_err.display_rich());
        return;
    }

    if let Some(task_err) = err.downcast_ref::<qo::TaskOperationError>() {
        eprintln!("{}", task_err.display_rich());
        return;
    }

    // Check the error chain for storage errors
    for cause in err.chain() {
        if let Some(storage_err) = cause.downcast_ref::<StorageError>() {
            eprintln!("Error: {}\n", err);
            eprintln!("Caused by: {}", storage_err.display_rich());
            return;
        }
    }

    // Default: just print the error
    eprintln!("Error: {}", err);
}

/// Convert QoConfig to S3Config for client initialization.
fn qo_config_to_s3_config(config: &QoConfig) -> Result<S3Config> {
    if config.bucket.is_empty() {
        return Err(anyhow!(
            "bucket not configured. Set S3_BUCKET environment variable or add to config file."
        ));
    }
    if config.region.is_empty() {
        return Err(anyhow!(
            "region not configured. Set S3_REGION environment variable or add to config file."
        ));
    }

    Ok(S3Config::new(
        config.endpoint.clone(),
        config.bucket.clone(),
        config.region.clone(),
    ))
}

fn parse_index_mode(raw: &str) -> Result<IndexMode> {
    match raw.to_lowercase().as_str() {
        "index-only" | "index" => Ok(IndexMode::IndexOnly),
        "hybrid" => Ok(IndexMode::Hybrid),
        "log-scan" | "log" => Ok(IndexMode::LogScan),
        _ => Err(anyhow!(
            "Invalid index mode '{}'. Use index-only, hybrid, or log-scan.",
            raw
        )),
    }
}

/// Parse a human-readable duration string like "1h", "30m", "1d", "90s".
fn parse_duration(s: &str) -> Option<chrono::Duration> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return None;
    }

    // Find where the numeric part ends
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if num_end == 0 {
        return None;
    }

    let num: i64 = s[..num_end].parse().ok()?;
    let unit = s[num_end..].trim();

    match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => chrono::Duration::try_seconds(num),
        "m" | "min" | "mins" | "minute" | "minutes" => chrono::Duration::try_minutes(num),
        "h" | "hr" | "hrs" | "hour" | "hours" => chrono::Duration::try_hours(num),
        "d" | "day" | "days" => chrono::Duration::try_days(num),
        "w" | "week" | "weeks" => chrono::Duration::try_weeks(num),
        "" => chrono::Duration::try_seconds(num), // Default to seconds if no unit
        _ => None,
    }
}

fn allow_no_versioning() -> bool {
    match env::var("QO_ALLOW_NO_VERSIONING") {
        Ok(value) => matches!(value.to_lowercase().as_str(), "1" | "true" | "yes"),
        Err(_) => false,
    }
}

/// Run the doctor command to check environment and S3 connectivity.
async fn run_doctor(
    write_test: bool,
    profile: Option<&str>,
    custom_config: Option<&PathBuf>,
) -> Result<()> {
    println!("qo doctor - checking environment\n");

    let mut all_passed = true;
    let mut suggestions: Vec<String> = Vec::new();

    // Try to load config first
    let qo_config = match load_config(profile, custom_config) {
        Ok(cfg) => {
            if !cfg.bucket.is_empty() {
                println!("\u{2713} bucket is configured ({})", cfg.bucket);
            }
            if !cfg.region.is_empty() {
                println!("\u{2713} region is configured ({})", cfg.region);
            }
            Some(cfg)
        }
        Err(e) => {
            println!("\u{26A0} Config loading error: {}", e);
            None
        }
    };

    // Check S3_BUCKET
    let bucket = match qo_config.as_ref().and_then(|c| {
        if c.bucket.is_empty() {
            None
        } else {
            Some(c.bucket.clone())
        }
    }) {
        Some(val) => {
            println!("\u{2713} S3_BUCKET is set ({})", val);
            Some(val)
        }
        None => {
            println!("\u{2717} S3_BUCKET is not set");
            all_passed = false;
            suggestions.push(
                "Set S3_BUCKET environment variable or add bucket to config file".to_string(),
            );
            None
        }
    };

    // Check S3_REGION
    let region = match qo_config.as_ref().and_then(|c| {
        if c.region.is_empty() {
            None
        } else {
            Some(c.region.clone())
        }
    }) {
        Some(val) => {
            println!("\u{2713} S3_REGION is set ({})", val);
            Some(val)
        }
        None => {
            println!("\u{2717} S3_REGION is not set");
            all_passed = false;
            suggestions.push(
                "Set S3_REGION environment variable or add region to config file".to_string(),
            );
            None
        }
    };

    // Check AWS credentials
    let has_access_key = env::var("AWS_ACCESS_KEY_ID").is_ok();
    let has_secret_key = env::var("AWS_SECRET_ACCESS_KEY").is_ok();

    if has_access_key && has_secret_key {
        println!("\u{2713} AWS credentials configured");
    } else if has_access_key {
        println!("\u{26A0} AWS_ACCESS_KEY_ID set but AWS_SECRET_ACCESS_KEY missing");
        all_passed = false;
        suggestions.push("Set AWS_SECRET_ACCESS_KEY environment variable".to_string());
    } else if has_secret_key {
        println!("\u{26A0} AWS_SECRET_ACCESS_KEY set but AWS_ACCESS_KEY_ID missing");
        all_passed = false;
        suggestions.push("Set AWS_ACCESS_KEY_ID environment variable".to_string());
    } else {
        println!("\u{2717} AWS credentials not configured");
        all_passed = false;
        suggestions.push(
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables".to_string(),
        );
    }

    // If we don't have bucket and region, we can't continue
    let (Some(bucket), Some(region)) = (bucket, region) else {
        println!();
        print_suggestions(&suggestions);
        std::process::exit(1);
    };

    // Try to initialize S3 client
    let endpoint = qo_config.as_ref().and_then(|c| c.endpoint.clone());
    let config = S3Config::new(endpoint.clone(), bucket.clone(), region);

    let client = match S3Client::new(config).await {
        Ok(c) => {
            println!("\u{2713} S3 client initialized");
            c
        }
        Err(e) => {
            println!("\u{2717} S3 client initialization failed: {}", e);
            if let Some(ref ep) = endpoint {
                suggestions.push(format!("Check S3_ENDPOINT is correct (current: {})", ep));
            }
            suggestions.push("Ensure AWS credentials are valid".to_string());
            println!();
            print_suggestions(&suggestions);
            std::process::exit(1);
        }
    };

    // Check bucket versioning (also validates bucket exists and is accessible)
    if allow_no_versioning() {
        println!(
            "\u{26A0} Bucket '{}' versioning check skipped (QO_ALLOW_NO_VERSIONING)",
            bucket
        );
    } else {
        match client.ensure_bucket_versioning_enabled().await {
            Ok(()) => {
                println!(
                    "\u{2713} Bucket '{}' accessible (versioning enabled)",
                    bucket
                );
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("versioning") {
                    println!(
                        "\u{26A0} Bucket '{}' accessible but versioning not enabled",
                        bucket
                    );
                    suggestions.push(format!("Enable versioning on bucket '{}'", bucket));
                    all_passed = false;
                } else {
                    println!("\u{2717} Bucket access failed: {}", e);
                    if let Some(ref ep) = endpoint {
                        suggestions.push(format!("Check S3_ENDPOINT is correct (current: {})", ep));
                        suggestions
                            .push("Ensure S3 service is running: docker compose up -d".to_string());
                    }
                    suggestions.push(format!("Verify bucket '{}' exists", bucket));
                    println!();
                    print_suggestions(&suggestions);
                    std::process::exit(1);
                }
            }
        }
    }

    // Try to list objects (shards)
    match client.list_objects("tasks/", 100, None).await {
        Ok((keys, _)) => {
            // Count unique shards
            let shards: std::collections::HashSet<_> = keys
                .iter()
                .filter_map(|k| k.strip_prefix("tasks/"))
                .filter_map(|k| k.split('/').next())
                .collect();
            let shard_count = shards.len();
            if shard_count > 0 {
                println!("\u{2713} Can list objects ({} shards found)", shard_count);
            } else {
                println!("\u{2713} Can list objects (no tasks yet)");
            }
        }
        Err(e) => {
            println!("\u{2717} Failed to list objects: {}", e);
            all_passed = false;
        }
    }

    // Check for healthy workers
    match client.list_objects("workers/", 100, None).await {
        Ok((keys, _)) => {
            let mut healthy_count = 0;
            let mut total_count = 0;
            let now = client.now().await?;
            for key in keys {
                if let Ok((body, _)) = client.get_object(&key).await {
                    if let Ok(info) = serde_json::from_slice::<WorkerInfo>(&body) {
                        total_count += 1;
                        if info.is_healthy_at(now, chrono::Duration::seconds(60)) {
                            healthy_count += 1;
                        }
                    }
                }
            }
            if total_count == 0 {
                println!("\u{26A0} No workers registered");
            } else if healthy_count == total_count {
                println!("\u{2713} {} healthy worker(s) online", healthy_count);
            } else {
                println!(
                    "\u{26A0} {}/{} worker(s) healthy",
                    healthy_count, total_count
                );
            }
        }
        Err(e) => {
            println!("\u{2717} Failed to check workers: {}", e);
            all_passed = false;
        }
    }

    // Optional write test
    if write_test {
        let test_key = format!(".doctor-test/{}", uuid::Uuid::new_v4());
        let test_data = b"qo doctor write test".to_vec();

        // Write test object
        match client
            .put_object(&test_key, test_data, PutCondition::None)
            .await
        {
            Ok(_) => {
                println!("\u{2713} Write test: created test object");

                // Delete test object
                match client.delete_object(&test_key).await {
                    Ok(()) => {
                        println!("\u{2713} Write test: deleted test object");
                    }
                    Err(e) => {
                        println!("\u{2717} Write test: failed to delete test object: {}", e);
                        all_passed = false;
                    }
                }
            }
            Err(e) => {
                println!("\u{2717} Write test failed: {}", e);
                all_passed = false;
                suggestions.push("Check that AWS credentials have write permissions".to_string());
            }
        }
    }

    println!();

    if all_passed {
        println!("All checks passed!");
        Ok(())
    } else {
        print_suggestions(&suggestions);
        std::process::exit(1);
    }
}

/// Print suggestions for fixing issues.
fn print_suggestions(suggestions: &[String]) {
    if !suggestions.is_empty() {
        println!("Suggestions:");
        for suggestion in suggestions {
            println!("  - {}", suggestion);
        }
    }
}

/// A task event for the tail output.
#[derive(Debug, Clone, Serialize)]
struct TailEvent {
    timestamp: DateTime<Utc>,
    task_id: String,
    task_type: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Previous state of a task for detecting changes.
#[derive(Debug, Clone)]
struct TaskState {
    status: TaskStatus,
    updated_at: DateTime<Utc>,
}

/// Run the tail command to stream live task events.
async fn run_tail(
    queue: &Queue,
    task_type_filter: Option<&str>,
    status_filter: Option<TaskStatus>,
    json_output: bool,
    limit: Option<usize>,
    interval_ms: u64,
) -> Result<()> {
    let mut seen_tasks: HashMap<String, TaskState> = HashMap::new();
    let mut event_count: usize = 0;
    let shards: Vec<String> = (0..16).map(|i| format!("{i:x}")).collect();

    // Set up signal handler for graceful shutdown
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(true);
    });

    loop {
        // Check for shutdown
        if *shutdown_rx.borrow() {
            break;
        }

        // Check if we've reached the limit
        if let Some(max) = limit {
            if event_count >= max {
                break;
            }
        }

        // Poll all shards
        for shard in &shards {
            let tasks = match queue.list(shard, None, 1000).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(shard = %shard, error = %e, "Failed to list tasks in shard");
                    continue;
                }
            };

            for task in tasks {
                let task_id_str = task.id.to_string();

                // Apply task type filter
                if let Some(tt_filter) = task_type_filter {
                    if task.task_type != tt_filter {
                        continue;
                    }
                }

                // Apply status filter
                if let Some(sf) = status_filter {
                    if task.status != sf {
                        continue;
                    }
                }

                // Check if this is a new task or status change
                let is_new_event = match seen_tasks.get(&task_id_str) {
                    None => true, // New task
                    Some(prev) => {
                        // Status changed or updated_at changed
                        prev.status != task.status || prev.updated_at != task.updated_at
                    }
                };

                if is_new_event {
                    let prev_state = seen_tasks.get(&task_id_str).cloned();

                    // Update seen state
                    seen_tasks.insert(
                        task_id_str.clone(),
                        TaskState {
                            status: task.status,
                            updated_at: task.updated_at,
                        },
                    );

                    // Generate event
                    let event = create_tail_event(&task, prev_state.as_ref());
                    print_tail_event(&event, json_output)?;

                    event_count += 1;

                    // Check limit after each event
                    if let Some(max) = limit {
                        if event_count >= max {
                            return Ok(());
                        }
                    }
                }
            }
        }

        // Wait before next poll
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Create a tail event from a task.
fn create_tail_event(task: &Task, prev_state: Option<&TaskState>) -> TailEvent {
    let status_str = match task.status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Archived => "archived",
        TaskStatus::Expired => "expired",
    };

    // Calculate duration for completed/failed tasks
    let duration_ms = if matches!(task.status, TaskStatus::Completed | TaskStatus::Failed) {
        task.completed_at
            .map(|completed| (completed - task.created_at).num_milliseconds())
    } else {
        None
    };

    // Extract error message for failed tasks
    let error = if task.status == TaskStatus::Failed {
        task.last_error.clone()
    } else {
        None
    };

    // Determine if this is a new task
    let _is_new_task = prev_state.is_none();

    TailEvent {
        timestamp: task.updated_at,
        task_id: task.id.to_string()[..8].to_string(), // Short ID
        task_type: task.task_type.clone(),
        status: status_str.to_string(),
        worker_id: task.worker_id.clone(),
        duration_ms,
        error,
    }
}

/// Print a tail event to stdout.
fn print_tail_event(event: &TailEvent, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string(event)?);
    } else {
        let time_str = event.timestamp.format("%H:%M:%S").to_string();

        // Build details string
        let details = match event.status.as_str() {
            "pending" => "(new task)".to_string(),
            "running" => {
                if let Some(ref worker) = event.worker_id {
                    format!("(worker: {})", worker)
                } else {
                    "(claimed)".to_string()
                }
            }
            "completed" => {
                if let Some(ms) = event.duration_ms {
                    format!("({}ms)", ms)
                } else {
                    "(done)".to_string()
                }
            }
            "failed" => {
                if let Some(ref err) = event.error {
                    // Truncate error message if too long
                    let truncated = if err.len() > 50 {
                        format!("{}...", &err[..47])
                    } else {
                        err.clone()
                    };
                    format!("(error: \"{}\")", truncated)
                } else {
                    "(failed)".to_string()
                }
            }
            "cancelled" => {
                if let Some(ref err) = event.error {
                    // Truncate error message if too long
                    let truncated = if err.len() > 50 {
                        format!("{}...", &err[..47])
                    } else {
                        err.clone()
                    };
                    format!("(reason: \"{}\")", truncated)
                } else {
                    "(cancelled)".to_string()
                }
            }
            "archived" => "(archived)".to_string(),
            "expired" => "(expired)".to_string(),
            _ => String::new(),
        };

        // Color the status based on type (using ANSI codes)
        let status_colored = match event.status.as_str() {
            "pending" => format!("\x1b[33m{}\x1b[0m", event.status), // Yellow
            "running" => format!("\x1b[34m{}\x1b[0m", event.status), // Blue
            "completed" => format!("\x1b[32m{}\x1b[0m", event.status), // Green
            "failed" => format!("\x1b[31m{}\x1b[0m", event.status),  // Red
            "cancelled" => format!("\x1b[35m{}\x1b[0m", event.status), // Magenta
            "archived" => format!("\x1b[90m{}\x1b[0m", event.status), // Gray
            "expired" => format!("\x1b[90m{}\x1b[0m", event.status), // Gray
            _ => event.status.clone(),
        };

        println!(
            "[{}] {} {:<14} {:<9} {}",
            time_str, event.task_id, event.task_type, status_colored, details
        );
    }

    // Flush stdout to ensure immediate output
    io::stdout().flush()?;

    Ok(())
}

/// Parse a status string to TaskStatus.
fn parse_status_filter(status: &str) -> Option<TaskStatus> {
    match status.to_lowercase().as_str() {
        "pending" => Some(TaskStatus::Pending),
        "running" => Some(TaskStatus::Running),
        "completed" => Some(TaskStatus::Completed),
        "failed" => Some(TaskStatus::Failed),
        "cancelled" => Some(TaskStatus::Cancelled),
        "archived" => Some(TaskStatus::Archived),
        "expired" => Some(TaskStatus::Expired),
        _ => None,
    }
}

/// Run the scheduler daemon that triggers recurring schedules.
async fn run_scheduler(
    queue: &Queue,
    manager: &qo::queue::ScheduleManager,
    interval_secs: u64,
) -> Result<()> {
    use tokio::time::{interval, Duration};

    let mut tick = interval(Duration::from_secs(interval_secs));

    loop {
        tick.tick().await;

        let now = Utc::now();
        let report = qo::queue::run_scheduler_tick(queue, manager, now).await?;

        for run in report.runs {
            println!(
                "[{}] Triggered schedule '{}' -> task {}",
                now.format("%Y-%m-%d %H:%M:%S"),
                run.schedule_id,
                run.task_id
            );
        }

        for err in report.errors {
            eprintln!("{}", err.message);
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(e) = run().await {
        display_error(&e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse_args();

    // Handle completions before S3 init (doesn't need credentials)
    if let Commands::Completions { shell } = cli.command {
        let mut cmd = Cli::command();
        generate(shell, &mut cmd, "qo", &mut io::stdout());
        return Ok(());
    }

    // Handle config commands before S3 init (doesn't need credentials)
    if let Commands::Config { command } = &cli.command {
        let sources = ConfigSources::discover().with_custom_config(cli.config.clone());
        let config = load_config(cli.profile.as_deref(), cli.config.as_ref())?;

        match command {
            ConfigCommands::Show => {
                print!("{}", format_config(&config, &sources));
            }
            ConfigCommands::Validate => {
                let errors = validate_config(&config);
                if errors.is_empty() {
                    println!("Configuration is valid.");
                } else {
                    eprintln!("Configuration errors:");
                    for error in &errors {
                        eprintln!("  - {}", error);
                    }
                    std::process::exit(1);
                }
            }
            ConfigCommands::Paths => {
                print!("{}", format_config_paths(&sources));
            }
        }
        return Ok(());
    }

    // Handle doctor command - tries to connect but handles failures gracefully
    if let Commands::Doctor { write_test } = cli.command {
        return run_doctor(write_test, cli.profile.as_deref(), cli.config.as_ref()).await;
    }

    // Load configuration using the new config system
    let qo_config = load_config(cli.profile.as_deref(), cli.config.as_ref())?;
    let s3_config = qo_config_to_s3_config(&qo_config)?;

    // Initialize S3 client
    let client = S3Client::new(s3_config).await?;
    if allow_no_versioning() {
        tracing::warn!(
            "Bucket versioning check skipped (QO_ALLOW_NO_VERSIONING). History/audit guarantees are disabled."
        );
    } else {
        client.ensure_bucket_versioning_enabled().await?;
    }
    let queue = Queue::new(client);

    match cli.command {
        Commands::Completions { .. } | Commands::Doctor { .. } | Commands::Config { .. } => {
            unreachable!()
        }
        Commands::Submit {
            task_type,
            input,
            timeout,
            retries,
            max_reschedules,
            at,
            delay,
            ttl,
            expires_at,
            json,
        } => {
            let input_value: serde_json::Value = serde_json::from_str(&input)?;

            // Parse schedule_at from --at or --delay
            let schedule_at = if let Some(at_str) = at {
                Some(
                    DateTime::parse_from_rfc3339(&at_str)
                        .map_err(|e| anyhow!("Invalid datetime '{}': {}. Use RFC3339 format like 2026-01-28T09:00:00Z", at_str, e))?
                        .with_timezone(&Utc),
                )
            } else if let Some(delay_str) = delay {
                let duration = parse_duration(&delay_str).ok_or_else(|| {
                    anyhow!(
                        "Invalid duration '{}'. Use format like 1h, 30m, 1d, 90s",
                        delay_str
                    )
                })?;
                Some(Utc::now() + duration)
            } else {
                None
            };

            // Parse TTL/expiration from --ttl or --expires-at
            let (ttl_seconds, expires_at_parsed) = if let Some(ttl_str) = ttl {
                let duration = parse_duration(&ttl_str).ok_or_else(|| {
                    anyhow!(
                        "Invalid TTL duration '{}'. Use format like 1h, 30m, 1d, 7d",
                        ttl_str
                    )
                })?;
                (Some(duration.num_seconds() as u64), None)
            } else if let Some(expires_str) = expires_at {
                let expires = DateTime::parse_from_rfc3339(&expires_str)
                    .map_err(|e| anyhow!("Invalid expires_at datetime '{}': {}. Use RFC3339 format like 2026-01-28T09:00:00Z", expires_str, e))?
                    .with_timezone(&Utc);
                (None, Some(expires))
            } else {
                (None, None)
            };

            let options = SubmitOptions {
                timeout_seconds: timeout,
                max_retries: retries,
                max_reschedules,
                schedule_at,
                ttl_seconds,
                expires_at: expires_at_parsed,
                ..Default::default()
            };
            let task = queue.submit(&task_type, input_value, options).await?;

            if json {
                println!("{}", serde_json::to_string_pretty(&task)?);
            } else if let Some(expires) = task.expires_at {
                if let Some(scheduled) = schedule_at {
                    println!(
                        "Task submitted: {} (scheduled for {}, expires at {})",
                        task.id, scheduled, expires
                    );
                } else {
                    println!("Task submitted: {} (expires at {})", task.id, expires);
                }
            } else if let Some(scheduled) = schedule_at {
                println!("Task submitted: {} (scheduled for {})", task.id, scheduled);
            } else {
                println!("Task submitted: {}", task.id);
            }
        }

        Commands::Status { task_id, json } => match queue.get(task_id).await? {
            Some((task, _)) => {
                let now = queue.now().await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&task)?);
                } else {
                    println!("Task: {}", task.id);
                    println!("  Type: {}", task.task_type);
                    println!("  Status: {:?}", task.status);
                    println!("  Shard: {}", task.shard);
                    println!("  Attempt: {}", task.attempt);
                    println!("  Retry Count: {}/{}", task.retry_count, task.max_retries);

                    // Show available_at if task is in backoff (future)
                    if task.available_at > now {
                        let wait_secs = (task.available_at - now).num_seconds();
                        println!("  Available In: {}s (backoff)", wait_secs);
                    }

                    // Show lease_expires_at countdown if running
                    if let Some(expires) = task.lease_expires_at {
                        let remaining = (expires - now).num_seconds();
                        if remaining > 0 {
                            println!("  Lease Expires In: {}s", remaining);
                        } else {
                            println!("  Lease: EXPIRED");
                        }
                    }

                    // Show task expiration (TTL)
                    if let Some(expires_at) = task.expires_at {
                        let remaining = (expires_at - now).num_seconds();
                        if remaining > 0 {
                            println!("  Expires At: {} (in {}s)", expires_at, remaining);
                        } else {
                            println!("  Expires At: {} (EXPIRED)", expires_at);
                        }
                    }

                    // Show when task was marked as expired
                    if let Some(expired_at) = task.expired_at {
                        println!("  Expired At: {}", expired_at);
                    }

                    if let Some(ref worker) = task.worker_id {
                        println!("  Worker: {}", worker);
                    }
                    if let Some(ref error) = task.last_error {
                        println!("  Last Error: {}", error);
                    }
                    println!("  Created: {}", task.created_at);
                    println!("  Updated: {}", task.updated_at);
                }
            }
            None => {
                let err = qo::TaskOperationError::not_found(task_id);
                eprintln!("{}", err.display_rich());
                std::process::exit(1);
            }
        },

        Commands::History { task_id, json } => {
            let history = queue.get_history(task_id).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&history)?);
            } else {
                for (i, task) in history.iter().enumerate() {
                    println!("Version {}: {:?} at {}", i, task.status, task.updated_at);
                }
            }
        }

        Commands::List {
            shard,
            status,
            task_type,
            limit,
            json,
        } => {
            let status_filter = status
                .as_ref()
                .and_then(|s| match s.to_lowercase().as_str() {
                    "pending" => Some(TaskStatus::Pending),
                    "running" => Some(TaskStatus::Running),
                    "completed" => Some(TaskStatus::Completed),
                    "failed" => Some(TaskStatus::Failed),
                    "cancelled" => Some(TaskStatus::Cancelled),
                    "archived" => Some(TaskStatus::Archived),
                    "expired" => Some(TaskStatus::Expired),
                    _ => None,
                });

            let shards: Vec<String> = if let Some(s) = shard {
                vec![s]
            } else {
                (0..16).map(|i| format!("{i:x}")).collect()
            };

            let mut all_tasks = Vec::new();
            for s in shards {
                let tasks = queue.list(&s, status_filter, limit).await?;
                all_tasks.extend(tasks);
            }

            // Filter by task type if specified
            let all_tasks: Vec<_> = if let Some(ref tt) = task_type {
                all_tasks
                    .into_iter()
                    .filter(|t| t.task_type == *tt)
                    .collect()
            } else {
                all_tasks
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&all_tasks)?);
            } else {
                for task in &all_tasks {
                    println!(
                        "{} | {} | {:?} | {}",
                        task.id, task.task_type, task.status, task.updated_at
                    );
                }
                println!("Total: {} tasks", all_tasks.len());
            }
        }

        Commands::Replay { task_id } => {
            qo::worker::replay_task(&queue, task_id).await?;
            println!("Task {} replayed", task_id);
        }

        Commands::Archive { task_id } => {
            qo::worker::archive_task(&queue, task_id).await?;
            println!("Task {} archived", task_id);
        }

        Commands::Cancel {
            task_ids,
            task_type,
            reason,
            cancelled_by,
            json: json_output,
        } => {
            if let Some(task_type) = task_type {
                // Cancel all pending tasks of a type
                let result = queue
                    .cancel_by_type(&task_type, reason.as_deref(), cancelled_by.as_deref())
                    .await?;

                if json_output {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "cancelled": result.cancelled.len(),
                            "failed": result.failed.len(),
                            "tasks": result.cancelled,
                        }))?
                    );
                } else {
                    println!(
                        "Cancelled {} tasks of type '{}' ({} failed)",
                        result.cancelled.len(),
                        task_type,
                        result.failed.len()
                    );
                    for task in &result.cancelled {
                        println!("  {} ({})", task.id, task.task_type);
                    }
                }
            } else if task_ids.len() > 1 {
                // Cancel multiple specific tasks
                let results = queue
                    .cancel_many(task_ids.clone(), reason.as_deref(), cancelled_by.as_deref())
                    .await;

                let mut cancelled = Vec::new();
                let mut failed = Vec::new();
                for (task_id, result) in task_ids.iter().zip(results.into_iter()) {
                    match result {
                        Ok(task) => cancelled.push(task),
                        Err(e) => failed.push((*task_id, e.to_string())),
                    }
                }

                if json_output {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "cancelled": cancelled.len(),
                            "failed": failed.len(),
                            "tasks": cancelled,
                            "errors": failed,
                        }))?
                    );
                } else {
                    println!(
                        "Cancelled {} tasks ({} failed)",
                        cancelled.len(),
                        failed.len()
                    );
                    for task in &cancelled {
                        println!("  {} ({})", task.id, task.task_type);
                    }
                    for (id, err) in &failed {
                        println!("  {} FAILED: {}", id, err);
                    }
                }
            } else if task_ids.len() == 1 {
                // Cancel a single task
                let task_id = task_ids[0];
                let task = queue
                    .cancel_with_reason(task_id, reason.as_deref(), cancelled_by.as_deref())
                    .await?;

                if json_output {
                    println!("{}", serde_json::to_string_pretty(&task)?);
                } else {
                    println!("Task {} cancelled", task_id);
                    if let Some(ref r) = reason {
                        println!("  Reason: {}", r);
                    }
                    if let Some(ref by) = cancelled_by {
                        println!("  Cancelled by: {}", by);
                    }
                }
            } else {
                // No task IDs provided and no --task-type
                anyhow::bail!("Must provide task IDs or --task-type");
            }
        }

        Commands::Workers { json } => {
            let (keys, _) = queue.client().list_objects("workers/", 100, None).await?;
            let mut workers = Vec::new();
            for key in keys {
                if let Ok((body, _)) = queue.client().get_object(&key).await {
                    if let Ok(info) = serde_json::from_slice::<WorkerInfo>(&body) {
                        workers.push(info);
                    }
                }
            }

            if json {
                println!("{}", serde_json::to_string_pretty(&workers)?);
            } else {
                let now = queue.now().await?;
                for worker in &workers {
                    let health = if worker.is_healthy_at(now, chrono::Duration::seconds(60)) {
                        "healthy"
                    } else {
                        "stale"
                    };
                    println!(
                        "{} | {} | completed: {} | failed: {}",
                        worker.worker_id, health, worker.tasks_completed, worker.tasks_failed
                    );
                }
            }
        }

        Commands::Worker {
            id,
            shards,
            index_mode,
            ready_page_size,
            log_scan_page_size,
            no_monitor,
        } => {
            let worker_id = id.unwrap_or_else(|| {
                let uuid_str = uuid::Uuid::new_v4().to_string();
                let short_id = uuid_str.split('-').next().unwrap_or("unknown");
                format!("worker-{short_id}")
            });

            // Use shards from config if not specified on command line
            let shard_list: Vec<String> = shards
                .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
                .or_else(|| qo_config.worker.shards.clone())
                .unwrap_or_else(|| (0..16).map(|i| format!("{i:x}")).collect());

            let monitor_queue = queue.clone();
            let mut worker = Worker::new(queue, worker_id, shard_list);

            let (shutdown_tx, shutdown_rx) = shutdown_signal();

            // Spawn signal handler
            tokio::spawn(wait_for_shutdown_signal(shutdown_tx));

            let with_monitor = !no_monitor;
            if with_monitor {
                let mut monitor = TimeoutMonitor::new(monitor_queue, MonitorConfig::default());
                let monitor_shutdown = shutdown_rx.clone();
                let _monitor_handle = tokio::spawn(async move {
                    if let Err(e) = monitor.run(monitor_shutdown).await {
                        tracing::warn!(error = %e, "Timeout monitor exited with error");
                    }
                });
            } else {
                tracing::warn!(
                    "Embedded monitor disabled. Ensure `qo monitor` is running to prevent task stalls."
                );
            }

            // Use index_mode from config if CLI uses default
            let effective_index_mode = if index_mode == "hybrid" {
                qo_config
                    .worker
                    .index_mode
                    .as_deref()
                    .unwrap_or(&index_mode)
            } else {
                &index_mode
            };

            let runner_config = RunnerConfig {
                index_mode: parse_index_mode(effective_index_mode)?,
                ready_page_size,
                log_scan_page_size,
                ..RunnerConfig::default()
            };

            worker.run(runner_config, shutdown_rx).await?;
        }

        Commands::Serve { port } => {
            let app = qo::web::create_router(queue);
            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
            println!("Dashboard running at http://localhost:{}", port);
            axum::serve(listener, app).await?;
        }

        Commands::Monitor {
            check_interval,
            worker_health_threshold,
            sweep_interval,
            sweep_page_size,
        } => {
            let (shutdown_tx, shutdown_rx) = shutdown_signal();
            tokio::spawn(wait_for_shutdown_signal(shutdown_tx));

            // Use config values as defaults, CLI overrides
            let effective_check_interval = if check_interval == 30 {
                qo_config
                    .monitor
                    .check_interval_secs
                    .unwrap_or(check_interval)
            } else {
                check_interval
            };
            let effective_sweep_interval = if sweep_interval == 300 {
                qo_config
                    .monitor
                    .sweep_interval_secs
                    .unwrap_or(sweep_interval)
            } else {
                sweep_interval
            };

            let config = MonitorConfig {
                check_interval: std::time::Duration::from_secs(effective_check_interval),
                worker_health_threshold: chrono::Duration::seconds(worker_health_threshold),
                sweep_interval: if effective_sweep_interval == 0 {
                    None
                } else {
                    Some(std::time::Duration::from_secs(effective_sweep_interval))
                },
                sweep_page_size,
                max_tasks_per_scan: qo_config.monitor.max_tasks_per_scan.unwrap_or(100),
            };
            let mut monitor = TimeoutMonitor::new(queue, config);
            monitor.run(shutdown_rx).await?;
        }

        Commands::Tail {
            task_type,
            status,
            json,
            limit,
            interval,
        } => {
            let status_filter = status.as_ref().and_then(|s| parse_status_filter(s));

            if status.is_some() && status_filter.is_none() {
                eprintln!(
                    "Invalid status filter. Use: pending, running, completed, failed, or archived"
                );
                std::process::exit(1);
            }

            run_tail(
                &queue,
                task_type.as_deref(),
                status_filter,
                json,
                limit,
                interval,
            )
            .await?;
        }

        Commands::MigrateShards {
            from,
            to,
            execute,
            concurrency,
            json,
        } => {
            // Validate prefix lengths
            if !(1..=4).contains(&from) {
                eprintln!("Error: --from must be between 1 and 4");
                std::process::exit(1);
            }
            if !(1..=4).contains(&to) {
                eprintln!("Error: --to must be between 1 and 4");
                std::process::exit(1);
            }
            if from == to {
                eprintln!("Error: --from and --to must be different");
                std::process::exit(1);
            }

            let dry_run = !execute;
            let migration = ShardMigration::new(queue.client().clone(), from, to, dry_run)
                .with_concurrency(concurrency);

            if dry_run {
                // Analyze only
                let plan = migration.analyze().await?;

                if json {
                    println!("{}", serde_json::to_string_pretty(&plan)?);
                } else {
                    println!("Shard Migration Analysis");
                    println!("========================");
                    println!();
                    println!(
                        "Prefix length: {} -> {} ({} shards -> {} shards)",
                        from,
                        to,
                        16_usize.pow(from as u32),
                        16_usize.pow(to as u32)
                    );
                    println!();
                    println!("Objects found:");
                    println!("  Tasks:         {}", plan.task_count);
                    println!("  Ready indexes: {}", plan.ready_index_count);
                    println!("  Lease indexes: {}", plan.lease_index_count);
                    println!("  Total:         {}", plan.total_objects);
                    println!();
                    println!("Migration impact:");
                    println!("  Will move:       {}", plan.moves.len());
                    println!("  Already correct: {}", plan.already_correct);
                    println!();

                    if !plan.moves.is_empty() {
                        println!("Sample moves (first 10):");
                        for m in plan.moves.iter().take(10) {
                            println!("  {} -> {}", m.from_key, m.to_key);
                        }
                        if plan.moves.len() > 10 {
                            println!("  ... and {} more", plan.moves.len() - 10);
                        }
                        println!();
                    }

                    println!("To execute this migration, run:");
                    println!("  qo migrate-shards --from {} --to {} --execute", from, to);
                }
            } else {
                // Execute migration
                println!(
                    "Executing shard migration from prefix {} to {}...",
                    from, to
                );
                println!();

                let result = migration.execute().await?;

                if json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    println!("Migration Complete");
                    println!("==================");
                    println!("  Migrated: {}", result.migrated);
                    println!("  Failed:   {}", result.failed);
                    println!("  Skipped:  {}", result.skipped);

                    if !result.errors.is_empty() {
                        println!();
                        println!("Errors:");
                        for err in &result.errors {
                            println!("  - {}", err);
                        }
                    }

                    if result.is_success() {
                        println!();
                        println!("Migration completed successfully!");
                        println!();
                        println!(
                            "IMPORTANT: Update your configuration to use shard_prefix_len={}",
                            to
                        );
                    } else {
                        println!();
                        eprintln!("Migration completed with errors. Some objects may not have been migrated.");
                        std::process::exit(1);
                    }
                }
            }
        }

        Commands::Schema { command } => match command {
            SchemaCommands::Publish {
                task_type,
                schema_file,
            } => {
                let content =
                    std::fs::read_to_string(&schema_file).context("Failed to read schema file")?;
                let schema: TaskSchema =
                    serde_json::from_str(&content).context("Failed to parse schema JSON")?;
                queue.publish_schema(&task_type, &schema).await?;
                println!("Published schema for '{}'", task_type);
            }
            SchemaCommands::Get { task_type } => match queue.get_schema(&task_type).await? {
                Some(schema) => {
                    println!("{}", serde_json::to_string_pretty(&schema)?);
                }
                None => {
                    eprintln!("No schema found for '{}'", task_type);
                    std::process::exit(1);
                }
            },
            SchemaCommands::List => {
                let schemas = queue.list_schemas().await?;
                if schemas.is_empty() {
                    println!("No schemas found");
                } else {
                    for task_type in schemas {
                        println!("{}", task_type);
                    }
                }
            }
            SchemaCommands::Delete { task_type } => {
                queue.delete_schema(&task_type).await?;
                println!("Deleted schema for '{}'", task_type);
            }
            SchemaCommands::Validate {
                task_type,
                input,
                output,
            } => {
                let schema = queue
                    .get_schema(&task_type)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("No schema found for '{}'", task_type))?;

                if let Some(input_json) = input {
                    let data: serde_json::Value =
                        serde_json::from_str(&input_json).context("Failed to parse input JSON")?;
                    match queue.validate_input(&schema, &data) {
                        Ok(()) => println!("Input is valid"),
                        Err(e) => {
                            eprintln!("Validation failed:");
                            for error in &e.errors {
                                eprintln!("  - {}", error);
                            }
                            std::process::exit(1);
                        }
                    }
                } else if let Some(output_json) = output {
                    let data: serde_json::Value = serde_json::from_str(&output_json)
                        .context("Failed to parse output JSON")?;
                    match queue.validate_output(&schema, &data) {
                        Ok(()) => println!("Output is valid"),
                        Err(e) => {
                            eprintln!("Validation failed:");
                            for error in &e.errors {
                                eprintln!("  - {}", error);
                            }
                            std::process::exit(1);
                        }
                    }
                } else {
                    eprintln!("Must specify --input or --output");
                    std::process::exit(1);
                }
            }
        },

        Commands::Schedule { command } => {
            use qo::queue::{Schedule, ScheduleManager};

            let manager = ScheduleManager::new(queue.client().clone());

            match command {
                ScheduleCommands::Create {
                    schedule_id,
                    task_type,
                    input,
                    cron,
                    timeout,
                    retries,
                } => {
                    let input_value: serde_json::Value = serde_json::from_str(&input)?;
                    let mut schedule = Schedule::new(&schedule_id, &task_type, input_value, &cron);
                    schedule.timeout_seconds = timeout;
                    schedule.max_retries = retries;

                    manager.create(&schedule).await?;
                    println!("Schedule created: {}", schedule_id);

                    if let Some(next) = schedule.next_run_after(Utc::now()) {
                        println!("  Next run: {}", next);
                    }
                }

                ScheduleCommands::List { json } => {
                    let schedules = manager.list().await?;

                    if json {
                        println!("{}", serde_json::to_string_pretty(&schedules)?);
                    } else if schedules.is_empty() {
                        println!("No schedules found.");
                    } else {
                        println!(
                            "{:<20} {:<20} {:<20} {:<8}",
                            "ID", "Task Type", "Cron", "Enabled"
                        );
                        println!("{}", "-".repeat(72));
                        for schedule in schedules {
                            println!(
                                "{:<20} {:<20} {:<20} {:<8}",
                                schedule.id,
                                schedule.task_type,
                                schedule.cron,
                                if schedule.enabled { "Yes" } else { "No" }
                            );
                        }
                    }
                }

                ScheduleCommands::Show { schedule_id, json } => {
                    if let Some((schedule, _)) = manager.get(&schedule_id).await? {
                        if json {
                            println!("{}", serde_json::to_string_pretty(&schedule)?);
                        } else {
                            println!("Schedule: {}", schedule.id);
                            println!("  Task Type: {}", schedule.task_type);
                            println!("  Cron: {}", schedule.cron);
                            println!("  Enabled: {}", schedule.enabled);
                            println!("  Created: {}", schedule.created_at);

                            if let Some(next) = schedule.next_run_after(Utc::now()) {
                                println!("  Next Run: {}", next);
                            }

                            if let Ok(Some((last_run, _))) =
                                manager.get_last_run(&schedule_id).await
                            {
                                println!("  Last Run: {}", last_run.last_run_at);
                                println!("  Last Task: {}", last_run.last_task_id);
                            }
                        }
                    } else {
                        eprintln!("Schedule not found: {}", schedule_id);
                        std::process::exit(1);
                    }
                }

                ScheduleCommands::Enable { schedule_id } => {
                    manager.enable(&schedule_id).await?;
                    println!("Schedule enabled: {}", schedule_id);
                }

                ScheduleCommands::Disable { schedule_id } => {
                    manager.disable(&schedule_id).await?;
                    println!("Schedule disabled: {}", schedule_id);
                }

                ScheduleCommands::Delete { schedule_id } => {
                    manager.delete(&schedule_id).await?;
                    println!("Schedule deleted: {}", schedule_id);
                }

                ScheduleCommands::Trigger { schedule_id } => {
                    if let Some((schedule, _)) = manager.get(&schedule_id).await? {
                        let options = SubmitOptions {
                            timeout_seconds: schedule.timeout_seconds,
                            max_retries: schedule.max_retries,
                            ..Default::default()
                        };
                        let task = queue
                            .submit(&schedule.task_type, schedule.input.clone(), options)
                            .await?;
                        println!(
                            "Task submitted: {} (from schedule {})",
                            task.id, schedule_id
                        );
                    } else {
                        eprintln!("Schedule not found: {}", schedule_id);
                        std::process::exit(1);
                    }
                }

                ScheduleCommands::Run { interval } => {
                    println!("Starting scheduler (interval: {}s)...", interval);
                    run_scheduler(&queue, &manager, interval).await?;
                }
            }
        }
    }

    Ok(())
}
