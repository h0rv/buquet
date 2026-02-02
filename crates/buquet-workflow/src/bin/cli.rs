//! CLI for buquet-workflow.
//!
//! Provides workflow management commands including the sweeper.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use buquet::storage::{S3Client, S3Config};
use buquet_workflow::state::StateManager;
use buquet_workflow::sweeper::WorkflowSweeper;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

/// buquet-workflow - Workflow orchestration CLI
#[derive(Parser, Debug)]
#[command(
    name = "buquet-workflow",
    version,
    about = "S3-only workflow orchestration"
)]
pub struct Cli {
    /// The subcommand to run
    #[command(subcommand)]
    pub command: Commands,
}

/// Available CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List workflow instances
    List {
        /// Optional prefix to filter workflows
        #[arg(long, default_value = "")]
        prefix: String,

        /// Max workflows to return
        #[arg(long, default_value = "100")]
        limit: i32,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Get workflow status
    Status {
        /// Workflow ID
        workflow_id: String,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Run the workflow sweeper for stall recovery detection
    Sweeper {
        /// Check interval in seconds (ignored if --once is set)
        #[arg(long, default_value = "300")]
        interval: u64,

        /// Run single sweep and exit
        #[arg(long)]
        once: bool,

        /// Optional prefix to filter workflows
        #[arg(long, default_value = "")]
        prefix: String,

        /// Max workflows to scan per sweep
        #[arg(long, default_value = "1000")]
        limit: i32,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("buquet=info".parse()?))
        .init();

    let cli = Cli::parse();

    // Get config from environment
    let bucket = std::env::var("S3_BUCKET").map_err(|_| anyhow::anyhow!("S3_BUCKET not set"))?;
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let region = std::env::var("S3_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());

    // Create S3 client
    let s3_config = S3Config::new(endpoint, bucket, region);
    let client = S3Client::new(s3_config).await?;

    let state_manager = StateManager::new(Arc::new(client));

    match cli.command {
        Commands::List {
            prefix,
            limit,
            json,
        } => {
            let workflows = state_manager.list(&prefix, limit).await?;

            if json {
                println!("{}", serde_json::to_string(&workflows)?);
            } else if workflows.is_empty() {
                println!("No workflows found");
            } else {
                println!("Workflows:");
                for wf in workflows {
                    println!("  {}", wf);
                }
            }
        }

        Commands::Status { workflow_id, json } => match state_manager.get(&workflow_id).await {
            Ok((state, _etag)) => {
                if json {
                    let json_bytes = state.to_json()?;
                    let json_str = String::from_utf8(json_bytes)?;
                    println!("{}", json_str);
                } else {
                    println!("Workflow: {}", state.id);
                    println!("  Type: {}", state.workflow_type);
                    println!("  Status: {:?}", state.status);
                    println!("  Current Steps: {:?}", state.current_steps);
                    println!("  Created: {}", state.created_at);
                    println!("  Updated: {}", state.updated_at);
                    if let Some(error) = &state.error {
                        println!("  Error: {} - {}", error.error_type, error.message);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        },

        Commands::Sweeper {
            interval,
            once,
            prefix,
            limit,
            json,
        } => {
            let sweeper = WorkflowSweeper::new(state_manager);

            loop {
                info!("Running sweep...");
                let result = sweeper.scan_simple(&prefix, limit).await?;

                if json {
                    #[derive(serde::Serialize)]
                    struct JsonResult {
                        scanned: usize,
                        terminal: usize,
                        healthy: usize,
                        needs_recovery: Vec<String>,
                    }
                    let json_result = JsonResult {
                        scanned: result.scanned,
                        terminal: result.terminal,
                        healthy: result.healthy,
                        needs_recovery: result
                            .needs_recovery
                            .iter()
                            .map(|w| w.workflow_id.clone())
                            .collect(),
                    };
                    println!("{}", serde_json::to_string(&json_result)?);
                } else {
                    println!(
                        "Sweep complete: {} scanned, {} terminal, {} healthy, {} need recovery",
                        result.scanned,
                        result.terminal,
                        result.healthy,
                        result.needs_recovery.len()
                    );

                    for w in &result.needs_recovery {
                        println!(
                            "  {} - {:?} (reason: {:?})",
                            w.workflow_id, w.state.status, w.reason
                        );
                    }
                }

                if once {
                    break;
                }

                info!("Sleeping for {}s...", interval);
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }
    }

    Ok(())
}
