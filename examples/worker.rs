//! Example Rust worker that processes orders.
//!
//! Run with:
//!     cargo run -p buquet --example worker
//!
//! This example demonstrates:
//! - Connecting to buquet via S3 (auto-loads .buquet.toml config)
//! - Implementing `TaskHandler` trait
//! - Registering handlers with a Worker
//! - Running the worker loop with graceful shutdown
//! - Polyglot interop (can consume tasks from Python producer)

use async_trait::async_trait;
use buquet::models::TaskError;
use buquet::queue::connect;
use buquet::worker::{
    shutdown_signal, wait_for_shutdown_signal, IndexMode, RunnerConfig, TaskHandler, Worker,
};
use serde_json::{json, Value};
use std::time::Duration;

// Simulated product catalog
fn get_product(product_id: &str) -> Option<(&'static str, f64)> {
    match product_id {
        "widget" => Some(("Widget", 19.99)),
        "gadget" => Some(("Gadget", 5.99)),
        "gizmo" => Some(("Gizmo", 14.99)),
        "doohickey" => Some(("Doohickey", 29.99)),
        "thingamajig" => Some(("Thingamajig", 9.99)),
        _ => None,
    }
}

/// Handler for processing order tasks
struct ProcessOrderHandler;

#[async_trait]
impl TaskHandler for ProcessOrderHandler {
    fn task_type(&self) -> &'static str {
        "process_order"
    }

    async fn handle(&self, input: Value) -> Result<Value, TaskError> {
        let order_id = input["order_id"].as_str().unwrap_or("unknown").to_string();

        let items = input["items"]
            .as_array()
            .ok_or_else(|| TaskError::Permanent("Missing items array".to_string()))?;

        println!("[worker] Processing order {order_id}...");

        let mut line_items = Vec::new();
        let mut subtotal = 0.0;

        for item in items {
            let product_id = item["product_id"]
                .as_str()
                .ok_or_else(|| TaskError::Permanent("Missing product_id".to_string()))?;

            let quantity = item["quantity"]
                .as_u64()
                .ok_or_else(|| TaskError::Permanent("Missing quantity".to_string()))?
                as f64;

            let (name, price) = get_product(product_id)
                .ok_or_else(|| TaskError::Permanent(format!("Unknown product: {product_id}")))?;

            let line_total = price * quantity;
            subtotal += line_total;

            println!(
                "[worker]   - {}x {} @ ${:.2} = ${:.2}",
                quantity as u64, name, price, line_total
            );

            line_items.push(json!({
                "product_id": product_id,
                "name": name,
                "quantity": quantity as u64,
                "unit_price": price,
                "total": (line_total * 100.0).round() / 100.0,
            }));
        }

        // Simulate some processing time
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate occasional transient failures (10% chance)
        if rand::random::<f64>() < 0.1 {
            println!("[worker] Transient error on {order_id}, will retry...");
            return Err(TaskError::Retryable(
                "Payment gateway temporarily unavailable".to_string(),
            ));
        }

        let subtotal = (subtotal * 100.0).round() / 100.0;
        println!("[worker]   Subtotal: ${subtotal:.2}");
        println!("[worker] Completed {order_id}");

        Ok(json!({
            "order_id": order_id,
            "line_items": line_items,
            "subtotal": subtotal,
            "status": "processed",
        }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("[worker] Rust worker starting...");

    // Connect using automatic config resolution (env vars / .buquet.toml)
    let queue = connect(None).await?;
    println!("[worker] Connected to bucket: {}", queue.client().bucket());

    // Create worker with all 16 shards
    let shards: Vec<String> = (0..16).map(|i| format!("{i:x}")).collect();
    let worker_id = format!("rust-worker-{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let mut worker = Worker::new(queue, &worker_id, shards);

    // Register handler
    worker.register_handler(Box::new(ProcessOrderHandler));

    println!("[worker] Worker ID: {worker_id}");
    println!("[worker] Registered handlers: {:?}", worker.handlers());
    println!("[worker] Polling for tasks (Ctrl+C to stop)...");
    println!();

    // Setup shutdown signal
    let (shutdown_tx, shutdown_rx) = shutdown_signal();
    tokio::spawn(wait_for_shutdown_signal(shutdown_tx));

    // Run the worker
    let runner_config = RunnerConfig {
        polling: buquet::worker::PollingStrategy::fixed(500),
        index_mode: IndexMode::Hybrid,
        ..Default::default()
    };

    worker.run(runner_config, shutdown_rx).await?;

    println!("[worker] Shutdown complete");

    Ok(())
}
