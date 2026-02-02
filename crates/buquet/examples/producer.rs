//! Example Rust producer that submits orders and monitors their status.
//!
//! Run with:
//!     cargo run -p buquet --example producer
//!
//! This example demonstrates:
//! - Connecting to buquet via S3
//! - Submitting tasks
//! - Polling for task completion
//! - Polyglot interop (can be consumed by Python worker)

#![allow(clippy::unwrap_used, clippy::expect_used)]

use buquet::models::TaskStatus;
use buquet::queue::{Queue, SubmitOptions};
use buquet::storage::{S3Client, S3Config};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::time::Duration;
use tokio::time::sleep;

// Sample orders to submit
fn sample_orders() -> Vec<serde_json::Value> {
    vec![
        json!({
            "order_id": "rust-order-1",
            "items": [
                {"product_id": "widget", "quantity": 2},
                {"product_id": "gadget", "quantity": 1}
            ]
        }),
        json!({
            "order_id": "rust-order-2",
            "items": [
                {"product_id": "gizmo", "quantity": 3}
            ]
        }),
        json!({
            "order_id": "rust-order-3",
            "items": [
                {"product_id": "doohickey", "quantity": 1},
                {"product_id": "thingamajig", "quantity": 2}
            ]
        }),
    ]
}

// Prices for display
fn get_price(product_id: &str) -> f64 {
    match product_id {
        "widget" => 19.99,
        "gadget" => 5.99,
        "gizmo" => 14.99,
        "doohickey" => 29.99,
        "thingamajig" => 9.99,
        _ => 0.0,
    }
}

fn calculate_expected_total(items: &[serde_json::Value]) -> f64 {
    items
        .iter()
        .map(|item| {
            let product_id = item["product_id"].as_str().unwrap_or("");
            let quantity = item["quantity"].as_u64().unwrap_or(0) as f64;
            get_price(product_id) * quantity
        })
        .sum()
}

fn format_order_summary(order: &serde_json::Value) -> String {
    let items = order["items"].as_array().unwrap();
    let parts: Vec<String> = items
        .iter()
        .map(|item| {
            format!(
                "{}x {}",
                item["quantity"],
                item["product_id"].as_str().unwrap_or("?")
            )
        })
        .collect();
    let total = calculate_expected_total(items);
    format!("{} = ${:.2}", parts.join(", "), total)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("[producer] Rust producer starting...");

    // Get S3 config from environment
    let bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "buquet-dev".to_string());
    let endpoint = env::var("S3_ENDPOINT").ok();
    let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    println!("[producer] Connecting to bucket: {}", bucket);
    if let Some(ref ep) = endpoint {
        println!("[producer] Using endpoint: {}", ep);
    }

    // Create S3 client and queue
    let config = S3Config::new(endpoint, bucket, region);
    let client = S3Client::new(config).await?;
    let queue = Queue::new(client);

    // Submit all orders
    let orders = sample_orders();
    let mut task_ids: HashMap<String, uuid::Uuid> = HashMap::new();

    println!("[producer] Submitting {} orders...", orders.len());
    println!();

    for order in &orders {
        let order_id = order["order_id"].as_str().unwrap().to_string();
        let task = queue
            .submit("process_order", order.clone(), SubmitOptions::default())
            .await?;

        println!(
            "[producer] Submitted {}: {}",
            order_id,
            format_order_summary(order)
        );
        task_ids.insert(order_id, task.id);
    }

    println!();
    println!("[producer] Waiting for tasks to complete...");
    println!();

    // Poll for completion
    let mut pending: std::collections::HashSet<String> = task_ids.keys().cloned().collect();

    while !pending.is_empty() {
        sleep(Duration::from_millis(500)).await;

        for order_id in pending.clone() {
            let task_id = task_ids[&order_id];

            if let Ok(Some((task, _etag))) = queue.get(task_id).await {
                match task.status {
                    TaskStatus::Completed => {
                        if let Some(ref output) = task.output {
                            let total = output["subtotal"].as_f64().unwrap_or(0.0);
                            println!("[producer] {}: completed - Total: ${:.2}", order_id, total);
                        } else {
                            println!("[producer] {}: completed", order_id);
                        }
                        pending.remove(&order_id);
                    }
                    TaskStatus::Failed => {
                        let error = task.last_error.as_deref().unwrap_or("unknown error");
                        println!("[producer] {}: failed - {}", order_id, error);
                        pending.remove(&order_id);
                    }
                    TaskStatus::Running => {
                        // Still processing
                    }
                    TaskStatus::Pending => {
                        // Not yet claimed
                    }
                    TaskStatus::Archived => {
                        println!("[producer] {}: archived", order_id);
                        pending.remove(&order_id);
                    }
                    TaskStatus::Cancelled => {
                        println!("[producer] {}: cancelled", order_id);
                        pending.remove(&order_id);
                    }
                    TaskStatus::Expired => {
                        println!("[producer] {}: expired", order_id);
                        pending.remove(&order_id);
                    }
                }
            }
        }
    }

    println!();
    println!("[producer] All orders processed!");

    Ok(())
}
