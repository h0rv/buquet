//! HTTP handlers for the web UI.

use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{Event, Sse},
        Html,
    },
    routing::{get, post},
    Router,
};
use maud::Markup;
use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};
use tokio_stream::StreamExt;
use uuid::Uuid;

use super::templates;
use crate::models::{TaskStatus, WorkerInfo};
use crate::queue::Queue;

/// Application state shared across handlers.
pub type AppState = Arc<Queue>;

/// Create the web UI router with all routes.
pub fn create_router(queue: Queue) -> Router {
    let state: AppState = Arc::new(queue);

    Router::new()
        .route("/", get(index))
        .route("/view/tasks", get(tasks_view))
        .route("/view/workers", get(workers_view))
        .route("/tasks", get(tasks_partial))
        .route("/task/:id", get(task_detail))
        .route("/task/:id/replay", post(replay_task))
        .route("/task/:id/archive", post(archive_task))
        .route("/workers", get(workers_partial))
        .route("/sse/tasks", get(tasks_sse))
        .with_state(state)
}

async fn index(State(_queue): State<AppState>) -> Html<String> {
    let content = maud::html! {
        header {
            nav class="tabs" {
                button #tab-tasks class="tab active"
                    hx-get="/view/tasks" hx-target="#main-content" hx-swap="innerHTML"
                    onclick="document.querySelectorAll('.tab').forEach(t => t.classList.remove('active')); this.classList.add('active')"
                    { "Tasks" }
                button #tab-workers class="tab"
                    hx-get="/view/workers" hx-target="#main-content" hx-swap="innerHTML"
                    onclick="document.querySelectorAll('.tab').forEach(t => t.classList.remove('active')); this.classList.add('active')"
                    { "Workers" }
            }
            div class="header-right" {
                button #tz-toggle class="tz-toggle" onclick="toggleTimezone()" title="Toggle UTC/Local time" { "UTC" }
                h1 { "qo" }
            }
        }

        div class="layout" {
            main #main-content hx-get="/view/tasks" hx-trigger="load" {}

            aside #detail {
                p class="placeholder" { "Select a task" }
            }
        }
    };

    Html(templates::layout("qo", &content).into_string())
}

/// Time range options for filtering tasks.
const TIME_RANGES: &[(&str, &str)] = &[
    ("15m", "Last 15 min"),
    ("1h", "Last hour"),
    ("6h", "Last 6 hours"),
    ("24h", "Last 24 hours"),
    ("7d", "Last 7 days"),
    ("", "All time"),
];

async fn tasks_view(State(_queue): State<AppState>) -> Markup {
    maud::html! {
        div class="controls" {
            label {
                "Time"
                select #filter-time name="time" hx-get="/tasks" hx-target="#tasks" hx-include="#filter-shard,#filter-status" {
                    @for (value, label) in TIME_RANGES {
                        option value=(*value) selected[*value == "1h"] { (*label) }
                    }
                }
            }
            label {
                "Status"
                select #filter-status name="status" hx-get="/tasks" hx-target="#tasks" hx-include="#filter-shard,#filter-time" {
                    option value="" { "All" }
                    // Exhaustive: uses TaskStatus::ALL so new variants cause compile error
                    @for status in TaskStatus::ALL {
                        option value=(status.as_str()) { (status.display_name()) }
                    }
                }
            }
            label {
                "Shard"
                select #filter-shard name="shard" hx-get="/tasks" hx-target="#tasks" hx-include="#filter-status,#filter-time" {
                    option value="" { "All" }
                    @for i in 0..16 {
                        option value=(format!("{:x}", i)) { (format!("{:x}", i)) }
                    }
                }
            }
            button hx-get="/tasks" hx-target="#tasks" hx-include="#filter-shard,#filter-status,#filter-time" { "Refresh" }
        }
        div hx-ext="sse" sse-connect="/sse/tasks" {
            // SSE refresh includes current filter values
            div #tasks
                hx-get="/tasks"
                hx-trigger="load, sse:refresh"
                hx-include="#filter-shard,#filter-status,#filter-time" {}
        }
    }
}

async fn workers_view(State(_queue): State<AppState>) -> Markup {
    maud::html! {
        div class="controls" {
            button hx-get="/workers" hx-target="#workers" { "Refresh" }
        }
        div #workers hx-get="/workers" hx-trigger="load, every 10s" {}
    }
}

/// Parse time range string to chrono::Duration.
fn parse_time_range(s: &str) -> Option<chrono::Duration> {
    match s {
        "15m" => chrono::Duration::try_minutes(15),
        "1h" => chrono::Duration::try_hours(1),
        "6h" => chrono::Duration::try_hours(6),
        "24h" => chrono::Duration::try_hours(24),
        "7d" => chrono::Duration::try_days(7),
        _ => None,
    }
}

async fn tasks_partial(
    State(queue): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Markup {
    let shard = params.get("shard").filter(|s| !s.is_empty());
    let status = params
        .get("status")
        .filter(|s| !s.is_empty())
        .and_then(|s| TaskStatus::from_str(s));
    let time_range = params
        .get("time")
        .filter(|s| !s.is_empty())
        .and_then(|s| parse_time_range(s));

    let shards: Vec<String> = shard.map_or_else(
        || (0..16).map(|i| format!("{i:x}")).collect(),
        |s| vec![s.clone()],
    );

    let mut tasks = Vec::new();
    for s in shards {
        if let Ok(list) = queue.list(&s, status, 500).await {
            tasks.extend(list);
        }
    }

    // Filter by time range if specified
    if let Some(duration) = time_range {
        let cutoff = chrono::Utc::now() - duration;
        tasks.retain(|t| t.updated_at >= cutoff);
    }

    // Sort by updated_at descending (most recent first)
    tasks.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

    // Limit after sorting to get the most recent
    tasks.truncate(200);

    templates::task_list(&tasks)
}

async fn task_detail(State(queue): State<AppState>, Path(id): Path<Uuid>) -> Markup {
    if let Ok(Some((task, _))) = queue.get(id).await {
        templates::task_detail(&task)
    } else {
        maud::html! { p { "Task not found" } }
    }
}

async fn replay_task(State(queue): State<AppState>, Path(id): Path<Uuid>) -> &'static str {
    match crate::worker::replay_task(&queue, id).await {
        Ok(_) => "OK",
        Err(_) => "Error",
    }
}

async fn archive_task(State(queue): State<AppState>, Path(id): Path<Uuid>) -> &'static str {
    match crate::worker::archive_task(&queue, id).await {
        Ok(_) => "OK",
        Err(_) => "Error",
    }
}

async fn workers_partial(State(queue): State<AppState>) -> Markup {
    let workers = match queue.client().list_objects("workers/", 100, None).await {
        Ok((keys, _)) => {
            let mut workers = Vec::new();
            for key in keys {
                if let Ok((body, _)) = queue.client().get_object(&key).await {
                    if let Ok(info) = serde_json::from_slice::<WorkerInfo>(&body) {
                        workers.push(info);
                    }
                }
            }
            workers
        }
        Err(_) => Vec::new(),
    };
    let now = queue.now().await.ok();
    templates::workers_list(&workers, now)
}

async fn tasks_sse(
    State(_queue): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_secs(5)))
            .map(|_| Ok(Event::default().event("refresh").data("")));

    Sse::new(stream)
}
