//! HTML templates using maud.

use maud::{html, Markup, PreEscaped, DOCTYPE};

use crate::models::{Task, TaskStatus, WorkerInfo};
use chrono::DateTime;

/// Base layout
pub fn layout(title: &str, content: &Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="UTF-8";
                meta name="viewport" content="width=device-width, initial-scale=1.0";
                title { (title) }
                script src="https://unpkg.com/htmx.org@1.9.10" {}
                script src="https://unpkg.com/htmx.org@1.9.10/dist/ext/sse.js" {}
                style { (PreEscaped(CSS)) }
                script { (PreEscaped(JS)) }
            }
            body {
                (content)
            }
        }
    }
}

/// Task list partial (for htmx updates)
pub fn task_list(tasks: &[Task]) -> Markup {
    // Compute stats from tasks
    let mut pending = 0;
    let mut running = 0;
    let mut completed = 0;
    let mut failed = 0;
    let mut cancelled = 0;

    for task in tasks {
        match task.status {
            TaskStatus::Pending => pending += 1,
            TaskStatus::Running => running += 1,
            TaskStatus::Completed => completed += 1,
            TaskStatus::Failed => failed += 1,
            TaskStatus::Cancelled => cancelled += 1,
            TaskStatus::Archived | TaskStatus::Expired => {}
        }
    }

    html! {
        div class="stats-bar" {
            div class="stat" {
                span class="stat-value pending" { (pending) }
                span class="stat-label" { "pending" }
            }
            div class="stat" {
                span class="stat-value running" { (running) }
                span class="stat-label" { "running" }
            }
            div class="stat" {
                span class="stat-value completed" { (completed) }
                span class="stat-label" { "completed" }
            }
            div class="stat" {
                span class="stat-value failed" { (failed) }
                span class="stat-label" { "failed" }
            }
            @if cancelled > 0 {
                div class="stat" {
                    span class="stat-value cancelled" { (cancelled) }
                    span class="stat-label" { "cancelled" }
                }
            }
            div class="stat total" {
                span class="stat-value" { (tasks.len()) }
                span class="stat-label" { "total" }
            }
        }
        table {
            thead {
                tr {
                    th { "ID" }
                    th { "Type" }
                    th { "Status" }
                    th { "Attempt" }
                    th { "Updated" }
                    th { }
                }
            }
            tbody {
                @for task in tasks {
                    (task_row(task))
                }
                @if tasks.is_empty() {
                    tr { td colspan="6" class="empty" { "No tasks in this time range" } }
                }
            }
        }
    }
}

/// Render a single task row
pub fn task_row(task: &Task) -> Markup {
    let status_class = match task.status {
        TaskStatus::Pending => "badge pending",
        TaskStatus::Running => "badge running",
        TaskStatus::Completed => "badge completed",
        TaskStatus::Failed => "badge failed",
        TaskStatus::Cancelled => "badge cancelled",
        TaskStatus::Archived => "badge archived",
        TaskStatus::Expired => "badge expired",
    };

    let short_id = &task.id.to_string()[..8];

    html! {
        tr class="clickable" hx-get={"/task/" (task.id)} hx-target="#detail" {
            td class="mono" { (short_id) }
            td class="mono" { (task.task_type) }
            td { span class=(status_class) { (status_display(task.status)) } }
            td class="num" { (task.attempt) }
            td class="muted" data-ts=(task.updated_at.to_rfc3339()) { (task.updated_at.format("%H:%M:%S")) }
            td class="actions" onclick="event.stopPropagation()" {
                @if task.status == TaskStatus::Failed {
                    button hx-post={"/task/" (task.id) "/replay"} hx-swap="none" { "Replay" }
                }
                @if matches!(task.status, TaskStatus::Completed | TaskStatus::Failed) {
                    button class="secondary" hx-post={"/task/" (task.id) "/archive"} hx-swap="none" { "Archive" }
                }
            }
        }
    }
}

/// Render task detail panel
pub fn task_detail(task: &Task) -> Markup {
    html! {
        h3 class="mono" { (task.id) }

        dl {
            dt { "Type" } dd class="mono" { (task.task_type) }
            dt { "Status" } dd { span class=(status_badge(task.status)) { (status_display(task.status)) } }
            dt { "Shard" } dd class="mono" { (task.shard) }
            dt { "Attempt" } dd { (task.attempt) " / " (task.max_retries) }

            @if let Some(ref worker) = task.worker_id {
                dt { "Worker" } dd class="mono" { (worker) }
            }
            @if let Some(expires) = task.lease_expires_at {
                dt { "Lease expires" } dd data-ts=(expires.to_rfc3339()) { (expires.format("%H:%M:%S")) }
            }
            @if let Some(ref error) = task.last_error {
                dt { "Error" } dd class="error" { (error) }
            }

            dt { "Created" } dd data-ts=(task.created_at.to_rfc3339()) data-ts-full="true" { (task.created_at.format("%Y-%m-%d %H:%M:%S")) }
            dt { "Updated" } dd data-ts=(task.updated_at.to_rfc3339()) data-ts-full="true" { (task.updated_at.format("%Y-%m-%d %H:%M:%S")) }
        }

        @if !task.input.is_null() {
            details open {
                summary { "Input" }
                pre { (serde_json::to_string_pretty(&task.input).unwrap_or_default()) }
            }
        }
        @if let Some(ref output) = task.output {
            details open {
                summary { "Output" }
                pre { (serde_json::to_string_pretty(output).unwrap_or_default()) }
            }
        }
    }
}

/// Render workers list
pub fn workers_list(workers: &[WorkerInfo], now: Option<DateTime<chrono::Utc>>) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "Worker" }
                    th { "Status" }
                    th { "Current" }
                    th class="num" { "Done" }
                    th class="num" { "Failed" }
                    th { "Heartbeat" }
                }
            }
            tbody {
                @for worker in workers {
                    (worker_row(worker, now))
                }
                @if workers.is_empty() {
                    tr { td colspan="6" class="empty" { "No workers" } }
                }
            }
        }
    }
}

/// Render a single worker row
pub fn worker_row(worker: &WorkerInfo, now: Option<DateTime<chrono::Utc>>) -> Markup {
    let healthy = now.is_some_and(|now| worker.is_healthy_at(now, chrono::Duration::seconds(60)));

    let current = worker
        .current_task
        .map_or_else(|| "—".to_string(), |id| id.to_string()[..8].to_string());

    html! {
        tr {
            td class="mono" { (worker.worker_id) }
            td {
                @if healthy {
                    span class="badge completed" { "Healthy" }
                } @else {
                    span class="badge pending" { "Stale" }
                }
            }
            td class="mono muted" { (current) }
            td class="num" { (worker.tasks_completed) }
            td class="num" { (worker.tasks_failed) }
            td class="muted" data-ts=(worker.last_heartbeat.to_rfc3339()) { (worker.last_heartbeat.format("%H:%M:%S")) }
        }
    }
}

/// Returns CSS class for status badge.
/// Exhaustive match ensures compile error if new status is added.
const fn status_badge(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "badge pending",
        TaskStatus::Running => "badge running",
        TaskStatus::Completed => "badge completed",
        TaskStatus::Failed => "badge failed",
        TaskStatus::Cancelled => "badge cancelled",
        TaskStatus::Archived => "badge archived",
        TaskStatus::Expired => "badge expired",
    }
}

/// Returns display text for status.
/// Uses the canonical `display_name` from `TaskStatus`.
const fn status_display(status: TaskStatus) -> &'static str {
    status.display_name()
}

const CSS: &str = r#"
*, *::before, *::after { box-sizing: border-box; }

:root {
    --bg: #fff;
    --bg-secondary: #f9f9f9;
    --border: #e5e5e5;
    --text: #1a1a1a;
    --text-muted: #737373;
    --font: ui-sans-serif, system-ui, -apple-system, sans-serif;
    --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace;
}

@media (prefers-color-scheme: dark) {
    :root {
        --bg: #0d0d0d;
        --bg-secondary: #171717;
        --border: #2e2e2e;
        --text: #e5e5e5;
        --text-muted: #a3a3a3;
    }
}

html, body {
    height: 100%;
    margin: 0;
    overflow: hidden;
}

body {
    font-family: var(--font);
    font-size: 14px;
    line-height: 1.5;
    color: var(--text);
    background: var(--bg);
    display: flex;
    flex-direction: column;
}

header {
    flex-shrink: 0;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 24px;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
}

h1 {
    font-size: 16px;
    font-weight: 600;
    letter-spacing: -0.01em;
    margin: 0;
}

.tabs {
    display: flex;
    gap: 2px;
}

.tab {
    font-family: var(--font);
    font-size: 13px;
    font-weight: 500;
    padding: 6px 14px;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: var(--text-muted);
    cursor: pointer;
}

.tab:hover {
    background: var(--bg-secondary);
    color: var(--text);
}

.tab.active {
    background: var(--bg-secondary);
    color: var(--text);
}

h2 {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    margin: 0 0 12px;
    padding: 0 16px;
}

h2:not(:first-child) {
    margin-top: 24px;
}

h3 {
    font-size: 13px;
    font-weight: 500;
    margin: 0 0 16px;
    word-break: break-all;
}

.layout {
    flex: 1;
    display: grid;
    grid-template-columns: 1fr 360px;
    min-height: 0;
    overflow: hidden;
}

main {
    overflow-y: auto;
    padding: 16px 0;
}

.controls {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 0 16px 12px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 8px;
}

.controls label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: var(--text-muted);
}

select, input {
    font-family: var(--font);
    font-size: 13px;
    padding: 6px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg);
    color: var(--text);
}

select:focus, input:focus {
    outline: none;
    border-color: #999;
}

button {
    font-family: var(--font);
    font-size: 12px;
    font-weight: 500;
    padding: 6px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--bg);
    color: var(--text);
    cursor: pointer;
}

button:hover {
    background: var(--bg-secondary);
}

button.secondary {
    color: var(--text-muted);
}

table {
    width: 100%;
    border-collapse: collapse;
}

thead {
    position: sticky;
    top: 0;
    background: var(--bg);
    z-index: 1;
}

th {
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    text-align: left;
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
}

td {
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
}

tr.clickable {
    cursor: pointer;
}

tr.clickable:hover td {
    background: var(--bg-secondary);
}

.mono {
    font-family: var(--mono);
    font-size: 12px;
}

.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

.muted {
    color: var(--text-muted);
}

.empty {
    color: var(--text-muted);
    text-align: center;
    padding: 32px 16px;
}

.task-count {
    font-size: 12px;
    color: var(--text-muted);
    padding: 8px 16px;
}

.stats-bar {
    display: flex;
    gap: 24px;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
}

.stat {
    display: flex;
    align-items: baseline;
    gap: 6px;
}

.stat-value {
    font-size: 20px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
}

.stat-value.pending { color: #ca8a04; }
.stat-value.running { color: #2563eb; }
.stat-value.completed { color: #16a34a; }
.stat-value.failed { color: #dc2626; }
.stat-value.cancelled { color: #9333ea; }

.stat-label {
    font-size: 12px;
    color: var(--text-muted);
}

.stat.total {
    margin-left: auto;
}

@media (prefers-color-scheme: dark) {
    .stat-value.pending { color: #facc15; }
    .stat-value.running { color: #60a5fa; }
    .stat-value.completed { color: #4ade80; }
    .stat-value.failed { color: #f87171; }
    .stat-value.cancelled { color: #c084fc; }
}

.actions {
    text-align: right;
}

.actions button {
    font-size: 11px;
    padding: 4px 8px;
}

a {
    color: var(--text);
    text-decoration: none;
}

a:hover {
    text-decoration: underline;
}

.badge {
    display: inline-block;
    font-size: 11px;
    font-weight: 500;
    padding: 2px 8px;
    border-radius: 4px;
}

.badge.pending {
    background: #fef3c7;
    color: #92400e;
}

.badge.running {
    background: #dbeafe;
    color: #1e40af;
}

.badge.completed {
    background: #d1fae5;
    color: #065f46;
}

.badge.failed {
    background: #fee2e2;
    color: #991b1b;
}

.badge.archived {
    background: #f3f4f6;
    color: #6b7280;
}

.badge.cancelled {
    background: #fae8ff;
    color: #86198f;
}

.badge.expired {
    background: #fef3c7;
    color: #78350f;
}

@media (prefers-color-scheme: dark) {
    .badge.pending {
        background: #422006;
        color: #fcd34d;
    }
    .badge.running {
        background: #1e3a5f;
        color: #93c5fd;
    }
    .badge.completed {
        background: #064e3b;
        color: #6ee7b7;
    }
    .badge.failed {
        background: #450a0a;
        color: #fca5a5;
    }
    .badge.archived {
        background: #262626;
        color: #a3a3a3;
    }
    .badge.cancelled {
        background: #4a044e;
        color: #e879f9;
    }
    .badge.expired {
        background: #422006;
        color: #fbbf24;
    }
    .error {
        color: #f87171;
    }
}

#detail {
    background: var(--bg-secondary);
    border-left: 1px solid var(--border);
    padding: 20px;
    overflow-y: auto;
}

#detail .placeholder {
    color: var(--text-muted);
    text-align: center;
    padding: 40px 20px;
}

dl {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 6px 16px;
    font-size: 13px;
    margin-bottom: 16px;
}

dt {
    color: var(--text-muted);
}

dd {
    margin: 0;
}

.error {
    color: #dc2626;
}

details {
    margin-top: 12px;
}

summary {
    font-size: 12px;
    font-weight: 500;
    color: var(--text-muted);
    cursor: pointer;
    margin-bottom: 8px;
}

pre {
    font-family: var(--mono);
    font-size: 12px;
    line-height: 1.6;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px;
    margin: 0;
    overflow-x: auto;
    white-space: pre-wrap;
    word-break: break-word;
}

.header-right {
    display: flex;
    align-items: center;
    gap: 12px;
}

.tz-toggle {
    font-family: var(--mono);
    font-size: 11px;
    padding: 4px 8px;
    min-width: 44px;
}
"#;

const JS: &str = r"
// Timezone toggle - load preference from localStorage
let useLocal = localStorage.getItem('tz') === 'local';

function formatTime(iso, full) {
    const d = new Date(iso);
    if (useLocal) {
        return full
            ? d.toLocaleString('sv-SE').replace('T', ' ')
            : d.toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
    } else {
        return full
            ? iso.slice(0, 19).replace('T', ' ')
            : iso.slice(11, 19);
    }
}

function updateTimestamps() {
    document.querySelectorAll('[data-ts]').forEach(el => {
        const iso = el.dataset.ts;
        const full = el.dataset.tsFull === 'true';
        el.textContent = formatTime(iso, full);
    });
    const btn = document.getElementById('tz-toggle');
    if (btn) btn.textContent = useLocal ? 'Local' : 'UTC';
}

function toggleTimezone() {
    useLocal = !useLocal;
    localStorage.setItem('tz', useLocal ? 'local' : 'utc');
    updateTimestamps();
}

// Apply stored preference immediately when script loads
(function() {
    const btn = document.getElementById('tz-toggle');
    if (btn) btn.textContent = useLocal ? 'Local' : 'UTC';
})();

// Update timestamps on page load and after htmx swaps
document.addEventListener('DOMContentLoaded', updateTimestamps);
document.body.addEventListener('htmx:afterSwap', updateTimestamps);
document.body.addEventListener('htmx:afterSettle', updateTimestamps);
";
