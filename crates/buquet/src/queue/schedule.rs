//! Recurring schedule management for the task queue.
//!
//! This module provides support for cron-like recurring task schedules.
//! Schedules are stored in S3 and a scheduler worker periodically checks
//! for due schedules and submits tasks.

use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;
use uuid::Uuid;

use crate::storage::{PutCondition, S3Client, StorageError};

use super::{Queue, SubmitOptions};

/// A recurring schedule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    /// Unique schedule identifier.
    pub id: String,
    /// Task type to submit when triggered.
    pub task_type: String,
    /// Input data for the task.
    pub input: serde_json::Value,
    /// Cron expression (5-field standard format).
    pub cron: String,
    /// Whether the schedule is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Task timeout in seconds.
    pub timeout_seconds: Option<u64>,
    /// Task max retries.
    pub max_retries: Option<u32>,
    /// When the schedule was created.
    pub created_at: DateTime<Utc>,
    /// When the schedule was last updated.
    pub updated_at: DateTime<Utc>,
}

const fn default_true() -> bool {
    true
}

/// Tracks the last run of a schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleLastRun {
    /// Schedule ID.
    pub schedule_id: String,
    /// When the schedule was last triggered.
    pub last_run_at: DateTime<Utc>,
    /// Task ID of the last submitted task.
    pub last_task_id: Uuid,
    /// Next scheduled run time.
    pub next_run_at: DateTime<Utc>,
}

impl Schedule {
    /// Creates a new schedule.
    pub fn new(
        id: impl Into<String>,
        task_type: impl Into<String>,
        input: serde_json::Value,
        cron: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            task_type: task_type.into(),
            input,
            cron: cron.into(),
            enabled: true,
            timeout_seconds: None,
            max_retries: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns the S3 key for this schedule.
    pub fn key(&self) -> String {
        format!("schedules/{}.json", self.id)
    }

    /// Returns the S3 key for this schedule's last run tracking.
    pub fn last_run_key(&self) -> String {
        format!("schedules/{}/last_run.json", self.id)
    }

    /// Validates the cron expression.
    pub fn validate_cron(&self) -> Result<(), ScheduleError> {
        // cron crate expects 6 or 7 fields (with seconds), so prepend "0" for 5-field
        let cron_with_seconds = format!("0 {}", self.cron);
        CronSchedule::from_str(&cron_with_seconds)
            .map_err(|e| ScheduleError::InvalidCron(e.to_string()))?;
        Ok(())
    }

    /// Calculates the next run time after the given time.
    pub fn next_run_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let cron_with_seconds = format!("0 {}", self.cron);
        let schedule = CronSchedule::from_str(&cron_with_seconds).ok()?;
        schedule.after(&after).next()
    }

    /// Checks if the schedule is due to run at the given time.
    pub fn is_due(&self, now: DateTime<Utc>, last_run: Option<&ScheduleLastRun>) -> bool {
        if !self.enabled {
            return false;
        }

        let last_run_at = last_run.map(|lr| lr.last_run_at);

        // Get the next scheduled time after the last run (or epoch if never run)
        let after = last_run_at.unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap_or(now));

        self.next_run_after(after).is_some_and(|next| next <= now)
    }
}

/// Errors that can occur during schedule operations.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    /// The cron expression is invalid.
    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),

    /// The schedule was not found.
    #[error("Schedule not found: {0}")]
    NotFound(String),

    /// A schedule with this ID already exists.
    #[error("Schedule already exists: {0}")]
    AlreadyExists(String),

    /// An S3 storage error occurred.
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Schedule management operations.
pub struct ScheduleManager {
    client: S3Client,
}

/// A scheduler run result for a single schedule.
#[derive(Debug, Clone)]
pub struct SchedulerRun {
    /// Schedule ID that was triggered.
    pub schedule_id: String,
    /// Task ID created for the run.
    pub task_id: Uuid,
}

/// A non-fatal scheduler error encountered during a tick.
#[derive(Debug, Clone)]
pub struct SchedulerTickError {
    /// Schedule ID associated with the error.
    pub schedule_id: String,
    /// Human-readable error message.
    pub message: String,
}

/// Summary of a scheduler tick.
#[derive(Debug, Default, Clone)]
pub struct SchedulerTickReport {
    /// Successful schedule runs.
    pub runs: Vec<SchedulerRun>,
    /// Non-fatal errors encountered.
    pub errors: Vec<SchedulerTickError>,
}

impl ScheduleManager {
    /// Creates a new schedule manager.
    pub const fn new(client: S3Client) -> Self {
        Self { client }
    }

    /// Creates a new schedule.
    pub async fn create(&self, schedule: &Schedule) -> Result<(), ScheduleError> {
        schedule.validate_cron()?;

        let body = serde_json::to_vec(schedule)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        self.client
            .put_object(&schedule.key(), body, PutCondition::IfNoneMatch)
            .await
            .map_err(|e| match e {
                StorageError::PreconditionFailed { .. } => {
                    ScheduleError::AlreadyExists(schedule.id.clone())
                }
                other => ScheduleError::Storage(other),
            })?;

        Ok(())
    }

    /// Gets a schedule by ID.
    pub async fn get(&self, id: &str) -> Result<Option<(Schedule, String)>, ScheduleError> {
        let key = format!("schedules/{id}.json");
        match self.client.get_object(&key).await {
            Ok((body, etag)) => {
                let schedule: Schedule = serde_json::from_slice(&body)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                Ok(Some((schedule, etag)))
            }
            Err(StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(ScheduleError::Storage(e)),
        }
    }

    /// Updates an existing schedule.
    pub async fn update(&self, schedule: &Schedule, etag: &str) -> Result<(), ScheduleError> {
        schedule.validate_cron()?;

        let body = serde_json::to_vec(schedule)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        self.client
            .put_object(
                &schedule.key(),
                body,
                PutCondition::IfMatch(etag.to_string()),
            )
            .await?;

        Ok(())
    }

    /// Deletes a schedule.
    pub async fn delete(&self, id: &str) -> Result<(), ScheduleError> {
        let key = format!("schedules/{id}.json");
        self.client.delete_object(&key).await?;

        // Also delete last_run if exists (best effort)
        let last_run_key = format!("schedules/{id}/last_run.json");
        let _ = self.client.delete_object(&last_run_key).await;

        Ok(())
    }

    /// Lists all schedules.
    pub async fn list(&self) -> Result<Vec<Schedule>, ScheduleError> {
        let keys = self
            .client
            .list_objects_paginated("schedules/", i32::MAX)
            .await?;

        let mut schedules = Vec::new();
        for key in keys {
            // Only process top-level schedule files, not subdirectories
            if Path::new(&key)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                && !key.contains("/last_run.json")
            {
                if let Ok((body, _)) = self.client.get_object(&key).await {
                    if let Ok(schedule) = serde_json::from_slice::<Schedule>(&body) {
                        schedules.push(schedule);
                    }
                }
            }
        }

        Ok(schedules)
    }

    /// Gets the last run info for a schedule.
    pub async fn get_last_run(
        &self,
        schedule_id: &str,
    ) -> Result<Option<(ScheduleLastRun, String)>, ScheduleError> {
        let key = format!("schedules/{schedule_id}/last_run.json");
        match self.client.get_object(&key).await {
            Ok((body, etag)) => {
                let last_run: ScheduleLastRun = serde_json::from_slice(&body)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                Ok(Some((last_run, etag)))
            }
            Err(StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(ScheduleError::Storage(e)),
        }
    }

    /// Updates the last run info with CAS.
    pub async fn update_last_run(
        &self,
        last_run: &ScheduleLastRun,
        etag: Option<&str>,
    ) -> Result<(), ScheduleError> {
        let key = format!(
            "schedules/{schedule_id}/last_run.json",
            schedule_id = last_run.schedule_id
        );
        let body = serde_json::to_vec(last_run)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        let condition = etag.map_or(PutCondition::IfNoneMatch, |e| {
            PutCondition::IfMatch(e.to_string())
        });

        self.client.put_object(&key, body, condition).await?;
        Ok(())
    }

    /// Enables a schedule.
    pub async fn enable(&self, id: &str) -> Result<(), ScheduleError> {
        if let Some((mut schedule, etag)) = self.get(id).await? {
            schedule.enabled = true;
            schedule.updated_at = Utc::now();
            self.update(&schedule, &etag).await?;
            Ok(())
        } else {
            Err(ScheduleError::NotFound(id.to_string()))
        }
    }

    /// Disables a schedule.
    pub async fn disable(&self, id: &str) -> Result<(), ScheduleError> {
        if let Some((mut schedule, etag)) = self.get(id).await? {
            schedule.enabled = false;
            schedule.updated_at = Utc::now();
            self.update(&schedule, &etag).await?;
            Ok(())
        } else {
            Err(ScheduleError::NotFound(id.to_string()))
        }
    }
}

/// Runs a single scheduler tick for all schedules.
///
/// This checks for due schedules, submits tasks with idempotency, and updates
/// `last_run` state using CAS. Errors per schedule are collected and returned
/// in the report while the tick continues.
pub async fn run_scheduler_tick(
    queue: &Queue,
    manager: &ScheduleManager,
    now: DateTime<Utc>,
) -> Result<SchedulerTickReport, ScheduleError> {
    let schedules = manager.list().await?;
    let mut report = SchedulerTickReport::default();

    for schedule in schedules {
        if !schedule.enabled {
            continue;
        }

        let last_run = manager.get_last_run(&schedule.id).await.ok().flatten();
        let last_run_ref = last_run.as_ref().map(|(lr, _)| lr);

        let after = last_run_ref.map_or_else(
            || DateTime::from_timestamp(0, 0).unwrap_or(now),
            |lr| lr.last_run_at,
        );
        let Some(due_at) = schedule.next_run_after(after) else {
            continue;
        };
        if due_at > now {
            continue;
        }

        let idempotency_key = format!("schedule:{}:{}", schedule.id, due_at.timestamp());
        let options = SubmitOptions {
            timeout_seconds: schedule.timeout_seconds,
            max_retries: schedule.max_retries,
            idempotency_key: Some(idempotency_key),
            ..Default::default()
        };

        match queue
            .submit(&schedule.task_type, schedule.input.clone(), options)
            .await
        {
            Ok(task) => {
                let next_run = schedule.next_run_after(now).unwrap_or(now);
                let new_last_run = ScheduleLastRun {
                    schedule_id: schedule.id.clone(),
                    last_run_at: now,
                    last_task_id: task.id,
                    next_run_at: next_run,
                };

                let etag = last_run.as_ref().map(|(_, e)| e.as_str());
                match manager.update_last_run(&new_last_run, etag).await {
                    Ok(()) => report.runs.push(SchedulerRun {
                        schedule_id: schedule.id.clone(),
                        task_id: task.id,
                    }),
                    Err(ScheduleError::Storage(StorageError::PreconditionFailed { .. })) => {
                        tracing::debug!(
                            "Schedule '{}' already triggered by another instance",
                            schedule.id
                        );
                    }
                    Err(e) => report.errors.push(SchedulerTickError {
                        schedule_id: schedule.id.clone(),
                        message: format!(
                            "Failed to update last_run for schedule '{}': {}",
                            schedule.id, e
                        ),
                    }),
                }
            }
            Err(e) => report.errors.push(SchedulerTickError {
                schedule_id: schedule.id.clone(),
                message: format!(
                    "Failed to submit task for schedule '{}': {}",
                    schedule.id, e
                ),
            }),
        }
    }

    Ok(report)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_cron_validation() {
        let schedule = Schedule::new("test", "task_type", serde_json::json!({}), "0 9 * * *");
        assert!(schedule.validate_cron().is_ok());

        let bad = Schedule::new("test", "task_type", serde_json::json!({}), "invalid");
        assert!(bad.validate_cron().is_err());
    }

    #[test]
    fn test_next_run() {
        let schedule = Schedule::new("test", "task_type", serde_json::json!({}), "0 9 * * *");
        let now = Utc::now();
        let next = schedule.next_run_after(now);
        assert!(next.is_some());
        assert!(next.unwrap() > now);
    }
}
