//! Workflow execution engine.
//!
//! This module contains the core orchestration logic for driving workflow execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::dag::{
    compute_ready_steps, has_unrecoverable_failure, is_workflow_complete, should_pause_workflow,
};
use crate::error::Result;
use crate::signals::SignalManager;
use crate::state::StateManager;
use crate::types::{
    Signal, SignalCursor, StepDef, StepState, StepStatus, WorkflowErrorInfo, WorkflowState,
    WorkflowStatus,
};
use oq::storage::S3Client;

/// Result of an orchestration tick.
#[derive(Debug, Clone)]
pub enum OrchestrateResult {
    /// Workflow is already in a terminal or stopped state - no action taken.
    Noop {
        /// The workflow ID.
        workflow_id: String,
        /// The current status.
        status: WorkflowStatus,
    },
    /// Workflow was paused due to version mismatch.
    VersionMismatch {
        /// The workflow ID.
        workflow_id: String,
        /// Expected definition hash.
        expected: String,
        /// Actual definition hash.
        actual: String,
    },
    /// Workflow completed successfully.
    Completed {
        /// The workflow ID.
        workflow_id: String,
    },
    /// Workflow failed due to unrecoverable step failure.
    Failed {
        /// The workflow ID.
        workflow_id: String,
    },
    /// Workflow was paused due to step failure policy.
    Paused {
        /// The workflow ID.
        workflow_id: String,
        /// Reason for pause.
        reason: String,
    },
    /// No steps ready to run - waiting for running steps or signals.
    Waiting {
        /// The workflow ID.
        workflow_id: String,
        /// Currently running steps.
        current_steps: Vec<String>,
    },
    /// Steps were scheduled for execution.
    Scheduled {
        /// The workflow ID.
        workflow_id: String,
        /// Steps that were submitted.
        submitted_steps: Vec<String>,
        /// All currently running steps.
        current_steps: Vec<String>,
    },
}

/// Result of consuming a signal.
#[derive(Debug, Clone)]
pub struct ConsumeSignalResult {
    /// The signal that was consumed, if any.
    pub signal: Option<Signal>,
    /// The new cursor position.
    pub cursor: String,
}

/// Workflow execution engine.
///
/// Provides the core logic for orchestrating workflow execution.
#[derive(Clone)]
pub struct WorkflowEngine {
    state_manager: StateManager,
    signal_manager: SignalManager,
}

impl WorkflowEngine {
    /// Create a new workflow engine.
    pub fn new(client: Arc<S3Client>) -> Self {
        Self {
            state_manager: StateManager::new(client.clone()),
            signal_manager: SignalManager::new(client),
        }
    }

    /// Get the state manager.
    #[must_use]
    pub const fn state(&self) -> &StateManager {
        &self.state_manager
    }

    /// Get the signal manager.
    #[must_use]
    pub const fn signals(&self) -> &SignalManager {
        &self.signal_manager
    }

    /// Start a workflow with idempotency support.
    ///
    /// Creates the initial workflow state if it doesn't exist. If the workflow
    /// already exists, returns the existing state without modification.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `workflow_type` - The workflow type name
    /// * `definition_hash` - Hash of the workflow definition for versioning
    /// * `data` - Initial workflow data
    /// * `now` - Current timestamp
    ///
    /// # Returns
    ///
    /// Tuple of (created: bool, state: `WorkflowState`). If created is false,
    /// the workflow already existed.
    pub async fn start_workflow(
        &self,
        workflow_id: &str,
        workflow_type: &str,
        definition_hash: &str,
        data: serde_json::Value,
        now: &str,
    ) -> Result<(bool, WorkflowState)> {
        // Build initial state
        let state = WorkflowState {
            id: workflow_id.to_string(),
            workflow_type: workflow_type.to_string(),
            definition_hash: definition_hash.to_string(),
            status: WorkflowStatus::Running,
            current_steps: vec![],
            data,
            steps: BTreeMap::new(),
            signals: BTreeMap::new(),
            error: None,
            created_at: now.to_string(),
            updated_at: now.to_string(),
            orchestrator_task_id: None,
        };

        // Try to create (idempotent - fails if exists)
        if self
            .state_manager
            .create_if_not_exists(workflow_id, &state)
            .await?
            .is_some()
        {
            Ok((true, state))
        } else {
            // Already exists - return existing state
            let (existing, _etag) = self.state_manager.get(workflow_id).await?;
            Ok((false, existing))
        }
    }

    /// Run one orchestration tick.
    ///
    /// This is the core orchestration logic:
    /// 1. Check if workflow is in a stoppable state (no-op if so)
    /// 2. Check version compatibility
    /// 3. Check for completion/failure/pause conditions
    /// 4. Compute and return ready steps for scheduling
    ///
    /// Note: This does NOT submit tasks - that's done by the Python layer
    /// which has access to the oq queue. This returns what steps to submit.
    pub async fn orchestrate(
        &self,
        workflow_id: &str,
        steps: &BTreeMap<String, StepDef>,
        definition_hash: &str,
        now: &str,
    ) -> Result<(OrchestrateResult, Option<WorkflowState>)> {
        // Get current state
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        // Guard: no-op if workflow is already stopped or terminal
        if matches!(
            state.status,
            WorkflowStatus::Paused
                | WorkflowStatus::Cancelled
                | WorkflowStatus::Completed
                | WorkflowStatus::Failed
        ) {
            return Ok((
                OrchestrateResult::Noop {
                    workflow_id: workflow_id.to_string(),
                    status: state.status,
                },
                None,
            ));
        }

        // Check version compatibility
        if state.definition_hash != definition_hash {
            state.status = WorkflowStatus::Paused;
            state.error = Some(WorkflowErrorInfo::new(
                "VersionMismatch",
                format!(
                    "Workflow definition changed. Expected {}, got {}",
                    state.definition_hash, definition_hash
                ),
            ));
            self.state_manager
                .update(workflow_id, &state, &etag, now)
                .await?;
            return Ok((
                OrchestrateResult::VersionMismatch {
                    workflow_id: workflow_id.to_string(),
                    expected: state.definition_hash.clone(),
                    actual: definition_hash.to_string(),
                },
                Some(state),
            ));
        }

        // Check for completion
        if is_workflow_complete(steps, &state) {
            state.status = WorkflowStatus::Completed;
            state.current_steps.clear();
            self.state_manager
                .update(workflow_id, &state, &etag, now)
                .await?;
            return Ok((
                OrchestrateResult::Completed {
                    workflow_id: workflow_id.to_string(),
                },
                Some(state),
            ));
        }

        // Check for unrecoverable failure
        if has_unrecoverable_failure(steps, &state) {
            state.status = WorkflowStatus::Failed;
            self.state_manager
                .update(workflow_id, &state, &etag, now)
                .await?;
            return Ok((
                OrchestrateResult::Failed {
                    workflow_id: workflow_id.to_string(),
                },
                Some(state),
            ));
        }

        // Check for pause
        if should_pause_workflow(steps, &state) {
            state.status = WorkflowStatus::Paused;
            self.state_manager
                .update(workflow_id, &state, &etag, now)
                .await?;
            return Ok((
                OrchestrateResult::Paused {
                    workflow_id: workflow_id.to_string(),
                    reason: "step_failure_policy".to_string(),
                },
                Some(state),
            ));
        }

        // Compute ready steps
        let ready_steps = compute_ready_steps(steps, &state);

        if ready_steps.is_empty() {
            // No steps ready - waiting for running steps or signals
            return Ok((
                OrchestrateResult::Waiting {
                    workflow_id: workflow_id.to_string(),
                    current_steps: state.current_steps.clone(),
                },
                None,
            ));
        }

        // Return ready steps - caller will submit tasks and call mark_steps_running
        Ok((
            OrchestrateResult::Scheduled {
                workflow_id: workflow_id.to_string(),
                submitted_steps: ready_steps,
                current_steps: state.current_steps.clone(),
            },
            Some(state),
        ))
    }

    /// Mark steps as running after tasks have been submitted.
    ///
    /// Call this after successfully submitting step tasks.
    pub async fn mark_steps_running(
        &self,
        workflow_id: &str,
        step_names: &[String],
        now: &str,
    ) -> Result<()> {
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        for step_name in step_names {
            let attempt = state.steps.get(step_name).map_or(0, |s| s.attempt) + 1;

            state.steps.insert(
                step_name.clone(),
                StepState::running(now.to_string(), attempt, None),
            );
        }

        // Update current_steps
        state.current_steps = state
            .steps
            .iter()
            .filter(|(_, s)| s.status == StepStatus::Running)
            .map(|(name, _)| name.clone())
            .collect();
        state.status = WorkflowStatus::Running;

        self.state_manager
            .update(workflow_id, &state, &etag, now)
            .await?;
        Ok(())
    }

    /// Mark a step as completed.
    ///
    /// This also saves the step result to a separate S3 location for
    /// easier inspection and potential payload reference support.
    pub async fn complete_step(
        &self,
        workflow_id: &str,
        step_name: &str,
        result: serde_json::Value,
        now: &str,
    ) -> Result<()> {
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        let (started_at, attempt) = state
            .steps
            .get(step_name)
            .map_or((None, 0), |s| (s.started_at.clone(), s.attempt));

        // Save result to separate location (idempotent - ignores if already exists)
        let result_bytes = serde_json::to_vec(&result)?;
        let _ = self
            .state_manager
            .save_step_result(workflow_id, step_name, &result_bytes)
            .await?;

        state.steps.insert(
            step_name.to_string(),
            StepState::completed(started_at, now.to_string(), result, attempt),
        );

        // Remove from current_steps
        state.current_steps.retain(|s| s != step_name);

        self.state_manager
            .update(workflow_id, &state, &etag, now)
            .await?;
        Ok(())
    }

    /// Mark a step as failed (only call after retries exhausted).
    pub async fn fail_step(
        &self,
        workflow_id: &str,
        step_name: &str,
        error: &str,
        now: &str,
    ) -> Result<()> {
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        let (started_at, attempt) = state
            .steps
            .get(step_name)
            .map_or((None, 0), |s| (s.started_at.clone(), s.attempt));

        state.steps.insert(
            step_name.to_string(),
            StepState::failed(started_at, now.to_string(), error.to_string(), attempt),
        );

        // Remove from current_steps
        state.current_steps.retain(|s| s != step_name);

        self.state_manager
            .update(workflow_id, &state, &etag, now)
            .await?;
        Ok(())
    }

    /// Mark a step as cancelled.
    pub async fn cancel_step(
        &self,
        workflow_id: &str,
        step_name: &str,
        reason: &str,
        now: &str,
    ) -> Result<()> {
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        let (started_at, attempt) = state
            .steps
            .get(step_name)
            .map_or((None, 0), |s| (s.started_at.clone(), s.attempt));

        state.steps.insert(
            step_name.to_string(),
            StepState::cancelled(started_at, now.to_string(), reason.to_string(), attempt),
        );

        // Remove from current_steps
        state.current_steps.retain(|s| s != step_name);

        self.state_manager
            .update(workflow_id, &state, &etag, now)
            .await?;
        Ok(())
    }

    /// Consume the next signal and advance the cursor atomically.
    ///
    /// This uses CAS to ensure correct at-least-once semantics under concurrent consumers.
    pub async fn consume_signal(
        &self,
        workflow_id: &str,
        signal_name: &str,
        now: &str,
    ) -> Result<Option<Signal>> {
        let (mut state, etag) = self.state_manager.get(workflow_id).await?;

        // Get current cursor
        let cursor = state
            .signals
            .get(signal_name)
            .and_then(|c| c.cursor.clone());

        // Get next signal
        let result = self
            .signal_manager
            .get_next(workflow_id, signal_name, cursor.as_deref())
            .await?;

        let Some((new_cursor, signal)) = result else {
            return Ok(None);
        };

        // Update cursor in state with CAS
        state
            .signals
            .entry(signal_name.to_string())
            .or_insert_with(SignalCursor::new)
            .cursor = Some(new_cursor);

        self.state_manager
            .update(workflow_id, &state, &etag, now)
            .await?;

        Ok(Some(signal))
    }
}
