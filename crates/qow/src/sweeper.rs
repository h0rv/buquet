//! Workflow sweeper for stall recovery.
//!
//! The sweeper detects workflows that are stuck in active states but have
//! no active orchestrator task, enabling recovery.

use crate::error::Result;
use crate::state::StateManager;
use crate::types::WorkflowState;

/// Workflow that needs recovery.
#[derive(Debug, Clone)]
pub struct WorkflowNeedsRecovery {
    /// The workflow ID.
    pub workflow_id: String,
    /// The workflow state.
    pub state: WorkflowState,
    /// Reason for needing recovery.
    pub reason: RecoveryReason,
}

/// Reason a workflow needs recovery.
#[derive(Debug, Clone)]
pub enum RecoveryReason {
    /// No orchestrator task ID set.
    NoOrchestratorTask,
    /// Orchestrator task completed or failed.
    OrchestratorTaskTerminal,
    /// Orchestrator task lease expired.
    OrchestratorLeaseExpired,
}

/// Result of a sweep operation.
#[derive(Debug, Clone)]
pub struct SweepResult {
    /// Number of workflows scanned.
    pub scanned: usize,
    /// Workflows that need recovery.
    pub needs_recovery: Vec<WorkflowNeedsRecovery>,
    /// Number of workflows in terminal states (skipped).
    pub terminal: usize,
    /// Number of healthy workflows (active with valid orchestrator).
    pub healthy: usize,
}

/// Workflow sweeper for detecting stalled workflows.
///
/// The sweeper scans workflow states and identifies workflows that are
/// in active states (running, `waiting_signal`) but have no active
/// orchestrator task.
#[derive(Clone)]
pub struct WorkflowSweeper {
    state_manager: StateManager,
}

impl WorkflowSweeper {
    /// Create a new workflow sweeper.
    #[must_use]
    pub const fn new(state_manager: StateManager) -> Self {
        Self { state_manager }
    }

    /// Scan all workflows and identify those needing recovery.
    ///
    /// This returns workflows that:
    /// - Are in an active state (running, `waiting_signal`)
    /// - Have no `orchestrator_task_id`, OR
    /// - Have an orchestrator task that is no longer live
    ///
    /// # Arguments
    ///
    /// * `task_checker` - Function to check if a task is still live.
    ///   Returns `Some(true)` if live, `Some(false)` if terminated, `None` if not found.
    ///
    /// # Note
    ///
    /// The actual task status check is delegated to the caller because the
    /// workflow engine doesn't have direct access to the qo queue. The Python
    /// layer should provide a closure that calls `queue.get(task_id)`.
    pub async fn scan<F, Fut>(
        &self,
        prefix: &str,
        limit: i32,
        task_checker: F,
    ) -> Result<SweepResult>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Option<bool>>,
    {
        let workflow_ids = self.state_manager.list(prefix, limit).await?;

        let mut result = SweepResult {
            scanned: 0,
            needs_recovery: vec![],
            terminal: 0,
            healthy: 0,
        };

        for wf_id in workflow_ids {
            result.scanned += 1;

            let Ok((state, _etag)) = self.state_manager.get(&wf_id).await else {
                continue; // Skip if can't read state
            };

            // Skip terminal/paused workflows
            if !state.status.is_active() {
                result.terminal += 1;
                continue;
            }

            // Check orchestrator task
            let needs_recovery = match &state.orchestrator_task_id {
                None => Some(RecoveryReason::NoOrchestratorTask),
                Some(task_id) => {
                    match task_checker(task_id.clone()).await {
                        None => Some(RecoveryReason::OrchestratorTaskTerminal), // Not found
                        Some(false) => Some(RecoveryReason::OrchestratorLeaseExpired), // Expired
                        Some(true) => None,                                     // Still live
                    }
                }
            };

            if let Some(reason) = needs_recovery {
                result.needs_recovery.push(WorkflowNeedsRecovery {
                    workflow_id: wf_id,
                    state,
                    reason,
                });
            } else {
                result.healthy += 1;
            }
        }

        Ok(result)
    }

    /// Scan for workflows needing recovery without task checking.
    ///
    /// This simpler version only checks for missing `orchestrator_task_id`.
    /// Use this when you don't have access to the task queue.
    pub async fn scan_simple(&self, prefix: &str, limit: i32) -> Result<SweepResult> {
        self.scan(prefix, limit, |_| async { Some(true) }).await
    }
}
