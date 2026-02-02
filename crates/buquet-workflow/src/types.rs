//! Core types for buquet-workflow.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Status of a workflow instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Workflow created but not yet started.
    Pending,
    /// Workflow is actively executing steps.
    Running,
    /// Workflow is waiting for a signal.
    WaitingSignal,
    /// Workflow is running compensation handlers.
    Compensating,
    /// Workflow is paused (can be resumed).
    Paused,
    /// Workflow completed successfully.
    Completed,
    /// Workflow failed.
    Failed,
    /// Workflow was cancelled.
    Cancelled,
}

impl WorkflowStatus {
    /// Check if this is a terminal state.
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Check if this is an active state (workflow is processing).
    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Running | Self::WaitingSignal | Self::Compensating
        )
    }
}

/// Status of a workflow step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Step has not started.
    Pending,
    /// Step is currently executing.
    Running,
    /// Step completed successfully.
    Completed,
    /// Step failed.
    Failed,
    /// Step was cancelled.
    Cancelled,
}

/// Action to take when a step fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Fail the entire workflow.
    #[default]
    FailWorkflow,
    /// Pause the workflow for manual intervention.
    PauseWorkflow,
    /// Continue with other steps (if possible).
    Continue,
    /// Run compensation handlers.
    Compensate,
}

/// Error information for a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowErrorInfo {
    /// Error type/category.
    #[serde(rename = "type")]
    pub error_type: String,
    /// Error message.
    pub message: String,
    /// Step that caused the error (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// Attempt number when error occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Timestamp when error occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

impl WorkflowErrorInfo {
    /// Create a new workflow error.
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            step: None,
            attempt: None,
            at: None,
        }
    }

    /// Set the step that caused the error.
    #[must_use]
    pub fn with_step(mut self, step: impl Into<String>) -> Self {
        self.step = Some(step.into());
        self
    }

    /// Set the attempt number.
    #[must_use]
    pub const fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = Some(attempt);
        self
    }

    /// Set the timestamp.
    #[must_use]
    pub fn with_at(mut self, at: impl Into<String>) -> Self {
        self.at = Some(at.into());
        self
    }
}

/// Cursor for signal consumption.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalCursor {
    /// The cursor position (`timestamp_uuid` suffix).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl SignalCursor {
    /// Create a new empty cursor.
    pub const fn new() -> Self {
        Self { cursor: None }
    }

    /// Create a cursor at a specific position.
    pub fn at(cursor: impl Into<String>) -> Self {
        Self {
            cursor: Some(cursor.into()),
        }
    }
}

/// State of a workflow step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepState {
    /// Current status.
    pub status: StepStatus,
    /// When the step started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When the step completed (success or failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    /// Step result (if completed successfully).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Current attempt number.
    #[serde(default)]
    pub attempt: u32,
    /// Task ID for the current execution (for failure detection).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

impl Default for StepState {
    fn default() -> Self {
        Self {
            status: StepStatus::Pending,
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
            attempt: 0,
            task_id: None,
        }
    }
}

impl StepState {
    /// Create a new pending step state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a running step state.
    pub fn running(started_at: impl Into<String>, attempt: u32, task_id: Option<String>) -> Self {
        Self {
            status: StepStatus::Running,
            started_at: Some(started_at.into()),
            completed_at: None,
            result: None,
            error: None,
            attempt,
            task_id,
        }
    }

    /// Create a completed step state.
    pub fn completed(
        started_at: Option<String>,
        completed_at: impl Into<String>,
        result: serde_json::Value,
        attempt: u32,
    ) -> Self {
        Self {
            status: StepStatus::Completed,
            started_at,
            completed_at: Some(completed_at.into()),
            result: Some(result),
            error: None,
            attempt,
            task_id: None,
        }
    }

    /// Create a failed step state.
    pub fn failed(
        started_at: Option<String>,
        completed_at: impl Into<String>,
        error: impl Into<String>,
        attempt: u32,
    ) -> Self {
        Self {
            status: StepStatus::Failed,
            started_at,
            completed_at: Some(completed_at.into()),
            result: None,
            error: Some(error.into()),
            attempt,
            task_id: None,
        }
    }

    /// Create a cancelled step state.
    pub fn cancelled(
        started_at: Option<String>,
        completed_at: impl Into<String>,
        reason: impl Into<String>,
        attempt: u32,
    ) -> Self {
        Self {
            status: StepStatus::Cancelled,
            started_at,
            completed_at: Some(completed_at.into()),
            result: None,
            error: Some(reason.into()),
            attempt,
            task_id: None,
        }
    }
}

/// Full state of a workflow instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowState {
    /// Unique workflow ID.
    pub id: String,
    /// Workflow type name.
    #[serde(rename = "type")]
    pub workflow_type: String,
    /// Hash of the workflow definition structure.
    pub definition_hash: String,
    /// Current workflow status.
    pub status: WorkflowStatus,
    /// Currently executing steps.
    #[serde(default)]
    pub current_steps: Vec<String>,
    /// Workflow input data.
    #[serde(default)]
    pub data: serde_json::Value,
    /// Step states keyed by step name.
    #[serde(default)]
    pub steps: BTreeMap<String, StepState>,
    /// Signal cursors keyed by signal name.
    #[serde(default)]
    pub signals: BTreeMap<String, SignalCursor>,
    /// Error information (if failed/paused).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowErrorInfo>,
    /// When the workflow was created.
    pub created_at: String,
    /// When the workflow was last updated.
    pub updated_at: String,
    /// Current orchestrator task ID (for stall detection).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orchestrator_task_id: Option<String>,
}

impl WorkflowState {
    /// Create a new workflow state.
    pub fn new(
        id: impl Into<String>,
        workflow_type: impl Into<String>,
        definition_hash: impl Into<String>,
        data: serde_json::Value,
        now: impl Into<String>,
    ) -> Self {
        let now = now.into();
        Self {
            id: id.into(),
            workflow_type: workflow_type.into(),
            definition_hash: definition_hash.into(),
            status: WorkflowStatus::Pending,
            current_steps: Vec::new(),
            data,
            steps: BTreeMap::new(),
            signals: BTreeMap::new(),
            error: None,
            created_at: now.clone(),
            updated_at: now,
            orchestrator_task_id: None,
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// A signal sent to a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// Unique signal ID.
    pub id: String,
    /// Signal name.
    pub name: String,
    /// Signal payload.
    pub payload: serde_json::Value,
    /// When the signal was created.
    pub created_at: String,
}

impl Signal {
    /// Create a new signal.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        payload: serde_json::Value,
        created_at: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            payload,
            created_at: created_at.into(),
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// Definition of a workflow step (for DAG computation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    /// Step name.
    pub name: String,
    /// Steps that must complete before this one.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Maximum retry attempts.
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u32,
    /// Action on failure.
    #[serde(default)]
    pub on_failure: OnFailure,
    /// Whether this step has a compensation handler.
    #[serde(default)]
    pub has_compensation: bool,
}

const fn default_retries() -> u32 {
    3
}

const fn default_timeout() -> u32 {
    300
}

impl StepDef {
    /// Create a new step definition.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            depends_on: Vec::new(),
            retries: 3,
            timeout: 300,
            on_failure: OnFailure::FailWorkflow,
            has_compensation: false,
        }
    }

    /// Add dependencies.
    #[must_use]
    pub fn with_depends_on(mut self, deps: Vec<String>) -> Self {
        self.depends_on = deps;
        self
    }

    /// Set retry count.
    #[must_use]
    pub const fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: u32) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set failure behavior.
    #[must_use]
    pub const fn with_on_failure(mut self, on_failure: OnFailure) -> Self {
        self.on_failure = on_failure;
        self
    }

    /// Set compensation flag.
    #[must_use]
    pub const fn with_compensation(mut self) -> Self {
        self.has_compensation = true;
        self
    }
}

/// A running workflow instance (client view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRun {
    /// Workflow ID.
    pub id: String,
    /// Current status.
    pub status: WorkflowStatus,
    /// Workflow type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_type: Option<String>,
    /// Workflow data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Currently executing steps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_steps: Option<Vec<String>>,
    /// Error information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<WorkflowErrorInfo>,
}

impl WorkflowRun {
    /// Create a new workflow run.
    pub fn new(id: impl Into<String>, status: WorkflowStatus) -> Self {
        Self {
            id: id.into(),
            status,
            workflow_type: None,
            data: None,
            current_steps: None,
            error: None,
        }
    }

    /// Create from workflow state.
    pub fn from_state(state: &WorkflowState) -> Self {
        Self {
            id: state.id.clone(),
            status: state.status,
            workflow_type: Some(state.workflow_type.clone()),
            data: Some(state.data.clone()),
            current_steps: Some(state.current_steps.clone()),
            error: state.error.clone(),
        }
    }
}

/// Compute a hash of the workflow definition structure.
///
/// The hash is based on step names and their dependencies only,
/// not on handler code, timeouts, or retry policies.
pub fn compute_definition_hash(steps: &BTreeMap<String, StepDef>) -> String {
    // Build canonical representation
    let mut canonical = serde_json::Map::new();

    // Sorted step names
    let step_names: Vec<&str> = steps.keys().map(String::as_str).collect();
    canonical.insert(
        "steps".to_string(),
        serde_json::Value::Array(step_names.iter().map(|s| serde_json::json!(s)).collect()),
    );

    // Dependencies for each step
    let mut deps_map = serde_json::Map::new();
    for (name, step) in steps {
        let mut sorted_deps = step.depends_on.clone();
        sorted_deps.sort();
        deps_map.insert(
            name.clone(),
            serde_json::Value::Array(sorted_deps.iter().map(|d| serde_json::json!(d)).collect()),
        );
    }
    canonical.insert(
        "dependencies".to_string(),
        serde_json::Value::Object(deps_map),
    );

    // Compute hash
    let content = serde_json::to_string(&serde_json::Value::Object(canonical))
        .unwrap_or_else(|_| String::new());

    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let result = hasher.finalize();
    let hash_hex = hex::encode(&result[..8]); // First 16 hex chars

    format!("sha256:{hash_hex}")
}

// Add hex encoding
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0xf) as usize] as char);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_status_is_terminal() {
        assert!(!WorkflowStatus::Pending.is_terminal());
        assert!(!WorkflowStatus::Running.is_terminal());
        assert!(!WorkflowStatus::WaitingSignal.is_terminal());
        assert!(!WorkflowStatus::Compensating.is_terminal());
        assert!(!WorkflowStatus::Paused.is_terminal());
        assert!(WorkflowStatus::Completed.is_terminal());
        assert!(WorkflowStatus::Failed.is_terminal());
        assert!(WorkflowStatus::Cancelled.is_terminal());
    }

    #[test]
    fn test_workflow_status_is_active() {
        assert!(!WorkflowStatus::Pending.is_active());
        assert!(WorkflowStatus::Running.is_active());
        assert!(WorkflowStatus::WaitingSignal.is_active());
        assert!(WorkflowStatus::Compensating.is_active());
        assert!(!WorkflowStatus::Paused.is_active());
        assert!(!WorkflowStatus::Completed.is_active());
        assert!(!WorkflowStatus::Failed.is_active());
        assert!(!WorkflowStatus::Cancelled.is_active());
    }

    #[test]
    fn test_compute_definition_hash() {
        let mut steps = BTreeMap::new();
        steps.insert("a".to_string(), StepDef::new("a"));
        steps.insert(
            "b".to_string(),
            StepDef::new("b").with_depends_on(vec!["a".to_string()]),
        );

        let hash = compute_definition_hash(&steps);
        assert!(hash.starts_with("sha256:"));
        assert_eq!(hash.len(), 7 + 16); // "sha256:" + 16 hex chars
    }

    #[test]
    fn test_definition_hash_is_deterministic() {
        let mut steps1 = BTreeMap::new();
        steps1.insert("a".to_string(), StepDef::new("a"));
        steps1.insert(
            "b".to_string(),
            StepDef::new("b").with_depends_on(vec!["a".to_string()]),
        );

        let mut steps2 = BTreeMap::new();
        steps2.insert(
            "b".to_string(),
            StepDef::new("b").with_depends_on(vec!["a".to_string()]),
        );
        steps2.insert("a".to_string(), StepDef::new("a"));

        assert_eq!(
            compute_definition_hash(&steps1),
            compute_definition_hash(&steps2)
        );
    }

    #[test]
    fn test_definition_hash_changes_with_structure() {
        let mut steps1 = BTreeMap::new();
        steps1.insert("a".to_string(), StepDef::new("a"));

        let mut steps2 = BTreeMap::new();
        steps2.insert("a".to_string(), StepDef::new("a"));
        steps2.insert(
            "b".to_string(),
            StepDef::new("b").with_depends_on(vec!["a".to_string()]),
        );

        assert_ne!(
            compute_definition_hash(&steps1),
            compute_definition_hash(&steps2)
        );
    }

    #[test]
    fn test_workflow_state_json_roundtrip() {
        let state = WorkflowState::new(
            "wf-123",
            "order_fulfillment",
            "sha256:abc123",
            serde_json::json!({"order_id": "ORD-456"}),
            "2026-01-28T12:00:00Z",
        );

        let json = match state.to_json() {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected workflow state serialize to succeed: {err}");
                return;
            }
        };
        let parsed = match WorkflowState::from_json(&json) {
            Ok(value) => value,
            Err(err) => {
                assert!(
                    false,
                    "expected workflow state deserialize to succeed: {err}"
                );
                return;
            }
        };

        assert_eq!(parsed.id, state.id);
        assert_eq!(parsed.workflow_type, state.workflow_type);
        assert_eq!(parsed.definition_hash, state.definition_hash);
        assert_eq!(parsed.status, state.status);
    }

    #[test]
    fn test_signal_json_roundtrip() {
        let signal = Signal::new(
            "sig-123",
            "approval",
            serde_json::json!({"approved": true}),
            "2026-01-28T12:00:00Z",
        );

        let json = match signal.to_json() {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected signal serialize to succeed: {err}");
                return;
            }
        };
        let parsed = match Signal::from_json(&json) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected signal deserialize to succeed: {err}");
                return;
            }
        };

        assert_eq!(parsed.id, signal.id);
        assert_eq!(parsed.name, signal.name);
        assert_eq!(parsed.payload, signal.payload);
        assert_eq!(parsed.created_at, signal.created_at);
    }

    #[test]
    fn test_step_state_with_task_id_json_roundtrip() {
        // Test with task_id present
        let state = StepState::running("2026-01-28T12:00:00Z", 1, Some("task-abc123".to_string()));
        let json = match serde_json::to_string(&state) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected step state serialize to succeed: {err}");
                return;
            }
        };
        let parsed: StepState = match serde_json::from_str(&json) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected step state deserialize to succeed: {err}");
                return;
            }
        };

        assert_eq!(parsed.status, StepStatus::Running);
        assert_eq!(parsed.task_id, Some("task-abc123".to_string()));
        assert_eq!(parsed.attempt, 1);

        // Test with task_id absent (should not appear in JSON due to skip_serializing_if)
        let state_no_task = StepState::default();
        let json_no_task = match serde_json::to_string(&state_no_task) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected step state serialize to succeed: {err}");
                return;
            }
        };
        assert!(!json_no_task.contains("task_id"));

        let parsed_no_task: StepState = match serde_json::from_str(&json_no_task) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "expected step state deserialize to succeed: {err}");
                return;
            }
        };
        assert_eq!(parsed_no_task.task_id, None);
    }
}
