//! DAG computation for workflow step scheduling.
//!
//! This module contains pure functions for computing workflow execution order,
//! detecting failures, and managing compensation chains.

use super::types::{OnFailure, StepDef, StepStatus, WorkflowState};
use std::collections::{BTreeMap, HashSet};

/// Errors that can occur during DAG operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    /// A step depends on an unknown step.
    UnknownDependency {
        /// The step that has the invalid dependency.
        step: String,
        /// The unknown dependency name.
        dependency: String,
    },
    /// A cycle was detected in the dependency graph.
    CycleDetected {
        /// The steps involved in the cycle.
        steps: Vec<String>,
    },
    /// A step depends on itself.
    SelfDependency {
        /// The step that depends on itself.
        step: String,
    },
}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownDependency { step, dependency } => {
                write!(f, "Step '{step}' depends on unknown step '{dependency}'")
            }
            Self::CycleDetected { steps } => {
                write!(f, "Cycle detected in workflow involving steps: {steps:?}")
            }
            Self::SelfDependency { step } => {
                write!(f, "Step '{step}' cannot depend on itself")
            }
        }
    }
}

impl std::error::Error for DagError {}

/// Compute which steps are ready to run.
///
/// A step is ready if:
/// 1. It's not already completed, running, or failed
/// 2. All its dependencies are completed
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that are ready to run
#[must_use]
pub fn compute_ready_steps(
    steps: &BTreeMap<String, StepDef>,
    state: &WorkflowState,
) -> Vec<String> {
    let mut ready = Vec::new();

    for (name, step_def) in steps {
        // Check current step status
        if let Some(step_state) = state.steps.get(name) {
            // Skip if already running, completed, or failed
            if matches!(
                step_state.status,
                StepStatus::Running | StepStatus::Completed | StepStatus::Failed
            ) {
                continue;
            }
        }

        // Check if all dependencies are completed
        let deps_satisfied = step_def.depends_on.iter().all(|dep| {
            state
                .steps
                .get(dep)
                .is_some_and(|s| s.status == StepStatus::Completed)
        });

        if deps_satisfied {
            ready.push(name.clone());
        }
    }

    ready
}

/// Check if all steps in the workflow are completed.
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if all steps are completed
#[must_use]
pub fn is_workflow_complete(steps: &BTreeMap<String, StepDef>, state: &WorkflowState) -> bool {
    steps.keys().all(|name| {
        state
            .steps
            .get(name)
            .is_some_and(|s| s.status == StepStatus::Completed)
    })
}

/// Check if there's a failed step that blocks further progress.
///
/// A failure is unrecoverable if:
/// 1. A step has failed
/// 2. That step's `on_failure` is `FAIL_WORKFLOW` or `COMPENSATE`
/// 3. Or other steps depend on it (directly or transitively) when `on_failure` is `CONTINUE`
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if workflow cannot make further progress due to failure
#[must_use]
pub fn has_unrecoverable_failure(steps: &BTreeMap<String, StepDef>, state: &WorkflowState) -> bool {
    for (name, step_def) in steps {
        let Some(step_state) = state.steps.get(name) else {
            continue;
        };

        // Check if this step has failed
        if step_state.status == StepStatus::Failed {
            // If on_failure is FAIL_WORKFLOW or COMPENSATE, workflow is blocked
            if matches!(
                step_def.on_failure,
                OnFailure::FailWorkflow | OnFailure::Compensate
            ) {
                return true;
            }

            // If on_failure is CONTINUE, check if other steps depend on this failed step
            if step_def.on_failure == OnFailure::Continue {
                // Find steps that depend on the failed step
                for (other_name, other_def) in steps {
                    if other_name != name && other_def.depends_on.contains(name) {
                        // Another step depends on this failed step
                        // With CONTINUE, we still can't proceed past this dependency
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Check if workflow should pause due to step failure with `PAUSE_WORKFLOW` policy.
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// True if workflow should transition to paused state
#[must_use]
pub fn should_pause_workflow(steps: &BTreeMap<String, StepDef>, state: &WorkflowState) -> bool {
    for (name, step_def) in steps {
        if let Some(step_state) = state.steps.get(name) {
            if step_state.status == StepStatus::Failed
                && step_def.on_failure == OnFailure::PauseWorkflow
            {
                return true;
            }
        }
    }

    false
}

/// Get the compensation chain for completed steps in reverse order.
///
/// When a step fails with COMPENSATE policy, we need to run compensation
/// handlers for all previously completed steps in reverse order.
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that need compensation, in reverse completion order
#[must_use]
pub fn get_compensation_chain(
    steps: &BTreeMap<String, StepDef>,
    state: &WorkflowState,
) -> Vec<String> {
    // Find completed steps with compensation handlers
    let mut completed_with_compensation: Vec<(String, Option<&String>)> = Vec::new();

    for (name, step_def) in steps {
        if let Some(step_state) = state.steps.get(name) {
            if step_state.status == StepStatus::Completed && step_def.has_compensation {
                completed_with_compensation.push((name.clone(), step_state.completed_at.as_ref()));
            }
        }
    }

    // Sort by completion time (most recent first)
    completed_with_compensation.sort_by(|a, b| {
        let a_time = a.1.map_or("", String::as_str);
        let b_time = b.1.map_or("", String::as_str);
        b_time.cmp(a_time)
    });

    completed_with_compensation
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Get steps that are blocked by failed dependencies.
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
/// * `state` - Current workflow state
///
/// # Returns
///
/// List of step names that cannot run due to failed dependencies
#[must_use]
pub fn get_blocked_steps(steps: &BTreeMap<String, StepDef>, state: &WorkflowState) -> Vec<String> {
    let mut blocked = Vec::new();

    // Build set of failed steps
    let failed_steps: HashSet<&String> = state
        .steps
        .iter()
        .filter(|(_, step_state)| step_state.status == StepStatus::Failed)
        .map(|(name, _)| name)
        .collect();

    for (name, step_def) in steps {
        // Only check pending steps (or steps with no state yet)
        let is_pending = state
            .steps
            .get(name)
            .is_none_or(|s| s.status == StepStatus::Pending);

        if !is_pending {
            continue;
        }

        // Check if any dependency has failed
        for dep in &step_def.depends_on {
            if failed_steps.contains(dep) {
                blocked.push(name.clone());
                break;
            }
        }
    }

    blocked
}

/// Sort steps in topological order (dependencies before dependents).
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
///
/// # Returns
///
/// List of step names in execution order, or an error if the DAG is invalid
pub fn topological_sort(steps: &BTreeMap<String, StepDef>) -> Result<Vec<String>, DagError> {
    // Build adjacency list and in-degree count
    let mut in_degree: BTreeMap<&String, usize> = steps.keys().map(|name| (name, 0)).collect();
    let mut dependents: BTreeMap<&String, Vec<&String>> =
        steps.keys().map(|name| (name, Vec::new())).collect();

    for (name, step_def) in steps {
        for dep in &step_def.depends_on {
            if !steps.contains_key(dep) {
                return Err(DagError::UnknownDependency {
                    step: name.clone(),
                    dependency: dep.clone(),
                });
            }
            if let Some(dep_list) = dependents.get_mut(dep) {
                dep_list.push(name);
            }
            if let Some(degree) = in_degree.get_mut(name) {
                *degree += 1;
            }
        }
    }

    // Kahn's algorithm
    let mut queue: Vec<&String> = in_degree
        .iter()
        .filter(|(_, &degree)| degree == 0)
        .map(|(&name, _)| name)
        .collect();

    let mut result = Vec::new();

    while !queue.is_empty() {
        // Sort for deterministic ordering
        queue.sort();
        let current = queue.remove(0);
        result.push(current.clone());

        for dependent in &dependents[current] {
            if let Some(degree) = in_degree.get_mut(dependent) {
                *degree -= 1;
                if *degree == 0 {
                    queue.push(dependent);
                }
            }
        }
    }

    if result.len() != steps.len() {
        // Find steps involved in cycle
        let remaining: Vec<String> = steps
            .keys()
            .filter(|name| !result.contains(name))
            .cloned()
            .collect();
        return Err(DagError::CycleDetected { steps: remaining });
    }

    Ok(result)
}

/// Validate the workflow DAG structure.
///
/// # Arguments
///
/// * `steps` - Step definitions from the workflow
///
/// # Returns
///
/// List of validation errors (empty if valid)
#[must_use]
pub fn validate_dag(steps: &BTreeMap<String, StepDef>) -> Vec<String> {
    let mut errors = Vec::new();

    // Check for missing dependencies
    for (name, step_def) in steps {
        for dep in &step_def.depends_on {
            if !steps.contains_key(dep) {
                errors.push(format!("Step '{name}' depends on unknown step '{dep}'"));
            }
        }
    }

    // Check for self-dependencies
    for (name, step_def) in steps {
        if step_def.depends_on.contains(name) {
            errors.push(format!("Step '{name}' cannot depend on itself"));
        }
    }

    // Check for cycles (only if no missing deps)
    if errors.is_empty() {
        if let Err(e) = topological_sort(steps) {
            errors.push(e.to_string());
        }
    }

    errors
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::super::types::StepState;
    use super::*;

    fn make_steps(defs: &[(&str, &[&str])]) -> BTreeMap<String, StepDef> {
        defs.iter()
            .map(|(name, deps)| {
                let step = StepDef::new(*name)
                    .with_depends_on(deps.iter().map(|s| (*s).to_string()).collect());
                ((*name).to_string(), step)
            })
            .collect()
    }

    fn make_state(id: &str) -> WorkflowState {
        WorkflowState::new(
            id,
            "test_workflow",
            "sha256:abc123",
            serde_json::json!({}),
            "2026-01-28T12:00:00Z",
        )
    }

    // ==================== compute_ready_steps tests ====================

    #[test]
    fn test_compute_ready_steps_empty_workflow() {
        let steps = BTreeMap::new();
        let state = make_state("wf-1");
        let ready = compute_ready_steps(&steps, &state);
        assert!(ready.is_empty());
    }

    #[test]
    fn test_compute_ready_steps_no_dependencies() {
        let steps = make_steps(&[("a", &[]), ("b", &[]), ("c", &[])]);
        let state = make_state("wf-1");
        let ready = compute_ready_steps(&steps, &state);
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&"a".to_string()));
        assert!(ready.contains(&"b".to_string()));
        assert!(ready.contains(&"c".to_string()));
    }

    #[test]
    fn test_compute_ready_steps_with_dependencies() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["a", "b"])]);
        let state = make_state("wf-1");
        let ready = compute_ready_steps(&steps, &state);
        assert_eq!(ready, vec!["a".to_string()]);
    }

    #[test]
    fn test_compute_ready_steps_after_completion() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["a", "b"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(
                Some("2026-01-28T12:00:00Z".to_string()),
                "2026-01-28T12:01:00Z",
                serde_json::json!({}),
                1,
            ),
        );
        let ready = compute_ready_steps(&steps, &state);
        assert_eq!(ready, vec!["b".to_string()]);
    }

    #[test]
    fn test_compute_ready_steps_excludes_running() {
        let steps = make_steps(&[("a", &[]), ("b", &[])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::running("2026-01-28T12:00:00Z", 1, None),
        );
        let ready = compute_ready_steps(&steps, &state);
        assert_eq!(ready, vec!["b".to_string()]);
    }

    #[test]
    fn test_compute_ready_steps_excludes_failed() {
        let steps = make_steps(&[("a", &[]), ("b", &[])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(
                Some("2026-01-28T12:00:00Z".to_string()),
                "2026-01-28T12:01:00Z",
                "error",
                1,
            ),
        );
        let ready = compute_ready_steps(&steps, &state);
        assert_eq!(ready, vec!["b".to_string()]);
    }

    // ==================== is_workflow_complete tests ====================

    #[test]
    fn test_is_workflow_complete_empty() {
        let steps = BTreeMap::new();
        let state = make_state("wf-1");
        assert!(is_workflow_complete(&steps, &state));
    }

    #[test]
    fn test_is_workflow_complete_all_completed() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::completed(None, "2026-01-28T12:02:00Z", serde_json::json!({}), 1),
        );
        assert!(is_workflow_complete(&steps, &state));
    }

    #[test]
    fn test_is_workflow_complete_some_pending() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        assert!(!is_workflow_complete(&steps, &state));
    }

    #[test]
    fn test_is_workflow_complete_some_running() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::running("2026-01-28T12:02:00Z", 1, None),
        );
        assert!(!is_workflow_complete(&steps, &state));
    }

    #[test]
    fn test_is_workflow_complete_some_failed() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::failed(None, "2026-01-28T12:02:00Z", "error", 1),
        );
        assert!(!is_workflow_complete(&steps, &state));
    }

    // ==================== has_unrecoverable_failure tests ====================

    #[test]
    fn test_has_unrecoverable_failure_no_failures() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        assert!(!has_unrecoverable_failure(&steps, &state));
    }

    #[test]
    fn test_has_unrecoverable_failure_fail_workflow() {
        let mut steps = make_steps(&[("a", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::FailWorkflow;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        assert!(has_unrecoverable_failure(&steps, &state));
    }

    #[test]
    fn test_has_unrecoverable_failure_compensate() {
        let mut steps = make_steps(&[("a", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::Compensate;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        assert!(has_unrecoverable_failure(&steps, &state));
    }

    #[test]
    fn test_has_unrecoverable_failure_continue_with_dependents() {
        let mut steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::Continue;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        // Step b depends on a, so this is unrecoverable
        assert!(has_unrecoverable_failure(&steps, &state));
    }

    #[test]
    fn test_has_unrecoverable_failure_continue_no_dependents() {
        let mut steps = make_steps(&[("a", &[]), ("b", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::Continue;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        // Step b doesn't depend on a, so we can continue
        assert!(!has_unrecoverable_failure(&steps, &state));
    }

    #[test]
    fn test_has_unrecoverable_failure_pause_workflow() {
        let mut steps = make_steps(&[("a", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::PauseWorkflow;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        // PauseWorkflow doesn't count as unrecoverable (can be resumed)
        assert!(!has_unrecoverable_failure(&steps, &state));
    }

    // ==================== should_pause_workflow tests ====================

    #[test]
    fn test_should_pause_workflow_no_failures() {
        let steps = make_steps(&[("a", &[])]);
        let state = make_state("wf-1");
        assert!(!should_pause_workflow(&steps, &state));
    }

    #[test]
    fn test_should_pause_workflow_with_pause_policy() {
        let mut steps = make_steps(&[("a", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::PauseWorkflow;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        assert!(should_pause_workflow(&steps, &state));
    }

    #[test]
    fn test_should_pause_workflow_with_fail_policy() {
        let mut steps = make_steps(&[("a", &[])]);
        let Some(step) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step.on_failure = OnFailure::FailWorkflow;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        assert!(!should_pause_workflow(&steps, &state));
    }

    // ==================== get_compensation_chain tests ====================

    #[test]
    fn test_get_compensation_chain_empty() {
        let steps = make_steps(&[("a", &[])]);
        let state = make_state("wf-1");
        let chain = get_compensation_chain(&steps, &state);
        assert!(chain.is_empty());
    }

    #[test]
    fn test_get_compensation_chain_no_compensation_handlers() {
        let steps = make_steps(&[("a", &[])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        let chain = get_compensation_chain(&steps, &state);
        assert!(chain.is_empty());
    }

    #[test]
    fn test_get_compensation_chain_with_handlers() {
        let mut steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let Some(step_a) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step_a.has_compensation = true;
        let Some(step_b) = steps.get_mut("b") else {
            panic!("expected step b");
        };
        step_b.has_compensation = true;
        // c has no compensation handler

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::completed(None, "2026-01-28T12:02:00Z", serde_json::json!({}), 1),
        );

        let chain = get_compensation_chain(&steps, &state);
        // Should be in reverse completion order
        assert_eq!(chain, vec!["b".to_string(), "a".to_string()]);
    }

    #[test]
    fn test_get_compensation_chain_excludes_non_completed() {
        let mut steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let Some(step_a) = steps.get_mut("a") else {
            panic!("expected step a");
        };
        step_a.has_compensation = true;
        let Some(step_b) = steps.get_mut("b") else {
            panic!("expected step b");
        };
        step_b.has_compensation = true;

        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::completed(None, "2026-01-28T12:01:00Z", serde_json::json!({}), 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::running("2026-01-28T12:02:00Z", 1, None),
        );

        let chain = get_compensation_chain(&steps, &state);
        assert_eq!(chain, vec!["a".to_string()]);
    }

    // ==================== get_blocked_steps tests ====================

    #[test]
    fn test_get_blocked_steps_none_blocked() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let state = make_state("wf-1");
        let blocked = get_blocked_steps(&steps, &state);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_get_blocked_steps_by_failed_dependency() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );

        let blocked = get_blocked_steps(&steps, &state);
        assert_eq!(blocked, vec!["b".to_string()]);
    }

    #[test]
    fn test_get_blocked_steps_multiple_blocked() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );

        let blocked = get_blocked_steps(&steps, &state);
        assert!(blocked.contains(&"b".to_string()));
        assert!(blocked.contains(&"c".to_string()));
    }

    #[test]
    fn test_get_blocked_steps_excludes_running() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"])]);
        let mut state = make_state("wf-1");
        state.steps.insert(
            "a".to_string(),
            StepState::failed(None, "2026-01-28T12:01:00Z", "error", 1),
        );
        state.steps.insert(
            "b".to_string(),
            StepState::running("2026-01-28T12:02:00Z", 1, None),
        );

        let blocked = get_blocked_steps(&steps, &state);
        // b is running, not pending, so it shouldn't be in blocked list
        assert!(blocked.is_empty());
    }

    // ==================== topological_sort tests ====================

    #[test]
    fn test_topological_sort_empty() {
        let steps = BTreeMap::new();
        let Ok(result) = topological_sort(&steps) else {
            panic!("expected topological sort success");
        };
        assert!(result.is_empty());
    }

    #[test]
    fn test_topological_sort_single_step() {
        let steps = make_steps(&[("a", &[])]);
        let Ok(result) = topological_sort(&steps) else {
            panic!("expected topological sort success");
        };
        assert_eq!(result, vec!["a".to_string()]);
    }

    #[test]
    fn test_topological_sort_linear_chain() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let Ok(result) = topological_sort(&steps) else {
            panic!("expected topological sort success");
        };
        assert_eq!(
            result,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_topological_sort_diamond() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]);
        let Ok(result) = topological_sort(&steps) else {
            panic!("expected topological sort success");
        };

        // a must come first, d must come last
        assert_eq!(result[0], "a".to_string());
        assert_eq!(result[3], "d".to_string());

        // b and c can be in either order
        let middle: Vec<_> = result[1..3].to_vec();
        assert!(middle.contains(&"b".to_string()));
        assert!(middle.contains(&"c".to_string()));
    }

    #[test]
    fn test_topological_sort_parallel_steps() {
        let steps = make_steps(&[("a", &[]), ("b", &[]), ("c", &[])]);
        let Ok(result) = topological_sort(&steps) else {
            panic!("expected topological sort success");
        };
        // All can run in parallel, but sorted alphabetically for determinism
        assert_eq!(
            result,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_topological_sort_unknown_dependency() {
        let steps = make_steps(&[("a", &["unknown"])]);
        let result = topological_sort(&steps);
        assert!(matches!(result, Err(DagError::UnknownDependency { .. })));
    }

    #[test]
    fn test_topological_sort_cycle_two_nodes() {
        let steps = make_steps(&[("a", &["b"]), ("b", &["a"])]);
        let result = topological_sort(&steps);
        let Err(DagError::CycleDetected { steps }) = result else {
            panic!("expected CycleDetected error");
        };
        assert!(steps.contains(&"a".to_string()));
        assert!(steps.contains(&"b".to_string()));
    }

    #[test]
    fn test_topological_sort_cycle_three_nodes() {
        let steps = make_steps(&[("a", &["c"]), ("b", &["a"]), ("c", &["b"])]);
        let result = topological_sort(&steps);
        assert!(matches!(result, Err(DagError::CycleDetected { .. })));
    }

    // ==================== validate_dag tests ====================

    #[test]
    fn test_validate_dag_valid() {
        let steps = make_steps(&[("a", &[]), ("b", &["a"]), ("c", &["a", "b"])]);
        let errors = validate_dag(&steps);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_dag_empty() {
        let steps = BTreeMap::new();
        let errors = validate_dag(&steps);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_dag_unknown_dependency() {
        let steps = make_steps(&[("a", &[]), ("b", &["unknown"])]);
        let errors = validate_dag(&steps);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("unknown"));
    }

    #[test]
    fn test_validate_dag_self_dependency() {
        let steps = make_steps(&[("a", &["a"])]);
        let errors = validate_dag(&steps);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("cannot depend on itself"));
    }

    #[test]
    fn test_validate_dag_cycle() {
        let steps = make_steps(&[("a", &["b"]), ("b", &["a"])]);
        let errors = validate_dag(&steps);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("Cycle detected"));
    }

    #[test]
    fn test_validate_dag_multiple_errors() {
        let steps = make_steps(&[("a", &["a"]), ("b", &["unknown"])]);
        let errors = validate_dag(&steps);
        assert_eq!(errors.len(), 2);
    }
}
