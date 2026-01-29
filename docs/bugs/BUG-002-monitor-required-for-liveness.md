# BUG-002: Liveness depends on monitor/sweeper being present

Status: Resolved (default-on embedded monitor; explicit opt-out)

## Summary
Missing ready/lease index objects could permanently hide tasks if the timeout monitor/sweeper is not running and workers are not log-scanning.

## Impact
Tasks can become invisible forever (liveness failure), violating correctness expectations.

## Example
Ready index creation fails on submit; no monitor is running; workers in index-only mode never see the task.

## Resolution
Workers now start an embedded monitor by default; disabling it requires an explicit opt-out. This makes liveness repair the default path.

## Follow-ups
Consider making the opt-out unavailable in production builds or add a leader-election mechanism for safe multi-monitor deployments.
