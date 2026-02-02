# BUG-008: Integration tests are ignored and miss critical paths

Status: Resolved (Rust integration suite runs with LocalStack; feature-gated for local runs)

## Summary
Integration tests are no longer permanently ignored. They are feature-gated for
local runs and executed in CI against LocalStack. Critical behaviors now have a
repeatable integration path.

## Impact
Correctness regressions are likely to slip into production undetected.

## Example
A refactor breaks sweeper logic; no tests fail and tasks become invisible.

## Fix Applied
- CI runs the Rust integration suite against LocalStack.
- Local runs are supported via `just integration` (feature-gated tests).

## Notes
Python integration tests are wired into CI via LocalStack.
