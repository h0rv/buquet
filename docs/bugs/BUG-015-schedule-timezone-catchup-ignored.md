# BUG-015: Schedule timezone and catch_up are ignored

Status: Resolved

## Summary
`Schedule.timezone` and `Schedule.catch_up` were stored but never used. The scheduler always evaluated cron in UTC and triggered at most one run per tick.

## Impact
Schedules configured for non-UTC timezones ran at the wrong time. Missed runs were never caught up even when `catch_up` was enabled.

## Example
- Schedule cron: "0 9 * * *", timezone: "America/New_York".
- Scheduler evaluates in UTC and runs at 09:00 UTC instead of 09:00 local.
- Scheduler downtime over a scheduled window does not trigger missed runs.

## Location
- `crates/oq/src/queue/schedule.rs:30-129`
- `crates/oq/src/main.rs:653-671`

## Proposed Fix
For simplicity, enforce UTC-only scheduling and remove the unused fields.

## Resolution
- Removed `timezone` and `catch_up` from the schedule model and APIs.
- Documented UTC-only cron evaluation.
