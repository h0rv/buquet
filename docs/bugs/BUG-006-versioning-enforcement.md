# BUG-006: Bucket versioning not enforced

Status: Resolved

## Summary
The system relied on S3 versioning for audit history but did not verify it was enabled.

## Impact
History APIs silently degrade if versioning is off, breaking event-sourcing guarantees.

## Example
A dev/test bucket without versioning yields empty histories even though state transitions occurred.

## Resolution
Startup now checks bucket versioning and fails fast if it is not enabled.

## Dev override
Set `BUQUET_ALLOW_NO_VERSIONING=1` to skip the check in development. This disables history/audit guarantees.

## Follow-ups
None.
