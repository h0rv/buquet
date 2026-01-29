# ISSUE-003: S3 compatibility variance across providers

Status: Documented

## Summary
oq relies on specific S3 behaviors (strong consistency, conditional writes).
Not all "S3-compatible" providers implement these features fully or with
comparable performance.

## Impact
- Conditional writes (`If-Match`, `If-None-Match`) may be missing or buggy.
- LIST/GET performance can vary widely and affect latency/cost assumptions.
- Versioning semantics differ across providers.

## Mitigations
- Document required S3 features prominently (`docs/S3-REFERENCES.md`).
- Provide a startup self-check that verifies required behavior.
- Maintain a compatibility matrix for common providers.

## Notes
This is a design constraint of the S3-only approach, not a bug.
