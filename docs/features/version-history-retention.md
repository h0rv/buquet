# Feature: Version History Retention Policy

## Status: Implemented

## Overview
Provide guidance and optional tooling for managing S3 version history growth,
so auditability remains practical at scale.

## Problem
Every state transition creates a new object version. For high-volume queues,
this can drive storage cost and slow `get_history()`.

## Solution
- Document recommended S3 lifecycle policies for version expiration.
- Optionally expose a `history_retention_days` configuration that warns when
  versioning is enabled without a lifecycle policy.

## Recommended Lifecycle Policy (Example)
- Keep all versions for 30 days
- Transition older versions to cheaper storage (if supported)
- Expire non-current versions after 90 days

## Behavior
- No change to correctness; only affects audit depth.
- Should be explicitly opt-in to avoid surprising deletions.

## Implementation
- Comprehensive guide: `docs/guides/version-history-retention.md`
- Getting started section: `docs/getting-started.md` (Version History and Retention)
- Startup informational log: Workers log a reminder about lifecycle policies when versioning is detected

## Related Issues
- `docs/issues/ISSUE-004-version-history-costs.md`
