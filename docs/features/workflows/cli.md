# Feature: Workflow CLI (qo-workflow)

## Status: Implemented

## Overview
`qo-workflow` is a small CLI for inspecting workflow state stored in S3 and for
detecting stalled workflows via the sweeper.

This CLI is part of the separate `qow` package (not core qo).

## Requirements
The CLI uses the same S3 configuration as qo:

- `S3_BUCKET` (required)
- `S3_ENDPOINT` (optional)
- `S3_REGION` or `AWS_REGION` (optional; defaults to `us-east-1`)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (if required by your backend)

## Install / Run

```bash
# Run directly with Cargo
cargo run -p qow --bin qo-workflow -- <command>

# Or install locally
cargo install --path crates/qow
qo-workflow <command>
```

## Commands

### List workflows
```bash
qo-workflow list --prefix wf- --limit 100
qo-workflow list --json
```

### Inspect workflow status
```bash
qo-workflow status wf-123
qo-workflow status wf-123 --json
```

### Sweeper (stall detection)
```bash
# One-shot scan
qo-workflow sweeper --once

# Continuous scan
qo-workflow sweeper --interval 300

# Filter + JSON output
qo-workflow sweeper --prefix wf- --limit 1000 --json
```

**Note:** The sweeper CLI only reports workflows that need recovery. It does not
submit new orchestrator tasks. Recovery should be handled by your workflow
orchestrator process.

## Output
By default the CLI prints human-readable output. Use `--json` to emit machine-
readable results.
