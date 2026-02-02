# Feature: Workflow CLI (buquet-workflow)

## Status: Implemented

## Overview
`buquet-workflow` is a small CLI for inspecting workflow state stored in S3 and for
detecting stalled workflows via the sweeper.

This CLI is part of the separate `buquet-workflow` package (not core buquet).

## Requirements
The CLI uses the same S3 configuration as buquet:

- `S3_BUCKET` (required)
- `S3_ENDPOINT` (optional)
- `S3_REGION` or `AWS_REGION` (optional; defaults to `us-east-1`)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (if required by your backend)

## Install / Run

```bash
# Run directly with Cargo
cargo run -p buquet-workflow --bin buquet-workflow -- <command>

# Or install locally
cargo install --path crates/buquet-workflow
buquet-workflow <command>
```

## Commands

### List workflows
```bash
buquet-workflow list --prefix wf- --limit 100
buquet-workflow list --json
```

### Inspect workflow status
```bash
buquet-workflow status wf-123
buquet-workflow status wf-123 --json
```

### Sweeper (stall detection)
```bash
# One-shot scan
buquet-workflow sweeper --once

# Continuous scan
buquet-workflow sweeper --interval 300

# Filter + JSON output
buquet-workflow sweeper --prefix wf- --limit 1000 --json
```

**Note:** The sweeper CLI only reports workflows that need recovery. It does not
submit new orchestrator tasks. Recovery should be handled by your workflow
orchestrator process.

## Output
By default the CLI prints human-readable output. Use `--json` to emit machine-
readable results.
