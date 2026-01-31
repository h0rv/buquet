# Feature: Payload References (Input/Output Offloading)

## Status: Implemented

## Overview
Store large task inputs/outputs outside the canonical task object and keep the
task JSON small. This reduces rewrite cost for state transitions and lowers
S3 request/latency overhead for large payloads.

## Problem
Every state transition overwrites the task object. If inputs/outputs are large,
this becomes expensive and slow, and can dominate overall cost.

## Solution
Add optional input/output references:
- Task object stores only small metadata and a reference key
- Payload stored at `payloads/{task_id}/input.json` and `payloads/{task_id}/output.json`
- Readers load payloads lazily when needed

## API Sketch (Python)
```python
# Submit with payload refs
await queue.submit(
    "process_blob",
    input={"payload_ref": "payloads/abc/input.json"},
    use_payload_refs=True,
)

# Worker returns output ref
return {"payload_ref": "payloads/abc/output.json"}
```

## Storage Layout
```
bucket/
├── tasks/{shard}/{id}.json
└── payloads/{id}/
    ├── input.json
    └── output.json
```

## Migration / Compatibility
- Default remains inline payloads (no behavior change).
- Payload refs are opt-in per task or per queue.

## Related Issues
- `docs/bugs/BUG-007-input-output-refs-unimplemented.md`
