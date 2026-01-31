# BUG-007: input_ref/output_ref are modeled but not implemented

Status: **Won't Implement** (Closed)

## Summary
The Task model included `input_ref`/`output_ref` fields for large payload support, but no code implemented them.

## Resolution
Fields removed from the Task model. Users who need large payloads can store them in S3 themselves and reference via their input schema:

```python
@worker.task("process_file")
async def handle(task):
    data = await s3.get(task.input["file_ref"])
    result = process(data)
    return {"output_ref": await s3.put(f"results/{task.id}.json", result)}
```

This keeps qo simple and avoids complexity around credentials, cross-bucket access, and streaming semantics.
