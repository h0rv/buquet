# Task Schemas

> **Status:** Implemented
> **Effort:** ~50 lines Rust, ~20 lines Python
> **Tier:** 2 (Production Essential)

Store and validate JSON Schemas for task inputs/outputs. Format-agnostic - bring your own schema generator.

## Philosophy

Schemas are just another S3 object. qo stores them, retrieves them, and validates against them. That's it.

No version tracking. No automatic validation. No sync enforcement. No control plane.

## Storage

```
schemas/{task_type}.json
```

```json
{
  "input": {
    "type": "object",
    "properties": {
      "to": { "type": "string" },
      "subject": { "type": "string" }
    },
    "required": ["to", "subject"]
  },
  "output": {
    "type": "object",
    "properties": {
      "sent": { "type": "boolean" }
    }
  }
}
```

## API

### Python

```python
# Store (accepts any JSON Schema dict)
await queue.publish_schema("send_email", {
    "input": {...},
    "output": {...}
})

# Retrieve
schema = await queue.get_schema("send_email")

# Validate (opt-in, raises SchemaValidationError if invalid)
queue.validate_input("send_email", {"to": "x", "subject": "y"})
queue.validate_output("send_email", {"sent": True})

# List all schemas
schemas = await queue.list_schemas()  # ["send_email", "process_order"]
```

### CLI

```bash
# Store
qo schema publish send_email schema.json

# Retrieve
qo schema get send_email

# Validate
qo schema validate send_email --input '{"to": "x"}'
qo schema validate send_email --output '{"sent": true}'

# List
qo schema list

# Delete
qo schema delete send_email
```

### Rust

```rust
// Store
queue.publish_schema("send_email", schema_json).await?;

// Retrieve
let schema = queue.get_schema("send_email").await?;

// Validate
queue.validate_input("send_email", &input_json)?;
```

## User Responsibilities

qo follows the same philosophy as the rest of the system: **simple primitives, user handles coordination.**

| Concern | qo's job | User's job |
|---------|----------|------------|
| Schema storage | Store/retrieve from S3 | Decide what schema to publish |
| Validation | Validate JSON against schema | Decide when to call validate |
| Versioning | None | Use task_type naming: `send_email_v2` |
| Sync enforcement | None | Coordinate deploys, call validate |
| Breaking changes | None | Manage schema evolution |
| Schema deletion | Delete when asked | Ensure no tasks depend on it |

### Out-of-sync scenarios

**All user's responsibility:**

| Scenario | What happens | User should |
|----------|--------------|-------------|
| Producer schema ≠ worker code | Worker may fail or misbehave | Coordinate deploys |
| Schema updated mid-flight | Tasks validated against current schema | Deploy carefully |
| Schema deleted with pending tasks | Validation calls fail | Don't delete active schemas |
| Multiple worker versions | Different behavior per worker | Use versioned task_types |

### If you want safety

```python
# Validate at submit time
queue.validate_input("send_email", data)  # raises if invalid
await queue.submit("send_email", data)

# Validate at worker time
@worker.task("send_email")
async def handle(input):
    queue.validate_input("send_email", input)  # optional
    # ... process
    output = {"sent": True}
    queue.validate_output("send_email", output)  # optional
    return output
```

If you don't call validate, that's your choice.

## Debuggable

Schemas are just S3 objects:

```bash
# Inspect directly
aws s3 cat s3://my-bucket/schemas/send_email.json

# Copy from local
aws s3 cp schema.json s3://my-bucket/schemas/send_email.json

# List all
aws s3 ls s3://my-bucket/schemas/
```

## Implementation

### Files to Change

- `crates/qo/src/queue/schema.rs` (new) - ~30 lines: publish, get, list, delete
- `crates/qo/src/queue/validate.rs` (new) - ~20 lines: validate against schema
- `crates/qo/src/python/queue.rs` - Add schema methods
- `crates/qo/src/cli/commands.rs` - Add `Schema` subcommand
- `crates/qo/python/qo/_qo.pyi` - Type stubs

### Dependencies

```toml
jsonschema = "0.18"  # JSON Schema validation
```

No Python dependencies.

## Non-Goals

Things we intentionally don't do:

- **Version tracking** - User manages via task_type naming
- **Automatic validation** - Always opt-in
- **Embedded schema version in tasks** - Storage overhead, complexity
- **Compatibility checking** - No control plane
- **Migration tooling** - User's domain
- **Schema registry UI** - Maybe later, not MVP
