# `buquet doctor` Command

> **Status:** COMPLETED (2026-01-26)
> **Effort:** ~200 lines of Rust
> **Tier:** 1 (Quick Win)

A diagnostic command that validates the environment and S3 connectivity.

## Why

When something doesn't work, users waste time debugging. `buquet doctor` immediately tells them what's wrong and how to fix it.

## Usage

```bash
# Basic checks (read-only)
buquet doctor

# Include write test (creates/deletes test object)
buquet doctor --write-test
```

## Example Output

### Success
```
$ buquet doctor
buquet doctor - checking environment

✓ Environment variables set
✓ S3 client initialized
✓ Bucket 'buquet-dev' exists and is accessible
✓ Can list objects (found 16 task shards)
✓ 2 healthy worker(s) online

All checks passed!
```

### Failure with suggestions
```
$ buquet doctor
buquet doctor - checking environment

✓ Environment variables set
✗ S3 connection failed: connection refused

Suggestions:
  - Check S3_ENDPOINT is correct (current: http://localhost:3900)
  - Ensure your S3 service is running: docker compose up -d
  - Verify network connectivity: curl http://localhost:3900
```

## Implementation

```rust
#[derive(Args)]
pub struct DoctorArgs {
    /// Run all checks including writes (creates/deletes test object)
    #[arg(long)]
    write_test: bool,
}

async fn doctor(args: DoctorArgs) -> Result<()> {
    println!("buquet doctor - checking environment\n");

    // 1. Check environment variables
    check_env("S3_BUCKET")?;
    check_env("S3_REGION")?;
    check_env("AWS_ACCESS_KEY_ID")?;
    check_env("AWS_SECRET_ACCESS_KEY")?;

    // 2. Test S3 connectivity
    let client = S3Client::from_env().await?;
    println!("✓ S3 client initialized");

    // 3. Check bucket exists and is accessible
    client.head_bucket().await?;
    println!("✓ Bucket '{}' exists and is accessible", bucket);

    // 4. List some tasks to verify read access
    let tasks = client.list_objects("tasks/", 1).await?;
    println!("✓ Can list objects (found {} task shards)", tasks.len());

    // 5. Check for active workers
    let workers = list_workers(&client).await?;
    let healthy = workers.iter().filter(|w| w.is_healthy()).count();
    if healthy > 0 {
        println!("✓ {} healthy worker(s) online", healthy);
    } else {
        println!("⚠ No healthy workers (last heartbeat may be stale)");
    }

    // 6. Optional write test
    if args.write_test {
        let test_key = format!("_buquet_doctor_test_{}", Uuid::new_v4());
        client.put_object(&test_key, b"test").await?;
        client.delete_object(&test_key).await?;
        println!("✓ Write test passed (created and deleted test object)");
    }

    println!("\nAll checks passed!");
    Ok(())
}
```

## Checks to Perform

1. Environment variables are set
2. S3 client can initialize
3. Bucket exists and is accessible
4. Can list objects (read permission)
5. Workers are online and healthy
6. (Optional) Can write and delete objects

## Dependencies

None (uses existing S3Client)
