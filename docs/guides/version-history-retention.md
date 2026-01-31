# Version History Retention Guide

qo uses S3 bucket versioning to automatically maintain a complete audit trail of every task state transition. While this provides powerful debugging and compliance capabilities, it can also drive storage costs at scale without proper lifecycle management.

This guide explains how version history works, its cost implications, and how to configure retention policies for your use case.

## How Version History Works

Every task in qo is stored as a single JSON object at `tasks/{shard}/{task_id}.json`. When S3 versioning is enabled, each state change creates a new version:

```
Task Lifecycle Versions:
┌─────────────────────────────────────────────────────────────┐
│ v1: Created (status: Pending)                               │
│ v2: Claimed by worker (status: Running, lease_expires_at)   │
│ v3: Completed (status: Completed, output)                   │
└─────────────────────────────────────────────────────────────┘
```

A task that completes successfully on the first attempt creates 3 versions. Tasks with retries create additional versions:

| Scenario | Versions per Task |
|----------|-------------------|
| Success on first attempt | 3 |
| Success after 1 retry | 5 |
| Success after 3 retries | 9 |
| Failed after max retries (3) | 8 |

## Cost Implications

S3 charges for storage based on total bytes stored, including all versions. Without lifecycle policies, version history grows indefinitely.

### Example: Production Queue

Consider a production queue processing 100,000 tasks/day:

| Parameter | Value |
|-----------|-------|
| Tasks per day | 100,000 |
| Average task JSON size | 2 KB |
| Average versions per task | 4 |
| Storage per day | 100,000 x 2 KB x 4 = 800 MB |
| Storage per 30 days | 24 GB |
| Storage per year (no cleanup) | 292 GB |

**Monthly cost estimates (AWS S3 Standard, us-east-1):**

| Retention | Storage | Monthly Cost |
|-----------|---------|--------------|
| 7 days | ~5.6 GB | ~$0.13 |
| 30 days | ~24 GB | ~$0.55 |
| 90 days | ~72 GB | ~$1.66 |
| No expiration (1 year) | ~292 GB | ~$6.72 |

For high-volume queues (1M+ tasks/day), costs scale proportionally. A queue processing 1 million tasks daily without lifecycle policies would accumulate nearly 3 TB of versions per year.

### Hidden Costs

Beyond storage, version history affects:

1. **`get_history()` latency**: Retrieving task history requires `ListObjectVersions` followed by individual `GET` requests per version. High version counts increase latency.

2. **LIST operations**: While qo uses index prefixes to avoid listing task objects directly, administrative queries and the dashboard may list versions.

## Recommended Lifecycle Policies

S3 lifecycle policies automatically manage version retention. Configure these at the bucket level to expire old versions.

### Policy Structure

A lifecycle policy for qo should:
1. Keep current versions indefinitely (active tasks)
2. Retain non-current versions for your audit window
3. Optionally transition older versions to cheaper storage
4. Expire non-current versions after a defined period

### Development Environment

Short retention for development and testing:

```json
{
  "Rules": [
    {
      "ID": "expire-old-versions-dev",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "tasks/"
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 7
      }
    }
  ]
}
```

### Production (30-Day Retention)

Standard retention for production workloads:

```json
{
  "Rules": [
    {
      "ID": "expire-old-versions-production",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "tasks/"
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      }
    }
  ]
}
```

### Compliance (Tiered Storage with Long Retention)

For regulatory requirements needing longer retention with cost optimization:

```json
{
  "Rules": [
    {
      "ID": "archive-old-versions",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "tasks/"
      },
      "NoncurrentVersionTransitions": [
        {
          "NoncurrentDays": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "NoncurrentDays": 90,
          "StorageClass": "GLACIER"
        }
      ],
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 365
      }
    }
  ]
}
```

**Storage class costs (AWS S3, us-east-1):**

| Storage Class | Cost/GB/Month | Best For |
|---------------|---------------|----------|
| Standard | $0.023 | Active access |
| Standard-IA | $0.0125 | Infrequent access (30+ days) |
| Glacier Instant | $0.004 | Archive with instant retrieval |
| Glacier | $0.0036 | Archive (minutes to retrieve) |

## Configuring Lifecycle Policies

### AWS CLI

Save your policy to a file (e.g., `lifecycle.json`) and apply:

```bash
# Apply lifecycle policy
aws s3api put-bucket-lifecycle-configuration \
  --bucket your-qo-bucket \
  --lifecycle-configuration file://lifecycle.json

# Verify the policy
aws s3api get-bucket-lifecycle-configuration \
  --bucket your-qo-bucket
```

### AWS Console

1. Navigate to S3 > Your Bucket > Management > Lifecycle rules
2. Click "Create lifecycle rule"
3. Name: `qo-version-retention`
4. Filter: Prefix `tasks/`
5. Actions:
   - Select "Permanently delete noncurrent versions of objects"
   - Set "Days after objects become noncurrent": 30 (or your preference)
6. Review and create

### Terraform

```hcl
resource "aws_s3_bucket" "qo" {
  bucket = "your-qo-bucket"
}

resource "aws_s3_bucket_versioning" "qo" {
  bucket = aws_s3_bucket.qo.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "qo" {
  bucket = aws_s3_bucket.qo.id

  rule {
    id     = "qo-version-retention"
    status = "Enabled"

    filter {
      prefix = "tasks/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
```

### Terraform with Tiered Storage

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "qo_compliance" {
  bucket = aws_s3_bucket.qo.id

  rule {
    id     = "qo-version-retention-compliance"
    status = "Enabled"

    filter {
      prefix = "tasks/"
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }
  }
}
```

## S3-Compatible Storage Providers

Not all S3-compatible storage providers support lifecycle policies identically:

| Provider | Versioning | Lifecycle Policies | Storage Classes |
|----------|------------|-------------------|-----------------|
| AWS S3 | Full | Full | Full |
| MinIO | Full | Full | Limited |
| Garage | Full | Partial | No |
| Cloudflare R2 | Limited | Limited | No |

Check your provider's documentation for specific capabilities.

## Tradeoffs

### Shorter Retention (7-14 days)

**Pros:**
- Lowest storage cost
- Faster `get_history()` queries
- Simpler compliance (less data to manage)

**Cons:**
- Limited debugging window for delayed investigations
- Cannot audit tasks older than retention period

**Best for:** Development, staging, high-volume/low-value tasks

### Medium Retention (30 days)

**Pros:**
- Balances cost and auditability
- Covers most incident investigation windows
- Standard for production workloads

**Cons:**
- May not meet regulatory requirements
- Still accumulates significant storage for very high volume

**Best for:** Most production workloads

### Long Retention with Tiering (90+ days)

**Pros:**
- Full audit trail for compliance
- Storage tiering reduces cost significantly
- Can retrieve historical data when needed

**Cons:**
- Higher complexity
- Retrieval from Glacier incurs additional cost and latency
- Requires planning for data retrieval workflows

**Best for:** Regulated industries, financial services, healthcare

## Monitoring Version Growth

Track version accumulation with S3 inventory or bucket metrics:

```bash
# Count versions for a specific task
aws s3api list-object-versions \
  --bucket your-qo-bucket \
  --prefix "tasks/" \
  --query 'length(Versions)'

# Get bucket storage metrics (requires metrics configuration)
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name BucketSizeBytes \
  --dimensions Name=BucketName,Value=your-qo-bucket Name=StorageType,Value=StandardStorage \
  --start-time $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 86400 \
  --statistics Average
```

## Related Documentation

- [Task History Bug (Pagination)](../bugs/BUG-001-history-pagination.md)
- [Version History Costs Issue](../issues/ISSUE-004-version-history-costs.md)
- [AWS S3 Lifecycle Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
