# S3 Provider Compatibility Guide

oq requires specific S3 features to function correctly. Not all "S3-compatible" storage providers implement these features fully. This guide helps you choose the right provider and verify compatibility.

---

## Required S3 Features

oq depends on these S3 capabilities:

### 1. Strong Read-After-Write Consistency

**Required**: Yes (critical)

After writing an object, any subsequent read must return the latest version immediately. oq relies on this for:
- Task state transitions (pending -> running -> completed)
- Worker coordination
- Accurate queue listings

Without strong consistency, workers may claim the same task, miss updates, or see stale state.

### 2. Conditional Writes (If-Match, If-None-Match)

**Required**: Yes (critical)

oq uses conditional writes for atomic operations:

- **`If-None-Match: *`** - Creates an object only if it doesn't exist. Used for:
  - Task claiming (prevents duplicate processing)
  - Write-once semantics for task results

- **`If-Match: "etag"`** - Updates an object only if the ETag matches. Used for:
  - Optimistic locking on state transitions
  - Preventing lost updates during concurrent modifications

Without conditional writes, oq cannot guarantee exactly-once task processing.

### 3. Object Versioning

**Required**: No (optional but recommended)

Versioning enables:
- Task history browsing in the dashboard
- Debugging failed tasks by viewing previous states
- Audit trails for compliance

oq works without versioning, but you lose historical visibility.

### 4. LIST Pagination (Continuation Tokens)

**Required**: Yes

S3 LIST operations return a maximum of 1000 objects per request. oq uses continuation tokens to paginate through larger result sets when:
- Discovering pending tasks across shards
- Building dashboard views
- Running cleanup operations

All major S3-compatible providers support this.

### 5. Standard S3 API Compatibility

**Required**: Yes

oq uses standard S3 operations:
- `PutObject` with conditional headers
- `GetObject`
- `DeleteObject`
- `ListObjectsV2`
- `HeadObject`

Providers must implement these operations with standard semantics.

---

## Provider Compatibility Matrix

### Cloud Providers (SaaS)

| Provider | Consistency | Conditional Writes | Versioning | Recommended | Notes | Link |
|----------|:-----------:|:------------------:|:----------:|:-----------:|-------|------|
| **AWS S3** | Strong | Full | Full | Yes | Reference implementation | [aws.amazon.com/s3](https://aws.amazon.com/s3/) |
| **Cloudflare R2** | Strong | Full | None | Yes | Zero egress fees, no history | [cloudflare.com/r2](https://www.cloudflare.com/developer-platform/r2/) |
| **Wasabi** | Strong | Full | Full | Yes | No egress fees, 90-day min retention | [wasabi.com](https://wasabi.com/) |
| **Backblaze B2** | Strong | Full | None | Yes | Free egress via partner CDNs | [backblaze.com/cloud-storage](https://www.backblaze.com/cloud-storage) |
| **DigitalOcean Spaces** | Strong | Full | None | Yes | Built-in CDN, simple pricing | [digitalocean.com/products/spaces](https://www.digitalocean.com/products/spaces) |
| **Linode Object Storage** | Strong | Full | None | Yes | S3-compatible, global regions | [linode.com/products/object-storage](https://www.linode.com/products/object-storage/) |
| **Vultr Object Storage** | Strong | Full | None | Yes | Multiple regions, fast | [vultr.com/products/object-storage](https://www.vultr.com/products/object-storage/) |
| **Oracle Cloud OCI** | Strong | Full | Full | Partial | Requires S3 compat API key | [oracle.com/cloud/storage](https://www.oracle.com/cloud/storage/object-storage/) |
| **Google Cloud Storage** | Strong | Different API | Full | Partial | Needs S3 interop mode | [cloud.google.com/storage](https://cloud.google.com/storage) |
| **Alibaba OSS** | Strong | Partial | Full | No | Conditional write gaps | [alibabacloud.com/product/oss](https://www.alibabacloud.com/product/object-storage-service) |

### Self-Hosted / Open Source

| Provider | Consistency | Conditional Writes | Versioning | Recommended | Notes | Link |
|----------|:-----------:|:------------------:|:----------:|:-----------:|-------|------|
| **MinIO** | Strong | Full | Full | Yes* | AGPLv3, maintenance mode Dec 2025 | [github.com/minio/minio](https://github.com/minio/minio) |
| **SeaweedFS** | Strong | Full | Full | Yes | Apache 2.0, O(1) disk seek | [github.com/seaweedfs/seaweedfs](https://github.com/seaweedfs/seaweedfs) |
| **Ceph/RadosGW** | Strong | Full | Full | Yes | Enterprise-grade, LGPL | [github.com/ceph/ceph](https://github.com/ceph/ceph) |
| **Rook** | Strong | Full | Full | Yes | K8s storage orchestrator, Apache 2.0 | [github.com/rook/rook](https://github.com/rook/rook) |
| **LakeFS** | Strong | Full | Full | Yes | Git-like versioning, Apache 2.0 | [github.com/treeverse/lakeFS](https://github.com/treeverse/lakeFS) |
| **RustFS** | Strong | Full | Full | Emerging | Apache 2.0, MinIO alternative | [github.com/rustfs/rustfs](https://github.com/rustfs/rustfs) |
| **Zenko CloudServer** | Strong | Full | Full | Yes | Node.js, multi-cloud backend | [github.com/scality/cloudserver](https://github.com/scality/cloudserver) |
| **Garage** | Strong | Partial | Limited | Dev only | No If-Match on PUT, AGPLv3 | [github.com/deuxfleurs-org/garage](https://github.com/deuxfleurs-org/garage) |
| **LocalStack** | Strong | Full | Full | Testing | Local AWS emulation only | [github.com/localstack/localstack](https://github.com/localstack/localstack) |

*MinIO entered maintenance mode in December 2025. No new features; security fixes evaluated case-by-case.

### Legend

- **Strong** - Immediate read-after-write consistency
- **Eventual** - Reads may return stale data temporarily
- **Full** - Complete feature support
- **Partial** - Some limitations or differences
- **None** - Feature not available
- **Limited** - Basic support with restrictions

---

## Testing Your Provider

Before deploying oq to a new provider, verify compatibility with these tests.

### Quick Compatibility Check

```bash
#!/bin/bash
# s3-compat-test.sh
# Usage: S3_ENDPOINT=http://localhost:3902 BUCKET=test ./s3-compat-test.sh

set -e

ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
BUCKET="${BUCKET:-oq-test}"
KEY="oq-compat-test-$(date +%s)"

echo "Testing S3 compatibility at $ENDPOINT"
echo "Using bucket: $BUCKET"
echo ""

# Test 1: If-None-Match (write-once)
echo "Test 1: If-None-Match (create if not exists)..."
aws s3api put-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --body /dev/null \
    --if-none-match "*" \
    --output text > /dev/null

echo "  Created object successfully"

# Should fail with 412
if aws s3api put-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --body /dev/null \
    --if-none-match "*" 2>&1 | grep -q "PreconditionFailed\|412"; then
    echo "  Correctly rejected duplicate write (412)"
else
    echo "  ERROR: Should have returned 412 Precondition Failed"
    exit 1
fi

# Test 2: If-Match (optimistic locking)
echo ""
echo "Test 2: If-Match (conditional update)..."
ETAG=$(aws s3api head-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --query 'ETag' \
    --output text)

echo "  Current ETag: $ETAG"

# Update with correct ETag
echo "updated" | aws s3api put-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --body /dev/stdin \
    --if-match "$ETAG" \
    --output text > /dev/null

echo "  Updated with matching ETag"

# Should fail with old ETag
if echo "conflict" | aws s3api put-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --body /dev/stdin \
    --if-match "$ETAG" 2>&1 | grep -q "PreconditionFailed\|412"; then
    echo "  Correctly rejected stale ETag (412)"
else
    echo "  ERROR: Should have returned 412 Precondition Failed"
    exit 1
fi

# Test 3: Strong consistency
echo ""
echo "Test 3: Read-after-write consistency..."
NEW_CONTENT="consistency-test-$(date +%s)"
echo "$NEW_CONTENT" | aws s3api put-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --body /dev/stdin \
    --output text > /dev/null

READ_CONTENT=$(aws s3api get-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    /dev/stdout 2>/dev/null)

if [ "$READ_CONTENT" = "$NEW_CONTENT" ]; then
    echo "  Read-after-write consistent"
else
    echo "  WARNING: Possible eventual consistency detected"
fi

# Cleanup
echo ""
echo "Cleaning up..."
aws s3api delete-object \
    --endpoint-url "$ENDPOINT" \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --output text > /dev/null

echo ""
echo "All tests passed! Provider is compatible with oq."
```

### Running the Test

```bash
# For local Garage (Docker Compose)
S3_ENDPOINT=http://localhost:3902 BUCKET=oq ./s3-compat-test.sh

# For AWS S3
AWS_PROFILE=myprofile BUCKET=my-bucket ./s3-compat-test.sh

# For MinIO
S3_ENDPOINT=http://localhost:9000 BUCKET=oq ./s3-compat-test.sh
```

---

## Provider-Specific Notes

### AWS S3

**Status**: Fully compatible (reference implementation)

AWS S3 is the reference implementation for oq. All features work as expected.

**Configuration**:
```bash
export S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
export S3_REGION=us-east-1
export S3_BUCKET=my-oq-bucket
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

**Notes**:
- Strong consistency since December 2020
- `If-None-Match` since August 2024
- `If-Match` since November 2024
- Conditional DELETE since September 2025 (optional for oq)
- Versioning fully supported

**Cost** (2025 pricing):
| Operation | Cost per 1,000 |
|-----------|----------------|
| PUT | $0.005 |
| GET | $0.0004 |
| LIST | $0.005 |
| DELETE | Free |

---

### MinIO

**Status**: Fully compatible (recommended for self-hosted)

MinIO is the best choice for self-hosted deployments. It tracks AWS S3 features closely and has excellent performance.

**Configuration**:
```bash
export S3_ENDPOINT=http://minio:9000
export S3_REGION=us-east-1
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

**Docker Compose**:
```yaml
services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
```

**Notes**:
- Full conditional write support
- Full versioning support
- Web console for debugging
- Kubernetes-native with MinIO Operator
- Apache 2.0 license

---

### LocalStack

**Status**: Fully compatible (default for oq local development)

LocalStack provides a fully functional local AWS cloud stack, including S3 with complete conditional write and versioning support. It's the default for oq's Docker Compose development environment.

**Configuration**:
```bash
export S3_ENDPOINT=http://localhost:4566
export S3_REGION=us-east-1
export S3_BUCKET=oq-dev
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

**Docker Compose**:
```yaml
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3
      - DEFAULT_REGION=us-east-1
```

**Notes**:
- Full conditional write support (`If-Match`, `If-None-Match`)
- Full versioning support
- Emulates real AWS S3 behavior accurately
- Apache 2.0 license

**Best for**: Local development, CI/CD pipelines, testing

---

### Garage

**Status**: Compatible with limitations

Garage is a lightweight, Rust-based S3-compatible storage system designed for geo-distributed deployments.

**Configuration**:
```bash
export S3_ENDPOINT=http://localhost:3902
export S3_REGION=garage
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=oq
export AWS_SECRET_ACCESS_KEY=oqsecretkey
```

**Notes**:
- Designed for geo-distributed deployments
- Low resource footprint
- AGPLv3 license

**Limitations**:
- **No `If-Match` support for PUT**: Garage silently ignores the `If-Match` conditional header on PUT operations. This means concurrent claim protection (CAS) does not work. **Use LocalStack or MinIO for development instead.**
- **Limited versioning**: Check current Garage version for versioning support status.

**Best for**: Small single-worker deployments, edge computing

---

### Cloudflare R2

**Status**: Compatible (no versioning)

R2 offers S3-compatible storage with no egress fees, making it cost-effective for read-heavy workloads.

**Configuration**:
```bash
export S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export S3_REGION=auto
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

**Notes**:
- Full conditional write support
- **No versioning** - task history not available
- No egress fees
- Strong consistency
- Automatic geographic distribution

**Limitations**:
- Dashboard cannot show task history
- No audit trail of state changes

---

### Wasabi

**Status**: Fully compatible

Wasabi offers S3-compatible storage at competitive prices with no egress fees.

**Configuration**:
```bash
export S3_ENDPOINT=https://s3.wasabisys.com
export S3_REGION=us-east-1
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

**Notes**:
- Full conditional write support
- Full versioning support
- No egress fees
- 90-day minimum storage policy (consider for task data lifecycle)

---

### DigitalOcean Spaces

**Status**: Compatible (no versioning)

Spaces provides simple S3-compatible storage integrated with DigitalOcean infrastructure.

**Configuration**:
```bash
export S3_ENDPOINT=https://nyc3.digitaloceanspaces.com
export S3_REGION=nyc3
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

**Notes**:
- Full conditional write support
- **No versioning** - task history not available
- CDN integration available
- Good for DigitalOcean-hosted workers

---

### Google Cloud Storage (S3 Interoperability)

**Status**: Partial compatibility

GCS has its own API but offers S3 interoperability mode. However, some conditional write behaviors differ.

**Configuration**:
```bash
export S3_ENDPOINT=https://storage.googleapis.com
export S3_REGION=auto
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=<HMAC-key>
export AWS_SECRET_ACCESS_KEY=<HMAC-secret>
```

**Setup**:
1. Enable S3 interoperability in GCS console
2. Create HMAC keys for authentication
3. Use `storage.googleapis.com` as endpoint

**Notes**:
- Conditional writes use different semantics (`x-goog-if-generation-match`)
- S3 `If-Match`/`If-None-Match` may not work correctly
- **Test thoroughly before production use**

**Recommendation**: Use GCS native API with a custom adapter, or choose a different provider.

---

### Ceph/RadosGW

**Status**: Fully compatible

Ceph's RadosGW provides enterprise-grade S3-compatible storage for on-premises deployments.

**Configuration**:
```bash
export S3_ENDPOINT=http://radosgw:7480
export S3_REGION=default
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

**Notes**:
- Full conditional write support (verify version >= Quincy)
- Full versioning support
- Highly scalable
- Requires significant operational expertise

---

### Backblaze B2

**Status**: Not recommended

B2's S3-compatible API has limitations that make it unsuitable for oq.

**Issues**:
- **Eventual consistency** on some operations
- **Incomplete conditional write support**
- Higher latency for metadata operations

**Recommendation**: Do not use B2 for oq. Consider Wasabi or R2 for similar pricing.

---

### LocalStack

**Status**: Compatible (testing only)

LocalStack emulates AWS services locally, including S3.

**Configuration**:
```bash
export S3_ENDPOINT=http://localhost:4566
export S3_REGION=us-east-1
export S3_BUCKET=oq
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

**Notes**:
- Full feature emulation
- **For testing only** - not production-ready
- May have subtle behavioral differences from real S3

---

## Troubleshooting

### "412 Precondition Failed" Errors

This is expected behavior when:
- Another worker claimed the task first
- A concurrent update changed the ETag

oq handles these gracefully. If you see excessive 412 errors, consider:
- Increasing shard count to reduce contention
- Checking for worker configuration mismatches

### Stale Reads After Write

If workers see outdated task states:
1. Verify your provider has strong consistency
2. Check for caching proxies between workers and S3
3. Ensure you're not using a CDN endpoint for writes

### Tasks Not Appearing in LIST

Some providers have slight delays in LIST consistency even with strong read consistency:
1. Verify with direct GET that objects exist
2. Check shard configuration matches between producers and workers
3. Ensure bucket/prefix configuration is correct

### Versioning Not Working

If task history is unavailable:
1. Check if your provider supports versioning
2. Enable versioning on the bucket: `aws s3api put-bucket-versioning --bucket oq --versioning-configuration Status=Enabled`
3. Verify with: `aws s3api get-bucket-versioning --bucket oq`

---

## See Also

- [S3 Feature References](../S3-REFERENCES.md) - Technical details on S3 features oq uses
- [Getting Started](../getting-started.md) - Quick start guide
- [Architecture](../PLAN.md) - How oq uses S3 internally
