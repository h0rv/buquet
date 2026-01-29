# S3 Features for Object Storage Databases

Technical references for S3 features that enable building databases and task queues on object storage.

---

## Key Features Timeline

| Feature | Date | Impact |
|---------|------|--------|
| Strong Consistency | Dec 2020 | Read-after-write guaranteed for all operations |
| `If-None-Match: *` | Aug 2024 | Write-once semantics, leader election |
| `If-Match: "etag"` | Nov 2024 | Compare-and-swap, optimistic locking |
| Conditional Delete (AWS S3) | Sep 2025 | Optional index cleanup |

---

## 1. Strong Consistency (December 2020)

### Source
- **AWS What's New**: [Amazon S3 now delivers strong read-after-write consistency](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/)
- **Werner Vogels Blog**: [Amazon S3: Strong Consistency](https://www.allthingsdistributed.com/2020/12/amazon-s3-strong-consistency.html)

### Key Quote
> "Any request for S3 storage is now strongly consistent. After a successful write of a new object or an overwrite of an existing object, any subsequent read request immediately receives the latest version of the object. S3 also provides strong consistency for list operations."

### Impact
Before this change, S3 had eventual consistency for overwrites and deletes. After December 2020, read-after-write consistency is guaranteed, making S3 viable for database-like workloads.

---

## 2. Conditional Writes: If-None-Match (August 2024)

### Source
- **AWS What's New**: [Amazon S3 adds new conditional write capability](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-write-capability/)
- **AWS Documentation**: [Using conditional requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)

### Usage
```http
PUT /object HTTP/1.1
If-None-Match: *
```

Prevents object creation if the key already exists. Returns `412 Precondition Failed` if object exists.

### Use Cases
- Write-once semantics
- Leader election
- Task claiming (create running/ entry only if not exists)

### Implementation Example
- **Gunnar Morling**: [Leader Election With S3 Conditional Writes](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/)

---

## 3. Conditional Writes: If-Match (November 2024)

### Source
- **AWS What's New**: [Amazon S3 adds new functionality for conditional writes](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)
- **Simon Willison**: [Analysis](https://simonwillison.net/2024/Nov/26/s3-conditional-writes/)

### Usage
```http
PUT /object HTTP/1.1
If-Match: "etag-value"
```

Only writes if the object's current ETag matches. Returns `412 Precondition Failed` on mismatch.

### Key Quote
> "S3 then evaluates if the object's ETag matches the value provided in the API request before committing the write... This new conditional header can help improve the efficiency of your large-scale analytics, distributed machine learning, and other highly parallelized workloads by reliably offloading compare and swap operations to S3."

### Use Cases
- Optimistic locking
- Atomic updates
- Preventing lost updates in concurrent writes

---

## 4. Supported Operations

| Operation | If-None-Match | If-Match |
|-----------|---------------|----------|
| PutObject | Yes | Yes |
| CompleteMultipartUpload | Yes | Yes |
| CopyObject | No | No |
| DeleteObject | AWS S3 only (2025) | AWS S3 only (2025) |

**Important**: COPY does not support conditional headers. Conditional DELETE is
available in AWS S3 as of 2025 but is not universal across S3-compatible
providers. Design your workflows to tolerate best-effort deletes.

---

## 5. S3-Compatible Storage Support

| Storage | If-None-Match | If-Match | Notes |
|---------|---------------|----------|-------|
| AWS S3 | Yes | Yes | Full support since Nov 2024 |
| Garage | Yes | Yes | Rust-based, AGPLv3 |
| MinIO | Yes | Yes | Apache 2.0 |
| Cloudflare R2 | Yes | Yes | S3-compatible |
| Google GCS | Yes | Yes | Had preconditions since ~2015 |

---

## 6. Real-World Implementations

### Turbopuffer
- **Blog**: [turbopuffer: fast search on object storage](https://turbopuffer.com/blog/turbopuffer)
- **AWS Startups**: [How turbopuffer is refactoring the economics of search](https://aws.amazon.com/startups/learn/how-turbopuffer-is-refactoring-the-economics-of-search)

Key quote from AWS:
> "The S3 team have been great partners in providing access to beta features and soliciting API feedback to help make turbopuffer the first database at scale running exclusively on object storage."

### Architecture Insight
From Jason Liu's analysis:
> "The TurboPuffer architecture leverages three technological advances: NVMe SSDs that are only 4-5x slower than memory but 100x cheaper than DRAM, S3's strong consistency guarantees (introduced in 2020), S3's compare-and-swap functionality."

---

## 7. Limitations

### ETag-Based, Not Value-Based
S3 conditional writes compare ETags (metadata), not object contents.

You **can** do:
```
UPDATE object IF etag == "expected-etag"
```

You **cannot** do:
```
UPDATE object IF value == "expected-content"
```

This is sufficient for task queues and most coordination patterns, but not for true value-based CAS.

### Conditional DELETE Is Not Universal
AWS S3 supports conditional deletes (2025), but many S3-compatible providers do not.
Design around this by:
- Using best-effort deletes with cleanup processes
- Keeping state in a single object and updating it conditionally

---

## 8. Cost Considerations (AWS S3, 2025)

| Operation | Cost per 1000 |
|-----------|---------------|
| PUT | $0.005 |
| GET | $0.0004 |
| LIST | $0.005 |
| DELETE | Free |

Example: 1M tasks/month with ~25 operations each = ~$100/month total.

---

## References

1. AWS What's New announcements (linked above)
2. Werner Vogels: [All Things Distributed](https://www.allthingsdistributed.com/)
3. Gunnar Morling: [Leader Election with S3](https://www.morling.dev/blog/leader-election-with-s3-conditional-writes/)
4. Simon Willison: [S3 Conditional Writes Analysis](https://simonwillison.net/2024/Nov/26/s3-conditional-writes/)
5. Tigris: [What's the Big Deal with Conditional Writes?](https://www.tigrisdata.com/blog/s3-conditional-writes/)
6. AWS Storage Blog: [Building multi-writer applications on S3](https://aws.amazon.com/blogs/storage/building-multi-writer-applications-on-amazon-s3-using-native-controls/)
