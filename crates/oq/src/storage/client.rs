use std::env;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};

use aws_sdk_s3::{
    config::BehaviorVersion,
    error::SdkError,
    operation::{
        copy_object::CopyObjectError, delete_object::DeleteObjectError,
        get_bucket_versioning::GetBucketVersioningError, get_object::GetObjectError,
        head_object::HeadObjectError, list_object_versions::ListObjectVersionsError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    },
    primitives::ByteStream,
    types::BucketVersioningStatus,
    Client,
};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::{Mutex, RwLock};

use super::error::StorageError;

/// Condition for conditional writes to S3.
#[derive(Debug, Clone)]
pub enum PutCondition {
    /// No condition - unconditional write
    None,
    /// `If-None-Match: *` - Create only if object doesn't exist
    IfNoneMatch,
    /// `If-Match: "etag"` - CAS update only if `ETag` matches
    IfMatch(String),
}

/// Represents a version of an S3 object.
#[derive(Debug, Clone)]
pub struct ObjectVersion {
    /// The version ID of this object version.
    pub version_id: String,
    /// When this version was last modified.
    pub last_modified: DateTime<Utc>,
    /// Whether this is the latest (current) version.
    pub is_latest: bool,
    /// The `ETag` of this version.
    pub etag: String,
}

/// Configuration for connecting to an S3-compatible storage service.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Optional custom endpoint URL (e.g., for local development with LocalStack/MinIO).
    pub endpoint: Option<String>,
    /// The S3 bucket name.
    pub bucket: String,
    /// The AWS region.
    pub region: String,
}

impl S3Config {
    /// Creates a new `S3Config` from environment variables.
    ///
    /// Reads the following environment variables:
    /// - `S3_ENDPOINT` (optional): Custom endpoint URL for S3-compatible storage
    /// - `S3_BUCKET` (required): The bucket name
    /// - `S3_REGION` (required): The AWS region
    ///
    /// # Errors
    ///
    /// Returns an error if `S3_BUCKET` or `S3_REGION` are not set.
    pub fn from_env() -> Result<Self, std::env::VarError> {
        let endpoint = env::var("S3_ENDPOINT").ok();
        let bucket = env::var("S3_BUCKET")?;
        let region = env::var("S3_REGION")?;

        Ok(Self {
            endpoint,
            bucket,
            region,
        })
    }

    /// Creates a new `S3Config` with explicit values.
    #[must_use]
    pub const fn new(endpoint: Option<String>, bucket: String, region: String) -> Self {
        Self {
            endpoint,
            bucket,
            region,
        }
    }
}

/// A wrapper around the AWS S3 client that provides simplified operations
/// for the task queue system.
#[derive(Debug, Clone)]
pub struct S3Client {
    client: Client,
    config: S3Config,
    clock_state: Arc<RwLock<ClockState>>,
    clock_sync_lock: Arc<Mutex<()>>,
    clock_sync_interval: StdDuration,
    clock_key: String,
}

#[derive(Debug, Clone, Default)]
struct ClockState {
    last_server_time: Option<DateTime<Utc>>,
    last_sync_instant: Option<Instant>,
}

impl S3Client {
    /// Creates a new `S3Client` with the given configuration.
    ///
    /// This initializes the AWS SDK client with the provided configuration,
    /// including support for custom endpoints (useful for local development
    /// with `LocalStack`, `MinIO`, etc.).
    pub async fn new(config: S3Config) -> Result<Self, StorageError> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region.clone()))
            .load()
            .await;

        // Build the S3 client config
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        // If a custom endpoint is provided, configure it
        if let Some(ref endpoint) = config.endpoint {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint)
                .force_path_style(true); // Required for most S3-compatible services
        }

        let client = Client::from_conf(s3_config_builder.build());

        Ok(Self {
            client,
            config,
            clock_state: Arc::new(RwLock::new(ClockState::default())),
            clock_sync_lock: Arc::new(Mutex::new(())),
            clock_sync_interval: StdDuration::from_secs(5),
            clock_key: "clock/now".to_string(),
        })
    }

    /// Returns the bucket name.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    /// Returns the current time from S3, with a short-lived local cache.
    ///
    /// This avoids relying on the local wall clock for correctness-critical
    /// time decisions by syncing against S3 server time.
    pub async fn now(&self) -> Result<DateTime<Utc>, StorageError> {
        if let Some(cached) = self.cached_now().await? {
            return Ok(cached);
        }

        let _lock = self.clock_sync_lock.lock().await;
        if let Some(cached) = self.cached_now().await? {
            return Ok(cached);
        }

        let server_time = self.sync_server_time().await?;
        let mut state = self.clock_state.write().await;
        state.last_server_time = Some(server_time);
        state.last_sync_instant = Some(Instant::now());
        drop(state);
        Ok(server_time)
    }

    async fn cached_now(&self) -> Result<Option<DateTime<Utc>>, StorageError> {
        let state = self.clock_state.read().await;
        let (Some(last_time), Some(last_sync)) = (state.last_server_time, state.last_sync_instant)
        else {
            return Ok(None);
        };
        drop(state);
        if last_sync.elapsed() > self.clock_sync_interval {
            return Ok(None);
        }

        let elapsed = last_sync.elapsed();
        let elapsed = Duration::from_std(elapsed).unwrap_or_else(|_| Duration::zero());
        Ok(Some(last_time + elapsed))
    }

    async fn sync_server_time(&self) -> Result<DateTime<Utc>, StorageError> {
        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&self.clock_key)
            .body(ByteStream::from(vec![]))
            .send()
            .await
            .map_err(|ref err| map_put_error(err, &self.clock_key))?;

        let response = self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&self.clock_key)
            .send()
            .await
            .map_err(|ref err| map_head_error(err, &self.clock_key))?;

        let last_modified = response.last_modified().ok_or_else(|| {
            StorageError::S3Error("Clock object missing last_modified".to_string())
        })?;
        let server_time =
            DateTime::from_timestamp(last_modified.secs(), last_modified.subsec_nanos())
                .ok_or_else(|| StorageError::S3Error("Invalid clock timestamp".to_string()))?;

        Ok(server_time)
    }

    /// Uploads an object to S3.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    /// * `body` - The object content as bytes
    /// * `condition` - Conditional write condition:
    ///   - `PutCondition::None` - Unconditional write
    ///   - `PutCondition::IfNoneMatch` - Adds `If-None-Match: *` header to ensure
    ///     the object doesn't already exist (returns `PreconditionFailed` if it does)
    ///   - `PutCondition::IfMatch(etag)` - Adds `If-Match` header for CAS operations
    ///     (returns `PreconditionFailed` if `ETag` doesn't match)
    ///
    /// # Returns
    ///
    /// The `ETag` of the created/updated object on success.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PreconditionFailed` if the condition is not met.
    pub async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: PutCondition,
    ) -> Result<String, StorageError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(ByteStream::from(body));

        match condition {
            PutCondition::None => {}
            PutCondition::IfNoneMatch => {
                request = request.if_none_match("*");
            }
            PutCondition::IfMatch(ref etag) => {
                request = request.if_match(etag);
            }
        }

        let response = request
            .send()
            .await
            .map_err(|ref err| map_put_error(err, key))?;

        let etag = response
            .e_tag()
            .map(ToString::to_string)
            .unwrap_or_default();

        Ok(etag)
    }

    /// Retrieves an object from S3.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    ///
    /// # Returns
    ///
    /// A tuple containing the object body as bytes and the `ETag`.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    pub async fn get_object(&self, key: &str) -> Result<(Vec<u8>, String), StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|ref err| map_get_error(err, key))?;

        let etag = response
            .e_tag()
            .map(ToString::to_string)
            .unwrap_or_default();

        let body = response
            .body
            .collect()
            .await
            .map_err(|err| StorageError::S3Error(format!("Failed to read body: {err}")))?
            .into_bytes()
            .to_vec();

        Ok((body, etag))
    }

    /// Deletes an object from S3.
    ///
    /// This is a best-effort delete operation. S3's `DeleteObject` returns success
    /// even if the object doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    pub async fn delete_object(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|ref err| map_delete_error(err, key))?;

        Ok(())
    }

    /// Copies an object within the same bucket.
    ///
    /// This is an efficient server-side copy that doesn't transfer data
    /// through the client. Useful for shard migrations and reorganizing objects.
    ///
    /// # Arguments
    ///
    /// * `source_key` - The source object key
    /// * `dest_key` - The destination object key
    ///
    /// # Returns
    ///
    /// The `ETag` of the copied object on success.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the source object doesn't exist.
    pub async fn copy_object(
        &self,
        source_key: &str,
        dest_key: &str,
    ) -> Result<String, StorageError> {
        // CopySource format: bucket/key (URL-encoded key for special characters)
        let copy_source = format!("{}/{}", self.config.bucket, source_key);

        let response = self
            .client
            .copy_object()
            .bucket(&self.config.bucket)
            .copy_source(&copy_source)
            .key(dest_key)
            .send()
            .await
            .map_err(|ref err| map_copy_error(err, source_key))?;

        let etag = response
            .copy_object_result()
            .and_then(|r| r.e_tag())
            .map(ToString::to_string)
            .unwrap_or_default();

        Ok(etag)
    }

    /// Lists objects in the bucket with a given prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The key prefix to filter objects
    /// * `limit` - Maximum number of keys to return
    /// * `continuation_token` - Optional continuation token for pagination
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A vector of object keys matching the prefix
    /// - An optional continuation token for fetching the next page
    pub async fn list_objects(
        &self,
        prefix: &str,
        limit: i32,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), StorageError> {
        self.list_objects_after(prefix, limit, continuation_token, None)
            .await
    }

    /// Lists objects in the bucket with a given prefix, starting after a specific key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The key prefix to filter objects
    /// * `limit` - Maximum number of keys to return
    /// * `continuation_token` - Optional continuation token for pagination
    /// * `start_after` - Optional key to start listing after (exclusive)
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A vector of object keys matching the prefix
    /// - An optional continuation token for fetching the next page
    pub async fn list_objects_after(
        &self,
        prefix: &str,
        limit: i32,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), StorageError> {
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(prefix)
            .max_keys(limit);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        if let Some(start) = start_after {
            request = request.start_after(start);
        }

        let response = request
            .send()
            .await
            .map_err(|ref err| map_list_error(err))?;

        let keys = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(ToString::to_string))
            .collect();

        let next_token = response.next_continuation_token().map(ToString::to_string);

        Ok((keys, next_token))
    }

    /// Lists objects in the bucket with a given prefix, following pagination.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The key prefix to filter objects
    /// * `limit` - Maximum number of keys to return across all pages
    ///
    /// # Returns
    ///
    /// A vector of object keys matching the prefix.
    #[allow(clippy::cast_sign_loss)]
    pub async fn list_objects_paginated(
        &self,
        prefix: &str,
        limit: i32,
    ) -> Result<Vec<String>, StorageError> {
        const MAX_KEYS_PER_PAGE: usize = 1000;

        if limit <= 0 {
            return Ok(Vec::new());
        }

        let limit = limit as usize;
        let mut keys: Vec<String> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let remaining = limit.saturating_sub(keys.len());
            if remaining == 0 {
                break;
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let page_limit = remaining.min(MAX_KEYS_PER_PAGE) as i32;
            let (mut page_keys, next_token) = self
                .list_objects(prefix, page_limit, continuation_token.as_deref())
                .await?;

            keys.append(&mut page_keys);

            if keys.len() >= limit {
                keys.truncate(limit);
                break;
            }

            match next_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(keys)
    }

    /// Lists all versions of an object.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    ///
    /// # Returns
    ///
    /// A vector of object versions, ordered by last modified time (newest first).
    pub async fn list_object_versions(
        &self,
        key: &str,
    ) -> Result<Vec<ObjectVersion>, StorageError> {
        let mut versions: Vec<ObjectVersion> = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_object_versions()
                .bucket(&self.config.bucket)
                .prefix(key);

            if let Some(ref marker) = key_marker {
                request = request.key_marker(marker);
            }
            if let Some(ref marker) = version_id_marker {
                request = request.version_id_marker(marker);
            }

            let response = request
                .send()
                .await
                .map_err(|ref err| map_list_versions_error(err))?;

            versions.extend(
                response
                    .versions()
                    .iter()
                    .filter(|v| v.key().is_some_and(|k| k == key))
                    .filter_map(|v| {
                        let version_id = v.version_id()?.to_string();
                        let last_modified = v.last_modified()?;
                        let is_latest = v.is_latest().unwrap_or(false);
                        let etag = v.e_tag().map(ToString::to_string).unwrap_or_default();

                        // Convert AWS DateTime to chrono DateTime<Utc>
                        let last_modified = DateTime::from_timestamp(
                            last_modified.secs(),
                            last_modified.subsec_nanos(),
                        )?;

                        Some(ObjectVersion {
                            version_id,
                            last_modified,
                            is_latest,
                            etag,
                        })
                    }),
            );

            let truncated = response.is_truncated().unwrap_or(false);
            if !truncated {
                break;
            }

            let next_key = response.next_key_marker().map(ToString::to_string);
            let next_version = response.next_version_id_marker().map(ToString::to_string);
            match (next_key, next_version) {
                (Some(next_key), Some(next_version)) => {
                    key_marker = Some(next_key);
                    version_id_marker = Some(next_version);
                }
                _ => {
                    return Err(StorageError::S3Error(
                        "ListObjectVersions truncated without continuation markers".to_string(),
                    ));
                }
            }
        }

        // Sort by last_modified descending (newest first)
        versions.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));

        Ok(versions)
    }

    /// Retrieves a specific version of an object from S3.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    /// * `version_id` - The version ID to retrieve
    ///
    /// # Returns
    ///
    /// The object body as bytes.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object or version doesn't exist.
    pub async fn get_object_version(
        &self,
        key: &str,
        version_id: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await
            .map_err(|ref err| map_get_error(err, key))?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|err| StorageError::S3Error(format!("Failed to read body: {err}")))?
            .into_bytes()
            .to_vec();

        Ok(body)
    }

    /// Checks if an object exists in S3.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key (path) in the bucket
    ///
    /// # Returns
    ///
    /// `true` if the object exists, `false` otherwise.
    pub async fn head_object(&self, key: &str) -> Result<bool, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(ref err) => {
                if is_not_found_head_error(err) {
                    Ok(false)
                } else {
                    Err(map_head_error(err, key))
                }
            }
        }
    }

    /// Ensures bucket versioning is enabled.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::ConfigurationError` if versioning is not enabled.
    pub async fn ensure_bucket_versioning_enabled(&self) -> Result<(), StorageError> {
        let response = self
            .client
            .get_bucket_versioning()
            .bucket(&self.config.bucket)
            .send()
            .await
            .map_err(|ref err| map_get_bucket_versioning_error(err))?;

        match response.status() {
            Some(BucketVersioningStatus::Enabled) => Ok(()),
            status => Err(StorageError::ConfigurationError(format!(
                "Bucket versioning must be enabled (current: {status:?})"
            ))),
        }
    }
}

/// Maps `PutObject` errors to `StorageError`.
fn map_put_error(err: &SdkError<PutObjectError>, key: &str) -> StorageError {
    match &err {
        SdkError::ServiceError(service_err) => {
            let raw = service_err.raw();
            if raw.status().as_u16() == 412 {
                return StorageError::PreconditionFailed {
                    key: key.to_string(),
                };
            }
        }
        SdkError::DispatchFailure(ref dispatch_err) => {
            if dispatch_err.is_io() || dispatch_err.is_timeout() {
                return StorageError::ConnectionError(err.to_string());
            }
        }
        _ => {}
    }
    StorageError::S3Error(err.to_string())
}

/// Maps `GetObject` errors to `StorageError`.
fn map_get_error(err: &SdkError<GetObjectError>, key: &str) -> StorageError {
    match &err {
        SdkError::ServiceError(service_err) => {
            if matches!(service_err.err(), GetObjectError::NoSuchKey(_)) {
                return StorageError::NotFound {
                    key: key.to_string(),
                };
            }
            // Also check HTTP status code for 404
            let raw = service_err.raw();
            if raw.status().as_u16() == 404 {
                return StorageError::NotFound {
                    key: key.to_string(),
                };
            }
        }
        SdkError::DispatchFailure(ref dispatch_err) => {
            if dispatch_err.is_io() || dispatch_err.is_timeout() {
                return StorageError::ConnectionError(err.to_string());
            }
        }
        _ => {}
    }
    StorageError::S3Error(err.to_string())
}

/// Maps `DeleteObject` errors to `StorageError`.
fn map_delete_error(err: &SdkError<DeleteObjectError>, _key: &str) -> StorageError {
    if let SdkError::DispatchFailure(ref dispatch_err) = &err {
        if dispatch_err.is_io() || dispatch_err.is_timeout() {
            return StorageError::ConnectionError(err.to_string());
        }
    }
    StorageError::S3Error(err.to_string())
}

/// Maps `CopyObject` errors to `StorageError`.
fn map_copy_error(err: &SdkError<CopyObjectError>, source_key: &str) -> StorageError {
    match &err {
        SdkError::ServiceError(service_err) => {
            // Check HTTP status for 404 (source not found)
            let raw = service_err.raw();
            if raw.status().as_u16() == 404 {
                return StorageError::NotFound {
                    key: source_key.to_string(),
                };
            }
        }
        SdkError::DispatchFailure(ref dispatch_err) => {
            if dispatch_err.is_io() || dispatch_err.is_timeout() {
                return StorageError::ConnectionError(err.to_string());
            }
        }
        _ => {}
    }
    StorageError::S3Error(err.to_string())
}

/// Maps `ListObjectsV2` errors to `StorageError`.
fn map_list_error(err: &SdkError<ListObjectsV2Error>) -> StorageError {
    // Check for connection errors
    if let SdkError::DispatchFailure(ref dispatch_err) = err {
        if dispatch_err.is_io() || dispatch_err.is_timeout() {
            return StorageError::ConnectionError(err.to_string());
        }
    }

    StorageError::S3Error(err.to_string())
}

/// Maps `GetBucketVersioning` errors to `StorageError`.
fn map_get_bucket_versioning_error(err: &SdkError<GetBucketVersioningError>) -> StorageError {
    if let SdkError::DispatchFailure(ref dispatch_err) = err {
        if dispatch_err.is_io() || dispatch_err.is_timeout() {
            return StorageError::ConnectionError(err.to_string());
        }
    }

    StorageError::S3Error(err.to_string())
}

/// Maps `ListObjectVersions` errors to `StorageError`.
fn map_list_versions_error(err: &SdkError<ListObjectVersionsError>) -> StorageError {
    // Check for connection errors
    if let SdkError::DispatchFailure(ref dispatch_err) = err {
        if dispatch_err.is_io() || dispatch_err.is_timeout() {
            return StorageError::ConnectionError(err.to_string());
        }
    }

    StorageError::S3Error(err.to_string())
}

/// Checks if a `HeadObject` error is a "not found" error.
fn is_not_found_head_error(err: &SdkError<HeadObjectError>) -> bool {
    if let SdkError::ServiceError(service_err) = err {
        if matches!(service_err.err(), HeadObjectError::NotFound(_)) {
            return true;
        }
        // Also check HTTP status code for 404
        let raw = service_err.raw();
        if raw.status().as_u16() == 404 {
            return true;
        }
    }
    false
}

/// Maps `HeadObject` errors to `StorageError`.
fn map_head_error(err: &SdkError<HeadObjectError>, key: &str) -> StorageError {
    if is_not_found_head_error(err) {
        return StorageError::NotFound {
            key: key.to_string(),
        };
    }

    // Check for connection errors
    if let SdkError::DispatchFailure(ref dispatch_err) = err {
        if dispatch_err.is_io() || dispatch_err.is_timeout() {
            return StorageError::ConnectionError(err.to_string());
        }
    }

    StorageError::S3Error(err.to_string())
}
