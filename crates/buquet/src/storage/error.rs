use thiserror::Error;

/// Errors that can occur when interacting with S3 storage.
#[derive(Debug, Clone, Error)]
pub enum StorageError {
    /// The requested object was not found (HTTP 404).
    #[error("Object not found: {key}")]
    NotFound {
        /// The key of the object that was not found.
        key: String,
    },

    /// A conditional request failed (HTTP 412).
    /// This typically occurs when using `If-None-Match: *` and the object already exists,
    /// or when using `If-Match` with a mismatched `ETag`.
    #[error("Precondition failed for object: {key}")]
    PreconditionFailed {
        /// The key of the object for which the precondition failed.
        key: String,
    },

    /// Attempted to create an object that already exists.
    #[error("Object already exists: {key}")]
    AlreadyExists {
        /// The key of the object that already exists.
        key: String,
    },

    /// Failed to connect to the S3 endpoint.
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Failed to serialize or deserialize data.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Invalid or unsupported configuration.
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Access denied (HTTP 403).
    #[error("Access denied to bucket '{bucket}'")]
    AccessDenied {
        /// The bucket that access was denied to.
        bucket: String,
    },

    /// Catch-all for other S3 errors.
    #[error("S3 error: {0}")]
    S3Error(String),
}

impl StorageError {
    /// Returns a helpful suggestion for resolving this error.
    ///
    /// These suggestions are designed to be educational and actionable,
    /// helping users diagnose and fix common issues.
    #[must_use]
    pub const fn suggestion(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => {
                "The object may have been deleted or never existed. \
                 Check that the key/path is correct."
            }
            Self::PreconditionFailed { .. } => {
                "Another process modified this object between your read and write. \
                 This is normal in high-concurrency environments - the system is \
                 working correctly to prevent conflicts."
            }
            Self::AlreadyExists { .. } => {
                "An object with this key already exists. If you want to update it, \
                 use a conditional write with the current ETag."
            }
            Self::ConnectionError(_) => {
                "Check that your S3 endpoint is correct and the service is running. \
                 For local development, ensure the S3-compatible service is started \
                 (e.g., docker compose up -d). Verify network connectivity \
                 with: curl <your-endpoint>"
            }
            Self::SerializationError(_) => {
                "The data could not be serialized or deserialized. This usually \
                 indicates corrupted data or a schema mismatch."
            }
            Self::ConfigurationError(_) => {
                "Check your configuration settings. Common issues include: \
                 missing bucket versioning, invalid region, or incorrect bucket name."
            }
            Self::AccessDenied { .. } => {
                "Check that AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set correctly. \
                 Verify the credentials have permission to access this bucket. \
                 Check the bucket policy allows your IAM user/role."
            }
            Self::S3Error(_) => {
                "An unexpected S3 error occurred. Check the error message for details \
                 and verify your S3 configuration is correct."
            }
        }
    }

    /// Returns a richly formatted error message with context and suggestions.
    ///
    /// This format is designed for CLI output to help users understand
    /// what went wrong and how to fix it.
    #[must_use]
    pub fn display_rich(&self) -> String {
        format!("Error: {}\n\nSuggestion:\n  {}", self, self.suggestion())
    }

    /// Returns true if this error type supports rich display.
    ///
    /// All `StorageError` variants support rich display.
    #[must_use]
    pub const fn has_rich_display(&self) -> bool {
        true
    }
}
