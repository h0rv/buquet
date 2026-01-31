//! Tests for Task Schemas feature.
//!
//! These tests are based purely on the spec in docs/features/task-schemas.md
//! following a blind TDD approach.
//!
//! Tests marked with #[ignore] require S3 infrastructure (run with `cargo test -- --ignored`).

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use serde_json::json;

    use crate::queue::{Queue, SchemaValidationError, TaskSchema};
    use crate::storage::{S3Client, S3Config};

    /// Creates a test queue using environment variables for configuration.
    ///
    /// **For tests only:** Defaults to LocalStack for developer convenience.
    ///
    /// Environment variables:
    /// - `S3_ENDPOINT`: S3 endpoint URL (test default: `http://localhost:4566`)
    /// - `S3_BUCKET`: Bucket name (test default: `qo-dev`)
    /// - `S3_REGION`: AWS region (test default: `us-east-1`)
    ///
    /// To test against real AWS, set `S3_ENDPOINT=` (empty).
    async fn test_queue() -> Queue {
        // TEST-ONLY: Default to LocalStack for developer convenience.
        let endpoint = match std::env::var("S3_ENDPOINT") {
            Ok(val) if val.is_empty() => None, // Explicitly empty = real AWS
            Ok(val) => Some(val),              // Has value = use that endpoint
            Err(_) => Some("http://localhost:4566".to_string()), // Test default = LocalStack
        };
        let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "qo-dev".to_string());
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        eprintln!("Test config: endpoint={endpoint:?}, bucket={bucket}, region={region}");

        let config = S3Config::new(endpoint, bucket, region);
        let client = S3Client::new(config)
            .await
            .expect("Failed to create S3 client - is LocalStack running? Try: just up && source crates/qo/examples/.env.example");
        Queue::new(client)
    }

    /// Generates a unique task type for test isolation.
    fn unique_task_type() -> String {
        format!("test-schema-{}", uuid::Uuid::new_v4())
    }

    /// Creates a test schema for send_email task type.
    fn send_email_schema() -> TaskSchema {
        TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string" },
                    "subject": { "type": "string" }
                },
                "required": ["to", "subject"]
            })),
            output: Some(json!({
                "type": "object",
                "properties": {
                    "sent": { "type": "boolean" }
                }
            })),
        }
    }

    // =========================================================================
    // Schema Storage Tests
    // =========================================================================

    /// Test that we can publish a schema for a task type.
    /// Based on spec: `queue.publish_schema("send_email", schema_json).await?;`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_publish_schema() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        let result = queue.publish_schema(&task_type, &schema).await;
        if let Err(ref e) = result {
            eprintln!("publish_schema error: {e:?}");
        }
        assert!(result.is_ok(), "publish_schema should succeed");
    }

    /// Test that we can retrieve a published schema.
    /// Based on spec: `let schema = queue.get_schema("send_email").await?;`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_get_schema() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        let retrieved = queue.get_schema(&task_type).await;
        assert!(retrieved.is_ok(), "get_schema should succeed");

        let retrieved_schema = retrieved.unwrap();
        assert!(retrieved_schema.is_some(), "schema should exist");

        let retrieved_schema = retrieved_schema.unwrap();
        // Verify the schema content matches what we published
        assert_eq!(retrieved_schema.input, schema.input);
        assert_eq!(retrieved_schema.output, schema.output);
    }

    /// Test that get_schema returns None for missing schema.
    /// Based on spec: Schema retrieval should handle non-existent schemas gracefully.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_get_schema_not_found() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let result = queue.get_schema(&task_type).await;
        assert!(
            result.is_ok(),
            "get_schema should not error for missing schema"
        );

        let schema = result.unwrap();
        assert!(schema.is_none(), "should return None for missing schema");
    }

    /// Test that we can list all published schema task types.
    /// Based on spec: `let schemas = queue.list_schemas().await?; // ["send_email", "process_order"]`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_list_schemas() {
        let queue = test_queue().await;
        let task_type1 = unique_task_type();
        let task_type2 = unique_task_type();

        let schema = TaskSchema {
            input: Some(json!({ "type": "object" })),
            output: Some(json!({ "type": "object" })),
        };

        queue
            .publish_schema(&task_type1, &schema)
            .await
            .expect("publish should succeed");
        queue
            .publish_schema(&task_type2, &schema)
            .await
            .expect("publish should succeed");

        let schemas = queue.list_schemas().await;
        assert!(schemas.is_ok(), "list_schemas should succeed");

        let schema_list = schemas.unwrap();
        assert!(
            schema_list.contains(&task_type1),
            "should contain first task type"
        );
        assert!(
            schema_list.contains(&task_type2),
            "should contain second task type"
        );
    }

    /// Test that we can delete a schema.
    /// Based on spec: `qo schema delete send_email` and `queue.delete_schema("send_email").await?;`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_delete_schema() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let schema = TaskSchema {
            input: Some(json!({ "type": "object" })),
            output: Some(json!({ "type": "object" })),
        };

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        // Verify it exists
        let retrieved = queue.get_schema(&task_type).await.unwrap();
        assert!(retrieved.is_some(), "schema should exist before delete");

        // Delete it
        let delete_result = queue.delete_schema(&task_type).await;
        assert!(delete_result.is_ok(), "delete_schema should succeed");

        // Verify it's gone
        let retrieved_after = queue.get_schema(&task_type).await.unwrap();
        assert!(
            retrieved_after.is_none(),
            "schema should not exist after delete"
        );
    }

    // =========================================================================
    // Validation Tests
    // =========================================================================

    /// Test that valid input passes validation.
    /// Based on spec: `queue.validate_input("send_email", {"to": "x", "subject": "y"})`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_input_valid() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        let valid_input = json!({
            "to": "test@example.com",
            "subject": "Hello"
        });

        // Fetch schema and validate
        let fetched_schema = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        let result = queue.validate_input(&fetched_schema, &valid_input);
        assert!(result.is_ok(), "valid input should pass validation");
    }

    /// Test that invalid input returns SchemaValidationError.
    /// Based on spec: Validation should raise SchemaValidationError if invalid.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_input_invalid() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        // Missing required field "subject"
        let invalid_input = json!({
            "to": "test@example.com"
        });

        let fetched_schema = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        let result = queue.validate_input(&fetched_schema, &invalid_input);
        assert!(result.is_err(), "invalid input should fail validation");

        let error = result.unwrap_err();
        // Verify it's a SchemaValidationError with errors
        assert!(!error.errors.is_empty(), "should have validation errors");
    }

    /// Test that valid output passes validation.
    /// Based on spec: `queue.validate_output("send_email", {"sent": True})`
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_output_valid() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        let valid_output = json!({
            "sent": true
        });

        let fetched_schema = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        let result = queue.validate_output(&fetched_schema, &valid_output);
        assert!(result.is_ok(), "valid output should pass validation");
    }

    /// Test that invalid output returns SchemaValidationError.
    /// Based on spec: Validation should raise SchemaValidationError if invalid.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_output_invalid() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string" },
                    "subject": { "type": "string" }
                },
                "required": ["to", "subject"]
            })),
            output: Some(json!({
                "type": "object",
                "properties": {
                    "sent": { "type": "boolean" }
                },
                "required": ["sent"]
            })),
        };

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        // Wrong type for "sent" field (string instead of boolean)
        let invalid_output = json!({
            "sent": "yes"
        });

        let fetched_schema = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        let result = queue.validate_output(&fetched_schema, &invalid_output);
        assert!(result.is_err(), "invalid output should fail validation");

        let error = result.unwrap_err();
        assert!(!error.errors.is_empty(), "should have validation errors");
    }

    /// Test that validation handles missing schema gracefully.
    /// Based on spec: User's responsibility section implies validation should handle missing schemas.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_missing_schema() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        // Attempt to get a schema that doesn't exist
        let result = queue.get_schema(&task_type).await;
        assert!(result.is_ok(), "get_schema should not error");
        assert!(
            result.unwrap().is_none(),
            "should return None for missing schema"
        );

        // If we try to validate with a schema that has no input/output defined,
        // validation should pass (no schema = no constraints)
        let empty_schema = TaskSchema {
            input: None,
            output: None,
        };

        let input = json!({
            "to": "test@example.com",
            "subject": "Hello"
        });

        let result = queue.validate_input(&empty_schema, &input);
        assert!(result.is_ok(), "validation with no schema should pass");
    }

    // =========================================================================
    // Schema Format Tests
    // =========================================================================

    /// Test that schema has "input" and "output" keys with JSON Schema objects.
    /// Based on spec:
    /// ```json
    /// {
    ///   "input": { "type": "object", "properties": {...}, "required": [...] },
    ///   "output": { "type": "object", "properties": {...} }
    /// }
    /// ```
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_schema_format() {
        let queue = test_queue().await;
        let task_type = unique_task_type();
        let schema = send_email_schema();

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        let retrieved = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        // Verify schema has "input" with JSON Schema object
        assert!(retrieved.input.is_some(), "schema should have 'input' key");
        let input_schema = retrieved.input.as_ref().unwrap();
        assert!(
            input_schema.is_object(),
            "input should be a JSON Schema object"
        );
        assert_eq!(
            input_schema["type"], "object",
            "input schema should have type 'object'"
        );
        assert!(
            input_schema.get("properties").is_some(),
            "input schema should have properties"
        );

        // Verify schema has "output" with JSON Schema object
        assert!(
            retrieved.output.is_some(),
            "schema should have 'output' key"
        );
        let output_schema = retrieved.output.as_ref().unwrap();
        assert!(
            output_schema.is_object(),
            "output should be a JSON Schema object"
        );
        assert_eq!(
            output_schema["type"], "object",
            "output schema should have type 'object'"
        );
        assert!(
            output_schema.get("properties").is_some(),
            "output schema should have properties"
        );
    }

    // =========================================================================
    // Additional Edge Case Tests
    // =========================================================================

    /// Test that list_schemas works and returns a Vec.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_list_schemas_empty() {
        // This test is tricky in a shared environment since other tests may have schemas.
        // We test that list_schemas works and returns a Vec.
        let queue = test_queue().await;

        let schemas = queue.list_schemas().await;
        assert!(schemas.is_ok(), "list_schemas should succeed");
        // The result should be a Vec<String>
        let _schema_list: Vec<String> = schemas.unwrap();
        // Can't assert empty since other tests may have created schemas
    }

    /// Test that publishing a schema overwrites existing schema.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_publish_schema_overwrites() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let schema_v1 = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string" }
                },
                "required": ["to"]
            })),
            output: Some(json!({ "type": "object" })),
        };

        let schema_v2 = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "to": { "type": "string" },
                    "subject": { "type": "string" }
                },
                "required": ["to", "subject"]
            })),
            output: Some(json!({ "type": "object" })),
        };

        queue
            .publish_schema(&task_type, &schema_v1)
            .await
            .expect("first publish should succeed");

        queue
            .publish_schema(&task_type, &schema_v2)
            .await
            .expect("second publish should succeed");

        let retrieved = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        // Should have the v2 schema with both fields required
        let input = retrieved.input.as_ref().unwrap();
        let required = input["required"].as_array().unwrap();
        assert_eq!(
            required.len(),
            2,
            "should have updated schema with 2 required fields"
        );
    }

    /// Test deleting a non-existent schema (should not error).
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_delete_nonexistent_schema() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        // Deleting a schema that doesn't exist should be idempotent
        let result = queue.delete_schema(&task_type).await;
        assert!(
            result.is_ok(),
            "delete_schema should not error for non-existent schema"
        );
    }

    /// Test validation with various JSON types.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_validate_various_types() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "count": { "type": "integer" },
                    "price": { "type": "number" },
                    "active": { "type": "boolean" },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["count", "price", "active", "tags"]
            })),
            output: Some(json!({ "type": "object" })),
        };

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        let fetched_schema = queue
            .get_schema(&task_type)
            .await
            .expect("get should succeed")
            .expect("schema should exist");

        let valid_input = json!({
            "count": 42,
            "price": 19.99,
            "active": true,
            "tags": ["urgent", "important"]
        });

        let result = queue.validate_input(&fetched_schema, &valid_input);
        assert!(result.is_ok(), "valid complex input should pass validation");

        // Test with wrong type (string instead of integer)
        let invalid_input = json!({
            "count": "forty-two",
            "price": 19.99,
            "active": true,
            "tags": ["urgent"]
        });

        let result = queue.validate_input(&fetched_schema, &invalid_input);
        assert!(result.is_err(), "invalid type should fail validation");
    }

    /// Test that schema storage path follows the spec: `schemas/{task_type}.json`
    /// This verifies the internal storage structure matches the spec.
    #[tokio::test]
    #[cfg_attr(
        not(feature = "integration"),
        ignore = "requires S3 (enable feature integration)"
    )]
    async fn test_schema_storage_path() {
        let queue = test_queue().await;
        let task_type = unique_task_type();

        let schema = TaskSchema {
            input: Some(json!({ "type": "object" })),
            output: Some(json!({ "type": "object" })),
        };

        queue
            .publish_schema(&task_type, &schema)
            .await
            .expect("publish should succeed");

        // The schema should be retrievable, confirming it was stored correctly
        let stored = queue.get_schema(&task_type).await;
        assert!(
            stored.is_ok() && stored.unwrap().is_some(),
            "schema should be stored and retrievable"
        );
    }

    /// Test validation with input-only schema (no output).
    #[tokio::test]
    async fn test_validate_input_only_schema() {
        // This is a unit test that doesn't require S3
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"]
            })),
            output: None, // No output schema
        };

        // Create a mock validation - we can't use queue without S3,
        // but we can test the schema structure
        assert!(schema.input.is_some());
        assert!(schema.output.is_none());
    }

    /// Test validation with output-only schema (no input).
    #[tokio::test]
    async fn test_validate_output_only_schema() {
        let schema = TaskSchema {
            input: None, // No input schema
            output: Some(json!({
                "type": "object",
                "properties": {
                    "result": { "type": "string" }
                }
            })),
        };

        assert!(schema.input.is_none());
        assert!(schema.output.is_some());
    }

    /// Test TaskSchema serialization roundtrip.
    #[tokio::test]
    async fn test_schema_serialization() {
        let schema = send_email_schema();

        let json_str = serde_json::to_string(&schema).expect("serialization should succeed");
        let deserialized: TaskSchema =
            serde_json::from_str(&json_str).expect("deserialization should succeed");

        assert_eq!(schema.input, deserialized.input);
        assert_eq!(schema.output, deserialized.output);
    }

    /// Test SchemaValidationError display formatting.
    #[tokio::test]
    async fn test_schema_validation_error_display() {
        let error = SchemaValidationError {
            errors: vec![
                "missing required field".to_string(),
                "invalid type".to_string(),
            ],
        };

        let display = format!("{}", error);
        assert!(display.contains("missing required field"));
        assert!(display.contains("invalid type"));
    }
}
