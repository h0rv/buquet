//! Task schema storage and validation.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::queue::Queue;
use crate::storage::{PutCondition, StorageError};

/// A task schema with optional input and output JSON schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSchema {
    /// JSON Schema for validating task input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// JSON Schema for validating task output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
}

impl Queue {
    /// Publish a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task to publish schema for
    /// * `schema` - The schema containing optional input and output JSON schemas
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the storage operation fails.
    pub async fn publish_schema(
        &self,
        task_type: &str,
        schema: &TaskSchema,
    ) -> Result<(), StorageError> {
        let key = format!("schemas/{task_type}.json");
        let json = serde_json::to_vec(schema)
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        self.client()
            .put_object(&key, json, PutCondition::None)
            .await?;
        Ok(())
    }

    /// Get a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task to get schema for
    ///
    /// # Returns
    ///
    /// `Some(schema)` if a schema exists for this task type, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails or deserialization fails.
    pub async fn get_schema(&self, task_type: &str) -> Result<Option<TaskSchema>, StorageError> {
        let key = format!("schemas/{task_type}.json");
        match self.client().get_object(&key).await {
            Ok((bytes, _etag)) => {
                let schema: TaskSchema = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                Ok(Some(schema))
            }
            Err(StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// List all task types that have schemas.
    ///
    /// # Returns
    ///
    /// A vector of task type names that have schemas defined.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    pub async fn list_schemas(&self) -> Result<Vec<String>, StorageError> {
        let (objects, _next_token) = self.client().list_objects("schemas/", 1000, None).await?;
        let task_types: Vec<String> = objects
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix("schemas/")
                    .and_then(|s| s.strip_suffix(".json"))
                    .map(str::to_string)
            })
            .collect();
        Ok(task_types)
    }

    /// Delete a schema for a task type.
    ///
    /// # Arguments
    ///
    /// * `task_type` - The type of task to delete schema for
    ///
    /// # Errors
    ///
    /// Returns an error if the storage operation fails.
    pub async fn delete_schema(&self, task_type: &str) -> Result<(), StorageError> {
        let key = format!("schemas/{task_type}.json");
        self.client().delete_object(&key).await
    }

    /// Validate input data against a task's input schema.
    ///
    /// Returns `Ok(())` if valid or no schema exists.
    /// Returns `Err` with validation errors if invalid.
    ///
    /// # Arguments
    ///
    /// * `schema` - The task schema containing the input schema to validate against
    /// * `data` - The input data to validate
    ///
    /// # Errors
    ///
    /// Returns a `SchemaValidationError` if the data does not match the schema.
    pub fn validate_input(
        &self,
        schema: &TaskSchema,
        data: &Value,
    ) -> Result<(), SchemaValidationError> {
        let Some(input_schema) = &schema.input else {
            return Ok(()); // No input schema defined
        };
        validate_json(input_schema, data)
    }

    /// Validate output data against a task's output schema.
    ///
    /// Returns `Ok(())` if valid or no schema exists.
    /// Returns `Err` with validation errors if invalid.
    ///
    /// # Arguments
    ///
    /// * `schema` - The task schema containing the output schema to validate against
    /// * `data` - The output data to validate
    ///
    /// # Errors
    ///
    /// Returns a `SchemaValidationError` if the data does not match the schema.
    pub fn validate_output(
        &self,
        schema: &TaskSchema,
        data: &Value,
    ) -> Result<(), SchemaValidationError> {
        let Some(output_schema) = &schema.output else {
            return Ok(());
        };
        validate_json(output_schema, data)
    }
}

/// Error returned when schema validation fails.
#[derive(Debug, Clone)]
pub struct SchemaValidationError {
    /// The list of validation error messages.
    pub errors: Vec<String>,
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Schema validation failed: {}", self.errors.join("; "))
    }
}

impl std::error::Error for SchemaValidationError {}

/// Validate JSON data against a JSON Schema.
fn validate_json(schema: &Value, data: &Value) -> Result<(), SchemaValidationError> {
    let compiled = jsonschema::draft202012::new(schema).map_err(|e| SchemaValidationError {
        errors: vec![format!("Invalid schema: {}", e)],
    })?;

    let result = compiled.validate(data);
    if result.is_ok() {
        Ok(())
    } else {
        let errors: Vec<String> = compiled.iter_errors(data).map(|e| e.to_string()).collect();
        Err(SchemaValidationError { errors })
    }
}
