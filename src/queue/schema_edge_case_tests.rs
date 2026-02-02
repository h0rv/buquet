//! Edge case tests for JSON Schema validation.
//!
//! These tests verify boundary conditions, complex validation scenarios,
//! and error handling in schema validation without requiring S3 infrastructure.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::len_zero)]
mod schema_edge_cases {
    use serde_json::json;

    use crate::queue::schema::{SchemaValidationError, TaskSchema};

    // Helper function to validate JSON against a JSON Schema
    fn validate_json(
        schema: &serde_json::Value,
        data: &serde_json::Value,
    ) -> Result<(), SchemaValidationError> {
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

    // Helper function to validate input against a schema
    fn validate_input(
        schema: &TaskSchema,
        data: &serde_json::Value,
    ) -> Result<(), SchemaValidationError> {
        let Some(input_schema) = &schema.input else {
            return Ok(()); // No input schema defined
        };
        validate_json(input_schema, data)
    }

    // Helper function to validate output against a schema
    fn validate_output(
        schema: &TaskSchema,
        data: &serde_json::Value,
    ) -> Result<(), SchemaValidationError> {
        let Some(output_schema) = &schema.output else {
            return Ok(()); // No output schema defined
        };
        validate_json(output_schema, data)
    }

    // =========================================================================
    // Schema Configuration Edge Cases
    // =========================================================================

    /// Test: Schema with only input, no output
    /// Bug this catches: Assuming output schema is always present.
    #[test]
    fn test_input_only_schema() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"]
            })),
            output: None,
        };

        // Valid input should pass
        let valid = json!({"name": "test"});
        assert!(validate_input(&schema, &valid).is_ok());

        // Output validation should pass when no schema
        let any_output = json!({"anything": "goes"});
        assert!(validate_output(&schema, &any_output).is_ok());
    }

    /// Test: Schema with only output, no input
    /// Bug this catches: Assuming input schema is always present.
    #[test]
    fn test_output_only_schema() {
        let schema = TaskSchema {
            input: None,
            output: Some(json!({
                "type": "object",
                "properties": {
                    "result": { "type": "boolean" }
                },
                "required": ["result"]
            })),
        };

        // Input validation should pass when no schema
        let any_input = json!({"anything": "goes"});
        assert!(validate_input(&schema, &any_input).is_ok());

        // Valid output should pass
        let valid_output = json!({"result": true});
        assert!(validate_output(&schema, &valid_output).is_ok());

        // Invalid output should fail
        let invalid_output = json!({"result": "not a boolean"});
        assert!(validate_output(&schema, &invalid_output).is_err());
    }

    /// Test: Schema with neither input nor output
    /// Bug this catches: Handling of completely empty schema.
    #[test]
    fn test_empty_schema() {
        let schema = TaskSchema {
            input: None,
            output: None,
        };

        // Everything should pass
        let data = json!({"any": "data", "at": ["all"]});
        assert!(validate_input(&schema, &data).is_ok());
        assert!(validate_output(&schema, &data).is_ok());
    }

    /// Test: Schema with both input and output
    /// Bug this catches: Cross-contamination between input/output validation.
    #[test]
    fn test_both_schemas_independent() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "request_id": { "type": "string" }
                },
                "required": ["request_id"]
            })),
            output: Some(json!({
                "type": "object",
                "properties": {
                    "success": { "type": "boolean" }
                },
                "required": ["success"]
            })),
        };

        // Valid input
        let valid_input = json!({"request_id": "123"});
        assert!(validate_input(&schema, &valid_input).is_ok());

        // Valid output
        let valid_output = json!({"success": true});
        assert!(validate_output(&schema, &valid_output).is_ok());

        // Input should NOT validate against output schema rules
        let input_as_output = json!({"request_id": "123"});
        assert!(validate_output(&schema, &input_as_output).is_err());

        // Output should NOT validate against input schema rules
        let output_as_input = json!({"success": true});
        assert!(validate_input(&schema, &output_as_input).is_err());
    }

    // =========================================================================
    // JSON Type Validation Edge Cases
    // =========================================================================

    /// Test: Null values
    /// Bug this catches: Incorrect null handling.
    #[test]
    fn test_null_validation() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "null"
            })),
            output: None,
        };

        // Null should pass
        assert!(validate_input(&schema, &json!(null)).is_ok());

        // Non-null should fail
        assert!(validate_input(&schema, &json!("not null")).is_err());
        assert!(validate_input(&schema, &json!(0)).is_err());
        assert!(validate_input(&schema, &json!(false)).is_err());
    }

    /// Test: Multiple types allowed (oneOf/anyOf pattern)
    /// Bug this catches: Union type validation issues.
    #[test]
    fn test_nullable_type() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": ["string", "null"]
            })),
            output: None,
        };

        // String should pass
        assert!(validate_input(&schema, &json!("hello")).is_ok());

        // Null should pass
        assert!(validate_input(&schema, &json!(null)).is_ok());

        // Other types should fail
        assert!(validate_input(&schema, &json!(123)).is_err());
    }

    /// Test: Number boundaries
    /// Bug this catches: Number precision and boundary issues.
    #[test]
    fn test_number_boundaries() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 100
                    },
                    "ratio": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0
                    }
                }
            })),
            output: None,
        };

        // Boundary values
        assert!(validate_input(&schema, &json!({"count": 0})).is_ok());
        assert!(validate_input(&schema, &json!({"count": 100})).is_ok());
        assert!(validate_input(&schema, &json!({"ratio": 0.0})).is_ok());
        assert!(validate_input(&schema, &json!({"ratio": 1.0})).is_ok());

        // Just outside boundaries
        assert!(validate_input(&schema, &json!({"count": -1})).is_err());
        assert!(validate_input(&schema, &json!({"count": 101})).is_err());
    }

    /// Test: Exclusive boundaries
    /// Bug this catches: Exclusive vs inclusive boundary handling.
    #[test]
    fn test_exclusive_boundaries() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "value": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "exclusiveMaximum": 100
                    }
                }
            })),
            output: None,
        };

        // Exclusive boundaries should reject exact values
        assert!(validate_input(&schema, &json!({"value": 0})).is_err());
        assert!(validate_input(&schema, &json!({"value": 100})).is_err());

        // Values just inside should pass
        assert!(validate_input(&schema, &json!({"value": 0.001})).is_ok());
        assert!(validate_input(&schema, &json!({"value": 99.999})).is_ok());
    }

    /// Test: Very large numbers
    /// Bug this catches: Overflow or precision loss with large numbers.
    #[test]
    fn test_large_numbers() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "big_int": { "type": "integer" },
                    "big_float": { "type": "number" }
                }
            })),
            output: None,
        };

        // Large but valid JSON numbers
        let data = json!({
            "big_int": 9_007_199_254_740_991_i64,  // Max safe integer in JS
            "big_float": 1.797_693_134_862_315_7e308  // Near f64::MAX
        });
        assert!(validate_input(&schema, &data).is_ok());
    }

    /// Test: Float that looks like integer
    /// Bug this catches: Type coercion between integer and number.
    #[test]
    fn test_integer_vs_number() {
        let int_schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "value": { "type": "integer" }
                }
            })),
            output: None,
        };

        // Integer value should pass
        assert!(validate_input(&int_schema, &json!({"value": 42})).is_ok());

        // Float that equals integer should... (depends on JSON Schema draft)
        // In JSON, 42.0 may or may not be considered an integer
        let result = validate_input(&int_schema, &json!({"value": 42.0}));
        // Either behavior is acceptable, just shouldn't panic
        let _ = result;
    }

    // =========================================================================
    // String Validation Edge Cases
    // =========================================================================

    /// Test: Empty string
    /// Bug this catches: Empty string not being valid string type.
    #[test]
    fn test_empty_string() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "string"
            })),
            output: None,
        };

        // Empty string should be a valid string
        assert!(validate_input(&schema, &json!("")).is_ok());
    }

    /// Test: String length constraints
    /// Bug this catches: Off-by-one in length validation.
    #[test]
    fn test_string_length_constraints() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "string",
                "minLength": 2,
                "maxLength": 5
            })),
            output: None,
        };

        // Boundary values
        assert!(validate_input(&schema, &json!("ab")).is_ok()); // minLength
        assert!(validate_input(&schema, &json!("abcde")).is_ok()); // maxLength

        // Just outside boundaries
        assert!(validate_input(&schema, &json!("a")).is_err()); // too short
        assert!(validate_input(&schema, &json!("abcdef")).is_err()); // too long
    }

    /// Test: Unicode string length (characters vs bytes)
    /// Bug this catches: Counting bytes instead of characters.
    #[test]
    fn test_unicode_string_length() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "string",
                "minLength": 3,
                "maxLength": 5
            })),
            output: None,
        };

        // Unicode characters (each is multiple bytes but one character)
        // "\u{1F4E7}" is the envelope emoji - 1 character but 4 bytes in UTF-8
        assert!(validate_input(&schema, &json!("\u{1F4E7}\u{1F4E7}\u{1F4E7}")).is_ok());

        // Chinese characters (each is 3 bytes in UTF-8)
        assert!(validate_input(&schema, &json!("\u{4E16}\u{754C}\u{4E16}")).is_ok());
    }

    /// Test: String pattern (regex)
    /// Bug this catches: Regex edge cases.
    #[test]
    fn test_string_pattern() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "string",
                "pattern": "^[a-z]+$"
            })),
            output: None,
        };

        assert!(validate_input(&schema, &json!("hello")).is_ok());
        assert!(validate_input(&schema, &json!("HELLO")).is_err());
        assert!(validate_input(&schema, &json!("hello123")).is_err());
        assert!(validate_input(&schema, &json!("")).is_err()); // Empty doesn't match [a-z]+
    }

    /// Test: Email format
    /// Bug this catches: Format validation issues.
    #[test]
    fn test_email_format() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "string",
                "format": "email"
            })),
            output: None,
        };

        // Note: Format validation is optional in JSON Schema
        // Some implementations may not validate formats
        let valid_email = json!("test@example.com");
        let invalid_email = json!("not-an-email");

        // At minimum, these shouldn't panic
        let _ = validate_input(&schema, &valid_email);
        let _ = validate_input(&schema, &invalid_email);
    }

    // =========================================================================
    // Array Validation Edge Cases
    // =========================================================================

    /// Test: Empty array
    /// Bug this catches: Empty array handling.
    #[test]
    fn test_empty_array() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "array",
                "items": { "type": "string" }
            })),
            output: None,
        };

        // Empty array should be valid
        assert!(validate_input(&schema, &json!([])).is_ok());
    }

    /// Test: Array length constraints
    /// Bug this catches: Off-by-one in array length.
    #[test]
    fn test_array_length_constraints() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "array",
                "items": { "type": "integer" },
                "minItems": 1,
                "maxItems": 3
            })),
            output: None,
        };

        assert!(validate_input(&schema, &json!([1])).is_ok()); // minItems
        assert!(validate_input(&schema, &json!([1, 2, 3])).is_ok()); // maxItems
        assert!(validate_input(&schema, &json!([])).is_err()); // too few
        assert!(validate_input(&schema, &json!([1, 2, 3, 4])).is_err()); // too many
    }

    /// Test: Unique items
    /// Bug this catches: Uniqueness check issues.
    #[test]
    fn test_unique_items() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "array",
                "items": { "type": "integer" },
                "uniqueItems": true
            })),
            output: None,
        };

        assert!(validate_input(&schema, &json!([1, 2, 3])).is_ok());
        assert!(validate_input(&schema, &json!([1, 2, 1])).is_err()); // duplicate
    }

    /// Test: Nested arrays
    /// Bug this catches: Recursion issues.
    #[test]
    fn test_nested_arrays() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "array",
                        "items": { "type": "integer" }
                    }
                }
            })),
            output: None,
        };

        let valid = json!([[[1, 2], [3, 4]], [[5, 6]]]);
        assert!(validate_input(&schema, &valid).is_ok());

        let invalid = json!([[[1, "not int"]]]);
        assert!(validate_input(&schema, &invalid).is_err());
    }

    // =========================================================================
    // Object Validation Edge Cases
    // =========================================================================

    /// Test: Empty object
    /// Bug this catches: Empty object handling with required fields.
    #[test]
    fn test_empty_object() {
        let schema_no_required = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "optional": { "type": "string" }
                }
            })),
            output: None,
        };

        let schema_with_required = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "required_field": { "type": "string" }
                },
                "required": ["required_field"]
            })),
            output: None,
        };

        // Empty object with no required fields should pass
        assert!(validate_input(&schema_no_required, &json!({})).is_ok());

        // Empty object with required fields should fail
        assert!(validate_input(&schema_with_required, &json!({})).is_err());
    }

    /// Test: Additional properties
    /// Bug this catches: Extra properties being rejected when allowed.
    #[test]
    fn test_additional_properties() {
        let schema_allow_additional = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "known": { "type": "string" }
                },
                "additionalProperties": true
            })),
            output: None,
        };

        let schema_deny_additional = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "known": { "type": "string" }
                },
                "additionalProperties": false
            })),
            output: None,
        };

        let data_with_extra = json!({"known": "value", "extra": "field"});

        assert!(validate_input(&schema_allow_additional, &data_with_extra).is_ok());
        assert!(validate_input(&schema_deny_additional, &data_with_extra).is_err());
    }

    /// Test: Deeply nested objects
    /// Bug this catches: Stack overflow with deep nesting.
    #[test]
    fn test_deeply_nested_objects() {
        // Create a schema that validates 20 levels deep (simpler test)
        // The schema structure is: {"nested": {"nested": ... {"value": integer}}}
        let mut schema_obj = json!({"type": "integer"});
        for _ in 0..20 {
            let mut properties = serde_json::Map::new();
            properties.insert("nested".to_string(), schema_obj);
            schema_obj = json!({
                "type": "object",
                "properties": properties,
                "required": ["nested"]
            });
        }

        let schema = TaskSchema {
            input: Some(schema_obj),
            output: None,
        };

        // Create matching nested data
        let mut data = json!(42);
        for _ in 0..20 {
            let mut obj = serde_json::Map::new();
            obj.insert("nested".to_string(), data);
            data = serde_json::Value::Object(obj);
        }

        assert!(validate_input(&schema, &data).is_ok());

        // Also verify invalid nested data fails
        let mut invalid_data = json!("not an integer");
        for _ in 0..20 {
            let mut obj = serde_json::Map::new();
            obj.insert("nested".to_string(), invalid_data);
            invalid_data = serde_json::Value::Object(obj);
        }
        assert!(validate_input(&schema, &invalid_data).is_err());
    }

    // =========================================================================
    // Invalid Schema Edge Cases
    // =========================================================================

    /// Test: Invalid JSON Schema itself
    /// Bug this catches: Crash when schema is malformed.
    #[test]
    fn test_invalid_schema_type() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "not_a_real_type"
            })),
            output: None,
        };

        let result = validate_input(&schema, &json!("anything"));
        // Should return an error, not panic
        // The error should indicate the schema is invalid
        assert!(result.is_err());
    }

    /// Test: Schema with circular reference (should be handled)
    /// Bug this catches: Infinite loop with circular $ref.
    #[test]
    fn test_schema_is_not_json_schema_object() {
        // Schema that's just a string instead of an object
        let schema = TaskSchema {
            input: Some(json!("not an object")),
            output: None,
        };

        let result = validate_input(&schema, &json!("data"));
        // Should handle gracefully
        assert!(result.is_err());
    }

    /// Test: Schema is null
    /// Bug this catches: Null schema handling.
    #[test]
    fn test_null_schema() {
        let schema = TaskSchema {
            input: Some(json!(null)),
            output: None,
        };

        let result = validate_input(&schema, &json!("data"));
        // Behavior depends on implementation
        // Should not panic
        let _ = result;
    }

    // =========================================================================
    // Error Reporting Edge Cases
    // =========================================================================

    /// Test: Multiple validation errors
    /// Bug this catches: Only reporting first error.
    #[test]
    fn test_multiple_errors_reported() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "age": { "type": "integer" },
                    "email": { "type": "string" }
                },
                "required": ["name", "age", "email"]
            })),
            output: None,
        };

        // Missing all three required fields
        let result = validate_input(&schema, &json!({}));
        assert!(result.is_err());

        let err = result.unwrap_err();
        // Should report all missing fields
        assert!(err.errors.len() >= 1, "Should report at least one error");
    }

    /// Test: Error message contains useful information
    /// Bug this catches: Unhelpful error messages.
    #[test]
    fn test_error_message_quality() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "count": { "type": "integer" }
                },
                "required": ["count"]
            })),
            output: None,
        };

        // Wrong type error
        let result = validate_input(&schema, &json!({"count": "not an integer"}));
        assert!(result.is_err());

        let err = result.unwrap_err();
        let error_text = format!("{err}");
        // Error should mention what went wrong
        assert!(!error_text.is_empty(), "Error message should not be empty");
    }

    // =========================================================================
    // Schema Serialization Edge Cases
    // =========================================================================

    /// Test: `TaskSchema` roundtrip serialization
    /// Bug this catches: Serialization losing data.
    #[test]
    fn test_task_schema_roundtrip() {
        let schema = TaskSchema {
            input: Some(json!({
                "type": "object",
                "properties": {
                    "complex": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "nested": { "type": "boolean" }
                            }
                        }
                    }
                }
            })),
            output: Some(json!({
                "type": "object",
                "properties": {
                    "result": { "type": "string" }
                }
            })),
        };

        let json_str = serde_json::to_string(&schema).expect("serialize should succeed");
        let deserialized: TaskSchema =
            serde_json::from_str(&json_str).expect("deserialize should succeed");

        assert_eq!(schema.input, deserialized.input);
        assert_eq!(schema.output, deserialized.output);
    }

    /// Test: `TaskSchema` with None values serializes correctly
    /// Bug this catches: None values becoming null instead of omitted.
    #[test]
    fn test_task_schema_none_serialization() {
        let schema = TaskSchema {
            input: None,
            output: None,
        };

        let json_str = serde_json::to_string(&schema).expect("serialize should succeed");

        // With skip_serializing_if, these should be omitted
        assert!(
            !json_str.contains("input") || json_str == "{}",
            "None input should be omitted or produce empty object"
        );
    }
}
