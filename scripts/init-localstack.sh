#!/bin/bash
# Initialize LocalStack S3 bucket for qo development

set -e

echo "Creating S3 bucket: qo-dev"
awslocal s3 mb s3://qo-dev 2>/dev/null || echo "Bucket already exists"

# Enable versioning for full S3 compatibility
echo "Enabling versioning on qo-dev"
awslocal s3api put-bucket-versioning \
    --bucket qo-dev \
    --versioning-configuration Status=Enabled

echo "LocalStack S3 initialization complete!"
echo "  Endpoint: http://localhost:4566"
echo "  Bucket: qo-dev"
echo "  Versioning: Enabled"
