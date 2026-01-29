#!/bin/bash
# Initialize LocalStack S3 bucket for oq development

set -e

echo "Creating S3 bucket: oq-dev"
awslocal s3 mb s3://oq-dev 2>/dev/null || echo "Bucket already exists"

# Enable versioning for full S3 compatibility
echo "Enabling versioning on oq-dev"
awslocal s3api put-bucket-versioning \
    --bucket oq-dev \
    --versioning-configuration Status=Enabled

echo "LocalStack S3 initialization complete!"
echo "  Endpoint: http://localhost:4566"
echo "  Bucket: oq-dev"
echo "  Versioning: Enabled"
