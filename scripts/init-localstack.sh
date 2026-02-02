#!/bin/bash
# Initialize LocalStack S3 bucket for buquet development

set -e

echo "Creating S3 bucket: buquet-dev"
awslocal s3 mb s3://buquet-dev 2>/dev/null || echo "Bucket already exists"

# Enable versioning for full S3 compatibility
echo "Enabling versioning on buquet-dev"
awslocal s3api put-bucket-versioning \
    --bucket buquet-dev \
    --versioning-configuration Status=Enabled

echo "LocalStack S3 initialization complete!"
echo "  Endpoint: http://localhost:4566"
echo "  Bucket: buquet-dev"
echo "  Versioning: Enabled"
