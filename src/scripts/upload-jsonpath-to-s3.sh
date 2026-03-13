#!/bin/bash

# ====================================================================
# Upload JSONPath Files to S3
# ====================================================================
# This script uploads the JSONPath mapping files to S3 so Redshift
# can reference them during COPY operations
# ====================================================================

set -e

BUCKET="gaming-cdc-data-lake-2025"
JSONPATH_PREFIX="jsonpath"
REGION="us-east-1"

echo "============================================"
echo "Uploading JSONPath files to S3..."
echo "Bucket: s3://${BUCKET}/${JSONPATH_PREFIX}/"
echo "============================================"

# Upload each JSONPath file
echo ""
echo "Uploading jsonpath-dim-user.json..."
aws s3 cp jsonpath-dim-user.json \
    s3://${BUCKET}/${JSONPATH_PREFIX}/jsonpath-dim-user.json \
    --region ${REGION}

echo "Uploading jsonpath-dim-game.json..."
aws s3 cp jsonpath-dim-game.json \
    s3://${BUCKET}/${JSONPATH_PREFIX}/jsonpath-dim-game.json \
    --region ${REGION}

echo "Uploading jsonpath-dim-session.json..."
aws s3 cp jsonpath-dim-session.json \
    s3://${BUCKET}/${JSONPATH_PREFIX}/jsonpath-dim-session.json \
    --region ${REGION}

echo "Uploading jsonpath-fact-game-event.json..."
aws s3 cp jsonpath-fact-game-event.json \
    s3://${BUCKET}/${JSONPATH_PREFIX}/jsonpath-fact-game-event.json \
    --region ${REGION}

echo ""
echo "============================================"
echo "✅ All JSONPath files uploaded successfully!"
echo "============================================"
echo ""
echo "Verify files:"
aws s3 ls s3://${BUCKET}/${JSONPATH_PREFIX}/ --region ${REGION}

echo ""
echo "You can now run the Redshift COPY commands in redshift-serverless-scd2.sql"



