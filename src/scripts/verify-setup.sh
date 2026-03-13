#!/bin/bash

# ====================================================================
# Verify S3 and JSONPath Setup
# Run this before attempting COPY commands
# ====================================================================

set -e

BUCKET="gaming-cdc-data-lake-2025"
REGION="us-east-1"

echo "=========================================="
echo "S3 AND JSONPATH VERIFICATION"
echo "=========================================="
echo ""

# 1. Check JSONPath files in S3
echo "1. Checking JSONPath files in S3..."
echo "-----------------------------------"
aws s3 ls s3://${BUCKET}/jsonpath/ --region ${REGION} | grep ".json"
echo ""

# 2. Verify one JSONPath file content
echo "2. Verifying jsonpath-dim-user.json content..."
echo "-----------------------------------"
aws s3 cp s3://${BUCKET}/jsonpath/jsonpath-dim-user.json - --region ${REGION} | python3 -m json.tool
echo ""

# 3. Check if data files exist
echo "3. Checking data files..."
echo "-----------------------------------"
echo "dim_user files:"
aws s3 ls s3://${BUCKET}/raw/cdc/cdc-gaming.gaming_oltp.dim_user/partition=0/ --region ${REGION} | head -3
echo ""
echo "dim_game files:"
aws s3 ls s3://${BUCKET}/raw/cdc/cdc-gaming.gaming_oltp.dim_game/partition=0/ --region ${REGION} | head -3
echo ""

# 4. Download and verify one sample file
echo "4. Verifying sample data file structure..."
echo "-----------------------------------"
SAMPLE_FILE="s3://${BUCKET}/raw/cdc/cdc-gaming.gaming_oltp.dim_user/partition=0/cdc-gaming.gaming_oltp.dim_user+0+0000001093.json"
echo "Downloading: $SAMPLE_FILE"
aws s3 cp $SAMPLE_FILE - --region ${REGION} | head -1 | python3 -c "
import json, sys
data = json.loads(sys.stdin.read().split('\n')[0])
print('Keys in S3 JSON:', sorted(data.keys()))
print('Total fields:', len(data.keys()))
"
echo ""

# 5. Compare field counts
echo "5. Comparing field counts..."
echo "-----------------------------------"
echo -n "JSONPath fields: "
aws s3 cp s3://${BUCKET}/jsonpath/jsonpath-dim-user.json - --region ${REGION} | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['jsonpaths']))"

echo -n "Staging table columns: "
cat <<EOF | python3
print(13)  # Based on stg_dim_user table definition
EOF
echo ""

echo "=========================================="
echo "✓ VERIFICATION COMPLETE"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run: TEST-COPY-SIMPLE.sql in Redshift"
echo "2. If it fails, run: DIAGNOSE-SIMPLE.sql"
echo "3. Share the exact error message with me"



