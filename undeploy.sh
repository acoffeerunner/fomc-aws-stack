#!/bin/bash

# FOMC Debriefs AWS Cleanup Script
set -e

# Configuration
STACK_NAME="fomc-gists-stack"
REGION="us-east-1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🗑️  Starting FOMC Debriefs AWS Cleanup${NC}"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    echo -e "${RED}❌ AWS CLI not configured. Please run 'aws configure' first.${NC}"
    exit 1
fi

# Check if stack exists
if ! aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION &>/dev/null; then
    echo -e "${RED}❌ Stack $STACK_NAME not found in region $REGION${NC}"
    exit 1
fi

# Get S3 bucket name before deletion
echo -e "${YELLOW}📋 Getting stack resources...${NC}"
S3_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`S3BucketName`].OutputValue' \
    --output text \
    --region $REGION 2>/dev/null)

# Get AWS Account ID for temp bucket cleanup
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Empty main S3 bucket if it exists
if [ ! -z "$S3_BUCKET" ] && [ "$S3_BUCKET" != "None" ]; then
    echo -e "${YELLOW}🗑️  Emptying main S3 bucket: $S3_BUCKET${NC}"
    aws s3 rm s3://$S3_BUCKET --recursive --region $REGION 2>/dev/null || true
else
    echo -e "${YELLOW}⚠️  Could not find main S3 bucket or already deleted${NC}"
fi

# Find and empty any temporary deployment buckets
echo -e "${YELLOW}🔍 Looking for temporary deployment buckets...${NC}"
TEMP_BUCKETS=$(aws s3api list-buckets --query "Buckets[?contains(Name, 'temp-deploy') && contains(Name, '$ACCOUNT_ID')].Name" --output text --region $REGION 2>/dev/null || true)

if [ ! -z "$TEMP_BUCKETS" ]; then
    for temp_bucket in $TEMP_BUCKETS; do
        echo -e "${YELLOW}🗑️  Emptying temporary bucket: $temp_bucket${NC}"
        aws s3 rm s3://$temp_bucket --recursive --region $REGION 2>/dev/null || true
        echo -e "${YELLOW}🗑️  Deleting temporary bucket: $temp_bucket${NC}"
        aws s3 rb s3://$temp_bucket --region $REGION 2>/dev/null || true
    done
else
    echo -e "${YELLOW}ℹ️  No temporary deployment buckets found${NC}"
fi

# Delete CloudFormation stack
echo -e "${YELLOW}☁️  Deleting CloudFormation stack: $STACK_NAME${NC}"
aws cloudformation delete-stack \
    --stack-name $STACK_NAME \
    --region $REGION

echo -e "${YELLOW}⏳ Waiting for stack deletion to complete...${NC}"
aws cloudformation wait stack-delete-complete \
    --stack-name $STACK_NAME \
    --region $REGION

echo -e "${GREEN}✅ Cleanup completed successfully!${NC}"
echo
echo -e "${YELLOW}📋 Cleanup Summary:${NC}"
echo "  Deleted Stack: $STACK_NAME"
echo "  Deleted S3 Bucket: $S3_BUCKET"
echo "  Deleted DynamoDB Table: fomc-gists-dynamodb"
echo "  Deleted Lambda Functions: All FOMC functions"
echo "  Deleted API Gateway: fomc-gists-api"
echo "  Deleted Glue Job: fomc-transform-job"
echo "  Region: $REGION"
echo
echo -e "${GREEN}🎉 All AWS resources have been cleaned up!${NC}"
