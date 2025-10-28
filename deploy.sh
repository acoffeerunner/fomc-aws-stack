#!/bin/bash

# FOMC Debriefs AWS Deployment Script
set -e

# Configuration
STACK_NAME="fomc-gists-stack"
REGION="us-east-1"
BUCKET_PREFIX="fomc-gists-s3"
DYNAMO_DB_NAME="fomc-gists-dynamodb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting FOMC Debriefs AWS Deployment${NC}"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    echo -e "${RED}❌ AWS CLI not configured. Please run 'aws configure' first.${NC}"
    exit 1
fi

# Get AWS Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
S3_BUCKET="${BUCKET_PREFIX}-${ACCOUNT_ID}"

echo -e "${YELLOW}📋 Deployment Configuration:${NC}"
echo "  Stack Name: $STACK_NAME"
echo "  Region: $REGION"
echo "  Account ID: $ACCOUNT_ID"
echo "  S3 Bucket: $S3_BUCKET"

# Load API keys from .env file
echo -e "${YELLOW}🔑 Loading API keys from .env file...${NC}"
if [ -f "../.env" ]; then
    # Source the .env file and extract the keys
    export $(grep -E "^(YOUTUBE_API_KEY|GOOGLE_AI_API_KEY|FED_CHANNEL_ID)=" ../.env | xargs)

    # Check if required keys are set
    if [ -z "$YOUTUBE_API_KEY" ]; then
        echo -e "${RED}❌ YOUTUBE_API_KEY not found in .env file${NC}"
        exit 1
    fi

    if [ -z "$GOOGLE_AI_API_KEY" ]; then
        echo -e "${RED}❌ GOOGLE_AI_API_KEY not found in .env file${NC}"
        exit 1
    fi

    # Set default for FED_CHANNEL_ID if not provided
    FED_CHANNEL_ID=${FED_CHANNEL_ID:-UCAzhpt9DmG6PnHXjmJTvRGQ}

    echo -e "${GREEN}✅ API keys loaded successfully${NC}"
else
    echo -e "${RED}❌ .env file not found in parent directory${NC}"
    echo -e "${YELLOW}Please create a .env file in the root directory with:${NC}"
    echo "YOUTUBE_API_KEY=your_youtube_api_key"
    echo "GOOGLE_AI_API_KEY=your_google_ai_api_key"
    echo "FED_CHANNEL_ID=UCAzhpt9DmG6PnHXjmJTvRGQ"
    exit 1
fi

# Clean up any existing temporary deployment buckets first
echo -e "${YELLOW}🧹 Cleaning up existing temporary deployment buckets...${NC}"
EXISTING_TEMP_BUCKETS=$(aws s3api list-buckets --query "Buckets[?contains(Name, 'temp-deploy') && contains(Name, '$ACCOUNT_ID')].Name" --output text --region $REGION 2>/dev/null || true)

if [ ! -z "$EXISTING_TEMP_BUCKETS" ]; then
    for existing_bucket in $EXISTING_TEMP_BUCKETS; do
        echo -e "${YELLOW}🗑️  Emptying existing temp bucket: $existing_bucket${NC}"
        aws s3 rm s3://$existing_bucket --recursive --region $REGION 2>/dev/null || true
        echo -e "${YELLOW}🗑️  Deleting existing temp bucket: $existing_bucket${NC}"
        aws s3 rb s3://$existing_bucket --region $REGION 2>/dev/null || true
    done
else
    echo -e "${YELLOW}ℹ️  No existing temporary deployment buckets found${NC}"
fi

# Create temporary S3 bucket for deployment artifacts
echo -e "${YELLOW}📦 Creating temporary S3 bucket for deployment...${NC}"
TEMP_BUCKET="temp-deploy-$(date +%s)-${ACCOUNT_ID}"
aws s3 mb s3://$TEMP_BUCKET --region $REGION

# Upload layer.zip to temp bucket
echo -e "${YELLOW}📤 Uploading Lambda layer...${NC}"
if [ -f "../layer.zip" ]; then
    aws s3 cp ../layer.zip s3://$TEMP_BUCKET/layers/layer.zip
else
    echo -e "${RED}❌ layer.zip not found in parent directory${NC}"
    exit 1
fi

# Package Lambda functions
echo -e "${YELLOW}📦 Packaging Lambda functions...${NC}"
mkdir -p temp_deployment
cp lambda_*.py temp_deployment/

# Create Lambda deployment packages
cd temp_deployment
for lambda_file in lambda_*.py; do
    if [ -f "$lambda_file" ]; then
        function_name=$(basename "$lambda_file" .py | sed 's/lambda_//')
        echo "Packaging $lambda_file..."
        zip -r "${function_name}.zip" "$lambda_file"
        aws s3 cp "${function_name}.zip" s3://$TEMP_BUCKET/functions/
    fi
done
cd ..

# Deploy CloudFormation stack
echo -e "${YELLOW}☁️ Deploying CloudFormation stack...${NC}"
aws cloudformation deploy \
    --template-file cloudformation-template.yaml \
    --stack-name $STACK_NAME \
    --parameter-overrides \
        YoutubeApiKey="$YOUTUBE_API_KEY" \
        GoogleAiApiKey="$GOOGLE_AI_API_KEY" \
        FedChannelId="$FED_CHANNEL_ID" \
    --capabilities CAPABILITY_IAM \
    --region $REGION

# Get the actual S3 bucket name from stack outputs
ACTUAL_S3_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`S3BucketName`].OutputValue' \
    --output text \
    --region $REGION)

echo -e "${YELLOW}📤 Uploading assets to actual S3 bucket: $ACTUAL_S3_BUCKET${NC}"

# Upload layer to actual bucket
aws s3 cp ../layer.zip s3://$ACTUAL_S3_BUCKET/layers/layer.zip

# Create Lambda Layer
echo -e "${YELLOW}📦 Creating Lambda layer...${NC}"
LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name fomc-python-dependencies \
    --description "Python dependencies for FOMC processing" \
    --content S3Bucket=$ACTUAL_S3_BUCKET,S3Key=layers/layer.zip \
    --compatible-runtimes python3.10 \
    --region $REGION \
    --query 'LayerVersionArn' \
    --output text)

echo -e "${GREEN}✅ Layer created: $LAYER_ARN${NC}"

# Update Lambda function codes and add layer
echo -e "${YELLOW}🔄 Updating Lambda function codes and adding layer...${NC}"
for lambda_file in lambda_*.py; do
    if [ -f "$lambda_file" ]; then
        function_name="fomc-$(basename "$lambda_file" .py | sed 's/lambda_//' | sed 's/_/-/g')"
        echo "Updating $function_name..."

        # Create a temporary zip with just the Lambda function
        temp_zip="temp_${function_name}.zip"
        zip "$temp_zip" "$lambda_file"

        # Update the Lambda function code
        aws lambda update-function-code \
            --function-name "$function_name" \
            --zip-file "fileb://$temp_zip" \
            --region $REGION

        # Wait for the code update to complete before updating configuration
        echo "Waiting for code update to complete..."
        aws lambda wait function-updated \
            --function-name "$function_name" \
            --region $REGION

        # Add the layer to the function (skip for data-api-gateway and db-transform as they don't need dependencies)
        if [[ "$function_name" != *"data-api-gateway"* ]] && [[ "$function_name" != *"db-transform"* ]]; then
            echo "Adding layer to $function_name..."
            aws lambda update-function-configuration \
                --function-name "$function_name" \
                --layers "$LAYER_ARN" \
                --region $REGION
        fi

        rm "$temp_zip"
    fi
done

# Clean up temp bucket
echo -e "${YELLOW}🧹 Cleaning up temporary resources...${NC}"
aws s3 rb s3://$TEMP_BUCKET --force
rm -rf temp_deployment

# Get API Gateway URL
API_URL=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`ApiGatewayUrl`].OutputValue' \
    --output text \
    --region $REGION)

# Get API Key ID
API_KEY_ID=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`ApiKeyId`].OutputValue' \
    --output text \
    --region $REGION)

# Get State Machine ARN
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' \
    --output text \
    --region $REGION)

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"
echo
echo -e "${YELLOW}📋 Deployment Summary:${NC}"
echo "  Stack Name: $STACK_NAME"
echo "  API Gateway URL: $API_URL"
echo "  API Key ID: $API_KEY_ID"
echo "  State Machine ARN: $STATE_MACHINE_ARN"
echo "  S3 Bucket: $ACTUAL_S3_BUCKET"
echo "  DynamoDB Table: $DYNAMO_DB_NAME"
echo "  Region: $REGION"
echo
echo -e "${YELLOW}📝 To retrieve your API key value:${NC}"
echo "  aws apigateway get-api-key --api-key $API_KEY_ID --include-value --query 'value' --output text"
echo
echo -e "${GREEN}🎉 Your FOMC Debriefs infrastructure is ready!${NC}"
echo "Update your .env file with: API_BASE_URL=$API_URL"
