# FOMC Debriefs AWS Deployment

This directory contains all the necessary files to deploy the FOMC Debriefs infrastructure to AWS using CloudFormation.

## Architecture Overview

The deployment creates the following AWS resources:

- **DynamoDB Table**: `fomc-gists-dynamodb` - Stores processed FOMC meeting data
- **S3 Bucket**: `fomc-gists-s3-{account-id}` - Stores raw files, Lambda layers, and Glue scripts
- **API Gateway**: REST API with 4 endpoints for data access
- **Lambda Functions**: 6 functions for different processing stages
- **Glue Job**: Data transformation job to process and store meeting data
- **IAM Roles**: Proper permissions for all services

## Prerequisites

1. **AWS CLI** configured with appropriate credentials
2. **Environment file** (`.env`) in the parent directory with required API keys
3. **Lambda Layer** (`layer.zip`) in the parent directory containing Python dependencies

### Setting Up API Keys

1. Copy the example environment file to the parent directory:
   ```bash
   cp .env.example ../.env
   ```

2. Edit `../.env` and fill in your API keys:
   ```
   # YouTube Data API v3 Key
   # Get one at: https://console.cloud.google.com/apis/credentials
   YOUTUBE_API_KEY=your_youtube_api_key_here

   # Google AI Studio API Key (for Gemini)
   # Get one at: https://aistudio.google.com/app/apikey
   GOOGLE_AI_API_KEY=your_google_ai_api_key_here

   # Federal Reserve YouTube Channel ID (default provided)
   FED_CHANNEL_ID=UCAzhpt9DmG6PnHXjmJTvRGQ
   ```

3. The deployment script will:
   - Read these keys from the `.env` file
   - Pass them securely to CloudFormation as parameters
   - Store them in AWS Secrets Manager as `fomc-gists/env-keys`
   - Lambda functions retrieve keys from Secrets Manager at runtime

## Deployment Instructions

### Option 1: Using Bash Script (Linux/macOS/WSL)

```bash
cd aws
chmod +x deploy.sh
./deploy.sh
```

### Option 2: Using PowerShell Script (Windows)

```powershell
cd aws
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\deploy.ps1
```

### Option 3: Manual CloudFormation

```bash
# Load environment variables from .env file
export $(grep -E "^(YOUTUBE_API_KEY|GOOGLE_AI_API_KEY|FED_CHANNEL_ID)=" ../.env | xargs)

# Deploy the stack
aws cloudformation deploy \
    --template-file cloudformation-template.yaml \
    --stack-name fomc-gists-stack \
    --parameter-overrides \
        YoutubeApiKey="$YOUTUBE_API_KEY" \
        GoogleAiApiKey="$GOOGLE_AI_API_KEY" \
        FedChannelId="${FED_CHANNEL_ID:-UCAzhpt9DmG6PnHXjmJTvRGQ}" \
    --capabilities CAPABILITY_IAM \
    --region us-east-1

# Upload required files manually
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name fomc-gists-stack \
    --query 'Stacks[0].Outputs[?OutputKey==`S3BucketName`].OutputValue' \
    --output text)

aws s3 cp ../layer.zip s3://$BUCKET_NAME/layers/layer.zip
aws s3 cp glue_job_transform_for_db.py s3://$BUCKET_NAME/scripts/

# Update Lambda function codes (repeat for each function)
aws lambda update-function-code \
    --function-name fomc-data-api-gateway \
    --zip-file fileb://lambda_data_api_gateway.zip
```

## API Endpoints

After deployment, the API Gateway will provide these endpoints:

1. **GET /meetings/years** - Returns all available years
2. **GET /meetings/{year}** - Returns all meeting dates for a specific year
3. **GET /meetings/{year}/{month-date}** - Returns full meeting data
4. **GET /meetings/{year}/{month-date}/opening_statement_transcript** - Returns just the opening statement

Example API URL: `https://abcdefghij.execute-api.us-east-1.amazonaws.com`

## Environment Variables

The deployment automatically configures these environment variables for Lambda functions:

- `YOUTUBE_API_KEY` - For livestream monitoring
- `GOOGLE_AI_API_KEY` - For AI analysis
- `FED_CHANNEL_ID` - YouTube channel to monitor
- `S3_BUCKET` - S3 bucket name
- `DYNAMODB_TABLE` - DynamoDB table name
- `GLUE_JOB_NAME` - Glue job name for data processing

## File Structure

```
aws/
├── cloudformation-template.yaml  # Infrastructure as Code
├── deploy.sh                     # Bash deployment script
├── undeploy.sh                   # Cleanup script
├── .env.example                  # Example environment file (copy to ../.env)
├── lambda_data_api_gateway.py    # API Gateway integration Lambda
├── lambda_livestream_monitor.py  # YouTube monitoring Lambda
├── lambda_transcriber.py         # Transcription processing
├── lambda_opening_statement_analysis.py # Opening statement analysis
├── lambda_press_qa_analysis.py   # Press Q&A analysis
├── lambda_glue_trigger.py        # Triggers Glue job
├── lambda_scheduler.py           # FOMC meeting scheduler
├── glue_job_transform_for_db.py  # Data transformation for DynamoDB
└── README.md                     # This file
```

## Workflow

1. **Livestream Monitor** - Monitors YouTube for FOMC meetings
2. **Transcriber** - Processes video and creates transcripts
3. **Opening Statement Analysis** - Analyzes opening statements using AI
4. **Press Q&A Analysis** - Analyzes press conference Q&A using AI
5. **Glue Trigger** - Triggered when all analysis is complete
6. **Glue Job** - Transforms and loads data into DynamoDB
7. **API Gateway** - Serves processed data to applications

## Cleanup

To remove all AWS resources:

```bash
# Using the cleanup script
./undeploy.sh

# Or manually
aws cloudformation delete-stack --stack-name fomc-gists-stack
```

## Costs

Expected monthly costs (with minimal usage):

- **DynamoDB**: ~$1-5 (pay per request)
- **Lambda**: ~$1-10 (generous free tier)
- **S3**: ~$1-5 (storage and requests)
- **API Gateway**: ~$1-5 (per million requests)
- **Glue**: ~$0.44/hour when running

**Total estimated**: $5-25/month depending on usage

## Troubleshooting

### Common Issues

1. **Layer.zip not found**: Ensure `layer.zip` exists in the parent directory
2. **API Keys**: Make sure your YouTube and Google AI API keys are valid
3. **Permissions**: Ensure your AWS credentials have sufficient permissions
4. **Region**: The default region is `us-east-1`, modify scripts if needed

### Logs

Check CloudWatch Logs for each Lambda function:
- `/aws/lambda/fomc-livestream-monitor`
- `/aws/lambda/fomc-transcriber`
- `/aws/lambda/fomc-opening-statement-analysis`
- `/aws/lambda/fomc-press-qa-analysis`
- `/aws/lambda/fomc-glue-trigger`
- `/aws/lambda/fomc-data-api-gateway`

## Security

- API keys are stored in AWS Secrets Manager (`fomc-gists/env-keys`)
- Lambda functions retrieve secrets at runtime (not stored in environment variables)
- S3 bucket has public access blocked
- IAM roles follow least privilege principle
- All communications use HTTPS

## Support

For issues with deployment:
1. Check CloudFormation events in AWS Console
2. Review Lambda function logs in CloudWatch
3. Verify all prerequisites are met
4. Ensure proper AWS permissions
