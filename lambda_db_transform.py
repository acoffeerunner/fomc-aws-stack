import json
import logging
import os

import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

bucket = os.environ["S3_BUCKET"]
table_name = os.environ["DYNAMODB_TABLE"]
table = dynamodb.Table(table_name)


def lambda_handler(event, context):
    """
    Read transcript + analysis files from S3 and write combined record to DynamoDB.
    """
    logger.info(f"Event: {json.dumps(event)}")

    date_dir = event["date_dir"]
    year = date_dir[:4]
    month_date = date_dir[5:]
    logger.info(f"year: {year}, month_date: {month_date}")

    data = {"year": year, "month_date": month_date}

    files = [
        "output_transcript.json",
        "output_opening_analysis.json",
        "output_press_qa_analysis.json",
    ]

    for f in files:
        key = f"{date_dir}/{f}"
        obj = s3.get_object(Bucket=bucket, Key=key)
        logger.info(f"Read {key}")
        content = json.loads(obj["Body"].read())

        if f == "output_transcript.json":
            data["opening_statement_transcript"] = content["transcript"][0]
            data["press_qa_transcript"] = content["transcript"][1:]
        elif f == "output_opening_analysis.json":
            data["opening_statement_analysis"] = content
        elif f == "output_press_qa_analysis.json":
            data["press_qa_analysis"] = content["press_q_and_a_themes"]
            data["most_profound_question"] = content["most_profound_question"]

    table.put_item(Item=data)
    logger.info(f"Wrote DynamoDB item for {year}/{month_date}")

    return {"date_dir": date_dir, "status": "transform_complete"}
