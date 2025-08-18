import json
import logging
import sys

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["date_dir"])
date = args["date_dir"]

# initialize logging
logger = logging.getLogger()
logger.setLevel("INFO")

# initialize clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
import os

bucket = os.environ.get("S3_BUCKET", "fomc-gists-s3")
table_name = os.environ.get("DYNAMODB_TABLE", "fomc-gists-dynamodb")
table = dynamodb.Table(table_name)

# expected files
files = [
    "output_transcript.json",
    "output_opening_analysis.json",
    "output_press_qa_analysis.json",
]

data = {}

year = date[:4]
month_date = date[5:]
data["year"] = year
data["month_date"] = month_date
logger.info(f"year: {year}, month_date: {month_date}")


def push_to_table(data, bucket):
    for f in files:
        try:
            key = f"{date}/{f}"
            obj = s3.get_object(Bucket=bucket, Key=key)
            logger.info(f"Obtained obj: {key} from bucket")
            content = json.loads(obj["Body"].read())

            if f == "output_transcript.json":
                data["opening_statement_transcript"] = content["transcript"][0]
                data["press_qa_transcript"] = content["transcript"][1:]

            elif f == "output_opening_analysis.json":
                data["opening_statement_analysis"] = content

            elif f == "output_press_qa_analysis.json":
                data["press_qa_analysis"] = content["press_q_and_a_themes"]
                data["most_profound_question"] = content["most_profound_question"]
        except:
            logger.error(f"Error while transforming {key}")
            return

    table.put_item(Item=data)


push_to_table(data, bucket)
