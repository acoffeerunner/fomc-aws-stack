import json
import os

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("DYNAMODB_TABLE", "fomc-gists-dynamodb")
table = dynamodb.Table(table_name)


def get_years():
    """Scan table to collect all distinct years."""
    response = table.scan(
        ProjectionExpression="#yr", ExpressionAttributeNames={"#yr": "year"}
    )
    years = {item["year"] for item in response["Items"]}

    # Handle pagination if scan > 1MB
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="#yr",
            ExpressionAttributeNames={"#yr": "year"},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        years.update({item["year"] for item in response["Items"]})

    return sorted(list(years))


def lambda_handler(event, context):
    # 1. List all years
    if event["routeKey"] == "GET /meetings/years":
        years = get_years()
        return {
            "statusCode": 200,
            "body": json.dumps(years),
            "headers": {"Content-Type": "application/json"},
        }

    # 2. Get all meetings in a year
    if event["routeKey"] == "GET /meetings/{year}":
        response = table.query(
            Select="SPECIFIC_ATTRIBUTES",
            KeyConditionExpression=Key("year").eq(event["pathParameters"]["year"]),
            ProjectionExpression="month_date",
        )
        items = response.get("Items", [])
        items = [item["month_date"] for item in items]
        return {
            "statusCode": 200,
            "body": json.dumps(items, default=str),
            "headers": {"Content-Type": "application/json"},
        }

    # 3. Get a specific meeting
    if event["routeKey"] == "GET /meetings/{year}/{month_date}":
        response = table.get_item(
            Key={
                "year": event["pathParameters"]["year"],
                "month_date": event["pathParameters"]["month_date"],
            }
        )
        item = response.get("Item")
        if not item:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Meeting not found"}),
                "headers": {"Content-Type": "application/json"},
            }
        return {
            "statusCode": 200,
            "body": json.dumps(item, default=str),
            "headers": {"Content-Type": "application/json"},
        }
    # 4. Get a specific meeting's opening statement
    if (
        event["routeKey"]
        == "GET /meetings/{year}/{month_date}/opening_statement_transcript"
    ):
        response = table.get_item(
            Key={
                "year": event["pathParameters"]["year"],
                "month_date": event["pathParameters"]["month_date"],
            },
            ProjectionExpression="opening_statement_transcript",
        )
        item = response.get("Item")
        if not item:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Meeting not found"}),
                "headers": {"Content-Type": "application/json"},
            }
        return {
            "statusCode": 200,
            "body": json.dumps(item, default=str),
            "headers": {"Content-Type": "application/json"},
        }
    return {
        "statusCode": 400,
        "body": json.dumps({"error": "Invalid request"}),
        "headers": {"Content-Type": "application/json"},
    }
