import json
import os

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("DYNAMODB_TABLE", "fomc-gists-dynamodb")
table = dynamodb.Table(table_name)

CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "https://fomcdebriefs.netlify.app",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
}


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
    # Build route key from REST API v1 event format
    route_key = f"{event['httpMethod']} {event['resource']}"

    # 1. List all years
    if route_key == "GET /meetings/years":
        years = get_years()
        return {
            "statusCode": 200,
            "body": json.dumps(years),
            "headers": CORS_HEADERS,
        }

    # 2. Get all meetings in a year
    if route_key == "GET /meetings/{year}":
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
            "headers": CORS_HEADERS,
        }

    # 3. Get a specific meeting
    if route_key == "GET /meetings/{year}/{month_date}":
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
                "headers": CORS_HEADERS,
            }
        return {
            "statusCode": 200,
            "body": json.dumps(item, default=str),
            "headers": CORS_HEADERS,
        }
    # 4. Get a specific meeting's opening statement
    if (
        route_key
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
                "headers": CORS_HEADERS,
            }
        return {
            "statusCode": 200,
            "body": json.dumps(item, default=str),
            "headers": CORS_HEADERS,
        }
    return {
        "statusCode": 400,
        "body": json.dumps({"error": "Invalid request"}),
        "headers": CORS_HEADERS,
    }
