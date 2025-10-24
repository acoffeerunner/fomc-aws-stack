import json
import logging
import os
import re
from datetime import datetime
from time import sleep

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from pytz import timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
events_client = boto3.client("events")
s3_client = boto3.client("s3")


class VideoNotFoundError(Exception):
    """Raised when the FOMC video is not found after exhausting the polling loop."""
    pass


def lambda_handler(event, context):
    """
    Monitor FOMC YouTube livestream. Returns video info on success,
    raises VideoNotFoundError if not found (Step Functions retries).
    """
    logger.info("=== FOMC Monitor Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    rule_name = event.get("rule_name")

    tz = timezone("US/Eastern")
    d = datetime.now(tz)
    date_str = d.strftime("%B %d, %Y")
    date_dir = d.strftime("%Y-%m-%d")

    logger.info(f"Current ET time: {d}")
    logger.info(f"Looking for FOMC meeting: {date_str}")
    logger.info(f"Target date directory: {date_dir}")

    # Get API keys and configuration
    keys = get_keys()

    # Initialize YouTube API client
    youtube = build("youtube", "v3", developerKey=keys["yt_api_key"])

    # Search for completed FOMC livestreams
    request = youtube.search().list(
        part="snippet",
        eventType="completed",
        channelId=keys["fed_channel_id"],
        maxResults=1,
        order="date",
        type="video",
    )

    call_count = 0
    max_calls = 20

    logger.info(f"Starting monitoring loop (max {max_calls} calls)")

    while call_count < max_calls:
        logger.info(f"API call #{call_count + 1} of {max_calls}")

        try:
            response = request.execute()
            call_count += 1

            if not response.get("items"):
                logger.warning("No videos found in search results")
                sleep(30)
                continue

            video = response["items"][0]
            title = video["snippet"]["title"]
            video_id = video["id"]["videoId"]
            published_at = video["snippet"]["publishedAt"]

            logger.info(f"Found video: '{title}' (ID: {video_id})")
            logger.info(f"Published at: {published_at}")

            expected_title = f"FOMC Press Conference {date_str}"
            expected_title = re.sub(r"[^a-zA-Z0-9\s]", "", expected_title)
            title = re.sub(r"[^a-zA-Z0-9\s]", "", title)

            # Collapse multiple spaces to a single space
            title = " ".join(title.split())
            expected_title = " ".join(expected_title.split())
            logger.info(f"Comparing titles: '{title}' vs '{expected_title}'")

            if title == expected_title:
                logger.info("FOUND MATCHING FOMC PRESS CONFERENCE!")

                # Create S3 directory for this date
                bucket_name = keys["s3_name"]
                s3_client.put_object(Bucket=bucket_name, Key=(date_dir + "/"))
                logger.info(f"Created S3 directory: s3://{bucket_name}/{date_dir}/")

                # Clean up EventBridge rule since we found the video
                if rule_name:
                    cleanup_eventbridge_rule(rule_name)

                return {"date_dir": str(date_dir), "video_id": video_id}

            else:
                logger.info(f"Title mismatch: '{title}' != '{expected_title}'")
                sleep(30)
                continue

        except Exception as e:
            logger.error(f"Error during API call #{call_count}: {e}")
            if call_count >= max_calls:
                break

    # Polling loop exhausted without finding the video
    logger.warning("Completed monitoring loop without finding FOMC video")
    raise VideoNotFoundError(
        f"FOMC video not found after {max_calls} polling attempts for {date_str}"
    )


def cleanup_eventbridge_rule(rule_name):
    """Clean up an EventBridge rule and its targets"""
    try:
        logger.info(f"Cleaning up EventBridge rule: {rule_name}")
        events_client.remove_targets(Rule=rule_name, Ids=["1"])
        events_client.delete_rule(Name=rule_name)
        logger.info(f"Successfully cleaned up rule: {rule_name}")
    except events_client.exceptions.ResourceNotFoundException:
        logger.info(f"Rule {rule_name} not found (already deleted)")
    except Exception as e:
        logger.error(f"Failed to cleanup rule {rule_name}: {e}")


def get_keys():
    """Retrieve API keys and configuration from AWS Secrets Manager"""
    secret_name = "fomc-gists/env-keys"
    region_name = "us-east-1"

    logger.info(f"Retrieving secret: {secret_name} from region: {region_name}")

    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        secret_resp = client.get_secret_value(SecretId=secret_name)
        logger.info("Successfully retrieved secret from Secrets Manager")

        if isinstance(secret_resp["SecretString"], str):
            secret_data = json.loads(secret_resp["SecretString"])
        else:
            secret_data = secret_resp["SecretString"]

        keys = {
            "yt_api_key": secret_data["YOUTUBE_API_KEY"],
            "fed_channel_id": secret_data["FED_CHANNEL_ID"],
            "s3_name": secret_data["S3_NAME"],
        }

        logger.info("Successfully parsed secret keys")
        logger.info(f"S3 bucket: {keys['s3_name']}")
        logger.info(f"Fed channel ID: {keys['fed_channel_id']}")

        return keys

    except ClientError as e:
        logger.error(f"Failed to retrieve secret from Secrets Manager: {e}")
        raise e
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse secret: {e}")
        raise e
