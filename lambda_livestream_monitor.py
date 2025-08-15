import json
import logging
import os
import re
from datetime import datetime, timedelta
from time import sleep

import boto3
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from pytz import timezone
from zoneinfo import ZoneInfo

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
events_client = boto3.client("events")
lambda_client = boto3.client("lambda")
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    Monitor FOMC YouTube livestream and trigger processing when completed.
    """
    logger.info("=== FOMC Monitor Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    # Check if this is a scheduled trigger from the scheduler
    is_scheduled = event.get("scheduled_trigger", False)
    rule_name = event.get("rule_name")
    max_attempts = event.get("max_attempts", 3)
    current_attempt = event.get("current_attempt", 1)

    logger.info(f"Scheduled trigger: {is_scheduled}")
    logger.info(f"Rule name: {rule_name}")
    logger.info(f"Attempt {current_attempt} of {max_attempts}")

    try:
        # Get Lambda function details
        function_name = context.function_name
        function_arn = context.invoked_function_arn

        tz = timezone("US/Eastern")
        d = datetime.now(tz)
        date_str = d.strftime("%B %d, %Y")
        date_dir = d.strftime("%Y-%m-%d")

        logger.info(f"Current ET time: {d}")
        logger.info(f"Looking for FOMC meeting: {date_str}")
        logger.info(f"Target date directory: {date_dir}")

        # Get API keys and configuration
        logger.info("Retrieving API keys from Secrets Manager...")
        keys = get_keys()
        logger.info("Successfully retrieved API keys")

        # Initialize YouTube API client
        logger.info("Initializing YouTube API client...")
        youtube = build("youtube", "v3", developerKey=keys["yt_api_key"])

        # Search for completed FOMC livestreams
        logger.info(
            f"Searching for completed videos on Fed channel: {keys['fed_channel_id']}"
        )
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
        response_body = {"video_id": "", "date_dir": ""}

        logger.info(f"Starting monitoring loop (max {max_calls} calls)")

        while call_count < max_calls:
            logger.info(f"API call #{call_count + 1} of {max_calls}")

            try:
                response = request.execute()
                call_count += 1

                if not response.get("items"):
                    logger.warning("No videos found in search results")
                    # For scheduled triggers, just continue the loop
                    logger.info(
                        "Scheduled trigger: No videos found, continuing search..."
                    )
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

                # NEW: collapse multiple spaces to a single space
                title = " ".join(title.split())
                expected_title = " ".join(expected_title.split())
                logger.info(f"Comparing titles: '{title}' vs '{expected_title}'")

                if title == expected_title:
                    logger.info("🎯 FOUND MATCHING FOMC PRESS CONFERENCE!")
                    logger.info(f"Title matches expected: '{expected_title}'")

                    # Create S3 directory for this date
                    bucket_name = keys["s3_name"]
                    logger.info(
                        f"Creating S3 directory: s3://{bucket_name}/{date_dir}/"
                    )

                    try:
                        s3_client.put_object(Bucket=bucket_name, Key=(date_dir + "/"))
                        logger.info("Successfully created S3 directory")
                    except Exception as e:
                        logger.error(f"Failed to create S3 directory: {e}")
                        return create_response(500, f"S3 error: {e}")

                    # Prepare payload for transcriber Lambda
                    response_body["date_dir"] = str(date_dir)
                    response_body["video_id"] = video_id

                    logger.info(
                        f"Invoking transcriber Lambda with payload: {response_body}"
                    )

                    invoke_lambda("fomc-transcriber", response_body)

                    # Clean up EventBridge rules since we found the video
                    if is_scheduled and rule_name:
                        cleanup_eventbridge_rule(rule_name)

                    # Clean up any temporary rule if this was a retry attempt
                    temp_rule_name = event.get("temp_rule_name")
                    if temp_rule_name:
                        cleanup_eventbridge_rule(temp_rule_name)

                    return create_response(
                        200, "FOMC video found and processing started", response_body
                    )

                else:
                    logger.info(f"Title mismatch: '{title}' != '{expected_title}'")
                    logger.info("Continuing search...")
                    sleep(30)
                    continue

            except Exception as e:
                logger.error(f"Error during API call #{call_count}: {e}")
                if call_count >= max_calls:
                    logger.error("Maximum API calls reached")
                    logger.info("Scheduled trigger: Max calls reached, exiting")
                    break

        # If we exit the loop without finding the video
        logger.warning("Completed monitoring loop without finding FOMC video")

        if is_scheduled:
            # Handle scheduled attempts
            if current_attempt < max_attempts:
                # Schedule next attempt in 10.5 minutes
                schedule_next_scheduled_attempt(event, context, current_attempt + 1)
                return create_response(
                    200,
                    f"Attempt {current_attempt} failed, scheduled attempt {current_attempt + 1}",
                )
            else:
                # All attempts exhausted, cleanup rule
                logger.warning(
                    f"All {max_attempts} attempts exhausted, cleaning up rule"
                )
                if rule_name:
                    cleanup_eventbridge_rule(rule_name)
                return create_response(
                    200, "All scheduled attempts exhausted, rule cleaned up"
                )

    except Exception as e:
        logger.error(f"Unexpected error in lambda_handler: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return create_response(500, f"Unexpected error: {e}")


def cleanup_current_rule(event):
    """Clean up the current EventBridge rule"""
    if "rule_name" in event:
        old_rule_name = event["rule_name"]
        try:
            logger.info(f"Cleaning up rule: {old_rule_name}")

            # Remove targets first
            events_client.remove_targets(Rule=old_rule_name, Ids=["1"])

            # Delete the rule
            events_client.delete_rule(Name=old_rule_name)

            logger.info(f"Successfully cleaned up rule: {old_rule_name}")
        except Exception as e:
            logger.warning(f"Could not clean up rule {old_rule_name}: {e}")


def create_response(status_code, message, data=None):
    """Create standardized Lambda response"""
    response_body = {"message": message}
    if data:
        response_body.update(data)

    logger.info(f"Returning response: {status_code} - {message}")

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response_body),
    }


def get_keys():
    """Retrieve API keys and configuration from AWS Secrets Manager"""
    secret_name = "fomc-gists/env-keys"
    region_name = "us-east-1"

    logger.info(f"Retrieving secret: {secret_name} from region: {region_name}")

    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        secret_resp = client.get_secret_value(SecretId=secret_name)
        logger.info("Successfully retrieved secret from Secrets Manager")

        # Parse the secret string (assuming it's JSON)
        if isinstance(secret_resp["SecretString"], str):
            import json

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
        error_code = e.response["Error"]["Code"]
        if error_code == "DecryptionFailureException":
            logger.error("Secret cannot be decrypted using the provided KMS key")
        elif error_code == "InternalServiceErrorException":
            logger.error("Internal service error occurred")
        elif error_code == "InvalidParameterException":
            logger.error("Invalid parameter provided")
        elif error_code == "InvalidRequestException":
            logger.error("Invalid request")
        elif error_code == "ResourceNotFoundException":
            logger.error(f"Secret {secret_name} not found")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse secret as JSON: {e}")
        raise e
    except KeyError as e:
        logger.error(f"Missing required key in secret: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error retrieving keys: {e}")
        raise e


def schedule_next_scheduled_attempt(event, context, next_attempt):
    """Schedule the next attempt for scheduled monitoring (10.5 minutes later)"""
    try:
        rule_name = event.get("rule_name")
        if not rule_name:
            logger.error("No rule name provided for scheduling next attempt")
            return

        # Create a new one-time rule for 10.5 minutes from now
        run_time = datetime.utcnow() + timedelta(minutes=10.5)
        cron_expression = f"cron({run_time.minute} {run_time.hour} {run_time.day} {run_time.month} ? {run_time.year})"
        temp_rule_name = f"{rule_name}-attempt-{next_attempt}"

        logger.info(f"Scheduling attempt {next_attempt} at {run_time.isoformat()}")

        # Create temporary rule for next attempt on default event bus (scheduled rules only work on default bus)
        events_client.put_rule(
            Name=temp_rule_name,
            ScheduleExpression=cron_expression,
            Description=f"FOMC monitoring attempt {next_attempt}",
            State="ENABLED",
        )

        # Update event payload for next attempt
        updated_event = event.copy()
        updated_event["current_attempt"] = next_attempt
        updated_event["temp_rule_name"] = temp_rule_name

        # Add target
        events_client.put_targets(
            Rule=temp_rule_name,
            Targets=[
                {
                    "Id": "1",
                    "Arn": context.invoked_function_arn,
                    "Input": json.dumps(updated_event),
                }
            ],
        )

        # Grant permission
        try:
            lambda_client.add_permission(
                FunctionName=context.function_name,
                StatementId=f"allow-eventbridge-{temp_rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{context.invoked_function_arn.split(':')[3]}:{context.invoked_function_arn.split(':')[4]}:rule/{temp_rule_name}",
            )
        except lambda_client.exceptions.ResourceConflictException:
            logger.info("EventBridge permission already exists")

        logger.info(f"Successfully scheduled attempt {next_attempt}")

    except Exception as e:
        logger.error(f"Failed to schedule next attempt: {e}")


def cleanup_eventbridge_rule(rule_name):
    """Clean up an EventBridge rule and its targets"""
    try:
        logger.info(f"Cleaning up EventBridge rule: {rule_name}")

        # Remove targets first
        events_client.remove_targets(Rule=rule_name, Ids=["1"])

        # Delete the rule
        events_client.delete_rule(Name=rule_name)

        logger.info(f"Successfully cleaned up rule: {rule_name}")

    except events_client.exceptions.ResourceNotFoundException:
        logger.info(f"Rule {rule_name} not found (already deleted)")
    except Exception as e:
        logger.error(f"Failed to cleanup rule {rule_name}: {e}")


def invoke_lambda(lambda_name, payload):
    try:
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="Event",  # Async invocation
            Payload=json.dumps(payload),
        )

        logger.info(f"Successfully invoked {lambda_name}")
        logger.info(f"Lambda response status: {lambda_response['StatusCode']}")

    except Exception as e:
        logger.error(f"Failed to invoke {lambda_name} Lambda: {e}")
