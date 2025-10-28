import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()

_cached_keys = None


def get_keys():
    """
    Retrieve API keys from AWS Secrets Manager with module-level caching.
    Cached at cold start — subsequent warm invocations skip the API call.
    """
    global _cached_keys
    if _cached_keys:
        return _cached_keys

    secret_name = "fomc-gists/env-keys"
    region_name = "us-east-1"

    logger.info(f"Retrieving secret: {secret_name} from region: {region_name}")

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    secret_resp = client.get_secret_value(SecretId=secret_name)
    logger.info("Successfully retrieved secret from Secrets Manager")

    secret_data = json.loads(secret_resp["SecretString"])

    _cached_keys = {
        "yt_api_key": secret_data["YOUTUBE_API_KEY"],
        "fed_channel_id": secret_data.get("FED_CHANNEL_ID", ""),
        "ai_key": secret_data.get("GOOGLE_AI_API_KEY", ""),
        "s3_name": secret_data["S3_NAME"],
    }

    logger.info("Successfully parsed and cached secret keys")
    return _cached_keys
