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


# --- LLM Observability Helpers ---

_GEMINI_25_FLASH_PRICING = {
    "input_per_token": 0.30 / 1_000_000,
    "output_per_token": 2.50 / 1_000_000,  # thinking included in output price
}


def extract_usage_metadata(final_chunk):
    """Extract token usage metadata from the final streaming chunk.

    Uses defensive getattr chains because any level can be None:
    the chunk itself, usage_metadata, individual fields, or candidates list.
    """
    if final_chunk is None:
        return {
            "prompt_token_count": 0,
            "candidates_token_count": 0,
            "total_token_count": 0,
            "thoughts_token_count": 0,
            "model_version": "",
            "finish_reason": "",
        }

    usage = getattr(final_chunk, "usage_metadata", None)

    # Extract finish_reason from candidates list
    candidates = getattr(final_chunk, "candidates", None) or []
    finish_reason = ""
    if candidates:
        raw = getattr(candidates[0], "finish_reason", None)
        finish_reason = str(raw) if raw is not None else ""

    return {
        "prompt_token_count": getattr(usage, "prompt_token_count", None) or 0,
        "candidates_token_count": getattr(usage, "candidates_token_count", None) or 0,
        "total_token_count": getattr(usage, "total_token_count", None) or 0,
        "thoughts_token_count": getattr(usage, "thoughts_token_count", None) or 0,
        "model_version": getattr(final_chunk, "model_version", None) or "",
        "finish_reason": finish_reason,
    }


def calculate_cost(prompt_tokens, candidates_tokens, thinking_tokens):
    """Calculate estimated cost in USD for a Gemini 2.5 Flash API call.

    Thinking tokens are priced at the output rate per Google's pricing.
    """
    input_cost = prompt_tokens * _GEMINI_25_FLASH_PRICING["input_per_token"]
    output_cost = (candidates_tokens + thinking_tokens) * _GEMINI_25_FLASH_PRICING["output_per_token"]
    return round(input_cost + output_cost, 6)


def build_metadata_record(
    usage_metadata, latency_seconds, retry_count,
    validation_passed_first_try, quality_warnings, cost_usd,
):
    """Assemble an observability metadata record ready for JSON/S3."""
    return {
        "token_usage": {
            "prompt_token_count": usage_metadata["prompt_token_count"],
            "candidates_token_count": usage_metadata["candidates_token_count"],
            "total_token_count": usage_metadata["total_token_count"],
            "thoughts_token_count": usage_metadata["thoughts_token_count"],
        },
        "model_version": usage_metadata["model_version"],
        "finish_reason": usage_metadata["finish_reason"],
        "latency_seconds": latency_seconds,
        "retry_count": retry_count,
        "validation_passed_first_try": validation_passed_first_try,
        "quality_warnings": quality_warnings,
        "estimated_cost_usd": cost_usd,
    }
