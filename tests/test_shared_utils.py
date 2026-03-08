"""Tests for shared_utils Secrets Manager caching."""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset the module-level cache before each test."""
    import shared_utils
    shared_utils._cached_keys = None
    yield
    shared_utils._cached_keys = None


@pytest.fixture
def mock_secretsmanager():
    """Mock Secrets Manager client."""
    secret_data = {
        "YOUTUBE_API_KEY": "test-yt-key",
        "GOOGLE_AI_API_KEY": "test-ai-key",
        "FED_CHANNEL_ID": "test-channel-id",
        "S3_NAME": "test-bucket",
    }
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(secret_data),
    }

    with patch("boto3.session.Session") as mock_session:
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


class TestGetKeys:
    def test_returns_all_keys(self, mock_secretsmanager):
        from shared_utils import get_keys

        keys = get_keys()

        assert keys["yt_api_key"] == "test-yt-key"
        assert keys["ai_key"] == "test-ai-key"
        assert keys["fed_channel_id"] == "test-channel-id"
        assert keys["s3_name"] == "test-bucket"

    def test_caches_result(self, mock_secretsmanager):
        from shared_utils import get_keys

        keys1 = get_keys()
        keys2 = get_keys()

        assert keys1 is keys2
        # Secrets Manager should only be called once
        mock_secretsmanager.get_secret_value.assert_called_once()

    def test_cache_survives_multiple_calls(self, mock_secretsmanager):
        from shared_utils import get_keys

        for _ in range(10):
            get_keys()

        mock_secretsmanager.get_secret_value.assert_called_once()


class TestExtractUsageMetadata:
    def test_valid_chunk(self):
        from shared_utils import extract_usage_metadata

        chunk = MagicMock()
        chunk.usage_metadata.prompt_token_count = 100
        chunk.usage_metadata.candidates_token_count = 200
        chunk.usage_metadata.total_token_count = 300
        chunk.usage_metadata.thoughts_token_count = 50
        chunk.model_version = "gemini-2.5-flash-preview-04-17"
        candidate = MagicMock()
        candidate.finish_reason = "STOP"
        chunk.candidates = [candidate]

        result = extract_usage_metadata(chunk)

        assert result["prompt_token_count"] == 100
        assert result["candidates_token_count"] == 200
        assert result["total_token_count"] == 300
        assert result["thoughts_token_count"] == 50
        assert result["model_version"] == "gemini-2.5-flash-preview-04-17"
        assert result["finish_reason"] == "STOP"

    def test_none_chunk(self):
        from shared_utils import extract_usage_metadata

        result = extract_usage_metadata(None)

        assert result["prompt_token_count"] == 0
        assert result["candidates_token_count"] == 0
        assert result["total_token_count"] == 0
        assert result["thoughts_token_count"] == 0
        assert result["model_version"] == ""
        assert result["finish_reason"] == ""

    def test_none_usage_metadata(self):
        from shared_utils import extract_usage_metadata

        chunk = MagicMock()
        chunk.usage_metadata = None
        chunk.model_version = "gemini-2.5-flash"
        chunk.candidates = []

        result = extract_usage_metadata(chunk)

        assert result["prompt_token_count"] == 0
        assert result["candidates_token_count"] == 0
        assert result["model_version"] == "gemini-2.5-flash"
        assert result["finish_reason"] == ""

    def test_none_individual_fields(self):
        from shared_utils import extract_usage_metadata

        chunk = MagicMock()
        chunk.usage_metadata.prompt_token_count = None
        chunk.usage_metadata.candidates_token_count = None
        chunk.usage_metadata.total_token_count = None
        chunk.usage_metadata.thoughts_token_count = None
        chunk.model_version = None
        chunk.candidates = None

        result = extract_usage_metadata(chunk)

        assert result["prompt_token_count"] == 0
        assert result["candidates_token_count"] == 0
        assert result["total_token_count"] == 0
        assert result["thoughts_token_count"] == 0
        assert result["model_version"] == ""
        assert result["finish_reason"] == ""


class TestCalculateCost:
    def test_zero_tokens(self):
        from shared_utils import calculate_cost

        assert calculate_cost(0, 0, 0) == 0.0

    def test_input_only(self):
        from shared_utils import calculate_cost

        # 1M input tokens at $0.30/1M = $0.30
        assert calculate_cost(1_000_000, 0, 0) == 0.3

    def test_output_only(self):
        from shared_utils import calculate_cost

        # 1M output tokens at $2.50/1M = $2.50
        assert calculate_cost(0, 1_000_000, 0) == 2.5

    def test_typical_call(self):
        from shared_utils import calculate_cost

        # 5000 input ($0.0015) + 1000 output ($0.0025) + 2000 thinking ($0.005) = $0.009
        cost = calculate_cost(5000, 1000, 2000)
        assert cost == 0.009

    def test_rounding(self):
        from shared_utils import calculate_cost

        cost = calculate_cost(1, 1, 1)
        # Should be rounded to 6 decimal places
        assert cost == round(cost, 6)


class TestBuildMetadataRecord:
    def test_complete_record(self):
        from shared_utils import build_metadata_record

        usage = {
            "prompt_token_count": 4521,
            "candidates_token_count": 1203,
            "total_token_count": 5724,
            "thoughts_token_count": 1847,
            "model_version": "gemini-2.5-flash-preview-04-17",
            "finish_reason": "STOP",
        }

        result = build_metadata_record(
            usage_metadata=usage,
            latency_seconds=42.37,
            retry_count=1,
            validation_passed_first_try=False,
            quality_warnings=["Missing theme"],
            cost_usd=0.007393,
        )

        assert result["token_usage"]["prompt_token_count"] == 4521
        assert result["token_usage"]["candidates_token_count"] == 1203
        assert result["model_version"] == "gemini-2.5-flash-preview-04-17"
        assert result["finish_reason"] == "STOP"
        assert result["latency_seconds"] == 42.37
        assert result["retry_count"] == 1
        assert result["validation_passed_first_try"] is False
        assert result["quality_warnings"] == ["Missing theme"]
        assert result["estimated_cost_usd"] == 0.007393

    def test_zero_retries_no_warnings(self):
        from shared_utils import build_metadata_record

        usage = {
            "prompt_token_count": 100,
            "candidates_token_count": 50,
            "total_token_count": 150,
            "thoughts_token_count": 0,
            "model_version": "gemini-2.5-flash",
            "finish_reason": "STOP",
        }

        result = build_metadata_record(
            usage_metadata=usage,
            latency_seconds=5.0,
            retry_count=0,
            validation_passed_first_try=True,
            quality_warnings=[],
            cost_usd=0.000155,
        )

        assert result["retry_count"] == 0
        assert result["validation_passed_first_try"] is True
        assert result["quality_warnings"] == []
