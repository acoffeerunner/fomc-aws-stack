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
