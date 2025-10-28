"""Tests for lambda_data_api_gateway route matching and CORS."""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_dynamodb():
    """Mock DynamoDB table at module level before importing."""
    with patch("boto3.resource") as mock_resource:
        mock_table = MagicMock()
        mock_resource.return_value.Table.return_value = mock_table
        # Must import after patching since module-level code runs on import
        import importlib
        import lambda_data_api_gateway
        importlib.reload(lambda_data_api_gateway)
        yield mock_table, lambda_data_api_gateway


class TestRouteMatching:
    def test_get_years(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.scan.return_value = {"Items": [{"year": "2024"}, {"year": "2025"}]}

        event = {"httpMethod": "GET", "resource": "/meetings/years"}
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert json.loads(result["body"]) == ["2024", "2025"]

    def test_get_meetings_by_year(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.query.return_value = {
            "Items": [{"month_date": "01-29"}, {"month_date": "03-19"}]
        }

        event = {
            "httpMethod": "GET",
            "resource": "/meetings/{year}",
            "pathParameters": {"year": "2025"},
        }
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 200
        assert json.loads(result["body"]) == ["01-29", "03-19"]

    def test_get_specific_meeting(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.get_item.return_value = {
            "Item": {"year": "2025", "month_date": "01-29", "data": "test"}
        }

        event = {
            "httpMethod": "GET",
            "resource": "/meetings/{year}/{month_date}",
            "pathParameters": {"year": "2025", "month_date": "01-29"},
        }
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 200

    def test_get_meeting_not_found(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.get_item.return_value = {}

        event = {
            "httpMethod": "GET",
            "resource": "/meetings/{year}/{month_date}",
            "pathParameters": {"year": "2099", "month_date": "01-01"},
        }
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 404

    def test_get_opening_statement(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.get_item.return_value = {
            "Item": {"opening_statement_transcript": "test transcript"}
        }

        event = {
            "httpMethod": "GET",
            "resource": "/meetings/{year}/{month_date}/opening_statement_transcript",
            "pathParameters": {"year": "2025", "month_date": "01-29"},
        }
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 200

    def test_invalid_route(self, mock_dynamodb):
        _, module = mock_dynamodb
        event = {"httpMethod": "GET", "resource": "/invalid"}
        result = module.lambda_handler(event, None)

        assert result["statusCode"] == 400


class TestCORS:
    def test_cors_headers_present(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.scan.return_value = {"Items": []}

        event = {"httpMethod": "GET", "resource": "/meetings/years"}
        result = module.lambda_handler(event, None)

        assert result["headers"]["Access-Control-Allow-Origin"] == "https://fomcdebriefs.netlify.app"
        assert "GET" in result["headers"]["Access-Control-Allow-Methods"]

    def test_cors_on_error_response(self, mock_dynamodb):
        _, module = mock_dynamodb
        event = {"httpMethod": "GET", "resource": "/invalid"}
        result = module.lambda_handler(event, None)

        assert result["headers"]["Access-Control-Allow-Origin"] == "https://fomcdebriefs.netlify.app"

    def test_cors_on_404(self, mock_dynamodb):
        mock_table, module = mock_dynamodb
        mock_table.get_item.return_value = {}

        event = {
            "httpMethod": "GET",
            "resource": "/meetings/{year}/{month_date}",
            "pathParameters": {"year": "2099", "month_date": "01-01"},
        }
        result = module.lambda_handler(event, None)

        assert result["headers"]["Access-Control-Allow-Origin"] == "https://fomcdebriefs.netlify.app"
