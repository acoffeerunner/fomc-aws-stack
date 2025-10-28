"""Tests for lambda_db_transform data reshaping logic."""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_aws():
    """Mock S3 and DynamoDB before import."""
    with patch.dict("os.environ", {"S3_BUCKET": "test-bucket", "DYNAMODB_TABLE": "test-table"}):
        with patch("boto3.client") as mock_s3_client, \
             patch("boto3.resource") as mock_dynamo_resource:
            mock_s3 = MagicMock()
            mock_s3_client.return_value = mock_s3
            mock_table = MagicMock()
            mock_dynamo_resource.return_value.Table.return_value = mock_table

            import importlib
            import lambda_db_transform
            importlib.reload(lambda_db_transform)

            # Re-bind module globals after reload
            lambda_db_transform.s3 = mock_s3
            lambda_db_transform.table = mock_table
            lambda_db_transform.bucket = "test-bucket"

            yield mock_s3, mock_table, lambda_db_transform


class TestDbTransform:
    def _mock_s3_responses(self, mock_s3):
        """Set up S3 get_object to return test data for each file."""
        transcript = {
            "transcript": [
                {"speaker": {"name": "Powell"}, "text": "Opening statement"},
                {"speaker": {"name": "Reporter"}, "text": "Question"},
                {"speaker": {"name": "Powell"}, "text": "Answer"},
            ]
        }
        opening_analysis = [
            {"theme": "Monetary Policy", "summary": "Rates held steady"}
        ]
        press_qa_analysis = {
            "press_q_and_a_themes": [
                {"theme": "Inflation", "summary": "Discussed CPI"}
            ],
            "most_profound_question": {
                "question": "What about inflation?",
                "answer": "We are monitoring.",
                "reporter": {"name": "Reporter"},
                "reasoning": "Core issue",
            },
        }

        def get_object_side_effect(Bucket, Key):
            body = MagicMock()
            if "output_transcript.json" in Key:
                body.read.return_value = json.dumps(transcript).encode()
            elif "output_opening_analysis.json" in Key:
                body.read.return_value = json.dumps(opening_analysis).encode()
            elif "output_press_qa_analysis.json" in Key:
                body.read.return_value = json.dumps(press_qa_analysis).encode()
            return {"Body": body}

        mock_s3.get_object.side_effect = get_object_side_effect
        return transcript, opening_analysis, press_qa_analysis

    def test_handler_returns_correct_format(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        self._mock_s3_responses(mock_s3)

        result = module.lambda_handler({"date_dir": "2025-01-29"}, None)

        assert result["date_dir"] == "2025-01-29"
        assert result["status"] == "transform_complete"

    def test_handler_writes_to_dynamodb(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        self._mock_s3_responses(mock_s3)

        module.lambda_handler({"date_dir": "2025-01-29"}, None)

        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["year"] == "2025"
        assert item["month_date"] == "01-29"

    def test_handler_splits_transcript_correctly(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        transcript, _, _ = self._mock_s3_responses(mock_s3)

        module.lambda_handler({"date_dir": "2025-01-29"}, None)

        item = mock_table.put_item.call_args[1]["Item"]
        # First transcript entry is opening statement
        assert item["opening_statement_transcript"] == transcript["transcript"][0]
        # Rest is press Q&A
        assert item["press_qa_transcript"] == transcript["transcript"][1:]

    def test_handler_includes_analysis_data(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        _, opening_analysis, press_qa_analysis = self._mock_s3_responses(mock_s3)

        module.lambda_handler({"date_dir": "2025-01-29"}, None)

        item = mock_table.put_item.call_args[1]["Item"]
        assert item["opening_statement_analysis"] == opening_analysis
        assert item["press_qa_analysis"] == press_qa_analysis["press_q_and_a_themes"]
        assert item["most_profound_question"] == press_qa_analysis["most_profound_question"]

    def test_handler_parses_date_dir(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        self._mock_s3_responses(mock_s3)

        module.lambda_handler({"date_dir": "2026-03-18"}, None)

        item = mock_table.put_item.call_args[1]["Item"]
        assert item["year"] == "2026"
        assert item["month_date"] == "03-18"

    def test_handler_reads_all_three_files(self, mock_aws):
        mock_s3, mock_table, module = mock_aws
        self._mock_s3_responses(mock_s3)

        module.lambda_handler({"date_dir": "2025-01-29"}, None)

        assert mock_s3.get_object.call_count == 3
        keys_read = [call[1]["Key"] for call in mock_s3.get_object.call_args_list]
        assert "2025-01-29/output_transcript.json" in keys_read
        assert "2025-01-29/output_opening_analysis.json" in keys_read
        assert "2025-01-29/output_press_qa_analysis.json" in keys_read
