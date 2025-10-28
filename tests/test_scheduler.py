"""Tests for lambda_scheduler date parsing and fallback logic."""

import os
from datetime import datetime
from unittest.mock import patch

import pytest

# Set required env vars before importing the module
os.environ.setdefault("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123456789:stateMachine:test")
os.environ.setdefault("EVENTBRIDGE_SF_ROLE_ARN", "arn:aws:iam::123456789:role/test")

from lambda_scheduler import get_fallback_fomc_date, parse_date_from_parts

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


class TestParseDateFromParts:
    def test_single_date(self):
        result = parse_date_from_parts("March", "18", 2026, MONTH_MAP)
        assert result == [datetime(2026, 3, 18)]

    def test_date_range(self):
        result = parse_date_from_parts("January", "27-28", 2026, MONTH_MAP)
        assert result == [datetime(2026, 1, 27), datetime(2026, 1, 28)]

    def test_date_with_asterisk(self):
        """Asterisk denotes SEP meeting — should be stripped."""
        result = parse_date_from_parts("March", "17-18*", 2026, MONTH_MAP)
        assert result == [datetime(2026, 3, 17), datetime(2026, 3, 18)]

    def test_invalid_month(self):
        result = parse_date_from_parts("Smarch", "18", 2026, MONTH_MAP)
        assert result == []

    def test_non_numeric_date(self):
        result = parse_date_from_parts("March", "abc", 2026, MONTH_MAP)
        assert result == []

    def test_invalid_day(self):
        result = parse_date_from_parts("February", "31", 2026, MONTH_MAP)
        assert result == []

    def test_same_day_range(self):
        """A range like '18-18' should return one date."""
        result = parse_date_from_parts("March", "18-18", 2026, MONTH_MAP)
        assert result == [datetime(2026, 3, 18)]


class TestGetFallbackFomcDate:
    @patch("lambda_scheduler.datetime")
    def test_returns_next_future_date(self, mock_dt):
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        mock_dt.now.return_value = datetime(2026, 2, 1, tzinfo=eastern)
        mock_dt.fromisoformat = datetime.fromisoformat
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = get_fallback_fomc_date()
        assert result is not None
        assert result.month == 3
        assert result.day == 18
        assert result.year == 2026

    @patch("lambda_scheduler.datetime")
    def test_returns_none_when_all_dates_past(self, mock_dt):
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        mock_dt.now.return_value = datetime(2027, 1, 1, tzinfo=eastern)
        mock_dt.fromisoformat = datetime.fromisoformat
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = get_fallback_fomc_date()
        assert result is None

    @patch("lambda_scheduler.datetime")
    def test_skips_past_dates(self, mock_dt):
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        # Set "now" to after the first few meetings
        mock_dt.now.return_value = datetime(2026, 5, 1, tzinfo=eastern)
        mock_dt.fromisoformat = datetime.fromisoformat
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = get_fallback_fomc_date()
        assert result is not None
        assert result.month == 6
        assert result.day == 17

    def test_all_dates_are_2026(self):
        """Verify fallback dates were updated to 2026."""
        import inspect
        source = inspect.getsource(get_fallback_fomc_date)
        assert "2025-" not in source
        assert "2024-" not in source
        assert "2026-" in source
