import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import boto3
import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
from zoneinfo import ZoneInfo

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
events_client = boto3.client("events")
lambda_client = boto3.client("lambda")


def lambda_handler(event, context):
    """
    Schedule FOMC meeting monitoring by creating EventBridge rule for the next meeting.
    """
    logger.info("=== FOMC Scheduler Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    try:
        # Find the next FOMC meeting date
        next_meeting_date = get_next_fomc_meeting_date()

        if not next_meeting_date:
            logger.error("Could not determine next FOMC meeting date")
            return {
                "statusCode": 500,
                "body": json.dumps("Could not determine next FOMC meeting date"),
            }

        logger.info(f"Next FOMC meeting date: {next_meeting_date}")

        # Create EventBridge rule to trigger livestream monitor
        rule_name = f"fomc-livestream-monitor-{next_meeting_date.strftime('%Y-%m-%d')}"

        # Schedule for 3:10 PM Eastern on the meeting day
        schedule_time = next_meeting_date.replace(
            hour=15, minute=10, second=0, microsecond=0
        )

        # Convert to cron expression (EventBridge uses UTC)
        utc_time = schedule_time.astimezone(ZoneInfo("UTC"))
        cron_expression = f"cron({utc_time.minute} {utc_time.hour} {utc_time.day} {utc_time.month} ? {utc_time.year})"

        logger.info(f"Creating EventBridge rule: {rule_name}")
        logger.info(f"Cron expression: {cron_expression}")

        # Create the rule on default event bus (scheduled rules only work on default bus)
        events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expression,
            Description=f"Trigger FOMC livestream monitoring for meeting on {next_meeting_date.date()}",
            State="ENABLED",
        )

        # Add target (the livestream monitor Lambda)
        events_client.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "1",
                    "Arn": f"arn:aws:lambda:{context.invoked_function_arn.split(':')[3]}:{context.invoked_function_arn.split(':')[4]}:function:fomc-livestream-monitor",
                    "Input": json.dumps(
                        {
                            "scheduled_trigger": True,
                            "meeting_date": next_meeting_date.isoformat(),
                            "rule_name": rule_name,
                            "max_attempts": 3,
                            "attempt_interval_minutes": 10.5,
                        }
                    ),
                }
            ],
        )

        # Grant EventBridge permission to invoke the Lambda
        try:
            lambda_client.add_permission(
                FunctionName="fomc-livestream-monitor",
                StatementId=f"allow-eventbridge-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{context.invoked_function_arn.split(':')[3]}:{context.invoked_function_arn.split(':')[4]}:rule/{rule_name}",
            )
        except lambda_client.exceptions.ResourceConflictException:
            # Permission already exists
            logger.info("EventBridge permission already exists for livestream monitor")

        logger.info(f"Successfully created EventBridge rule: {rule_name}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "FOMC meeting monitoring scheduled successfully",
                    "meeting_date": next_meeting_date.isoformat(),
                    "rule_name": rule_name,
                    "schedule_expression": cron_expression,
                }
            ),
        }

    except Exception as e:
        logger.error(f"Error in FOMC scheduler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error scheduling FOMC meeting monitoring: {str(e)}"),
        }


def get_next_fomc_meeting_date():
    """
    Get the next FOMC meeting date by scraping the Federal Reserve's official calendar.
    """
    try:
        logger.info("Fetching FOMC meeting dates from Federal Reserve website...")

        calendar_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        eastern = ZoneInfo("America/New_York")

        # Get current and next year meetings
        current_year = datetime.now().year
        meetings = []

        for year in [current_year, current_year + 1]:
            year_meetings = fetch_fomc_meetings_for_year(year, calendar_url, eastern)
            meetings.extend(year_meetings)

        if not meetings:
            logger.error("No FOMC meetings found from Federal Reserve website")
            return None

        # Find the next upcoming meeting
        now = datetime.now(eastern)

        for meeting in meetings:
            meeting_datetime = meeting["press_conference_time"]
            if meeting_datetime > now:
                logger.info(
                    f"Next FOMC meeting: {meeting['date']} at {meeting_datetime}"
                )
                return meeting_datetime

        logger.warning("No future FOMC meetings found")
        return None

    except Exception as e:
        logger.error(f"Error fetching FOMC calendar from Fed website: {e}")
        logger.info("Falling back to hardcoded dates...")
        return get_fallback_fomc_date()


def fetch_fomc_meetings_for_year(
    year: int, calendar_url: str, eastern_tz
) -> List[Dict]:
    """Fetch FOMC meeting dates for a specific year from Fed website"""
    try:
        response = requests.get(calendar_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        meetings = []

        # Month mapping
        month_map = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }

        # Find the year heading
        year_heading = None
        for h4 in soup.find_all("h4"):
            year_text = h4.get_text(strip=True)
            if "FOMC Meetings" in year_text and str(year) in year_text:
                year_heading = h4
                break

        if not year_heading:
            logger.warning(f"No FOMC meetings section found for year {year}")
            return meetings

        # Look for meeting information after the year heading
        for sibling in year_heading.find_all_next("div", class_="row"):
            # Stop if we hit another year's heading
            if sibling.find_previous("h4") != year_heading:
                break

            text = sibling.get_text(separator=" ", strip=True)
            if not text:
                continue

            parts = text.split()
            if len(parts) < 2:
                continue

            month_name = parts[0]
            date_part = parts[1]

            # Skip if this doesn't look like a date
            if not any(char.isdigit() for char in date_part):
                continue

            # Check for SEP announcement (asterisk)
            has_SEP = "*" in date_part

            # Parse the dates
            parsed_dates = parse_date_from_parts(month_name, date_part, year, month_map)

            # For 2-day meetings, only keep the last day (decision announcement day)
            if len(parsed_dates) > 1:
                parsed_dates = [max(parsed_dates)]

            for meeting_date in parsed_dates:
                # Press conference is at 2:30 PM ET
                press_conf_time = meeting_date.replace(
                    hour=14, minute=30, second=0, microsecond=0, tzinfo=eastern_tz
                )

                meeting_info = {
                    "date": meeting_date.date(),
                    "year": year,
                    "month": month_name,
                    "has_SEP": has_SEP,
                    "press_conference_time": press_conf_time,
                }
                meetings.append(meeting_info)

        logger.info(f"Found {len(meetings)} FOMC meetings for {year}")
        return meetings

    except Exception as e:
        logger.error(f"Error parsing FOMC calendar for year {year}: {e}")
        return []


def parse_date_from_parts(
    month_name: str, date_part: str, year: int, month_map: dict
) -> List[datetime]:
    """Parse date from month name and date part"""
    dates = []

    if month_name not in month_map:
        return dates

    month_num = month_map[month_name]

    try:
        # Clean date part - remove asterisks and other non-numeric chars except dash
        date_clean = re.sub(r"[^0-9\-]", "", date_part)

        if "-" in date_clean:
            # Handle ranges like "17-18"
            start_day, end_day = date_clean.split("-")
            if start_day.isdigit() and end_day.isdigit():
                start_date = datetime(year, month_num, int(start_day))
                end_date = datetime(year, month_num, int(end_day))
                dates.append(start_date)
                if start_date != end_date:
                    dates.append(end_date)
        else:
            # Single date
            if date_clean.isdigit():
                single_date = datetime(year, month_num, int(date_clean))
                dates.append(single_date)

    except (ValueError, TypeError) as e:
        logger.error(f"Error parsing date '{date_part}' for {month_name} {year}: {e}")

    return dates


def get_fallback_fomc_date():
    """Fallback to hardcoded dates if Fed website is unavailable"""
    logger.info("Using fallback hardcoded FOMC dates")
    eastern = ZoneInfo("America/New_York")

    # Current known dates (update periodically)
    fallback_dates = [
        "2024-12-18",
        "2025-01-29",
        "2025-03-19",
        "2025-04-30",
        "2025-06-11",
        "2025-07-30",
        "2025-09-17",
        "2025-11-05",
        "2025-12-17",
    ]

    now = datetime.now(eastern)
    for date_str in fallback_dates:
        meeting_date = datetime.fromisoformat(date_str).replace(
            hour=14, minute=30, second=0, microsecond=0, tzinfo=eastern
        )
        if meeting_date > now:
            return meeting_date

    return None
