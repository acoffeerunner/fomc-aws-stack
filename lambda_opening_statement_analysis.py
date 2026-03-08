import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from math import floor
from time import sleep

import boto3
from dateutil.parser import isoparse

# Import your existing classes (these would need to be packaged with the Lambda)
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from isodate import parse_duration
from pydantic import BaseModel, Field
from shared_utils import get_keys, extract_usage_metadata, calculate_cost, build_metadata_record
from zoneinfo import ZoneInfo


class Theme(BaseModel):
    theme: str = Field(..., description="identified theme")
    summary: str = Field(..., description="summary of the theme")


# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    logger.info("=== FOMC Opening Statement Analysis Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    date_dir = event.get("date_dir")
    logger.info(f"date_dir: {date_dir}")

    logger.info("=== Retrieving Keys ===")
    keys = get_keys()
    logger.info("=== Keys Retrieved ===")

    logger.info("=== Getting Opening Statement ===")
    opening_statement = get_opening_statement(date_dir)
    logger.info("=== Opening Statement Retrieved ===")
    logger.info(f"{opening_statement['text']}")

    logger.info("=== Getting Opening Statement Analysis ===")
    get_opening_statement_analysis(opening_statement, keys, date_dir)
    logger.info("=== Opening Statement Analysis Retrieved ===")

    logger.info("=== FOMC Opening Statement Analysis Lambda Completed ===")
    return {"date_dir": date_dir, "status": "analysis_complete"}


def get_opening_statement(date_dir):
    logger.info("=== Retrieving Opening Statement ===")
    bucket_name = os.environ.get("S3_BUCKET", "fomc-gists-s3")
    logger.info("=== Transcript Retrieved ===")

    try:
        response = s3_client.get_object(
            Bucket=bucket_name, Key=date_dir + "/output_transcript.json"
        )
        object_content = response["Body"].read().decode("utf-8")
        data = json.loads(object_content)
        opening_statement = data["transcript"][0]
        return opening_statement
    except s3_client.exceptions.NoSuchKey:
        print(f"Object not found in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def validate_opening_analysis_quality(themes):
    """Validate quality of opening statement analysis. Returns list of warnings."""
    warnings = []
    if len(themes) < 2:
        warnings.append(f"Expected at least 2 themes, got {len(themes)}")
    if themes and themes[0].theme != "Monetary Policy Stance":
        warnings.append(
            f"First theme should be 'Monetary Policy Stance', got '{themes[0].theme}'"
        )
    return warnings


def get_opening_statement_analysis(opening_statement_transcript, keys, date_dir):
    client = genai.Client(api_key=keys["ai_key"])
    system_instruction = """You are an expert financial transcriber and analyst specializing in Federal Open Market Committee (FOMC) press conferences. Your task is to accurately transcribe and structure the content of the provided video segments. For each segment, you will identify all speakers, their full name, their role at their organization, the organization itself, and the verbatim text of their speech. For example, Name: Jerome Powell, Role: Chair, Organization: Federal Reserve. Another example: Name: Chris Rugaber, Role: Journalist, Organization: AP

An FOMC press conference generally follows a predictable two-part structure:

Opening Statement: The conference begins with the Chair of the Federal Reserve (currently Jerome Powell) reading a prepared statement. This statement outlines the FOMC's recent policy decisions, such as changes to interest rates or asset purchases, and provides an overview of the committee's economic outlook.

Press Q&A: After the prepared statement, the floor is opened for questions from journalists. The Fed Chair takes questions from a select group of reporters. During this section, the Chair can provide more context and nuance to the committee's decision, and their answers are heavily scrutinized by financial markets.

In the transcript, ensure that you capture the following:
- To enhance readability, remove filler words (such as 'um', 'uh', and 'you know') and repetitive words or phrases (such as "I I I I", "we're going to we're going to", "we're just we're going to"). Correct any stutters or false starts.
- Expand any contractions (e.g., change "don't" to "do not", "it's" to "it is", etc.) used.
- Change word "percent" to the symbol '%', for example "percent" to "%".
- Change fractions to decimals like "three and a half percent" to "3.5 percent", "4 and a quarter" to "4.25", "4 and a half percent" to "4.5%".
- Maintain the original meaning and tone of the speakers while making these adjustments.

Your output must be a valid JSON array of objects, where each object strictly adheres to the provided schema. Correctly attribute each block of dialogue to the correct speaker, role, and organization."""

    max_retries = 3
    validation_passed_first_try = True

    for attempt in range(1, max_retries + 1):
        try:
            opening_analysis_plaintext = ""
            final_chunk = None
            thematic_summary_prompt = f"""Provide a thematic summary of the opening statement given by the Fed Chair. Divide the summary of the Chair's statement into logical sections based on the topics covered. For each section/topic, summarize the Chair's key points, data cited, and any nuances in their assessment. Include 1 theme "Monetary Policy Stance" (if the Fed is tightening the monetary policy/increasing interest rates or loosening the monetary policy/decreasing interest rates). This theme should be the first in your list of themes. WHATEVER HAPPENS, DO NOT REPEAT YOURSELF. PROVIDE THE ANSWER ONCE. Given below is the transcript for the Fed Chair's transcript: {opening_statement_transcript["text"]}"""

            start_time = time.time()
            response_thematic_summary = client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents=types.Part(text=thematic_summary_prompt),
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_budget=-1,
                    ),
                    response_mime_type="application/json",
                    response_schema=list[Theme],
                    system_instruction=[types.Part(text=system_instruction)],
                ),
            )
            chunks = []

            for chunk in response_thematic_summary:
                final_chunk = chunk
                chunks.append(chunk.text) if chunk.text else ""

            latency = round(time.time() - start_time, 2)

            opening_analysis_plaintext = "".join(chunks)

            # Validate and parse the JSON response
            opening_analysis_data = json.loads(opening_analysis_plaintext)
            opening_analysis_themes = [
                Theme.model_validate(item) for item in opening_analysis_data
            ]

            # Quality validation
            quality_warnings = validate_opening_analysis_quality(opening_analysis_themes)
            if quality_warnings:
                logger.warning(f"Quality warnings: {quality_warnings}")
                if attempt == 1:
                    validation_passed_first_try = False

            # Build and save metadata
            usage = extract_usage_metadata(final_chunk)
            cost = calculate_cost(
                usage["prompt_token_count"],
                usage["candidates_token_count"],
                usage["thoughts_token_count"],
            )
            metadata = build_metadata_record(
                usage_metadata=usage,
                latency_seconds=latency,
                retry_count=attempt - 1,
                validation_passed_first_try=validation_passed_first_try,
                quality_warnings=quality_warnings,
                cost_usd=cost,
            )
            logger.info(f"LLM metadata: {json.dumps(metadata)}")

            logger.info(f"Opening Analysis JSON Response: {opening_analysis_plaintext}")
            put_in_s3(opening_analysis_data, f"{date_dir}/output_opening_analysis.json")
            put_in_s3(metadata, f"{date_dir}/metadata_opening_analysis.json")

            return

        except Exception as e:
            logger.error(f"Attempt {attempt}/{max_retries} failed: {e}")
            logger.error("Raw response text:")
            logger.error(opening_analysis_plaintext)
            if attempt < max_retries:
                sleep(30)

    raise RuntimeError(
        f"Opening statement analysis failed after {max_retries} attempts"
    )


def put_in_s3(data_to_save, file_name):
    # Define S3 bucket and object key (file name)
    bucket_name = os.environ.get("S3_BUCKET", "fomc-gists-s3")

    try:
        # 3. Convert data to JSON string and then bytes
        json_data_bytes = json.dumps(data_to_save).encode("utf-8")

        # 4. Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data_bytes,
            ContentType="application/json",  # Optional: Set content type
        )

        print(f"Successfully uploaded {file_name} to {bucket_name}")
        return {
            "statusCode": 200,
            "body": json.dumps("JSON data uploaded to S3 successfully!"),
        }
    except Exception as e:
        print(f"Error uploading JSON to S3: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error uploading JSON to S3: {str(e)}"),
        }
