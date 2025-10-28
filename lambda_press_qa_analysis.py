import json
import logging
import os
import re
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
from shared_utils import get_keys
from zoneinfo import ZoneInfo


class SpeakerInfo(BaseModel):
    """Represents a reporter in the transcript."""

    name: str = Field(..., description="name of speaker")
    role: str = Field(..., description="role of speaker at the organization")
    organization: str = Field(
        ..., description="organization that the speaker belongs to"
    )


class Theme(BaseModel):
    theme: str = Field(..., description="identified theme")
    summary: str = Field(..., description="summary of the theme")


class PressQATheme(Theme):
    reporters: list[SpeakerInfo] = Field(
        ..., description="list of reporters who asked questions related to this theme"
    )


class MostProfoundQuestion(BaseModel):
    question: str = Field(
        ..., description="the most profound question asked during the press Q&A session"
    )
    answer: str = Field(
        ..., description="the Fed Chair's answer to the most profound question"
    )
    reporter: SpeakerInfo = Field(
        ...,
        description="information about the reporter who asked the most profound question",
    )
    reasoning: str = Field(
        ..., description="reasoning behind why this question was the most profound"
    )


class PressQATranscriptAnalysis(BaseModel):
    press_q_and_a_themes: list[PressQATheme] = Field(
        ...,
        description="list of identified themes and their summaries in the press Q&A session",
    )
    most_profound_question: MostProfoundQuestion = Field(
        ...,
        description="the most profound question asked during the press Q&A session along with the Fed Chair's answer and reasoning",
    )


# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    logger.info("=== FOMC Press Q&A Analysis Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    date_dir = event.get("date_dir")
    logger.info(f"date_dir: {date_dir}")

    logger.info("=== Retrieving Keys ===")
    keys = get_keys()
    logger.info("=== Keys Retrieved ===")

    logger.info("=== Getting Press Q&A Transcript ===")
    press_qa_transcript = get_press_qa_transcript(date_dir)
    logger.info("=== Press Q&A Transcript Retrieved ===")
    logger.info(f"{press_qa_transcript}")

    logger.info("=== Getting Press Q&A Analysis ===")
    get_press_qa_analysis(press_qa_transcript, keys, date_dir)
    logger.info("=== Press Q&A Analysis Retrieved ===")

    logger.info("=== FOMC Press Q&A Analysis Lambda Completed ===")
    return {"date_dir": date_dir, "status": "analysis_complete"}


def get_press_qa_transcript(date_dir):
    logger.info("=== Retrieving Opening Statement ===")
    bucket_name = os.environ.get("S3_BUCKET", "fomc-gists-s3")
    logger.info("=== Transcript Retrieved ===")

    try:
        response = s3_client.get_object(
            Bucket=bucket_name, Key=date_dir + "/output_transcript.json"
        )
        object_content = response["Body"].read().decode("utf-8")
        data = json.loads(object_content)
        opening_statement = data["transcript"][1:]
        return opening_statement
    except s3_client.exceptions.NoSuchKey:
        print(f"Object not found in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_press_qa_analysis(press_q_and_a_transcript, keys, date_dir):
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
    while True:
        try:
            press_qa_plaintext = ""
            thematic_summary_prompt = f"""Provide a thematic summary of the Press Q&A session that follows the Chair's opening statement. Identify the main themes discussed during the Q&A, such as inflation, interest rates, economic outlook, etc. For each theme, summarize the key questions asked by reporters and the Chair's responses. I also require you to provide me with the most profound question that a reporter asked the Chair and your reasoning for choosing the same question. Highlight any significant insights that may not have been included in the opening statement. WHATEVER HAPPENS, DO NOT REPEAT YOURSELF. PROVIDE THE ANSWER ONCE. Given is the transcript for the press Q&A session in JSON: {press_q_and_a_transcript}"""

            response_thematic_summary = client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents=types.Part(text=thematic_summary_prompt),
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_budget=-1,
                    ),
                    response_mime_type="application/json",
                    response_schema=PressQATranscriptAnalysis,
                    system_instruction=[types.Part(text=system_instruction)],
                ),
            )
            chunks = []

            for chunk in response_thematic_summary:
                chunks.append(chunk.text) if chunk.text else ""

            press_qa_plaintext = "".join(chunks)

            # Validate and parse the JSON response
            press_qa_analysis_data = json.loads(press_qa_plaintext)
            PressQATranscriptAnalysis.model_validate(press_qa_analysis_data)
            logger.info(f"Press Q&A Analysis JSON Response: {press_qa_plaintext}")
            put_in_s3(
                press_qa_analysis_data, f"{date_dir}/output_press_qa_analysis.json"
            )

            return

        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error("Raw response text:")
            logger.error(press_qa_plaintext)
            sleep(60)


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
