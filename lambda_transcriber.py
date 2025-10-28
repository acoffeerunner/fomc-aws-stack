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


class SpeakerWithText(BaseModel):
    """Represents a speaker in the transcript."""

    speaker: SpeakerInfo = Field(..., description="information about the speaker")
    text: str = Field(..., description="transcribed dialog")


class transcriptResponse(BaseModel):
    transcript: list[SpeakerWithText] = Field(
        ..., description="transcript of FOMC press conference"
    )


class FullResponse(BaseModel):
    opening_statement_transcript: SpeakerWithText = Field(
        ..., description="transcript of Federal Reserve Chair's opening statement"
    )
    press_q_and_a_transcript: list[SpeakerWithText] = Field(
        ..., description="transcript of press Q&A"
    )


# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize AWS clients
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    Transcribe FOMC press conference video using Gemini.
    """
    logger.info("=== FOMC Transcriber Lambda Started ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Request ID: {context.aws_request_id}")

    video_id = event.get("video_id")
    logger.info(f"Video ID: {video_id}")
    date_dir = event.get("date_dir")
    logger.info(f"Date Directory: {date_dir}")

    logger.info("=== Retrieving Keys ===")
    keys = get_keys()
    logger.info("=== Keys Retrieved ===")

    logger.info("=== Getting checks for transcript generation ===")
    s_timestamp, e_text = get_checks(video_id, keys)
    logger.info(f"Starting timestamp: {s_timestamp}")
    if s_timestamp < 0:
        s_timestamp = 0
    logger.info(f"Ending text: {e_text}")
    logger.info("=== Got checks for transcript generation ===")

    logger.info("=== Generating transcript ===")
    get_verbatim_transcript_from_video(video_id, s_timestamp, e_text, keys, date_dir)
    logger.info("=== Transcript generated ===")

    return {"date_dir": date_dir, "status": "transcript_saved"}


def get_checks(video_id, keys):
    # Get starting point
    youtube = build("youtube", "v3", developerKey=keys["yt_api_key"])
    logger.info(f"Video ID: {video_id}")
    request_s_timestamp = youtube.videos().list(
        part="snippet,contentDetails,statistics", id=video_id
    )
    response_s_timestamp = request_s_timestamp.execute()
    logger.info(f"Response: {response_s_timestamp}")
    total_duration = parse_duration(
        response_s_timestamp["items"][0]["contentDetails"]["duration"]
    ).seconds
    published_at = isoparse(
        response_s_timestamp["items"][0]["snippet"]["publishedAt"]
    ).astimezone(ZoneInfo("America/New_York"))
    conference_start = datetime.now(tz=ZoneInfo(key="America/New_York")).replace(
        hour=14, minute=30, second=0, microsecond=0
    )
    s_timestamp = total_duration - (published_at - conference_start).seconds
    logger.info(f"Starting timestamp: {s_timestamp}")

    # Ending text for transcript completion checks
    client = genai.Client(api_key=keys["ai_key"])
    input_content = [
        types.Part(
            file_data=types.FileData(
                mime_type="video/*",
                file_uri=f"https://www.youtube.com/watch?v={video_id}",
            ),
            video_metadata=types.VideoMetadata(
                start_offset=f"{str(total_duration - 200)}s",
            ),
        )
    ]
    prompt = "Watch the video and transcribe only the final sentence spoken by the Chair, exactly as said, including any words of thanks to the attending public. Output only the verbatim transcription, with no additional text or formatting."

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=input_content + [types.Part(text=prompt)],
        config=types.GenerateContentConfig(
            media_resolution="MEDIA_RESOLUTION_LOW",
        ),
    )

    logger.info(f"Response ending text: {response.text}")
    e_text = response.text.split(". ")[-1]
    e_text = re.sub(r"[^a-zA-Z0-9\s]", "", e_text)
    logger.info(f"Ending text: {e_text}")

    return s_timestamp, e_text


def get_verbatim_transcript_from_video(video_id, s_timestamp, end_text, keys, date_dir):
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

    client = genai.Client(api_key=keys["ai_key"])
    input_content = [
        types.Part(
            file_data=types.FileData(
                mime_type="video/*",
                file_uri=f"https://www.youtube.com/watch?v={video_id}",
            ),
            video_metadata=types.VideoMetadata(
                start_offset=f"{str(s_timestamp)}s",
            ),
        )
    ]

    while True:
        response_transcribed_plaintext = ""
        try:
            transcript_prompt = "Transcribe the full opening statement and subsequent Press Q&A. For each speaker, identify their name, role, organization, and the verbatim text of their speech. Present the output as a JSON array of objects, with each object matching the provided schema. WHATEVER HAPPENS, DO NOT REPEAT YOURSELF. PROVIDE THE ANSWER ONCE."
            logger.info("Attempting to fetch full transcript...")
            response_transcribed = client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents=input_content + [types.Part(text=transcript_prompt)],
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_budget=-1,
                    ),
                    response_mime_type="application/json",
                    response_schema=transcriptResponse,
                    system_instruction=[types.Part(text=system_instruction)],
                    media_resolution="MEDIA_RESOLUTION_LOW",
                ),
            )

            chunks = []

            for chunk in response_transcribed:
                chunks.append(chunk.text) if chunk.text else ""

            response_transcribed_plaintext = "".join(chunks)

            full_response_data = transcriptResponse.model_validate_json(
                response_transcribed_plaintext
            )

            if re.sub(
                r"[^a-zA-Z0-9\s]", "", full_response_data.transcript[-1].text.lower()
            ).endswith(end_text.lower()) or (
                re.sub(
                    r"[^a-zA-Z0-9\s]",
                    "",
                    full_response_data.transcript[-1].text.lower(),
                ).endswith(" ".join(end_text.split()[1:]).lower())
                and len(end_text.split()) - 1 > 1
            ):
                logger.info("Successfully fetched full transcript.")

                transcript_segmented = FullResponse

                # Find where the opening statement ends and Q&A begins
                opening_statement_entries = []
                press_qa_start_index = 0

                # Concatenate all non-journalist entries at the beginning as opening statement
                for i, entry in enumerate(full_response_data.transcript):
                    # Check if this entry is from a journalist
                    is_journalist = (
                        entry.speaker.role == "Journalist"
                        or "journalist" in entry.speaker.role.lower()
                    )

                    if not is_journalist:
                        opening_statement_entries.append(entry)
                    else:
                        # First journalist entry marks start of Q&A
                        press_qa_start_index = i
                        break

                # If no journalist found, assume everything is opening statement
                if press_qa_start_index == 0:
                    press_qa_start_index = len(opening_statement_entries)

                # Combine all opening statement entries into one
                if opening_statement_entries:
                    combined_opening_text = " ".join(
                        [entry.text for entry in opening_statement_entries]
                    )

                    # Create a single combined opening statement entry
                    combined_opening = type(opening_statement_entries[0])(
                        speaker=opening_statement_entries[0].speaker,
                        text=combined_opening_text,
                    )
                    transcript_segmented.opening_statement_transcript = [
                        combined_opening
                    ]
                else:
                    transcript_segmented.opening_statement_transcript = [
                        full_response_data.transcript[0]
                    ]

                transcript_segmented.press_q_and_a_transcript = (
                    full_response_data.transcript[press_qa_start_index:]
                )

                logger.info("Opening Statement Summary (Pydantic validated JSON):")
                opening_statement_dicts = [
                    item.model_dump()
                    for item in transcript_segmented.opening_statement_transcript
                ]
                logger.info(json.dumps(opening_statement_dicts, indent=2))

                logger.info("Press Q&A Summary (Pydantic validated JSON):")
                qa_dicts = [
                    item.model_dump()
                    for item in transcript_segmented.press_q_and_a_transcript
                ]
                logger.info(json.dumps(qa_dicts, indent=2))

                transcript_json = {"transcript": []}
                transcript_json["transcript"].extend(opening_statement_dicts)
                transcript_json["transcript"].extend(qa_dicts)

                put_in_s3(transcript_json, date_dir + "/output_transcript.json")

                return
            else:
                logger.info(
                    "Transcript does not end with expected text. Retrying in 60 seconds..."
                )

                logger.info(response_transcribed_plaintext)

                raise ValueError("Incomplete transcript")

        except Exception as e:
            logger.info(f"Error: {e}")
            logger.info("Raw response text:")
            logger.info(response_transcribed_plaintext)
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
