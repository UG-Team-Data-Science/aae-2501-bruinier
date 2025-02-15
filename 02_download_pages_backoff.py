# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boto3",
#     "tqdm",
#     "trafilatura",
#     "warcio",
# ]
# ///
import concurrent.futures
import gzip
import json
import random
import time
from pathlib import Path
from urllib.parse import urlparse
from typing import Iterator, Dict
import trafilatura

import boto3
from botocore.config import Config
from warcio.archiveiterator import ArchiveIterator

STORAGE_VOLUME = Path("/slow-data/p266261/aae-2501-bruinier")
INPUT_DIR = STORAGE_VOLUME.joinpath("filtered_indices_domains")
OUTPUT_DIR = STORAGE_VOLUME.joinpath("content")
OUTPUT_DIR.mkdir(exist_ok=True)

from tqdm import tqdm


class S3RateLimiter:
    def __init__(self, max_requests_per_second=3):
        """Initialize rate limiter with requests per second limit"""
        self.delay = 1.0 / max_requests_per_second
        self.last_request = 0
        self.retries = 0
        self.max_retries = 5

        # Configure boto3 with retries and backoff
        config = Config(retries=dict(max_attempts=10, mode="adaptive"))

        self.s3_client = boto3.client("s3", config=config)

    def wait(self):
        """Wait appropriate time between requests"""
        now = time.time()
        time_passed = now - self.last_request
        if time_passed < self.delay:
            time.sleep(self.delay - time_passed)
        self.last_request = time.time()

    def get_object(self, filename, start_byte, end_byte):
        """Get object from S3 with rate limiting and retries"""
        self.wait()

        try:
            response = self.s3_client.get_object(
                Bucket="commoncrawl",
                Key=filename,
                Range=f"bytes={start_byte}-{end_byte}",
            )
            self.retries = 0
            return response["Body"]

        except Exception as e:
            self.retries += 1
            if self.retries > self.max_retries:
                raise Exception(f"Max retries exceeded for {filename}")

            # Exponential backoff with jitter
            delay = (2**self.retries) + random.uniform(0, 1)
            print(f"Error accessing S3, retrying in {delay:.2f}s: {str(e)}")
            time.sleep(delay)
            return self.get_object(filename, start_byte, end_byte)


def download_page(url_data, s3_limiter):
    """Download a single page from Common Crawl WARC file"""
    try:
        if url_data["status"] != "200":
            return url_data

        # Extract filename from full path
        filename = url_data["filename"]

        start_byte = int(url_data["offset"])
        end_byte = start_byte + int(url_data["length"]) - 1

        # Get object with rate limiting
        response = s3_limiter.get_object(filename, start_byte, end_byte)

        # Parse WARC record
        gzip_file = gzip.GzipFile(fileobj=response)
        for record in ArchiveIterator(gzip_file):
            if record.rec_type == "response":
                content = record.content_stream().read()
                return url_data | {"page_content": content}

    except Exception as e:
        print(f"Error downloading {url_data['url']}: {str(e)}")
    return None


def read_gzipped_jsonlines(input_path):
    """
    Read gzipped jsonlines files from a directory or single file

    Args:
        input_path: Path to gzipped jsonlines file or directory containing .jsonl.gz files

    Yields:
        Dict: Each JSON object from the files
    """
    input_path = Path(input_path)

    # Handle both single file and directory
    if input_path.is_file():
        files = [input_path]
    else:
        files = list(input_path.glob("*.json.gz"))

    for file in files:
        with gzip.open(file, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    yield json.loads(line.strip())
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON from {file}: {e}")
                    continue


def write_jsonl_gz_buffered(
    records: Iterator[Dict], output_path: str, buffer_size: int = 1000
) -> None:
    """
    Write dictionaries to gzipped jsonlines file with buffering for better performance.

    Args:
        records: Iterator of dictionaries to write
        output_path: Path to output .jsonl.gz file
        buffer_size: Number of records to buffer before writing
    """
    buffer = []
    with gzip.open(output_path, "wt", encoding="utf-8") as f:
        for record in records:
            buffer.append(json.dumps(record, ensure_ascii=False) + "\n")

            if len(buffer) >= buffer_size:
                f.writelines(buffer)
                buffer.clear()

        # Write any remaining records in buffer
        if buffer:
            f.writelines(buffer)


def extract_text(record):
    if "page_content" not in record:
        return record
    text = ""
    try:
        text = trafilatura.extract(
            record["page_content"],
            favor_precision=True,
            include_comments=False,
            deduplicate=True,
        )
    except Exception:
        pass
    del record["page_content"]
    return record | {"text": text}


if __name__ == "__main__":
    records = [rec for rec in read_gzipped_jsonlines(INPUT_DIR)]
    s3_limiter = S3RateLimiter()

    record_data = tqdm(
        (download_page(rec, s3_limiter) for rec in records), total=len(records)
    )

    with_text = (extract_text(rec) for rec in record_data)
    write_jsonl_gz_buffered(with_text, OUTPUT_DIR.joinpath("content.json.gz"), buffer_size=100)
