"""Silver layer: parse, clean, and deduplicate bronze documents into one row per doc.

Silver is the layer you debug against when retrieval quality degrades. Code
fences are stripped for embedding, titles are parsed from the first heading,
and records are deduplicated by ``doc_path`` so a document ingested more than
once collapses to its most recently pulled version.
"""
from __future__ import annotations

import datetime
import gzip
import json
import logging
import re
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.config import BRONZE_PREFIX, SILVER_PREFIX, get_aws_region, get_bronze_bucket

logger = logging.getLogger(__name__)

_RETRY_AWS = retry(
    retry=retry_if_exception_type((ClientError, BotoCoreError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)

_TITLE_RE = re.compile(r"^#\s+(.+?)$", re.MULTILINE)
_CODE_FENCE_RE = re.compile(r"```[\s\S]*?```")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BLANK_LINES_RE = re.compile(r"\n{3,}")


def parse_frontmatter(content: str) -> tuple[str | None, str | None]:
    """Extract a title from the first markdown H1 heading, if present.

    Returns:
        A ``(title, summary)`` tuple. Summary extraction is not yet
        implemented and always returns ``None``.
    """
    match = _TITLE_RE.search(content)
    title = match.group(1).strip() if match else None
    return title, None


def clean_body(content: str) -> str:
    """Strip code fences and HTML tags, and collapse blank lines for embedding-ready text."""
    body = _CODE_FENCE_RE.sub("", content)
    body = _HTML_TAG_RE.sub("", body)
    body = _BLANK_LINES_RE.sub("\n\n", body).strip()
    return body


@_RETRY_AWS
def get_latest_bronze_key(s3_client: Any, bucket: str) -> str:
    """Return the most recent bronze object key.

    Raises:
        ValueError: if no bronze files exist yet.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=BRONZE_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".jsonl.gz")
    ]
    if not keys:
        raise ValueError(f"No bronze files found under s3://{bucket}/{BRONZE_PREFIX}")
    return sorted(keys)[-1]


@_RETRY_AWS
def load_bronze_records(s3_client: Any, bucket: str, key: str) -> list[dict[str, Any]]:
    """Download and decode a gzipped JSONL bronze object."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    return [json.loads(line) for line in raw.strip().split("\n")]


def build_silver_records(
    bronze_records: list[dict[str, Any]], refined_at: str
) -> list[dict[str, Any]]:
    """Parse and clean bronze records into silver records, ready for deduplication."""
    silver_records: list[dict[str, Any]] = []
    for record in bronze_records:
        content = record.get("content", "")
        if not content:
            continue
        title, summary = parse_frontmatter(content)
        silver_records.append(
            {
                "doc_path": record["doc_path"],
                "title": title,
                "summary": summary,
                "body_for_embed": clean_body(content),
                "body_full": content,
                "content_hash": record["content_hash"],
                "_commit": record["_commit"],
                "_pulled_at": record["_pulled_at"],
                "_refined_at": refined_at,
            }
        )
    return silver_records


def deduplicate(silver_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the most recently pulled record for each ``doc_path``."""
    latest: dict[str, dict[str, Any]] = {}
    for record in silver_records:
        path = record["doc_path"]
        if path not in latest or record["_pulled_at"] > latest[path]["_pulled_at"]:
            latest[path] = record
    return list(latest.values())


@_RETRY_AWS
def write_silver_records(
    s3_client: Any, bucket: str, records: list[dict[str, Any]], ingestion_date: str
) -> str:
    """Gzip-compress silver records as newline-delimited JSON and write them to S3."""
    key = f"{SILVER_PREFIX}ingestion_date={ingestion_date}/silver_docs.jsonl.gz"
    body = "\n".join(json.dumps(record) for record in records).encode("utf-8")
    compressed = gzip.compress(body)

    s3_client.put_object(Bucket=bucket, Key=key, Body=compressed, ContentType="application/gzip")

    logger.info(
        "Written %d records to s3://%s/%s (%.1f KB)",
        len(records),
        bucket,
        key,
        len(compressed) / 1024,
    )
    return key


def run() -> str:
    """Entry point: refine the latest bronze file into a deduplicated silver file.

    Returns:
        The S3 key the silver file was written to.
    """
    bucket = get_bronze_bucket()
    region = get_aws_region()
    s3 = boto3.client("s3", region_name=region)

    today = datetime.date.today().isoformat()
    refined_at = datetime.datetime.now(datetime.UTC).isoformat()

    bronze_key = get_latest_bronze_key(s3, bucket)
    logger.info("Reading %s", bronze_key)
    bronze_records = load_bronze_records(s3, bucket, bronze_key)
    logger.info("Bronze records: %d", len(bronze_records))

    silver_records = deduplicate(build_silver_records(bronze_records, refined_at))
    logger.info("Silver records after dedup: %d", len(silver_records))

    return write_silver_records(s3, bucket, silver_records, today)


if __name__ == "__main__":
    from pipeline.config import configure_logging

    configure_logging()
    run()
