"""Gold layer: chunk silver documents and embed each chunk with Amazon Bedrock.

Gold is the only layer the retrieval Lambda ever touches. Its schema --
chunk_id, doc_path, chunk_text, embedding -- is the contract between the
pipeline and the agent. Only chunks whose content has changed need to be
re-embedded on a given run, keeping Bedrock cost near zero after the initial
load (the hash-gating itself lives upstream in the ingestion schedule; this
module always (re)embeds the chunks it is given).
"""
from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import logging
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.config import (
    GOLD_PREFIX,
    SILVER_PREFIX,
    get_aws_region,
    get_bedrock_model_id,
    get_bronze_bucket,
    get_chunk_overlap,
    get_chunk_size,
)

logger = logging.getLogger(__name__)

_RETRY_AWS = retry(
    retry=retry_if_exception_type((ClientError, BotoCoreError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)

_MIN_CHUNK_LENGTH = 200
_MAX_EMBEDDING_INPUT_CHARS = 8000


def chunk_text(text: str, size: int, overlap: int) -> list[tuple[int, str]]:
    """Split ``text`` into overlapping ``(offset, chunk)`` pairs.

    Chunks of ``_MIN_CHUNK_LENGTH`` characters or fewer are dropped -- they
    carry too little signal to embed usefully. Empty input returns an empty
    list.
    """
    chunks: list[tuple[int, str]] = []
    start = 0
    while start < len(text):
        end = start + size
        chunk = text[start:end]
        if len(chunk) > _MIN_CHUNK_LENGTH:
            chunks.append((start, chunk))
        start += size - overlap
    return chunks


@_RETRY_AWS
def get_embedding(bedrock_client: Any, text: str, model_id: str) -> list[float]:
    """Embed ``text`` with the configured Bedrock Titan model."""
    body = json.dumps({"inputText": text[:_MAX_EMBEDDING_INPUT_CHARS]})
    response = bedrock_client.invoke_model(
        modelId=model_id, body=body, contentType="application/json", accept="application/json"
    )
    result = json.loads(response["body"].read())
    return result["embedding"]


@_RETRY_AWS
def get_latest_silver_key(s3_client: Any, bucket: str) -> str:
    """Return the most recent silver object key.

    Raises:
        ValueError: if no silver files exist yet.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=SILVER_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".jsonl.gz")
    ]
    if not keys:
        raise ValueError(f"No silver files found under s3://{bucket}/{SILVER_PREFIX}")
    return sorted(keys)[-1]


@_RETRY_AWS
def load_silver_records(s3_client: Any, bucket: str, key: str) -> list[dict[str, Any]]:
    """Download and decode a gzipped JSONL silver object."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    return [json.loads(line) for line in raw.strip().split("\n")]


def build_gold_records(
    silver_records: list[dict[str, Any]],
    bedrock_client: Any,
    model_id: str,
    chunk_size: int,
    chunk_overlap: int,
    chunked_at: str,
) -> list[dict[str, Any]]:
    """Chunk each silver document and embed every chunk into a gold record."""
    gold_records: list[dict[str, Any]] = []
    for record in silver_records:
        body = record.get("body_for_embed", "")
        if not body:
            continue
        chunks = chunk_text(body, chunk_size, chunk_overlap)
        logger.info("%s -> %d chunks", record["doc_path"], len(chunks))
        for offset, text in chunks:
            chunk_id = hashlib.sha256(f"{record['doc_path']}::{offset}".encode()).hexdigest()[:16]
            embedding = get_embedding(bedrock_client, text, model_id)
            gold_records.append(
                {
                    "chunk_id": chunk_id,
                    "doc_path": record["doc_path"],
                    "title": record["title"],
                    "summary": record["summary"],
                    "chunk_offset": offset,
                    "chunk_text": text,
                    "embedding": embedding,
                    "embedding_model": model_id,
                    "content_hash": record["content_hash"],
                    "_commit": record["_commit"],
                    "_chunked_at": chunked_at,
                }
            )
    return gold_records


@_RETRY_AWS
def write_gold_records(
    s3_client: Any, bucket: str, records: list[dict[str, Any]], ingestion_date: str
) -> str:
    """Gzip-compress gold records as newline-delimited JSON and write them to S3."""
    key = f"{GOLD_PREFIX}ingestion_date={ingestion_date}/gold_docs_chunks.jsonl.gz"
    body = "\n".join(json.dumps(record) for record in records).encode("utf-8")
    compressed = gzip.compress(body)

    try:
        s3_client.put_object(
            Bucket=bucket, Key=key, Body=compressed, ContentType="application/gzip"
        )
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"Failed to write gold records to s3://{bucket}/{key}: {exc}") from exc

    logger.info(
        "Written %d chunks to s3://%s/%s (%.1f KB)",
        len(records),
        bucket,
        key,
        len(compressed) / 1024,
    )
    return key


def run() -> str:
    """Entry point: chunk and embed the latest silver file into a gold file.

    Returns:
        The S3 key the gold file was written to.
    """
    bucket = get_bronze_bucket()
    region = get_aws_region()
    model_id = get_bedrock_model_id()
    chunk_size = get_chunk_size()
    chunk_overlap = get_chunk_overlap()

    s3 = boto3.client("s3", region_name=region)
    bedrock = boto3.client("bedrock-runtime", region_name=region)

    today = datetime.date.today().isoformat()
    chunked_at = datetime.datetime.now(datetime.UTC).isoformat()

    silver_key = get_latest_silver_key(s3, bucket)
    logger.info("Reading %s", silver_key)
    silver_records = load_silver_records(s3, bucket, silver_key)
    logger.info("Silver records: %d", len(silver_records))

    gold_records = build_gold_records(
        silver_records, bedrock, model_id, chunk_size, chunk_overlap, chunked_at
    )
    logger.info("Total gold chunks: %d", len(gold_records))

    return write_gold_records(s3, bucket, gold_records, today)


if __name__ == "__main__":
    from pipeline.config import configure_logging

    configure_logging()
    run()
