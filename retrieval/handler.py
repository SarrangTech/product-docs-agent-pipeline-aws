"""AWS Lambda handler for the search-docs semantic retrieval tool.

The input/output schema below is the contract between this pipeline and any
agent that calls it (Claude tool use, OpenAI function calling, LangGraph,
LlamaIndex, ...) -- keep it stable. Deployed as the ``search-docs`` Lambda
function; do not rename it.
"""
from __future__ import annotations

import gzip
import json
import logging
import time
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.config import GOLD_PREFIX, get_aws_region, get_bedrock_model_id, get_bronze_bucket
from retrieval.search import rank_chunks

logger = logging.getLogger(__name__)

_RETRY_AWS = retry(
    retry=retry_if_exception_type((ClientError, BotoCoreError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)

# Cache chunks across warm Lambda invocations to avoid re-reading S3 on every call.
_chunks_cache: list[dict[str, Any]] | None = None


@_RETRY_AWS
def load_gold_chunks(s3_client: Any, bucket: str) -> list[dict[str, Any]]:
    """Load the most recent gold chunk file from S3.

    Raises:
        ValueError: if no gold files exist yet.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=GOLD_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".jsonl.gz")
    ]
    if not keys:
        raise ValueError(f"No gold files found under s3://{bucket}/{GOLD_PREFIX}")
    latest = sorted(keys)[-1]
    obj = s3_client.get_object(Bucket=bucket, Key=latest)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    return [json.loads(line) for line in raw.strip().split("\n")]


@_RETRY_AWS
def get_query_embedding(bedrock_client: Any, text: str, model_id: str) -> list[float]:
    """Embed the incoming query with the configured Bedrock Titan model."""
    body = json.dumps({"inputText": text[:8000]})
    response = bedrock_client.invoke_model(
        modelId=model_id, body=body, contentType="application/json", accept="application/json"
    )
    return json.loads(response["body"].read())["embedding"]


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Entry point invoked by AWS Lambda for the search-docs function.

    Expects ``event["body"]`` to be a JSON string (or dict) with a ``query``
    field and optional ``top_k`` and ``section`` fields. Returns an API
    Gateway-style response with a JSON body containing ranked chunks.
    """
    global _chunks_cache

    body = event.get("body", "{}")
    if isinstance(body, str):
        body = json.loads(body)

    query = body.get("query", "").strip()
    top_k = int(body.get("top_k", 5))
    section = body.get("section", None)

    if not query or len(query) < 2:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "query must be at least 2 characters"}),
        }

    top_k = min(max(top_k, 1), 20)

    bucket = get_bronze_bucket()
    region = get_aws_region()
    model_id = get_bedrock_model_id()

    s3 = boto3.client("s3", region_name=region)
    bedrock = boto3.client("bedrock-runtime", region_name=region)

    t0 = time.monotonic()

    if _chunks_cache is None:
        logger.info("Loading gold chunks from S3")
        _chunks_cache = load_gold_chunks(s3, bucket)
    chunks = _chunks_cache

    if section:
        chunks = [c for c in chunks if c["doc_path"].startswith(section)]

    query_embedding = get_query_embedding(bedrock, query, model_id)
    results = rank_chunks(query_embedding, chunks, top_k)

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "chunks": results,
                "total_chunks_searched": len(chunks),
                "embed_and_search_ms": elapsed_ms,
                "query": query,
            }
        ),
    }
