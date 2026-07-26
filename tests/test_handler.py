"""Unit tests for the retrieval Lambda handler (search-docs)."""
import gzip
import json
from io import BytesIO
from typing import Any

import boto3
import pytest
from moto import mock_aws

import retrieval.handler as handler_module
from pipeline.config import GOLD_PREFIX
from retrieval.handler import lambda_handler

BUCKET = "test-bronze-bucket"


class _StubBedrock:
    """Fake Bedrock runtime client that returns a fixed query embedding."""

    def invoke_model(
        self, modelId: str, body: str, contentType: str, accept: str
    ) -> dict[str, Any]:
        payload = json.dumps({"embedding": [1.0, 0.0]}).encode("utf-8")
        return {"body": BytesIO(payload)}


@pytest.fixture(autouse=True)
def _reset_chunks_cache() -> Any:
    handler_module._chunks_cache = None
    yield
    handler_module._chunks_cache = None


def test_lambda_handler_rejects_query_shorter_than_two_characters() -> None:
    response = lambda_handler({"body": json.dumps({"query": "a"})}, None)
    assert response["statusCode"] == 400


@mock_aws
def test_lambda_handler_returns_ranked_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    gold_records = [
        {
            "chunk_id": "1",
            "doc_path": "a.md",
            "title": "A",
            "summary": None,
            "chunk_text": "hello",
            "embedding": [1.0, 0.0],
        },
        {
            "chunk_id": "2",
            "doc_path": "b.md",
            "title": "B",
            "summary": None,
            "chunk_text": "world",
            "embedding": [0.0, 1.0],
        },
    ]
    body = "\n".join(json.dumps(r) for r in gold_records).encode("utf-8")
    key = f"{GOLD_PREFIX}ingestion_date=2026-01-01/gold_docs_chunks.jsonl.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(body))

    real_client = boto3.client

    def fake_client(service_name: str, *args: Any, **kwargs: Any) -> Any:
        if service_name == "bedrock-runtime":
            return _StubBedrock()
        return real_client(service_name, *args, **kwargs)

    monkeypatch.setattr("retrieval.handler.boto3.client", fake_client)

    response = lambda_handler({"body": json.dumps({"query": "hello world", "top_k": 1})}, None)

    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["chunks"][0]["chunk_id"] == "1"
    assert payload["total_chunks_searched"] == 2
