"""Unit tests for the gold chunking and embedding layer."""
import gzip
import json
from io import BytesIO
from typing import Any

import boto3
import pytest
from moto import mock_aws

from pipeline.config import SILVER_PREFIX
from pipeline.gold.embed import (
    build_gold_records,
    chunk_text,
    get_latest_silver_key,
    load_silver_records,
    run,
    write_gold_records,
)

BUCKET = "test-bronze-bucket"


class _StubBedrock:
    """Fake Bedrock runtime client that returns a fixed embedding."""

    def invoke_model(
        self, modelId: str, body: str, contentType: str, accept: str
    ) -> dict[str, Any]:
        payload = json.dumps({"embedding": [0.1, 0.2, 0.3]}).encode("utf-8")
        return {"body": BytesIO(payload)}


def test_chunk_text_produces_correct_number_of_chunks() -> None:
    text = "x" * 5000
    chunks = chunk_text(text, size=2000, overlap=200)
    assert [offset for offset, _ in chunks] == [0, 1800, 3600]


def test_chunk_text_overlap_between_consecutive_chunks_is_correct() -> None:
    text = "x" * 5000
    chunks = chunk_text(text, size=2000, overlap=200)
    first_end = chunks[0][0] + len(chunks[0][1])
    second_start = chunks[1][0]
    assert first_end - second_start == 200


def test_chunk_text_handles_empty_content_gracefully() -> None:
    assert chunk_text("", size=2000, overlap=200) == []


def test_chunk_text_drops_chunks_below_minimum_length() -> None:
    text = "x" * 100
    assert chunk_text(text, size=2000, overlap=200) == []


def test_build_gold_records_embeds_each_chunk_and_preserves_metadata() -> None:
    silver_records = [
        {
            "doc_path": "a.md",
            "title": "A",
            "summary": None,
            "body_for_embed": "x" * 300,
            "content_hash": "h1",
            "_commit": "c1",
        }
    ]

    gold = build_gold_records(
        silver_records,
        bedrock_client=_StubBedrock(),
        model_id="amazon.titan-embed-text-v2:0",
        chunk_size=2000,
        chunk_overlap=200,
        chunked_at="2026-07-26T00:00:00Z",
    )

    assert len(gold) == 1
    assert gold[0]["doc_path"] == "a.md"
    assert gold[0]["embedding"] == [0.1, 0.2, 0.3]
    assert gold[0]["embedding_model"] == "amazon.titan-embed-text-v2:0"


def test_build_gold_records_skips_records_with_no_body() -> None:
    silver_records = [{"doc_path": "a.md", "body_for_embed": ""}]
    assert build_gold_records(silver_records, _StubBedrock(), "m", 2000, 200, "t") == []


@mock_aws
def test_get_latest_silver_key_returns_most_recent_object() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{SILVER_PREFIX}ingestion_date=2026-01-01/silver_docs.jsonl.gz",
        Body=b"",
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{SILVER_PREFIX}ingestion_date=2026-02-01/silver_docs.jsonl.gz",
        Body=b"",
    )

    latest = get_latest_silver_key(s3, BUCKET)

    assert "2026-02-01" in latest


@mock_aws
def test_get_latest_silver_key_raises_when_none_found() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    with pytest.raises(ValueError):
        get_latest_silver_key(s3, BUCKET)


@mock_aws
def test_load_silver_records_round_trips_gzipped_jsonl() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    records = [{"doc_path": "a.md", "body_for_embed": "hi"}]
    key = f"{SILVER_PREFIX}ingestion_date=2026-01-01/silver_docs.jsonl.gz"
    body = "\n".join(json.dumps(r) for r in records).encode()
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(body))

    loaded = load_silver_records(s3, BUCKET, key)

    assert loaded == records


@mock_aws
def test_write_gold_records_writes_gzipped_jsonl_to_s3() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    records = [{"chunk_id": "1", "doc_path": "a.md", "embedding": [0.1]}]

    key = write_gold_records(s3, BUCKET, records, ingestion_date="2026-01-01")

    assert key == (
        "source=github/entity=docs-gold/ingestion_date=2026-01-01/gold_docs_chunks.jsonl.gz"
    )
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    written = [
        json.loads(line)
        for line in gzip.decompress(obj["Body"].read()).decode("utf-8").strip().split("\n")
    ]
    assert written == records


@mock_aws
def test_run_builds_gold_chunks_from_latest_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    silver_records = [
        {
            "doc_path": "a.md",
            "title": "A",
            "summary": None,
            "body_for_embed": "x" * 300,
            "content_hash": "h1",
            "_commit": "c1",
        }
    ]
    body = "\n".join(json.dumps(r) for r in silver_records).encode("utf-8")
    silver_key = f"{SILVER_PREFIX}ingestion_date=2026-01-01/silver_docs.jsonl.gz"
    s3.put_object(Bucket=BUCKET, Key=silver_key, Body=gzip.compress(body))

    real_client = boto3.client

    def fake_client(service_name: str, *args: Any, **kwargs: Any) -> Any:
        if service_name == "bedrock-runtime":
            return _StubBedrock()
        return real_client(service_name, *args, **kwargs)

    monkeypatch.setattr("pipeline.gold.embed.boto3.client", fake_client)

    gold_key = run()

    assert "docs-gold" in gold_key
    obj = s3.get_object(Bucket=BUCKET, Key=gold_key)
    written = [
        json.loads(line)
        for line in gzip.decompress(obj["Body"].read()).decode("utf-8").strip().split("\n")
    ]
    assert written[0]["embedding"] == [0.1, 0.2, 0.3]
