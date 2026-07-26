"""Unit tests for the silver refinement layer."""
import gzip
import json

import boto3
import pytest
from moto import mock_aws

from pipeline.config import BRONZE_PREFIX
from pipeline.silver.refine import (
    build_silver_records,
    clean_body,
    deduplicate,
    get_latest_bronze_key,
    load_bronze_records,
    parse_frontmatter,
    run,
    write_silver_records,
)

BUCKET = "test-bronze-bucket"


def test_parse_frontmatter_extracts_title_from_first_heading() -> None:
    title, summary = parse_frontmatter("# My Document Title\n\nSome body text.")
    assert title == "My Document Title"
    assert summary is None


def test_parse_frontmatter_returns_none_when_no_heading_present() -> None:
    title, _ = parse_frontmatter("Just a paragraph, no heading.")
    assert title is None


def test_clean_body_strips_code_fences() -> None:
    content = "Intro text\n\n```python\nprint('hi')\n```\n\nOutro text"
    cleaned = clean_body(content)
    assert "print" not in cleaned
    assert "Intro text" in cleaned
    assert "Outro text" in cleaned


def test_clean_body_strips_html_tags() -> None:
    content = "Some <b>bold</b> and <img src='x.png'/> text"
    cleaned = clean_body(content)
    assert "<b>" not in cleaned
    assert "<img" not in cleaned
    assert "bold" in cleaned


def test_deduplicate_keeps_latest_record_per_doc_path() -> None:
    records = [
        {"doc_path": "a.md", "_pulled_at": "2026-01-01T00:00:00Z", "content_hash": "old"},
        {"doc_path": "a.md", "_pulled_at": "2026-02-01T00:00:00Z", "content_hash": "new"},
        {"doc_path": "b.md", "_pulled_at": "2026-01-15T00:00:00Z", "content_hash": "only"},
    ]

    result = deduplicate(records)

    by_path = {record["doc_path"]: record for record in result}
    assert len(result) == 2
    assert by_path["a.md"]["content_hash"] == "new"
    assert by_path["b.md"]["content_hash"] == "only"


def test_build_silver_records_skips_records_with_no_content() -> None:
    bronze_records = [
        {
            "doc_path": "a.md",
            "content": "",
            "content_hash": "x",
            "_commit": "c1",
            "_pulled_at": "t1",
        },
        {
            "doc_path": "b.md",
            "content": "# B\n\nBody",
            "content_hash": "y",
            "_commit": "c1",
            "_pulled_at": "t1",
        },
    ]

    silver = build_silver_records(bronze_records, refined_at="2026-07-26T00:00:00Z")

    assert len(silver) == 1
    assert silver[0]["doc_path"] == "b.md"
    assert silver[0]["title"] == "B"


@mock_aws
def test_get_latest_bronze_key_returns_most_recent_object() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{BRONZE_PREFIX}ingestion_date=2026-01-01/commit=aaaaaaaa/docs.jsonl.gz",
        Body=b"",
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{BRONZE_PREFIX}ingestion_date=2026-02-01/commit=bbbbbbbb/docs.jsonl.gz",
        Body=b"",
    )

    latest = get_latest_bronze_key(s3, BUCKET)

    assert "2026-02-01" in latest


@mock_aws
def test_get_latest_bronze_key_raises_when_none_found() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    with pytest.raises(ValueError):
        get_latest_bronze_key(s3, BUCKET)


@mock_aws
def test_load_bronze_records_round_trips_gzipped_jsonl() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    records = [{"doc_path": "a.md", "content": "hi", "content_hash": "h"}]
    key = "source=github/entity=docs/ingestion_date=2026-01-01/commit=aaaaaaaa/docs.jsonl.gz"
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(body))

    loaded = load_bronze_records(s3, BUCKET, key)

    assert loaded == records


@mock_aws
def test_write_silver_records_writes_gzipped_jsonl_to_s3() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    records = [{"doc_path": "a.md", "title": "A", "content_hash": "h"}]

    key = write_silver_records(s3, BUCKET, records, ingestion_date="2026-01-01")

    assert key == "source=github/entity=docs-silver/ingestion_date=2026-01-01/silver_docs.jsonl.gz"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    written = [
        json.loads(line)
        for line in gzip.decompress(obj["Body"].read()).decode("utf-8").strip().split("\n")
    ]
    assert written == records


@mock_aws
def test_run_refines_latest_bronze_into_deduplicated_silver_file() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    bronze_records = [
        {
            "doc_path": "a.md",
            "content": "# A\n\nHello",
            "content_hash": "h1",
            "_commit": "c1",
            "_pulled_at": "2026-01-01T00:00:00Z",
        }
    ]
    body = "\n".join(json.dumps(r) for r in bronze_records).encode("utf-8")
    bronze_key = f"{BRONZE_PREFIX}ingestion_date=2026-01-01/commit=aaaaaaaa/docs.jsonl.gz"
    s3.put_object(Bucket=BUCKET, Key=bronze_key, Body=gzip.compress(body))

    silver_key = run()

    assert "docs-silver" in silver_key
    obj = s3.get_object(Bucket=BUCKET, Key=silver_key)
    written = [
        json.loads(line)
        for line in gzip.decompress(obj["Body"].read()).decode("utf-8").strip().split("\n")
    ]
    assert written[0]["doc_path"] == "a.md"
    assert written[0]["title"] == "A"
