"""Unit tests for the bronze ingestion layer."""
import gzip
import hashlib
import json
from pathlib import Path

import boto3
from moto import mock_aws

from pipeline.bronze.ingest import build_records_from_directory, write_bronze_records


def test_build_records_from_directory_reads_markdown_files(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("# Title A\n\nHello world", encoding="utf-8")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "b.md").write_text("# Title B\n\nBody", encoding="utf-8")
    (tmp_path / "ignore.txt").write_text("not markdown", encoding="utf-8")

    records = build_records_from_directory(
        root=tmp_path,
        repo_url="https://github.com/example/repo.git",
        commit="deadbeefcafe",
        pulled_at="2026-07-26T00:00:00Z",
    )

    doc_paths = {record["doc_path"] for record in records}
    assert doc_paths == {"a.md", str(Path("subdir") / "b.md")}


def test_build_records_from_directory_generates_content_hash_and_provenance(
    tmp_path: Path,
) -> None:
    (tmp_path / "a.md").write_text("# Title A\n\nHello world", encoding="utf-8")

    [record] = build_records_from_directory(
        root=tmp_path,
        repo_url="https://github.com/example/repo.git",
        commit="deadbeefcafe",
        pulled_at="2026-07-26T00:00:00Z",
    )

    assert record["content_hash"] == hashlib.sha256(record["content"].encode("utf-8")).hexdigest()
    assert record["_repo"] == "https://github.com/example/repo.git"
    assert record["_commit"] == "deadbeefcafe"
    assert record["_pulled_at"] == "2026-07-26T00:00:00Z"


@mock_aws
def test_write_bronze_records_writes_gzipped_jsonl_to_s3() -> None:
    bucket = "test-bronze-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    records = [
        {"doc_path": "a.md", "content": "hello", "content_hash": "abc123"},
        {"doc_path": "b.md", "content": "world", "content_hash": "def456"},
    ]

    key = write_bronze_records(
        s3_client=s3,
        bucket=bucket,
        records=records,
        ingestion_date="2026-07-26",
        commit="deadbeefcafe",
    )

    assert key == (
        "source=github/entity=docs/ingestion_date=2026-07-26/commit=deadbeef/docs.jsonl.gz"
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = gzip.decompress(obj["Body"].read()).decode("utf-8")
    written = [json.loads(line) for line in body.strip().split("\n")]
    assert written == records
