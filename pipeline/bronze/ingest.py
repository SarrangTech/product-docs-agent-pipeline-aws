"""Bronze layer: clone a GitHub repository, extract markdown docs, and land them in S3 verbatim.

Bronze is the raw landing zone -- no transformation, no assumptions. If a
downstream parser has a bug, reprocess from bronze without re-cloning the
source repository.
"""
from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import logging
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline.config import BRONZE_PREFIX, get_aws_region, get_bronze_bucket, get_github_repo_url

logger = logging.getLogger(__name__)

_RETRY_AWS = retry(
    retry=retry_if_exception_type((ClientError, BotoCoreError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)


class GitCloneError(RuntimeError):
    """Raised when the source repository cannot be cloned."""


def clone_repository(repo_url: str, dest: Path) -> str:
    """Shallow-clone ``repo_url`` into ``dest`` and return the checked-out commit SHA.

    Raises:
        GitCloneError: if ``git clone`` fails (bad URL, network error, auth failure).
    """
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(dest)],
            check=True,
            capture_output=True,
        )
        commit = (
            subprocess.check_output(["git", "-C", str(dest), "rev-parse", "HEAD"])
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="ignore") if exc.stderr else ""
        raise GitCloneError(f"Failed to clone {repo_url}: {stderr}") from exc
    return commit


def build_records_from_directory(
    root: Path, repo_url: str, commit: str, pulled_at: str
) -> list[dict[str, Any]]:
    """Read every markdown file under ``root`` into a bronze record with provenance.

    Each record carries ``content_hash`` (sha256 of the raw text) plus
    ``_repo``, ``_commit``, and ``_pulled_at`` so the agent can attribute
    answers to a specific document version.
    """
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.md")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        records.append(
            {
                "doc_path": str(path.relative_to(root)),
                "content": text,
                "content_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "_repo": repo_url,
                "_commit": commit,
                "_pulled_at": pulled_at,
            }
        )
    return records


@_RETRY_AWS
def write_bronze_records(
    s3_client: Any,
    bucket: str,
    records: list[dict[str, Any]],
    ingestion_date: str,
    commit: str,
) -> str:
    """Gzip-compress ``records`` as newline-delimited JSON and write them to the bronze prefix.

    The S3 key embeds the ingestion date and short commit SHA so re-running at the
    same commit overwrites the same object instead of creating duplicates.
    """
    key = f"{BRONZE_PREFIX}ingestion_date={ingestion_date}/commit={commit[:8]}/docs.jsonl.gz"
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
    """Entry point: clone the configured repo, extract docs, and land them in bronze.

    Returns:
        The S3 key the bronze file was written to.
    """
    bucket = get_bronze_bucket()
    repo_url = get_github_repo_url()
    region = get_aws_region()

    today = datetime.date.today().isoformat()
    pulled_at = datetime.datetime.now(datetime.UTC).isoformat()

    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        logger.info("Cloning %s", repo_url)
        commit = clone_repository(repo_url, tmp_path)
        logger.info("Checked out commit %s", commit)

        records = build_records_from_directory(tmp_path, repo_url, commit, pulled_at)
        logger.info("Found %d markdown files", len(records))

    s3 = boto3.client("s3", region_name=region)
    return write_bronze_records(s3, bucket, records, today, commit)


if __name__ == "__main__":
    from pipeline.config import configure_logging

    configure_logging()
    run()
