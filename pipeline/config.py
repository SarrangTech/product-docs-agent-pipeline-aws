"""Centralized configuration for the pipeline and retrieval Lambda.

All configuration is read from environment variables so the same code runs
unmodified in CI, local development, and the deployed Lambda. See .env.example
for the full list of variables, descriptions, and example values.
"""
from __future__ import annotations

import logging
import os

# S3 path conventions -- live in production, do not change.
BRONZE_PREFIX = "source=github/entity=docs/"
SILVER_PREFIX = "source=github/entity=docs-silver/"
GOLD_PREFIX = "source=github/entity=docs-gold/"

_DEFAULT_AWS_REGION = "us-east-1"
_DEFAULT_GITHUB_REPO_URL = "https://github.com/aws/aws-sdk-pandas.git"
_DEFAULT_BEDROCK_MODEL_ID = "amazon.titan-embed-text-v2:0"
_DEFAULT_CHUNK_SIZE = "2000"
_DEFAULT_CHUNK_OVERLAP = "200"
_DEFAULT_LOG_LEVEL = "INFO"


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing or invalid."""


def _require(name: str) -> str:
    """Return the value of a required environment variable or raise ``ConfigError``."""
    value = os.environ.get(name)
    if not value:
        raise ConfigError(
            f"Missing required environment variable: {name}. See .env.example "
            "for the full list of required variables."
        )
    return value


def get_bronze_bucket() -> str:
    """S3 bucket backing the bronze/silver/gold medallion layers."""
    return _require("BRONZE_BUCKET")


def get_aws_region() -> str:
    """AWS region for the S3, Bedrock, and Lambda clients."""
    return os.environ.get("AWS_REGION", _DEFAULT_AWS_REGION)


def get_github_repo_url() -> str:
    """GitHub repository URL that the bronze layer ingests documentation from."""
    return os.environ.get("GITHUB_REPO_URL", _DEFAULT_GITHUB_REPO_URL)


def get_bedrock_model_id() -> str:
    """Bedrock embedding model id used for both gold-layer and query-time embeddings."""
    return os.environ.get("BEDROCK_MODEL_ID", _DEFAULT_BEDROCK_MODEL_ID)


def get_chunk_size() -> int:
    """Character length of each gold-layer chunk."""
    return int(os.environ.get("CHUNK_SIZE", _DEFAULT_CHUNK_SIZE))


def get_chunk_overlap() -> int:
    """Character overlap between consecutive gold-layer chunks."""
    return int(os.environ.get("CHUNK_OVERLAP", _DEFAULT_CHUNK_OVERLAP))


def get_log_level() -> str:
    """Python logging level name (DEBUG, INFO, WARNING, ERROR)."""
    return os.environ.get("LOG_LEVEL", _DEFAULT_LOG_LEVEL)


def configure_logging() -> None:
    """Configure root logging with the project's structured log format.

    Call this once from each entry point's ``if __name__ == "__main__"`` block --
    library code should never call it, only top-level scripts.
    """
    logging.basicConfig(
        level=get_log_level(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
