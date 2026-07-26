"""Unit tests for environment-based configuration."""
import pytest

from pipeline import config


def test_get_bronze_bucket_returns_configured_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRONZE_BUCKET", "my-bucket")
    assert config.get_bronze_bucket() == "my-bucket"


def test_get_bronze_bucket_raises_clear_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BRONZE_BUCKET", raising=False)
    with pytest.raises(config.ConfigError, match="BRONZE_BUCKET"):
        config.get_bronze_bucket()


def test_optional_getters_fall_back_to_documented_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "AWS_REGION",
        "GITHUB_REPO_URL",
        "BEDROCK_MODEL_ID",
        "CHUNK_SIZE",
        "CHUNK_OVERLAP",
        "LOG_LEVEL",
    ):
        monkeypatch.delenv(name, raising=False)

    assert config.get_aws_region() == "us-east-1"
    assert config.get_github_repo_url() == "https://github.com/aws/aws-sdk-pandas.git"
    assert config.get_bedrock_model_id() == "amazon.titan-embed-text-v2:0"
    assert config.get_chunk_size() == 2000
    assert config.get_chunk_overlap() == 200
    assert config.get_log_level() == "INFO"


def test_configure_logging_does_not_raise() -> None:
    config.configure_logging()
