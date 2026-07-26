"""Shared test environment setup.

Set before any test module imports pipeline/retrieval code, so that
pipeline.config's required-variable checks never fail during collection,
and so boto3 never accidentally reaches real AWS credentials.
"""
import os

os.environ.setdefault("BRONZE_BUCKET", "test-bronze-bucket")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
