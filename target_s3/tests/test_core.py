"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

from typing import Any

from singer_sdk.testing import get_target_test_class

from target_s3.target import Targets3

SAMPLE_CONFIG: dict[str, Any] = {
    "format": {
        "format_type": "json",
    },
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "aws_bucket": "test-bucket",
            "aws_region": "us-east-1",
            "aws_endpoint_override": "http://localhost:9000",
        },
    },
    "prefix": "integration-tests",
}

TestTargetS3 = get_target_test_class(Targets3, config=SAMPLE_CONFIG)
