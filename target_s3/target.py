"""s3 target class."""

from __future__ import annotations

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from target_s3 import sinks

from target_s3.object_types.object_type_base import DATE_GRAIN

from target_s3.sinks import (
    s3Sink,
)


class Targets3(Target):
    """Sample target for s3."""

    name = "target-s3"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key",
            th.StringType,
            secret=True,
            description="The aws secret access key for auth to S3."
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            secret=True,
            required=False,
            description="The aws secret access key for auth to S3."
        ),
        th.Property(
            "aws_region",
            th.StringType,
            description="The aws region to target",
            required=True
        ),
        th.Property(
            "bucket",
            th.StringType,
            description="The aws bucket to target.",
            required=True
        ),
        th.Property(
            "prefix",
            th.StringType,
            description="The prefix for the key."
        ),
        th.Property(
            "append_date_to_prefix",
            th.BooleanType,
            description="A flag to append the date to the key prefix."
        ),
        th.Property(
            "append_date_to_prefix_grain",
            th.StringType,
            description="The grain of the date to append to the prefix.",
            allowed_values=DATE_GRAIN.keys()
        ),
        th.Property(
            "append_date_to_filename",
            th.BooleanType,
            description="A flag to append the date to the key filename."
        ),
        th.Property(
            "append_date_to_filename_grain",
            th.StringType,
            description="The grain of the date to append to the filename.",
            allowed_values=DATE_GRAIN.keys()
        ),
        th.Property(
            "object_format",
            th.StringType,
            description="The format of the storage object.",
            allowed_values=sinks.OBJECT_TYPE.keys()
        ),
        th.Property(
            "flatten_records",
            th.BooleanType,
            description="A flag indictating to flatten records.",
        ),
    ).to_dict()

    default_sink_class = s3Sink


if __name__ == "__main__":
    Targets3.cli()
