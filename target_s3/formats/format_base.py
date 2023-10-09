import re
import inflection
import json
import collections
import logging
from datetime import datetime
from abc import ABCMeta, abstractmethod

from boto3 import Session
from smart_open import open


LOGGER = logging.getLogger("target-s3")
DATE_GRAIN = {
    "year": 7,
    "month": 6,
    "day": 5,
    "hour": 4,
    "minute": 3,
    "second": 2,
    "microsecond": 1,
}
COMPRESSION = {}


def format_type_factory(object_type_class, *pargs, **kargs):
    """A factory for creating ObjectTypes."""
    return object_type_class(*pargs, **kargs)


class FormatBase(metaclass=ABCMeta):

    """This is the object type base class"""

    def __init__(self, config: dict, context: dict, extension: str) -> None:
        # TODO: perhaps we should do some scrubbing here?
        self.config = config

        self.format = config.get("format", None)
        assert self.format, "FormatBase.__init__: Expecting format in configuration."

        self.cloud_provider = config.get("cloud_provider", None)
        assert (
            self.cloud_provider
        ), "FormatBase.__init__: Expecting cloud provider in configuration"

        self.context = context
        self.extension = extension
        self.compression = "gz"  # TODO: need a list of compatible compression types

        self.stream_name_path_override = config.get("stream_name_path_override", None)

        if self.cloud_provider.get("cloud_provider_type", None) == "aws":
            aws_config = self.cloud_provider.get("aws", None)
            assert aws_config, "FormatBase.__init__: Expecting aws in configuration"

            self.bucket = aws_config.get("aws_bucket", None)  # required
            self.session = Session(
                aws_access_key_id=aws_config.get("aws_access_key_id", None),
                aws_secret_access_key=aws_config.get("aws_secret_access_key", None),
                aws_session_token=aws_config.get("aws_session_token", None),
                region_name=aws_config.get("aws_region"),
                profile_name=aws_config.get("aws_profile_name", None),
            )
            self.client = self.session.client(
                "s3",
                endpoint_url=aws_config.get("aws_endpoint_override", None),
            )

        self.prefix = config.get("prefix", None)
        self.logger = context["logger"]
        self.fully_qualified_key = self.create_key()
        self.logger.info(f"key: {self.fully_qualified_key}")

    @abstractmethod
    def _write(self, contents: str = None) -> None:
        """Execute the write to S3. (default)"""
        # TODO: create dynamic cloud
        # TODO: is there a better way to handle write contents ?
        with open(
            f"s3://{self.fully_qualified_key}.{self.extension}.{self.compression}",
            "w",
            transport_params={"client": self.client},
        ) as f:
            f.write(contents)

    @abstractmethod
    def run(self, records) -> None:
        """Execute the steps for preparing/writing records to S3. (default)"""
        self.records = records
        # prepare records for writing
        self._prepare_records()
        # write records to S3
        self._write()

    @abstractmethod
    def _prepare_records(self) -> None:
        """Execute record prep. (default)"""
        if self.config.get("include_process_date", None):
            self.records = self.append_process_date(self.records)

    def create_key(self) -> str:
        batch_start = self.context["batch_start_time"]
        stream_name = (
            self.context["stream_name"]
            if self.stream_name_path_override is None
            else self.stream_name_path_override
        )
        folder_path = f"{self.bucket}/{self.prefix}/{stream_name}/"
        file_name = ""
        if self.config["append_date_to_prefix"]:
            grain = DATE_GRAIN[self.config["append_date_to_prefix_grain"].lower()]
            partition_name_enabled = False
            if self.config["partition_name_enabled"]:
                partition_name_enabled = self.config["partition_name_enabled"]   
            folder_path += self.create_folder_structure(batch_start, grain, partition_name_enabled)
        if self.config["append_date_to_filename"]:
            grain = DATE_GRAIN[self.config["append_date_to_filename_grain"].lower()]
            file_name += f"{self.create_file_structure(batch_start, grain)}"

        return f"{folder_path}{file_name}"

    def create_folder_structure(self, batch_start: datetime, grain: int, partition_name_enabled: bool) -> str:
        ret = ""
        ret += f"{'year=' if partition_name_enabled        else  ''}{batch_start.year}/" if grain <= DATE_GRAIN["year"] else ""
        ret += f"{'month=' if partition_name_enabled       else  ''}{batch_start.month:02}/" if grain <= DATE_GRAIN["month"] else ""
        ret += f"{'day=' if partition_name_enabled         else  ''}{batch_start.day:02}/" if grain <= DATE_GRAIN["day"] else ""
        ret += f"{'hour=' if partition_name_enabled        else  ''}{batch_start.hour:02}/" if grain <= DATE_GRAIN["hour"] else ""
        ret += f"{'minute=' if partition_name_enabled      else  ''}{batch_start.minute:02}/" if grain <= DATE_GRAIN["minute"] else ""
        ret += f"{'second=' if partition_name_enabled      else  ''}{batch_start.second:02}/" if grain <= DATE_GRAIN["second"] else ""
        ret += f"{'microsecond=' if partition_name_enabled else  ''}{batch_start.microsecond}/" if grain <= DATE_GRAIN["microsecond"] else ""
        return ret

    def create_file_structure(self, batch_start: datetime, grain: int) -> str:
        ret = ""
        ret += f"{batch_start.year}" if grain <= DATE_GRAIN["year"] else ""
        ret += f"{batch_start.month:02}" if grain <= DATE_GRAIN["month"] else ""
        ret += f"{batch_start.day:02}" if grain <= DATE_GRAIN["day"] else ""
        ret += f"-{batch_start.hour:02}" if grain <= DATE_GRAIN["hour"] else ""
        ret += f"{batch_start.minute:02}" if grain <= DATE_GRAIN["minute"] else ""
        ret += f"{batch_start.second:02}" if grain <= DATE_GRAIN["second"] else ""
        ret += f"{batch_start.microsecond}" if grain <= DATE_GRAIN["microsecond"] else ""
        return ret

    def append_process_date(self, records) -> dict:
        """A function that appends the current UTC to every record"""

        def process_date(record):
            record["_PROCESS_DATE"] = datetime.utcnow().isoformat()
            return record

        return list(map(lambda x: process_date(x), records))
