"""s3 target sink class, which handles writing streams."""

from __future__ import annotations
import logging

from singer_sdk.sinks import BatchSink

from target_s3.object_types.object_type_parquet import ObjectTypeParquet
from target_s3.object_types.object_type_csv import ObjectTypeCsv
from target_s3.object_types.object_type_base import ObjectTypeBase


LOGGER = logging.getLogger("target-s3")


class s3Sink(BatchSink):
    """s3 target sink class."""

    MAX_SIZE = 10000  # Max records to write in one batch

    def __init__(self, target: any, stream_name: str, schema: dict, key_properties: list[str] | None) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # what type of file are we building?
        object_type = self.config.get('object_type')
        if object_type:
            aws_region = self.config.get('aws_region')
            s3_bucket = self.config.get('bucket')
            s3_prefix = self.config.get('prefix')
            if object_type == 'parquet':
                self.object_type_client = ObjectTypeParquet(aws_region, s3_bucket, s3_prefix)
            if object_type == 'csv':
                self.object_type_client = ObjectTypeCsv(aws_region, s3_bucket, s3_prefix)
            else:
                raise Exception(f"Unknown file type specified. {key_properties['type']}")

            # force base object_type_client to object_type_base class
            assert isinstance(self.object_type_client, ObjectTypeBase) is True, \
                f"object_type_client must be of type Base; Type: {type(self.object_type_client)}."

        else:
            raise Exception("No file type supplied.")

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.
        """
        context[context['batch_id']] = list()
        print(context)
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)
        context[context['batch_id']].append(record)
        print(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy
        self.object_type_client.prepare_records(context[context['batch_id']])
        print(context)
