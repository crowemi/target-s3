"""s3 target sink class, which handles writing streams."""

from __future__ import annotations
import logging

from singer_sdk.sinks import BatchSink

from target_s3.formats.format_base import FormatBase, object_type_factory
from target_s3.formats.format_parquet import FormatParquet
from target_s3.formats.format_csv import FormatCsv
from target_s3.formats.format_json import FormatJson


LOGGER = logging.getLogger("target-s3")
FORMAT_TYPE = {
    "parquet": FormatParquet,
    "csv": FormatCsv,
    "json": FormatJson
}


class s3Sink(BatchSink):
    """s3 target sink class."""

    MAX_SIZE = 10000  # Max records to write in one batch

    def __init__(self, target: any, stream_name: str, schema: dict, key_properties: list[str] | None) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # what type of file are we building?
        self.format_type = self.config.get('format_type')
        if self.format_type:
            if self.format_type not in FORMAT_TYPE:
                raise Exception(f"Unknown file type specified. {key_properties['type']}")
        else:
            raise Exception("No file type supplied.")

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # add stream name to context
        context['stream_name'] = self.stream_name
        context['logger'] = self.logger
        # creates new object for each batch
        format_type_client = object_type_factory(FORMAT_TYPE[self.format_type], self.config, context)
        # force base object_type_client to object_type_base class
        assert isinstance(format_type_client, FormatBase) is True, \
            f"format_type_client must be of type Base; Type: {type(self.format_type_client)}."

        format_type_client.run()
