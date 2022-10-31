"""s3 target sink class, which handles writing streams."""

from __future__ import annotations
import logging

from singer_sdk.sinks import BatchSink

from target_s3.formats.format_base import FormatBase, object_type_factory
from target_s3.formats.format_parquet import FormatParquet
from target_s3.formats.format_csv import FormatCsv


LOGGER = logging.getLogger("target-s3")
OBJECT_TYPE = {
    "parquet": FormatParquet,
    "csv": FormatCsv
}


class s3Sink(BatchSink):
    """s3 target sink class."""

    MAX_SIZE = 10000  # Max records to write in one batch

    def __init__(self, target: any, stream_name: str, schema: dict, key_properties: list[str] | None) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # what type of file are we building?
        self.object_type = self.config.get('object_type')
        if self.object_type:
            if self.object_type not in OBJECT_TYPE:
                raise Exception(f"Unknown file type specified. {key_properties['type']}")
        else:
            raise Exception("No file type supplied.")

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # add stream name to context
        context['stream_name'] = self.stream_name
        context['logger'] = self.logger
        # creates new object for each batch
        object_type_client = self.object_type_client = object_type_factory(OBJECT_TYPE[self.object_type], self.config, context)
        # force base object_type_client to object_type_base class
        assert isinstance(self.object_type_client, FormatBase) is True, \
            f"object_type_client must be of type Base; Type: {type(self.object_type_client)}."

        object_type_client.run()
