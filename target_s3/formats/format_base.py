import re
import inflection
import json
import collections
import logging
from datetime import datetime
from abc import ABCMeta, abstractmethod
from smart_open import open


LOGGER = logging.getLogger("target-s3")
DATE_GRAIN = {
    'year': 7,
    'month': 6,
    'day': 5,
    'hour': 4,
    'minute': 3,
    'second': 2,
    'microsecond': 1
}
COMPRESSION = {}


def format_type_factory(object_type_class, *pargs, **kargs):
    """ A factory for creating ObjectTypes. """
    return object_type_class(*pargs, **kargs)


class FormatBase(metaclass=ABCMeta):

    """ This is the object type base class """
    def __init__(self, config: dict, context: dict, extension: str) -> None:
        # TODO: perhaps we should do some scrubbing here?
        self.config = config
        self.context = context
        self.extension = extension
        self.compression = 'gz'  # TODO: need a list of compatible compression types

        self.aws_region = config.get('aws_region')  # required
        self.bucket = config.get('bucket')  # required
        self.prefix = config.get('prefix', None)
        self.logger = context['logger']
        self.stream_name_path_override = config.get('stream_name_path_override', None)

        self.fully_qualified_key = self.create_key()
        self.logger.info(f"key: {self.fully_qualified_key}")

    @abstractmethod
    def _write(self, contents: str = None) -> None:
        """ Execute the write to S3. (default) """
        # TODO: create dynamic cloud
        # TODO: is there a better way to handle write contents ?
        with open(f"s3://{self.fully_qualified_key}.{self.extension}.{self.compression}", "w") as f:
            f.write(contents)

    @abstractmethod
    def run(self, records) -> None:
        """ Execute the steps for preparing/writing records to S3. (default) """
        self.records = records
        # prepare records for writing
        self._prepare_records()
        # write records to S3
        self._write()

    @abstractmethod
    def _prepare_records(self) -> None:
        """ Execute record prep. (default) """
        if self.config.get('flatten_records', None):
            # flatten records
            self.records = list(map(lambda record: self.flatten_record(record), self.records))


    def create_key(self) -> str:
        batch_start = self.context['batch_start_time']
        stream_name = self.context['stream_name'] if self.stream_name_path_override is None else self.stream_name_path_override
        folder_path = f"{self.bucket}/{self.prefix}/{stream_name}/"
        file_name = ''
        if self.config['append_date_to_prefix']:
            grain = DATE_GRAIN[self.config['append_date_to_prefix_grain'].lower()]
            folder_path += self.create_folder_structure(batch_start, grain)
        if self.config['append_date_to_filename']:
            grain = DATE_GRAIN[self.config['append_date_to_filename_grain'].lower()]
            file_name += f"{self.create_file_structure(batch_start, grain)}"

        return f"{folder_path}{file_name}"

    def create_folder_structure(self, batch_start: datetime, grain: int) -> str:
        ret = ''
        ret += f"{batch_start.year}/" if grain <= 7 else ''
        ret += f"{batch_start.month:02}/" if grain <= 6 else ''
        ret += f"{batch_start.day:02}/" if grain <= 5 else ''
        ret += f"{batch_start.hour:02}/" if grain <= 4 else ''
        ret += f"{batch_start.minute:02}/" if grain <= 3 else ''
        ret += f"{batch_start.second:02}/" if grain <= 4 else ''
        ret += f"{batch_start.microsecond}/" if grain <= 1 else ''
        return ret

    def create_file_structure(self, batch_start: datetime, grain: int) -> str:
        ret = ''
        ret += f"{batch_start.year}" if grain <= 7 else ''
        ret += f"{batch_start.month:02}" if grain <= 6 else ''
        ret += f"{batch_start.day:02}" if grain <= 5 else ''
        ret += f"-{batch_start.hour:02}" if grain <= 4 else ''
        ret += f"{batch_start.minute:02}" if grain <= 3 else ''
        ret += f"{batch_start.second:02}" if grain <= 4 else ''
        ret += f"{batch_start.microsecond}" if grain <= 1 else ''
        return ret

    def flatten_key(self, k, parent_key, sep) -> str:
        """"""
        # TODO: standardize in the SDK?
        full_key = parent_key + [k]
        inflected_key = [n for n in full_key]
        reducer_index = 0
        while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
            reduced_key = re.sub(
                r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
            )
            inflected_key[reducer_index] = (
                reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]
            ).lower()
            reducer_index += 1

        return sep.join(inflected_key)

    def flatten_record(self, d, parent_key=[], sep="__") -> dict:
        """"""
        # TODO: standardize in the SDK?
        items = []
        for k in sorted(d.keys()):
            v = d[k]
            new_key = self.flatten_key(k, parent_key, sep)
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten_record(v, parent_key + [k], sep=sep).items())
            else:
                items.append((new_key, json.dumps(v) if type(v) is list else v))
        return dict(items)
