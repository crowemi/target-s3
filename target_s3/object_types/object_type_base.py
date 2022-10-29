from abc import ABC
import abc


class ObjectTypeBase(ABC):
    """ This is the object type base class """
    def __init__(self, aws_region: str, s3_bucket: str, s3_prefix: str) -> None:
        self.aws_region = aws_region
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    @abc.abstractmethod
    def prepare_records(self, records):
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, records) -> None:
        raise NotImplementedError
