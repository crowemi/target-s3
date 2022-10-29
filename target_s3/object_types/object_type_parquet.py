from pyarrow import fs

from target_s3.object_types.object_type_base import ObjectTypeBase


class ObjectTypeParquet(ObjectTypeBase):
    def __init__(self, aws_region: str, s3_bucket: str, s3_prefix: str) -> None:
        super().__init__(aws_region, s3_bucket, s3_prefix)
        self.extension = 'parquet'
        self.create_filesystem()

    def create_filesystem(self):
        """ Creates a pyarrow FileSystem object for accessing S3."""
        try:
            self.file_system = fs.S3FileSystem(region=self.aws_region)
        except Exception as e:
            raise e

    def prepare_records(self, records):
        pass

    def write(self, records: list) -> None:
        list(map(lambda x: print(x), records))
