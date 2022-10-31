import pyarrow
from pyarrow import fs
from pyarrow.parquet import ParquetWriter

from target_s3.object_types.object_type_base import ObjectTypeBase


class ObjectTypeParquet(ObjectTypeBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, 'parquet')
        self.create_filesystem()
        # execute process
        self.run()

    def create_filesystem(self) -> None:
        """Creates a pyarrow FileSystem object for accessing S3."""
        try:
            self.file_system = fs.S3FileSystem(region=self.aws_region)
        except Exception as e:
            self.logger.error('Failed to create parquet file system.')
            self.logger.error(e)
            raise e

    def create_dataframe(self) -> pyarrow.Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in self.records:
                fields = fields.union(d.keys())
            dataframe = pyarrow.table({f: [row.get(f) for row in self.records] for f in fields})
        except Exception as e:
            self.logger.error('Failed to create parquet dataframe.')
            self.logger.error(e)
            raise e

        return dataframe

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self) -> None:
        df = self.create_dataframe()
        try:
            ParquetWriter(
                self.full_qualified_key,
                df.schema,
                compression='gzip',
                filesystem=self.file_system,
            ).write_table(df)
        except Exception as e:
            self.logger.error('Failed to write parquet file to S3.')
            raise e

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context['records'])
