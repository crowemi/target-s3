import pyarrow
from pyarrow import fs
from pyarrow.parquet import ParquetWriter

from target_s3.formats.format_base import FormatBase


class FormatParquet(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "parquet")
        self.create_filesystem(
            config.get("aws_profile_name", None),
            config.get("aws_region", None),
        )

    def create_filesystem(
        self,
        aws_profile: str = None,
        aws_region: str = None,
    ) -> None:
        """Creates a pyarrow FileSystem object for accessing S3."""
        aws_region = self.aws_region if aws_region is None else aws_region
        try:
            if self.session:
                self.file_system = fs.S3FileSystem(
                    access_key=self.session.get_credentials().access_key,
                    secret_key=self.session.get_credentials().secret_key,
                    session_token=self.session.get_credentials().token,
                )
            else:
                self.file_system = fs.S3FileSystem(
                    region=aws_region,
                )
        except Exception as e:
            self.logger.error("Failed to create parquet file system.")
            self.logger.error(e)
            raise e

    def validate(self, field, value):
        if isinstance(value, dict) and not value:
            # pyarrow can't process empty struct
            return None

        return value

    def create_dataframe(self) -> pyarrow.Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in self.records:
                fields = fields.union(d.keys())
            dataframe = pyarrow.table(
                {
                    f: [self.validate(f, row.get(f)) for row in self.records]
                    for f in fields
                }
            )
        except Exception as e:
            self.logger.error("Failed to create parquet dataframe.")
            self.logger.error(e)
            raise e

        return dataframe

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self, contents: str = None) -> None:
        df = self.create_dataframe()
        try:
            ParquetWriter(
                f"{self.fully_qualified_key}.{self.extension}",
                df.schema,
                compression="gzip",  # TODO: support multiple compression types
                filesystem=self.file_system,
            ).write_table(df)
        except Exception as e:
            self.logger.error("Failed to write parquet file to S3.")
            raise e

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
