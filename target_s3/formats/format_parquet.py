import pyarrow
from pyarrow import fs, Table
from pyarrow.parquet import ParquetWriter

from target_s3.formats.format_base import FormatBase


class FormatParquet(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "parquet")
        cloud_provider_config = config.get("cloud_provider", None)
        cloud_provider_config_type = cloud_provider_config.get(
            "cloud_provider_type", None
        )
        self.file_system = self.create_filesystem(
            cloud_provider_config_type,
            cloud_provider_config.get(cloud_provider_config_type, None),
        )

    def create_filesystem(
        self,
        cloud_provider: str,
        cloud_provider_config: dict,
    ) -> fs.FileSystem:
        """Creates a pyarrow FileSystem object for accessing S3."""
        try:
            if cloud_provider == "aws":
                return fs.S3FileSystem(
                    access_key=self.session.get_credentials().access_key,
                    secret_key=self.session.get_credentials().secret_key,
                    session_token=self.session.get_credentials().token,
                    region=self.session.region_name,
                    endpoint_override=cloud_provider_config.get(
                        "aws_endpoint_override", None
                    ),
                )
        except Exception as e:
            self.logger.error("Failed to create parquet file system.")
            self.logger.error(e)
            raise e

    def validate(self, schema: dict, field, value) -> dict:
        def unpack_dict(record):
            ret = dict()
            for field in record:
                if isinstance(value[field], dict):
                    ret[field] = unpack_dict(value[field])
                else:
                    ret[field] = {"type": type(value[field])}
            return ret

        def validate_dict(value):
            fields = schema[field].get("fields")
            for v in value:
                # make sure value is in fields
                if not v in fields:
                    # add field and type
                    if isinstance(value[v], dict):
                        fields[v] = unpack_dict(value[v])
                    else:
                        fields[v] = {"type": type(value[v])}
                else:
                    # check data type
                    if isinstance(value[v], dict):
                        value[v] = unpack_dict(value[field])
                    else:
                        expected_type = fields[v].get("type")
                        if not isinstance(value[v], expected_type):
                            value[v] = expected_type(value[v])
            return value

        if field in schema:
            # make sure datatypes align
            if isinstance(value, dict):
                if not value:
                    # pyarrow can't process empty struct, return None
                    return None
                else:
                    validate_dict(value)
            else:
                expected_type = schema[field].get("type")
                if not isinstance(value, expected_type):
                    # if the values don't match try to cast current value to expected type, this souldn't happen,
                    # an error will occur during target instantiation.
                    value = expected_type(value)

        else:
            # add new entry for field
            if isinstance(value, dict):
                schema[field] = {"type": type(value), "fields": unpack_dict(value)}
            else:
                schema[field] = {"type": type(value)}

        return value

    def create_dataframe(self) -> Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in self.records:
                fields = fields.union(d.keys())

            if self.format.get("format_parquet", None).get("validate", None):
                schema = dict()
                input = {
                    f: [self.validate(schema, f, row.get(f)) for row in self.records]
                    for f in fields
                }
            else:
                input = {f: [row.get(f) for row in self.records] for f in fields}

            ret = Table.from_pydict(mapping=input)
        except Exception as e:
            self.logger.error("Failed to create parquet dataframe.")
            self.logger.error(e)
            raise e

        return ret

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
