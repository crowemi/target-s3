import pyarrow
from pyarrow import fs, Table
from pyarrow.parquet import ParquetWriter

from target_s3.formats.format_base import FormatBase
import pandas as pd


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

    def validate(self, schema: dict) -> dict:
        for record in self.records:
            for field in record:
                expected_type = schema[field]
                actual_type = type(record[field])
                if isinstance(actual_type, dict):
                    pass
                else:
                    if expected_type != actual_type:
                        print("d")

    def sanitize(self, value):
        if isinstance(value, dict) and not value:
            # pyarrow can't process empty struct
            return None
        return value

    def create_schema_types(
        self,
        record: dict,
        schema: dict,
    ):
        for field in record:
            if isinstance(record[field], dict):
                # unpack dictionary
                if field not in schema:
                    # field isn't already in schema, create a new schema
                    child_schema = dict()
                    self.create_schema_types(record[field], child_schema)
                    schema[field] = child_schema
                else:
                    if isinstance(schema[field], dict):
                        # NOTE: this was causing an error when the field schema was NoneType
                        self.create_schema_types(record[field], schema[field])
                    else:
                        # NOTE: DRY, same as above
                        if schema[field] == type(None):
                            # if the previously set schema type is None, we need to re-establish the dictionary definition
                            child_schema = dict()
                            self.create_schema_types(record[field], child_schema)
                            schema[field] = child_schema
                        else:
                            # if the previously set schema type is not None, we need to set the definition to string
                            # TODO: log warning
                            schema[field] = str()
            elif isinstance(record[field], list):
                # unpack list
                if field not in schema:
                    # field isn't already in schema, create a new schema
                    child_schema = list()
                    if len(record[field]) > 0:
                        for i, v in enumerate(record[field]):
                            element_type = type(v)
                            if isinstance(v, dict):
                                # unpack dictionary
                                child_dict = dict()
                                self.create_schema_types(v, child_dict)
                                child_schema.append(child_dict)
                            elif isinstance(element_type, list):
                                pass
                            else:
                                child_schema.append(element_type)
                        schema[field] = child_schema
                    else:
                        schema[field] = type(None)
                else:
                    if len(record[field]) > 0:
                        for i, v in enumerate(record[field]):
                            if len(schema[field]) >= i:
                                self.create_schema_types(v, schema[field][i])
                            else:
                                pass
            else:
                record_field_type = type(record[field])
                # assign type to schema
                if field not in schema:
                    # field is not in schema, assign type
                    schema[field] = record_field_type
                else:
                    schema_field_type = schema[field]
                    if not schema_field_type == record_field_type:
                        # what happens when the field type and schema type are different?
                        pass
        self.logger.debug(f"format_parquet.create_schema_types: end processing record.")

    def create_schema(self, schema: dict):
        """Create schema from every data element within collection"""
        self.logger.info("format_parquet.create_schema: start create schema.")
        [self.create_schema_types(record, schema) for record in self.records]
        self.logger.info("format_parquet.create_schema: end create schema.")

    def create_dataframe(self) -> Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in self.records:
                fields = fields.union(d.keys())

            format_parquet = self.format.get("format_parquet", None)
            if format_parquet and format_parquet.get("validate", None) == True:
                # NOTE: we may could use schema to build a pyarrow schema https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html
                # and pass that into from_pydict(). The schema is inferred by pyarrow, but we could always be explicit about it.
                schema = dict()
                # we need to create a schema for every field in the record set
                self.create_schema(schema)
                # we need to validate records against schema
                self.validate(schema)
                input = {
                    f: [
                        self.validate(schema, self.sanitize(f), row.get(f))
                        for row in self.records
                    ]
                    for f in fields
                }
            else:
                # default should be pyarrow implied schema
                input = {f: [row.get(f) for row in self.records] for f in fields}

            ret = Table.from_pydict(mapping=input)

        except Exception as e:
            self.logger.info(self.records)
            self.logger.error("Failed to create parquet dataframe.")
            self.logger.error(e)
            raise e

        return ret

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self, contents: str = None) -> None:
        df = self.create_dataframe()
        # df = Table.from_pandas(pd.DataFrame(self.records))
        try:
            ParquetWriter(
                f"{self.fully_qualified_key}.{self.extension}",
                df.schema,
                compression="gzip",  # TODO: support multiple compression types {‘NONE’, ‘SNAPPY’, ‘GZIP’, ‘BROTLI’, ‘LZ4’, ‘ZSTD’} https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
                filesystem=self.file_system,
            ).write_table(df)
        except Exception as e:
            self.logger.error("Failed to write parquet file to S3.")
            raise e

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
