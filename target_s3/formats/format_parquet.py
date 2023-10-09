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
        """
        Validates data elements against a given schema and field. If the field is not in the schema, it will be added.
        If the value does not match the expected type in the schema, it will be cast to the expected type.
        The method returns the validated value.

        :param schema: A dictionary representing the schema to validate against.
        :param field: The field to validate.
        :param value: The value to validate.
        :return: The validated value.
        """

        def unpack_dict(record) -> dict:
            ret = dict()
            # set empty dictionaries to type string
            if len(record) == 0:
                ret = {"type": type(str())}
            for field in record:
                if isinstance(record[field], dict):
                    ret[field] = unpack_dict(record[field])
                elif isinstance(record[field], list):
                    ret[field] = unpack_list(record[field])
                else:
                    ret[field] = {"type": type(record[field])}
            return ret

        def unpack_list(record) -> dict:
            ret = dict()
            for idx, value in enumerate(record):
                if isinstance(record[idx], dict):
                    ret[idx] = unpack_dict(value)
                elif isinstance(record[idx], list):
                    ret[idx] = unpack_list(value)
                else:
                    ret[idx] = {"type": type(value)}
            return ret

        def validate_dict(value, fields):
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
                        value[v] = validate_dict(value[v], fields[v])
                    if isinstance(value[v], list):
                        value[v] = validate_list(value[v], fields[v])
                    else:
                        expected_type = fields[v].get("type")
                        if not isinstance(value[v], expected_type):
                            value[v] = expected_type(value[v])
            return value

        def validate_list(value, fields):
            for i, v in enumerate(value):
                if not i in fields:
                    # add field and type
                    if isinstance(v, dict):
                        fields[i] = unpack_dict(v)
                    if isinstance(v, list):
                        fields[i] = unpack_list(v)
                    else:
                        fields[i] = {"type": type(v)}
                else:
                    # validate
                    if isinstance(v, dict):
                        value[i] = validate_dict(v, fields[i])
                    if isinstance(v, list):
                        value[i] = validate_list(v, fields[i])
                    else:
                        expected_type = fields[i].get("type")
                        if not isinstance(v, expected_type):
                            value[i] = expected_type(v)
            return value

        if field in schema:
            # make sure datatypes align
            if isinstance(value, dict):
                if not value:
                    # pyarrow can't process empty struct, return None
                    return None
                else:
                    validate_dict(value, schema[field].get("fields"))
            elif isinstance(value, list):
                validate_list(value, schema[field].get("fields"))
            else:
                expected_type = schema[field].get("type")
                if not isinstance(value, expected_type):
                    # if the values don't match try to cast current value to expected type, this shouldn't happen,
                    # an error will occur during target instantiation.
                    value = expected_type(value)

        else:
            # add new entry for field
            if isinstance(value, dict):
                schema[field] = {"type": type(value), "fields": unpack_dict(value)}
                validate_dict(value, schema[field].get("fields"))
            elif isinstance(value, list):
                schema[field] = {"type": type(value), "fields": unpack_list(value)}
                validate_list(value, schema[field].get("fields"))
            else:
                schema[field] = {"type": type(value)}
                expected_type = schema[field].get("type")
                if not isinstance(value, expected_type):
                    # if the values don't match try to cast current value to expected type, this shouldn't happen,
                    # an error will occur during target instantiation.
                    value = expected_type(value)

        return value

    def sanitize(self, value):
        if isinstance(value, dict) and not value:
            # pyarrow can't process empty struct
            return None
        return value

    def create_batch_schema(self) -> pyarrow.schema:
        """Generates schema from the records schema present in the tap.
        This is effective way to declare schema instead of relying on pyarrow to
        detect schema type.

        Note: At level 0 (outermost level) any key that is of type datetime in record
        is converted to datetime by base target class. Hence string at level 0 is handled with
        type datetime.

        :return: schema made from stream's schema definition
        :rtype: pyarrow.schema
        """

        # TODO: handle non nullable types; by default nullable
        def get_schema_from_array(items: dict, level: int):
            """Returns item schema for an array.

            :param items: items definition of array
            :type items: dict
            :param level: depth level of array in jsonschema
            :type level: int
            :return: detected datatype for all items of array.
            :rtype: pyarrow datatype
            """
            type = items.get("type")
            properties = items.get("properties")
            items = items.get("items")
            if "integer" in type:
                return pyarrow.int64()
            elif "number" in type:
                return pyarrow.float64()
            elif "string" in type:
                return pyarrow.string()
            elif "array" in type:
                return pyarrow.list_(get_schema_from_array(items=items, level=level))
            elif "object" in type:
                return pyarrow.struct(
                    get_schema_from_object(properties=properties, level=level + 1)
                )
            else:
                return pyarrow.null()

        def get_schema_from_object(properties: dict, level: int = 0):
            """Returns schema for an object.

            :param properties: properties definition of object
            :type properties: dict
            :param level: depth level of object in jsonschema
            :type level: int
            :return: detected fields for properties in object.
            :rtype: pyarrow datatype
            """
            fields = []
            for key, val in properties.items():
                type = val["type"]
                format = val.get("format")
                if "integer" in type:
                    fields.append(pyarrow.field(key, pyarrow.int64()))
                elif "number" in type:
                    fields.append(pyarrow.field(key, pyarrow.float64()))
                elif "string" in type:
                    if format and level == 0:
                        # this is done to handle explicit datetime conversion
                        # which happens only at level 1 of a record
                        if format == "date":
                            fields.append(pyarrow.field(key, pyarrow.date64()))
                        elif format == "time":
                            fields.append(pyarrow.field(key, pyarrow.time64()))
                        else:
                            fields.append(
                                pyarrow.field(key, pyarrow.timestamp("s", tz="utc"))
                            )
                    else:
                        fields.append(pyarrow.field(key, pyarrow.string()))
                elif "array" in type:
                    items = val.get("items")
                    if items:
                        item_type = get_schema_from_array(items=items, level=level)
                        if item_type == pyarrow.null():
                            self.logger.warn(
                                f"""
                                            key: {key} is defined as list of null, while this would be
                                            correct for list of all null but it is better to define
                                            exact item types for the list, if not null."""
                            )
                        fields.append(pyarrow.field(key, pyarrow.list_(item_type)))
                    else:
                        self.logger.warn(
                            f"""
                                        key: {key} is defined as list of null, while this would be
                                        correct for list of all null but it is better to define
                                        exact item types for the list, if not null."""
                        )
                        fields.append(pyarrow.field(key, pyarrow.list_(pyarrow.null())))
                elif "object" in type:
                    prop = val.get("properties")
                    inner_fields = get_schema_from_object(
                        properties=prop, level=level + 1
                    )
                    if not inner_fields:
                        self.logger.warn(
                            f"""
                                        key: {key} has no fields defined, this may cause
                                        saving parquet failure as parquet doesn't support
                                        empty/null complex types [array, structs] """
                        )
                    fields.append(pyarrow.field(key, pyarrow.struct(inner_fields)))
            return fields

        properties = self.context["batch_schema"].get("properties")
        schema = pyarrow.schema(get_schema_from_object(properties=properties))
        return schema

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
                input = {
                    f: [
                        self.validate(schema, self.sanitize(f), row.get(f))
                        for row in self.records
                    ]
                    for f in fields
                }
            else:
                input = {
                    f: [self.sanitize(row.get(f)) for row in self.records]
                    for f in fields
                }

            if format_parquet and format_parquet.get("get_schema_from_tap", False):
                ret = Table.from_pydict(
                    mapping=input, schema=self.create_batch_schema()
                )
            else:
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
        try:
            ParquetWriter(
                f"{self.fully_qualified_key}.{self.extension}",
                df.schema,
                compression="gzip",  # TODO: support multiple compression types
                filesystem=self.file_system,
            ).write_table(df)
        except Exception as e:
            self.logger.error(e)
            if type(e) is pyarrow.lib.ArrowNotImplementedError:
                self.logger.error(
                    """Failed to write parquet file to S3. Complex types [array, object] in schema cannot be left without type definition """
                )
            else:
                self.logger.error("Failed to write parquet file to S3.")
            raise e

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
    