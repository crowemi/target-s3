from json import dumps, JSONEncoder
from datetime import datetime

from bson import ObjectId

from target_s3.formats.format_base import FormatBase


class JsonSerialize(JSONEncoder):
    def default(self, obj: any) -> any:
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            raise TypeError(f"Type {type(obj)} not serializable")


class FormatJson(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "json")
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        # TODO: validate json records?
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write(dumps(self.records, cls=JsonSerialize))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
