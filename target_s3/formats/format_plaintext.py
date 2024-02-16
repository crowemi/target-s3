from datetime import datetime

from bson import ObjectId

from target_s3.formats.format_base import FormatBase


class FormatPlaintext(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "plaintext")
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write("\n".join(self.records))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
