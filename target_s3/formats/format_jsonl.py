from functools import partial

import simplejson as json

from target_s3.formats.format_base import FormatBase

from .format_json import JsonSerialize

dumps = partial(json.dumps, cls=JsonSerialize)


class FormatJsonl(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "jsonl")
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write("\n".join(map(dumps, self.records)))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
