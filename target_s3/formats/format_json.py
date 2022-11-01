import json
from target_s3.formats.format_base import FormatBase


class FormatJson(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, 'json')
        pass

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        # TODO: validate json records?
        return super()._prepare_records()

    def _write(self) -> None:
        return super()._write(json.dumps(self.records))

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context['records'])
