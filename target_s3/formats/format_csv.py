from target_s3.formats.format_base import FormatBase


class FormatCsv(FormatBase):
    def __init__(self) -> None:
        super().__init__()
        raise NotImplementedError
