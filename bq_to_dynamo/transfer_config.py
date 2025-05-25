from pathlib import Path

import yaml
from pydantic import BaseModel


class Destination(BaseModel):
    table_arn: str
    # primary_key: str


# TODO Validate later that only one of {table, query} is set
class Source(BaseModel):
    project: str
    table: str | None = None
    query: str | None = None


class Transfer(BaseModel):
    destination: Destination
    source: Source


def read_transfer_config(file_path: Path | str) -> Transfer:
    file_content = Path(file_path).read_text()
    config = yaml.safe_load(file_content)
    transfer = Transfer(**config)
    return transfer
