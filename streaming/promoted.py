"""Optional typed columns promoted out of the CDC `_extra` payload.

The file is a local override for a schema change. It is not required for the
stream to keep running. Unknown fields stay in `_extra` until they are listed
here and added to the dbt staging select.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
_TYPE = re.compile(r"^(varchar|bigint|boolean|timestamp|date)$")


def promoted_columns(table: str) -> list[tuple[str, str]]:
    path = os.environ.get("CDC_PROMOTED_COLUMNS", "")
    if not path:
        return []
    file = Path(path)
    if not file.exists():
        return []
    payload = json.loads(file.read_text())
    columns = payload.get(table, {})
    if not isinstance(columns, dict):
        raise ValueError(f"promoted columns for {table} must be an object")
    checked: list[tuple[str, str]] = []
    for name, typ in columns.items():
        if not _NAME.match(name) or not _TYPE.match(str(typ)):
            raise ValueError(f"refusing promoted column {name}:{typ}")
        checked.append((name, str(typ)))
    return checked
