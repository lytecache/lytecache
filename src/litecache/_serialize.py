"""Value <-> (blob, value_type) conversion.

str / int / float / bytes are stored natively (as UTF-8 text) so they round-trip
exactly and can be manipulated in raw SQL (see the atomic counter UPSERTs in
core.py). Everything else is JSON-encoded. Pickle is intentionally never used.
"""

from __future__ import annotations

import json
from typing import Any

from ._schema import TYPE_BYTES, TYPE_FLOAT, TYPE_INT, TYPE_JSON, TYPE_STR
from .exceptions import SerializationError


def serialize(value: Any) -> tuple[bytes, int]:
    if isinstance(value, bytes):
        return value, TYPE_BYTES
    if isinstance(value, str):
        return value.encode("utf-8"), TYPE_STR
    # bool is a subclass of int; encode it as JSON so it round-trips as a bool.
    if isinstance(value, bool):
        return json.dumps(value).encode("utf-8"), TYPE_JSON
    if isinstance(value, int):
        return str(value).encode("utf-8"), TYPE_INT
    if isinstance(value, float):
        return repr(value).encode("utf-8"), TYPE_FLOAT
    try:
        encoded = json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise SerializationError(
            f"cannot serialize value of type {type(value).__name__!r}: {exc}. "
            "litecache only stores str/int/float/bytes natively and JSON-serializable "
            "objects otherwise (no pickle) -- serialize this value yourself first."
        ) from exc
    return encoded.encode("utf-8"), TYPE_JSON


def deserialize(blob: bytes, value_type: int) -> Any:
    if value_type == TYPE_BYTES:
        return blob
    if value_type == TYPE_STR:
        return blob.decode("utf-8")
    if value_type == TYPE_INT:
        return int(blob.decode("utf-8"))
    if value_type == TYPE_FLOAT:
        return float(blob.decode("utf-8"))
    if value_type == TYPE_JSON:
        return json.loads(blob.decode("utf-8"))
    raise SerializationError(f"unknown value_type {value_type!r} in stored row")
