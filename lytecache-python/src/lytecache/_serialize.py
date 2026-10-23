"""Value <-> (blob, value_type) conversion.

See SPEC.md for the full type-code table. Codes 0-4 are cross-language
portable: str/int/float/bytes/bool are stored as native UTF-8 text, and
dicts, lists, dataclasses, and plain objects are JSON-encoded. Code 5
(pickle) is a Python-only opt-in escape hatch that is never written unless
the cache is constructed with serializer="pickle". Code 6 is reserved for
other languages' native serializers (e.g. Java); lytecache never writes it,
and reading it always raises SerializationError rather than returning raw
bytes silently.

SECURITY: unpickling can execute arbitrary code. lytecache never pickles
under serializer="auto" (the default) or serializer="json". Only
serializer="pickle" ever writes pickled data, and reading pickled data back
requires either serializer="pickle" or serializer="auto" with
allow_pickle=True. Treat any cache file that might contain pickled data like
application code: never open one from an untrusted source.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from typing import Any, get_type_hints

from ._schema import TYPE_BYTES, TYPE_FLOAT, TYPE_INT, TYPE_JAVA, TYPE_JSON, TYPE_PICKLE, TYPE_STR
from .exceptions import SerializationError


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if hasattr(value, "__dict__"):
        return vars(value)
    raise TypeError(f"object of type {type(value).__name__!r} is not JSON-serializable")


def _encode_json(value: Any) -> bytes:
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    return text.encode("utf-8")


def encode_json_text(value: Any) -> str:
    """Encode one hash field value as JSON text, with the same rules as code 4.

    NaN and infinities are rejected: they are not valid JSON, and a document
    containing them would not be readable by SQLite's JSON functions or by
    the other lytecache implementations.
    """
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise SerializationError(
            f"cannot store a value of type {type(value).__name__!r} in a hash field: {exc}. "
            "Hash fields hold JSON values (str, int, float, bool, None, dict, list, "
            "dataclasses, plain objects); bytes and pickled values are not supported."
        ) from exc


def serialize(value: Any, *, mode: str) -> tuple[bytes, int]:
    """Encode a Python value to (blob, value_type) under the given serializer mode.

    mode is one of "auto", "json", "pickle". "auto" and "json" behave
    identically here (both refuse to pickle); "pickle" falls back to
    pickling a value that isn't natively storable or JSON-representable.
    """
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
        return _encode_json(value), TYPE_JSON
    except TypeError as exc:
        if mode == "pickle":
            return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), TYPE_PICKLE
        raise SerializationError(
            f"cannot serialize value of type {type(value).__name__!r}: {exc}. "
            "lytecache stores str/int/float/bytes/bool natively and JSON-encodes "
            "dicts, lists, dataclasses, and plain objects (via vars()) otherwise. "
            "Either make this value JSON-representable, or construct "
            "LyteCache(serializer='pickle') to opt into pickling as a last resort."
        ) from exc


def _resolve_type_hints(cls: type) -> dict[str, Any]:
    try:
        return get_type_hints(cls)
    except Exception:
        return {}


def _build_from_cls(data: Any, cls: type) -> Any:
    if not isinstance(data, dict):
        raise SerializationError(
            f"cannot reconstruct {cls.__name__!r} from a stored {type(data).__name__}; "
            "cls reconstruction requires the stored value to be a JSON object"
        )
    kwargs = dict(data)
    if dataclasses.is_dataclass(cls):
        hints = _resolve_type_hints(cls)
        for field in dataclasses.fields(cls):
            if field.name not in kwargs or not isinstance(kwargs[field.name], dict):
                continue
            field_type = hints.get(field.name, field.type)
            if isinstance(field_type, type) and dataclasses.is_dataclass(field_type):
                kwargs[field.name] = _build_from_cls(kwargs[field.name], field_type)
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise SerializationError(
            f"cannot reconstruct {cls.__name__!r} from stored data {data!r}: {exc}"
        ) from exc


def deserialize(
    blob: bytes,
    value_type: int,
    *,
    mode: str,
    allow_pickle: bool,
    cls: type[Any] | None = None,
) -> Any:
    """Decode a stored (blob, value_type) back to a Python value.

    mode/allow_pickle gate whether a pickled (value_type=5) row may be read:
    "json" mode never allows it; "auto" mode allows it only with
    allow_pickle=True; "pickle" mode always allows it.
    """
    if value_type == TYPE_BYTES:
        return blob
    if value_type == TYPE_STR:
        return blob.decode("utf-8")
    if value_type == TYPE_INT:
        return int(blob.decode("utf-8"))
    if value_type == TYPE_FLOAT:
        return float(blob.decode("utf-8"))
    if value_type == TYPE_JSON:
        data = json.loads(blob.decode("utf-8"))
        if cls is None:
            return data
        return _build_from_cls(data, cls)
    if value_type == TYPE_PICKLE:
        if mode == "json":
            raise SerializationError(
                "this value was stored with pickle (value_type=5), but this LyteCache "
                "was opened with serializer='json', which never reads pickled data; "
                "reopen with serializer='auto' (and allow_pickle=True) or serializer='pickle'"
            )
        if mode == "auto" and not allow_pickle:
            raise SerializationError(
                "this value was stored with pickle (value_type=5); reading it under "
                "serializer='auto' requires allow_pickle=True, or reopen with "
                "serializer='pickle'"
            )
        return pickle.loads(blob)
    if value_type == TYPE_JAVA:
        raise SerializationError(
            "value_type=6 is a Java-native serialization format and cannot be read from Python"
        )
    raise SerializationError(f"unknown value_type {value_type!r} in stored row")
