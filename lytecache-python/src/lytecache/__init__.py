"""lytecache: Redis-like caching with zero infrastructure, backed by SQLite."""

from ._events import ChangeRecord, Event, Subscription
from .core import LyteCache
from .exceptions import (
    CacheFullError,
    GapError,
    LockTimeout,
    LyteCacheError,
    SchemaVersionError,
    SerializationError,
)

__version__ = "0.2.0"

__all__ = [
    "LyteCache",
    "LyteCacheError",
    "CacheFullError",
    "SerializationError",
    "SchemaVersionError",
    "LockTimeout",
    "GapError",
    "Event",
    "Subscription",
    "ChangeRecord",
    "__version__",
]
