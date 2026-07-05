"""litecache: Redis-like caching with zero infrastructure, backed by SQLite."""

from .core import LiteCache
from .exceptions import (
    CacheFullError,
    LiteCacheError,
    LockTimeout,
    SchemaVersionError,
    SerializationError,
)

__version__ = "0.1.0"

__all__ = [
    "LiteCache",
    "LiteCacheError",
    "CacheFullError",
    "SerializationError",
    "SchemaVersionError",
    "LockTimeout",
    "__version__",
]
