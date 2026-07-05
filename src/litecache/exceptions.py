"""Exception hierarchy for litecache."""

from __future__ import annotations


class LiteCacheError(Exception):
    """Base class for all exceptions raised by litecache."""


class CacheFullError(LiteCacheError):
    """Raised when the cache is full and the eviction policy is ``noeviction``."""


class SerializationError(LiteCacheError):
    """Raised when a value cannot be serialized to, or deserialized from, storage."""


class SchemaVersionError(LiteCacheError):
    """Raised when a database file's schema version is newer than this library supports."""


class LockTimeout(LiteCacheError):
    """Raised by :meth:`LiteCache.lock` when the lock cannot be acquired in time."""
