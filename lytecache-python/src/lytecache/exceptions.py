"""Exception hierarchy for lytecache."""

from __future__ import annotations


class LyteCacheError(Exception):
    """Base class for all exceptions raised by lytecache."""


class CacheFullError(LyteCacheError):
    """Raised when the cache is full and the eviction policy is ``noeviction``."""


class SerializationError(LyteCacheError):
    """Raised when a value cannot be serialized to, or deserialized from, storage."""


class SchemaVersionError(LyteCacheError):
    """Raised when a database file's schema version is newer than this library supports."""


class LockTimeout(LyteCacheError):
    """Raised by :meth:`LyteCache.lock` when the lock cannot be acquired in time."""


class GapError(LyteCacheError):
    """Raised by a :class:`Subscription` when events it should have seen are gone.

    Events are deleted by retention (age or count) whether or not anyone read
    them. A subscriber resuming from a cursor older than the oldest retained
    event -- or one that falls that far behind while running -- gets this
    error instead of being silently resumed at the wrong place. To accept the
    loss and carry on, watch again with ``from_seq=err.resume_seq``.
    """

    def __init__(self, requested_seq: int, oldest_seq: int, *, ahead: bool = False) -> None:
        self.requested_seq = requested_seq
        self.oldest_seq = oldest_seq
        self.resume_seq = oldest_seq - 1
        self.ahead = ahead
        if ahead:
            msg = (
                f"cursor {requested_seq} is ahead of the last event ever published "
                f"({oldest_seq - 1}); the database file was probably recreated"
            )
        else:
            msg = (
                f"events after seq {requested_seq} are no longer retained (oldest retained "
                f"is {oldest_seq}); events {requested_seq + 1}..{oldest_seq - 1} were lost"
            )
        super().__init__(msg)
