"""Resolution of the zero-config default database path.

Layout: ``<platform cache dir>/litecache/<project-id>.db`` where the platform
cache dir is resolved with the standard library only, and ``<project-id>`` is
a short hash of the resolved current working directory -- so two different
projects on the same machine never collide, without any configuration.
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

_ENV_VAR = "LITECACHE_PATH"


def platform_cache_dir() -> Path:
    """Return the OS-appropriate cache directory, stdlib only."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches"
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            return Path(local_app_data)
        return Path.home() / "AppData" / "Local"
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        return Path(xdg_cache_home)
    return Path.home() / ".cache"


def project_id(cwd: Path) -> str:
    """A short, stable hash identifying a project by its resolved working directory."""
    digest = hashlib.sha256(str(cwd.resolve()).encode("utf-8")).hexdigest()
    return digest[:16]


def default_path(cwd: Path | None = None) -> Path:
    """Resolve the default database path.

    Honors the ``LITECACHE_PATH`` environment variable override; otherwise
    derives a per-project path from the platform cache directory and the
    current working directory.
    """
    override = os.environ.get(_ENV_VAR)
    if override:
        return Path(override).expanduser()
    base = platform_cache_dir() / "litecache"
    pid = project_id(cwd if cwd is not None else Path.cwd())
    return base / f"{pid}.db"
