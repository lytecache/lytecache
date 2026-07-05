from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Allow `import _workers` for multiprocessing targets (see test_concurrency.py).
sys.path.insert(0, str(Path(__file__).parent))

from litecache import LiteCache  # noqa: E402


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "cache.db"


@pytest.fixture
def cache(db_path: Path):
    c = LiteCache(db_path, sweep_interval=None)
    yield c
    c.close()
