"""Background daemon thread that periodically calls a maintenance callback."""

from __future__ import annotations

import threading
from typing import Callable


class Sweeper:
    def __init__(self, interval: float, target: Callable[[], None]) -> None:
        self._interval = interval
        self._target = target
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="litecache-sweeper", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            self._target()

    def stop(self, timeout: float | None = 2.0) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)
