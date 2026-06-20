from __future__ import annotations

import threading


class BlockingActiveCounter:
    def __init__(self, *, auto_release_at: int | None = None) -> None:
        self.active = 0
        self.peak = 0
        self._auto_release_at = auto_release_at
        self._condition = threading.Condition()
        self._released = threading.Event()

    def enter(self) -> None:
        with self._condition:
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self._auto_release_at is not None and self.active >= self._auto_release_at:
                self.release()
            self._condition.notify_all()

    def exit(self) -> None:
        with self._condition:
            self.active -= 1
            self._condition.notify_all()

    def release(self) -> None:
        self._released.set()

    def wait_released(self, *, timeout: float = 2.0) -> None:
        assert self._released.wait(timeout=timeout)

    def wait_until_peak(self, expected: int, *, timeout: float = 2.0) -> None:
        with self._condition:
            assert self._condition.wait_for(lambda: self.peak >= expected, timeout=timeout)
