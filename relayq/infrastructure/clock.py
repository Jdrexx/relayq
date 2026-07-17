from __future__ import annotations

from datetime import datetime, timezone


class Clock:
    """Thin abstraction over system time to make tests deterministic.

    Every module that needs the current time should accept a Clock
    instance (or a factory) rather than calling datetime.utcnow()
    directly.  Tests inject a FakeClock to control time precisely.

    This is the "clock pattern" from Michael Nygard's "Release It!"
    — simple, testable, and avoids mocking the stdlib.
    """

    def utcnow(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        """Monotonic seconds for measuring elapsed time."""
        import time

        return time.monotonic()


class FakeClock(Clock):
    """Clock with controllable time for testing.

    Usage:
        clock = FakeClock()
        clock.advance(seconds=30)
        assert clock.utcnow() == BASE_TIME + timedelta(seconds=30)
    """

    BASE_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)

    def __init__(self):
        self._now = self.BASE_TIME
        self._mono = 0.0

    def utcnow(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def advance(self, *, seconds: float = 0, minutes: float = 0):
        from datetime import timedelta

        delta = timedelta(seconds=seconds, minutes=minutes)
        self._now += delta
        self._mono += seconds + minutes * 60
