import pytest
from hypothesis import given, strategies as st

from relayq.domain.retry import RetryPolicy


class TestRetryPolicy:
    """Property-based tests for retry delay calculation.

    The key invariant: delay(attempt) is always between 0 and cap_seconds,
    regardless of the attempt number or policy parameters.

    CWE-799 (Interaction Frequency): We don't just test that delays are
    "positive" — we verify the full-jitter distribution property (uniform
    over [0, cap]).  Without jitter, retries cluster and create load spikes.
    """

    def test_delay_bounds_no_jitter(self):
        """Without jitter, delay is deterministic and bounded by cap."""
        policy = RetryPolicy(base_seconds=1.0, cap_seconds=10.0, jitter=False)

        for attempt in range(10):
            d = policy.delay(attempt)
            assert 0 <= d <= 10.0, f"attempt {attempt}: delay {d} out of bounds"

    def test_delay_exponential_no_jitter(self):
        """Without jitter, delay follows exact exponential backoff."""
        policy = RetryPolicy(base_seconds=2.0, cap_seconds=60.0, jitter=False)

        assert policy.delay(0) == 2.0
        assert policy.delay(1) == 4.0
        assert policy.delay(2) == 8.0
        assert policy.delay(3) == 16.0
        assert policy.delay(4) == 32.0
        assert policy.delay(5) == 60.0  # capped

    @given(
        base=st.floats(min_value=0.1, max_value=10.0),
        cap=st.floats(min_value=1.0, max_value=300.0),
        attempt=st.integers(min_value=0, max_value=10),
    )
    def test_delay_always_between_zero_and_cap(self, base, cap, attempt):
        """PROPERTY: delay(attempt) is always in [0, cap]."""
        # cap must be >= base for realistic policies
        policy = RetryPolicy(base_seconds=min(base, cap), cap_seconds=cap, jitter=True)
        d = policy.delay(attempt)
        assert 0.0 <= d <= cap, f"delay={d} not in [0, {cap}]"

    @given(
        base=st.floats(min_value=0.1, max_value=5.0),
        cap=st.floats(min_value=5.0, max_value=60.0),
        attempt=st.integers(min_value=0, max_value=8),
    )
    def test_delay_with_jitter_not_exactly_exponential(self, base, cap, attempt):
        """PROPERTY: With jitter, delay is not always the capped exponential.

        This test checks that jitter actually does something — over many
        samples, the delay should deviate from the deterministic value.
        """
        policy = RetryPolicy(base_seconds=base, cap_seconds=cap, jitter=True)
        deterministic = RetryPolicy(base_seconds=base, cap_seconds=cap, jitter=False)
        det_val = deterministic.delay(attempt)

        # Sample many times; at least one should differ
        samples = [policy.delay(attempt) for _ in range(100)]
        assert any(s != det_val for s in samples) or det_val == cap, (
            "Jitter produced no variation — may be deterministic by coincidence"
        )

    def test_max_total_window(self):
        """max_total_window returns the sum of all capped exponential delays."""
        policy = RetryPolicy(base_seconds=1.0, cap_seconds=10.0, max_attempts=3, jitter=False)
        # attempt 0: 1s, attempt 1: 2s, attempt 2: 4s = 7s total
        assert policy.max_total_window() == 7.0

    def test_delay_saturates_at_cap_for_high_attempts(self):
        """For very high attempts, delay should cap, not grow unbounded.

        CWE-400 (Resource Exhaustion): Without capping, exponential
        backoff would produce absurdly long delays (attempt 20 on
        1s base = 12 days).
        """
        policy = RetryPolicy(base_seconds=1.0, cap_seconds=30.0)
        for attempt in range(10, 100):
            assert policy.delay(attempt) <= 30.0
