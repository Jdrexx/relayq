from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class RetryPolicy:
    """Backoff policy using **full jitter** for retry delay calculation.

    Instead of naive exponential backoff (which causes thundering herds
    when N workers all retry simultaneously), we use full jitter:

        sleep = random_between(0, min(cap, base * 2^attempt))

    This is the AWS-recommended approach from the "Exponential Backoff
    and Jitter" blog post. The distribution is uniform over [0, cap],
    which spreads retries evenly across the window.

    CWE-799 (Interaction Frequency): Full jitter prevents retry storms
    that could overwhelm downstream services. Without jitter, N workers
    retrying on the same schedule produce N× the load at deterministic
    intervals. Full jitter smooths this to background noise.

    CWE-400 (Resource Exhaustion): Both base_seconds and cap_seconds
    are bounded, and max_attempts puts a hard ceiling on total retry
    window duration.
    """

    base_seconds: float = 1.0
    cap_seconds: float = 60.0
    max_attempts: int = 3
    jitter: bool = True  # set False for testing to get deterministic delays

    def delay(self, attempt: int) -> float:
        """Return the sleep duration before retry *attempt* (0-indexed).

        Guaranteed: 0 <= delay <= cap_seconds for all valid attempt values.
        """
        if attempt >= self.max_attempts:
            return self.cap_seconds  # saturate; caller should DLQ instead

        exponential = self.base_seconds * (2**attempt)
        capped = min(exponential, self.cap_seconds)

        if not self.jitter:
            return capped  # deterministic for tests

        # Full jitter: uniform random between 0 and the capped exponential.
        # This preserves the exponential backoff ceiling but eliminates
        # the coordinated-retry problem.
        return random.uniform(0, capped)

    def max_total_window(self) -> float:
        """Upper bound on total time consumed by all retries (no jitter).

        Useful for setting client-side timeouts.  Actual wall-clock time
        will be lower due to jitter.
        """
        total = 0.0
        for i in range(self.max_attempts):
            total += min(self.base_seconds * (2**i), self.cap_seconds)
        return total
