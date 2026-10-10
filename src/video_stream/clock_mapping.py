"""Conservative application-clock mapping for capture-age measurements.

Timestamps use Python's monotonic clock on each process. No system clock is
changed. The returned age is an upper bound under nonnegative path delays and
the configured drift allowance; it is not an assumed one-way network delay or RTT/2.
"""

from dataclasses import dataclass
import math
import threading
from typing import Optional


@dataclass(frozen=True)
class ClockSample:
    """Four-timestamp clock synchronization sample."""

    # t1/t4: receiver monotonic seconds; t2/t3: sender monotonic seconds.
    t1: float
    t2: float
    t3: float
    t4: float
    sender_clock_epoch: Optional[str] = None
    path_id: Optional[str] = None

    @property
    def rtt(self) -> float:
        return (self.t4 - self.t1) - (self.t3 - self.t2)

    @property
    def offset_lower(self) -> float:
        return self.t3 - self.t4

    @property
    def offset_upper(self) -> float:
        return self.t2 - self.t1

    @property
    def uncertainty(self) -> float:
        return (self.offset_upper - self.offset_lower) / 2


@dataclass(frozen=True)
class FrameAgeResult:
    """Result of frame age upper bound evaluation."""

    status: str  # "MEASURED" or "UNKNOWN"
    age_upper_ms: Optional[float] = None
    age_lower_ms: Optional[float] = None
    uncertainty_ms: Optional[float] = None
    reason: str = ""
    sample_age_s: Optional[float] = None
    sender_clock_epoch: Optional[str] = None


class ClockMapper:
    """Keep the lowest-delay recent four-timestamp exchange and compute conservative age bounds."""

    def __init__(
        self,
        stale_after_s: float,
        max_drift_ppm: float = 100.0,
        max_uncertainty_ms: float = 1500.0,
    ) -> None:
        if (
            not math.isfinite(stale_after_s)
            or stale_after_s <= 0
            or not math.isfinite(max_drift_ppm)
            or max_drift_ppm < 0
            or not math.isfinite(max_uncertainty_ms)
            or max_uncertainty_ms <= 0
        ):
            raise ValueError("invalid clock mapper limits")
        self.stale_after_s = stale_after_s
        self.max_drift_ppm = max_drift_ppm
        self.max_uncertainty_ms = max_uncertainty_ms
        self.active_epoch: Optional[str] = None
        self._retired_epochs: set[str] = set()
        self._samples: list[ClockSample] = []
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._samples.clear()
            self.active_epoch = None
            self._retired_epochs.clear()

    def add(
        self,
        t1: float,
        t2: float,
        t3: float,
        t4: float,
        sender_clock_epoch: Optional[str] = None,
        path_id: Optional[str] = None,
    ) -> bool:
        if not all(math.isfinite(x) for x in (t1, t2, t3, t4)):
            return False
        if t1 > t4 or t2 > t3:
            return False
        sample = ClockSample(t1, t2, t3, t4, sender_clock_epoch, path_id)
        if sample.offset_lower > sample.offset_upper:
            return False

        with self._lock:
            if sender_clock_epoch is not None:
                # Late response from an already retired epoch must NOT resurrect that epoch!
                if sender_clock_epoch in self._retired_epochs:
                    return False
                if self.active_epoch is not None and sender_clock_epoch != self.active_epoch:
                    # Clock epoch changed (e.g. sender restarted); retire old epoch and flush old samples!
                    self._retired_epochs.add(self.active_epoch)
                    self._samples.clear()
                    self.active_epoch = sender_clock_epoch
                elif self.active_epoch is None:
                    self.active_epoch = sender_clock_epoch
            self._samples.append(sample)
            self._samples = self._samples[-32:]
        return True

    record_sample = add

    def measure_age(
        self,
        capture_monotonic_ns: Optional[int],
        receiver_now_s: float,
        sender_clock_epoch: Optional[str] = None,
    ) -> FrameAgeResult:
        """Compute conservative upper bound and uncertainty of frame age.

        Returns FrameAgeResult with status='MEASURED' if a valid, consistent sample exists
        from the matching clock epoch; otherwise status='UNKNOWN' with reason.
        """
        if capture_monotonic_ns is None or capture_monotonic_ns < 0:
            return FrameAgeResult(
                status="UNKNOWN", reason="missing_or_negative_capture_timestamp"
            )
        if not math.isfinite(receiver_now_s) or receiver_now_s < 0:
            return FrameAgeResult(
                status="UNKNOWN", reason="invalid_receiver_time"
            )

        with self._lock:
            if (
                sender_clock_epoch is not None
                and (
                    sender_clock_epoch in self._retired_epochs
                    or (self.active_epoch is not None and sender_clock_epoch != self.active_epoch)
                )
            ):
                return FrameAgeResult(
                    status="UNKNOWN",
                    reason="clock_epoch_mismatch",
                    sender_clock_epoch=sender_clock_epoch,
                )

            if sender_clock_epoch is not None:
                # A sample without epoch is NOT allowed to establish age for an epoch-tagged frame:
                recent = [
                    s
                    for s in self._samples
                    if s.sender_clock_epoch == sender_clock_epoch
                    and 0 <= receiver_now_s - s.t4 <= self.stale_after_s
                ]
            else:
                # Legacy unverified frame without epoch: can use samples without epoch
                recent = [
                    s
                    for s in self._samples
                    if s.sender_clock_epoch is None
                    and 0 <= receiver_now_s - s.t4 <= self.stale_after_s
                ]
            all_samples = list(self._samples)

        if not recent:
            if not all_samples:
                return FrameAgeResult(status="UNKNOWN", reason="no_clock_sample")
            # All available samples are expired or mismatched
            sample_age = receiver_now_s - max(s.t4 for s in all_samples)
            if sample_age > self.stale_after_s:
                return FrameAgeResult(
                    status="UNKNOWN",
                    reason="stale_clock_sample",
                    sample_age_s=sample_age,
                )
            return FrameAgeResult(
                status="UNKNOWN",
                reason="clock_epoch_mismatch",
                sender_clock_epoch=sender_clock_epoch,
            )

        best = min(recent, key=lambda s: s.offset_upper - s.offset_lower)
        raw_uncertainty_ms = (best.offset_upper - best.offset_lower) * 1000 / 2
        if (best.offset_upper - best.offset_lower) * 1000 > self.max_uncertainty_ms:
            return FrameAgeResult(
                status="UNKNOWN",
                reason="excessive_uncertainty",
                uncertainty_ms=raw_uncertainty_ms,
                sample_age_s=receiver_now_s - best.t4,
                sender_clock_epoch=best.sender_clock_epoch,
            )

        # Elapsed time on receiver since probe reply:
        dt_recv = max(0.0, receiver_now_s - best.t4)
        # Elapsed time on sender between probe and frame capture:
        t_cap_s = capture_monotonic_ns / 1_000_000_000.0
        dt_cap = abs(t_cap_s - best.t2)
        # Total drift interval across both clocks:
        dt_drift = dt_recv + dt_cap
        drift_s = dt_drift * (self.max_drift_ppm / 1_000_000.0)

        # Sender clock minus receiver clock:
        # Age = (receiver_now_s - t_cap_s) + offset
        diff_s = receiver_now_s - t_cap_s
        upper_s = diff_s + best.offset_upper + drift_s
        lower_s = diff_s + best.offset_lower - drift_s
        unc_s = (upper_s - lower_s) / 2.0

        if upper_s < 0:
            # Capture timestamp is in the future even under upper bound
            return FrameAgeResult(
                status="UNKNOWN",
                reason="inconsistent_timestamps",
                sample_age_s=dt_recv,
                sender_clock_epoch=best.sender_clock_epoch,
            )

        lower_ms = max(0.0, lower_s * 1000.0)
        upper_ms = max(lower_ms, upper_s * 1000.0)

        return FrameAgeResult(
            status="MEASURED",
            age_upper_ms=upper_ms,
            age_lower_ms=lower_ms,
            uncertainty_ms=unc_s * 1000.0,
            reason="ok",
            sample_age_s=dt_recv,
            sender_clock_epoch=best.sender_clock_epoch,
        )

    def age_upper_ms(
        self, capture_monotonic_ns: int, receiver_now_s: float
    ) -> float | None:
        """Backward-compatible upper bound age in milliseconds (or None if UNKNOWN)."""
        res = self.measure_age(capture_monotonic_ns, receiver_now_s)
        return res.age_upper_ms if res.status == "MEASURED" else None
