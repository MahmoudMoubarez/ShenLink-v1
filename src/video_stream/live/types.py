from dataclasses import dataclass
from enum import Enum
import math
from typing import Any, Callable, Optional
from ..contracts.types import PixelFormat


@dataclass(frozen=True)
class FrameTiming:
    """Frame age and clock uncertainty measurement.

    Attributes:
        status: "MEASURED" if verified against active clock sample, or "UNKNOWN".
        origin: "sensor_capture" (declared by app) or "source_ingress" (pipeline entrance) or "unknown".
        source_timestamp_ns: Capture timestamp in sender monotonic nanoseconds.
        sender_clock_epoch: Random epoch identifier of sender clock.
        age_upper_ms: Conservative upper bound of age in milliseconds (or None if UNKNOWN).
        age_lower_ms: Lower bound of age in milliseconds (or None if UNKNOWN).
        uncertainty_ms: Uncertainty window width (+/- ms) (or None if UNKNOWN).
        measured_at_monotonic_s: Monotonic seconds at receiver when measurement was evaluated.
        reason: Explanation if status is UNKNOWN (e.g. "no_clock_sample", "clock_epoch_mismatch", etc.).
    """

    status: str
    origin: str
    source_timestamp_ns: Optional[int]
    sender_clock_epoch: Optional[str]
    age_upper_ms: Optional[float]
    age_lower_ms: Optional[float] = None
    uncertainty_ms: Optional[float] = None
    measured_at_monotonic_s: float = 0.0
    reason: str = ""


@dataclass(frozen=True)
class LiveFrame:
    """An application-owned decoded video frame.

    The `data` attribute contains an independent, immutable copy of the raw pixel
    bytes. It is completely safe to retain, queue across threads, or pass into UI
    renderers without risk of memory corruption or use-after-free.
    """

    path_id: str
    frame_id: Optional[int]
    width: int
    height: int
    pixel_format: PixelFormat | str
    stride: int
    data: bytes
    arrival_time: float  # monotonic seconds at receiver
    session_id: Optional[str] = None
    source_timestamp_ns: Optional[int] = None
    sender_clock_epoch: Optional[str] = None
    timestamp_origin: Optional[str] = None
    timing: Optional[FrameTiming] = None
    delivery_kind: str = "new_frame"  # Or quality_upgrade of the current capture.

    def to_image(self):
        """Convert pixel data to a PIL Image (requires Pillow)."""
        from PIL import Image

        mode = "RGB" if self.pixel_format in (PixelFormat.RGB, "RGB") else "RGBA"
        return Image.frombytes(
            mode,
            (self.width, self.height),
            self.data,
            "raw",
            mode,
            self.stride,
            1,
        )


@dataclass
class WebRTCPath:
    """Configuration for a single WebRTC network transport path.

    Attributes:
        path_id: Unique identifier for this link (e.g. "a", "b", "eth0", "lte").
        local_ip: Local network interface IP to bind ICE candidate gathering.
        remote_url: Remote signaling endpoint URL (used by receiver/client).
        adapter: Optional custom SenderPathAdapter or ReceiverPathAdapter.
                 If omitted, the default WebRtcSenderAdapter or WebRtcReceiverAdapter is used.
        requires_negotiation: True for WebRTC (requires SDP offer/answer).
    """

    path_id: str
    local_ip: str
    remote_url: Optional[str] = None
    adapter: Optional[Any] = None
    requires_negotiation: bool = True

    def __post_init__(self):
        if not self.path_id:
            raise ValueError("path_id must be non-empty")
        if not self.local_ip:
            raise ValueError("local_ip must be non-empty")


@dataclass
class CustomPath:
    """Configuration for a custom transport path adapter (TCP, UDP, or user-defined).

    Attributes:
        path_id: Unique identifier for this link (e.g. "a", "b", "tcp", "udp").
        adapter: Custom SenderPathAdapter or ReceiverPathAdapter instance.
        local_ip: Local network interface IP (default "127.0.0.1").
        remote_url: Optional remote endpoint/signaling URL.
        requires_negotiation: Whether this adapter requires SDP offer/answer exchange.
                              Defaults to False for custom socket/packet adapters.
    """

    path_id: str
    adapter: Any
    local_ip: str = "127.0.0.1"
    remote_url: Optional[str] = None
    requires_negotiation: bool = False

    def __post_init__(self):
        if not self.path_id:
            raise ValueError("path_id must be non-empty")
        if self.adapter is None:
            raise ValueError("adapter must be specified for CustomPath")


@dataclass(frozen=True)
class QualityBounds:
    """Receiver quality bounds and optimization preferences.

    The receiver selects the highest sustainable resolution and framerate within
    these limits on the primary path, keeping backup paths at minimum baseline.

    The policy threshold (`max_frame_age_ms`) bounds frame age up to delivery
    into the library on the station (`PathObservation.frame_age_upper_ms`). Frame
    age at UI presentation time can be evaluated via `LiveReceiver.measure_frame_age(frame)`,
    but UI presentation delay does not automatically feed the network transport policy.
    The sender cannot override or expand the age limits chosen by the receiver.
    """

    min_resolution: tuple[int, int] = (640, 360)
    max_resolution: tuple[int, int] = (1280, 720)
    min_fps: int = 24
    max_fps: int = 24
    frame_weight: float = 2.0
    detail_weight: float = 1.0
    freeze_seconds: float = 0.6
    max_frame_age_ms: float = 750.0
    warning_margin_ms: float = 150.0
    allow_unverified_age_fallback: bool = False
    allow_unknown_capacity_trial: bool = False
    allow_unverified_probe: bool = False  # for backward compatibility

    def __post_init__(self):
        min_w, min_h = self.min_resolution
        max_w, max_h = self.max_resolution
        if min_w <= 0 or min_h <= 0 or max_w <= 0 or max_h <= 0:
            raise ValueError("Resolutions must be strictly positive")
        if min_w > max_w or min_h > max_h:
            raise ValueError("min_resolution cannot exceed max_resolution")
        if self.min_fps <= 0 or self.max_fps <= 0:
            raise ValueError("FPS must be positive")
        if self.min_fps > self.max_fps:
            raise ValueError("min_fps cannot exceed max_fps")
        if self.frame_weight < 0 or self.detail_weight < 0 or (self.frame_weight + self.detail_weight) == 0:
            raise ValueError("Weights must be nonnegative and not both zero")
        if self.freeze_seconds <= 0:
            raise ValueError("freeze_seconds must be positive")
        if not math.isfinite(self.max_frame_age_ms) or self.max_frame_age_ms <= 0:
            raise ValueError("max_frame_age_ms must be positive and finite")
        if not math.isfinite(self.warning_margin_ms) or not (0 <= self.warning_margin_ms < self.max_frame_age_ms):
            raise ValueError("warning_margin_ms must be finite and 0 <= warning_margin_ms < max_frame_age_ms")

        # Map legacy allow_unverified_probe to specific sub-flags if enabled
        if self.allow_unverified_probe:
            if not self.allow_unverified_age_fallback:
                object.__setattr__(self, "allow_unverified_age_fallback", True)
            if not self.allow_unknown_capacity_trial:
                object.__setattr__(self, "allow_unknown_capacity_trial", True)


class VideoDeliveryMode(str, Enum):
    """Video delivery mode: real-time latest vs bounded ordered delivery."""
    LATEST = "latest"
    ORDERED_BOUNDED = "ordered_bounded"


@dataclass(frozen=True)
class VideoDeliveryConfig:
    """Configuration for receiver video delivery ordering and buffering.

    Attributes:
        mode: LATEST (freshest frame fast-path) or ORDERED_BOUNDED (capture-order delivery).
        max_reorder_wait_ms: Maximum bounded wait time in milliseconds for missing frame IDs.
        max_buffer_frames: Maximum number of pending unique frames retained in reorder buffer.
        max_buffer_bytes: Maximum total frame payload bytes retained in reorder buffer.
        on_event: Optional structured event callback for delivery diagnostics.
        latest_any_path: Opt-in decoded output from any verified newer frame
            in LATEST mode, requiring fixed equal-quality receiver bounds.
            Control-path selection remains independent of public output.
    """
    mode: VideoDeliveryMode = VideoDeliveryMode.LATEST
    max_reorder_wait_ms: int = 100
    max_buffer_frames: int = 8
    max_buffer_bytes: int = 32 * 1024 * 1024
    on_event: Optional[Callable[["VideoDeliveryEvent"], None]] = None
    # Opt-in for equal-quality redundant streams. Does not change control-path
    # selection or merge independently encoded H264 before decoding.
    latest_any_path: bool = False

    def __post_init__(self):
        if not isinstance(self.latest_any_path, bool):
            raise TypeError("latest_any_path must be boolean")
        if isinstance(self.mode, str):
            try:
                valid_mode = VideoDeliveryMode(self.mode)
                object.__setattr__(self, "mode", valid_mode)
            except ValueError:
                raise ValueError(
                    f"Invalid VideoDeliveryMode: {self.mode!r}. Must be 'latest' or 'ordered_bounded'"
                )
        elif not isinstance(self.mode, VideoDeliveryMode):
            raise TypeError(f"mode must be VideoDeliveryMode or str, got {type(self.mode)}")

        if self.latest_any_path and self.mode != VideoDeliveryMode.LATEST:
            raise ValueError("latest_any_path requires LATEST delivery")

        if not isinstance(self.max_reorder_wait_ms, (int, float)) or self.max_reorder_wait_ms <= 0:
            raise ValueError(f"max_reorder_wait_ms must be positive, got {self.max_reorder_wait_ms}")
        if not isinstance(self.max_buffer_frames, int) or self.max_buffer_frames <= 0:
            raise ValueError(f"max_buffer_frames must be positive integer, got {self.max_buffer_frames}")
        if not isinstance(self.max_buffer_bytes, int) or self.max_buffer_bytes <= 0:
            raise ValueError(f"max_buffer_bytes must be positive integer, got {self.max_buffer_bytes}")


@dataclass(frozen=True)
class VideoDeliveryEvent:
    """Structured delivery event emitted on ordering transitions and discards."""
    kind: str
    epoch: Optional[str]
    frame_id: Optional[int]
    path_id: Optional[str]
    count: int
    reason: str


@dataclass(frozen=True)
class VideoDeliveryStats:
    """Read-only counters for video delivery layer."""
    frames_delivered: int = 0
    duplicate_frames_dropped: int = 0
    late_frames_dropped: int = 0
    skipped_frame_ids: int = 0
    buffer_overflow_dropped: int = 0
    missing_order_metadata: int = 0
