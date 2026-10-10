"""Core data types and structures for the transport-agnostic video streaming library."""

from dataclasses import dataclass
from enum import Enum, auto
from typing import FrozenSet, Optional


class PixelFormat(Enum):
    """Supported decoded pixel formats."""
    RGB = auto()
    RGBA = auto()
    BGR = auto()
    I420 = auto()


@dataclass
class DecodedImage:
    """Decoded image memory reference.
    
    Rule: The underlying data memoryview is valid only during the frame callback
    invocation. Consumers wishing to retain or queue the image must copy it explicitly.
    """
    width: int
    height: int
    pixel_format: PixelFormat
    stride: int
    data: memoryview


class ConnectionState(Enum):
    """Path connection lifecycle states."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DEGRADED = auto()
    FAILED = auto()
    CLOSED = auto()


class MediaHandling(Enum):
    """How the transport adapter manages media streams."""
    PIPELINE_MANAGED = auto()  # Native pipeline (e.g. GStreamer C pipeline); no media bytes in Python
    FRAME_CONSUMER = auto()    # Decoded frame level exchange
    PACKET_STREAM = auto()     # Packet/chunk based exchange (TCP/UDP)


@dataclass(frozen=True)
class AdapterCapabilities:
    """Explicitly declared capabilities of a transport path adapter."""
    media_handling: MediaHandling
    supports_dynamic_bitrate: bool = True
    supports_dynamic_resolution: bool = True
    supports_dynamic_fps: bool = True
    measurable_metrics: FrozenSet[str] = frozenset()
    control_channel: bool = False


@dataclass(frozen=True)
class QualityTarget:
    """Abstract quality target with physical dimensional values."""
    width: int
    height: int
    fps: int
    target_kbps: int

    def __post_init__(self):
        if self.width <= 0 or self.height <= 0 or self.fps <= 0 or self.target_kbps <= 0:
            raise ValueError("All QualityTarget dimensions must be positive integers")


@dataclass(frozen=True)
class QualityCommand:
    """Asynchronous quality adjustment command directed to a specific path."""
    path_id: str
    target: QualityTarget
    session_id: str
    sequence_id: int
    timestamp_ns: int


class QualityEventType(Enum):
    """Asynchronous events in the lifecycle of a quality command."""
    ACCEPTED = auto()   # Received and queued by sender after sequence/session verification
    APPLIED = auto()    # Pipeline/encoder reconfigured with the requested parameters
    OBSERVED = auto()   # Verified in receiver decoded frames
    REJECTED = auto()   # Rejected (session mismatch, stale sequence, or unsupported capability)
    FAILED = auto()     # Error during transport or encoder execution


@dataclass(frozen=True)
class QualityEvent:
    """Notification of a quality command state change."""
    path_id: str
    command: QualityCommand
    event_type: QualityEventType
    timestamp_ns: int
    detail: str = ""


@dataclass(frozen=True)
class FrameEvent:
    """Decoded frame event delivered to the receiver engine.
    
    frame_id represents source-frame identity and MUST be generated once at the source
    (before branching/tee) and be comparable across all paths carrying that source.
    frame_id=None indicates unverified ordering (e.g. stream without marker).
    """
    path_id: str
    frame_id: Optional[int] = None
    source_timestamp_ns: Optional[int] = None
    arrival_monotonic_ns: int = 0
    image: Optional[DecodedImage] = None
    session_id: Optional[str] = None
    sender_clock_epoch: Optional[str] = None
    timestamp_origin: Optional[str] = None

    @property
    def received_at(self) -> float:
        """Receiver arrival time in fractional seconds (for compatibility)."""
        return self.arrival_monotonic_ns / 1_000_000_000.0


@dataclass(frozen=True)
class PathMetrics:
    """Path health and diagnostic measurements.
    
    Fields are None unless explicitly measurable by the adapter. Target bitrate
    is never confused with measured network throughput.
    """
    path_id: str
    state: ConnectionState
    last_frame_received_at_ns: Optional[int] = None
    decoded_width: Optional[int] = None
    decoded_height: Optional[int] = None
    decoded_fps: Optional[float] = None
    frame_age_upper_ms: Optional[float] = None
    freshness_proxy_ms: Optional[float] = None
    estimated_capacity_bps: Optional[int] = None
    packets_received: Optional[int] = None
    rtp_packets_submitted: Optional[int] = None  # Submitted to WebRTC, not confirmed delivered.
    decoded_frames: Optional[int] = None
    packets_lost: Optional[int] = None
    rtt_ms: Optional[float] = None


@dataclass(frozen=True)
class EncodedPacket:
    """Optional encoded media packet used with PACKET_STREAM adapters."""
    frame_id: Optional[int]
    timestamp_ns: Optional[int]
    is_keyframe: bool
    payload: bytes
    path_id: Optional[str] = None
    sender_clock_epoch: Optional[str] = None
    timestamp_origin: Optional[str] = None
    session_id: Optional[str] = None


# Alias for custom transport implementations
TransportMetrics = PathMetrics
