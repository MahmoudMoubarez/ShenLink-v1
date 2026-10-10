"""Core contracts, protocols, and types for transport-agnostic streaming."""

from .base import (
    BasePathAdapter,
    PacketTransportExtension,
    ReceiverPathAdapter,
    SenderPathAdapter,
)
from .session import SequenceGuard
from .types import (
    AdapterCapabilities,
    ConnectionState,
    DecodedImage,
    EncodedPacket,
    FrameEvent,
    MediaHandling,
    PathMetrics,
    PixelFormat,
    QualityCommand,
    QualityEvent,
    QualityEventType,
    QualityTarget,
)

__all__ = [
    "AdapterCapabilities",
    "BasePathAdapter",
    "ConnectionState",
    "DecodedImage",
    "EncodedPacket",
    "FrameEvent",
    "MediaHandling",
    "PacketTransportExtension",
    "PathMetrics",
    "PixelFormat",
    "QualityCommand",
    "QualityEvent",
    "QualityEventType",
    "QualityTarget",
    "ReceiverPathAdapter",
    "SenderPathAdapter",
    "SequenceGuard",
]
