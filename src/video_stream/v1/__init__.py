"""Public first-version video API.

Use this namespace rather than the historical ``video_stream`` root exports.
These are the real endpoint classes, not another codec or policy wrapper.
Importing this module does not open transports or initialize GStreamer.
"""
from ..configuration import ReceiverSettings, SenderSettings
from ..endpoints import VideoReceiver, VideoSender
from ..transport import (
    SendStatus, TransportAdapter, TransportCallbacks, TransportCapabilities,
    TransportState, validate_adapter,
)
from ..adapters.udp_transport import UdpTransport
from ..adapters.tcp_transport import TcpTransport
from ..shared.ordered import DeliveryMode
from ..shared.delivery import DelayPolicy
from ..shared.fps import AdaptationPriority
from ..shared.negotiation import ReceiverRequirements
from ..live.sources import CameraSource, LiveVideoSource
from ..live.types import LiveFrame, FrameTiming

__all__ = [
    "VideoSender", "VideoReceiver", "SenderSettings", "ReceiverSettings",
    "ReceiverRequirements", "DelayPolicy", "CameraSource", "LiveVideoSource",
    "LiveFrame", "FrameTiming", "UdpTransport", "TcpTransport", "DeliveryMode", "TransportAdapter",
    "TransportCallbacks", "TransportCapabilities", "TransportState",
    "SendStatus", "validate_adapter",
    "AdaptationPriority",
]
