"""Minimal abstract contracts and protocols for path transport adapters."""

from typing import Callable, Optional, Protocol, runtime_checkable

from .types import (
    AdapterCapabilities,
    ConnectionState,
    EncodedPacket,
    FrameEvent,
    PathMetrics,
    QualityCommand,
    QualityEvent,
)


@runtime_checkable
class BasePathAdapter(Protocol):
    """Common foundation for all path adapters."""
    path_id: str
    capabilities: AdapterCapabilities
    state: ConnectionState

    def open(self, session_id: str) -> None:
        """Initialize the path under the given session."""
        ...

    def close(self) -> None:
        """Tear down the path and release native resources."""
        ...

    def set_state_callback(self, callback: Callable[[str, ConnectionState, str], None]) -> None:
        """Register a callback for path connection state transitions."""
        ...


@runtime_checkable
class SenderPathAdapter(BasePathAdapter, Protocol):
    """Sender-side path contract.
    
    Does not force media copying to Python for pipeline-managed adapters (e.g. WebRTC).
    """
    def submit_quality_command(self, command: QualityCommand) -> bool:
        """Submit a quality command for asynchronous execution.
        
        Returns True if successfully queued/scheduled for transmission/application.
        """
        ...

    def set_quality_event_callback(self, callback: Callable[[QualityEvent], None]) -> None:
        """Register a callback for quality command lifecycle events (ACCEPTED, APPLIED, REJECTED)."""
        ...


@runtime_checkable
class ReceiverPathAdapter(BasePathAdapter, Protocol):
    """Receiver-side path contract."""
    def get_metrics(self, now_ns: int) -> PathMetrics:
        """Retrieve latest measurable diagnostics for this path."""
        ...

    def send_quality_command(self, command: QualityCommand) -> bool:
        """Transmit a quality command across the control channel to the sender."""
        ...

    def send_selection_feedback(self, active_path: str, sequence_id: int, session_id: str) -> None:
        """Notify sender which path is currently selected by the receiver."""
        ...

    def set_frame_callback(self, callback: Callable[[FrameEvent], None]) -> None:
        """Register listener for incoming decoded frames."""
        ...

    def set_quality_event_callback(self, callback: Callable[[QualityEvent], None]) -> None:
        """Register listener for quality lifecycle notifications."""
        ...

    def set_resolution_callback(self, callback: Callable[[str, int, int], None]) -> None:
        """Register listener for changes in observed decoded dimensions (path_id, width, height)."""
        ...


@runtime_checkable
class PacketTransportExtension(Protocol):
    """Optional extension for adapters operating in PACKET_STREAM mode (TCP, UDP)."""
    def send_packet(self, packet: EncodedPacket) -> bool:
        """Send an encoded media packet."""
        ...

    def push_encoded_unit(self, packet: EncodedPacket) -> bool:
        """Receive an encoded media access unit from the sender pipeline."""
        ...

    def set_packet_callback(self, callback: Callable[[EncodedPacket], None]) -> None:
        """Register callback for received encoded media packets."""
        ...
