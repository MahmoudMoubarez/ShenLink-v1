"""Common types, enums, protocols, and exceptions for multiflow_comm.

Directional messaging channels, delivery modes, ticket tracking, and QoS types.
Strictly decoupled from GStreamer and media pipeline specifics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import json
import threading
import time
from typing import Any, Callable, Optional, Protocol, runtime_checkable


class DeliveryMode(str, Enum):
    """Message delivery semantic mode."""
    ORDERED_ALL = "ORDERED_ALL"
    LATEST_ONLY = "LATEST_ONLY"


class ReceiptPolicy(str, Enum):
    """Receipt / acknowledgement policy."""
    NONE = "NONE"
    RECEIVED = "RECEIVED"


class PathPolicy(str, Enum):
    """Multi-path dispatch strategy."""
    PRIMARY_THEN_BACKUP = "PRIMARY_THEN_BACKUP"
    REDUNDANT = "REDUNDANT"


class TicketStatus(str, Enum):
    """Delivery ticket lifecycle states."""
    QUEUED = "QUEUED"
    DISPATCHED = "DISPATCHED"
    RECEIVED = "RECEIVED"
    SUPERSEDED = "SUPERSEDED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


class NetworkQoSStatus(str, Enum):
    """Network priority marking attempt status on outer IP packet.
    
    Ref: RFC 2474, RFC 8837.
    Does NOT claim end-to-end priority guarantees.
    """
    NOT_REQUESTED = "NOT_REQUESTED"
    MARK_APPLIED_LOCALLY = "MARK_APPLIED_LOCALLY"
    NETWORK_PRIORITY_UNAVAILABLE = "NETWORK_PRIORITY_UNAVAILABLE"


class NetworkPriorityScope(str, Enum):
    """Level at which an adapter supports applying network priority (DSCP/TOS)."""
    NONE = "NONE"
    FLOW = "FLOW"
    PER_MESSAGE = "PER_MESSAGE"


class ConnectionState(str, Enum):
    """Connection state of a transport path adapter."""
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


class FlowProcessingCapability(str, Enum):
    """How work for this flow is processed by the scheduler."""
    MANAGED_QUEUE = "MANAGED_QUEUE"
    NATIVE_UNMANAGED = "NATIVE_UNMANAGED"


class FrameKind(str, Enum):
    """Allowed wire frame kinds."""
    HELLO = "HELLO"
    DATA = "DATA"
    ACK = "ACK"
    ABORT = "ABORT"


@dataclass(frozen=True)
class AdapterCapabilities:
    """Reported transport capabilities of a message path adapter."""
    max_frame_size: int = 65536
    has_ack_return_path: bool = False
    network_priority_scope: NetworkPriorityScope = NetworkPriorityScope.NONE


@dataclass(frozen=True)
class SessionResetEvent:
    """Emitted when a new session handshake is negotiated."""
    channel_id: str
    session_id: str
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True)
class ChannelBlockedEvent:
    """Emitted when an ORDERED_ALL channel encounters unrecoverable failure and halts."""
    channel_id: str
    session_id: str
    reason: str
    last_sequence: int
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True)
class ErrorEvent:
    """Emitted on local callback errors or unhandled channel errors."""
    channel_id: str
    session_id: str
    error: Exception
    context: str = ""
    timestamp: float = field(default_factory=time.time)


class MultiflowCommError(Exception):
    """Base exception for multiflow_comm."""


class QueueFullError(MultiflowCommError):
    """Raised when channel queue capacity is reached."""


class ChannelBlockedError(MultiflowCommError):
    """Raised when attempting to send on a channel that is BLOCKED."""


class DeliveryFailedError(MultiflowCommError):
    """Raised when a message cannot be delivered or gap is detected."""


class NetworkPriorityError(MultiflowCommError):
    """Raised when require_network_priority is True but marking is unavailable."""


class ProtocolError(MultiflowCommError):
    """Raised on invalid wire format, unknown version, or corrupted frames."""


class SerializationError(MultiflowCommError):
    """Raised when payload or header serialization fails."""


class CodecError(MultiflowCommError):
    """Raised when an object codec fails to encode or decode."""


@runtime_checkable
class Codec(Protocol):
    """Codec protocol for arbitrary object serialization to bytes."""
    def encode(self, value: Any) -> bytes:
        ...

    def decode(self, data: bytes) -> Any:
        ...


class JsonCodec:
    """Standard JSON codec encoding to UTF-8 bytes."""
    def encode(self, value: Any) -> bytes:
        try:
            return json.dumps(value, ensure_ascii=False).encode("utf-8")
        except Exception as e:
            raise CodecError(f"JSON encode failed: {e}") from e

    def decode(self, data: bytes) -> Any:
        try:
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            raise CodecError(f"JSON decode failed: {e}") from e


class DeliveryTicket:
    """Tracks status and outcome of an asynchronous message dispatch."""

    def __init__(
        self,
        message_id: str,
        channel_id: str,
        session_id: str,
        sequence: int,
        priority: int,
        mode: DeliveryMode,
        receipt: ReceiptPolicy,
        latest_key: Optional[str] = None,
    ) -> None:
        self.message_id = message_id
        self.channel_id = channel_id
        self.session_id = session_id
        self.sequence = sequence
        self.priority = priority
        self.mode = mode
        self.receipt = receipt
        self.latest_key = latest_key

        self._status = TicketStatus.QUEUED
        self._network_qos_status = NetworkQoSStatus.NOT_REQUESTED
        self._error: Optional[Exception] = None
        self._created_at = time.time()
        self._dispatched_at: Optional[float] = None
        self._completed_at: Optional[float] = None

        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self._done_callbacks: list[Callable[[DeliveryTicket], None]] = []

    @property
    def status(self) -> TicketStatus:
        with self._lock:
            return self._status

    @property
    def network_qos_status(self) -> NetworkQoSStatus:
        with self._lock:
            return self._network_qos_status

    @property
    def error(self) -> Optional[Exception]:
        with self._lock:
            return self._error

    @property
    def created_at(self) -> float:
        return self._created_at

    @property
    def dispatched_at(self) -> Optional[float]:
        with self._lock:
            return self._dispatched_at

    @property
    def completed_at(self) -> Optional[float]:
        with self._lock:
            return self._completed_at

    def set_dispatched(self, qos_status: NetworkQoSStatus = NetworkQoSStatus.NOT_REQUESTED) -> None:
        with self._lock:
            if self._status in (TicketStatus.SUPERSEDED, TicketStatus.FAILED, TicketStatus.EXPIRED):
                return
            self._status = TicketStatus.DISPATCHED
            self._network_qos_status = qos_status
            self._dispatched_at = time.time()
            if self.receipt == ReceiptPolicy.NONE:
                # In ReceiptPolicy.NONE, dispatch completes the transmission lifecycle
                self._completed_at = self._dispatched_at
                self._done_event.set()
                callbacks = list(self._done_callbacks)
            else:
                callbacks = []
        for cb in callbacks:
            try:
                cb(self)
            except Exception:
                pass

    def set_received(self) -> None:
        with self._lock:
            if self._status in (TicketStatus.SUPERSEDED, TicketStatus.FAILED, TicketStatus.EXPIRED):
                return
            self._status = TicketStatus.RECEIVED
            self._completed_at = time.time()
            self._done_event.set()
            callbacks = list(self._done_callbacks)
        for cb in callbacks:
            try:
                cb(self)
            except Exception:
                pass

    def set_superseded(self) -> None:
        with self._lock:
            if self._is_terminal():
                return
            self._status = TicketStatus.SUPERSEDED
            self._completed_at = time.time()
            self._done_event.set()
            callbacks = list(self._done_callbacks)
        for cb in callbacks:
            try:
                cb(self)
            except Exception:
                pass

    def set_failed(self, error: Exception) -> None:
        with self._lock:
            if self._is_terminal():
                return
            self._status = TicketStatus.FAILED
            self._error = error
            self._completed_at = time.time()
            self._done_event.set()
            callbacks = list(self._done_callbacks)
        for cb in callbacks:
            try:
                cb(self)
            except Exception:
                pass

    def set_expired(self) -> None:
        with self._lock:
            if self._is_terminal():
                return
            self._status = TicketStatus.EXPIRED
            self._completed_at = time.time()
            self._done_event.set()
            callbacks = list(self._done_callbacks)
        for cb in callbacks:
            try:
                cb(self)
            except Exception:
                pass

    def _is_terminal(self) -> bool:
        if self.receipt == ReceiptPolicy.NONE:
            return self._status in (
                TicketStatus.DISPATCHED,
                TicketStatus.SUPERSEDED,
                TicketStatus.EXPIRED,
                TicketStatus.FAILED,
            )
        return self._status in (
            TicketStatus.RECEIVED,
            TicketStatus.SUPERSEDED,
            TicketStatus.EXPIRED,
            TicketStatus.FAILED,
        )

    def is_done(self) -> bool:
        with self._lock:
            return self._is_terminal()

    def wait(self, timeout: Optional[float] = None) -> TicketStatus:
        """Wait synchronously for the ticket to reach a terminal state."""
        self._done_event.wait(timeout=timeout)
        with self._lock:
            return self._status

    def add_done_callback(self, callback: Callable[[DeliveryTicket], None]) -> None:
        """Register a callback invoked when ticket enters a terminal state."""
        invoke_now = False
        with self._lock:
            if self._is_terminal():
                invoke_now = True
            else:
                self._done_callbacks.append(callback)
        if invoke_now:
            try:
                callback(self)
            except Exception:
                pass
