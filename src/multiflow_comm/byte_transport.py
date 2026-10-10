"""Protocol-independent byte-message contract shared by video and commands.

No codecs, video policy, priority scheduler, sockets or retry mechanism live here.
Adapters frame their transport into complete messages, including over TCP.
ACCEPTED means local acceptance, never remote application delivery.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Protocol, runtime_checkable


class TransportState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    FAILED = "failed"
    CLOSED = "closed"


class SendStatus(str, Enum):
    ACCEPTED = "accepted"
    BACKPRESSURE = "backpressure"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class TransportCapabilities:
    """Connection-level capabilities, not end-to-end delivery guarantees.

    max_message_bytes includes the library's eventual envelope. A library may
    fragment media into multiple messages within that bound. Incoming messages
    must be bounded too. Queue limits belong to the adapter implementation.
    secure=True means this configuration authenticates the peer and protects
    message confidentiality/integrity; a session tag alone is not security.
    """
    reliable: bool = False
    ordered: bool = False
    secure: bool = False
    max_message_bytes: int = 1400

    def __post_init__(self):
        if any(type(getattr(self,name)) is not bool for name in ("reliable","ordered","secure")):
            raise ValueError("Transport capability flags must be boolean")
        if type(self.max_message_bytes) is not int or self.max_message_bytes <= 0:
            raise ValueError("A positive message size bound is required")


@dataclass(frozen=True)
class TransportCallbacks:
    """Callbacks may run on adapter threads; the library must not block them.

    on_message receives one complete immutable bytes message, not a TCP chunk.
    No callbacks may occur after close() returns. Transport failure is reported
    to the library, which decides when to request reconnect().
    """
    on_message: Callable[[bytes], None]
    on_state: Callable[[TransportState, str], None]
    on_error: Callable[[Exception], None]

    def __post_init__(self):
        if not all(callable(c) for c in (self.on_message,self.on_state,self.on_error)):
            raise ValueError("All transport callbacks must be callable")


@runtime_checkable
class TransportAdapter(Protocol):
    """One developer-owned bidirectional path, opaque to media policy.

    open/reconnect initiate connection setup without an unbounded blocking wait.
    send is bounded and nonblocking, thread-safe with close/reconnect. On
    BACKPRESSURE or UNAVAILABLE the submitted message was NOT accepted.
    close is idempotent, quiesces callbacks and releases transport resources.
    Reconnect must not mix bytes/messages from the old connection with the new
    connection. The library owns session epochs and video/control multiplexing.
    A live transport does not prove usable video: the library owns media probes.
    """
    path_id: str
    capabilities: TransportCapabilities
    state: TransportState

    def open(self, callbacks: TransportCallbacks) -> None: ...
    def send(self, message: bytes) -> SendStatus: ...
    def reconnect(self) -> None: ...
    def close(self) -> None: ...


def validate_adapter(adapter: TransportAdapter) -> TransportAdapter:
    """Validate declared shape; cannot certify an adapter's runtime promises."""
    if not isinstance(adapter,TransportAdapter):
        raise TypeError("Adapter does not implement the transport contract")
    if not isinstance(adapter.path_id,str) or not adapter.path_id.strip():
        raise ValueError("Adapter needs a nonempty path_id")
    if not isinstance(adapter.capabilities,TransportCapabilities):
        raise TypeError("Adapter needs TransportCapabilities")
    if not isinstance(adapter.state,TransportState):
        raise TypeError("Adapter state must be TransportState")
    if not all(callable(getattr(adapter,name)) for name in ("open","send","reconnect","close")):
        raise TypeError("Adapter operations must be callable")
    return adapter
