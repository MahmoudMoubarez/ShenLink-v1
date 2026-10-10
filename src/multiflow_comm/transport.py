"""Transport adapter contracts and simulated/fake adapters for multiflow_comm.

Defines MessageSenderAdapter and MessageReceiverAdapter protocols, completely
distinct from video adapters.
"""

from __future__ import annotations

import threading
from typing import Callable, Optional, Protocol, runtime_checkable

from .types import (
    AdapterCapabilities,
    ConnectionState,
    NetworkPriorityScope,
    NetworkQoSStatus,
)


@runtime_checkable
class MessageSenderAdapter(Protocol):
    """Sender path adapter contract for a single network route.

    Optional send_probe(frame) implementations must handle reconnecting their
    own transport on FAILED/DISCONNECTED before sending the health frame;
    the library schedules and evaluates these probes. Without send_probe, the
    library retries open() on those states instead.
    """
    path_id: str
    capabilities: AdapterCapabilities
    state: ConnectionState

    def open(self) -> None:
        """Initialize or retry after FAILED/DISCONNECTED; safe to call repeatedly."""
        ...

    def close(self) -> None:
        """Shut down adapter and release network resources (idempotent)."""
        ...

    def set_state_callback(self, callback: Callable[[str, ConnectionState, str], None]) -> None:
        """Register callback for connection state changes (path_id, new_state, reason)."""
        ...

    def send_frame(
        self,
        frame: bytes,
        priority: int = 0,
        dscp: Optional[int] = None,
        require_marking: bool = False,
    ) -> tuple[bool, NetworkQoSStatus]:
        """Transmit a frame. If marking is required, never send it unmarked."""
        ...

    def can_apply_dscp(self, dscp: int) -> bool:
        """Check whether this adapter can apply the specified DSCP marking."""
        ...


@runtime_checkable
class MessageReceiverAdapter(Protocol):
    """Receiver path adapter contract for a single network route."""
    path_id: str
    capabilities: AdapterCapabilities
    state: ConnectionState

    def open(self) -> None:
        """Bind or retry after FAILED; safe to call repeatedly."""
        ...

    def close(self) -> None:
        """Shut down adapter and release network resources (idempotent)."""
        ...

    def set_state_callback(self, callback: Callable[[str, ConnectionState, str], None]) -> None:
        """Register callback for connection state changes (path_id, new_state, reason)."""
        ...

    def set_frame_callback(self, callback: Callable[[str, bytes], None]) -> None:
        """Register callback for incoming binary frames (path_id, frame_bytes)."""
        ...

    def send_frame(
        self,
        frame: bytes,
        priority: int = 0,
        dscp: Optional[int] = None,
    ) -> tuple[bool, NetworkQoSStatus]:
        """Optional return path to send ACK frames back to sender (if has_ack_return_path)."""
        ...


class FakeMessageSenderAdapter:
    """Deterministic in-memory sender adapter for tests and lab verification."""

    def __init__(
        self,
        path_id: str,
        capabilities: Optional[AdapterCapabilities] = None,
        simulate_send_failure: bool = False,
        simulate_marking_failure: bool = False,
    ) -> None:
        self.path_id = path_id
        self.capabilities = capabilities or AdapterCapabilities(
            max_frame_size=65536,
            has_ack_return_path=True,
            network_priority_scope=NetworkPriorityScope.PER_MESSAGE,
        )
        self.state = ConnectionState.DISCONNECTED
        self.simulate_send_failure = simulate_send_failure
        self.simulate_marking_failure = simulate_marking_failure

        self._state_callback: Optional[Callable[[str, ConnectionState, str], None]] = None
        self._ack_callback: Optional[Callable[[str, bytes], None]] = None
        self._peer_receiver: Optional[FakeMessageReceiverAdapter] = None
        self.sent_frames: list[tuple[bytes, int]] = []
        self._flow_dscp: Optional[int] = None
        self._lock = threading.Lock()

    def set_peer(self, receiver: FakeMessageReceiverAdapter) -> None:
        """Pair with a receiver adapter for end-to-end loopback without network sockets."""
        self._peer_receiver = receiver
        receiver._peer_sender = self

    def set_state_callback(self, callback: Callable[[str, ConnectionState, str], None]) -> None:
        self._state_callback = callback

    def set_ack_callback(self, callback: Callable[[str, bytes], None]) -> None:
        self._ack_callback = callback

    def _on_inbound_ack(self, path_id: str, frame: bytes) -> None:
        cb = self._ack_callback
        if cb:
            cb(path_id, frame)

    def can_apply_dscp(self, dscp: int) -> bool:
        """Check whether this adapter can apply the specified DSCP marking."""
        with self._lock:
            if self.simulate_marking_failure:
                return False
            if self.capabilities.network_priority_scope == NetworkPriorityScope.NONE:
                return False
            if self.capabilities.network_priority_scope == NetworkPriorityScope.FLOW:
                return self._flow_dscp is None or self._flow_dscp == dscp
            return True

    def open(self) -> None:
        with self._lock:
            self.state = ConnectionState.CONNECTED
        if self._state_callback:
            self._state_callback(self.path_id, ConnectionState.CONNECTED, "Opened")

    def close(self) -> None:
        with self._lock:
            self.state = ConnectionState.CLOSED
        if self._state_callback:
            self._state_callback(self.path_id, ConnectionState.CLOSED, "Closed")

    def send_frame(
        self,
        frame: bytes,
        priority: int = 0,
        dscp: Optional[int] = None,
        require_marking: bool = False,
    ) -> tuple[bool, NetworkQoSStatus]:
        with self._lock:
            if self.state != ConnectionState.CONNECTED or self.simulate_send_failure:
                return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE

            if len(frame) > self.capabilities.max_frame_size:
                return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE

            # Do not record/deliver a frame until strict marking has succeeded.
            if dscp is None:
                qos_status = NetworkQoSStatus.NOT_REQUESTED
            elif self.simulate_marking_failure:
                qos_status = NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            elif self.capabilities.network_priority_scope == NetworkPriorityScope.NONE:
                qos_status = NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            elif self.capabilities.network_priority_scope == NetworkPriorityScope.FLOW:
                target_val = dscp
                is_hello = b'"kind":"HELLO"' in frame
                if is_hello:
                    qos_status = NetworkQoSStatus.MARK_APPLIED_LOCALLY
                elif self._flow_dscp is None:
                    self._flow_dscp = target_val
                    qos_status = NetworkQoSStatus.MARK_APPLIED_LOCALLY
                elif self._flow_dscp == target_val:
                    qos_status = NetworkQoSStatus.MARK_APPLIED_LOCALLY
                else:
                    # Connection-level FLOW cannot differentiate per-message
                    qos_status = NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            else:  # PER_MESSAGE
                qos_status = NetworkQoSStatus.MARK_APPLIED_LOCALLY

            if require_marking and qos_status != NetworkQoSStatus.MARK_APPLIED_LOCALLY:
                return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE

            self.sent_frames.append((frame, priority))

            peer = self._peer_receiver

        # Deliver to peer receiver if connected
        if peer and peer.state == ConnectionState.CONNECTED:
            peer._on_inbound_frame(self.path_id, frame)

        return True, qos_status




class FakeMessageReceiverAdapter:
    """Deterministic in-memory receiver adapter for tests and lab verification."""

    def __init__(
        self,
        path_id: str,
        capabilities: Optional[AdapterCapabilities] = None,
    ) -> None:
        self.path_id = path_id
        self.capabilities = capabilities or AdapterCapabilities(
            max_frame_size=65536,
            has_ack_return_path=True,
            network_priority_scope=NetworkPriorityScope.PER_MESSAGE,
        )
        self.state = ConnectionState.DISCONNECTED
        self._state_callback: Optional[Callable[[str, ConnectionState, str], None]] = None
        self._frame_callback: Optional[Callable[[str, bytes], None]] = None
        self._peer_sender: Optional[FakeMessageSenderAdapter] = None
        self.sent_return_frames: list[tuple[bytes, int]] = []
        self._lock = threading.Lock()

    def set_state_callback(self, callback: Callable[[str, ConnectionState, str], None]) -> None:
        self._state_callback = callback

    def set_frame_callback(self, callback: Callable[[str, bytes], None]) -> None:
        self._frame_callback = callback

    def open(self) -> None:
        with self._lock:
            self.state = ConnectionState.CONNECTED
        if self._state_callback:
            self._state_callback(self.path_id, ConnectionState.CONNECTED, "Opened")

    def close(self) -> None:
        with self._lock:
            self.state = ConnectionState.CLOSED
        if self._state_callback:
            self._state_callback(self.path_id, ConnectionState.CLOSED, "Closed")

    def send_frame(self, frame: bytes, priority: int = 0) -> tuple[bool, NetworkQoSStatus]:
        """Return path for ACK transmission back to sender."""
        with self._lock:
            if not self.capabilities.has_ack_return_path:
                return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            if self.state != ConnectionState.CONNECTED:
                return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            self.sent_return_frames.append((frame, priority))
            peer = self._peer_sender

        if peer and hasattr(peer, "_on_inbound_ack"):
            peer._on_inbound_ack(self.path_id, frame)
        return True, NetworkQoSStatus.MARK_APPLIED_LOCALLY

    def _on_inbound_frame(self, path_id: str, frame: bytes) -> None:
        cb = self._frame_callback
        if cb:
            cb(path_id, frame)
