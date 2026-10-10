"""Command endpoints over the same opaque byte-path contract as video v1.

No second delivery engine: bridges adapt complete transport messages to the
existing ordered/confirmed command core. Transport instances are exclusively
owned by one endpoint; video and commands use distinct instances/connections.
"""
from dataclasses import dataclass
import math
import threading

from .byte_transport import (
    SendStatus, TransportCallbacks, TransportState, validate_adapter,
)
from .messages import MessageSender, MessageReceiver
from .types import AdapterCapabilities, ConnectionState, NetworkQoSStatus
from .wire import MAX_PAYLOAD_SIZE, encode_frame


def _positive(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def _validate(settings):
    if not isinstance(settings.channel_id, str) or not settings.channel_id.strip():
        raise ValueError("Nonempty channel_id required")
    if type(settings.max_queue_size) is not int or settings.max_queue_size <= 0:
        raise ValueError("Positive integer max_queue_size required")
    if type(settings.max_payload_bytes) is not int or not 1 <= settings.max_payload_bytes <= MAX_PAYLOAD_SIZE:
        raise ValueError("Invalid command payload bound")
    paths = tuple(settings.paths)
    if not paths:
        raise ValueError("At least one transport path required")
    for path in paths:
        validate_adapter(path)
        if path.state != TransportState.DISCONNECTED:
            raise ValueError("Command paths must be fresh and exclusively owned")
    if len({p.path_id for p in paths}) != len(paths):
        raise ValueError("Duplicate path IDs")
    # Reserve the full wire envelope, including a 64-bit sequence number.
    # This happens once, not as a second serialization on each command.
    bound = len(encode_frame(kind="DATA", session_id="0" * 12,
        channel_id=settings.channel_id, message_id="0" * 12,
        sequence=2**64 - 1, mode="ORDERED_ALL", priority=700,
        receipt="RECEIVED", payload=bytes(settings.max_payload_bytes)))
    if any(p.capabilities.max_message_bytes < bound for p in paths):
        raise ValueError("Command payload plus wire envelope exceeds a path's message bound")
    object.__setattr__(settings, "paths", paths)


@dataclass(frozen=True)
class CommandSenderSettings:
    paths: tuple
    channel_id: str = "commands"
    max_payload_bytes: int = 1024
    max_queue_size: int = 256
    ack_timeout_s: float = 0.5
    probe_interval_s: float = 1.0
    probe_timeout_s: float = 0.5
    max_ordered_retries: int | None = None

    def __post_init__(self):
        _validate(self)
        for name in ("ack_timeout_s", "probe_interval_s", "probe_timeout_s"):
            _positive(getattr(self, name), name)
        if self.max_ordered_retries is not None and (
            type(self.max_ordered_retries) is not int or self.max_ordered_retries < 0
        ):
            raise ValueError("max_ordered_retries must be a nonnegative integer or None")


@dataclass(frozen=True)
class CommandReceiverSettings:
    paths: tuple
    channel_id: str = "commands"
    max_payload_bytes: int = 1024
    max_queue_size: int = 256
    reconnect_interval_s: float = 2.0

    def __post_init__(self):
        _validate(self)
        _positive(self.reconnect_interval_s, "reconnect_interval_s")


class _ByteMessagePath:
    """Internal compatibility bridge, not a new transport implementation."""
    def __init__(self, transport, *, sender, on_error, on_state):
        self.transport = transport
        self.path_id = transport.path_id
        self.capabilities = AdapterCapabilities(
            max_frame_size=transport.capabilities.max_message_bytes,
            has_ack_return_path=True,
        )
        self.state = ConnectionState.DISCONNECTED
        self._sender = sender
        self._on_error, self._on_state = on_error, on_state
        self._frame_callback = self._state_callback = None
        self._opened = self._closed = False
        self._lock = threading.RLock()
        self.cleanup_error = None

    def set_state_callback(self, callback):
        self._state_callback = callback

    def set_ack_callback(self, callback):
        self._frame_callback = callback

    set_frame_callback = set_ack_callback

    def _change(self, state, reason):
        with self._lock:
            if self._closed and state != ConnectionState.CLOSED:
                return
            changed = self.state != state
            self.state = state
        if changed:
            if self._state_callback:
                self._state_callback(self.path_id, state, reason)
            if self._on_state:
                self._on_state(self.path_id, TransportState(state.value.lower()), reason)

    def _transport_state(self, state, reason):
        # A socket/peer datagram is not command HELLO verification. Senders
        # become usable only after the command core validates the matching ACK.
        if self._sender and state == TransportState.CONNECTED:
            return
        self._change(ConnectionState(state.value.upper()), reason)

    def _message(self, message):
        with self._lock:
            if self._closed:
                return
            callback = self._frame_callback
        if type(message) is not bytes or not 0 < len(message) <= self.capabilities.max_frame_size:
            self._error(ValueError("Transport returned an invalid or oversized message frame"))
            return
        if callback:
            callback(self.path_id, message)

    def _error(self, error):
        self._change(ConnectionState.FAILED, str(error))
        if self._on_error:
            self._on_error(error)

    def open(self):
        with self._lock:
            if self._closed:
                return
            opened = self._opened
        self._change(ConnectionState.CONNECTING, "peer verification pending")
        try:
            if opened:
                self.transport.reconnect()
            else:
                self.transport.open(TransportCallbacks(self._message, self._transport_state, self._error))
                with self._lock:
                    self._opened = True
        except Exception:
            self._change(ConnectionState.FAILED, "transport open failed")
            raise

    def send_frame(self, frame, priority=0, dscp=None, require_marking=False):
        if dscp is not None or require_marking:
            return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
        with self._lock:
            if self._closed or (self._sender and self.state != ConnectionState.CONNECTED):
                return False, NetworkQoSStatus.NOT_REQUESTED
        return self.transport.send(frame) == SendStatus.ACCEPTED, NetworkQoSStatus.NOT_REQUESTED

    def can_apply_dscp(self, dscp):
        return False

    def send_probe(self, frame):
        with self._lock:
            if self._closed:
                return False
            opened = self._opened
        if not opened or self.transport.state in (TransportState.FAILED, TransportState.DISCONNECTED):
            self.open()
        return self.transport.send(frame) == SendStatus.ACCEPTED

    def record_success(self):
        self._change(ConnectionState.CONNECTED, "message peer verified")

    def record_timeout(self):
        self._change(ConnectionState.FAILED, "peer verification timed out")

    def close(self):
        with self._lock:
            self._closed = True
        try:
            self.transport.close()
            self.cleanup_error = None
        except Exception as exc:
            self.cleanup_error = exc
            raise
        finally:
            self._change(ConnectionState.CLOSED, "closed")


class _Endpoint:
    def _initialize(self, settings, *, sender, on_error, on_state):
        for callback in (on_error, on_state):
            if callback is not None and not callable(callback):
                raise TypeError("Endpoint callbacks must be callable or None")
        self.settings = settings
        self._started = self._closed = False
        self._paths = [_ByteMessagePath(p, sender=sender, on_error=on_error, on_state=on_state)
                       for p in settings.paths]

    @property
    def path_states(self):
        """Command-verified states, not merely locally open sockets."""
        return {p.path_id: TransportState(p.state.value.lower()) for p in self._paths}

    @property
    def session_id(self):
        return self._core.session_id

    def start(self):
        if self._closed:
            raise RuntimeError("Command endpoint is closed; create a fresh endpoint")
        self._core.start()
        self._started = True
        return self

    def stop(self):
        self._closed = True
        self._core.close()
        errors = [p.cleanup_error for p in self._paths if p.cleanup_error is not None]
        if errors:
            raise RuntimeError(f"Command transport cleanup failed: {errors}")

    close = stop

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


class CommandSender(_Endpoint):
    """Ordered, redundant, receiver-confirmed commands; not execution receipts."""
    def __init__(self, settings: CommandSenderSettings, *, on_error=None,
                 on_state=None, on_blocked=None):
        if not isinstance(settings, CommandSenderSettings):
            raise TypeError("CommandSenderSettings required")
        if on_blocked is not None and not callable(on_blocked):
            raise TypeError("on_blocked must be callable or None")
        self._initialize(settings, sender=True, on_error=on_error, on_state=on_state)
        self._core = MessageSender(settings.channel_id, self._paths,
            max_queue_size=settings.max_queue_size, ack_timeout=settings.ack_timeout_s,
            probe_interval_s=settings.probe_interval_s, probe_timeout_s=settings.probe_timeout_s,
            max_ordered_retries=settings.max_ordered_retries,
            on_error=on_error, on_channel_blocked=on_blocked)

    @property
    def is_blocked(self):
        return self._core.is_blocked

    def send_bytes(self, data):
        if not self._started or self._closed:
            raise RuntimeError("Start the command sender before sending")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("Command data must be bytes-like")
        data = bytes(data)
        if len(data) > self.settings.max_payload_bytes:
            raise ValueError("Command exceeds configured payload bound")
        return self._core.send_bytes(data)

    def send_text(self, text, encoding="utf-8"):
        return self.send_bytes(text.encode(encoding))

    def send_object(self, value, codec=None):
        from .types import JsonCodec
        return self.send_bytes((codec or JsonCodec()).encode(value))

    def restart_session(self):
        if not self._started or self._closed:
            raise RuntimeError("Session reset requires a started sender")
        self._core.restart_session()


class CommandReceiver(_Endpoint):
    def __init__(self, settings: CommandReceiverSettings, *, on_command,
                 on_error=None, on_state=None, on_session=None):
        if not isinstance(settings, CommandReceiverSettings):
            raise TypeError("CommandReceiverSettings required")
        if not callable(on_command):
            raise TypeError("on_command must be callable")
        if on_session is not None and not callable(on_session):
            raise TypeError("on_session must be callable or None")
        self._initialize(settings, sender=False, on_error=on_error, on_state=on_state)
        self._core = MessageReceiver(settings.channel_id, self._paths,
            on_message=on_command, on_session_reset=on_session, on_error=on_error,
            max_queue_size=settings.max_queue_size,
            reconnect_interval_s=settings.reconnect_interval_s,
            max_payload_size=settings.max_payload_bytes)
