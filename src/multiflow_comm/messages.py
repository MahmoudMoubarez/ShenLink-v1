"""Directional messaging API: MessageSender and MessageReceiver.

Supports bytes, text, and object codecs over independent transport channels
and paths, with ORDERED_ALL (window size 1, gap halt, retries) and LATEST_ONLY
(key-based coalescing and sequence rejection) delivery semantics.
"""

from __future__ import annotations

import queue
from collections import deque
from concurrent.futures import CancelledError, ThreadPoolExecutor
import threading
import time
from typing import Any, Callable, Optional
import uuid

from .qos import NetworkQoSConfig
from .scheduler import Scheduler
from .transport import (
    MessageReceiverAdapter,
    MessageSenderAdapter,
)
from .types import (
    AdapterCapabilities,
    ChannelBlockedError,
    ChannelBlockedEvent,
    Codec,
    ConnectionState,
    DeliveryFailedError,
    DeliveryMode,
    DeliveryTicket,
    ErrorEvent,
    FrameKind,
    JsonCodec,
    NetworkPriorityError,
    NetworkQoSStatus,
    PathPolicy,
    ProtocolError,
    QueueFullError,
    ReceiptPolicy,
    SessionResetEvent,
    TicketStatus,
)
from .wire import decode_frame, encode_frame


class ReceivedMessage:
    """Delivered incoming message representation."""

    def __init__(
        self,
        channel_id: str,
        session_id: str,
        message_id: str,
        sequence: int,
        priority: int,
        mode: DeliveryMode,
        payload: bytes,
        path_id: str,
        latest_key: Optional[str] = None,
        received_monotonic_ns: Optional[int] = None,
    ) -> None:
        self.channel_id = channel_id
        self.session_id = session_id
        self.message_id = message_id
        self.sequence = sequence
        self.priority = priority
        self.mode = mode
        self.payload = payload
        self.path_id = path_id
        self.latest_key = latest_key
        self.received_at = time.time()
        self.received_monotonic_ns = (
            received_monotonic_ns if received_monotonic_ns is not None else time.monotonic_ns()
        )

    def text(self, encoding: str = "utf-8") -> str:
        """Decode payload as text string."""
        return self.payload.decode(encoding)

    def decode_object(self, codec: Codec = JsonCodec()) -> Any:
        """Decode payload using provided codec."""
        return codec.decode(self.payload)


class MessageSender:
    """Directional message sender publisher across one or more transport paths."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageSenderAdapter],
        mode: DeliveryMode = DeliveryMode.ORDERED_ALL,
        receipt: Optional[ReceiptPolicy] = None,
        priority: int = 700,
        path_policy: PathPolicy = PathPolicy.REDUNDANT,
        require_network_priority: bool = False,
        qos_config: Optional[NetworkQoSConfig] = None,
        scheduler: Optional[Scheduler] = None,
        max_queue_size: int = 256,
        ack_timeout: float = 0.5,
        max_retries: int = 3,
        probe_interval_s: float = 1.0,
        probe_timeout_s: Optional[float] = None,
        on_channel_blocked: Optional[Callable[[ChannelBlockedEvent], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        max_ordered_retries: Optional[int] = None,
    ) -> None:
        if not adapters:
            raise ValueError("At least one adapter must be provided")

        # Validate path_id uniqueness
        seen_path_ids: set[str] = set()
        for ad in adapters:
            if ad.path_id in seen_path_ids:
                raise ValueError(f"Duplicate path_id '{ad.path_id}' detected in channel '{channel_id}'")
            seen_path_ids.add(ad.path_id)

        self.channel_id = channel_id
        self.adapters = list(adapters)
        self.mode = mode
        # Default receipt policy depends on delivery mode
        if receipt is None:
            self.receipt = ReceiptPolicy.RECEIVED if mode == DeliveryMode.ORDERED_ALL else ReceiptPolicy.NONE
        else:
            self.receipt = receipt

        self.priority = priority
        self.path_policy = path_policy
        self.require_network_priority = require_network_priority
        self.qos_config = qos_config
        self.max_queue_size = max_queue_size
        self.ack_timeout = ack_timeout
        # Finite retry budget applies to LATEST_ONLY/RECEIVED. Confirmed
        # ORDERED_ALL keeps the same sequence pending across outages unless max_ordered_retries is set.
        self.max_retries = max_retries
        if max_ordered_retries is not None:
            if isinstance(max_ordered_retries, bool) or not isinstance(max_ordered_retries, int) or max_ordered_retries < 0:
                raise ValueError("max_ordered_retries must be a non-negative int or None")
        self.max_ordered_retries = max_ordered_retries
        if probe_interval_s <= 0 or (probe_timeout_s is not None and probe_timeout_s <= 0):
            raise ValueError("Probe interval and timeout must be positive")
        self.probe_interval_s = probe_interval_s
        self.probe_timeout_s = probe_timeout_s or max(0.2, ack_timeout)
        self.on_channel_blocked = on_channel_blocked
        self.on_error = on_error

        # Session and sequence state
        self._session_id = uuid.uuid4().hex[:12]
        self._lock = threading.RLock()
        self._is_started = False
        self._is_closed = False
        self._is_blocked = False

        self._ordered_seq_counter = 0
        self._latest_seq_counters: dict[str, int] = {}
        # In-flight latest value tracking for cancelling obsolete retries in LATEST_ONLY
        self._latest_key_in_flight: dict[str, int] = {}
        self._outstanding_tickets: dict[str, DeliveryTicket] = {}

        # Scheduler
        self._owns_scheduler = scheduler is None
        self._scheduler = scheduler or Scheduler(
            resource_group=f"sender_{channel_id}",
            max_queue_per_channel=max_queue_size,
        )
        self._scheduler.register_channel(self.channel_id, self.mode)

        # Include message ID: distinct LATEST_ONLY keys can share a sequence.
        self._pending_acks: dict[tuple[str, str, str], threading.Event] = {}
        self._pending_probes: dict[tuple[str, str], threading.Event] = {}
        self._probe_stop = threading.Event()
        self._probe_thread: Optional[threading.Thread] = None
        # Dedicated, bounded path-health workers: failed paths must not delay
        # verification of another path or occupy the data fanout workers.
        self._probe_executor = ThreadPoolExecutor(
            max_workers=len(self.adapters), thread_name_prefix=f"MultiflowPathProbe-{channel_id}",
        )
        self._probe_worker_local = threading.local()
        # One worker per configured path permits a redundant copy to start on
        # every path without waiting for another adapter's send_frame call.
        self._fanout_executor = ThreadPoolExecutor(
            max_workers=len(self.adapters), thread_name_prefix=f"MultiflowFanout-{channel_id}",
        )
        # Ordered channels keep their existing one-message ACK window but reuse
        # one dispatch worker. Keep it separate from fan-out: dispatch waits for
        # path sends, so sharing that executor would deadlock with one path.
        self._ordered_dispatch_executor = (
            ThreadPoolExecutor(max_workers=1,
                               thread_name_prefix=f"MultiflowOrderedDispatch-{channel_id}")
            if mode == DeliveryMode.ORDERED_ALL else None
        )
        # Unconfirmed latest-value telemetry has its own bounded parallel pool.
        self._latest_dispatch_executor = (
            ThreadPoolExecutor(max_workers=min(4, max_queue_size),
                               thread_name_prefix=f"MultiflowLatestDispatch-{channel_id}")
            if mode == DeliveryMode.LATEST_ONLY and self.receipt == ReceiptPolicy.NONE else None
        )
        # Bound running + queued pool jobs: ThreadPoolExecutor's own queue is
        # unbounded. Saturation explicitly fails a ticket, never silently drops.
        self._latest_dispatch_slots = threading.BoundedSemaphore(max_queue_size)
        self._dispatch_worker_local = threading.local()

    @property
    def session_id(self) -> str:
        with self._lock:
            return self._session_id

    @property
    def is_blocked(self) -> bool:
        with self._lock:
            return self._is_blocked

    def start(self) -> None:
        """Start sender adapters, send HELLO handshake, and start scheduler."""
        with self._lock:
            if self._is_closed or self._is_started:
                return
            self._is_started = True

            # Register ACK listener on adapters
            for ad in self.adapters:
                if hasattr(ad, "set_ack_callback"):
                    ad.set_ack_callback(self._on_inbound_ack)
                elif hasattr(ad, "set_frame_callback"):
                    ad.set_frame_callback(self._on_inbound_ack)

            # Install the startup gate before open() can invoke an application
            # callback that queues an ordered command on a shared scheduler.
            if self.mode == DeliveryMode.ORDERED_ALL:
                self._scheduler.set_channel_in_flight(self.channel_id, True)

            for ad in self.adapters:
                try:
                    ad.open()
                except Exception as exc:
                    # Adapter implementations may raise rather than report a
                    # FAILED state; keep the other paths usable and retry it.
                    try:
                        ad.state = ConnectionState.FAILED
                    except Exception:
                        pass
                    if self.on_error:
                        try:
                            self.on_error(exc)
                        except Exception:
                            pass

            session_id = self._session_id
            if self.mode != DeliveryMode.ORDERED_ALL and self._owns_scheduler:
                self._scheduler.start()

        # A fake or custom adapter can synchronously return an ACK from a
        # fan-out worker. Never hold the sender lock while waiting for workers.
        try:
            self._send_hello_frame(session_id, verify_connecting=self.mode == DeliveryMode.ORDERED_ALL)
        finally:
            if self.mode == DeliveryMode.ORDERED_ALL:
                self._scheduler.set_channel_in_flight(self.channel_id, False)
        with self._lock:
            if self._is_closed:
                return
            if self._owns_scheduler and self.mode == DeliveryMode.ORDERED_ALL:
                self._scheduler.start()
            self._probe_stop.clear()
            self._probe_thread = threading.Thread(
                target=self._probe_loop,
                name=f"MultiflowProbe-{self.channel_id}",
                daemon=True,
            )
            self._probe_thread.start()

    def restart_session(self) -> None:
        """Explicitly reset channel session with new session_id, clearing any BLOCKED state."""
        with self._lock:
            outstanding = list(self._outstanding_tickets.values())
            self._session_id = uuid.uuid4().hex[:12]
            self._is_blocked = False
            self._ordered_seq_counter = 0
            self._latest_seq_counters.clear()
            self._latest_key_in_flight.clear()
            self._pending_acks.clear()
            session_id = self._session_id
        for ticket in outstanding:
            ticket.set_failed(DeliveryFailedError("Message session was reset"))
        self._send_hello_frame(session_id)

    def close(self) -> None:
        """Shut down sender, adapters, and scheduler (idempotent)."""
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True
            outstanding = list(self._outstanding_tickets.values())
            pending_ack_events = list(self._pending_acks.values())
            pending_probe_events = list(self._pending_probes.values())

        for ticket in outstanding:
            ticket.set_failed(DeliveryFailedError("Sender closed before delivery"))
        # A reused worker must finish promptly even with a long configured ACK
        # timeout. Tickets are already failed, so this wake cannot report receipt.
        for event in pending_ack_events:
            event.set()

        self._probe_stop.set()
        for event in pending_probe_events:
            event.set()
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=max(2.0, self.ack_timeout + 0.5))
        self._probe_executor.shutdown(
            wait=not getattr(self._probe_worker_local, "active", False), cancel_futures=True,
        )

        if self._owns_scheduler:
            self._scheduler.stop()

        if self._latest_dispatch_executor is not None:
            # A user's ticket callback may close its sender from a pool worker.
            self._latest_dispatch_executor.shutdown(
                wait=not getattr(self._dispatch_worker_local, "active", False), cancel_futures=True,
            )
        if self._ordered_dispatch_executor is not None:
            self._ordered_dispatch_executor.shutdown(
                wait=not getattr(self._dispatch_worker_local, "active", False), cancel_futures=True,
            )
        self._fanout_executor.shutdown(wait=True, cancel_futures=True)

        for ad in self.adapters:
            try:
                ad.close()
            except Exception:
                pass

    def send_bytes(
        self,
        data: bytes,
        priority: Optional[int] = None,
        latest_key: Optional[str] = None,
    ) -> DeliveryTicket:
        """Enqueue raw binary data for transmission."""
        with self._lock:
            if self._is_closed:
                raise DeliveryFailedError("Sender is closed")
            if self._is_blocked:
                raise ChannelBlockedError(f"Channel '{self.channel_id}' is BLOCKED due to an earlier failure")

            eff_priority = self.priority if priority is None else max(0, min(1000, int(priority)))
            msg_id = uuid.uuid4().hex[:12]

            if self.mode == DeliveryMode.ORDERED_ALL:
                next_seq = self._ordered_seq_counter + 1
            else:
                key = latest_key or ""
                next_seq = self._latest_seq_counters.get(key, 0) + 1

            ticket = DeliveryTicket(
                message_id=msg_id,
                channel_id=self.channel_id,
                session_id=self._session_id,
                sequence=next_seq,
                priority=eff_priority,
                mode=self.mode,
                receipt=self.receipt,
                latest_key=latest_key,
            )

            # Submit to priority scheduler
            task_fn = self._make_dispatch_fn(ticket, bytes(data))
            try:
                self._scheduler.submit(
                    task_id=msg_id,
                    channel_id=self.channel_id,
                    priority=eff_priority,
                    mode=self.mode,
                    sequence=next_seq,
                    fn=task_fn,
                    latest_key=latest_key,
                    ticket=ticket,
                )
            except Exception:
                # Do NOT commit sequence advancement if submission fails (e.g. QueueFullError)
                raise

            # Only commit counter once scheduler submission has succeeded
            if self.mode == DeliveryMode.ORDERED_ALL:
                self._ordered_seq_counter = next_seq
            else:
                self._latest_seq_counters[key] = next_seq

            self._outstanding_tickets[msg_id] = ticket
            ticket.add_done_callback(
                lambda done: self._forget_ticket(done.message_id)
            )

            return ticket

    def _forget_ticket(self, message_id: str) -> None:
        with self._lock:
            self._outstanding_tickets.pop(message_id, None)

    def send_text(
        self,
        text: str,
        encoding: str = "utf-8",
        priority: Optional[int] = None,
        latest_key: Optional[str] = None,
    ) -> DeliveryTicket:
        """Enqueue text string encoded as bytes."""
        return self.send_bytes(text.encode(encoding), priority=priority, latest_key=latest_key)

    def send_object(
        self,
        value: Any,
        codec: Codec = JsonCodec(),
        priority: Optional[int] = None,
        latest_key: Optional[str] = None,
    ) -> DeliveryTicket:
        """Enqueue object encoded using provided codec."""
        payload = codec.encode(value)
        return self.send_bytes(payload, priority=priority, latest_key=latest_key)

    def _make_dispatch_fn(self, ticket: DeliveryTicket, payload: bytes) -> Callable[[], None]:
        def dispatch() -> None:
            if self._latest_dispatch_executor is not None:
                self._submit_latest_dispatch(ticket, payload)
                return
            if self.mode == DeliveryMode.ORDERED_ALL:
                self._submit_ordered_dispatch(ticket, payload)
            else:
                t = threading.Thread(
                    target=self._execute_dispatch,
                    args=(ticket, payload),
                    daemon=True,
                )
                t.start()
        return dispatch

    def _submit_ordered_dispatch(self, ticket: DeliveryTicket, payload: bytes) -> None:
        with self._lock:
            if self._is_closed or ticket.is_done():
                return
            # Scheduler retains all waiting messages in its existing bounded
            # queue. Only one job reaches this executor until the previous
            # Future completes, including its ACK wait and cleanup.
            self._scheduler.set_channel_in_flight(self.channel_id, True)
            try:
                future = self._ordered_dispatch_executor.submit(self._run_ordered_dispatch, ticket, payload)
            except Exception as exc:
                self._scheduler.set_channel_in_flight(self.channel_id, False)
                ticket.set_failed(DeliveryFailedError(f"Ordered dispatch submission failed: {exc}"))
                return
            # Also clears the scheduler gate if close cancels a queued job
            # before it runs. Do not leave shared schedulers permanently blocked.
            future.add_done_callback(
                lambda done: self._scheduler.set_channel_in_flight(self.channel_id, False)
            )

    def _submit_latest_dispatch(self, ticket: DeliveryTicket, payload: bytes) -> None:
        with self._lock:
            if self._is_closed or ticket.is_done():
                return
            if not self._latest_dispatch_slots.acquire(blocking=False):
                ticket.set_failed(QueueFullError("Latest dispatch running/queued capacity exceeded"))
                return
            try:
                future = self._latest_dispatch_executor.submit(self._run_latest_dispatch, ticket, payload)
            except Exception as exc:
                self._latest_dispatch_slots.release()
                ticket.set_failed(DeliveryFailedError(f"Latest dispatch submission failed: {exc}"))
                return
            # Release even when shutdown cancels a job before it starts.
            future.add_done_callback(lambda done: self._latest_dispatch_slots.release())

    def _run_latest_dispatch(self, ticket: DeliveryTicket, payload: bytes) -> None:
        self._dispatch_worker_local.active = True
        try:
            # The existing sequence/session/closed checks still run here, so a
            # superseded queued value is not replayed after worker contention.
            self._execute_dispatch(ticket, payload)
        except Exception as exc:
            ticket.set_failed(DeliveryFailedError(f"Latest dispatch failed: {exc}"))
        finally:
            self._dispatch_worker_local.active = False

    def _run_ordered_dispatch(self, ticket: DeliveryTicket, payload: bytes) -> None:
        self._dispatch_worker_local.active = True
        try:
            self._execute_dispatch(ticket, payload)
        except Exception as exc:
            ticket.set_failed(DeliveryFailedError(f"Ordered dispatch failed: {exc}"))
        finally:
            self._dispatch_worker_local.active = False

    def _execute_dispatch(self, ticket: DeliveryTicket, payload: bytes) -> None:
        """Execute dispatch of a message over the network (window size 1 per channel)."""
        with self._lock:
            if self._is_blocked:
                ticket.set_failed(ChannelBlockedError("Channel is BLOCKED"))
                return
            if ticket.status == TicketStatus.SUPERSEDED:
                return

        # QoS determination and PRE-CHECK before sending any bytes to the wire
        dscp_target = self.qos_config.get_dscp(ticket.priority) if self.qos_config is not None else None
        is_qos_requested = dscp_target is not None

        if self.require_network_priority:
            can_mark = False
            if dscp_target is not None:
                for ad in self.adapters:
                    if ad.state == ConnectionState.CONNECTED and ad.can_apply_dscp(dscp_target):
                        can_mark = True
                        break

            # No currently connected path is an outage, not a QoS capability
            # failure. The ordered message must wait for reconnection.
            active_paths = [ad for ad in self.adapters if ad.state == ConnectionState.CONNECTED]
            if active_paths and not can_mark:
                err = NetworkPriorityError(
                    f"Network priority marking required (DSCP {dscp_target}) but unavailable on any active path"
                )
                ticket.set_failed(err)
                if ticket.mode == DeliveryMode.ORDERED_ALL:
                    self._block_channel("Network priority unavailable", ticket.sequence)
                return

        frame = encode_frame(
            kind=FrameKind.DATA.value,
            session_id=ticket.session_id,
            channel_id=ticket.channel_id,
            message_id=ticket.message_id,
            sequence=ticket.sequence,
            mode=ticket.mode.value,
            priority=ticket.priority,
            receipt=ticket.receipt.value,
            payload=payload,
            latest_key=ticket.latest_key,
        )

        ack_key = (ticket.session_id, ticket.channel_id, ticket.message_id)
        ack_event = threading.Event()
        with self._lock:
            self._pending_acks[ack_key] = ack_event

        try:
            retries = 0
            while not self._is_closed:
                if ticket.is_done():
                    return
                with self._lock:
                    if ticket.session_id != self._session_id:
                        ticket.set_failed(DeliveryFailedError("Message session was reset"))
                        return
                # In LATEST_ONLY, check if a newer sequence was queued for this key
                if ticket.mode == DeliveryMode.LATEST_ONLY:
                    with self._lock:
                        key = ticket.latest_key or ""
                        if self._latest_seq_counters.get(key, 0) > ticket.sequence:
                            ticket.set_superseded()
                            return

                if self.require_network_priority:
                    active_paths = [ad for ad in self.adapters if ad.state == ConnectionState.CONNECTED]
                    if active_paths and not any(
                        dscp_target is not None and ad.can_apply_dscp(dscp_target)
                        for ad in active_paths
                    ):
                        ticket.set_failed(NetworkPriorityError("No active path can apply required DSCP"))
                        if ticket.mode == DeliveryMode.ORDERED_ALL:
                            self._block_channel("Network priority unavailable", ticket.sequence)
                        return

                send_ok, adapter_qos, used_adapters = self._transmit_frame(
                    frame, ticket.priority, dscp=dscp_target,
                    require_marking=self.require_network_priority and is_qos_requested,
                )

                if not is_qos_requested:
                    qos_status = NetworkQoSStatus.NOT_REQUESTED
                else:
                    qos_status = adapter_qos

                if (is_qos_requested and self.require_network_priority
                        and send_ok and qos_status != NetworkQoSStatus.MARK_APPLIED_LOCALLY):
                    err = NetworkPriorityError(
                        f"Network priority marking required but unavailable on channel '{self.channel_id}'"
                    )
                    ticket.set_failed(err)
                    if ticket.mode == DeliveryMode.ORDERED_ALL:
                        self._block_channel("Network priority unavailable", ticket.sequence)
                    return

                if not send_ok:
                    if self.require_network_priority:
                        still_active = [ad for ad in self.adapters if ad.state == ConnectionState.CONNECTED]
                        if still_active:
                            # Strict marking is fail-closed when a connected
                            # adapter refuses the send. A genuine outage has
                            # no active path and remains retryable.
                            ticket.set_failed(NetworkPriorityError("Required DSCP marking or send failed"))
                            if ticket.mode == DeliveryMode.ORDERED_ALL:
                                self._block_channel("Network priority unavailable", ticket.sequence)
                            return
                    if ticket.mode == DeliveryMode.LATEST_ONLY:
                        # A state update that missed all paths is obsolete; never
                        # replay it merely because a path later reconnects.
                        ticket.set_expired()
                        return
                    if ticket.receipt == ReceiptPolicy.NONE:
                        ticket.set_failed(DeliveryFailedError(
                            "ORDERED_ALL needs RECEIVED receipts to survive an outage"
                        ))
                        self._block_channel("Unconfirmed ordered delivery", ticket.sequence)
                        return
                    # Retain the same id and sequence until an ACK arrives.
                    # Do not consume the retry budget while all paths are down.
                    if self._probe_stop.wait(min(self.probe_interval_s, self.ack_timeout)):
                        break
                    continue
                else:
                    ticket.set_dispatched(qos_status)
                    if ticket.receipt == ReceiptPolicy.NONE:
                        # With NONE receipt policy, transmission completes here
                        return

                # Wait for ACK
                if ack_event.wait(timeout=self.ack_timeout):
                    ticket.set_received()
                    # _on_inbound_ack records success on the path that actually
                    # returned the ACK. The last path sent is not necessarily it.
                    return
                else:
                    for used_adapter in used_adapters:
                        if hasattr(used_adapter, "record_timeout"):
                            used_adapter.record_timeout()

                retries += 1
                if ticket.mode == DeliveryMode.LATEST_ONLY and retries > self.max_retries:
                    err = DeliveryFailedError(
                        f"ACK not received after {self.max_retries} retries for seq {ticket.sequence}"
                    )
                    ticket.set_failed(err)
                    return
                if (
                    ticket.mode == DeliveryMode.ORDERED_ALL
                    and self.max_ordered_retries is not None
                    and retries > self.max_ordered_retries
                ):
                    err = DeliveryFailedError(
                        f"ACK not received after {self.max_ordered_retries} retries for ordered seq {ticket.sequence}"
                    )
                    ticket.set_failed(err)
                    self._block_channel(str(err), ticket.sequence)
                    return

            if not ticket.is_done():
                ticket.set_failed(DeliveryFailedError("Sender closed before delivery"))

        finally:
            with self._lock:
                self._pending_acks.pop(ack_key, None)

    def _transmit_frame(
        self,
        frame: bytes,
        priority: int,
        dscp: Optional[int] = None,
        require_marking: bool = False,
    ) -> tuple[bool, NetworkQoSStatus, list[Any]]:
        """Dispatch frame across paths according to path_policy."""
        connected_adapters = [ad for ad in self.adapters if ad.state == ConnectionState.CONNECTED]
        if not connected_adapters:
            return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE, []

        if self.path_policy == PathPolicy.PRIMARY_THEN_BACKUP:
            for ad in connected_adapters:
                if require_marking and (dscp is None or not ad.can_apply_dscp(dscp)):
                    continue
                try:
                    success, qos_status = ad.send_frame(
                        frame, priority, dscp=dscp, require_marking=require_marking,
                    )
                except Exception:
                    continue
                if success:
                    return True, qos_status, [ad]
            return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE, []

        elif self.path_policy == PathPolicy.REDUNDANT:
            any_success = False
            best_qos = NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE
            sent_adapters = []
            eligible = [ad for ad in connected_adapters
                        if not require_marking or (dscp is not None and ad.can_apply_dscp(dscp))]
            def send_copy(ad):
                try:
                    ok, qos = ad.send_frame(
                        frame, priority, dscp=dscp, require_marking=require_marking,
                    )
                    return ad, ok, qos
                except Exception:
                    return ad, False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE

            # map submits every copy before collecting results. Waiting for all
            # send calls to finish preserves per-path ORDERED_ALL send order.
            for ad, ok, qos in self._fanout_executor.map(send_copy, eligible):
                if ok:
                    any_success = True
                    sent_adapters.append(ad)
                    if qos == NetworkQoSStatus.MARK_APPLIED_LOCALLY:
                        best_qos = qos
            return any_success, best_qos, sent_adapters

        return False, NetworkQoSStatus.NETWORK_PRIORITY_UNAVAILABLE, []


    def _send_hello_frame(self, session_id: Optional[str] = None, *, verify_connecting: bool = False) -> None:
        """Send HELLO handshake frame to establish/synchronize session."""
        frame = encode_frame(
            kind=FrameKind.HELLO.value,
            session_id=session_id or self.session_id,
            channel_id=self.channel_id,
            message_id=uuid.uuid4().hex[:12],
            sequence=0,
            mode=self.mode.value,
            priority=self.priority,
            receipt=ReceiptPolicy.NONE.value,
            payload=b"",
        )
        self._transmit_frame(frame, self.priority)
        if verify_connecting:
            self._verify_starting_paths(frame, session_id or self.session_id)

    def _verify_starting_paths(self, frame: bytes, session_id: str) -> None:
        """One bounded, parallel initial probe round for unverified adapters.

        UDP binds locally in CONNECTING, not CONNECTED. The ordinary HELLO
        fanout excludes that state, so periodic serial probes previously left
        the first command dependent on which path completed verification first.
        Reuse existing probe capabilities and the existing fanout workers; do
        not send user DATA to an unverified or unavailable path.
        """
        message_id = decode_frame(frame)[0]["message_id"]
        with self._lock:
            if self._is_closed:
                return
            probes = [(ad, threading.Event()) for ad in self.adapters
                      if ad.state == ConnectionState.CONNECTING
                      and hasattr(ad, "send_probe") and hasattr(ad, "record_success")]
            for ad, event in probes:
                self._pending_probes[(ad.path_id, message_id)] = event
        if not probes:
            return

        def verify(item):
            ad, event = item
            try:
                sent = ad.send_probe(frame)
            except Exception:
                sent = False
            if sent and event.wait(self.probe_timeout_s):
                with self._lock:
                    if self._is_closed or self._session_id != session_id:
                        return
                ad.record_success()

        try:
            # Submit every initial probe before awaiting any ACK. One missing
            # peer costs one configured timeout, not one timeout per path.
            for _ in self._fanout_executor.map(verify, probes):
                pass
        except (RuntimeError, CancelledError):
            if not self._is_closed:
                raise
        finally:
            with self._lock:
                for ad, _ in probes:
                    self._pending_probes.pop((ad.path_id, message_id), None)

    def _probe_loop(self) -> None:
        """Start one independent health loop per path, with no queued rounds."""
        with self._lock:
            if self._is_closed or self._probe_stop.is_set():
                return
            for ad in self.adapters:
                self._probe_executor.submit(self._probe_path_loop, ad)

    def _probe_path_loop(self, ad) -> None:
        self._probe_worker_local.active = True
        try:
            self._run_probe_path(ad)
        finally:
            self._probe_worker_local.active = False

    def _run_probe_path(self, ad) -> None:
        """Keep the configured timeout/interval, independently for each path."""
        while not self._probe_stop.wait(self.probe_interval_s):
            try:
                # An adapter with send_probe owns the low-level reconnect
                # attempt in that method. Calling open() first would attempt
                # two TCP connects on each health-check cycle.
                if (ad.state in (ConnectionState.FAILED, ConnectionState.DISCONNECTED)
                        and not hasattr(ad, "send_probe")):
                    try:
                        ad.open()
                    except Exception as exc:
                        try:
                            ad.state = ConnectionState.FAILED
                        except Exception:
                            pass
                        if self.on_error:
                            try:
                                self.on_error(exc)
                            except Exception:
                                pass
                if not hasattr(ad, "send_probe"):
                    continue
                with self._lock:
                    session_id = self._session_id
                probe_id = uuid.uuid4().hex[:12]
                key = (ad.path_id, probe_id)
                event = threading.Event()
                frame = encode_frame(
                    kind=FrameKind.HELLO.value,
                    session_id=session_id,
                    channel_id=self.channel_id,
                    message_id=probe_id,
                    sequence=0,
                    mode=self.mode.value,
                    priority=self.priority,
                    receipt=ReceiptPolicy.NONE.value,
                    payload=b"",
                )
                with self._lock:
                    if self._is_closed or self._probe_stop.is_set():
                        return
                    self._pending_probes[key] = event
                try:
                    try:
                        sent = ad.send_probe(frame)
                    except Exception:
                        sent = False
                    acknowledged = sent and event.wait(timeout=self.probe_timeout_s)
                    with self._lock:
                        # close() wakes waiters without supplying an ACK; a
                        # reset session also invalidates the old health result.
                        if self._is_closed or self._probe_stop.is_set() or session_id != self._session_id:
                            continue
                        if acknowledged:
                            if hasattr(ad, "record_success"):
                                ad.record_success()
                        elif hasattr(ad, "record_timeout"):
                            ad.record_timeout()
                finally:
                    with self._lock:
                        self._pending_probes.pop(key, None)
            except Exception as exc:
                if self.on_error and not self._probe_stop.is_set():
                    try:
                        self.on_error(exc)
                    except Exception:
                        pass

    def _block_channel(self, reason: str, last_seq: int) -> None:
        """Transition channel to BLOCKED state and emit event outside locks."""
        callback = None
        event = None
        with self._lock:
            self._is_blocked = True
            callback = self.on_channel_blocked
            event = ChannelBlockedEvent(
                channel_id=self.channel_id,
                session_id=self._session_id,
                reason=reason,
                last_sequence=last_seq,
            )

        if callback and event:
            try:
                callback(event)
            except Exception:
                pass

    def _on_inbound_ack(self, path_id: str, frame_bytes: bytes) -> None:
        """Handle incoming ACK frames from peer receiver."""
        try:
            header, _ = decode_frame(frame_bytes)
        except Exception:
            return

        if header.get("kind") != FrameKind.ACK.value:
            return
        if header.get("channel_id") != self.channel_id:
            return
        if header.get("session_id") != self.session_id:
            return

        if header.get("sequence") == 0:
            with self._lock:
                probe = self._pending_probes.get((path_id, header.get("message_id", "")))
            if probe:
                probe.set()
            return

        key = (
            header.get("session_id", ""),
            header.get("channel_id", ""),
            header.get("message_id", ""),
        )
        with self._lock:
            event = self._pending_acks.get(key)
        if event:
            event.set()
            for ad in self.adapters:
                if ad.path_id == path_id and hasattr(ad, "record_success"):
                    ad.record_success()
                    break


class MessageReceiver:
    """Directional message receiver across one or more transport paths."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageReceiverAdapter],
        mode: DeliveryMode = DeliveryMode.ORDERED_ALL,
        on_message: Optional[Callable[[ReceivedMessage], None]] = None,
        on_session_reset: Optional[Callable[[SessionResetEvent], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        max_queue_size: int = 256,
        reconnect_interval_s: float = 2.0,
        time_source: Optional[Callable[[], int]] = None,
        max_payload_size: Optional[int] = None,
    ) -> None:
        if not adapters:
            raise ValueError("At least one receiver adapter must be provided")
        if reconnect_interval_s <= 0:
            raise ValueError("Reconnect interval must be positive")
        if max_payload_size is not None and (type(max_payload_size) is not int or max_payload_size <= 0):
            raise ValueError("max_payload_size must be a positive integer or None")
        self.max_payload_size = max_payload_size

        seen_path_ids: set[str] = set()
        for ad in adapters:
            if ad.path_id in seen_path_ids:
                raise ValueError(f"Duplicate path_id '{ad.path_id}' detected in channel '{channel_id}'")
            seen_path_ids.add(ad.path_id)

        self.channel_id = channel_id
        self.adapters = list(adapters)
        self.mode = mode
        self.on_message = on_message
        self.on_session_reset = on_session_reset
        self.on_error = on_error
        self.max_queue_size = max_queue_size
        self._time_source = time_source or time.monotonic_ns

        self._lock = threading.RLock()
        self._is_started = False
        self._is_closed = False
        self._is_blocked = False

        self._session_id: Optional[str] = None
        self._retired_sessions: set[str] = set()
        self._expected_seq = 1
        self._seen_sequences: set[int] = set()
        self._last_seq_per_key: dict[str, int] = {}

        # Bounded delivery queue for sequential callback execution outside locks
        self._delivery_queue: queue.Queue[Optional[ReceivedMessage]] = queue.Queue(maxsize=max_queue_size)
        self._pending_latest: dict[str, ReceivedMessage] = {}
        self._pending_keys: deque[str] = deque()
        self._latest_condition = threading.Condition(self._lock)
        self._worker_thread: Optional[threading.Thread] = None
        self._recovery_stop = threading.Event()
        self._recovery_thread: Optional[threading.Thread] = None
        self.reconnect_interval_s = reconnect_interval_s

    @property
    def session_id(self) -> Optional[str]:
        with self._lock:
            return self._session_id

    def start(self) -> None:
        """Start receiver adapters and begin delivery loop."""
        with self._lock:
            if self._is_closed or self._is_started:
                return
            self._is_started = True

            for ad in self.adapters:
                ad.set_frame_callback(self._on_inbound_frame)
                try:
                    ad.open()
                except Exception as exc:
                    try:
                        ad.state = ConnectionState.FAILED
                    except Exception:
                        pass
                    self._notify_error(exc)

            self._worker_thread = threading.Thread(
                target=self._delivery_worker_loop,
                name=f"MultiflowReceiverDelivery-{self.channel_id}",
                daemon=True,
            )
            self._worker_thread.start()
            self._recovery_stop.clear()
            self._recovery_thread = threading.Thread(
                target=self._recovery_loop,
                name=f"MultiflowReceiverRecovery-{self.channel_id}",
                daemon=True,
            )
            self._recovery_thread.start()

    def close(self) -> None:
        """Shut down receiver, adapters, and worker thread (idempotent)."""
        worker = None
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True
            worker = self._worker_thread
            self._worker_thread = None
            if self.mode == DeliveryMode.LATEST_ONLY:
                self._pending_latest.clear()
                self._pending_keys.clear()
                self._latest_condition.notify_all()

        if self.mode == DeliveryMode.ORDERED_ALL:
            try:
                self._delivery_queue.put_nowait(None)
            except Exception:
                pass

        if worker and worker.is_alive() and worker is not threading.current_thread():
            worker.join(timeout=2.0)

        self._recovery_stop.set()
        if self._recovery_thread and self._recovery_thread.is_alive():
            self._recovery_thread.join(timeout=2.0)

        for ad in self.adapters:
            try:
                ad.close()
            except Exception:
                pass

    def _recovery_loop(self) -> None:
        """Retry a failed receiver bind without requiring application polling."""
        while not self._recovery_stop.wait(self.reconnect_interval_s):
            for ad in self.adapters:
                if self._recovery_stop.is_set():
                    return
                if ad.state == ConnectionState.FAILED:
                    try:
                        ad.open()
                    except Exception as exc:
                        try:
                            ad.state = ConnectionState.FAILED
                        except Exception:
                            pass
                        self._notify_error(exc)

    def _on_inbound_frame(self, path_id: str, frame_bytes: bytes) -> None:
        """Decode and process an incoming raw binary frame."""
        arrival_monotonic_ns = self._time_source()
        try:
            header, payload = decode_frame(frame_bytes)
        except Exception as e:
            self._notify_error(e)
            return

        if header.get("channel_id") != self.channel_id:
            return

        if self.max_payload_size is not None and len(payload) > self.max_payload_size:
            self._notify_error(ProtocolError("Incoming message exceeds receiver payload bound"))
            return

        kind = header.get("kind")
        incoming_session = header.get("session_id", "")

        # Session reset check
        session_reset_to_fire = None
        with self._lock:
            if self._session_id is None or incoming_session != self._session_id:
                if incoming_session in self._retired_sessions:
                    return
                if kind == FrameKind.HELLO.value or self._session_id is None:
                    if self._session_id is not None:
                        self._retired_sessions.add(self._session_id)
                    self._session_id = incoming_session
                    self._expected_seq = 1
                    self._seen_sequences.clear()
                    self._last_seq_per_key.clear()
                    self._pending_latest.clear()
                    self._pending_keys.clear()
                    self._is_blocked = False
                    session_reset_to_fire = SessionResetEvent(self.channel_id, incoming_session)
                else:
                    # Ignore frames from stale session
                    return

        if session_reset_to_fire and self.on_session_reset:
            try:
                self.on_session_reset(session_reset_to_fire)
            except Exception as e:
                self._notify_error(e)

        if kind == FrameKind.HELLO.value:
            # A HELLO with sequence zero is also a path-health probe. The ACK
            # is scoped to its sender path and does not acknowledge user data.
            self._send_ack(path_id, header)
            return

        if kind != FrameKind.DATA.value:
            return

        seq = header.get("sequence", 0)
        receipt_policy = header.get("receipt", ReceiptPolicy.NONE.value)

        # Validation, enqueue, and sequence advancement must be atomic across
        # concurrent receive threads (e.g. redundant packets on two paths).
        error_to_fire: Optional[Exception] = None
        should_ack = False

        with self._lock:
            if self._is_blocked:
                return

            if self.mode == DeliveryMode.ORDERED_ALL:
                if seq in self._seen_sequences or seq < self._expected_seq:
                    should_ack = receipt_policy == ReceiptPolicy.RECEIVED.value

                elif seq == self._expected_seq:
                    message = ReceivedMessage(
                        channel_id=self.channel_id,
                        session_id=incoming_session,
                        message_id=header["message_id"],
                        sequence=seq,
                        priority=header["priority"],
                        mode=self.mode,
                        payload=payload,
                        path_id=path_id,
                        latest_key=header.get("latest_key"),
                        received_monotonic_ns=arrival_monotonic_ns,
                    )
                    try:
                        self._delivery_queue.put_nowait(message)
                    except queue.Full:
                        error_to_fire = QueueFullError(
                            f"Receiver delivery queue full ({self.max_queue_size})"
                        )
                    else:
                        self._seen_sequences.add(seq)
                        self._expected_seq += 1
                        should_ack = receipt_policy == ReceiptPolicy.RECEIVED.value
                else:
                    # ORDERED_ALL GAP!
                    self._is_blocked = True
                    error_to_fire = DeliveryFailedError(
                        f"Ordered gap detected on channel '{self.channel_id}': expected seq {self._expected_seq}, got {seq}"
                    )

            elif self.mode == DeliveryMode.LATEST_ONLY:
                raw_key = header.get("latest_key")
                key = raw_key if (raw_key is not None and raw_key != "") else ""
                last_seq = self._last_seq_per_key.get(key, 0)
                if seq <= last_seq:
                    should_ack = receipt_policy == ReceiptPolicy.RECEIVED.value and seq == last_seq
                else:
                    message = ReceivedMessage(
                        channel_id=self.channel_id,
                        session_id=incoming_session,
                        message_id=header["message_id"],
                        sequence=seq,
                        priority=header["priority"],
                        mode=self.mode,
                        payload=payload,
                        path_id=path_id,
                        latest_key=raw_key,
                        received_monotonic_ns=arrival_monotonic_ns,
                    )
                    if key in self._pending_latest:
                        # In-place replacement of pending item for this key
                        self._pending_latest[key] = message
                        self._last_seq_per_key[key] = seq
                        self._latest_condition.notify()
                        should_ack = receipt_policy == ReceiptPolicy.RECEIVED.value
                    else:
                        if len(self._pending_latest) >= self.max_queue_size:
                            error_to_fire = QueueFullError(
                                f"Receiver delivery queue full ({self.max_queue_size})"
                            )
                        else:
                            self._pending_latest[key] = message
                            self._pending_keys.append(key)
                            self._last_seq_per_key[key] = seq
                            self._latest_condition.notify()
                            should_ack = receipt_policy == ReceiptPolicy.RECEIVED.value

        if error_to_fire:
            self._notify_error(error_to_fire)
        if should_ack:
            self._send_ack(path_id, header)


    def _send_ack(self, path_id: str, data_header: dict[str, Any]) -> None:
        """Send an ACK frame back to sender."""
        ack_frame = encode_frame(
            kind=FrameKind.ACK.value,
            session_id=data_header["session_id"],
            channel_id=self.channel_id,
            message_id=data_header["message_id"],
            sequence=data_header["sequence"],
            mode=data_header["mode"],
            priority=data_header["priority"],
            receipt=ReceiptPolicy.NONE.value,
            payload=b"",
        )

        # Find adapter with matching path_id or any adapter supporting ACK return
        target_ad = None
        for ad in self.adapters:
            if ad.path_id == path_id and ad.capabilities.has_ack_return_path:
                target_ad = ad
                break
        if target_ad is None:
            for ad in self.adapters:
                if ad.capabilities.has_ack_return_path:
                    target_ad = ad
                    break

        if target_ad and hasattr(target_ad, "send_frame"):
            try:
                target_ad.send_frame(ack_frame, data_header["priority"])
            except Exception:
                pass

    def _delivery_worker_loop(self) -> None:
        """Sequential callback execution loop outside all internal locks."""
        if self.mode == DeliveryMode.ORDERED_ALL:
            self._delivery_worker_loop_ordered()
        else:
            self._delivery_worker_loop_latest()

    def _delivery_worker_loop_ordered(self) -> None:
        """Sequential callback execution loop for ORDERED_ALL outside locks."""
        while True:
            try:
                msg = self._delivery_queue.get()
            except Exception:
                break

            if msg is None:
                break

            # Execute callback outside locks
            if self.on_message:
                try:
                    self.on_message(msg)
                except Exception as e:
                    # Local callback error reported locally; do not re-execute!
                    self._notify_error(e)

            self._delivery_queue.task_done()

    def _delivery_worker_loop_latest(self) -> None:
        """Sequential callback execution loop for LATEST_ONLY outside locks."""
        while True:
            with self._lock:
                while not self._is_closed and not self._pending_keys:
                    self._latest_condition.wait()

                if self._is_closed:
                    break

                key = self._pending_keys.popleft()
                msg = self._pending_latest.pop(key, None)

            if msg is not None:
                if self.on_message:
                    try:
                        self.on_message(msg)
                    except Exception as e:
                        # Local callback error reported locally; do not re-execute!
                        self._notify_error(e)

    def _notify_error(self, err: Exception) -> None:
        """Invoke on_error handler outside locks."""
        cb = self.on_error
        if cb:
            try:
                cb(err)
            except Exception:
                pass
