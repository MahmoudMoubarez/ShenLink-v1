"""Ready-to-use lab UDP path: codec-free, bounded, heartbeat/reopen capable.

Not encrypted/authenticated. The session tag filters accidents, not attackers.
Use trusted lab networks only. DSCP capability means local marking, NOT router QoS.
"""
import json
import queue
import select
import socket
import threading
import time

from .core import FragmentAssembler, MAGIC

CONTROL = b"SVC1"


class UdpVideoPath:
    """Transport operation lives here; the session requests maintenance/recovery."""
    def __init__(self, path_id, local, peer, epoch, *, dscp=None,
                 probe_interval_s=.2, failed_after_s=.8):
        if not path_id or not 0 < probe_interval_s < failed_after_s:
            raise ValueError("Invalid path/probe configuration")
        if dscp is not None and not 0 <= dscp <= 63:
            raise ValueError("Invalid DSCP")
        self.path_id, self.local, self.peer, self.epoch = path_id, local, peer, epoch
        self.dscp = dscp
        self.probe_interval_s, self.failed_after_s = probe_interval_s, failed_after_s
        self.network_priority_status = "NOT_REQUESTED"
        self.state = "DISCONNECTED"
        self.on_unit = self.on_state = self.on_clock = self.on_keyframe_request = None
        self.on_error = None
        self.on_negotiation = None
        self.on_video_feedback = None
        self.assembler = FragmentAssembler(epoch)
        self.stats = dict(received_units=0, sent_units=0, sent_bytes=0,
                          received_bytes=0, malformed=0, queue_drops=0,
                          socket_reopens=0, heartbeat_recoveries=0)
        self._outgoing = queue.Queue(maxsize=2)
        self._stop = threading.Event()
        self._thread = None
        self._socket = None
        self._io_lock = threading.RLock()
        self._wake_lock = threading.Lock()
        self._wake_reader = self._wake_writer = None
        self._last_rx = None
        self._last_probe = 0.
        self._last_open = 0.
        self._pending_probes = {}
        self._last_idr_request = 0.

    def _state(self, state):
        if self.state != state:
            old = self.state
            self.state = state
            if state == "CONNECTED" and old == "FAILED":
                self.stats["heartbeat_recoveries"] += 1
            if self.on_state:
                self.on_state(self.path_id, state)

    def _open_socket(self):
        with self._io_lock:
            self._last_open = time.monotonic()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.bind(self.local)
                sock.setblocking(False)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
                if self.dscp is not None:
                    from multiflow_comm.qos import apply_socket_dscp
                    self.network_priority_status = ("MARK_APPLIED_LOCALLY" if
                        apply_socket_dscp(sock, self.dscp) else "NETWORK_PRIORITY_UNAVAILABLE")
                self._socket = sock
                self.stats["socket_reopens"] += 1
                self._state("CONNECTING")
            except OSError:
                sock.close()
                self._state("FAILED")

    def open(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        with self._wake_lock:
            if self._wake_reader is None:
                self._wake_reader, self._wake_writer = socket.socketpair()
                self._wake_reader.setblocking(False)
                self._wake_writer.setblocking(False)
        self._open_socket()
        self._thread = threading.Thread(target=self._loop, name="shared-udp-" + self.path_id, daemon=True)
        self._thread.start()

    def send_packets(self, packets):
        """Queue one AU without blocking other paths. Accepted != delivered."""
        if self._stop.is_set():
            return False
        try:
            self._outgoing.put_nowait(packets)
            self._wake()
            return True
        except queue.Full:
            self.stats["queue_drops"] += 1
            return False

    def _wake(self):
        # A full wake socket is already readable: notifications may coalesce.
        with self._wake_lock:
            if self._wake_writer is not None:
                try:
                    self._wake_writer.send(b"\x01")
                except BlockingIOError:
                    pass

    def _drain_wake(self):
        while True:
            try:
                if not self._wake_reader.recv(4096):
                    break
            except BlockingIOError:
                break

    def _send(self, packet):
        with self._io_lock:
            sock = self._socket
            if sock is None:
                return False
            try:
                sent = sock.sendto(packet, self.peer)
                self.stats["sent_bytes"] += sent
                return sent == len(packet)
            except BlockingIOError:
                return False
            except OSError:
                sock.close()
                self._socket = None
                self._state("FAILED")
                return False

    def _control(self, kind, **fields):
        data = dict(kind=kind, epoch=self.epoch, **fields)
        return self._send(CONTROL + json.dumps(data, separators=(",", ":")).encode())

    def request_keyframe(self):
        now = time.monotonic()
        if now - self._last_idr_request >= .5:
            self._last_idr_request = now
            self._control("IDR")

    def send_negotiation(self, message):
        packet = CONTROL + json.dumps(dict(kind="NEGOTIATE", epoch=self.epoch,
            message=message), separators=(",", ":"), allow_nan=False).encode()
        if len(packet) > 1400:
            raise ValueError("Negotiation control exceeds datagram bound")
        return self._send(packet)

    def send_video_feedback(self, message):
        packet=CONTROL+json.dumps(dict(kind='VIDEO_FEEDBACK',epoch=self.epoch,message=message),
            separators=(',',':'),allow_nan=False).encode()
        if len(packet)>1400:raise ValueError('Video feedback exceeds datagram bound')
        return self._send(packet)

    def maintain(self):
        """Called by session supervisor, never require user-authored retry code."""
        now = time.monotonic()
        if self._socket is None and now - self._last_open >= self.probe_interval_s:
            self._open_socket()
        if now - self._last_probe >= self.probe_interval_s:
            self._last_probe = now
            ns = time.monotonic_ns()
            with self._io_lock:
                self._pending_probes[ns] = now
                self._pending_probes = {n: t for n, t in self._pending_probes.items()
                                        if now - t <= self.failed_after_s * 2}
            self._control("PING", t1=ns)
        if self._last_rx is not None and now - self._last_rx >= self.failed_after_s:
            self._state("FAILED")
        elif self._last_rx is None and now - self._last_open >= self.failed_after_s:
            self._state("FAILED")

    def _receive(self, packet, now_ns):
        if packet.startswith(CONTROL):
            if len(packet) > 2048:
                raise ValueError("Control too large")
            value = json.loads(packet[len(CONTROL):])
            if not isinstance(value, dict):
                raise ValueError("Invalid control object")
            if value.get("epoch") != self.epoch:
                return
            kind = value.get("kind")
            if kind == "PING":
                t1 = value["t1"]
                if not isinstance(t1, int) or t1 < 0:
                    raise ValueError("Invalid probe")
                self._control("PONG", t1=t1, t2=now_ns, t3=time.monotonic_ns())
            elif kind == "PONG":
                t1, t2, t3 = value["t1"], value["t2"], value["t3"]
                if not all(isinstance(t, int) and t >= 0 for t in (t1, t2, t3)):
                    raise ValueError("Invalid clock response")
                with self._io_lock:
                    pending = self._pending_probes.pop(t1, None)
                if pending is None:
                    return
                if self.on_clock:
                    self.on_clock(t1 / 1e9, t2 / 1e9, t3 / 1e9, now_ns / 1e9)
            elif kind == "NEGOTIATE":
                if self.on_negotiation:
                    self.on_negotiation(value.get("message"))
            elif kind == 'VIDEO_FEEDBACK':
                if self.on_video_feedback:
                    self.on_video_feedback(value.get('message'))
            elif kind == "IDR":
                if self.on_keyframe_request:
                    self.on_keyframe_request()
            else:
                return
        elif packet.startswith(MAGIC):
            unit = self.assembler.push(packet, now_ns)
            if unit is not None:
                self.stats["received_units"] += 1
                if self.on_unit:
                    self.on_unit(self.path_id, unit, now_ns)
        else:
            return
        self._last_rx = now_ns / 1e9
        self._state("CONNECTED")

    def _loop(self):
        try:
            while not self._stop.is_set():
                sock = self._socket
                if sock is None:
                    self._stop.wait(.02)
                    continue
                try:
                    # Never sleep with queued work. Otherwise enqueue wakes this
                    # select directly, without polling faster or busy-spinning.
                    timeout = 0 if not self._outgoing.empty() else .01
                    readable, _, _ = select.select([sock, self._wake_reader], [], [], timeout)
                    if self._wake_reader in readable:
                        self._drain_wake()
                    if sock in readable:
                        for _ in range(64):
                            try:
                                packet, address = sock.recvfrom(1500)
                            except BlockingIOError:
                                break
                            if address != self.peer:
                                continue
                            self.stats["received_bytes"] += len(packet)
                            try:
                                self._receive(packet, time.monotonic_ns())
                            except (ValueError, KeyError, UnicodeError, TypeError):
                                self.stats["malformed"] += 1
                    packets = self._outgoing.get_nowait()
                    delivered = True
                    for packet in packets:
                        if not self._send(packet):
                            delivered = False
                            break
                    if delivered:
                        self.stats["sent_units"] += 1
                except queue.Empty:
                    pass
                except (OSError, ValueError):
                    with self._io_lock:
                        if self._socket is sock:
                            sock.close()
                            self._socket = None
                    self._state("FAILED")
        except Exception as exc:
            if self.on_error:
                self.on_error(exc)
            self._stop.set()

    def close(self):
        self._stop.set()
        self._wake()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                raise RuntimeError("UDP worker did not stop")
        with self._io_lock:
            if self._socket:
                self._socket.close()
                self._socket = None
        with self._wake_lock:
            for wake_socket in (self._wake_reader, self._wake_writer):
                if wake_socket is not None:
                    wake_socket.close()
            self._wake_reader = self._wake_writer = None
        self._state("CLOSED")
