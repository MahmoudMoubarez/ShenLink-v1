"""Multiplex the existing codec/control engine over a developer byte adapter."""
import json
import queue
import threading
import time
from ..transport import SendStatus,TransportCallbacks,TransportState
from .core import CHUNK,HEADER
from .udp import UdpVideoPath

HANDSHAKE=b"V1C"
LANES={"baseline":b"V1B","high":b"V1H"}


class TransportHub:
    def __init__(self,adapter,on_control,on_error,on_state=None):
        if adapter.capabilities.max_message_bytes<HEADER.size+CHUNK+3:
            raise ValueError("Adapter message limit cannot carry current video fragments")
        self.adapter=adapter
        self.on_control,self.on_error,self.on_state=on_control,on_error,on_state
        self.channels={}
        self.opened=False
        self.last_retry=0.
        self._lock=threading.Lock()

    def open(self):
        self.opened=True
        try:self.adapter.open(TransportCallbacks(self._receive,self._state,self.on_error))
        except Exception:
            self.close()
            raise

    def _state(self,state,reason):
        if self.on_state:self.on_state(self.adapter.path_id,state,reason)

    def _receive(self,message):
        if type(message) is not bytes or len(message)>self.adapter.capabilities.max_message_bytes:return
        if message.startswith(HANDSHAKE):
            try:value=json.loads(message[3:])
            except (ValueError,UnicodeError):return
            self.on_control(value)
        else:
            channel=self.channels.get(message[:3])
            if channel and channel.enabled:
                try:channel._receive(message[3:],time.monotonic_ns())
                except (ValueError,KeyError,UnicodeError,TypeError):
                    channel.stats["malformed"]+=1

    def send(self,message):
        if len(message)>self.adapter.capabilities.max_message_bytes:raise ValueError("Control/message exceeds adapter bound")
        return self.adapter.send(message)==SendStatus.ACCEPTED

    def control(self,value):
        return self.send(HANDSHAKE+json.dumps(value,separators=(",",":"),allow_nan=False).encode())

    def maintain(self):
        with self._lock:
            now=time.monotonic()
            if self.opened and self.adapter.state==TransportState.FAILED and now-self.last_retry>=.2:
                self.last_retry=now
                self.adapter.reconnect()

    def close(self):
        with self._lock:
            opened=self.opened
            self.opened=False
        if opened:self.adapter.close()


class MessageVideoPath(UdpVideoPath):
    """Reuse tested media framing/probes, never the old UDP socket implementation."""
    def __init__(self,hub,tier,epoch):
        super().__init__(hub.adapter.path_id,("127.0.0.1",0),("127.0.0.1",0),epoch)
        self.hub,self.lane=hub,LANES[tier]
        self.enabled=False
        self._tx_ready=threading.Event()
        self._tx_lock=threading.Lock()
        hub.channels[self.lane]=self

    def open(self):
        with self._tx_lock:
            if self.enabled:return
            self.enabled=True
            self._stop.clear()
            self._tx_ready.clear()
            self._last_open=time.monotonic()
            self._thread=threading.Thread(target=self._transmit,
                name="video-send-"+self.path_id+"-"+self.lane.decode(),daemon=True)
            self._thread.start()

    def close(self):
        with self._tx_lock:
            self.enabled=False
            self._stop.set()
            self._tx_ready.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
            if self._thread.is_alive():raise RuntimeError("Video transmit worker did not quiesce")
        while True:
            try:self._outgoing.get_nowait()
            except queue.Empty:break
        self._state("CLOSED")

    def _send(self,packet):
        if not self.enabled:return False
        accepted=self.hub.send(self.lane+packet)
        if accepted:self.stats["sent_bytes"]+=len(packet)
        return accepted

    def send_packets(self,packets):
        """Accept one complete AU locally; never send from the codec callback.

        Two queued AUs plus one in flight, just like the legacy path. No
        independent fragment dropping/replacement: H264 reference safety is
        still handled by the receiver gate when transport delivery fails.
        """
        with self._tx_lock:
            if not self.enabled or self._stop.is_set():return False
            try:self._outgoing.put_nowait(packets)
            except queue.Full:
                self.stats["queue_drops"]+=1
                return False
            self._tx_ready.set()
            return True

    def _transmit(self):
        try:
            while not self._stop.is_set():
                self._tx_ready.wait()
                self._tx_ready.clear()
                while not self._stop.is_set():
                    try:packets=self._outgoing.get_nowait()
                    except queue.Empty:break
                    for packet in packets:
                        if self._stop.is_set() or not self._send(packet):
                            self.stats["queue_drops"]+=1
                            break
                    else:self.stats["sent_units"]+=1
        except Exception as exc:
            self._stop.set()
            self.hub.on_error(exc)

    def maintain(self):
        self.hub.maintain()
        now=time.monotonic()
        if now-self._last_probe>=self.probe_interval_s:
            self._last_probe=now
            ns=time.monotonic_ns()
            with self._io_lock:
                self._pending_probes[ns]=now
                self._pending_probes={n:t for n,t in self._pending_probes.items() if now-t<=self.failed_after_s*2}
            self._control("PING",t1=ns)
        if self._last_rx is not None and now-self._last_rx>=self.failed_after_s:
            self._state("FAILED")
