"""Bounded, nonblocking UDP byte-message adapter. Trusted networks only."""
import select
import socket
import threading
from collections import deque
from .byte_transport import TransportCapabilities,TransportCallbacks,TransportState,SendStatus


class UdpTransport:
    capabilities=TransportCapabilities(max_message_bytes=1400)

    def __init__(self,path_id,local,peer):
        if not isinstance(path_id,str) or not path_id.strip():raise ValueError("Nonempty path_id required")
        self.path_id=path_id
        self.local=local
        # Resolve once, before opening resources; avoid DNS in codec callbacks.
        self.peer=(socket.gethostbyname(peer[0]),peer[1])
        self.state=TransportState.DISCONNECTED
        self._lock=threading.RLock()
        self._callback_lock=threading.RLock()
        self._events=deque(maxlen=16)
        self._stop=threading.Event()
        self._socket=self._thread=self._callbacks=None

    def _state(self,state,reason):
        if self.state!=state:
            self.state=state
            self._events.append((state,reason))

    def _bind(self):
        sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        try:
            sock.bind(self.local)
            sock.setblocking(False)
            sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,1024*1024)
        except Exception:
            sock.close()
            raise
        self._socket=sock
        self._state(TransportState.CONNECTING,"bound; peer not yet observed")

    def open(self,callbacks:TransportCallbacks):
        with self._lock:
            if self._thread and self._thread.is_alive():raise RuntimeError("Adapter already open")
            self._callbacks=callbacks
            self._stop.clear()
            try:self._bind()
            except Exception:
                self._callbacks=None
                self.state=TransportState.FAILED
                raise
            self._thread=threading.Thread(target=self._read,name="video-udp-"+self.path_id,daemon=True)
            self._thread.start()

    def send(self,message):
        if type(message) is not bytes:raise TypeError("Transport messages must be immutable bytes")
        if not message or len(message)>self.capabilities.max_message_bytes:raise ValueError("UDP message size outside bounds")
        with self._lock:
            if self._socket is None or self._stop.is_set():return SendStatus.UNAVAILABLE
            try:
                return SendStatus.ACCEPTED if self._socket.sendto(message,self.peer)==len(message) else SendStatus.UNAVAILABLE
            except BlockingIOError:return SendStatus.BACKPRESSURE
            except OSError:
                self._socket.close()
                self._socket=None
                self._state(TransportState.FAILED,"socket send failed")
                return SendStatus.UNAVAILABLE

    def _read(self):
        try:self._read_loop()
        except Exception as exc:
            with self._lock:
                self._stop.set()
                self.state=TransportState.FAILED
                callbacks=self._callbacks
            if callbacks:callbacks.on_error(exc)

    def _read_loop(self):
        while not self._stop.is_set():
            with self._callback_lock:
                with self._lock:
                    events=list(self._events);self._events.clear()
                    callbacks=self._callbacks
                if callbacks:
                    for state,reason in events:
                        if self._stop.is_set():break
                        callbacks.on_state(state,reason)
            with self._lock:sock=self._socket
            if sock is None:
                self._stop.wait(.02)
                continue
            try:
                ready,_,_=select.select([sock],[],[],.02)
                if not ready:continue
                for _ in range(64):
                    with self._lock:
                        if self._socket is not sock or self._stop.is_set():break
                        try:data,address=sock.recvfrom(self.capabilities.max_message_bytes+1)
                        except BlockingIOError:break
                        if address!=self.peer or not data or len(data)>self.capabilities.max_message_bytes:continue
                        self._state(TransportState.CONNECTED,"peer datagram observed")
                    # Dispatch immediately rather than draining a batch first.
                    # Callbacks remain outside the socket lock and close still
                    # waits for their quiescence through the callback lock.
                    with self._callback_lock:
                        with self._lock:
                            callbacks=self._callbacks if self._socket is sock and not self._stop.is_set() else None
                        if callbacks:callbacks.on_message(data)
            except (OSError,ValueError):
                with self._lock:
                    if self._socket is sock and not self._stop.is_set():
                        sock.close()
                        self._socket=None
                        self._state(TransportState.FAILED,"socket receive failed")
            except Exception as exc:
                with self._lock:
                    self._stop.set()
                    self._state(TransportState.FAILED,"callback failed")
                    callbacks=self._callbacks
                if callbacks:callbacks.on_error(exc)

    def reconnect(self):
        if self._thread is threading.current_thread():
            raise RuntimeError("Request reconnect from the library supervisor, not an adapter callback")
        with self._lock:
            callbacks=self._callbacks
            if self._stop.is_set() or callbacks is None:return
        # Linux may retain a socket's bind while another thread is in select().
        # Quiesce that reader before rebinding, rather than relying on reuse flags.
        self.close()
        self.open(callbacks)

    def close(self):
        with self._lock:
            self._stop.set()
            if self._socket:self._socket.close()
            self._socket=None
            thread=self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
            if thread.is_alive():raise RuntimeError("UDP callback thread did not quiesce")
        with self._callback_lock:
            with self._lock:
                callbacks=self._callbacks
                notify=self.state!=TransportState.CLOSED
                self.state=TransportState.CLOSED
                self._events.clear()
            try:
                if callbacks and notify:callbacks.on_state(self.state,"closed")
            finally:
                with self._lock:self._callbacks=None
