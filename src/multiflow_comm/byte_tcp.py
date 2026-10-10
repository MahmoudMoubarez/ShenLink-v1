"""Bounded message-framed TCP adapter; trusted networks, no TLS authentication."""
from collections import deque
import errno
import select
import socket
import struct
import threading
from .byte_transport import TransportCapabilities, TransportState, SendStatus


class MessageFramer:
    """TCP chunks are not messages. Reject oversized lengths before allocation."""
    def __init__(self,limit):self.limit=limit;self.buffer=bytearray()
    def feed(self,data):
        self.buffer.extend(data)
        while len(self.buffer)>=4:
            size=struct.unpack_from('!I',self.buffer)[0]
            if not 0<size<=self.limit:raise ValueError('Invalid TCP message length')
            if len(self.buffer)<size+4:break
            message=bytes(self.buffer[4:size+4]);del self.buffer[:size+4]
            yield message


class TcpTransport:
    capabilities=TransportCapabilities(reliable=True,ordered=True,max_message_bytes=1400)
    def __init__(self,path_id,local,peer,*,mode='connect',queue_bytes=262144):
        if not isinstance(path_id,str) or not path_id.strip():raise ValueError('Nonempty path ID required')
        if mode not in ('connect','listen'):raise ValueError('TCP mode must be connect or listen')
        if type(queue_bytes) is not int or not 1404<=queue_bytes<=16777216:raise ValueError('Invalid TCP queue bound')
        self.path_id=path_id;self.local=local;self.peer=(socket.gethostbyname(peer[0]),peer[1])
        self.mode=mode;self.queue_bytes=queue_bytes;self.state=TransportState.DISCONNECTED
        self._lock=threading.RLock();self._callbacks_lock=threading.RLock()
        self._stop=threading.Event();self._queue=deque();self._queued_bytes=0
        self._socket=self._listener=self._thread=self._callbacks=None
        self._wake_r=self._wake_w=None;self._connecting=False

    def _notify(self,state,reason):
        with self._lock:
            self.state=state;callbacks=self._callbacks
        with self._callbacks_lock:
            if callbacks and not self._stop.is_set():callbacks.on_state(state,reason)

    def open(self,callbacks):
        with self._lock:
            if self._thread and self._thread.is_alive():raise RuntimeError('Adapter already open')
            self._callbacks=callbacks;self._stop.clear();self._queue.clear();self._queued_bytes=0
            self._wake_r,self._wake_w=socket.socketpair()
            self._wake_r.setblocking(False);self._wake_w.setblocking(False)
            sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                sock.setblocking(False);sock.bind(self.local)
                if self.mode=='listen':sock.listen(1);self._listener=sock
                else:
                    result=sock.connect_ex(self.peer)
                    if result not in (0,errno.EINPROGRESS,errno.EWOULDBLOCK,errno.EALREADY,10035):
                        raise OSError(result,'TCP connect failed')
                    self._socket=sock;self._connecting=result!=0
                self.state=TransportState.CONNECTING
                self._thread=threading.Thread(target=self._run,name='video-tcp-'+self.path_id,daemon=True)
                self._thread.start()
            except Exception:
                sock.close()
                for wake in (self._wake_r,self._wake_w):wake.close()
                self._wake_r=self._wake_w=None;self._socket=self._listener=None
                self._callbacks=None;self.state=TransportState.FAILED
                raise

    def send(self,message):
        if type(message) is not bytes:raise TypeError('TCP messages must be immutable bytes')
        if not 0<len(message)<=self.capabilities.max_message_bytes:raise ValueError('TCP message outside bounds')
        with self._lock:
            if self.state!=TransportState.CONNECTED or self._stop.is_set():return SendStatus.UNAVAILABLE
            size=len(message)+4
            if self._queued_bytes+size>self.queue_bytes:return SendStatus.BACKPRESSURE
            self._queue.append(struct.pack('!I',len(message))+message);self._queued_bytes+=size
            try:self._wake_w.send(b'x')
            except BlockingIOError:pass
            return SendStatus.ACCEPTED

    def _run(self):
        framer=MessageFramer(self.capabilities.max_message_bytes);pending=b''
        try:
            self._notify(TransportState.CONNECTING,'TCP setup')
            if self._socket is not None and not self._connecting:self._notify(TransportState.CONNECTED,'TCP connected')
            while not self._stop.is_set():
                with self._lock:
                    sock=self._socket;listener=self._listener
                    if not pending and self._queue:pending=self._queue.popleft()
                readers=[self._wake_r]+([listener] if listener else [])+([sock] if sock and not self._connecting else [])
                writers=[sock] if sock and (self._connecting or pending) else []
                read,write,_=select.select(readers,writers,[],.1)
                if self._stop.is_set():break
                if self._wake_r in read:
                    while True:
                        try:
                            if not self._wake_r.recv(4096):break
                        except BlockingIOError:break
                if listener and listener in read:
                    connected,address=listener.accept()
                    if address[0]!=self.peer[0] or (self.peer[1] and address[1]!=self.peer[1]):connected.close();continue
                    connected.setblocking(False);connected.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                    with self._lock:self._socket=connected;self._listener=None
                    listener.close();self._notify(TransportState.CONNECTED,'TCP peer accepted')
                if sock and sock in write:
                    if self._connecting:
                        result=sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                        if result:raise OSError(result,'TCP connection failed')
                        self._connecting=False;self._notify(TransportState.CONNECTED,'TCP connected')
                    if pending:
                        try:sent=sock.send(pending)
                        except BlockingIOError:sent=0
                        pending=pending[sent:]
                        with self._lock:self._queued_bytes-=sent
                if sock and sock in read:
                    try:data=sock.recv(65536)
                    except BlockingIOError:continue
                    if not data:raise ConnectionError('TCP peer disconnected; queued delivery is not guaranteed')
                    for message in framer.feed(data):
                        with self._callbacks_lock:
                            if self._stop.is_set():break
                            callbacks=self._callbacks
                            if callbacks:
                                try:callbacks.on_message(message)
                                except Exception as exc:
                                    self._notify(TransportState.FAILED,'TCP message callback failed')
                                    callbacks.on_error(exc)
                                    return
        except Exception as exc:
            if not self._stop.is_set():self._notify(TransportState.FAILED,str(exc))
        finally:
            with self._lock:
                for sock in (self._socket,self._listener):
                    if sock:sock.close()
                self._socket=self._listener=None;self._queue.clear();self._queued_bytes=0

    def reconnect(self):
        if self._thread is threading.current_thread():raise RuntimeError('Reconnect from supervisor, not callback')
        with self._lock:callbacks=self._callbacks
        if callbacks is not None and not self._stop.is_set():self.close();self.open(callbacks)

    def close(self):
        with self._lock:
            self._stop.set();thread=self._thread
            if self._wake_w:
                try:self._wake_w.send(b'x')
                except (BlockingIOError,OSError):pass
        if thread and thread is not threading.current_thread():
            thread.join(2)
            if thread.is_alive():raise RuntimeError('TCP callback worker did not stop')
        with self._callbacks_lock,self._lock:
            callbacks=self._callbacks;notify=self.state!=TransportState.CLOSED
            self._callbacks=None;self.state=TransportState.CLOSED
            self._queue.clear();self._queued_bytes=0
            for wake in (self._wake_r,self._wake_w):
                if wake:wake.close()
            self._wake_r=self._wake_w=None
            if notify and callbacks:callbacks.on_state(TransportState.CLOSED,'TCP closed')
