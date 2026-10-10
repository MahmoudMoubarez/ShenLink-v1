"""Independent health recovery, ACK scope, and bounded shutdown regressions."""
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from multiflow_comm.messages import MessageSender
from multiflow_comm.transport import FakeMessageSenderAdapter
from multiflow_comm.types import ConnectionState, DeliveryMode, ReceiptPolicy
from multiflow_comm.wire import decode_frame, encode_frame


class ProbePath(FakeMessageSenderAdapter):
    def __init__(self, path):
        super().__init__(path)
        self.state = ConnectionState.FAILED
        self.called = threading.Event()
        self.success = threading.Event()
        self.sent = []
        self.timeouts = 0
        self.success_count = 0
        self.hook = lambda frame: None

    def send_probe(self, frame):
        self.sent.append(frame)
        self.called.set()
        self.hook(frame)
        return True

    def record_success(self):
        self.success_count += 1
        self.state = ConnectionState.CONNECTED
        self.success.set()

    def record_timeout(self):
        self.timeouts += 1
        self.state = ConnectionState.FAILED


def sender_for(paths, timeout=.5):
    return MessageSender('sensors', paths, mode=DeliveryMode.LATEST_ONLY,
                         receipt=ReceiptPolicy.NONE, probe_interval_s=.02,
                         probe_timeout_s=timeout)


def start_probes(sender):
    sender._probe_thread = threading.Thread(target=sender._probe_loop, daemon=True)
    sender._probe_thread.start()


def ack(sender, path, frame, **overrides):
    header, _ = decode_frame(frame)
    fields = dict(kind='ACK', session_id=header['session_id'], channel_id=header['channel_id'],
                  message_id=header['message_id'], sequence=0, mode=header['mode'],
                  priority=700, receipt='NONE', payload=b'')
    fields.update(overrides)
    sender._on_inbound_ack(path, encode_frame(**fields))


def test_dead_paths_do_not_delay_third_path_or_fanout_pool():
    paths = [ProbePath(path) for path in 'abc']
    sender = sender_for(paths)
    paths[2].hook = lambda frame: ack(sender, 'c', frame)
    try:
        start = time.monotonic()
        start_probes(sender)
        assert paths[2].success.wait(.3)
        assert time.monotonic()-start < .3
        assert paths[0].called.wait(.1) and paths[1].called.wait(.1)
        assert sender._fanout_executor.submit(lambda: 'free').result(timeout=.1) == 'free'
        assert paths[0].timeouts == paths[1].timeouts == 0  # Their 500ms waits remain in progress.
    finally:
        sender.close()
    assert not sender._pending_probes
    assert all(not thread.is_alive() for thread in sender._probe_executor._threads)


def test_success_on_one_path_keeps_probing_without_waiting_for_dead_rounds():
    paths = [ProbePath(path) for path in 'abc']
    sender = sender_for(paths)
    paths[2].hook = lambda frame: ack(sender, 'c', frame)
    try:
        start_probes(sender)
        assert paths[2].success.wait(.3)
        time.sleep(.12)
        assert len(paths[2].sent) >= 3
        assert len(paths[0].sent) == len(paths[1].sent) == 1
    finally:
        sender.close()


def test_wrong_ack_scope_cannot_verify_path():
    path = ProbePath('c')
    sender = sender_for([path], timeout=.08)
    def wrong(frame):
        ack(sender, 'c', frame, session_id='old')
        ack(sender, 'c', frame, channel_id='wrong')
        ack(sender, 'a', frame)
        ack(sender, 'c', frame, message_id='wrong')
    path.hook = wrong
    try:
        start_probes(sender)
        assert path.called.wait(.2)
        assert not path.success.wait(.15)
        assert path.timeouts >= 1
    finally:
        sender.close()


def test_shutdown_wakeup_is_not_an_ack_and_does_not_wait_full_timeout():
    paths = [ProbePath(path) for path in 'abc']
    sender = sender_for(paths, timeout=10)
    start_probes(sender)
    assert all(path.called.wait(.3) for path in paths)
    start = time.monotonic()
    sender.close()
    assert time.monotonic()-start < .5
    assert all(path.success_count == path.timeouts == 0 for path in paths)
    assert not sender._pending_probes
    assert all(not thread.is_alive() for thread in sender._probe_executor._threads)


def test_session_reset_invalidates_already_received_old_probe_ack():
    path = ProbePath('c')
    sender = sender_for([path], timeout=.08)
    def reset_after_ack(frame):
        if len(path.sent) != 1:
            return
        ack(sender, 'c', frame)
        with sender._lock:
            sender._session_id = 'replacement'
    path.hook = reset_after_ack
    try:
        start_probes(sender)
        assert path.called.wait(.2)
        assert not path.success.wait(.1)
    finally:
        sender.close()


def test_close_from_health_callback_does_not_join_its_own_worker():
    path = ProbePath('c')
    sender = sender_for([path])
    path.hook = lambda frame: ack(sender, 'c', frame)
    closed = threading.Event()
    def success():
        sender.close()
        closed.set()
    path.record_success = success
    start_probes(sender)
    assert closed.wait(.5)
    sender._probe_executor.shutdown(wait=True)
    assert not sender._pending_probes
