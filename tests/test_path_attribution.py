"""A reordered copy keeps the path that actually supplied its encoded unit."""
import threading

import pytest

from video_stream.shared.core import AccessUnit, DecodeGate
from video_stream.shared.ordered import OrderedDecodeGate
from video_stream.shared.session import SharedVideoReceiver


@pytest.mark.parametrize("gate", [DecodeGate("epoch"), OrderedDecodeGate("epoch")])
def test_reordered_video_unit_retains_first_arrival_path(gate):
    receiver = SharedVideoReceiver.__new__(SharedVideoReceiver)
    receiver.gate = gate
    receiver._decode_lock = threading.RLock()
    receiver._pending_paths = {}
    receiver.on_path_unit = None
    submitted = []
    receiver._submit = lambda unit, path: submitted.append((unit.sequence, path))

    def unit(sequence, keyframe=False):
        return AccessUnit(sequence, sequence, sequence, "epoch",
                          b"\x00\x00\x01\x65" if keyframe else b"\x00\x00\x01\x41")

    receiver._unit("a", unit(1, True), 1)
    receiver._unit("c", unit(3), 2)
    receiver._unit("a", unit(3), 3)  # Duplicate does not steal path C.
    receiver._unit("b", unit(2), 4)  # Releases the previously buffered C unit.

    assert submitted == [(1, "a"), (2, "b"), (3, "c")]
    assert receiver._pending_paths == {}
