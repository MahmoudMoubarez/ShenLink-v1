"""Bounded encoded-unit assembly and deduplication BEFORE shared H264 decode.

One encoded sequence/epoch is copied to every path. Independently encoded
streams MUST NOT be fed to this gate. No wait for all path copies is performed.
This is lab framing, not a replacement for an authenticated transport protocol.
"""
from dataclasses import dataclass
import hashlib
import math
import struct

HEADER = struct.Struct("!4s16sQQQHHI")
MAGIC = b"SVB1"
CHUNK = 1150
MAX_UNIT = 1024 * 1024


def epoch_bytes(epoch):
    if not isinstance(epoch, str) or not epoch:
        raise ValueError("Nonempty stream epoch required")
    return hashlib.sha256(epoch.encode()).digest()[:16]


def is_idr(payload):
    """Annex-B H264 IDR detection; no decoder or image allocation."""
    offset = 0
    while True:
        offset = payload.find(b"\x00\x00\x01", offset)
        if offset < 0 or offset + 3 >= len(payload):
            return False
        if payload[offset + 3] & 31 == 5:
            return True
        offset += 3


@dataclass(frozen=True)
class AccessUnit:
    sequence: int  # Encoded order, independent of capture IDs dropped pre-encode.
    frame_id: int
    source_ns: int
    epoch: str
    payload: bytes

    @property
    def keyframe(self):
        return is_idr(self.payload)


def fragment_unit(unit):
    if not unit.payload or len(unit.payload) > MAX_UNIT:
        raise ValueError("Encoded unit outside size bound")
    if min(unit.sequence, unit.frame_id, unit.source_ns) < 0:
        raise ValueError("Negative encoded metadata")
    count = math.ceil(len(unit.payload) / CHUNK)
    epoch = epoch_bytes(unit.epoch)
    return tuple(HEADER.pack(MAGIC, epoch, unit.sequence, unit.frame_id,
                             unit.source_ns, i, count, len(unit.payload))
                 + unit.payload[i * CHUNK:(i + 1) * CHUNK]
                 for i in range(count))


class FragmentAssembler:
    """One path's incomplete AUs, strictly bounded by bytes, count and time."""
    def __init__(self, epoch, *, timeout_ns=150_000_000, max_units=8):
        if timeout_ns <= 0 or max_units <= 0:
            raise ValueError("Positive assembly bounds required")
        self.epoch = epoch
        self.tag = epoch_bytes(epoch)
        self.timeout_ns = timeout_ns
        self.max_units = max_units
        self.pending = {}
        self.expired = self.invalid = 0

    def expire(self, now_ns):
        for seq, entry in list(self.pending.items()):
            if now_ns - entry[0] > self.timeout_ns:
                del self.pending[seq]
                self.expired += 1

    def push(self, packet, now_ns):
        self.expire(now_ns)
        if len(packet) < HEADER.size:
            raise ValueError("Truncated shared video header")
        magic, epoch, seq, fid, source, index, count, size = HEADER.unpack_from(packet)
        if (magic != MAGIC or epoch != self.tag or not 0 < size <= MAX_UNIT
                or count != math.ceil(size / CHUNK) or not 0 <= index < count):
            raise ValueError("Invalid/shared epoch mismatch")
        part = packet[HEADER.size:]
        expected = min(CHUNK, size - index * CHUNK)
        if len(part) != expected:
            raise ValueError("Invalid fragment length")
        signature = (fid, source, count, size)
        entry = self.pending.get(seq)
        if entry is None:
            if len(self.pending) >= self.max_units:
                oldest = min(self.pending, key=lambda s: self.pending[s][0])
                del self.pending[oldest]
                self.expired += 1
            entry = (now_ns, signature, {})
            self.pending[seq] = entry
        if entry[1] != signature:
            del self.pending[seq]
            raise ValueError("Conflicting encoded metadata")
        if index in entry[2] and entry[2][index] != part:
            del self.pending[seq]
            raise ValueError("Conflicting duplicate fragment")
        entry[2][index] = part
        if len(entry[2]) != count:
            return None
        del self.pending[seq]
        return AccessUnit(seq, fid, source, self.epoch,
                          b"".join(entry[2][i] for i in range(count)))


class DecodeGate:
    """First complete copy wins. Preserve H264 reference order, not path order.

Missing encoded sequence: bounded reorder wait, then IDR recovery. A stale
reference may need decode, but public output age filtering is a separate step.
"""
    def __init__(self, epoch, *, reorder_ns=50_000_000, max_units=8,
                 max_bytes=2 * MAX_UNIT):
        if min(reorder_ns, max_units, max_bytes) <= 0:
            raise ValueError("Positive decode bounds required")
        self.epoch = epoch
        self.reorder_ns = reorder_ns
        self.max_units = max_units
        self.max_bytes = max_bytes
        self.pending = {}
        self.last_sequence = -1
        self.waiting_idr = True
        self.stats = dict(submitted=0, duplicates_before_decode=0,
                          discarded_for_recovery=0, reference_breaks=0)

    def push(self, unit, now_ns):
        if unit.epoch != self.epoch:
            raise ValueError("Wrong encoded epoch")
        if not unit.payload or len(unit.payload) > MAX_UNIT:
            raise ValueError("Encoded unit outside size bound")
        if unit.sequence <= self.last_sequence:
            self.stats["duplicates_before_decode"] += 1
            return []
        if unit.sequence in self.pending:
            if self.pending[unit.sequence][0] != unit:
                raise ValueError("Conflicting duplicate access unit")
            self.stats["duplicates_before_decode"] += 1
            return []
        self.pending[unit.sequence] = (unit, now_ns)
        if (len(self.pending) > self.max_units
                or sum(len(v[0].payload) for v in self.pending.values()) > self.max_bytes):
            self.waiting_idr = True
            self.stats["reference_breaks"] += 1
            newest = max(self.pending)
            kept = self.pending[newest]
            self.stats["discarded_for_recovery"] += len(self.pending) - 1
            self.pending = {newest: kept}
        return self.drain(now_ns)

    def drain(self, now_ns):
        ready = []
        while self.pending:
            if self.waiting_idr:
                keys = [s for s, (u, _) in self.pending.items() if u.keyframe]
                if not keys:
                    # Non-IDRs cannot bootstrap; retain no unbounded dead chain.
                    self.stats["discarded_for_recovery"] += len(self.pending)
                    self.pending.clear()
                    break
                seq = max(keys)
                for old in [s for s in self.pending if s < seq]:
                    del self.pending[old]
                    self.stats["discarded_for_recovery"] += 1
                self.last_sequence = seq - 1
                self.waiting_idr = False
            seq = self.last_sequence + 1
            if seq not in self.pending:
                oldest_at = min(at for _, at in self.pending.values())
                # A later IDR is a safe reset without waiting for a missing delta.
                if any(u.keyframe for u, _ in self.pending.values()):
                    self.waiting_idr = True
                    continue
                if now_ns - oldest_at >= self.reorder_ns:
                    self.waiting_idr = True
                    self.stats["reference_breaks"] += 1
                    continue
                break
            unit, _ = self.pending.pop(seq)
            self.last_sequence = seq
            self.stats["submitted"] += 1
            ready.append(unit)
        return ready
