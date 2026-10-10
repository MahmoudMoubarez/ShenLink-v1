"""Bounded pre-stream agreement over adapter control messages.

One receiver, one immutable agreement per epoch. Not live quality adaptation.
Acceptance promises the selected configuration and output age filter, NOT that
an arbitrary network can continuously meet the requested latency or FPS.
"""
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import math
import secrets
import threading
import time

from .session import BaselineProfile


class NegotiationError(RuntimeError):
    pass


def positive_int(value):
    return type(value) is int and value > 0


@dataclass(frozen=True)
class ReceiverRequirements:
    min_resolution: tuple = (640, 360)
    max_resolution: tuple = (1280, 720)
    min_fps: int = 15
    max_fps: int = 30
    min_bitrate_kbps: int = 300
    max_bitrate_kbps: int = 1800
    max_frame_age_ms: float = 300.
    codec: str = "H264"

    def __post_init__(self):
        for name in ("min_resolution", "max_resolution"):
            pair = getattr(self, name)
            if not isinstance(pair, (list, tuple)) or len(pair) != 2 or not all(positive_int(v) for v in pair):
                raise ValueError("Resolution must contain two positive integers")
            object.__setattr__(self, name, tuple(pair))
        if any(a > b for a, b in zip(self.min_resolution, self.max_resolution)):
            raise ValueError("Minimum resolution exceeds maximum")
        for low, high in ((self.min_fps, self.max_fps), (self.min_bitrate_kbps, self.max_bitrate_kbps)):
            if not positive_int(low) or not positive_int(high) or low > high:
                raise ValueError("Invalid FPS/bitrate bounds")
        if (type(self.max_frame_age_ms) not in (int, float)
                or not math.isfinite(self.max_frame_age_ms) or self.max_frame_age_ms <= 0):
            raise ValueError("Age budget must be positive and finite")
        if not isinstance(self.codec, str) or not 1 <= len(self.codec) <= 32:
            raise ValueError("Invalid codec name")

    def accepts(self, profile):
        return (self.codec == "H264"
                and all(a <= v <= b for a, v, b in zip(self.min_resolution,
                    (profile.width, profile.height), self.max_resolution))
                and self.min_fps <= profile.fps <= self.max_fps
                and self.min_bitrate_kbps <= profile.bitrate_kbps <= self.max_bitrate_kbps
                and profile.max_frame_age_ms == self.max_frame_age_ms)


def profile_from_wire(value):
    if not isinstance(value, dict) or set(value) != set(asdict(BaselineProfile())):
        raise ValueError("Invalid profile fields")
    if not all(positive_int(value[k]) for k in ("width", "height", "fps", "bitrate_kbps")):
        raise ValueError("Invalid media dimensions/FPS/bitrate")
    if value['width'] % 2 or value['height'] % 2:
        raise ValueError("H264 I420 requires even dimensions")
    if type(value['max_frame_age_ms']) not in (int, float):
        raise ValueError("Invalid profile age budget")
    return BaselineProfile(**value)


def choose_profile(requirements, supported):
    if requirements.codec != "H264":
        raise NegotiationError("UNSUPPORTED_CODEC: this implementation supports H264 only")
    if not 1 <= len(supported) <= 32:
        raise ValueError("Advertise between 1 and 32 supported profiles")
    profiles = [replace(profile_from_wire(asdict(p)), max_frame_age_ms=requirements.max_frame_age_ms)
                for p in supported]
    candidates = [p for p in profiles if requirements.accepts(p)]
    if not candidates:
        raise NegotiationError("NO_COMMON_PROFILE: resolution/FPS/bitrate bounds cannot be satisfied")
    # Start at the lowest supported quality within the receiver's bounds.
    return min(candidates, key=lambda p: (p.width*p.height, p.fps, p.bitrate_kbps, p.width, p.height))


def agreement_id(request_id, profile):
    return hashlib.sha256(json.dumps([request_id, asdict(profile)], sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()[:24]


class VideoNegotiator:
    """Three-path retrying REQUEST/OFFER/ACCEPT/COMMIT/READY handshake.

    Sender releases only on READY. Receiver releases only on verified COMMIT.
    Retained callbacks acknowledge duplicate COMMITs after wait() returns.
    Media sessions must be constructed/started only after successful wait().
    """
    def __init__(self, paths, epoch, role, *, requirements=None, supported_profiles=(), timeout_s=10., contract_id=None):
        if contract_id is not None and (not isinstance(contract_id,str) or len(contract_id)!=64
                or any(c not in '0123456789abcdef' for c in contract_id)):
            raise ValueError('Invalid full configuration contract')
        self.contract_id=contract_id
        if (role not in ("sender", "receiver") or not paths
                or len({p.path_id for p in paths}) != len(paths)
                or any(p.epoch != epoch for p in paths)
                or not math.isfinite(timeout_s) or not 0 < timeout_s <= 60):
            raise ValueError("Invalid negotiation configuration")
        if role == "receiver" and not isinstance(requirements, ReceiverRequirements):
            raise ValueError("Receiver requirements required")
        if role == "sender":
            if not 1 <= len(supported_profiles) <= 32:
                raise ValueError("Sender supported profiles required")
            for p in supported_profiles: profile_from_wire(asdict(p))
        self.paths, self.epoch, self.role = list(paths), epoch, role
        self.requirements, self.supported = requirements, tuple(supported_profiles)
        self.timeout_s = timeout_s
        self.request_id = secrets.token_hex(12) if role == "receiver" else None
        self.profile = None
        self.reason = None
        self.state = "REQUESTING" if role == "receiver" else "WAITING"
        self._lock = threading.RLock()
        self._outgoing = None
        self._opened = False
        self._started = None
        self._last_send = 0.
        self.stats = dict(control_transmissions=0, malformed=0, elapsed_ms=None)
        for path in self.paths:
            path.on_negotiation = lambda message, path=path: self._receive(message, path)

    def _message(self, phase, **fields):
        if self.contract_id is not None:fields['contract_id']=self.contract_id
        return dict(version=1, request_id=self.request_id, phase=phase, **fields)

    def _send(self, message, reply_path=None):
        if message:
            # Immediate replies use the arriving path. Only timed retries fan
            # out, avoiding exponential duplicate control amplification.
            for path in ([reply_path] if reply_path is not None else self.paths):
                path.send_negotiation(message)
                self.stats['control_transmissions'] += 1

    def _receive(self, message, reply_path=None):
        with self._lock:
            try:
                if not isinstance(message, dict) or type(message.get('version')) is not int or message['version'] != 1:
                    raise ValueError("Unsupported control version")
                rid = message.get('request_id')
                if not isinstance(rid, str) or len(rid) != 24 or any(c not in '0123456789abcdef' for c in rid):
                    raise ValueError("Invalid request identity")
                phase = message.get('phase')
                if message.get('contract_id')!=self.contract_id:
                    if self.role=='sender' and phase=='REQUEST':
                        self.request_id=rid
                        self.reason='CONFIGURATION_MISMATCH: ladder or delay policy differs'
                        self.state='REJECTING'
                        self._outgoing=self._message('REJECT',reason=self.reason)
                        self._send(self._outgoing,reply_path)
                    elif self.role=='receiver' and rid==self.request_id:
                        self.reason='CONFIGURATION_MISMATCH: ladder or delay policy differs'
                        self.state='REJECTED'
                    return
                if self.role == 'sender' and phase == 'REQUEST':
                    if self.request_id is not None and self.request_id != rid:
                        return  # No implicit mid-stream renegotiation.
                    request = ReceiverRequirements(**message['requirements'])
                    if self.request_id is not None:
                        if request != self.requirements:
                            raise ValueError("Conflicting duplicate request")
                        self._send(self._outgoing, reply_path)
                        return
                    self.request_id, self.requirements = rid, request
                    try:
                        self.profile = choose_profile(request, self.supported)
                        self.state = 'OFFERING'
                        self._outgoing = self._message('OFFER', profile=asdict(self.profile),
                            agreement_id=agreement_id(rid, self.profile))
                    except NegotiationError as exc:
                        self.reason = str(exc)
                        self.state = 'REJECTING'
                        self._outgoing = self._message('REJECT', reason=self.reason)
                    self._send(self._outgoing, reply_path)
                    return
                if rid != self.request_id:
                    return
                if self.role == 'receiver' and phase == 'REJECT':
                    if self.state == 'AGREED': return
                    reason = message.get('reason')
                    if not isinstance(reason, str) or not 1 <= len(reason) <= 240:
                        raise ValueError("Invalid rejection reason")
                    self.reason, self.state = reason, 'REJECTED'
                    self._outgoing = self._message('REJECT_ACK')
                elif self.role == 'sender' and phase == 'REJECT_ACK' and self.state == 'REJECTING':
                    self.state = 'REJECTED'
                elif self.role == 'receiver' and phase == 'OFFER':
                    profile = profile_from_wire(message['profile'])
                    if not self.requirements.accepts(profile) or message.get('agreement_id') != agreement_id(rid, profile):
                        raise ValueError("Offer outside receiver bounds or identity mismatch")
                    if self.profile is not None and self.profile != profile:
                        raise ValueError("Conflicting profile offer")
                    if self.state not in ('REQUESTING', 'ACCEPTING', 'AGREED'): return
                    self.profile = profile
                    if self.state != 'AGREED':
                        self.state = 'ACCEPTING'
                        self._outgoing = self._message('ACCEPT', agreement_id=agreement_id(rid, profile))
                elif self.profile is not None and message.get('agreement_id') == agreement_id(rid, self.profile):
                    if self.role == 'sender' and phase == 'ACCEPT' and self.state in ('OFFERING', 'COMMITTING', 'AGREED'):
                        if self.state != 'AGREED': self.state = 'COMMITTING'
                        self._outgoing = self._message('COMMIT', agreement_id=agreement_id(rid, self.profile))
                    elif self.role == 'receiver' and phase == 'COMMIT' and self.state in ('ACCEPTING', 'AGREED'):
                        self.state = 'AGREED'
                        self._outgoing = self._message('READY', agreement_id=agreement_id(rid, self.profile))
                    elif self.role == 'sender' and phase == 'READY' and self.state in ('COMMITTING', 'AGREED'):
                        self.state = 'AGREED'
                        return
                    else:return
                else:return
                self._send(self._outgoing, reply_path)
            except (ValueError, KeyError, TypeError, OverflowError):
                self.stats['malformed'] += 1

    def wait(self):
        if self._opened: raise RuntimeError("Negotiation wait is single-use")
        self._opened = True
        self._started = time.monotonic()
        if self.role == 'receiver':
            self._outgoing = self._message('REQUEST', requirements=asdict(self.requirements))
        try:
            for path in self.paths: path.open()
            while True:
                with self._lock:
                    if self.state == 'AGREED':
                        self.stats['elapsed_ms'] = (time.monotonic()-self._started)*1000
                        return self.profile
                    if self.state == 'REJECTED': raise NegotiationError(self.reason)
                    if time.monotonic()-self._started >= self.timeout_s:
                        raise NegotiationError(self.reason or "NEGOTIATION_TIMEOUT: no confirmed agreement; video not started")
                    if time.monotonic()-self._last_send >= .2:
                        self._last_send = time.monotonic()
                        self._send(self._outgoing)
                for path in self.paths: path.maintain()
                time.sleep(.01)
        except Exception:
            for path in self.paths: path.close()
            raise
