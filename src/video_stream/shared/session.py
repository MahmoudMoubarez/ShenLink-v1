"""Single-profile shared encoder/decoder sessions, independent of path count.

Opt-in phase one, not adaptive high-quality mode or cross-service priority proof.
Native GStreamer does codec work. Python manages bounded compressed units only.
"""
from collections import deque
from dataclasses import dataclass
import threading
import time

from ..clock_mapping import ClockMapper
from ..live.types import FrameTiming, LiveFrame
from ..contracts.types import PixelFormat
from .core import AccessUnit, DecodeGate, fragment_unit
from .delivery import DelayPolicy,accepts_age
from .ordered import OrderedDecodeGate


@dataclass(frozen=True)
class BaselineProfile:
    width: int = 1280
    height: int = 720
    fps: int = 30
    bitrate_kbps: int = 1800
    max_frame_age_ms: float = 300.

    def __post_init__(self):
        import math
        if min(self.width, self.height, self.fps, self.bitrate_kbps) <= 0:
            raise ValueError("Positive media profile required")
        if not math.isfinite(self.max_frame_age_ms) or self.max_frame_age_ms <= 0:
            raise ValueError("Positive finite age limit required")


def pack_rgb_rows(data,width,height,stride,offset=0):
    """Normalize native row padding without exposing it as image pixels."""
    row_bytes=width*3
    if stride<row_bytes or offset<0 or offset+(height-1)*stride+row_bytes>len(data):
        raise ValueError('Invalid native RGB plane layout')
    if stride==row_bytes and offset==0 and len(data)==row_bytes*height:return data
    return b''.join(data[offset+y*stride:offset+y*stride+row_bytes] for y in range(height))


def gst_modules():
    import gi
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst
    Gst.init(None)
    return Gst


def codec_counts(pipeline):
    """Inspect actual instantiated native elements, not declared config counts."""
    Gst = gst_modules()
    result = dict(encoders=0, decoders=0)
    iterator = pipeline.iterate_recurse()
    while True:
        status, element = iterator.next()
        if status == Gst.IteratorResult.DONE:
            break
        if status != Gst.IteratorResult.OK:
            raise RuntimeError("Unstable codec inventory")
        factory = element.get_factory()
        if factory:
            klass = factory.get_metadata("klass") or ""
            if "Encoder/Video" in klass:
                result["encoders"] += 1
            if "Decoder/Video" in klass:
                result["decoders"] += 1
    return result


class _Session:
    def __init__(self, paths, epoch, on_error, on_state):
        if not paths or len({p.path_id for p in paths}) != len(paths):
            raise ValueError("Unique nonempty video paths required")
        if any(p.epoch != epoch for p in paths):
            raise ValueError("Every path must carry the same shared epoch")
        self.paths, self.epoch = list(paths), epoch
        self.on_error, self.on_state = on_error, on_state
        self._stop = threading.Event()
        self._supervisor = None
        self.pipeline = None
        self._started = False
        for path in self.paths:
            path.on_error = self._fail
            path.on_state = on_state

    def _fail(self, error):
        self._stop.set()
        if self.on_error:
            self.on_error(error)

    def _supervise(self):
        Gst = gst_modules()
        bus = self.pipeline.get_bus()
        try:
            while not self._stop.wait(.02):
                for path in self.paths:
                    path.maintain()
                self._tick()
                error = bus.pop_filtered(Gst.MessageType.ERROR)
                if error:
                    raise RuntimeError(str(error.parse_error()))
        except Exception as exc:
            self._fail(exc)

    def _tick(self):
        pass

    def start(self):
        if self._started:
            raise RuntimeError("Use a new session/epoch after stop")
        self._started = True
        self._stop.clear()
        Gst = gst_modules()
        try:
            for path in self.paths:
                path.open()
            if self.pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
                raise RuntimeError("Native video pipeline failed to start")
            self._supervisor = threading.Thread(target=self._supervise,
                                               name="shared-video-supervisor", daemon=True)
            self._supervisor.start()
        except Exception:
            self.stop()
            raise

    def stop(self):
        self._stop.set()
        if self._supervisor and self._supervisor is not threading.current_thread():
            self._supervisor.join(timeout=3)
            if self._supervisor.is_alive():
                raise RuntimeError("Video supervisor did not stop")
        # Stop codec callbacks before tearing down transport workers.
        if self.pipeline:
            self.pipeline.set_state(gst_modules().State.NULL)
        for path in self.paths:
            path.close()

    def counters(self):
        return dict(codecs=codec_counts(self.pipeline),
                    paths={p.path_id: dict(p.stats, state=p.state,
                              network_priority_status=p.network_priority_status)
                           for p in self.paths})


class SharedVideoSender(_Session):
    """Encode once; immutable AU fragments are shared by every path queue."""
    def __init__(self, source, paths, epoch, profile=None, *, on_source=None,
                 on_error=None, on_state=None,preserve_order=False):
        super().__init__(paths, epoch, on_error, on_state)
        self.profile = profile or BaselineProfile()
        self.source = source
        self.preserve_order=preserve_order
        if self.profile.fps > source.fps:
            raise ValueError("Requested FPS exceeds source cadence")
        self.on_source = on_source
        self.stats = dict(captured=0, encoded=0, transport_queue_rejections=0,
                          keyframe_requests=0, metadata_misses=0)
        self._capture_by_pts = {}
        self._pending = deque()
        self._pts_offset = None
        self._last_keyframe_request = 0.
        self._encoder = None
        self._metadata_lock = threading.Lock()
        self._build()
        for path in self.paths:
            path.on_keyframe_request = self.request_keyframe

    def _build(self):
        Gst = gst_modules()
        self.pipeline = Gst.Pipeline.new("shared-video-sender")
        _, capture = self.source.build_elements(self.pipeline)
        p = self.profile
        # Equivalent source pixel format to the existing native SRT benchmark.
        capture.set_property("caps", Gst.Caps.from_string(
            f"video/x-raw,format=I420,width={self.source.width},height={self.source.height},"
            f"framerate={self.source.fps}/1"))
        capture.get_static_pad("src").add_probe(Gst.PadProbeType.BUFFER, self._capture)
        queue_mode="no" if self.preserve_order else "downstream"
        branch = Gst.parse_bin_from_description(
            f"queue max-size-buffers=2 max-size-bytes=0 max-size-time=0 leaky={queue_mode} ! "
            "videorate skip-to-first=true ! videoscale ! "
            f"video/x-raw,format=I420,width={p.width},height={p.height},framerate={p.fps}/1 ! "
            "identity name=shared-encoder-input ! "
            f"x264enc name=shared-encoder bitrate={p.bitrate_kbps} speed-preset=ultrafast "
            f"tune=zerolatency key-int-max={p.fps} bframes=0 ! "
            "video/x-h264,stream-format=byte-stream,alignment=au ! "
            "appsink name=shared-encoded emit-signals=true sync=false max-buffers=2 drop=false", True)
        self.pipeline.add(branch)
        if not capture.link(branch):
            raise RuntimeError("Cannot attach shared encoder")
        self._encoder = branch.get_by_name("shared-encoder")
        branch.get_by_name("shared-encoder-input").get_static_pad("src").add_probe(
            Gst.PadProbeType.BUFFER, self._encoder_input)
        branch.get_by_name("shared-encoded").connect("new-sample", self._encoded)

    def _capture(self, pad, info):
        Gst = gst_modules()
        buffer = info.get_buffer()
        if buffer is not None:
            self.stats["captured"] += 1
            fid = self.stats["captured"]
            if hasattr(self.source, "get_frame_timestamp"):
                record = self.source.get_frame_timestamp(buffer.pts)
                if record is None:
                    self._fail(ValueError("Application capture timestamp missing"))
                    return Gst.PadProbeReturn.DROP
                source_ns, _ = record
            else:
                source_ns = time.monotonic_ns()
            with self._metadata_lock:
                self._capture_by_pts[buffer.pts] = (fid, source_ns)
                while len(self._capture_by_pts) > 256:
                    del self._capture_by_pts[next(iter(self._capture_by_pts))]
            if self.on_source:
                self.on_source(fid, source_ns)
        return Gst.PadProbeReturn.OK

    def _encoder_input(self, pad, info):
        buffer = info.get_buffer()
        if buffer is not None:
            with self._metadata_lock:
                record = self._capture_by_pts.pop(buffer.pts, None)
                if record is None or len(self._pending) >= 32:
                    self._fail(RuntimeError("Encoder metadata missing/backlogged"))
                    return gst_modules().PadProbeReturn.DROP
                self._pending.append((buffer.pts, record))
        return gst_modules().PadProbeReturn.OK

    def _encoded(self, sink):
        Gst = gst_modules()
        if self._stop.is_set():
            return Gst.FlowReturn.FLUSHING
        try:
            sample = sink.emit("pull-sample")
            buffer = sample.get_buffer()
            with self._metadata_lock:
                if not self._pending:
                    raise RuntimeError("Encoded AU has no source metadata")
                if self._pts_offset is None:
                    self._pts_offset = buffer.pts - self._pending[0][0]
                expected = buffer.pts - self._pts_offset
                while self._pending and self._pending[0][0] < expected:
                    self._pending.popleft()
                if not self._pending or self._pending[0][0] != expected:
                    self.stats["metadata_misses"] += 1
                    raise RuntimeError("Encoded AU PTS identity mismatch")
                _, (fid, source_ns) = self._pending.popleft()
            self.stats["encoded"] += 1
            unit = AccessUnit(self.stats["encoded"], fid, source_ns, self.epoch,
                              buffer.extract_dup(0, buffer.get_size()))
            packets = fragment_unit(unit)  # Build ONE fragment set, not one per path.
            accepted=False
            for path in self.paths:
                if not path.send_packets(packets):
                    self.stats["transport_queue_rejections"] += 1
                else:accepted=True
            if self.preserve_order and not accepted:
                raise RuntimeError('Ordered transport queues unavailable/full; frame not silently dropped')
            return Gst.FlowReturn.OK
        except Exception as exc:
            self._fail(exc)
            return Gst.FlowReturn.ERROR

    def request_keyframe(self):
        now = time.monotonic()
        with self._metadata_lock:
            if now - self._last_keyframe_request < .5:
                return
            self._last_keyframe_request = now
        self.stats["keyframe_requests"] += 1
        Gst = gst_modules()
        # A standard upstream force-key-unit event; never rebuild the pipeline.
        structure = Gst.Structure.new_empty("GstForceKeyUnit")
        structure.set_value("all-headers", True)
        self._encoder.get_static_pad("src").send_event(
            Gst.Event.new_custom(Gst.EventType.CUSTOM_UPSTREAM, structure))

    def counters(self):
        return dict(super().counters(), sender=dict(self.stats))


class SharedVideoReceiver(_Session):
    """Merge complete copies before ONE decoder; age-filter public output."""
    def __init__(self, paths, epoch, profile=None, *, on_frame=None,
                 on_path_unit=None, on_error=None, on_state=None, allowed_profiles=None,delay_policy=DelayPolicy.STRICT,
                 preserve_order=False):
        super().__init__(paths, epoch, on_error, on_state)
        self.profile = profile or BaselineProfile()
        self.delay_policy=DelayPolicy(delay_policy)
        self.preserve_order=preserve_order
        if preserve_order and self.delay_policy!=DelayPolicy.BEST_AVAILABLE:
            raise ValueError('Ordered output cannot use age-based discarding')
        self.ordered_finished=False
        self._ordered_last_progress=time.monotonic()
        profiles=tuple(allowed_profiles or (self.profile,))
        if any(p.fps>self.profile.fps or p.max_frame_age_ms!=self.profile.max_frame_age_ms
               or p.width>self.profile.width or p.height>self.profile.height for p in profiles):
            raise ValueError('Decoded ladder exceeds negotiated envelope')
        self._allowed_dimensions={(p.width,p.height) for p in profiles}
        self.on_frame, self.on_path_unit = on_frame, on_path_unit
        self.gate = OrderedDecodeGate(epoch) if preserve_order else DecodeGate(epoch)
        self.clock_mapper = ClockMapper(stale_after_s=3., max_uncertainty_ms=200.)
        self._decode_lock = threading.RLock()
        # A gate may release a unit that arrived earlier on another path.
        self._pending_paths = {}
        self._metadata = {}
        self._last_public_id = -1
        self.stats = dict(decoded=0, delivered=0, expired_output=0,
                          unknown_age_output=0, stale_output=0)
        Gst = gst_modules()
        p = self.profile
        fps_caps=f',framerate={p.fps}/1' if len({p.fps for p in profiles})==1 else ''
        self.pipeline = Gst.parse_launch(
            "appsrc name=shared-input is-live=true format=time block=false "
            "max-bytes=2097152 max-buffers=8 "
            f'caps="video/x-h264,stream-format=byte-stream,alignment=au{fps_caps}" ! '
            "h264parse ! avdec_h264 name=shared-decoder output-corrupt=false ! "
            "videoconvert ! video/x-raw,format=RGB ! "
            f"appsink name=shared-decoded emit-signals=true sync=false max-buffers={8 if preserve_order else 1} drop={'false' if preserve_order else 'true'}")
        self._input = self.pipeline.get_by_name("shared-input")
        self.pipeline.get_by_name("shared-decoded").connect("new-sample", self._decoded)
        for path in self.paths:
            path.on_unit = self._unit
            def sample(t1, t2, t3, t4, path_id=path.path_id):
                self.clock_mapper.add(t1, t2, t3, t4, self.epoch, path_id)
            path.on_clock = sample

    def _unit(self, path_id, unit, now_ns):
        if self.on_path_unit:
            self.on_path_unit(path_id, unit)
        with self._decode_lock:
            if unit.sequence > self.gate.last_sequence:
                self._pending_paths.setdefault(unit.sequence, path_id)
            self._submit_ready(self.gate.push(unit, now_ns))

    def _submit_ready(self, units):
        for unit in units:
            path_id = self._pending_paths.pop(unit.sequence)
            self._submit(unit, path_id)
        # Recovery may discard pending units without returning them.
        for sequence in tuple(self._pending_paths):
            if sequence not in self.gate.pending:
                del self._pending_paths[sequence]

    def _submit(self, unit, path_id):
        Gst = gst_modules()
        # Explicit queue pressure: do not silently let appsrc grow past bounds.
        if self._input.get_property("current-level-buffers") >= 8:
            if self.preserve_order:raise RuntimeError('Ordered decoder queue exceeded bound; no silent skip')
            self.gate.waiting_idr = True
            self.gate.pending.clear()
            self.gate.stats["reference_breaks"] += 1
            return
        buffer = Gst.Buffer.new_allocate(None, len(unit.payload), None)
        buffer.fill(0, unit.payload)
        buffer.pts = unit.sequence * Gst.SECOND // self.profile.fps
        buffer.dts = buffer.pts
        buffer.duration = Gst.SECOND // self.profile.fps
        self._metadata[buffer.pts] = (unit, path_id)
        if len(self._metadata) > 256:
            raise RuntimeError("Shared decoder metadata backlog")
        if self._input.emit("push-buffer", buffer) != Gst.FlowReturn.OK:
            if not self._stop.is_set():
                raise RuntimeError("Shared decoder rejected AU")

    def _tick(self):
        if (self.preserve_order and not self.ordered_finished
                and time.monotonic()-self._ordered_last_progress>=max(2.,3./self.profile.fps)):
            raise RuntimeError('Ordered video stalled; completeness not guaranteed')
        with self._decode_lock:
            self._submit_ready(self.gate.drain(time.monotonic_ns()))
            if self.gate.waiting_idr:
                for path in self.paths:
                    path.request_keyframe()

    def _decoded(self, sink):
        Gst = gst_modules()
        try:
            sample = sink.emit("pull-sample")
            buffer = sample.get_buffer()
            now = time.monotonic()
            with self._decode_lock:
                metadata = self._metadata.pop(buffer.pts, None)
                # Metadata for decoder-dropped older output is no longer needed.
                for old in [pts for pts in self._metadata if pts < buffer.pts]:
                    del self._metadata[old]
            if metadata is None:
                raise RuntimeError("Decoded frame identity missing")
            unit, path_id = metadata
            self.stats["decoded"] += 1
            age = self.clock_mapper.measure_age(unit.source_ns, now, self.epoch)
            if age.status != "MEASURED":
                self.stats["unknown_age_output"] += 1
                if not self.preserve_order:return Gst.FlowReturn.OK
            if not self.preserve_order and not accepts_age(age.age_upper_ms,self.profile.max_frame_age_ms,self.delay_policy):
                self.stats["expired_output"] += 1
                return Gst.FlowReturn.OK
            if unit.frame_id <= self._last_public_id:
                if self.preserve_order:raise RuntimeError('Ordered decoder repeated/regressed frame ID')
                self.stats["stale_output"] += 1
                return Gst.FlowReturn.OK
            if self.preserve_order and unit.frame_id!=(1 if self._last_public_id<0 else self._last_public_id+1):
                raise RuntimeError('Ordered decoded frame missing; no silent skip')
            caps = sample.get_caps().get_structure(0)
            width, height = caps.get_value("width"), caps.get_value("height")
            if (width, height) not in self._allowed_dimensions:
                raise RuntimeError("Unexpected shared decoded resolution")
            data = buffer.extract_dup(0, buffer.get_size())
            if len(data) != width * height * 3:
                import gi
                gi.require_version('GstVideo','1.0')
                from gi.repository import GstVideo
                meta=GstVideo.buffer_get_video_meta(buffer)
                info=GstVideo.VideoInfo.new_from_caps(sample.get_caps())
                data=pack_rgb_rows(data,width,height,
                    meta.stride[0] if meta else info.stride[0],
                    meta.offset[0] if meta else info.offset[0])
            self._last_public_id = unit.frame_id
            if self.preserve_order:self._ordered_last_progress=time.monotonic()
            self.stats["delivered"] += 1
            if self.on_frame:
                self.on_frame(LiveFrame(path_id, unit.frame_id, width, height,
                    PixelFormat.RGB, width * 3, data, now, session_id=self.epoch,
                    source_timestamp_ns=unit.source_ns, sender_clock_epoch=self.epoch,
                    timestamp_origin="source_ingress", timing=FrameTiming(
                        age.status, "source_ingress", unit.source_ns, self.epoch,
                        age.age_upper_ms, age.age_lower_ms, age.uncertainty_ms, now)))
            return Gst.FlowReturn.OK
        except Exception as exc:
            self._fail(exc)
            return Gst.FlowReturn.ERROR

    def counters(self):
        with self._decode_lock:
            return dict(super().counters(), receiver=dict(self.stats), gate=dict(self.gate.stats))
