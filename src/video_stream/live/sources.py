"""Video sources for LiveSender: hardware camera, test pattern, or application frame pushing."""

import threading
import time
from typing import Optional, Protocol, Union


class LiveVideoSource(Protocol):
    """Protocol for a video source that can be linked into a GStreamer sender pipeline."""

    width: int
    height: int
    fps: int

    def build_elements(self, pipeline: object) -> tuple[object, object]:
        """Construct source and capture caps elements in pipeline, returning (src, caps)."""
        ...


class CameraSource:
    """Hardware camera or synthetic test pattern source.

    Attributes:
        index: Windows device camera index (0, 1, ...). If None or test_pattern=True, videotestsrc is used.
        name: Optional exact Windows camera device name (overrides index).
        width: Capture frame width.
        height: Capture frame height.
        fps: Capture framerate.
        test_pattern: If True, uses videotestsrc pattern instead of hardware camera.
    """

    def __init__(
        self,
        index: Optional[int] = 0,
        name: str = "",
        width: int = 1280,
        height: int = 720,
        fps: int = 24,
        test_pattern: bool = False,
    ) -> None:
        if width <= 0 or height <= 0 or fps <= 0:
            raise ValueError("width, height, and fps must be positive")
        self.index = index
        self.name = name
        self.width = width
        self.height = height
        self.fps = fps
        self.test_pattern = test_pattern or (index is None and not name)

    def build_elements(self, pipeline: object) -> tuple[object, object]:
        import gi
        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        if self.test_pattern:
            source = Gst.ElementFactory.make("videotestsrc", "live-camera-src")
            source.set_property("is-live", True)
            source.set_property("pattern", 18)  # moving ball pattern
        else:
            # Prefer Windows Media Foundation source, fallback to autovideosrc
            source = Gst.ElementFactory.make("mfvideosrc", "live-camera-src")
            if source is None:
                source = Gst.ElementFactory.make("ksvideosrc", "live-camera-src")
            if source is None:
                source = Gst.ElementFactory.make("autovideosrc", "live-camera-src")

            if self.name and source.find_property("device-name"):
                source.set_property("device-name", self.name)
            elif self.index is not None and source.find_property("device-index"):
                source.set_property("device-index", self.index)

        pipeline.add(source)

        convert = Gst.ElementFactory.make("videoconvert", "live-cam-convert")
        rate = Gst.ElementFactory.make("videorate", "live-cam-rate")
        scale = Gst.ElementFactory.make("videoscale", "live-cam-scale")
        caps = Gst.ElementFactory.make("capsfilter", "live-camera-caps")
        caps_str = f"video/x-raw,width={self.width},height={self.height},framerate={self.fps}/1"
        caps.set_property("caps", Gst.Caps.from_string(caps_str))

        for comp in (convert, rate, scale, caps):
            pipeline.add(comp)

        if not (source.link(convert) and convert.link(rate) and rate.link(scale) and scale.link(caps)):
            raise RuntimeError(f"Could not link camera source to caps: {caps_str}")

        return source, caps


class FramePushSource:
    """Application-driven raw frame ingest source.

    Allows an external application (e.g. OpenCV, PyTorch, Pygame, simulation engine)
    to push raw RGB or BGR frames into the video streaming library.

    Frames are buffered through a bounded, downstream-leaky queue to drop stale frames
    and prevent latency accumulation if transport or encoding stalls.
    """

    def __init__(
        self,
        width: int = 1280,
        height: int = 720,
        fps: int = 24,
        pixel_format: str = "RGB",
    ) -> None:
        if width <= 0 or height <= 0 or fps <= 0:
            raise ValueError("width, height, and fps must be positive")
        fmt = pixel_format.upper()
        if fmt not in ("RGB", "BGR", "RGBA", "BGRA"):
            raise ValueError(f"Unsupported pixel_format: {pixel_format}")
        self.width = width
        self.height = height
        self.fps = fps
        self.pixel_format = fmt

        self._appsrc = None
        self._lock = threading.Lock()
        # PTS belongs to the media running-time domain, not the host's absolute
        # monotonic clock. Keep capture time separately for frame-age metrics.
        self._pts_counter = 0
        self._frame_index = 0
        self._timestamp_records: dict[int, tuple[int, str]] = {}
        self.timestamp_match_miss = 0

    def build_elements(self, pipeline: object) -> tuple[object, object]:
        import gi
        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        appsrc = Gst.ElementFactory.make("appsrc", "live-push-src")
        appsrc.set_property("is-live", True)
        appsrc.set_property("format", Gst.Format.TIME)
        appsrc.set_property("block", False)

        caps_str = (
            f"video/x-raw,format={self.pixel_format},width={self.width},"
            f"height={self.height},framerate={self.fps}/1"
        )
        appsrc.set_property("caps", Gst.Caps.from_string(caps_str))
        pipeline.add(appsrc)

        # Bounded leaky queue: drop oldest frames if encoding/pipeline is slower than push rate
        queue = Gst.ElementFactory.make("queue", "live-push-queue")
        queue.set_property("max-size-buffers", 2)
        queue.set_property("max-size-time", 0)
        queue.set_property("max-size-bytes", 0)
        # downstream leaky: drop oldest buffer when full
        queue.set_property("leaky", 2)
        pipeline.add(queue)

        convert = Gst.ElementFactory.make("videoconvert", "live-push-convert")
        pipeline.add(convert)

        caps = Gst.ElementFactory.make("capsfilter", "live-push-caps")
        caps.set_property("caps", Gst.Caps.from_string(
            f"video/x-raw,width={self.width},height={self.height},framerate={self.fps}/1"
        ))
        pipeline.add(caps)

        if not (appsrc.link(queue) and queue.link(convert) and convert.link(caps)):
            raise RuntimeError("Could not link FramePushSource elements in pipeline")

        with self._lock:
            self._appsrc = appsrc

        return appsrc, caps

    def push(
        self,
        data: Union[bytes, bytearray, memoryview],
        width: Optional[int] = None,
        height: Optional[int] = None,
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Push an uncompressed video frame into the streaming pipeline.

        Args:
            data: Raw frame pixels (matching pixel_format). Copied into GStreamer buffer safely.
            width: Frame width (must match or default to configured width).
            height: Frame height (must match or default to configured height).
            timestamp_ns: Optional monotonic capture timestamp in nanoseconds. Must be non-negative
                          and recorded from time.monotonic_ns() on the sender machine.

        Returns:
            True if buffer was accepted by appsrc, False if pipeline is stopped.
        """
        import gi
        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        with self._lock:
            appsrc = self._appsrc
            if appsrc is None:
                return False

        if timestamp_ns is not None:
            if not isinstance(timestamp_ns, int) or timestamp_ns < 0:
                raise ValueError(
                    f"timestamp_ns must be a non-negative integer, got {timestamp_ns}"
                )
            cur_mono = time.monotonic_ns()
            if timestamp_ns > cur_mono + 2_000_000_000:
                raise ValueError(
                    f"timestamp_ns {timestamp_ns} is in the future relative to monotonic clock ({cur_mono})"
                )
            declared_ts = timestamp_ns
            origin = "sensor_capture"
        else:
            declared_ts = time.monotonic_ns()
            origin = "source_ingress"

        w = width or self.width
        h = height or self.height
        if w != self.width or h != self.height:
            raise ValueError(f"Frame dimensions ({w}x{h}) do not match source ({self.width}x{self.height})")

        with self._lock:
            pts = self._frame_index * 1_000_000_000 // self.fps
            next_pts = (self._frame_index + 1) * 1_000_000_000 // self.fps
            self._frame_index += 1
            self._pts_counter = pts
            self._timestamp_records[pts] = (declared_ts, origin)
            if len(self._timestamp_records) > 256:
                while len(self._timestamp_records) > 128:
                    self._timestamp_records.pop(next(iter(self._timestamp_records)))
                    self.timestamp_match_miss += 1

        raw_bytes = bytes(data)
        buf = Gst.Buffer.new_allocate(None, len(raw_bytes), None)
        buf.fill(0, raw_bytes)

        buf.pts = pts
        buf.dts = pts
        buf.duration = next_pts - pts

        flow = appsrc.emit("push-buffer", buf)
        if flow != Gst.FlowReturn.OK:
            with self._lock:
                self._timestamp_records.pop(pts, None)
            return False
        return True

    def get_frame_timestamp(self, pts: int) -> tuple[int, str] | None:
        """Retrieve and remove the (declared_timestamp_ns, origin) for an exact buffer PTS."""
        with self._lock:
            if pts in self._timestamp_records:
                record = self._timestamp_records.pop(pts)
                # Cleanup any uncollected records older than this PTS (dropped upstream in leaky queue)
                stale_keys = [k for k in self._timestamp_records if k < pts]
                for k in stale_keys:
                    self._timestamp_records.pop(k, None)
                    self.timestamp_match_miss += 1
                return record
            self.timestamp_match_miss += 1
            return None

    def push_frame(
        self,
        data: Union[bytes, bytearray, memoryview],
        width: Optional[int] = None,
        height: Optional[int] = None,
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Convenience alias for push()."""
        return self.push(data, width=width, height=height, timestamp_ns=timestamp_ns)
