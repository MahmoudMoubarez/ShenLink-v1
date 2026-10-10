"""Fixed baseline on every path plus fixed high quality on one chosen path.

One source, two encoders, two independent reference-safe decoders. No automatic
path selection or gradual quality changes. Transport/security stays in adapters.
"""
from collections import deque
from dataclasses import dataclass, field, replace
import math
import threading
import time

from .core import AccessUnit, fragment_unit
from .session import BaselineProfile, SharedVideoReceiver, _Session, gst_modules
from .negotiation import ReceiverRequirements, profile_from_wire
from dataclasses import asdict
from .delivery import DelayPolicy,accepts_age


@dataclass(frozen=True)
class DualQualityConfig:
    baseline: BaselineProfile = field(default_factory=lambda: BaselineProfile(640,360,30,600))
    high: BaselineProfile = field(default_factory=BaselineProfile)
    high_path: str = 'a'
    high_grace_ms: float = 75.
    requirements: ReceiverRequirements = field(default_factory=lambda: ReceiverRequirements(min_fps=30))
    allow_equal_resolution: bool = False
    allow_variable_fps: bool = False

    def __post_init__(self):
        for profile in (self.baseline,self.high):
            profile_from_wire(asdict(profile))
            if not self.requirements.accepts(profile):
                raise ValueError('Both fixed profiles must satisfy receiver requirements')
        if self.baseline.fps != self.high.fps and not self.allow_variable_fps:
            raise ValueError('This fixed two-quality phase requires equal FPS')
        if (self.high.width < self.baseline.width or self.high.height < self.baseline.height
                or self.high.bitrate_kbps < self.baseline.bitrate_kbps
                or (not self.allow_equal_resolution and (self.high.width,self.high.height)==(self.baseline.width,self.baseline.height))):
            raise ValueError('High profile must have strictly higher resolution and no lower bitrate')
        if (not self.high_path or not math.isfinite(self.high_grace_ms)
                or not 1000/self.baseline.fps < self.high_grace_ms <= self.requirements.max_frame_age_ms):
            raise ValueError('High grace must exceed one frame interval and fit the age budget')


class QualitySelector:
    """Publish the newest decoded capture immediately; never wait for quality.

    Each tier retains only its newest frame. Prefer high for an equal capture
    when both are already cached. A higher-resolution copy may upgrade the
    current capture, explicitly marked quality_upgrade; never rewind captures.
    """
    def __init__(self, config, on_frame, on_selection=None, *, high_profiles=None,
                 delay_policy=DelayPolicy.STRICT,on_delivery_status=None):
        self.config,self.on_frame,self.on_selection=config,on_frame,on_selection
        self.delay_policy=DelayPolicy(delay_policy);self.on_delivery_status=on_delivery_status
        profiles=tuple(high_profiles or (config.high,))
        if any(not config.requirements.accepts(p) for p in profiles):
            raise ValueError('High ladder outside receiver bounds')
        self._high_dimensions={(p.width,p.height) for p in profiles}
        self._lock=threading.RLock()
        self._baseline=self._high=None
        self._last_id=-1
        self._last_source=-1
        self._last_frame=None
        self.selected=None
        self.stats=dict(baseline_deliveries=0,high_deliveries=0,selection_changes=0,
                        stale_rejected=0,expired_rejected=0,quality_upgrades=0)

    def _current(self, frame, now):
        if frame is None or frame.timing is None or frame.timing.status!='MEASURED':return None
        if frame.frame_id is None or frame.source_timestamp_ns is None:return None
        elapsed=(now-frame.timing.measured_at_monotonic_s)*1000
        if elapsed < 0 or frame.timing.age_upper_ms is None:return None
        if not accepts_age(frame.timing.age_upper_ms+elapsed,self.config.requirements.max_frame_age_ms,self.delay_policy):return None
        return replace(frame,arrival_time=now,timing=replace(frame.timing,
            age_upper_ms=frame.timing.age_upper_ms+elapsed,
            age_lower_ms=None if frame.timing.age_lower_ms is None else frame.timing.age_lower_ms+elapsed,
            measured_at_monotonic_s=now))

    def _publish(self, tier, frame, now):
        current=self._current(frame,now)
        if current is None:
            self.stats['expired_rejected']+=1;return
        upgrade=self._is_upgrade(tier,current)
        if not upgrade and (current.frame_id<=self._last_id or current.source_timestamp_ns<=self._last_source):
            self.stats['stale_rejected']+=1;return
        current=replace(current,delivery_kind='quality_upgrade' if upgrade else 'new_frame')
        self._last_id,self._last_source=current.frame_id,current.source_timestamp_ns
        self._last_frame=current
        if upgrade:self.stats['quality_upgrades']+=1
        if self.selected!=tier:
            previous=self.selected;self.selected=tier;self.stats['selection_changes']+=1
            if self.on_selection:self.on_selection(previous,tier,now)
        self.stats[tier+'_deliveries']+=1
        if self.on_delivery_status:self.on_delivery_status(dict(path=current.path_id,tier=tier,
            frame_id=current.frame_id,age_upper_ms=current.timing.age_upper_ms,
            max_frame_age_ms=self.config.requirements.max_frame_age_ms,
            within_delay_budget=current.timing.age_upper_ms<=self.config.requirements.max_frame_age_ms,
            delay_policy=self.delay_policy.value,delivery_kind=current.delivery_kind))
        if self.on_frame:self.on_frame(tier,current)

    def _is_upgrade(self,tier,frame):
        previous=self._last_frame
        def capture_epoch(value):
            if value is None:return None
            return value.removesuffix('-base').removesuffix('-high')
        return (previous is not None and tier=='high'
                and frame.frame_id==self._last_id
                and frame.source_timestamp_ns==self._last_source
                and capture_epoch(frame.session_id)==capture_epoch(previous.session_id)
                and capture_epoch(frame.sender_clock_epoch)==capture_epoch(previous.sender_clock_epoch)
                and frame.width>=previous.width and frame.height>=previous.height
                and frame.width*frame.height>previous.width*previous.height)

    def _publish_newest(self, now):
        available=[(tier,current) for tier,frame in
                   (('baseline',self._baseline),('high',self._high))
                   if (current:=self._current(frame,now)) is not None]
        if not available:return
        tier,current=max(available,key=lambda item:(item[1].frame_id,item[0]=='high'))
        if current.frame_id>self._last_id or self._is_upgrade(tier,current):self._publish(tier,current,now)

    def push(self, tier, frame, now=None):
        if tier not in ('baseline','high'):raise ValueError('Unknown quality tier')
        now=time.monotonic() if now is None else now
        expected=self.config.baseline if tier=='baseline' else self.config.high
        dimensions=self._high_dimensions if tier=='high' else {(expected.width,expected.height)}
        if (frame.width,frame.height) not in dimensions:
            raise ValueError('Decoded tier violates negotiated dimensions')
        with self._lock:
            if self._current(frame,now) is None:
                self.stats['expired_rejected']+=1;return
            cached=self._baseline if tier=='baseline' else self._high
            if cached is not None and frame.frame_id<=cached.frame_id:
                self.stats['stale_rejected']+=1;return
            if tier=='high':
                self._high=frame
            else:
                self._baseline=frame
            self._publish_newest(now)

    def tick(self, now=None):
        now=time.monotonic() if now is None else now
        with self._lock:
            self._publish_newest(now)


class DualQualitySender(_Session):
    """One raw capture tee feeds exactly two independent H264 encoder branches."""
    def __init__(self, source, baseline_paths, high_paths, epoch, config=None, *,
                 on_source=None,on_capture=None,on_error=None,on_state=None):
        self.config=config or DualQualityConfig()
        if any(p.width>source.width or p.height>source.height
               for p in (self.config.baseline,self.config.high)):
            raise ValueError('Video profile exceeds source resolution; upscaling is not quality improvement')
        self.base_paths,self.high_paths=list(baseline_paths),list(high_paths)
        self.epoch_base,self.epoch_high=epoch+'-base',epoch+'-high'
        if (not self.base_paths or len({p.path_id for p in self.base_paths})!=len(self.base_paths)
                or len(self.high_paths)!=1 or self.high_paths[0].path_id!=self.config.high_path
                or self.config.high_path not in {p.path_id for p in self.base_paths}
                or any(p.epoch!=self.epoch_base for p in self.base_paths)
                or self.high_paths[0].epoch!=self.epoch_high):
            raise ValueError('Baseline path set and selected high path/epochs mismatch')
        # Parent validates baseline identities; add the distinct high channel.
        super().__init__(self.base_paths,self.epoch_base,on_error,on_state)
        self.paths=self.base_paths+self.high_paths
        self.high_paths[0].on_error=self._fail
        self.high_paths[0].on_state=on_state
        self.source=source
        if (source.fps<self.config.high.fps or (not self.config.allow_variable_fps
                and source.fps!=self.config.baseline.fps)):
            raise ValueError('Fixed two-quality source cadence must equal negotiated FPS')
        if hasattr(source,'get_frame_timestamp'):
            raise ValueError('Application-pushed capture is not implemented in this fixed two-quality phase')
        self.on_source,self.on_capture=on_source,on_capture
        self._lock=threading.RLock()
        self._records={}
        self._branches={}
        self.stats=dict(captured=0)
        self._build()

    def _build(self):
        Gst=gst_modules();self.pipeline=Gst.Pipeline.new('fixed-dual-sender')
        _,capture=self.source.build_elements(self.pipeline)
        capture.set_property('caps',Gst.Caps.from_string(
            f'video/x-raw,format=RGB,width={self.source.width},height={self.source.height},framerate={self.source.fps}/1'))
        capture.get_static_pad('src').add_probe(Gst.PadProbeType.BUFFER,self._capture)
        tee=Gst.ElementFactory.make('tee','common-source');self.pipeline.add(tee)
        if not capture.link(tee):raise RuntimeError('Could not attach common capture tee')
        for tier,profile,paths,epoch in [('baseline',self.config.baseline,self.base_paths,self.epoch_base),
                                         ('high',self.config.high,self.high_paths,self.epoch_high)]:
            branch=Gst.parse_bin_from_description(
                'queue max-size-buffers=2 max-size-bytes=0 max-size-time=0 leaky=downstream ! '
                + (f'videorate name=rate-{tier} drop-only=true skip-to-first=true ! ' if self.config.allow_variable_fps else '') +
                f'videoscale ! videoconvert ! capsfilter name=quality-{tier} caps="video/x-raw,format=I420,width={profile.width},height={profile.height},framerate={profile.fps}/1" ! '
                f'identity name=input-{tier} ! x264enc name=encoder-{tier} bitrate={profile.bitrate_kbps} '
                f'speed-preset=ultrafast tune=zerolatency key-int-max={profile.fps} bframes=0 ! '
                'video/x-h264,stream-format=byte-stream,alignment=au ! '
                f'appsink name=encoded-{tier} emit-signals=true sync=false max-buffers=2 drop=false',True)
            self.pipeline.add(branch)
            if not tee.link(branch):raise RuntimeError('Cannot attach encoder branch '+tier)
            record=dict(profile=profile,paths=paths,epoch=epoch,pending=deque(),offset=None,
                        encoder=branch.get_by_name('encoder-'+tier),caps=branch.get_by_name('quality-'+tier),
                        encoded=0,last_idr=0.,queue_rejections=0)
            self._branches[tier]=record
            branch.get_by_name('input-'+tier).get_static_pad('src').add_probe(
                Gst.PadProbeType.BUFFER,lambda pad,info,tier=tier:self._input(pad,info,tier))
            branch.get_by_name('encoded-'+tier).connect('new-sample',lambda sink,tier=tier:self._encoded(sink,tier))
            for path in paths:path.on_keyframe_request=lambda tier=tier:self._keyframe(tier)

    def _capture(self,pad,info):
        Gst=gst_modules();buffer=info.get_buffer()
        try:
            if buffer is not None:
                self.stats['captured']+=1;fid=self.stats['captured'];ns=time.monotonic_ns()
                with self._lock:
                    self._records[buffer.pts]=(fid,ns)
                    while len(self._records)>256:del self._records[next(iter(self._records))]
                if self.on_source:self.on_source(fid,ns)
                if self.on_capture:self.on_capture(fid,buffer,self.source.width,self.source.height)
        except Exception as exc:self._fail(exc);return Gst.PadProbeReturn.DROP
        return Gst.PadProbeReturn.OK

    def _input(self,pad,info,tier):
        Gst=gst_modules();buffer=info.get_buffer()
        if buffer is not None:
            with self._lock:
                record=self._records.get(buffer.pts);pending=self._branches[tier]['pending']
                if record is None or len(pending)>=32:
                    self._fail(RuntimeError('Dual encoder capture identity missing/backlogged'));return Gst.PadProbeReturn.DROP
                pending.append((buffer.pts,record))
        return Gst.PadProbeReturn.OK

    def _encoded(self,sink,tier):
        Gst=gst_modules()
        if self._stop.is_set():return Gst.FlowReturn.FLUSHING
        try:
            buffer=sink.emit('pull-sample').get_buffer()
            with self._lock:
                record=self._branches[tier];pending=record['pending']
                if not pending:raise RuntimeError('Encoded tier has no capture metadata')
                if record['offset'] is None:record['offset']=buffer.pts-pending[0][0]
                expected=buffer.pts-record['offset']
                while pending and pending[0][0]<expected:pending.popleft()
                if not pending or pending[0][0]!=expected:raise RuntimeError('Encoded tier identity mismatch')
                _,(fid,ns)=pending.popleft();record['encoded']+=1
                unit=AccessUnit(record['encoded'],fid,ns,record['epoch'],buffer.extract_dup(0,buffer.get_size()))
            packets=fragment_unit(unit)
            for path in record['paths']:
                if not path.send_packets(packets):record['queue_rejections']+=1
            return Gst.FlowReturn.OK
        except Exception as exc:self._fail(exc);return Gst.FlowReturn.ERROR

    def _keyframe(self,tier):
        with self._lock:
            record=self._branches[tier];now=time.monotonic()
            if now-record['last_idr']<.5:return
            record['last_idr']=now
        Gst=gst_modules();structure=Gst.Structure.new_empty('GstForceKeyUnit');structure.set_value('all-headers',True)
        record['encoder'].get_static_pad('src').send_event(Gst.Event.new_custom(Gst.EventType.CUSTOM_UPSTREAM,structure))

    def counters(self):
        result=super().counters()
        # Do not collide baseline A with the separate high-A channel.
        result['paths']={('baseline/' if p in self.base_paths else 'high/')+p.path_id:
                         dict(p.stats,state=p.state) for p in self.paths}
        result['capture']=dict(self.stats)
        result['tiers']={tier:dict(encoded=r['encoded'],queue_rejections=r['queue_rejections']) for tier,r in self._branches.items()}
        return result


class DualQualityReceiver:
    def __init__(self,baseline_paths,high_paths,epoch,config=None,*,on_frame=None,on_selection=None,
                 on_path_unit=None,on_error=None,on_state=None):
        self.config=config or DualQualityConfig()
        if len(high_paths)!=1 or high_paths[0].path_id!=self.config.high_path:
            raise ValueError('Exactly one configured high path required')
        if self.config.high_path not in {p.path_id for p in baseline_paths}:
            raise ValueError('High path must be one of the baseline routes')
        self.selector=QualitySelector(self.config,on_frame,on_selection)
        self._stop=threading.Event();self._thread=None;self._started=False
        self.on_error=on_error
        self.receivers={}
        for tier,paths,profile,suffix in [('baseline',baseline_paths,self.config.baseline,'-base'),
                                          ('high',high_paths,self.config.high,'-high')]:
            self.receivers[tier]=SharedVideoReceiver(paths,epoch+suffix,profile,
                on_frame=lambda frame,tier=tier:self.selector.push(tier,frame),
                on_path_unit=None if on_path_unit is None else lambda p,u,tier=tier:on_path_unit(tier,p,u),
                on_error=self._fail,on_state=on_state)

    def _fail(self,exc):
        self._stop.set()
        if self.on_error:self.on_error(exc)

    def _tick(self):
        try:
            while not self._stop.wait(.01):self.selector.tick()
        except Exception as exc:self._fail(exc)

    def start(self):
        if self._started:raise RuntimeError('Dual receiver is single-use')
        self._started=True
        try:
            for receiver in self.receivers.values():receiver.start()
            self._thread=threading.Thread(target=self._tick,name='quality-selection',daemon=True);self._thread.start()
        except Exception:self.stop();raise

    def stop(self):
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():self._thread.join(timeout=2)
        try:self.receivers['high'].stop()
        finally:self.receivers['baseline'].stop()

    def counters(self):
        tiers={tier:r.counters() for tier,r in self.receivers.items()}
        return dict(codecs={key:sum(r['codecs'][key] for r in tiers.values()) for key in ('encoders','decoders')},
                    tiers=tiers,selection=dict(self.selector.stats))
