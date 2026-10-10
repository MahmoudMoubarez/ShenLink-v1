"""Receiver-feedback driven two-encoder video. Trusted adapter control plane.

One bounded baseline on every path and ONE enhancement encoder/decoder, with a
developer-bounded quality ladder. No per-path encoder or bandwidth oracle.
Feedback is bounded best-effort control; silence never permits blind upgrades.
"""
from collections import deque
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import threading
import time

from .dual import DualQualityConfig, DualQualitySender, QualitySelector
from .session import BaselineProfile, SharedVideoReceiver
from .negotiation import ReceiverRequirements, profile_from_wire
from .delivery import DelayPolicy
from .ladder import quality_ladder
from .ordered import DeliveryMode
from .fps import AdaptationPriority, adaptation_ladder


@dataclass(frozen=True)
class AdaptiveConfig:
    requirements: ReceiverRequirements = field(default_factory=lambda: ReceiverRequirements(
        min_fps=30,max_frame_age_ms=150.,min_bitrate_kbps=600))
    levels: tuple | None = None
    quality_steps: int = 3
    dynamic_fps: bool = False
    fps_steps: int = 3
    adaptation_priority: AdaptationPriority = AdaptationPriority.FRAME_RATE_FIRST
    delay_policy: DelayPolicy = DelayPolicy.BEST_AVAILABLE
    delivery_mode: DeliveryMode = DeliveryMode.LATEST
    feedback_interval_s: float = .2
    stale_s: float = .6
    stable_s: float = 1.
    cooldown_s: float = 1.
    severe_age_multiplier: float = 2.
    severe_reaction_s: float = .2
    retry_delay_s: float = 2.
    max_retry_delay_s: float = 16.
    high_grace_ms: float = 75.

    def __post_init__(self):
        object.__setattr__(self,'delay_policy',DelayPolicy(self.delay_policy))
        object.__setattr__(self,'delivery_mode',DeliveryMode(self.delivery_mode))
        object.__setattr__(self,'adaptation_priority',AdaptationPriority(self.adaptation_priority))
        if type(self.dynamic_fps) is not bool:raise ValueError('dynamic_fps must be boolean')
        if type(self.fps_steps) is not int or not 1<=self.fps_steps<=8:raise ValueError('Invalid FPS steps')
        if self.dynamic_fps and self.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            raise ValueError('Dynamic FPS is latest-frame only')
        object.__setattr__(self,'levels',tuple(self.levels) if self.levels is not None else
            (adaptation_ladder(self.requirements,self.quality_steps,self.fps_steps,self.adaptation_priority)
             if self.dynamic_fps else quality_ladder(self.requirements,self.quality_steps)))
        for p in self.levels:
            profile_from_wire(asdict(p))
        if not 1<=len(self.levels)<=(15 if self.dynamic_fps else 8) or any(not self.requirements.accepts(p) for p in self.levels):
            raise ValueError('Ladder outside receiver bounds')
        if not self.dynamic_fps and len({p.fps for p in self.levels})!=1:raise ValueError('Ladder must use one fixed FPS')
        if any(b.bitrate_kbps<a.bitrate_kbps or b.width<a.width or b.height<a.height or b.fps<a.fps or b==a
               for a,b in zip(self.levels,self.levels[1:])):raise ValueError('Ladder must increase monotonically')
        if self.dynamic_fps and any(a.fps!=b.fps and
                (a.width,a.height,a.bitrate_kbps)!=(b.width,b.height,b.bitrate_kbps)
                for a,b in zip(self.levels,self.levels[1:])):
            raise ValueError('Change FPS or quality, not both in one step')
        if any(not math.isfinite(x) or x<=0 for x in (self.feedback_interval_s,self.stale_s,self.stable_s,self.cooldown_s,self.retry_delay_s,self.max_retry_delay_s,self.severe_reaction_s)):
            raise ValueError('Invalid adaptation timers')
        if not math.isfinite(self.severe_age_multiplier) or self.severe_age_multiplier<=1:
            raise ValueError('Severe delay must exceed the receiver budget')
        if not self.feedback_interval_s<=self.severe_reaction_s<=self.stale_s:
            raise ValueError('Severe reaction must span feedback and not exceed normal reaction')
        if self.retry_delay_s>self.max_retry_delay_s:raise ValueError('Invalid retry delay bounds')
        if self.feedback_interval_s>=self.stale_s:raise ValueError('Feedback interval must precede expiry')

    def dual(self,path):
        return DualQualityConfig(self.levels[0],self.levels[-1],path,self.high_grace_ms,self.requirements,
                                 allow_equal_resolution=True,allow_variable_fps=self.dynamic_fps)

    def contract_id(self):
        """Bind all ladder levels, delay policy and controller settings."""
        return hashlib.sha256(json.dumps(asdict(self),sort_keys=True,
            separators=(',',':')).encode()).hexdigest()


def validate_feedback(value, paths):
    if not isinstance(value,dict) or set(value)!= {'version','sequence','paths','high'}:
        raise ValueError('Invalid feedback schema')
    if type(value['version']) is not int or value['version']!=1 or type(value['sequence']) is not int or not 0<=value['sequence']<2**63:
        raise ValueError('Invalid feedback sequence')
    if not isinstance(value['paths'],dict) or set(value['paths'])!=set(paths):raise ValueError('Feedback path mismatch')
    def sample(row, high=False):
        fields={'fresh','age_ms','fps'} if high else {'fresh','age_ms'}
        if not isinstance(row,dict) or set(row)!=fields or type(row['fresh']) is not bool:
            raise ValueError('Invalid feedback sample')
        age=row['age_ms']
        if age is not None and (type(age) not in (int,float) or not math.isfinite(age) or not 0<=age<=10000):
            raise ValueError('Invalid feedback age')
        if row['fresh'] and age is None:raise ValueError('Fresh feedback needs age')
        if high and (type(row['fps']) not in (int,float) or not math.isfinite(row['fps']) or not 0<=row['fps']<=120):
            raise ValueError('Invalid feedback cadence')
    for row in value['paths'].values():sample(row)
    if not isinstance(value['high'],dict) or set(value['high'])!=set(paths):raise ValueError('High feedback path mismatch')
    for row in value['high'].values():sample(row,True)
    return value


class AdaptivePolicy:
    """Small hysteretic policy; observations are measured by the receiver."""
    def __init__(self,paths,config=None):
        self.config=config or AdaptiveConfig();self.paths=tuple(paths)
        if not self.paths or len(set(self.paths))!=len(self.paths):raise ValueError('Unique paths required')
        self.path=self.paths[0];self.level=0;self.feedback=None
        self.last_seq=-1;self.received=-math.inf;self.changed=-math.inf
        self.good_since=self.bad_since=self.better_since=None
        self.severe_since=self.severe_first_seq=None
        self.proven_levels={p:0 for p in self.paths}
        self.failed_levels={}

    def observe(self,value,now):
        validate_feedback(value,self.paths)
        if value['sequence']<=self.last_seq:return False
        self.last_seq=value['sequence'];self.feedback=value;self.received=now
        return True

    def _decision(self,path,level,reason,now):
        if (path,level)==(self.path,self.level):return None
        self.path,self.level=path,level;self.changed=now
        self.good_since=self.bad_since=self.better_since=None
        self.severe_since=self.severe_first_seq=None
        return dict(path=path,level=level,reason=reason,at=now)

    def tick(self,now):
        c=self.config
        if self.feedback is None or now-self.received>c.stale_s:
            return self._decision(self.path,0,'feedback_expired',now)
        # Recent but late media proves congestion, not a disconnected route.
        alive={p:r for p,r in self.feedback['paths'].items() if r['fresh']}
        viable={p:r for p,r in alive.items() if r['age_ms']<c.requirements.max_frame_age_ms*.8}
        if self.path not in alive:
            choices=viable or (alive if c.delay_policy==DelayPolicy.BEST_AVAILABLE else {})
            if not choices:return self._decision(self.path,0,'no_fresh_path',now)
            best=min(choices,key=lambda p:(choices[p]['age_ms'],p))
            return self._decision(best,0,'primary_unavailable',now)
        best=min(viable,key=lambda p:(-self.proven_levels[p],viable[p]['age_ms'],p)) if viable else self.path
        h=self.feedback['high'][self.path]
        congested=alive[self.path]['age_ms']>=c.requirements.max_frame_age_ms*.8
        fps=c.levels[self.level].fps
        good=not congested and h['fresh'] and h['age_ms']<c.requirements.max_frame_age_ms*.7 and h['fps']>=fps*.9
        bad=congested or not h['fresh'] or h['age_ms']>c.requirements.max_frame_age_ms*.9 or h['fps']<fps*.8
        severe=alive[self.path]['age_ms']>=c.requirements.max_frame_age_ms*c.severe_age_multiplier
        if severe:
            if self.severe_since is None:
                self.severe_since=now;self.severe_first_seq=self.last_seq
        else:self.severe_since=self.severe_first_seq=None
        # Only severe, sustained delay bypasses the normal post-change cooldown.
        # A second feedback report is required after EACH step; never react to
        # repeatedly ticking the same observation. Upgrade timing is unchanged.
        if now-self.changed<c.cooldown_s and not severe:return None
        if good:
            self.bad_since=None
            if self.good_since is None:self.good_since=now
            if now-self.good_since>=c.stable_s:
                self.proven_levels[self.path]=max(self.proven_levels[self.path],self.level)
                retry_at=self.failed_levels.get((self.path,self.level+1),(0.,0))[0]
                if self.level<len(c.levels)-1 and now>=retry_at:
                    return self._decision(self.path,self.level+1,'stable_upgrade',now)
        else:self.good_since=None
        if bad:
            if self.bad_since is None:self.bad_since=now
            confirmed_severe=(severe and now-self.severe_since>=c.severe_reaction_s
                              and self.last_seq>self.severe_first_seq)
            if confirmed_severe or (not severe and now-self.bad_since>=c.stale_s):
                self.proven_levels[self.path]=max(0,self.level-1)
                if self.level:
                    key=(self.path,self.level);failures=self.failed_levels.get(key,(0.,0))[1]+1
                    delay=min(c.max_retry_delay_s,c.retry_delay_s*2**min(failures-1,20))
                    self.failed_levels[key]=(now+delay,failures)
                    return self._decision(self.path,self.level-1,
                        'severe_quality_over_budget' if confirmed_severe else 'quality_over_budget',now)
                alternatives=[p for p in viable if p!=self.path]
                if not alternatives and c.delay_policy==DelayPolicy.BEST_AVAILABLE and not viable:
                    alternatives=[p for p in alive if p!=self.path
                                  and alive[p]['age_ms']+8<alive[self.path]['age_ms']]
                    if alternatives:
                        return self._decision(min(alternatives,key=lambda p:alive[p]['age_ms']),0,'best_available_degraded_path',now)
                if alternatives:return self._decision(min(alternatives,key=lambda p:viable[p]['age_ms']),0,
                    'severe_lowest_level_unhealthy' if confirmed_severe else 'lowest_level_unhealthy',now)
        else:self.bad_since=None
        # Switch healthy primaries only for a sustained, materially better route.
        if good and best!=self.path and (self.proven_levels[best]>self.proven_levels[self.path]
                or viable[best]['age_ms']+8<viable[self.path]['age_ms']):
            if self.better_since is None:self.better_since=now
            if now-self.better_since>=c.stable_s*2:return self._decision(best,0,'sustained_better_path',now)
        else:self.better_since=None
        return None


class AdaptiveVideoSender(DualQualitySender):
    def __init__(self,source,baseline_paths,high_paths,epoch,config=None,*,on_adaptation=None,**kwargs):
        self.adaptive=config or AdaptiveConfig();self.all_high_paths=list(high_paths)
        base=list(baseline_paths)
        ids={p.path_id for p in base}
        if {p.path_id for p in high_paths}!=ids or len(high_paths)!=len(ids):raise ValueError('Matching enhancement paths required')
        if any(p.epoch!=epoch+'-high' for p in high_paths):raise ValueError('High epoch mismatch')
        self.policy=AdaptivePolicy([p.path_id for p in base],self.adaptive)
        self.on_adaptation=on_adaptation;self._feedback_lock=threading.Lock()
        self._pending_feedback=None;self.adaptations=[]
        if any(p.width>source.width or p.height>source.height for p in self.adaptive.levels):
            raise ValueError('Ladder exceeds source resolution')
        super().__init__(source,base,[self.all_high_paths[0]],epoch,self.adaptive.dual(base[0].path_id),**kwargs)
        # A live upstream buffer may still have the preceding dimensions when
        # RECONFIGURE reaches the scaler. Accept those negotiated caps during
        # the transition instead of failing the whole capture tee.
        self._branches['high']['caps'].set_property('caps-change-mode',1)
        self.paths=base+self.all_high_paths
        for p in self.all_high_paths:
            p.on_error=self._fail;p.on_state=self.on_state;p.on_keyframe_request=lambda:self._keyframe('high')
        for p in base:p.on_video_feedback=self._feedback
        self.policy.changed=time.monotonic()
        self._apply(dict(path=base[0].path_id,level=0,reason='initial_baseline',at=self.policy.changed))

    def _feedback(self,value):
        validate_feedback(value,self.policy.paths)
        with self._feedback_lock:
            if self._pending_feedback is None or value['sequence']>self._pending_feedback['sequence']:
                self._pending_feedback=value

    def _apply(self,decision):
        profile=self.adaptive.levels[decision['level']]
        path=next(p for p in self.all_high_paths if p.path_id==decision['path'])
        with self._lock:
            r=self._branches['high'];r['paths']=[path];r['profile']=profile
            r['last_idr']=0.
        from .session import gst_modules
        r['encoder'].set_property('bitrate',profile.bitrate_kbps)
        r['caps'].set_property('caps',gst_modules().Caps.from_string(
            f'video/x-raw,format=I420,width={profile.width},height={profile.height},framerate={profile.fps}/1'))
        self._keyframe('high')
        self.adaptations.append(dict(decision,width=profile.width,height=profile.height,
                                     fps=profile.fps,bitrate_kbps=profile.bitrate_kbps))
        if len(self.adaptations)>128:self.adaptations.pop(0)
        if self.on_adaptation:self.on_adaptation(self.adaptations[-1])

    def _tick(self):
        with self._feedback_lock:
            feedback=self._pending_feedback;self._pending_feedback=None
        now=time.monotonic()
        if feedback:self.policy.observe(feedback,now)
        decision=self.policy.tick(now)
        if decision:self._apply(decision)

    def counters(self):
        result=super().counters()
        result['adaptation']=dict(primary=self.policy.path,level=self.policy.level,history=list(self.adaptations))
        return result


class AdaptiveVideoReceiver:
    def __init__(self,baseline_paths,high_paths,epoch,config=None,*,on_frame=None,on_selection=None,
                 on_path_unit=None,on_error=None,on_feedback=None,on_delivery_status=None):
        self.adaptive=config or AdaptiveConfig();self.base=list(baseline_paths);high=list(high_paths)
        ids={p.path_id for p in self.base}
        if {p.path_id for p in high}!=ids or len(high)!=len(ids):raise ValueError('Matching enhancement paths required')
        self.selector=QualitySelector(self.adaptive.dual(self.base[0].path_id),on_frame,on_selection,
                                      high_profiles=self.adaptive.levels,delay_policy=self.adaptive.delay_policy,
                                      on_delivery_status=on_delivery_status)
        self._stop=threading.Event();self._thread=None;self._lock=threading.Lock();self.sequence=0
        self.on_error=on_error;self.on_feedback=on_feedback;self._started=False
        self.units={(tier,p):deque(maxlen=64) for tier in ('baseline','high') for p in ids}
        self.decoded={p:deque(maxlen=64) for p in ids};self.receivers={}
        def unit(tier,path,u):
            now=time.monotonic()
            age=self.receivers[tier].clock_mapper.measure_age(u.source_ns,now,epoch+('-base' if tier=='baseline' else '-high'))
            if age.status=='MEASURED' and age.age_upper_ms is not None:
                with self._lock:self.units[tier,path].append((now,age.age_upper_ms))
            if on_path_unit:on_path_unit(tier,path,u)
        def frame(tier,f):
            if tier=='high' and f.path_id in self.decoded:
                with self._lock:self.decoded[f.path_id].append(time.monotonic())
            self.selector.push(tier,f)
        for tier,paths,profile,suffix in [('baseline',self.base,self.adaptive.levels[0],'-base'),
                                         ('high',high,self.adaptive.levels[-1],'-high')]:
            self.receivers[tier]=SharedVideoReceiver(paths,epoch+suffix,profile,
                delay_policy=self.adaptive.delay_policy,
                allowed_profiles=self.adaptive.levels if tier=='high' else None,
                on_frame=lambda f,t=tier:frame(t,f),on_path_unit=lambda p,u,t=tier:unit(t,p,u),on_error=self._fail)

    def _fail(self,error):
        self._stop.set()
        if self.on_error:self.on_error(error)

    def _feedback(self,now):
        self.sequence+=1;paths={};high={}
        with self._lock:
            for p in self.decoded:
                for tier,target in [('baseline',paths),('high',high)]:
                    values=[(t,a) for t,a in self.units[tier,p] if now-t<=1.]
                    fresh=bool(values) and now-values[-1][0]<=self.adaptive.stale_s
                    ages=sorted(a for _,a in values)
                    age=ages[min(len(ages)-1,math.ceil(.95*len(ages))-1)] if ages else None
                    target[p]=dict(fresh=fresh,age_ms=min(age,10000) if age is not None else None)
                    if tier=='high':target[p]['fps']=sum(now-t<=1. for t in self.decoded[p])
        value=dict(version=1,sequence=self.sequence,paths=paths,high=high)
        for path in self.base:path.send_video_feedback(value)
        if self.on_feedback:self.on_feedback(value)

    def _tick(self):
        last=-math.inf
        try:
            while not self._stop.wait(.01):
                self.selector.tick();now=time.monotonic()
                if now-last>=self.adaptive.feedback_interval_s:self._feedback(now);last=now
        except Exception as exc:self._fail(exc)

    def start(self):
        if self._started:raise RuntimeError('Adaptive receiver is single-use')
        self._started=True
        try:
            for r in self.receivers.values():r.start()
            self._thread=threading.Thread(target=self._tick,name='adaptive-video-feedback',daemon=True);self._thread.start()
        except Exception:self.stop();raise

    def stop(self):
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():self._thread.join(timeout=2)
        try:self.receivers['high'].stop()
        finally:self.receivers['baseline'].stop()

    def counters(self):
        tiers={tier:r.counters() for tier,r in self.receivers.items()}
        return dict(codecs={k:sum(r['codecs'][k] for r in tiers.values()) for k in ('encoders','decoders')},
                    tiers=tiers,selection=dict(self.selector.stats),feedback_sequence=self.sequence)
